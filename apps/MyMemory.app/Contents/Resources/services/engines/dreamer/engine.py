"""
Dreamer Engine — orchestrator for structural graph analysis.

Phase 3 of the pipeline: Collect & Normalize -> Ingestion -> Enrichment -> DREAMER

Pass order:
  1. DETERMINISTIC — schema-invalid edges, self-aliases, UUID-aliases (free, no LLM)
  2. MERGE — name-based discovery -> LLM evaluation -> skip-cache
  3. RENAME — regex discovery -> LLM evaluation -> flag-as-state
  4. SPLIT/RECAT — reads quality_flags from enrichment -> LLM evaluation -> clear flag
"""

import json
import logging
import os
from typing import Dict, List, Optional

from services.utils.graph_service import GraphService
from services.utils.vector_service import VectorService
from services.utils.llm_service import LLMService
from services.utils.schema_validator import SchemaValidator
from services.utils.config_loader import get_config
from services.utils.terminal_status import status as terminal_status

from services.engines.dreamer import (
    pass_deterministic,
    pass_merge,
    pass_rename,
    pass_split,
    pass_recategorize,
)

LOGGER = logging.getLogger('Dreamer')

# Load config
try:
    _CONFIG = get_config()
except FileNotFoundError:
    LOGGER.warning("Config file not found, using empty config for Dreamer")
    _CONFIG = {}


_SCHEMA_VALIDATOR = None


def _get_schema_validator():
    global _SCHEMA_VALIDATOR
    if _SCHEMA_VALIDATOR is None:
        _SCHEMA_VALIDATOR = SchemaValidator()
    return _SCHEMA_VALIDATOR


class Dreamer:
    """Systematic structural analysis of the entire knowledge graph."""

    def __init__(self, graph_service: GraphService, vector_service: VectorService,
                 prompts_path: str = "config/services_prompts.yaml"):
        self.graph_service = graph_service
        self.vector_service = vector_service
        self.llm_service = LLMService()
        self.prompts = self._load_prompts(prompts_path)
        self.schema_validator = _get_schema_validator()
        self.schema = self.schema_validator.schema

        # Config
        self.dreamer_config = _CONFIG.get('dreamer', {})

        # LLM settings
        model_key = self.dreamer_config.get('model', 'fast')
        self.model_id = self.llm_service.models.get(model_key, self.llm_service.models['fast'])
        self.max_output_tokens = self.dreamer_config.get('max_output_tokens', 32768)

    def _load_prompts(self, path: str) -> dict:
        try:
            import yaml
            with open(path, "r") as f:
                data = yaml.safe_load(f)
                return data.get("dreamer", {})
        except Exception as e:
            LOGGER.error(f"Failed to load dreamer prompts from {path}: {e}")
            raise

    def discover_and_evaluate_all(self, passes: Optional[List[str]] = None,
                                   limit: int = 0) -> Dict:
        """Phase 1: Run discover_and_evaluate for all LLM passes (graph reads + LLM).

        Does NOT run deterministic pass (that needs exclusive lock).

        Args:
            passes: Optional list of LLM passes to run.
            limit: Max candidates per pass (0 = use config defaults).

        Returns:
            Dict with {pass_name: (stats, decisions)} for each LLM pass.
        """
        llm_passes = ['merge', 'rename', 'split', 'recategorize']
        active_passes = [p for p in (passes or llm_passes) if p in llm_passes]

        sweep_results = {}

        LOGGER.info(f"Discover+Evaluate starting (passes={active_passes})")

        if 'merge' in active_passes:
            terminal_status("dreamer", "Merge analysis", "processing")
            sweep_results["merge"] = pass_merge.discover_and_evaluate(self, False, limit)

        if 'rename' in active_passes:
            terminal_status("dreamer", "Rename analysis", "processing")
            sweep_results["rename"] = pass_rename.discover_and_evaluate(self, False, limit)

        if 'split' in active_passes:
            terminal_status("dreamer", "Split analysis", "processing")
            sweep_results["split"] = pass_split.discover_and_evaluate(self, False, limit)

        if 'recategorize' in active_passes:
            terminal_status("dreamer", "Recategorize analysis", "processing")
            sweep_results["recategorize"] = pass_recategorize.discover_and_evaluate(self, False, limit)

        return sweep_results

    def execute_all(self, sweep_results: Dict, passes: Optional[List[str]] = None,
                    dry_run: bool = False) -> Dict:
        """Phase 2: Execute decisions for all passes (graph + vector writes).

        Runs deterministic pass FIRST (under same exclusive lock),
        then execute_decisions for each LLM pass.

        Args:
            sweep_results: Output from discover_and_evaluate_all.
            passes: Optional list of passes to run.
            dry_run: If True, skip deterministic.

        Returns:
            Dict with stats per pass.
        """
        all_passes = ['deterministic', 'merge', 'rename', 'split', 'recategorize']
        active_passes = passes or all_passes

        stats = {
            "deterministic": {"invalid_edges": 0, "self_aliases": 0, "uuid_aliases": 0},
            "merge": {"candidates": 0, "merged": 0, "skipped": 0, "cached": 0},
            "rename": {"candidates": 0, "renamed": 0, "skipped": 0},
            "split": {"candidates": 0, "split": 0, "skipped": 0},
            "recategorize": {"candidates": 0, "recategorized": 0, "skipped": 0},
        }

        # Deterministic runs FIRST under exclusive lock (no LLM, fast)
        if 'deterministic' in active_passes and not dry_run:
            terminal_status("dreamer", "Deterministic cleanup", "processing")
            stats["deterministic"] = pass_deterministic.run(
                self.graph_service, self.schema_validator, self.dreamer_config, dry_run
            )

        # Execute LLM pass decisions
        for pass_name in ['merge', 'rename', 'split', 'recategorize']:
            if pass_name not in active_passes:
                continue
            if pass_name not in sweep_results:
                continue

            pass_stats, decisions = sweep_results[pass_name]

            if decisions:
                terminal_status("dreamer", f"{pass_name.capitalize()} execution", "processing")
                if pass_name == 'merge':
                    stats["merge"] = pass_merge.execute_decisions(self, pass_stats, decisions)
                elif pass_name == 'rename':
                    stats["rename"] = pass_rename.execute_decisions(self, pass_stats, decisions)
                elif pass_name == 'split':
                    stats["split"] = pass_split.execute_decisions(self, pass_stats, decisions)
                elif pass_name == 'recategorize':
                    stats["recategorize"] = pass_recategorize.execute_decisions(self, pass_stats, decisions)
            else:
                stats[pass_name] = pass_stats

        LOGGER.info(f"Execute all complete: {json.dumps(stats, indent=2)}")
        terminal_status("dreamer", "Sweep complete", "done",
                        detail=f"merged={stats['merge']['merged']} renamed={stats['rename']['renamed']}")

        return stats

    def run_sweep(self, dry_run: bool = False, passes: Optional[List[str]] = None,
                  limit: int = 0) -> Dict:
        """
        Run a full graph sweep.

        Convenience wrapper used by tests, dry-run, and rebuild (where caller holds locks).

        Args:
            dry_run: Show candidates without executing.
            passes: Optional list of passes to run (default: all).
                    Options: 'deterministic', 'merge', 'rename', 'split', 'recategorize'
            limit: Max candidates per pass (0 = use config defaults).

        Returns:
            Dict with stats per pass.
        """
        all_passes = ['deterministic', 'merge', 'rename', 'split', 'recategorize']
        active_passes = passes or all_passes

        stats = {
            "deterministic": {"invalid_edges": 0, "self_aliases": 0, "uuid_aliases": 0},
            "merge": {"candidates": 0, "merged": 0, "skipped": 0, "cached": 0},
            "rename": {"candidates": 0, "renamed": 0, "skipped": 0},
            "split": {"candidates": 0, "split": 0, "skipped": 0},
            "recategorize": {"candidates": 0, "recategorized": 0, "skipped": 0},
        }

        LOGGER.info(f"Graph Sweep starting (dry_run={dry_run}, passes={active_passes})")
        terminal_status("dreamer", "Starting sweep", "processing")

        if 'deterministic' in active_passes:
            terminal_status("dreamer", "Deterministic cleanup", "processing")
            stats["deterministic"] = pass_deterministic.run(
                self.graph_service, self.schema_validator, self.dreamer_config, dry_run
            )

        if 'merge' in active_passes:
            terminal_status("dreamer", "Merge analysis", "processing")
            stats["merge"] = pass_merge.run(self, dry_run, limit)

        if 'rename' in active_passes:
            terminal_status("dreamer", "Rename analysis", "processing")
            stats["rename"] = pass_rename.run(self, dry_run, limit)

        if 'split' in active_passes:
            terminal_status("dreamer", "Split analysis", "processing")
            stats["split"] = pass_split.run(self, dry_run, limit)

        if 'recategorize' in active_passes:
            terminal_status("dreamer", "Recategorize analysis", "processing")
            stats["recategorize"] = pass_recategorize.run(self, dry_run, limit)

        # Summary
        LOGGER.info(f"Graph Sweep complete: {json.dumps(stats, indent=2)}")
        terminal_status("dreamer", "Sweep complete", "done",
                        detail=f"merged={stats['merge']['merged']} renamed={stats['rename']['renamed']}")

        return stats
