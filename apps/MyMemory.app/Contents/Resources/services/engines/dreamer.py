#!/usr/bin/env python3
"""
Dreamer Engine (OBJEKT-107)

Systematic structural analysis of the entire knowledge graph.
Phase 3 of the pipeline: Collect & Normalize -> Ingestion -> Enrichment -> DREAMER

Pass order:
  1. DETERMINISTIC — schema-invalid edges, self-aliases, UUID-aliases (free, no LLM)
  2. MERGE — name-based discovery → LLM evaluation → skip-cache
  3. RENAME — regex discovery → LLM evaluation → flag-as-state
  4. SPLIT/RECAT — reads quality_flags from enrichment → LLM evaluation → clear flag
"""

import difflib
import hashlib
import json
import logging
import os
import re
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from services.utils.graph_service import GraphService
from services.utils.vector_service import VectorService
from services.utils.llm_service import LLMService
from services.utils.schema_validator import SchemaValidator
from services.utils.config_loader import get_config

# Import shared utilities from enrichment
from services.engines.enrichment import (
    Enrichment,
    _short_id,
    _node_name,
    get_schema_validator,
    ENRICHMENT_CONFIG,
)

# Load config
try:
    _CONFIG = get_config()
except FileNotFoundError:
    _CONFIG = {}
LOG_FILE = os.path.expanduser(_CONFIG.get('logging', {}).get('system_log', '~/MyMemory/Logs/my_mem_system.log'))

log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

LOGGER = logging.getLogger('Dreamer')

# Terminal status
from services.utils.terminal_status import status as terminal_status

# Config
DREAMER_CONFIG = _CONFIG.get('dreamer', {})
THRESHOLDS = DREAMER_CONFIG.get('thresholds', {})


# =====================================================================
# DISCOVERY HELPERS (deterministic, no LLM)
# =====================================================================

def _fold_diacritics(s: str) -> str:
    """Fold diacritics: Ohlén → ohlen, Björkengren → bjorkengren."""
    return unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode().lower()


def _tokenize(name: str) -> set:
    """Split name into lowercase folded tokens."""
    return set(_fold_diacritics(name).split())


def _is_single_token(name: str) -> bool:
    """Check if name is a single word."""
    return len(name.strip().split()) == 1


def _is_weak_name(name: str) -> bool:
    """Identify UUIDs, generic placeholders, or single-token names."""
    patterns = [
        r'^[0-9a-f]{8}-[0-9a-f]{4}',
        r'^(Talare|Speaker) \d+$',
        r'^Unknown$',
        r'^Unit_.*$',
    ]
    if _is_single_token(name):
        return True
    return any(re.match(p, name, re.I) for p in patterns)


class Dreamer:
    """Systematic structural analysis of the entire knowledge graph."""

    def __init__(self, graph_service: GraphService, vector_service: VectorService,
                 prompts_path: str = "config/services_prompts.yaml"):
        self.graph_service = graph_service
        self.vector_service = vector_service
        self.llm_service = LLMService()
        self.prompts = self._load_prompts(prompts_path)
        self.schema = get_schema_validator().schema

    def _load_prompts(self, path: str) -> dict:
        try:
            import yaml
            with open(path, "r") as f:
                data = yaml.safe_load(f)
                return data.get("dreamer", {})
        except Exception as e:
            LOGGER.error(f"Failed to load dreamer prompts from {path}: {e}")
            return {}

    # =====================================================================
    # SHARED: NODE CONTEXT BUILDER
    # =====================================================================

    def _build_edges_text(self, node_id: str) -> str:
        """Build human-readable edge summary for a node."""
        edges_out = self.graph_service.get_edges_from(node_id)
        edges_in = self.graph_service.get_edges_to(node_id)
        # Filter out MENTIONS/DEALS_WITH
        edges_out = [e for e in edges_out if e["type"] not in ("MENTIONS", "DEALS_WITH")]
        edges_in = [e for e in edges_in if e["type"] not in ("MENTIONS", "DEALS_WITH")]

        lines = []
        for e in edges_out:
            other = self.graph_service.get_node(e["target"])
            if other:
                name = other.get("properties", {}).get("name", e["target"])
                lines.append(f"→[{e['type']}]→ {name} ({other.get('type', '?')})")
                rc = e.get("properties", {}).get("relation_context", [])
                for entry in rc[-3:]:
                    text = entry.get("text", "") if isinstance(entry, dict) else str(entry)
                    if text:
                        lines.append(f"    {text}")
        for e in edges_in:
            other = self.graph_service.get_node(e["source"])
            if other:
                name = other.get("properties", {}).get("name", e["source"])
                lines.append(f"←[{e['type']}]← {name} ({other.get('type', '?')})")
                rc = e.get("properties", {}).get("relation_context", [])
                for entry in rc[-3:]:
                    text = entry.get("text", "") if isinstance(entry, dict) else str(entry)
                    if text:
                        lines.append(f"    {text}")

        return "\n".join(lines) if lines else "(Inga relationer)"

    def _get_edge_count(self, node_id: str) -> int:
        """Count non-MENTIONS edges for a node."""
        edges_out = self.graph_service.get_edges_from(node_id)
        edges_in = self.graph_service.get_edges_to(node_id)
        return len([e for e in edges_out + edges_in if e["type"] not in ("MENTIONS", "DEALS_WITH")])

    # =====================================================================
    # SKIP-CACHE (MERGE only)
    # =====================================================================

    def _load_merge_skip_cache(self) -> dict:
        """Load skip-cache from JSON file."""
        cache_file = os.path.expanduser(
            DREAMER_CONFIG.get('merge', {}).get(
                'skip_cache_file', '~/Library/Application Support/MyMemory/merge_skip_cache.json')
        )
        if not os.path.exists(cache_file):
            return {}
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            LOGGER.warning(f"Could not load merge skip-cache: {e}")
            return {}

    def _save_merge_skip_cache(self, cache: dict):
        """Save skip-cache to JSON file."""
        cache_file = os.path.expanduser(
            DREAMER_CONFIG.get('merge', {}).get(
                'skip_cache_file', '~/Library/Application Support/MyMemory/merge_skip_cache.json')
        )
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        try:
            with open(cache_file, 'w') as f:
                json.dump(cache, f, indent=2, ensure_ascii=False)
        except OSError as e:
            LOGGER.error(f"Could not save merge skip-cache: {e}")

    def _compute_node_hash(self, node: dict) -> str:
        """Compute hash for a node based on context_summary + edge_count."""
        props = node.get("properties", {})
        ctx = props.get("context_summary", "")
        edge_count = self._get_edge_count(node["id"])
        raw = f"{ctx}|{edge_count}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _compute_pair_key(self, id_a: str, id_b: str) -> str:
        """Compute deterministic pair key (sorted)."""
        return "|".join(sorted([id_a, id_b]))

    # =====================================================================
    # DISCOVERY: MERGE CANDIDATES
    # =====================================================================

    def _discover_merge_candidates(self, max_candidates: int = None) -> List[Tuple[dict, dict, str]]:
        """Find merge candidates using name-based strategies.

        Returns list of (node_a, node_b, strategy) tuples.
        max_candidates=None means use config default. 0 means unlimited.
        """
        merge_config = DREAMER_CONFIG.get('merge', {})
        if not merge_config.get('enabled', True):
            return []

        strategies = merge_config.get('strategies', ['token_subset', 'diacritics_fold', 'fuzzy'])
        fuzzy_cutoff = merge_config.get('fuzzy_cutoff', 0.80)
        if max_candidates is None:
            max_candidates = merge_config.get('max_candidates_per_run', 20)

        # Load all entity nodes (skip Source + Event — events have dates in names, never merge)
        valid_types = set(self.schema.get("nodes", {}).keys())
        rows = self.graph_service.conn.execute(
            "SELECT id, type, properties FROM nodes WHERE type NOT IN ('Source', 'Event')"
        ).fetchall()

        nodes_by_type = {}
        for r in rows:
            if r[1] not in valid_types:
                continue
            node = self.graph_service.get_node(r[0])
            if node:
                node_type = node.get("type", "")
                if node_type not in nodes_by_type:
                    nodes_by_type[node_type] = []
                nodes_by_type[node_type].append(node)

        candidates = []
        seen_pairs = set()

        for node_type, nodes in nodes_by_type.items():
            for i, node_a in enumerate(nodes):
                name_a = _node_name(node_a)
                tokens_a = _tokenize(name_a)
                folded_a = _fold_diacritics(name_a)

                for node_b in nodes[i+1:]:
                    if max_candidates and len(candidates) >= max_candidates:
                        break

                    name_b = _node_name(node_b)
                    pair_key = self._compute_pair_key(node_a["id"], node_b["id"])
                    if pair_key in seen_pairs:
                        continue

                    matched_strategy = None
                    tokens_b = _tokenize(name_b)
                    folded_b = _fold_diacritics(name_b)

                    # Strategy 1: Token subset
                    if 'token_subset' in strategies:
                        if tokens_a and tokens_b:
                            smaller, larger = (tokens_a, tokens_b) if len(tokens_a) <= len(tokens_b) else (tokens_b, tokens_a)
                            if smaller.issubset(larger) and smaller != larger:
                                matched_strategy = "token_subset"

                    # Strategy 2: Diacritics fold
                    if not matched_strategy and 'diacritics_fold' in strategies:
                        if folded_a == folded_b and name_a != name_b:
                            matched_strategy = "diacritics_fold"

                    # Strategy 3a: Fuzzy first-token (single-token vs multi-token)
                    if not matched_strategy and 'fuzzy' in strategies:
                        if len(tokens_a) == 1 and len(tokens_b) > 1:
                            first_b = folded_b.split()[0]
                            ratio = difflib.SequenceMatcher(None, folded_a, first_b).ratio()
                            if ratio >= fuzzy_cutoff:
                                matched_strategy = "fuzzy_first_token"
                        elif len(tokens_b) == 1 and len(tokens_a) > 1:
                            first_a = folded_a.split()[0]
                            ratio = difflib.SequenceMatcher(None, folded_b, first_a).ratio()
                            if ratio >= fuzzy_cutoff:
                                matched_strategy = "fuzzy_first_token"

                    # Strategy 3b: Fuzzy full name (same token count, >=2 tokens)
                    if not matched_strategy and 'fuzzy' in strategies:
                        if len(tokens_a) == len(tokens_b) and len(tokens_a) >= 2:
                            ratio = difflib.SequenceMatcher(None, folded_a, folded_b).ratio()
                            if ratio >= fuzzy_cutoff and name_a != name_b:
                                matched_strategy = "fuzzy_full"

                    if matched_strategy:
                        seen_pairs.add(pair_key)
                        candidates.append((node_a, node_b, matched_strategy))

                if max_candidates and len(candidates) >= max_candidates:
                    break

        LOGGER.info(f"Merge discovery: {len(candidates)} candidates from {sum(len(v) for v in nodes_by_type.values())} nodes")
        return candidates

    # =====================================================================
    # DISCOVERY: RENAME CANDIDATES
    # =====================================================================

    def _discover_rename_candidates(self, max_candidates: int = None) -> List[Tuple[dict, str, str]]:
        """Find rename candidates: single-token names with full name in context.

        Returns list of (node, suggested_name, reason) tuples.
        max_candidates=None means use config default. 0 means unlimited.
        """
        rename_config = DREAMER_CONFIG.get('rename', {})
        if not rename_config.get('enabled', True):
            return []

        if max_candidates is None:
            max_candidates = rename_config.get('max_candidates_per_run', 30)

        rows = self.graph_service.conn.execute(
            "SELECT id, type, properties FROM nodes WHERE type = 'Person'"
        ).fetchall()

        candidates = []
        for r in rows:
            if max_candidates and len(candidates) >= max_candidates:
                break

            props = json.loads(r[2]) if r[2] else {}
            name = props.get("name", r[0])

            if not _is_single_token(name):
                continue

            ctx = props.get("context_summary", "")
            if not ctx:
                continue

            # Regex: find "Name Lastname" pattern in context
            pattern = re.compile(
                rf'\b{re.escape(name)}\s+([A-ZÅÄÖ][a-zåäöé]+(?:\s+[A-ZÅÄÖ][a-zåäöé]+)*)',
                re.UNICODE
            )
            match = pattern.search(ctx)
            if match:
                suggested = f"{name} {match.group(1)}"
                node = self.graph_service.get_node(r[0])
                if node:
                    candidates.append((node, suggested, "single_token_with_full_name_in_context"))

        LOGGER.info(f"Rename discovery: {len(candidates)} candidates")
        return candidates

    # =====================================================================
    # DETERMINISTIC CLEANUP (no LLM)
    # =====================================================================

    def _count_deterministic_issues(self) -> dict:
        """Count deterministic issues without modifying the graph (for dry-run)."""
        result = {"invalid_edges": 0, "self_aliases": 0, "uuid_aliases": 0}

        # Count invalid edges
        if DREAMER_CONFIG.get('deterministic', {}).get('invalid_edge_cleanup', True):
            validator = get_schema_validator()
            rows = self.graph_service.conn.execute(
                "SELECT source, target, edge_type FROM edges WHERE edge_type != 'MENTIONS' AND edge_type != 'DEALS_WITH'"
            ).fetchall()
            for source_id, target_id, edge_type in rows:
                source_node = self.graph_service.get_node(source_id)
                target_node = self.graph_service.get_node(target_id)
                if not source_node or not target_node:
                    result["invalid_edges"] += 1
                    continue
                nodes_map = {
                    source_id: source_node.get("type", "Unknown"),
                    target_id: target_node.get("type", "Unknown"),
                }
                edge = {"source": source_id, "target": target_id, "type": edge_type}
                ok, msg = validator.validate_edge(edge, nodes_map)
                if not ok:
                    result["invalid_edges"] += 1

            mentions_rows = self.graph_service.conn.execute(
                "SELECT source, target, edge_type FROM edges WHERE edge_type = 'MENTIONS'"
            ).fetchall()
            for source_id, target_id, edge_type in mentions_rows:
                target_node = self.graph_service.get_node(target_id)
                if not target_node or target_node.get("type") == "Roles":
                    result["invalid_edges"] += 1

        # Count self-aliases
        if DREAMER_CONFIG.get('deterministic', {}).get('self_alias_cleanup', True):
            rows = self.graph_service.conn.execute(
                "SELECT id, aliases, properties FROM nodes WHERE aliases IS NOT NULL AND aliases != '[]'"
            ).fetchall()
            for node_id, aliases_str, props_str in rows:
                aliases = json.loads(aliases_str) if aliases_str else []
                props = json.loads(props_str) if props_str else {}
                name = props.get("name", node_id)
                result["self_aliases"] += sum(1 for a in aliases if a == name)

        # Count UUID-aliases
        if DREAMER_CONFIG.get('deterministic', {}).get('uuid_alias_cleanup', True):
            uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
            rows = self.graph_service.conn.execute(
                "SELECT id, aliases FROM nodes WHERE aliases IS NOT NULL AND aliases != '[]'"
            ).fetchall()
            for node_id, aliases_str in rows:
                aliases = json.loads(aliases_str) if aliases_str else []
                result["uuid_aliases"] += sum(1 for a in aliases if uuid_pattern.match(a))

        return result

    def _cleanup_invalid_edges(self) -> int:
        """Remove edges that violate the schema. Returns count removed."""
        if not DREAMER_CONFIG.get('deterministic', {}).get('invalid_edge_cleanup', True):
            return 0

        validator = get_schema_validator()
        rows = self.graph_service.conn.execute(
            "SELECT source, target, edge_type FROM edges WHERE edge_type != 'MENTIONS' AND edge_type != 'DEALS_WITH'"
        ).fetchall()

        removed = 0
        for source_id, target_id, edge_type in rows:
            source_node = self.graph_service.get_node(source_id)
            target_node = self.graph_service.get_node(target_id)
            if not source_node or not target_node:
                # Orphaned edge — remove
                self.graph_service.delete_edge(source_id, target_id, edge_type)
                removed += 1
                continue

            nodes_map = {
                source_id: source_node.get("type", "Unknown"),
                target_id: target_node.get("type", "Unknown"),
            }
            edge = {"source": source_id, "target": target_id, "type": edge_type}
            ok, msg = validator.validate_edge(edge, nodes_map)
            if not ok:
                self.graph_service.delete_edge(source_id, target_id, edge_type)
                removed += 1

        # Also check MENTIONS edges: only valid targets (not Roles)
        mentions_rows = self.graph_service.conn.execute(
            "SELECT source, target, edge_type FROM edges WHERE edge_type = 'MENTIONS'"
        ).fetchall()
        for source_id, target_id, edge_type in mentions_rows:
            target_node = self.graph_service.get_node(target_id)
            if not target_node:
                self.graph_service.delete_edge(source_id, target_id, edge_type)
                removed += 1
                continue
            target_type = target_node.get("type", "")
            # MENTIONS should not point to Roles (FINDING-1 from PoC)
            if target_type == "Roles":
                self.graph_service.delete_edge(source_id, target_id, edge_type)
                removed += 1

        if removed:
            LOGGER.info(f"Deterministic: removed {removed} invalid edges")
        return removed

    def _cleanup_self_aliases(self) -> int:
        """Remove aliases where alias == name. Returns count cleaned."""
        if not DREAMER_CONFIG.get('deterministic', {}).get('self_alias_cleanup', True):
            return 0

        rows = self.graph_service.conn.execute(
            "SELECT id, aliases, properties FROM nodes WHERE aliases IS NOT NULL AND aliases != '[]'"
        ).fetchall()

        cleaned = 0
        for node_id, aliases_str, props_str in rows:
            aliases = json.loads(aliases_str) if aliases_str else []
            props = json.loads(props_str) if props_str else {}
            name = props.get("name", node_id)

            new_aliases = [a for a in aliases if a != name]
            if len(new_aliases) < len(aliases):
                self.graph_service.upsert_node(node_id, None, new_aliases, {})
                cleaned += len(aliases) - len(new_aliases)

        if cleaned:
            LOGGER.info(f"Deterministic: removed {cleaned} self-aliases")
        return cleaned

    def _cleanup_uuid_aliases(self) -> int:
        """Remove UUID-like aliases. Returns count cleaned."""
        if not DREAMER_CONFIG.get('deterministic', {}).get('uuid_alias_cleanup', True):
            return 0

        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)

        rows = self.graph_service.conn.execute(
            "SELECT id, aliases, properties FROM nodes WHERE aliases IS NOT NULL AND aliases != '[]'"
        ).fetchall()

        cleaned = 0
        for node_id, aliases_str, props_str in rows:
            aliases = json.loads(aliases_str) if aliases_str else []
            new_aliases = [a for a in aliases if not uuid_pattern.match(a)]
            if len(new_aliases) < len(aliases):
                self.graph_service.upsert_node(node_id, None, new_aliases, {})
                cleaned += len(aliases) - len(new_aliases)

        if cleaned:
            LOGGER.info(f"Deterministic: removed {cleaned} UUID aliases")
        return cleaned

    # =====================================================================
    # LLM EVALUATION
    # =====================================================================

    def _call_llm_json(self, prompt: str, step_name: str) -> Optional[Dict]:
        """Call LLM and return parsed JSON. Reuses Dreamer's repair logic."""
        model_key = DREAMER_CONFIG.get('model', 'fast')
        model_id = self.llm_service.models.get(model_key, self.llm_service.models['fast'])
        max_output_tokens = DREAMER_CONFIG.get('max_output_tokens', 32768)

        response = self.llm_service.generate(
            prompt=prompt,
            provider='anthropic',
            model=model_id,
            max_tokens=max_output_tokens
        )

        if not response.success:
            LOGGER.error(f"LLM {step_name} failed: {response.error}")
            return None

        try:
            cleaned = response.text.replace("```json", "").replace("```", "").strip()
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            LOGGER.error(f"LLM {step_name} JSON parse failed: {e}")
            # Try truncated JSON repair
            try:
                for i in range(len(cleaned) - 1, 0, -1):
                    if cleaned[i] == "}":
                        attempt = cleaned[:i+1]
                        open_b = attempt.count("[") - attempt.count("]")
                        open_c = attempt.count("{") - attempt.count("}")
                        suffix = "]" * open_b + "}" * open_c
                        try:
                            result = json.loads(attempt + suffix)
                            LOGGER.warning(f"LLM {step_name}: repaired truncated JSON")
                            return result
                        except json.JSONDecodeError:
                            continue
            except Exception:
                pass
            return None

    def _evaluate_merge(self, node_a: dict, node_b: dict) -> Optional[Dict]:
        """Evaluate a merge candidate pair via LLM."""
        template = self.prompts.get("merge_evaluation", "")
        if not template:
            LOGGER.error("Missing merge_evaluation prompt")
            return None

        props_a = node_a.get("properties", {})
        props_b = node_b.get("properties", {})

        prompt = template.format(
            name_a=props_a.get("name", node_a["id"]),
            type_a=node_a.get("type", "?"),
            context_a=props_a.get("context_summary", "(saknas)"),
            edges_a=self._build_edges_text(node_a["id"]),
            aliases_a=node_a.get("aliases", []),
            name_b=props_b.get("name", node_b["id"]),
            type_b=node_b.get("type", "?"),
            context_b=props_b.get("context_summary", "(saknas)"),
            edges_b=self._build_edges_text(node_b["id"]),
            aliases_b=node_b.get("aliases", []),
        )

        return self._call_llm_json(prompt, f"merge_eval({_short_id(node_a['id'])}↔{_short_id(node_b['id'])})")

    def _evaluate_rename(self, node: dict, suggested_name: str, reason: str) -> Optional[Dict]:
        """Evaluate a rename candidate via LLM."""
        template = self.prompts.get("rename_evaluation", "")
        if not template:
            LOGGER.error("Missing rename_evaluation prompt")
            return None

        props = node.get("properties", {})
        prompt = template.format(
            current_name=props.get("name", node["id"]),
            node_type=node.get("type", "?"),
            context_summary=props.get("context_summary", "(saknas)"),
            suggested_name=suggested_name,
            reason=reason,
            edges_text=self._build_edges_text(node["id"]),
        )

        return self._call_llm_json(prompt, f"rename_eval({_short_id(node['id'])})")

    def _evaluate_split(self, node: dict, flag_reason: str) -> Optional[Dict]:
        """Evaluate a split candidate via LLM."""
        template = self.prompts.get("split_evaluation", "")
        if not template:
            LOGGER.error("Missing split_evaluation prompt")
            return None

        props = node.get("properties", {})
        prompt = template.format(
            node_name=props.get("name", node["id"]),
            node_type=node.get("type", "?"),
            context_summary=props.get("context_summary", "(saknas)"),
            aliases=node.get("aliases", []),
            edges_text=self._build_edges_text(node["id"]),
            flag_reason=flag_reason,
        )

        return self._call_llm_json(prompt, f"split_eval({_short_id(node['id'])})")

    def _evaluate_recategorize(self, node: dict, flag_reason: str) -> Optional[Dict]:
        """Evaluate a recategorize candidate via LLM."""
        template = self.prompts.get("recategorize_evaluation", "")
        if not template:
            LOGGER.error("Missing recategorize_evaluation prompt")
            return None

        props = node.get("properties", {})
        allowed_types = list(self.schema.get("nodes", {}).keys())

        prompt = template.format(
            node_name=props.get("name", node["id"]),
            current_type=node.get("type", "?"),
            context_summary=props.get("context_summary", "(saknas)"),
            edges_text=self._build_edges_text(node["id"]),
            allowed_types=", ".join(allowed_types),
            flag_reason=flag_reason,
        )

        return self._call_llm_json(prompt, f"recat_eval({_short_id(node['id'])})")

    # =====================================================================
    # EXECUTION (uses graph_service methods)
    # =====================================================================

    def _execute_merge(self, primary_id: str, secondary_id: str) -> bool:
        """Execute a merge operation."""
        try:
            units = self.graph_service.get_related_unit_ids(secondary_id)
            self.graph_service.merge_nodes(primary_id, secondary_id)
            self.vector_service.delete(secondary_id)
            merged_node = self.graph_service.get_node(primary_id)
            if merged_node:
                self.vector_service.upsert_node(merged_node)
            # Propagate to affected Lake files
            if units:
                enrichment = Enrichment(self.graph_service, self.vector_service)
                enrichment.propagate_changes(list(units))
            return True
        except Exception as e:
            LOGGER.error(f"Merge execution failed: {e}")
            return False

    def _execute_rename(self, node_id: str, new_name: str) -> bool:
        """Execute a rename operation."""
        try:
            units = self.graph_service.get_related_unit_ids(node_id)
            self.graph_service.rename_node(node_id, new_name)
            # rename_node may auto-merge if new_name exists
            renamed_node = self.graph_service.get_node(new_name)
            if renamed_node:
                self.vector_service.upsert_node(renamed_node)
            if units:
                enrichment = Enrichment(self.graph_service, self.vector_service)
                enrichment.propagate_changes(list(units))
            return True
        except Exception as e:
            LOGGER.error(f"Rename execution failed: {e}")
            return False

    def _execute_split(self, node_id: str, clusters: list) -> bool:
        """Execute a split operation."""
        try:
            units = self.graph_service.get_related_unit_ids(node_id)
            new_nodes = self.graph_service.split_node(node_id, clusters)
            if new_nodes:
                for new_nid in new_nodes:
                    new_node = self.graph_service.get_node(new_nid)
                    if new_node:
                        self.vector_service.upsert_node(new_node)
            if units:
                enrichment = Enrichment(self.graph_service, self.vector_service)
                enrichment.propagate_changes(list(units))
            return True
        except Exception as e:
            LOGGER.error(f"Split execution failed: {e}")
            return False

    def _execute_recategorize(self, node_id: str, new_type: str) -> bool:
        """Execute a recategorize operation."""
        try:
            # Validate edges before changing type
            enrichment = Enrichment(self.graph_service, self.vector_service)
            valid_edges, invalid_edges = enrichment._validate_edges_for_recategorize(node_id, new_type)
            for edge in invalid_edges:
                self.graph_service.delete_edge(edge["source"], edge["target"], edge["type"])

            units = self.graph_service.get_related_unit_ids(node_id)
            self.graph_service.recategorize_node(node_id, new_type)
            recat_node = self.graph_service.get_node(node_id)
            if recat_node:
                self.vector_service.upsert_node(recat_node)
            if units:
                enrichment.propagate_changes(list(units))
            if invalid_edges:
                LOGGER.info(f"Recategorize {_short_id(node_id)} → {new_type}: "
                            f"removed {len(invalid_edges)} invalid edges")
            return True
        except Exception as e:
            LOGGER.error(f"Recategorize execution failed: {e}")
            return False

    def _clear_quality_flag(self, node_id: str, flag: str):
        """Remove a specific quality flag from a node."""
        node = self.graph_service.get_node(node_id)
        if not node:
            return
        props = node.get("properties", {})
        flags = props.get("quality_flags", [])
        if flag in flags:
            flags.remove(flag)
            props["quality_flags"] = flags
            if not flags:
                props.pop("quality_flags", None)
                props.pop("quality_flag_reason", None)
            self.graph_service.update_node_properties(node_id, props)

    # =====================================================================
    # MAIN SWEEP
    # =====================================================================

    def run_sweep(self, dry_run: bool = False, passes: Optional[List[str]] = None,
                  limit: int = 0) -> Dict:
        """
        Run a full graph sweep.

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

        # --- Pass 1: DETERMINISTIC ---
        if 'deterministic' in active_passes:
            LOGGER.info("Pass 1: DETERMINISTIC cleanup")
            terminal_status("dreamer", "Deterministic cleanup", "processing")
            if not dry_run:
                stats["deterministic"]["invalid_edges"] = self._cleanup_invalid_edges()
                stats["deterministic"]["self_aliases"] = self._cleanup_self_aliases()
                stats["deterministic"]["uuid_aliases"] = self._cleanup_uuid_aliases()
            else:
                det = self._count_deterministic_issues()
                stats["deterministic"] = det
                LOGGER.info(f"  [DRY] Deterministic: {det['invalid_edges']} invalid edges, "
                            f"{det['self_aliases']} self-aliases, {det['uuid_aliases']} UUID-aliases")

        # --- Pass 2: MERGE ---
        if 'merge' in active_passes:
            LOGGER.info("Pass 2: MERGE")
            terminal_status("dreamer", "Merge analysis", "processing")
            # dry-run: discover ALL candidates. execution: respect config/limit cap.
            if dry_run:
                candidates = self._discover_merge_candidates(max_candidates=limit if limit else 0)
            else:
                candidates = self._discover_merge_candidates(max_candidates=limit if limit else None)
            stats["merge"]["candidates"] = len(candidates)

            if not dry_run and candidates:
                skip_cache = self._load_merge_skip_cache()
                threshold = THRESHOLDS.get('merge', 0.90)

                for node_a, node_b, strategy in candidates:
                    pair_key = self._compute_pair_key(node_a["id"], node_b["id"])
                    hash_a = self._compute_node_hash(node_a)
                    hash_b = self._compute_node_hash(node_b)

                    # Check skip-cache
                    cached = skip_cache.get(pair_key)
                    if cached:
                        if cached.get("hash_a") == hash_a and cached.get("hash_b") == hash_b:
                            stats["merge"]["cached"] += 1
                            continue

                    result = self._evaluate_merge(node_a, node_b)
                    if not result:
                        continue

                    decision = result.get("decision", "SKIP")
                    conf = result.get("confidence", 0)
                    reason = result.get("reason", "")

                    if decision == "MERGE" and conf >= threshold:
                        primary = result.get("primary_node_id", node_a["id"])
                        secondary = result.get("secondary_node_id", node_b["id"])
                        LOGGER.info(f"MERGE: {_short_id(secondary)} → {_short_id(primary)} "
                                    f"(conf={conf:.2f}, strategy={strategy}) {reason}")
                        terminal_status("dreamer",
                                        f"MERGE: {_short_id(secondary)} → {_short_id(primary)}", "done")
                        self._execute_merge(primary, secondary)
                        stats["merge"]["merged"] += 1
                        # Remove from cache (nodes changed)
                        skip_cache.pop(pair_key, None)
                    else:
                        LOGGER.info(f"MERGE SKIP: {_node_name(node_a)} ↔ {_node_name(node_b)} "
                                    f"(conf={conf:.2f}) {reason}")
                        # Add to skip-cache
                        skip_cache[pair_key] = {
                            "hash_a": hash_a, "hash_b": hash_b,
                            "decision": decision, "confidence": conf,
                            "timestamp": datetime.now().isoformat(),
                        }
                        stats["merge"]["skipped"] += 1

                self._save_merge_skip_cache(skip_cache)
            elif dry_run and candidates:
                details = []
                for node_a, node_b, strategy in candidates:
                    details.append({
                        "a": _node_name(node_a), "b": _node_name(node_b),
                        "type": node_a.get("type", "?"), "strategy": strategy,
                    })
                    LOGGER.info(f"  [DRY] MERGE candidate: {_node_name(node_a)} ↔ {_node_name(node_b)} ({strategy})")
                stats["merge"]["details"] = details

        # --- Pass 3: RENAME ---
        if 'rename' in active_passes:
            LOGGER.info("Pass 3: RENAME")
            terminal_status("dreamer", "Rename analysis", "processing")
            if dry_run:
                candidates = self._discover_rename_candidates(max_candidates=limit if limit else 0)
            else:
                candidates = self._discover_rename_candidates(max_candidates=limit if limit else None)
            stats["rename"]["candidates"] = len(candidates)

            if not dry_run and candidates:
                threshold_normal = THRESHOLDS.get('rename_normal', 0.95)
                threshold_weak = THRESHOLDS.get('rename_weak', 0.70)

                for node, suggested, reason in candidates:
                    result = self._evaluate_rename(node, suggested, reason)
                    if not result:
                        continue

                    decision = result.get("decision", "SKIP")
                    conf = result.get("confidence", 0)
                    new_name = result.get("new_name")

                    is_weak = _is_weak_name(_node_name(node))
                    threshold = threshold_weak if is_weak else threshold_normal

                    if decision == "RENAME" and conf >= threshold and new_name:
                        LOGGER.info(f"RENAME: {_node_name(node)} → {new_name} (conf={conf:.2f})")
                        terminal_status("dreamer", f"RENAME: {_node_name(node)} → {new_name}", "done")
                        self._execute_rename(node["id"], new_name)
                        stats["rename"]["renamed"] += 1
                    else:
                        LOGGER.info(f"RENAME SKIP: {_node_name(node)} (conf={conf:.2f}) {result.get('reason', '')}")
                        stats["rename"]["skipped"] += 1
            elif dry_run and candidates:
                details = []
                for node, suggested, reason in candidates:
                    details.append({
                        "current": _node_name(node), "suggested": suggested, "reason": reason,
                    })
                    LOGGER.info(f"  [DRY] RENAME candidate: {_node_name(node)} → {suggested} ({reason})")
                stats["rename"]["details"] = details

        # --- Pass 4: SPLIT (from quality_flags) ---
        if 'split' in active_passes:
            LOGGER.info("Pass 4: SPLIT (quality_flags)")
            terminal_status("dreamer", "Split analysis", "processing")
            split_config = DREAMER_CONFIG.get('split', {})
            max_split = limit or split_config.get('max_candidates_per_run', 10)

            # Find flagged nodes
            rows = self.graph_service.conn.execute(
                "SELECT id, properties FROM nodes WHERE "
                "json_extract(properties, '$.quality_flags') IS NOT NULL"
            ).fetchall()

            split_candidates = []
            for node_id, props_str in rows:
                if len(split_candidates) >= max_split:
                    break
                props = json.loads(props_str) if props_str else {}
                flags = props.get("quality_flags", [])
                if "possible_split" in flags:
                    node = self.graph_service.get_node(node_id)
                    if node:
                        flag_reason = props.get("quality_flag_reason", {}).get("possible_split", "Flagged by enrichment")
                        split_candidates.append((node, flag_reason))

            stats["split"]["candidates"] = len(split_candidates)

            if not dry_run and split_candidates:
                threshold = THRESHOLDS.get('split', 0.90)

                for node, flag_reason in split_candidates:
                    result = self._evaluate_split(node, flag_reason)
                    if not result:
                        continue

                    decision = result.get("decision", "SKIP")
                    conf = result.get("confidence", 0)

                    if decision == "SPLIT" and conf >= threshold:
                        clusters = result.get("split_clusters", [])
                        if clusters:
                            LOGGER.info(f"SPLIT: {_node_name(node)} → {len(clusters)} clusters (conf={conf:.2f})")
                            terminal_status("dreamer",
                                            f"SPLIT: {_node_name(node)} → {len(clusters)}", "done")
                            self._execute_split(node["id"], clusters)
                            stats["split"]["split"] += 1
                    else:
                        stats["split"]["skipped"] += 1

                    # Clear flag regardless of decision
                    self._clear_quality_flag(node["id"], "possible_split")
            elif dry_run and split_candidates:
                details = []
                for node, flag_reason in split_candidates:
                    details.append({"name": _node_name(node), "reason": flag_reason})
                    LOGGER.info(f"  [DRY] SPLIT candidate: {_node_name(node)} — {flag_reason}")
                stats["split"]["details"] = details

        # --- Pass 5: RECATEGORIZE (from quality_flags) ---
        if 'recategorize' in active_passes:
            LOGGER.info("Pass 5: RECATEGORIZE (quality_flags)")
            terminal_status("dreamer", "Recategorize analysis", "processing")
            recat_config = DREAMER_CONFIG.get('recategorize', {})
            max_recat = limit or recat_config.get('max_candidates_per_run', 10)

            rows = self.graph_service.conn.execute(
                "SELECT id, properties FROM nodes WHERE "
                "json_extract(properties, '$.quality_flags') IS NOT NULL"
            ).fetchall()

            recat_candidates = []
            for node_id, props_str in rows:
                if len(recat_candidates) >= max_recat:
                    break
                props = json.loads(props_str) if props_str else {}
                flags = props.get("quality_flags", [])
                if "wrong_type" in flags:
                    node = self.graph_service.get_node(node_id)
                    if node:
                        flag_reason = props.get("quality_flag_reason", {}).get("wrong_type", "Flagged by enrichment")
                        recat_candidates.append((node, flag_reason))

            stats["recategorize"]["candidates"] = len(recat_candidates)

            if not dry_run and recat_candidates:
                threshold = THRESHOLDS.get('recategorize', 0.90)

                for node, flag_reason in recat_candidates:
                    result = self._evaluate_recategorize(node, flag_reason)
                    if not result:
                        continue

                    decision = result.get("decision", "SKIP")
                    conf = result.get("confidence", 0)
                    new_type = result.get("new_type")

                    if decision == "RECATEGORIZE" and conf >= threshold and new_type:
                        LOGGER.info(f"RECAT: {_node_name(node)} {node.get('type')} → {new_type} (conf={conf:.2f})")
                        terminal_status("dreamer",
                                        f"RECAT: {_node_name(node)} → {new_type}", "done")
                        self._execute_recategorize(node["id"], new_type)
                        stats["recategorize"]["recategorized"] += 1
                    else:
                        stats["recategorize"]["skipped"] += 1

                    # Clear flag regardless
                    self._clear_quality_flag(node["id"], "wrong_type")
            elif dry_run and recat_candidates:
                details = []
                for node, flag_reason in recat_candidates:
                    details.append({"name": _node_name(node), "type": node.get("type", "?"), "reason": flag_reason})
                    LOGGER.info(f"  [DRY] RECAT candidate: {_node_name(node)} ({node.get('type')}) — {flag_reason}")
                stats["recategorize"]["details"] = details

        # --- Summary ---
        LOGGER.info(f"Graph Sweep complete: {json.dumps(stats, indent=2)}")
        terminal_status("dreamer", "Sweep complete", "done",
                        detail=f"merged={stats['merge']['merged']} renamed={stats['rename']['renamed']}")

        return stats
