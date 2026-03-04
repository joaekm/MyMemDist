#!/usr/bin/env python3
"""
Dreamer Daemon (OBJEKT-107)

Time-based trigger for dreamer (structural analysis) cycles.
Runs every run_interval_hours (default: 24h).

Reads config from 'dreamer.daemon'.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.utils.graph_service import GraphService
from services.utils.vector_service import vector_scope
from services.utils.shared_lock import resource_lock
from services.engines.dreamer import Dreamer

# Load config for logging
from services.utils.config_loader import get_config
try:
    _LOGGING_CONFIG = get_config()
except FileNotFoundError:
    _LOGGING_CONFIG = {}
LOG_FILE = os.path.expanduser(_LOGGING_CONFIG.get('logging', {}).get('system_log', '~/MyMemory/Logs/my_mem_system.log'))

log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

# Configure root logger: file only, no console
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - DREAMER_DAEMON - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

# Tysta tredjepartsloggers
for _name in ['httpx', 'httpcore', 'google', 'google_genai', 'anyio']:
    logging.getLogger(_name).setLevel(logging.WARNING)

LOGGER = logging.getLogger('DreamerDaemon')

# Terminal status (visual feedback)
from services.utils.terminal_status import status as terminal_status, service_status


def _load_config() -> dict:
    """Load daemon config from my_mem_config.yaml."""
    try:
        return get_config()
    except Exception as e:
        LOGGER.error(f"Failed to load config: {e}")
        return {}


def _get_daemon_config(config: dict) -> dict:
    """Extract daemon-specific config with defaults."""
    daemon_config = config.get('dreamer', {}).get('daemon', {})
    return {
        'enabled': daemon_config.get('enabled', True),
        'run_interval_hours': daemon_config.get('run_interval_hours', 24),
        'poll_interval_seconds': daemon_config.get('poll_interval_seconds', 600),
        'state_file': os.path.expanduser(
            daemon_config.get('state_file', '~/Library/Application Support/MyMemory/dreamer_state.json')
        )
    }


def _load_state(state_file: str) -> dict:
    """Load state from JSON file."""
    if not os.path.exists(state_file):
        return {
            'last_run_timestamp': None,
            'last_run_result': None
        }

    try:
        with open(state_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        LOGGER.warning(f"Could not load state file: {e}. Starting fresh.")
        return {
            'last_run_timestamp': None,
            'last_run_result': None
        }


def _save_state(state_file: str, state: dict):
    """Save state to JSON file."""
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2, default=str)
    except Exception as e:
        LOGGER.error(f"Failed to save state: {e}")


def _enrichment_has_run_since(dreamer_last_run: str, config: dict) -> bool:
    """Check if Enrichment has completed since the given Dreamer run timestamp."""
    enrichment_state_file = os.path.expanduser(
        config.get('enrichment', {}).get('daemon', {}).get(
            'state_file', '~/Library/Application Support/MyMemory/enrichment_state.json'
        )
    )
    if not os.path.exists(enrichment_state_file):
        return False

    try:
        with open(enrichment_state_file, 'r') as f:
            enrichment_state = json.load(f)
        enrich_ts = enrichment_state.get('last_run_timestamp')
        if not enrich_ts:
            return False
        return datetime.fromisoformat(enrich_ts) > datetime.fromisoformat(dreamer_last_run)
    except (json.JSONDecodeError, ValueError, OSError) as e:
        LOGGER.warning(f"Could not read enrichment state: {e}")
        return True  # On error, don't block Dreamer


def _should_run(state: dict, daemon_config: dict, config: dict = None) -> tuple[bool, str]:
    """Check if dreamer should run.

    Requires that Enrichment has run since the last Dreamer run
    (Enrichment sets quality_flags consumed by Dreamer's split/recat passes).
    """
    last_run = state.get('last_run_timestamp')
    interval_hours = daemon_config['run_interval_hours']

    if last_run:
        try:
            last_run_dt = datetime.fromisoformat(last_run)
            hours_since = (datetime.now() - last_run_dt).total_seconds() / 3600
            if hours_since >= interval_hours:
                # Check enrichment ordering
                if config and not _enrichment_has_run_since(last_run, config):
                    return False, f"Interval reached ({hours_since:.1f}h) but Enrichment has not run since last Dreamer — waiting"
                return True, f"Interval reached: {hours_since:.1f}h >= {interval_hours}h"
            return False, f"Waiting: {hours_since:.1f}h / {interval_hours}h"
        except ValueError as e:
            LOGGER.warning(f"Could not parse last_run_timestamp: {e}")
            return True, "Invalid timestamp, running"
    else:
        return True, "First run"


def _run_dreamer(config: dict) -> dict:
    """Execute dreamer sweep with phased locking (#115).

    Phase 1 — Discover + Evaluate: read-only graph, LLM calls (shared lock OK for MCP)
    Phase 2 — Execute: exclusive graph + vector (~2-5s)
    """
    graph_path = os.path.expanduser(
        config.get('paths', {}).get('graph_db', '~/MyMemory/Index/my_mem_graph.duckdb')
    )

    try:
        # --- Phase 1: Discover + Evaluate (read-only graph) ---
        LOGGER.info("Phase 1: Discover + Evaluate (read-only)...")
        graph_ro = GraphService(graph_path, read_only=True)
        dreamer = Dreamer(graph_ro, vector_service=None)

        sweep_results = dreamer.discover_and_evaluate_all()
        graph_ro.close()

        # Check if there's anything to execute
        has_decisions = any(
            len(decisions) > 0
            for _stats, decisions in sweep_results.values()
        )
        if not has_decisions:
            LOGGER.info("No decisions to execute, skipping write phase")
            # Collect stats only
            stats = {
                "deterministic": {"invalid_edges": 0, "self_aliases": 0, "uuid_aliases": 0},
            }
            for pass_name, (pass_stats, _decisions) in sweep_results.items():
                stats[pass_name] = pass_stats
            return stats

        # --- Phase 2: Execute (exclusive locks, ~2-5s) ---
        LOGGER.info("Phase 2: Executing decisions (exclusive locks)...")
        with resource_lock("graph", exclusive=True):
            with vector_scope(exclusive=True) as vector_service:
                graph_rw = GraphService(graph_path)
                dreamer.graph_service = graph_rw
                dreamer.vector_service = vector_service

                result = dreamer.execute_all(sweep_results)

                graph_rw.close()

        LOGGER.info(f"Dreamer completed: {result}")
        return result

    except Exception as e:
        LOGGER.error(f"Dreamer failed: {e}", exc_info=True)
        return {'error': str(e)}


def run_daemon():
    """Main daemon loop."""
    config = _load_config()
    daemon_config = _get_daemon_config(config)

    if not daemon_config['enabled']:
        LOGGER.info("Dreamer daemon is disabled in config. Exiting.")
        return

    LOGGER.info("=" * 60)
    LOGGER.info("Dreamer Daemon starting")
    LOGGER.info(f"  Run interval: {daemon_config['run_interval_hours']}h")
    LOGGER.info(f"  Poll interval: {daemon_config['poll_interval_seconds']}s")
    LOGGER.info(f"  State file: {daemon_config['state_file']}")
    LOGGER.info("=" * 60)

    service_status("Dreamer Daemon", "started")

    while True:
        try:
            state = _load_state(daemon_config['state_file'])
            should_run, reason = _should_run(state, daemon_config, config)

            if should_run:
                LOGGER.info(f"Triggering Dreamer: {reason}")
                terminal_status("dreamer", reason, "processing")
                result = _run_dreamer(config)

                state['last_run_timestamp'] = datetime.now().isoformat()
                state['last_run_result'] = result
                _save_state(daemon_config['state_file'], state)

                merge_stats = result.get('merge', {})
                rename_stats = result.get('rename', {})
                summary = (f"{merge_stats.get('merged', 0)} merged, "
                           f"{rename_stats.get('renamed', 0)} renamed")
                terminal_status("dreamer", "Sweep complete", "done", detail=summary)

                LOGGER.info("State updated after dreamer run")
            else:
                LOGGER.debug(reason)

        except Exception as e:
            LOGGER.error(f"Daemon error: {e}", exc_info=True)
            terminal_status("dreamer", "Sweep failed", "failed", str(e))

        time.sleep(daemon_config['poll_interval_seconds'])


def run_once():
    """Run a single check (useful for testing/manual trigger)."""
    config = _load_config()
    daemon_config = _get_daemon_config(config)

    state = _load_state(daemon_config['state_file'])
    should_run, reason = _should_run(state, daemon_config, config)

    print(f"State: {json.dumps(state, indent=2, default=str)}")
    print(f"Should run: {should_run}")
    print(f"Reason: {reason}")

    if should_run:
        print("\nRunning Dreamer...")
        result = _run_dreamer(config)

        state['last_run_timestamp'] = datetime.now().isoformat()
        state['last_run_result'] = result
        _save_state(daemon_config['state_file'], state)

        print(f"Result: {json.dumps(result, indent=2, default=str)}")

    return should_run, reason


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Dreamer Daemon - time-based trigger")
    parser.add_argument('--once', action='store_true', help="Run single check then exit")
    parser.add_argument('--status', action='store_true', help="Show current state and exit")
    args = parser.parse_args()

    if args.status:
        config = _load_config()
        daemon_config = _get_daemon_config(config)
        state = _load_state(daemon_config['state_file'])
        print(json.dumps(state, indent=2, default=str))
    elif args.once:
        run_once()
    else:
        run_daemon()
