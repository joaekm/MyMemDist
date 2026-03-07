#!/usr/bin/env python3
"""
Dreamer Daemon (OBJEKT-107, #127)

Need-based trigger for dreamer (structural analysis) cycles.
Triggers when Enrichment has signalled work via the dreamer state counter.
No time-based fallback — runs only when there is actual need.

Signal chain: Ingestion -> Enrichment -> Dreamer (via state file counter).

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

from services.utils.graph_service import graph_scope
from services.utils.vector_service import vector_scope
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


def _write_graph_health(config: dict):
    """Beräkna och skriv grafhälsa till state-fil (#135).

    Kallas efter sweep. Menubar-appen läser filen för statuslampan.
    """
    try:
        from services.utils.graph_service import calculate_graph_health

        graph_path = os.path.expanduser(
            config.get('paths', {}).get('graph_db', '~/MyMemory/Index/my_mem_graph.duckdb')
        )

        with graph_scope(exclusive=False, db_path=graph_path) as graph:
            maint = graph.get_maintenance_stats()

        # Dreamer har just kört — counter är redan nollställd
        # Läs state för last_run_result
        dreamer_state_file = os.path.expanduser(
            config.get('dreamer', {}).get('daemon', {}).get(
                'state_file', '~/Library/Application Support/MyMemory/dreamer_state.json'
            )
        )
        dreamer_counter = 0
        last_sweep_had_decisions = False
        if os.path.exists(dreamer_state_file):
            with open(dreamer_state_file, 'r') as f:
                ds = json.load(f)
            dreamer_counter = ds.get('nodes_since_last_run', 0)
            lr = ds.get('last_run_result') or {}
            last_sweep_had_decisions = any(
                v.get('merged', 0) > 0 or v.get('renamed', 0) > 0
                for v in [lr.get('merge', {}), lr.get('rename', {})]
            )

        health = calculate_graph_health(maint, dreamer_counter, last_sweep_had_decisions)

        health_file = os.path.expanduser(
            config.get('graph_health', {}).get(
                'state_file', '~/Library/Application Support/MyMemory/graph_health.json'
            )
        )
        os.makedirs(os.path.dirname(health_file), exist_ok=True)
        health['timestamp'] = datetime.now().isoformat()
        with open(health_file, 'w') as f:
            json.dump(health, f, indent=2, default=str)

        LOGGER.info(f"Graph health: {health['score']:.2f} ({health['level']})")

    except Exception as e:
        LOGGER.warning(f"Could not write graph health: {e}")


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
        'node_threshold': daemon_config.get('node_threshold', 15),
        'poll_interval_seconds': daemon_config.get('poll_interval_seconds', 300),
        'state_file': os.path.expanduser(
            daemon_config.get('state_file', '~/Library/Application Support/MyMemory/dreamer_state.json')
        )
    }


def _load_state(state_file: str) -> dict:
    """Load state from JSON file."""
    default = {
        'nodes_since_last_run': 0,
        'last_run_timestamp': None,
        'last_run_result': None
    }
    if not os.path.exists(state_file):
        return default

    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
        # Backfill nodes_since_last_run for pre-#127 state files
        if 'nodes_since_last_run' not in state:
            state['nodes_since_last_run'] = 0
        return state
    except Exception as e:
        LOGGER.warning(f"Could not load state file: {e}. Starting fresh.")
        return default


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
    """Check if dreamer should run (#127: need-based, no time fallback).

    Triggers when Enrichment has signalled work via the state counter.
    Enrichment must have run since the last Dreamer run (ordering guard).
    """
    nodes_count = state.get('nodes_since_last_run', 0)
    last_run = state.get('last_run_timestamp')
    threshold = daemon_config['node_threshold']

    # Need-based: Enrichment signalled enough work
    if nodes_count >= threshold:
        # Verify enrichment ordering
        if last_run and config and not _enrichment_has_run_since(last_run, config):
            return False, f"Threshold reached ({nodes_count} >= {threshold}) but Enrichment has not run since last Dreamer — waiting"
        return True, f"Node threshold reached: {nodes_count} >= {threshold}"

    # First run with pending work
    if not last_run and nodes_count > 0:
        return True, f"First run with {nodes_count} pending signals"

    return False, f"No trigger: {nodes_count}/{threshold} signals, waiting"


def _run_dreamer(config: dict) -> dict:
    """Execute dreamer sweep with phased locking (#115).

    Phase 1 — Discover + Evaluate: read-only graph, LLM calls (shared lock OK for MCP)
    Phase 2 — Execute: exclusive graph + vector (~2-5s)
    """
    graph_path = os.path.expanduser(
        config.get('paths', {}).get('graph_db', '~/MyMemory/Index/my_mem_graph.duckdb')
    )

    try:
        # --- Phase 1: Discover + Evaluate (shared lock) ---
        LOGGER.info("Phase 1: Discover + Evaluate (shared lock)...")
        with graph_scope(exclusive=False, db_path=graph_path) as graph_ro:
            dreamer = Dreamer(graph_ro, vector_service=None)
            sweep_results = dreamer.discover_and_evaluate_all()

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
        with graph_scope(exclusive=True, db_path=graph_path) as graph_rw:
            with vector_scope(exclusive=True) as vector_service:
                dreamer.graph_service = graph_rw
                dreamer.vector_service = vector_service
                result = dreamer.execute_all(sweep_results)

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
    LOGGER.info(f"  Node threshold: {daemon_config['node_threshold']}")
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

                state['nodes_since_last_run'] = 0
                state['last_run_timestamp'] = datetime.now().isoformat()
                state['last_run_result'] = result
                _save_state(daemon_config['state_file'], state)

                merge_stats = result.get('merge', {})
                rename_stats = result.get('rename', {})
                summary = (f"{merge_stats.get('merged', 0)} merged, "
                           f"{rename_stats.get('renamed', 0)} renamed")
                terminal_status("dreamer", "Sweep complete", "done", detail=summary)

                # Uppdatera grafhälsa (#135)
                _write_graph_health(config)

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

        state['nodes_since_last_run'] = 0
        state['last_run_timestamp'] = datetime.now().isoformat()
        state['last_run_result'] = result
        _save_state(daemon_config['state_file'], state)

        print(f"Result: {json.dumps(result, indent=2, default=str)}")

    return should_run, reason


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Dreamer Daemon - need-based trigger (#127)")
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
