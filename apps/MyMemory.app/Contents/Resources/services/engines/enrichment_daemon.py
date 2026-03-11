#!/usr/bin/env python3
"""
Enrichment Daemon (OBJEKT-76, refactored OBJEKT-107)

Threshold-based trigger for enrichment cycles.
Polls a state file and triggers enrichment when:
1. node_threshold new graph nodes have been added, OR
2. max_hours_between_runs has passed since last run

Reads config from 'enrichment.daemon'.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import yaml

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.utils.graph_service import graph_scope
from services.utils.vector_service import vector_scope
from services.engines.enrichment import Enrichment

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
file_handler.setFormatter(logging.Formatter('%(asctime)s - ENRICHMENT_DAEMON - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

# Tysta tredjepartsloggers
for _name in ['httpx', 'httpcore', 'google', 'google_genai', 'anyio']:
    logging.getLogger(_name).setLevel(logging.WARNING)

LOGGER = logging.getLogger('EnrichmentDaemon')

# Terminal status (visual feedback)
from services.utils.terminal_status import status as terminal_status, service_status


def _get_dreamer_state_file(config: dict) -> str:
    """Resolve Dreamer state file path from config."""
    return os.path.expanduser(
        config.get('dreamer', {}).get('daemon', {}).get(
            'state_file', '~/Library/Application Support/MyMemory/dreamer_state.json'
        )
    )


def _increment_dreamer_counter(config: dict, enriched: int, flagged: int):
    """Signal to Dreamer that Enrichment produced work (#127).

    Increments Dreamer's nodes_since_last_run counter so Dreamer
    triggers based on actual need, not time intervals.
    """
    signal_count = enriched + flagged
    if signal_count <= 0:
        return

    dreamer_state_file = _get_dreamer_state_file(config)
    try:
        state = {'nodes_since_last_run': 0, 'last_run_timestamp': None, 'last_run_result': None}
        if os.path.exists(dreamer_state_file):
            with open(dreamer_state_file, 'r') as f:
                state = json.load(f)

        state['nodes_since_last_run'] = state.get('nodes_since_last_run', 0) + signal_count

        os.makedirs(os.path.dirname(dreamer_state_file), exist_ok=True)
        with open(dreamer_state_file, 'w') as f:
            json.dump(state, f, indent=2, default=str)

        LOGGER.info(f"Dreamer counter: +{signal_count} -> {state['nodes_since_last_run']} total")

    except (OSError, json.JSONDecodeError) as e:
        LOGGER.error(f"HARDFAIL: Could not update Dreamer state: {e}")
        raise RuntimeError(f"Failed to update Dreamer counter: {e}") from e


def _calculate_graph_health(config: dict) -> dict | None:
    """Beräkna grafhälsa (#135). Returnerar health-dict eller None vid fel."""
    try:
        from services.utils.graph_service import graph_scope, calculate_graph_health

        graph_path = os.path.expanduser(
            config.get('paths', {}).get('graph_db', '~/MyMemory/Index/my_mem_graph.duckdb')
        )

        with graph_scope(exclusive=False, db_path=graph_path) as graph:
            maint = graph.get_maintenance_stats()

        dreamer_counter = 0
        last_sweep_had_decisions = False
        dreamer_state_file = _get_dreamer_state_file(config)
        if os.path.exists(dreamer_state_file):
            with open(dreamer_state_file, 'r') as f:
                ds = json.load(f)
            dreamer_counter = ds.get('nodes_since_last_run', 0)
            lr = ds.get('last_run_result') or {}
            last_sweep_had_decisions = any(
                v.get('merged', 0) > 0 or v.get('renamed', 0) > 0
                for v in [lr.get('merge', {}), lr.get('rename', {})]
            )

        return calculate_graph_health(maint, dreamer_counter, last_sweep_had_decisions)

    except Exception as e:
        LOGGER.warning(f"Could not calculate graph health: {e}")
        return None


def _write_graph_health(config: dict):
    """Beräkna och skriv grafhälsa till state-fil (#135).

    Kallas efter sweep. Menubar-appen läser filen för statuslampan.
    """
    health = _calculate_graph_health(config)
    if not health:
        return

    try:
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
    """Extract daemon-specific config with defaults.

    """
    daemon_config = config.get('enrichment', {}).get('daemon', {})
    return {
        'enabled': daemon_config.get('enabled', True),
        'node_threshold': daemon_config.get('node_threshold', 50),
        'max_hours_between_runs': daemon_config.get('max_hours_between_runs', 12),
        'poll_interval_seconds': daemon_config.get('poll_interval_seconds', 300),
        'state_file': os.path.expanduser(
            daemon_config.get('state_file', '~/Library/Application Support/MyMemory/enrichment_state.json')
        )
    }


def _load_state(state_file: str) -> dict:
    """Load state from JSON file."""
    if not os.path.exists(state_file):
        return {
            'nodes_since_last_run': 0,
            'last_run_timestamp': None,
            'last_run_result': None
        }

    try:
        with open(state_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        LOGGER.warning(f"Could not load state file: {e}. Starting fresh.")
        return {
            'nodes_since_last_run': 0,
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


def _get_sleep_need(config: dict) -> float:
    """Hämta sömnbehov via _calculate_graph_health. Returnerar -1 vid fel."""
    health = _calculate_graph_health(config)
    if health:
        return health.get('score', -1)
    return -1


def _should_run(state: dict, daemon_config: dict, config: dict = None) -> tuple[bool, str]:
    """
    Check if Enrichment should run.

    Triggers:
    1. Node threshold (nya dokument via ingestion)
    2. Sömnbehov >= 0.20 (grafens backlog, #135)
    3. Tidsfallback (max_hours_between_runs)

    Returns:
        (should_run: bool, reason: str)
    """
    nodes_count = state.get('nodes_since_last_run', 0)
    last_run = state.get('last_run_timestamp')

    threshold = daemon_config['node_threshold']
    max_hours = daemon_config['max_hours_between_runs']

    # 1. Node threshold (nya dokument)
    if nodes_count >= threshold:
        return True, f"Node threshold reached: {nodes_count} >= {threshold}"

    # 2. Sömnbehov — grafens backlog driver Enrichment (#135)
    if config:
        sleep_threshold = config['enrichment']['daemon']['sleep_need_threshold']
        sleep_need = _get_sleep_need(config)
        if sleep_need >= sleep_threshold:
            return True, f"Sleep need: {sleep_need:.2f} >= {sleep_threshold}"

    # 3. Tidsfallback
    if last_run:
        try:
            last_run_dt = datetime.fromisoformat(last_run)
            hours_since = (datetime.now() - last_run_dt).total_seconds() / 3600
            if hours_since >= max_hours:
                return True, f"Time fallback: {hours_since:.1f}h >= {max_hours}h"
        except ValueError as e:
            LOGGER.warning(f"Could not parse last_run_timestamp: {e}")
    else:
        # Never run before - run if we have any nodes
        if nodes_count > 0:
            return True, f"First run with {nodes_count} pending nodes"

    return False, f"No trigger: {nodes_count}/{threshold} nodes, waiting"


def _run_enrichment(config: dict) -> dict:
    """
    Execute Enrichment cycle with phased locking (#115).

    Phase 1 — Prepare: read-only graph + shared vector (candidates + prompt)
    Phase 2 — LLM call: no locks
    Phase 3 — Write: exclusive graph + vector (~1-3s)

    Returns:
        Result dict from Enrichment
    """
    graph_path = os.path.expanduser(
        config.get('paths', {}).get('graph_db', '~/MyMemory/Index/my_mem_graph.duckdb')
    )

    try:
        # --- Phase 1: Prepare (read-only graph, shared lock) ---
        LOGGER.info("Phase 1: Preparing enrichment (shared lock)...")
        with graph_scope(exclusive=False, db_path=graph_path) as graph_ro:
            enrichment = Enrichment(graph_ro, vector_service=None)
            prepare_result = enrichment.prepare_cycle()

        if prepare_result.get("status") != "ready":
            LOGGER.info(f"Enrichment prepare: {prepare_result.get('status')}")
            return prepare_result

        prompt = prepare_result["prompt"]
        prompt_stats = prepare_result["prompt_stats"]

        # --- Phase 2: LLM call (no locks) ---
        LOGGER.info("Phase 2: LLM call (no locks)...")
        llm_result = enrichment.call_enrich_llm(prompt, prompt_stats)

        if llm_result.get("status") != "ready":
            LOGGER.info(f"Enrichment LLM: {llm_result.get('status')}")
            return llm_result

        enrich_result = llm_result["enrich_result"]
        total_usage = llm_result["usage"]

        # --- Phase 3: Write (exclusive locks, ~1-3s) ---
        LOGGER.info("Phase 3: Writing enrichments (exclusive locks)...")
        with graph_scope(exclusive=True, db_path=graph_path) as graph_rw:
            with vector_scope(exclusive=True) as vector_service:
                enrichment.graph_service = graph_rw
                enrichment.vector_service = vector_service
                enrich_stats = enrichment.write_cycle_results(enrich_result)

        result = {
            "status": "completed",
            "enrich": enrich_stats,
            "prompt_stats": prompt_stats,
            "usage": total_usage,
        }
        LOGGER.info(f"Enrichment completed: {result}")
        return result

    except Exception as e:
        LOGGER.error(f"Enrichment failed: {e}", exc_info=True)
        return {'error': str(e)}


def run_daemon():
    """Main daemon loop."""
    config = _load_config()
    daemon_config = _get_daemon_config(config)

    if not daemon_config['enabled']:
        LOGGER.info("Enrichment daemon is disabled in config. Exiting.")
        return

    LOGGER.info("=" * 60)
    LOGGER.info("Enrichment Daemon starting")
    LOGGER.info(f"  Node threshold: {daemon_config['node_threshold']}")
    LOGGER.info(f"  Max hours between runs: {daemon_config['max_hours_between_runs']}")
    LOGGER.info(f"  Poll interval: {daemon_config['poll_interval_seconds']}s")
    LOGGER.info(f"  State file: {daemon_config['state_file']}")
    LOGGER.info("=" * 60)

    # Terminal: visa att daemon startade
    service_status("Enrichment Daemon", "started")

    while True:
        try:
            state = _load_state(daemon_config['state_file'])
            should_run, reason = _should_run(state, daemon_config, config)

            if should_run:
                LOGGER.info(f"Triggering Enrichment: {reason}")
                terminal_status("enrichment", reason, "processing")
                result = _run_enrichment(config)

                # Update state after successful run
                state['nodes_since_last_run'] = 0
                state['last_run_timestamp'] = datetime.now().isoformat()
                state['last_run_result'] = result
                _save_state(daemon_config['state_file'], state)

                # Signal to Dreamer that work was produced (#127)
                enrich = result.get('enrich', {})
                _increment_dreamer_counter(
                    config,
                    enriched=enrich.get('nodes_updated', 0),
                    flagged=enrich.get('quality_flags', 0)
                )

                # Uppdatera grafhälsa (#135)
                _write_graph_health(config)

                # Terminal: visa resultat
                summary = (f"{enrich.get('nodes_updated', 0)} enriched, "
                           f"{enrich.get('quality_flags', 0)} flagged")
                terminal_status("enrichment", "Enrichment cycle", "done", detail=summary)

                LOGGER.info("State reset after Enrichment run")
            else:
                LOGGER.debug(reason)

        except Exception as e:
            LOGGER.error(f"Daemon error: {e}", exc_info=True)
            terminal_status("enrichment", "Resolution cycle", "failed", str(e))

        # Wait for next poll
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
        print("\nRunning Enrichment...")
        result = _run_enrichment(config)

        state['nodes_since_last_run'] = 0
        state['last_run_timestamp'] = datetime.now().isoformat()
        state['last_run_result'] = result
        _save_state(daemon_config['state_file'], state)

        print(f"Result: {result}")

    return should_run, reason


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Enrichment Daemon - threshold-based trigger")
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
