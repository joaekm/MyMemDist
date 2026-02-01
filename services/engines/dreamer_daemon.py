#!/usr/bin/env python3
"""
Dreamer Daemon (OBJEKT-76)

Threshold-based trigger for Dreamer entity resolution.
Polls a state file and triggers Dreamer when:
1. node_threshold new graph nodes have been added, OR
2. max_hours_between_runs has passed since last run

Designed for launchd on macOS, preparing for future menubar app.
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

from services.utils.graph_service import GraphService
from services.utils.vector_service import VectorService
from services.utils.shared_lock import resource_lock
from services.engines.dreamer import Dreamer

# Load config for logging
def _load_logging_config():
    config_path = PROJECT_ROOT / "config" / "my_mem_config.yaml"
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

_LOGGING_CONFIG = _load_logging_config()
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
    config_path = PROJECT_ROOT / "config" / "my_mem_config.yaml"
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        LOGGER.error(f"Failed to load config: {e}")
        return {}


def _get_daemon_config(config: dict) -> dict:
    """Extract daemon-specific config with defaults."""
    daemon_config = config.get('dreamer', {}).get('daemon', {})
    return {
        'enabled': daemon_config.get('enabled', True),
        'node_threshold': daemon_config.get('node_threshold', 15),
        'max_hours_between_runs': daemon_config.get('max_hours_between_runs', 24),
        'poll_interval_seconds': daemon_config.get('poll_interval_seconds', 300),
        'state_file': os.path.expanduser(
            daemon_config.get('state_file', '~/MyMemory/Index/.dreamer_state.json')
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


def _should_run(state: dict, daemon_config: dict) -> tuple[bool, str]:
    """
    Check if Dreamer should run.

    Returns:
        (should_run: bool, reason: str)
    """
    nodes_count = state.get('nodes_since_last_run', 0)
    last_run = state.get('last_run_timestamp')

    threshold = daemon_config['node_threshold']
    max_hours = daemon_config['max_hours_between_runs']

    # Check node threshold
    if nodes_count >= threshold:
        return True, f"Node threshold reached: {nodes_count} >= {threshold}"

    # Check time-based fallback
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


def _run_dreamer(config: dict) -> dict:
    """
    Execute Dreamer resolution cycle with resource locking.

    Takes exclusive locks on graph and vector to prevent conflicts
    with concurrent ingestion processes.

    Returns:
        Result dict from Dreamer
    """
    LOGGER.info("Acquiring locks for Dreamer cycle...")

    try:
        # Take exclusive locks for entire cycle (OBJEKT-73)
        with resource_lock("graph", exclusive=True):
            with resource_lock("vector", exclusive=True):
                LOGGER.info("Locks acquired, initializing Dreamer...")

                graph_path = os.path.expanduser(
                    config.get('paths', {}).get('graph_db', '~/MyMemory/Index/my_mem_graph.duckdb')
                )
                graph_service = GraphService(graph_path)
                vector_service = VectorService()
                dreamer = Dreamer(graph_service, vector_service)

                LOGGER.info("Running resolution cycle...")
                result = dreamer.run_resolution_cycle(dry_run=False)

                graph_service.close()
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
    LOGGER.info(f"  Max hours between runs: {daemon_config['max_hours_between_runs']}")
    LOGGER.info(f"  Poll interval: {daemon_config['poll_interval_seconds']}s")
    LOGGER.info(f"  State file: {daemon_config['state_file']}")
    LOGGER.info("=" * 60)

    # Terminal: visa att daemon startade
    service_status("Dreamer Daemon", "started")

    while True:
        try:
            state = _load_state(daemon_config['state_file'])
            should_run, reason = _should_run(state, daemon_config)

            if should_run:
                LOGGER.info(f"Triggering Dreamer: {reason}")
                terminal_status("dreamer", reason, "processing")
                result = _run_dreamer(config)

                # Update state after successful run
                state['nodes_since_last_run'] = 0
                state['last_run_timestamp'] = datetime.now().isoformat()
                state['last_run_result'] = result
                _save_state(daemon_config['state_file'], state)

                # Terminal: visa resultat
                summary = f"{result.get('renamed', 0)} renamed, {result.get('merged', 0)} merged, {result.get('recat', 0)} recat"
                terminal_status("dreamer", "Resolution cycle", "done", detail=summary)

                LOGGER.info("State reset after Dreamer run")
            else:
                LOGGER.debug(reason)

        except Exception as e:
            LOGGER.error(f"Daemon error: {e}", exc_info=True)
            terminal_status("dreamer", "Resolution cycle", "failed", str(e))

        # Wait for next poll
        time.sleep(daemon_config['poll_interval_seconds'])


def run_once():
    """Run a single check (useful for testing/manual trigger)."""
    config = _load_config()
    daemon_config = _get_daemon_config(config)

    state = _load_state(daemon_config['state_file'])
    should_run, reason = _should_run(state, daemon_config)

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

        print(f"Result: {result}")

    return should_run, reason


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Dreamer Daemon - threshold-based trigger")
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
