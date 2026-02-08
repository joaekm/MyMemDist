#!/usr/bin/env python3
"""
Collector Daemon

Kör periodiska collectors (Gmail, Calendar) i en loop.
Pollar med konfigurerbart intervall.

Collectors som körs:
- Gmail Collector: Hämtar mail med specifik label
- Calendar Collector: Hämtar kalenderhändelser
"""

import logging
import os
import signal
import sys
import time
from pathlib import Path

import yaml

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# --- SIGTERM handler för graceful shutdown ---
_shutdown_requested = False

def _handle_sigterm(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    os._exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# --- CONFIG ---
from services.utils.config_loader import get_config
try:
    CONFIG = get_config()
except FileNotFoundError:
    CONFIG = {}

# --- LOGGING ---
LOG_FILE = os.path.expanduser(CONFIG.get('logging', {}).get('system_log', '~/MyMemory/Logs/system.log'))
log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - COLLECTOR_DAEMON - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

# Tysta tredjepartsloggers
for _name in ['httpx', 'httpcore', 'google', 'googleapiclient', 'oauth2client', 'urllib3']:
    logging.getLogger(_name).setLevel(logging.WARNING)

LOGGER = logging.getLogger('CollectorDaemon')

# Terminal status
from services.utils.terminal_status import service_status, status as terminal_status

# --- DAEMON CONFIG ---
def _get_daemon_config() -> dict:
    """Hämta daemon-config med defaults."""
    daemon_config = CONFIG.get('collectors', {}).get('daemon', {})
    return {
        'enabled': daemon_config.get('enabled', True),
        'poll_interval_seconds': daemon_config.get('poll_interval_seconds', 300),  # 5 min default
    }


def run_daemon():
    """Main daemon loop."""
    daemon_config = _get_daemon_config()

    if not daemon_config['enabled']:
        LOGGER.info("Collector daemon is disabled in config. Exiting.")
        return

    poll_interval = daemon_config['poll_interval_seconds']

    LOGGER.info("=" * 60)
    LOGGER.info("Collector Daemon starting")
    LOGGER.info(f"  Poll interval: {poll_interval}s")
    LOGGER.info("=" * 60)

    service_status("Collector Daemon", "started")

    # Kör första gången direkt - med heartbeat
    _run_collectors(show_heartbeat=True)

    while not _shutdown_requested:
        try:
            # Vänta på nästa poll
            time.sleep(poll_interval)

            if _shutdown_requested:
                break

            _run_collectors()

        except Exception as e:
            LOGGER.error(f"Daemon error: {e}", exc_info=True)


def _run_collectors(show_heartbeat: bool = False):
    """Kör alla collectors.

    Args:
        show_heartbeat: Visa totaler vid uppstart (första körningen)
    """
    # Gmail
    try:
        from services.collectors.gmail_collector import run_collector as run_gmail, get_existing_message_ids
        run_gmail()
        if show_heartbeat:
            total_mail = len(get_existing_message_ids())
            terminal_status("gmail", "Gmail", "done", detail=f"{total_mail} mail")
    except Exception as e:
        LOGGER.error(f"Gmail collector failed: {e}")

    # Calendar
    try:
        from services.collectors.calendar_collector import run_collector as run_calendar, CALENDAR_FOLDER
        run_calendar()
        if show_heartbeat:
            import os
            total_events = len([f for f in os.listdir(CALENDAR_FOLDER) if f.endswith('.md')])
            terminal_status("calendar", "Calendar", "done", detail=f"{total_events} events")
    except Exception as e:
        LOGGER.error(f"Calendar collector failed: {e}")


if __name__ == "__main__":
    run_daemon()
