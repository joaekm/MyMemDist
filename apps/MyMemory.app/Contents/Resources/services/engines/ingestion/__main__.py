#!/usr/bin/env python3
"""
Ingestion Engine daemon entry point.

Usage:
    python -m services.engines.ingestion

Watches Asset folders for new files and processes them through
the ingestion pipeline.
"""

# Prevent tokenizers fork crash on macOS (must be before any imports)
import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"

import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# --- SIGTERM handler för graceful shutdown ---
# OBS: os._exit() istället för sys.exit() - undviker ThreadPoolExecutor cleanup-problem
# Se poc/process/signal_logging_poc.py (Test 5)
_shutdown_requested = False


def _handle_sigterm(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    os._exit(0)


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from services.engines.ingestion._shared import CONFIG, LAKE_STORE, UUID_SUFFIX_PATTERN
from services.engines.ingestion.orchestrator import process_document
from services.utils.terminal_status import service_status

os.makedirs(LAKE_STORE, exist_ok=True)
service_status("Ingestion Engine", "started")

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from services.engines.ingestion._shared import PROCESSED_FILES, PROCESS_LOCK


class WatchdogHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        process_document(event.src_path, os.path.basename(event.src_path))

    def on_modified(self, event):
        if event.is_directory:
            return
        fname = os.path.basename(event.src_path)
        if not UUID_SUFFIX_PATTERN.search(fname):
            return
        # Allow re-processing: clear from PROCESSED_FILES
        # so process_document can re-evaluate via _needs_reingest()
        with PROCESS_LOCK:
            PROCESSED_FILES.discard(fname)
        process_document(event.src_path, fname)


folders = [
    CONFIG['paths']['asset_documents'],
    CONFIG['paths']['asset_slack'],
    CONFIG.get('paths', {}).get('asset_mail'),
    CONFIG.get('paths', {}).get('asset_calendar'),
    CONFIG['paths']['asset_transcripts'],
    CONFIG.get('paths', {}).get('asset_ai_generated'),
]

with ThreadPoolExecutor(max_workers=5) as executor:
    for folder in folders:
        if folder and os.path.exists(folder):
            for f in os.listdir(folder):
                if UUID_SUFFIX_PATTERN.search(f):
                    executor.submit(process_document, os.path.join(folder, f), f)

observer = Observer()
for folder in folders:
    if folder and os.path.exists(folder):
        observer.schedule(WatchdogHandler(), folder, recursive=False)
observer.start()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
