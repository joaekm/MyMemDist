#!/usr/bin/env python3
"""
MyMemory Service Manager

Startar alla bakgrundstjänster för MyMemory-systemet.
Skriver "--- Ready ---" till terminal när redo för ingestion.

Singleton via flock:
- Vid start: tar exklusivt flock på ~/Library/Application Support/MyMemory/mymemory.lock
- Om låset redan är taget → en annan instans körs → avbryt med exit(1)
- OS:et släpper låset automatiskt vid krasch, kill -9, eller normal exit
- Ingen stale-state möjlig (till skillnad från PID-filer)

Process Group Strategy:
- Huvudprocessen blir process group leader (os.setpgrp)
- Alla subprocesser ärver process group
- Vid shutdown: os.killpg() dödar ALLA processer i gruppen
"""

import subprocess
import sys
import time
import os
import signal
import fcntl
import yaml
import logging

# Bli process group leader - alla barn ärver denna PGID
os.setpgrp()

# Config
def _load_config():
    # Prioritera portabel config (~/MyMemory/Settings/) före script-relativ
    user_config = os.path.expanduser('~/MyMemory/Settings/my_mem_config.yaml')
    script_config = os.path.join(os.path.dirname(__file__), 'config', 'my_mem_config.yaml')

    for config_path in [user_config, script_config]:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
    return {}

CONFIG = _load_config()

# Loggning till fil
LOG_FILE = os.path.expanduser(CONFIG.get('logging', {}).get('system_log', '~/MyMemory/Logs/system.log'))
log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - SERVICES - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger('StartServices')

# --- Singleton: flock hindrar duplicerade instanser ---
# OS:et släpper låset automatiskt när processen dör (oavsett orsak: krasch, kill -9, etc.)
LOCK_DIR = os.path.expanduser('~/Library/Application Support/MyMemory')
os.makedirs(LOCK_DIR, exist_ok=True)
LOCK_FILE_PATH = os.path.join(LOCK_DIR, 'mymemory.lock')

_lock_fd = open(LOCK_FILE_PATH, 'w')
try:
    fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    _lock_fd.write(str(os.getpid()))
    _lock_fd.flush()
except BlockingIOError:
    # En annan instans körs redan — avbryt omedelbart
    LOGGER.warning("Refused to start: another instance holds the lock")
    print("MyMemory services already running (lock held). Exiting.", file=sys.stderr)
    sys.exit(1)

# Terminal status
from services.utils.terminal_status import header, ready_message, service_status

# Import validering
from services.utils.validate_system import run_startup_checks

# Tjänsterna som ska startas (som moduler)
SERVICES = [
    {"module": "services.collectors.file_collector", "name": "File Retriever"},
    {"module": "services.collectors.slack_collector", "name": "Slack Collector"},
    {"module": "services.collectors.collector_daemon", "name": "Collector Daemon"},
    {"module": "services.engines.ingestion_engine", "name": "Ingestion Engine"},
    {"module": "services.processors.transcriber", "name": "Transcriber"},
    {"module": "services.collectors.rode_collector", "name": "Røde Collector"},
    {"module": "services.engines.dreamer_daemon", "name": "Dreamer Daemon"},
]

processes = []


def auto_repair(health_info):
    """Reparerar saknade filer i Vector."""
    if not health_info:
        return

    lake_count = health_info['lake_count']
    vector_count = health_info['vector_count']
    lake_store = health_info['lake_store']
    lake_ids_dict = health_info.get('lake_ids', {})

    if vector_count >= lake_count:
        return

    try:
        from services.utils.vector_service import vector_scope

        lake_id_set = set(lake_ids_dict.keys())

        with vector_scope(exclusive=True, timeout=30.0) as vector_service:
            coll = vector_service.collection
            vector_ids = set(coll.get()['ids'])

            missing = lake_id_set - vector_ids
            if not missing:
                return

            LOGGER.info(f"Repairing {len(missing)} missing Vector entries")

            for uid in missing:
                filename = lake_ids_dict.get(uid, f"{uid}.md")
                filepath = os.path.join(lake_store, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read()

                    if not content.startswith("---"):
                        continue
                    parts = content.split("---", 2)
                    if len(parts) < 3:
                        continue

                    metadata = yaml.safe_load(parts[1])
                    text = parts[2].strip()

                    ai_summary = metadata.get('ai_summary') or ""
                    timestamp = metadata.get('timestamp_ingestion') or ""

                    full_doc = f"FILENAME: {filename}\nSUMMARY: {ai_summary}\n\nCONTENT:\n{text[:8000]}"

                    coll.upsert(
                        ids=[uid],
                        documents=[full_doc],
                        metadatas=[{"timestamp": timestamp, "filename": filename}]
                    )
                except (OSError, ValueError) as e:  # File read/parse errors
                    LOGGER.warning(f"Could not index {filename}: {e}")
                    continue

            LOGGER.info("Vector repair complete")

    except TimeoutError:
        LOGGER.warning("auto_repair: could not acquire vector lock in 30s")
    except (OSError, RuntimeError) as e:  # Vector service errors
        LOGGER.error(f"Vector repair failed (non-critical): {e}")


def clean_old_meeting_chunks():
    """Rensa chunks äldre än 24h från MeetingBuffer."""
    meeting_config = CONFIG.get('meeting_transcriber', {})
    buffer_dir = os.path.expanduser(meeting_config.get('buffer_dir', '~/MyMemory/MeetingBuffer'))
    chunks_dir = os.path.join(buffer_dir, 'chunks')

    if not os.path.exists(chunks_dir):
        return

    now = time.time()
    max_age_seconds = 24 * 60 * 60  # 24 hours
    cleaned = 0

    for filename in os.listdir(chunks_dir):
        if filename.endswith('.txt'):
            filepath = os.path.join(chunks_dir, filename)
            try:
                file_age = now - os.path.getmtime(filepath)
                if file_age > max_age_seconds:
                    os.remove(filepath)
                    cleaned += 1
            except OSError:
                pass

    if cleaned > 0:
        LOGGER.info(f"Cleaned {cleaned} old meeting chunks (>24h)")


def start_all():
    """Starta alla tjänster."""
    header("MyMemory Services")

    # Rensa gamla meeting-chunks
    clean_old_meeting_chunks()

    # Validering och auto-repair
    health_info = run_startup_checks()
    auto_repair(health_info)

    python_exec = sys.executable

    for service in SERVICES:
        module_name = service["module"]
        try:
            p = subprocess.Popen(
                [python_exec, "-m", module_name]
            )
            processes.append({"process": p, "name": service["name"]})
            LOGGER.info(f"Started {service['name']}")
            time.sleep(0.5)
        except (OSError, subprocess.SubprocessError) as e:
            LOGGER.error(f"Failed to start {service['name']}: {e}")
            service_status(service["name"], "failed")
            continue

    # Vänta lite på att tjänsterna startar
    time.sleep(1)

    # Visa status för varje tjänst
    for svc in processes:
        if svc["process"].poll() is None:
            service_status(svc["name"], "started")
        else:
            service_status(svc["name"], "failed")

    ready_message()


# Flagga för att förhindra rekursiv signalhantering
# (os.killpg skickar signal till sig själv - se poc/process/killpg_poc.py)
_shutting_down = False


def stop_all(signum, frame):
    """Stoppa alla tjänster via process group."""
    global _shutting_down
    if _shutting_down:
        return  # Redan på väg att stänga ner, ignorera
    _shutting_down = True

    LOGGER.info("Shutting down services (killing process group)")

    # Skicka SIGTERM till hela process-gruppen (inkl. sub-subprocesser)
    try:
        os.killpg(os.getpgid(os.getpid()), signal.SIGTERM)
    except ProcessLookupError:
        pass

    # Vänta kort på graceful shutdown
    time.sleep(1)

    # Verifiera att alla barn är döda, annars force kill
    for svc in processes:
        try:
            if svc["process"].poll() is None:
                LOGGER.warning(f"{svc['name']} did not exit, killing")
                svc["process"].kill()
        except ProcessLookupError:
            pass

    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_all)
    signal.signal(signal.SIGTERM, stop_all)
    start_all()

    # Håll igång - övervaka processer
    try:
        while True:
            time.sleep(5)
            # Kolla om någon process dött
            for svc in processes:
                if svc["process"].poll() is not None:
                    LOGGER.warning(f"{svc['name']} exited unexpectedly")
    except KeyboardInterrupt:
        stop_all(None, None)
