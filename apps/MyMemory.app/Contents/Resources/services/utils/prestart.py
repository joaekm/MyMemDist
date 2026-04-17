"""
Prestart — one-shot uppstarts-check som menubar-appen kör innan supervisors
spawnas (#220 etapp 3).

Ersätter start_services.py:s init-fas. Inga processer startas härifrån —
bara miljö-kontroller och underhåll som tidigare låg i start_services.py:

1. Skapa runtime-mappar om saknas
2. Validera FileVault-status
3. Rensa gamla MeetingBuffer-chunks (>24h)
4. Kör systemets statuskontroller (Lake, Vector counts — mest relevant
   i legacy lokal-mode; cloud-mode returnerar 0 counts men felar inte)

Exitar nollkodat när klar. Menubar-appens ProcessSupervisor-system spawnar
bakgrundstjänsterna parallellt.

Körs via:
    python -m services.utils.prestart
"""

import logging
import os
import sys
import time
from pathlib import Path

# Path setup
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from services.utils.config_loader import get_config
from services.utils.validate_system import run_startup_checks

# Initiera logger innan vi läser config så except-blocket kan logga.
LOGGER = logging.getLogger('Prestart')

try:
    CONFIG = get_config()
except FileNotFoundError as e:
    LOGGER.warning(f"Config saknas ({e}), kör prestart med defaults")
    CONFIG = {}

LOG_FILE = os.path.expanduser(
    CONFIG.get('logging', {}).get('system_log', '~/MyMemory/Logs/system.log')
)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - PRESTART - %(levelname)s - %(message)s',
)


def clean_old_meeting_chunks() -> int:
    """Rensa MeetingBuffer-chunks äldre än 24h. Returnerar antal raderade."""
    meeting_config = CONFIG.get('meeting_transcriber', {})
    buffer_dir_raw = meeting_config.get('buffer_dir')
    if not buffer_dir_raw:
        LOGGER.info("meeting_transcriber.buffer_dir saknas i config — skippar cleanup")
        return 0
    buffer_dir = os.path.expanduser(buffer_dir_raw)
    chunks_dir = os.path.join(buffer_dir, 'chunks')

    if not os.path.exists(chunks_dir):
        return 0

    now = time.time()
    max_age_seconds = 24 * 60 * 60
    cleaned = 0

    for filename in os.listdir(chunks_dir):
        if not filename.endswith('.txt'):
            continue
        filepath = os.path.join(chunks_dir, filename)
        try:
            if now - os.path.getmtime(filepath) > max_age_seconds:
                os.remove(filepath)
                cleaned += 1
        except OSError as e:
            LOGGER.warning(f"Kunde inte radera {filename}: {e}")

    if cleaned > 0:
        LOGGER.info(f"Cleaned {cleaned} old meeting chunks (>24h)")
    return cleaned


def main() -> int:
    """Kör alla prestart-checks. Returnerar exit-code."""
    LOGGER.info("Prestart startad")
    clean_old_meeting_chunks()
    run_startup_checks()
    LOGGER.info("Prestart klar")
    return 0


if __name__ == "__main__":
    sys.exit(main())
