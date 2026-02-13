#!/usr/bin/env python3
"""
Røde Wireless Pro Collector

Detekterar inkopplade Røde Wireless Pro-mikrofoner (USB mass storage),
kopierar WAV-filer, konverterar till m4a 128kbps mono, och döper om
enligt namnstandarden: Inspelning_Rode_YYYYMMDD_HHMM_UUID.m4a

Flöde:
1. Pollar /Volumes/ efter "WirelessPRO*"-volymer
2. Skannar varje volym efter .WAV-filer
3. Hoppar över redan importerade filer (spårfil per enhet)
4. Konverterar WAV → m4a (128kbps, mono) via ffmpeg till tmp/-mapp
5. Atomic rename till Assets/Recordings/ (undviker race med transcriber watchdog)
6. Loggar till system.log med prefix RODE
"""

import os
import sys
import glob
import json
import time
import uuid
import shutil
import logging
import subprocess
import datetime
import yaml

# --- CONFIG ---
from services.utils.config_loader import get_config
from services.utils.audio_service import _get_ffmpeg, _get_ffprobe

CONFIG = get_config()
RECORDINGS_FOLDER = os.path.expanduser(CONFIG['paths']['asset_recordings'])
TMP_FOLDER = os.path.join(RECORDINGS_FOLDER, "tmp")
LOG_FILE = os.path.expanduser(CONFIG['logging'].get('system_log', '~/MyMemory/Logs/system.log'))

# Røde-specifik config
RODE_CONFIG = CONFIG.get('collectors', {}).get('rode', {})
VOLUME_PREFIX = RODE_CONFIG.get('volume_prefix', 'WirelessPRO')
POLL_INTERVAL = RODE_CONFIG.get('poll_interval_seconds', 30)
FFMPEG_BITRATE = RODE_CONFIG.get('ffmpeg_bitrate', '128k')
STATE_DIR = os.path.expanduser(RODE_CONFIG.get('state_dir', '~/MyMemory/Assets/.rode_state'))
SYSTEM_TZ = CONFIG.get('system', {}).get('timezone', 'Europe/Stockholm')

# --- LOGGING ---
log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - RODE - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

LOGGER = logging.getLogger('MyMem_Rode')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from services.utils.terminal_status import status as terminal_status, service_status


def find_rode_volumes():
    """Hitta alla monterade Røde Wireless Pro-volymer."""
    volumes = []
    volumes_dir = "/Volumes"
    if not os.path.isdir(volumes_dir):
        return volumes

    for name in os.listdir(volumes_dir):
        if name.startswith(VOLUME_PREFIX):
            vol_path = os.path.join(volumes_dir, name)
            if os.path.isdir(vol_path):
                volumes.append(vol_path)

    return sorted(volumes)


def get_wav_files(volume_path):
    """Hämta alla WAV-filer från en Røde-volym."""
    wav_files = []
    for entry in os.listdir(volume_path):
        if entry.upper().endswith('.WAV') and not entry.startswith('.'):
            full_path = os.path.join(volume_path, entry)
            if os.path.isfile(full_path):
                wav_files.append(full_path)
    return sorted(wav_files)


def load_state(volume_path):
    """Ladda importhistorik för en specifik volym."""
    os.makedirs(STATE_DIR, exist_ok=True)
    vol_name = os.path.basename(volume_path)
    state_file = os.path.join(STATE_DIR, f"{vol_name}.json")

    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return json.load(f)

    return {"imported_files": {}}


def save_state(volume_path, state):
    """Spara importhistorik för en specifik volym."""
    os.makedirs(STATE_DIR, exist_ok=True)
    vol_name = os.path.basename(volume_path)
    state_file = os.path.join(STATE_DIR, f"{vol_name}.json")

    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)


def file_signature(filepath):
    """Skapa en unik signatur för en fil (namn + storlek + mtime)."""
    stat = os.stat(filepath)
    return f"{os.path.basename(filepath)}|{stat.st_size}|{int(stat.st_mtime)}"


def _get_wav_duration_seconds(filepath):
    """Hämta WAV-filens duration i sekunder via ffprobe."""
    ffprobe_path = _get_ffprobe()

    cmd = [
        ffprobe_path,
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        filepath
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except (subprocess.TimeoutExpired, ValueError) as e:
        LOGGER.warning(f"Kunde inte hämta duration för {filepath}: {e}")

    return None


def get_recording_datetime(filepath):
    """Extrahera inspelningens STARTTID från mtime minus duration.

    Rode-enheten sätter mtime vid inspelningsslut. Vi subtraherar
    filens duration för att få starttiden.
    """
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(SYSTEM_TZ)
    except (ImportError, KeyError):
        tz = None

    mtime = os.path.getmtime(filepath)
    if tz:
        end_dt = datetime.datetime.fromtimestamp(mtime, tz=tz)
    else:
        end_dt = datetime.datetime.fromtimestamp(mtime)

    duration = _get_wav_duration_seconds(filepath)
    if duration:
        start_dt = end_dt - datetime.timedelta(seconds=duration)
        LOGGER.debug(f"Recording start: {start_dt} (end: {end_dt}, duration: {duration:.0f}s)")
        return start_dt

    LOGGER.warning(f"Använder mtime som fallback (duration ej tillgänglig): {filepath}")
    return end_dt


def generate_target_name(recording_dt, file_uuid):
    """Generera filnamn enligt namnstandarden: Inspelning_Rode_YYYYMMDD_HHMM_UUID.m4a"""
    date_str = recording_dt.strftime("%Y%m%d_%H%M")
    return f"Inspelning_Rode_{date_str}_{file_uuid}.m4a"


def convert_wav_to_m4a(src_path, dest_path):
    """Konvertera WAV till m4a 128kbps mono via ffmpeg."""
    cmd = [
        _get_ffmpeg(),
        "-i", src_path,
        "-ac", "1",            # mono
        "-b:a", FFMPEG_BITRATE,  # bitrate
        "-c:a", "aac",        # codec
        "-y",                  # overwrite
        dest_path
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=600  # 10 min timeout per fil
    )

    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {result.stderr[-500:]}")

    return dest_path


def process_volume(volume_path):
    """Processa alla nya WAV-filer på en Røde-volym.

    Konverterar till tmp/-mapp och gör atomic rename till Recordings/
    för att undvika race condition med transcriber watchdog.
    """
    vol_name = os.path.basename(volume_path)
    state = load_state(volume_path)
    imported = state.get("imported_files", {})

    wav_files = get_wav_files(volume_path)
    if not wav_files:
        return 0

    new_count = 0

    for wav_path in wav_files:
        filename = os.path.basename(wav_path)
        sig = file_signature(wav_path)

        # Hoppa över redan importerade
        if sig in imported:
            LOGGER.debug(f"Redan importerad: {filename} ({vol_name})")
            continue

        terminal_status("rode", f"{filename} ({vol_name})", "processing")
        LOGGER.info(f"Ny fil: {filename} på {vol_name}")

        tmp_path = None
        try:
            # Extrahera tidstämpel och generera namn
            recording_dt = get_recording_datetime(wav_path)
            file_uuid = str(uuid.uuid4())
            target_name = generate_target_name(recording_dt, file_uuid)
            tmp_path = os.path.join(TMP_FOLDER, target_name)
            dest_path = os.path.join(RECORDINGS_FOLDER, target_name)

            # Konvertera till tmp/
            LOGGER.info(f"Konverterar: {filename} -> tmp/{target_name}")
            convert_wav_to_m4a(wav_path, tmp_path)

            # Verifiera att tmp-filen skapades och har innehåll
            if not os.path.exists(tmp_path):
                raise RuntimeError(f"Utfil saknas efter konvertering: {tmp_path}")

            dest_size = os.path.getsize(tmp_path)
            if dest_size == 0:
                raise RuntimeError(f"Utfil tom efter konvertering: {tmp_path}")

            # Atomic rename till Recordings/ — triggar watchdog med färdig fil
            os.rename(tmp_path, dest_path)
            tmp_path = None  # Rensat, ingen cleanup behövs

            src_size = os.path.getsize(wav_path)

            # Spara i state
            imported[sig] = {
                "source_file": filename,
                "target_file": target_name,
                "uuid": file_uuid,
                "volume": vol_name,
                "recording_time": recording_dt.isoformat(),
                "imported_at": datetime.datetime.now().isoformat(),
                "source_size": src_size,
                "target_size": dest_size,
            }
            state["imported_files"] = imported
            save_state(volume_path, state)

            ratio = (dest_size / src_size * 100) if src_size > 0 else 0
            LOGGER.info(f"Klar: {target_name} ({dest_size // 1024}KB, {ratio:.0f}% av original)")
            terminal_status("rode", f"{filename} -> {target_name}", "done")
            new_count += 1

        except subprocess.TimeoutExpired:
            LOGGER.error(f"Timeout vid konvertering: {filename}")
            terminal_status("rode", filename, "failed", "ffmpeg timeout")
        except RuntimeError as e:
            LOGGER.error(f"Konverteringsfel: {filename}: {e}")
            terminal_status("rode", filename, "failed", str(e))
        except OSError as e:
            LOGGER.error(f"Filfel: {filename}: {e}")
            terminal_status("rode", filename, "failed", str(e))
        finally:
            # Rensa tmp-fil om den finns kvar efter fel
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError as cleanup_err:  # noqa: FALLBACK_DOCUMENTED - tmp-cleanup failure accepteras
                    LOGGER.warning(f"Kunde inte rensa tmp-fil {tmp_path}: {cleanup_err}")

    return new_count


def poll_loop():
    """Huvudloop - pollar efter Røde-volymer."""
    service_status("Røde Collector", "started")
    LOGGER.info(f"Røde Collector startad (prefix={VOLUME_PREFIX}, poll={POLL_INTERVAL}s)")

    previously_mounted = set()

    while True:
        try:
            volumes = find_rode_volumes()
            current_mounted = set(volumes)

            # Logga nya volymer
            new_volumes = current_mounted - previously_mounted
            for vol in new_volumes:
                LOGGER.info(f"Ny Røde-enhet detekterad: {vol}")

            # Logga borttagna volymer
            removed_volumes = previously_mounted - current_mounted
            for vol in removed_volumes:
                LOGGER.info(f"Røde-enhet bortkopplad: {vol}")

            previously_mounted = current_mounted

            # Processa alla monterade volymer
            for volume_path in volumes:
                try:
                    new_count = process_volume(volume_path)
                    if new_count > 0:
                        LOGGER.info(f"Importerade {new_count} fil(er) från {os.path.basename(volume_path)}")
                except OSError as e:
                    LOGGER.error(f"Fel vid processning av {volume_path}: {e}")

        except (OSError, RuntimeError, ValueError) as e:
            LOGGER.error(f"Fel i poll-loop: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    os.makedirs(RECORDINGS_FOLDER, exist_ok=True)
    os.makedirs(TMP_FOLDER, exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)
    poll_loop()
