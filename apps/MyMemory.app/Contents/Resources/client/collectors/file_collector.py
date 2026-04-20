import os
import time
import yaml
import logging
import shutil
import uuid
import re
import json
import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- CONFIG ---
from client.utils.config_loader import get_config

CONFIG = get_config()
DROP_FOLDER = os.path.expanduser(CONFIG['paths']['drop_folder'])
ASSET_STORE = os.path.expanduser(CONFIG['paths']['asset_store'])
LAKE_STORE = os.path.expanduser(CONFIG['paths'].get('lake_store', ''))
LOG_FILE = os.path.expanduser(CONFIG['logging'].get('system_log', '~/MyMemory/Logs/system.log'))

# Sub-folders för sortering
RECORDINGS_FOLDER = os.path.expanduser(CONFIG['paths']['asset_recordings'])
DOCUMENTS_FOLDER = os.path.expanduser(CONFIG['paths']['asset_documents'])

# Pending drop conflicts (fil-baserad IPC med menubar-appen)
PENDING_DROPS_DIR = os.path.expanduser(CONFIG['paths']['pending_drops'])
DOWNLOADS_FOLDER = os.path.expanduser(CONFIG['paths']['downloads_folder'])

# Extensions för sortering
AUDIO_EXTENSIONS = CONFIG.get('processing', {}).get('audio_extensions', [])
DOC_EXTENSIONS = CONFIG.get('processing', {}).get('document_extensions', [])

# --- LOGGING ---
log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

# Configure root logger: file only, no console
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - RETRIEVER - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

LOGGER = logging.getLogger('MyMem_Retriever')

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Regex för standard UUID (8-4-4-4-12) var som helst i texten
UUID_PATTERN = re.compile(r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})')


# --- DOCUMENT IDENTITY: Basnamnsmatch + Pending Conflicts ---

def _find_existing_asset(clean_base, ext_lower):
    """Sök befintlig Asset med samma basnamn (exakt match, UUID strippat)."""
    for folder in [DOCUMENTS_FOLDER, RECORDINGS_FOLDER]:
        if not os.path.isdir(folder):
            continue
        for f in os.listdir(folder):
            f_base, f_ext = os.path.splitext(f)
            if f_ext.lower() != ext_lower:
                continue
            match = UUID_PATTERN.search(f_base)
            if not match:
                continue
            # Strippa UUID och normalisera — samma logik som process_file
            existing_clean = f_base.replace(match.group(0), "")
            existing_clean = re.sub(r'[ _-]+', '_', existing_clean).strip('_')
            if existing_clean.lower() == clean_base.lower():
                return {
                    "existing_asset_path": os.path.join(folder, f),
                    "existing_uuid": match.group(0),
                    "existing_filename": f,
                }
    return None


def _get_context_summary(unit_id):
    """Läs context_summary från Lake-fil för visning i alert."""
    if not os.path.isdir(LAKE_STORE):
        return ""
    for f in os.listdir(LAKE_STORE):
        if unit_id in f and f.endswith('.md'):
            path = os.path.join(LAKE_STORE, f)
            try:
                with open(path, 'r', encoding='utf-8') as fh:
                    content = fh.read()
                if not content.startswith('---'):
                    return ""
                parts = content.split('---', 2)
                if len(parts) < 3:
                    return ""
                meta = yaml.safe_load(parts[1]) or {}
                return meta.get('context_summary', '')
            except (OSError, yaml.YAMLError) as e:
                LOGGER.warning(f"Kunde inte läsa Lake-metadata för {unit_id}: {e}")
                return ""
    return ""


def _is_pending(src_path):
    """Kolla om filen redan har en pending conflict."""
    if not os.path.isdir(PENDING_DROPS_DIR):
        return False
    filename = os.path.basename(src_path)
    for f in os.listdir(PENDING_DROPS_DIR):
        if not f.endswith('.json'):
            continue
        try:
            with open(os.path.join(PENDING_DROPS_DIR, f), 'r') as fh:
                data = json.load(fh)
                if data.get('drop_filename') == filename:
                    return True
        except (json.JSONDecodeError, OSError):
            continue
    return False


def _write_pending_conflict(src_path, existing_info, clean_name, ext):
    """Skriv conflict JSON för menubar-appen att plocka upp."""
    os.makedirs(PENDING_DROPS_DIR, exist_ok=True)
    conflict_id = str(uuid.uuid4())
    context = _get_context_summary(existing_info['existing_uuid'])

    conflict = {
        "id": conflict_id,
        "drop_path": src_path,
        "drop_filename": os.path.basename(src_path),
        "existing_asset_path": existing_info['existing_asset_path'],
        "existing_uuid": existing_info['existing_uuid'],
        "clean_name": clean_name,
        "extension": ext,
        "context_summary": context,
        "timestamp": datetime.datetime.now().isoformat(),
    }

    pending_file = os.path.join(PENDING_DROPS_DIR, f"{conflict_id}.json")
    with open(pending_file, 'w', encoding='utf-8') as f:
        json.dump(conflict, f, ensure_ascii=False, indent=2)

    LOGGER.info(f"Pending conflict: {os.path.basename(src_path)} matchar {existing_info['existing_filename']}")


def resolve_pending_drop(pending_id, action):
    """
    Lös en pending drop-konflikt. Anropas från menubar-appen via subprocess.

    action: "update" | "new" | "skip"
    """
    pending_file = os.path.join(PENDING_DROPS_DIR, f"{pending_id}.json")
    if not os.path.exists(pending_file):
        return {"status": "ERROR", "message": f"Pending conflict {pending_id} saknas"}

    with open(pending_file, 'r') as f:
        conflict = json.load(f)

    drop_path = conflict["drop_path"]
    if not os.path.exists(drop_path):
        os.remove(pending_file)
        return {"status": "ERROR", "message": f"Drop-filen finns inte längre: {drop_path}"}

    result_filename = ""

    if action == "update":
        # Skriv över befintlig Asset → mtime-ändring → re-ingest triggas
        shutil.move(drop_path, conflict["existing_asset_path"])
        result_filename = os.path.basename(conflict["existing_asset_path"])
        LOGGER.info(f"Resolved conflict (update): {result_filename}")

    elif action == "new":
        # Nytt UUID, flytta till Assets som vanligt
        new_uuid = str(uuid.uuid4())
        clean_name = conflict["clean_name"]
        ext = conflict["extension"]
        result_filename = f"{clean_name}_{new_uuid}{ext}"

        ext_lower = ext.lower()
        if ext_lower in AUDIO_EXTENSIONS:
            dest_folder = RECORDINGS_FOLDER
        else:
            dest_folder = DOCUMENTS_FOLDER

        os.makedirs(dest_folder, exist_ok=True)
        dest_path = os.path.join(dest_folder, result_filename)
        shutil.move(drop_path, dest_path)
        LOGGER.info(f"Resolved conflict (new): {result_filename}")

    elif action == "skip":
        # Flytta till Hämtade filer
        os.makedirs(DOWNLOADS_FOLDER, exist_ok=True)
        dest_path = os.path.join(DOWNLOADS_FOLDER, conflict["drop_filename"])
        # Undvik namnkollision i Downloads
        if os.path.exists(dest_path):
            name, ext = os.path.splitext(conflict["drop_filename"])
            dest_path = os.path.join(DOWNLOADS_FOLDER, f"{name}_{uuid.uuid4().hex[:8]}{ext}")
        shutil.move(drop_path, dest_path)
        result_filename = os.path.basename(dest_path)
        LOGGER.info(f"Resolved conflict (skip): {conflict['drop_filename']} → ~/Downloads/")

    else:
        return {"status": "ERROR", "message": f"Okänd action: {action}"}

    # Ta bort pending-filen
    os.remove(pending_file)

    return {"status": "OK", "action": action, "filename": result_filename}


class MoveHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory: return
        self.process_file(event.src_path)

    def on_modified(self, event):
        if event.is_directory: return
        if not os.path.exists(event.src_path): return
        time.sleep(1) 
        self.process_file(event.src_path)

    def process_file(self, src_path):
        filnamn = os.path.basename(src_path)
        if filnamn.startswith('.'): return
        
        base, ext = os.path.splitext(filnamn)
        
        # 1. SÖK UUID: Finns det ett giltigt UUID någonstans i namnet?
        match = UUID_PATTERN.search(base)
        
        if match:
            found_uuid = match.group(0)
            
            # 2. STÄDA: Ta bort UUID + ev. skräptecken från originalnamnet
            # Vi ersätter UUID med tom sträng
            clean_base = base.replace(found_uuid, "")
            
            # Städa bort dubbla understreck, mellanslag eller bindestreck som blev kvar
            # Ersätt alla separatorer med ett snyggt understreck
            clean_base = re.sub(r'[ _-]+', '_', clean_base)
            
            # Ta bort ledande/avslutande understreck
            clean_base = clean_base.strip('_')
            
            if not clean_base:
                clean_base = "Namnlos_Fil"

            # 3. KONSTRUERA: [Namn]_[UUID].[ext]
            final_name = f"{clean_base}_{found_uuid}{ext}"
            
            LOGGER.info(f"Normaliserar: {filnamn} -> {final_name}")
            
        else:
            clean_base = re.sub(r'[ _-]+', '_', base).strip('_')

            # Kolla om filen redan väntar på resolution
            if _is_pending(src_path):
                LOGGER.debug(f"Skippar {filnamn} — väntar på pending resolution")
                return

            # Kolla om det finns en befintlig Asset med samma basnamn
            existing = _find_existing_asset(clean_base, ext.lower())
            if existing:
                _write_pending_conflict(src_path, existing, clean_base, ext)
                LOGGER.info(f"Pending conflict: {filnamn} vs {existing}")
                return

            new_uuid = str(uuid.uuid4())
            final_name = f"{clean_base}_{new_uuid}{ext}"
            LOGGER.info(f"Ny UUID: {filnamn} -> {final_name}")

        # Sortera till rätt undermapp baserat på extension
        ext_lower = ext.lower()
        if ext_lower in AUDIO_EXTENSIONS:
            dest_folder = RECORDINGS_FOLDER
            folder_name = "Recordings"
        elif ext_lower in DOC_EXTENSIONS:
            dest_folder = DOCUMENTS_FOLDER
            folder_name = "Documents"
        else:
            dest_folder = DOCUMENTS_FOLDER  # Default: Documents
            folder_name = "Documents"
        
        os.makedirs(dest_folder, exist_ok=True)
        dest_path = os.path.join(dest_folder, final_name)
        
        if os.path.exists(dest_path):
            LOGGER.warning(f"Dubblett: {final_name}")
            return

        try:
            shutil.move(src_path, dest_path)
            LOGGER.info(f"Flyttad till {folder_name}: {final_name}")
        except (OSError, shutil.Error) as e:
            LOGGER.error(f"Flyttfel {filnamn}: {e}")

if __name__ == "__main__":
    os.makedirs(DROP_FOLDER, exist_ok=True)
    os.makedirs(RECORDINGS_FOLDER, exist_ok=True)
    os.makedirs(DOCUMENTS_FOLDER, exist_ok=True)

    # Kör igenom befintliga filer i Drop vid start
    pending = 0
    for f in os.listdir(DROP_FOLDER):
        full_path = os.path.join(DROP_FOLDER, f)
        if os.path.isfile(full_path) and not f.startswith('.'):
            pending += 1
            handler = MoveHandler()
            handler.process_file(full_path)

    LOGGER.info("File Retriever started")

    observer = Observer()
    observer.schedule(MoveHandler(), DROP_FOLDER, recursive=False)
    observer.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt: observer.stop()
    observer.join()