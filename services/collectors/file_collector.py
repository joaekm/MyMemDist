import os
import time
import yaml
import logging
import shutil
import uuid
import re
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- CONFIG ---
def load_yaml(filnamn):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    paths = [
        os.path.join(script_dir, '..', '..', 'config', filnamn),
        os.path.join(script_dir, '..', 'config', filnamn),
        os.path.join(script_dir, 'config', filnamn),
    ]
    for p in paths:
        if os.path.exists(p):
            with open(p, 'r') as f: return yaml.safe_load(f)
    exit(1)

CONFIG = load_yaml('my_mem_config.yaml')
DROP_FOLDER = os.path.expanduser(CONFIG['paths']['drop_folder'])
ASSET_STORE = os.path.expanduser(CONFIG['paths']['asset_store'])
LOG_FILE = os.path.expanduser(CONFIG['logging'].get('system_log', '~/MyMemory/Logs/system.log'))

# Sub-folders för sortering
RECORDINGS_FOLDER = os.path.expanduser(CONFIG['paths']['asset_recordings'])
DOCUMENTS_FOLDER = os.path.expanduser(CONFIG['paths']['asset_documents'])

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
import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from services.utils.terminal_status import status as terminal_status, service_status

# Regex för standard UUID (8-4-4-4-12) var som helst i texten
UUID_PATTERN = re.compile(r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})')

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
            new_uuid = str(uuid.uuid4())
            clean_base = re.sub(r'[ _-]+', '_', base).strip('_')
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
            terminal_status("retriever", filnamn, "done")
            LOGGER.info(f"Flyttad till {folder_name}: {final_name}")
        except (OSError, shutil.Error) as e:
            terminal_status("retriever", filnamn, "failed", str(e))
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

    service_status("File Retriever", "started")

    observer = Observer()
    observer.schedule(MoveHandler(), DROP_FOLDER, recursive=False)
    observer.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt: observer.stop()
    observer.join()