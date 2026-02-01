import os
import sys
import yaml
import re
import datetime
import logging
import duckdb
import json

from services.utils.vector_service import get_vector_service

# Enkel loggning för CLI-verktyg
logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')
LOGGER = logging.getLogger('SystemValidator')

# --- CONFIG LOADER ---
def load_yaml(filnamn):
    # Nu i services/utils/ - config ligger i ../../config/
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '..', '..', 'config', filnamn)
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    print(f"[FEL] Saknar {filnamn}")
    exit(1)

CONFIG = load_yaml('my_mem_config.yaml')

LAKE_STORE = os.path.expanduser(CONFIG['paths']['lake_store'])
ASSET_STORE = os.path.expanduser(CONFIG['paths']['asset_store'])
CHROMA_PATH = os.path.expanduser(CONFIG['paths']['chroma_db'])
GRAPH_DB_PATH = os.path.expanduser(CONFIG['paths']['graph_db'])
LOG_FILE = os.path.expanduser(CONFIG['logging'].get('system_log', '~/MyMemory/Logs/system.log'))

# Hämta extensions
DOC_EXTS = CONFIG.get('processing', {}).get('document_extensions', [])
AUDIO_EXTS = CONFIG.get('processing', {}).get('audio_extensions', [])

# Regex för Strict Mode
UUID_SUFFIX_PATTERN = re.compile(r'_([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\.[a-zA-Z0-9]+$')
UUID_MD_PATTERN = re.compile(r'_([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\.md$')

def print_header(title):
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def get_lake_ids():
    """Returnerar dict med {uuid: filnamn} för alla filer i Lake"""
    lake_ids = {}
    lake_files = [f for f in os.listdir(LAKE_STORE) if f.endswith('.md') and not f.startswith('.')]
    for f in lake_files:
        match = UUID_MD_PATTERN.search(f)
        if match:
            lake_ids[match.group(1)] = f
    return lake_ids

def validera_filer():
    print_header("1. FILSYSTEMS-AUDIT (Strict Mode)")
    
    all_assets = []
    doc_files = []
    
    # Rekursiv insamling
    for root, dirs, files in os.walk(ASSET_STORE):
        # Ignorera dolda mappar
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        for f in files:
            if f.startswith('.'): continue
            
            # Spara relativ sökväg för rapportering om så önskas, men vi jobbar mest med filnamnet
            full_path = os.path.join(root, f)
            rel_path = os.path.relpath(full_path, ASSET_STORE)
            
            all_assets.append(f) # Vi validerar filnamnet oavsett var det ligger
            
            if os.path.splitext(f)[1].lower() in DOC_EXTS:
                doc_files.append(f)

    lake_files = [f for f in os.listdir(LAKE_STORE) if f.endswith('.md') and not f.startswith('.')]
    
    # 1.1 KONTROLLERA UUID-NAMNSTANDARD I ASSETS
    invalid_names = []
    for f in all_assets:
        # Nu när vi loopar filer (från os.walk) vet vi att det är filer, inte mappar.
        if not UUID_SUFFIX_PATTERN.search(f):
            invalid_names.append(f)

    print(f"📦 Assets Totalt:     {len(all_assets)} st")
    
    if invalid_names:
        print(f"❌ [VARNING] Hittade {len(invalid_names)} filer i Assets som bryter mot namnstandarden!")
        for bad in invalid_names[:10]: # Visa max 10
            print(f"   - {bad}")
        if len(invalid_names) > 10:
            print(f"   ... och {len(invalid_names) - 10} till.")
    else:
        print("✅ Alla filer i Assets följer standarden [Namn]_[UUID].")

    print(f"   - Dokument/.txt:  {len(doc_files)} st (Målvärde för Sjön)")
    print(f"🌊 Lake (Markdown):  {len(lake_files)} st")
    
    # 1.2 INTEGRITETS-CHECK (Lake vs Assets)
    # Filerna i Lake ska ha EXAKT samma basnamn som dokumenten i Assets.
    # Ex: Assets: "Rapport_123.pdf" -> Lake: "Rapport_123.md"
    
    asset_bases = {os.path.splitext(f)[0] for f in doc_files}
    lake_bases = {os.path.splitext(f)[0] for f in lake_files}
    
    missing_in_lake = asset_bases - lake_bases
    zombies_in_lake = lake_bases - asset_bases # Filer i sjön som inte har en källa

    if len(lake_files) == len(doc_files) and not missing_in_lake:
        print(f"\n✅ BALANS: {len(lake_files)} filer i Sjön matchar antalet källdokument.")
    else:
        if missing_in_lake:
            print(f"\n❌ SAKNAS I LAKE: {len(missing_in_lake)} dokument har inte konverterats!")
            for m in sorted(list(missing_in_lake))[:10]:
                print(f"   - {m}")
            if len(missing_in_lake) > 10: print(f"   ... ({len(missing_in_lake)-10} till)")
        
        if zombies_in_lake:
            print(f"\n⚠️ ZOMBIES I LAKE: {len(zombies_in_lake)} filer i Sjön saknar källfil i Assets:")
            for z in sorted(list(zombies_in_lake))[:10]:
                print(f"   - {z}")
            if len(zombies_in_lake) > 10: print(f"   ... ({len(zombies_in_lake)-10} till)")

    return len(lake_files)

def validera_chroma(expected_lake_count, lake_ids):
    print_header("2. VEKTOR-AUDIT (CHROMA)")
    try:
        vector_service = get_vector_service("knowledge_base")
        coll = vector_service.collection
        total_count = coll.count()

        # Hämta alla vektorer med metadata
        all_vectors = coll.get(include=['metadatas'])

        # Separera dokument-vektorer och graf-noder
        doc_vectors = {}  # id -> metadata
        graph_vectors = {}  # id -> metadata

        for vid, meta in zip(all_vectors['ids'], all_vectors['metadatas']):
            if meta.get('source') == 'graph_node':
                graph_vectors[vid] = meta
            else:
                doc_vectors[vid] = meta

        print(f"🧠 Vektorer totalt: {total_count} st")
        print(f"   - Dokument: {len(doc_vectors)} st")
        print(f"   - Graf-noder: {len(graph_vectors)} st")

        # 2a. Validera dokument mot Lake
        lake_id_set = set(lake_ids.keys())
        doc_id_set = set(doc_vectors.keys())

        missing_in_vector = lake_id_set - doc_id_set
        orphan_docs = doc_id_set - lake_id_set

        if len(doc_vectors) == expected_lake_count and not missing_in_vector:
            print(f"\n✅ DOKUMENT SYNKADE: {len(doc_vectors)} vektorer matchar {expected_lake_count} Lake-filer")
        else:
            if missing_in_vector:
                print(f"\n❌ Saknas i Vector ({len(missing_in_vector)} st):")
                for uid in list(missing_in_vector)[:5]:
                    filename = lake_ids.get(uid, uid)
                    display_name = filename.rsplit('_', 1)[0] if '_' in filename else filename
                    print(f"   - {display_name}")
                if len(missing_in_vector) > 5:
                    print(f"   ... och {len(missing_in_vector) - 5} till")

            if orphan_docs:
                print(f"\n⚠️ Föräldralösa dokument ({len(orphan_docs)} st) - finns ej i Lake")

        # 2b. Validera graf-noder mot GraphDB
        if os.path.exists(GRAPH_DB_PATH):
            conn = duckdb.connect(GRAPH_DB_PATH, read_only=True)
            graph_node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            conn.close()

            if len(graph_vectors) == graph_node_count:
                print(f"✅ GRAF-NODER SYNKADE: {len(graph_vectors)} vektorer matchar {graph_node_count} graf-noder")
            else:
                diff = abs(len(graph_vectors) - graph_node_count)
                print(f"⚠️ GRAF-NODER DIFF: {len(graph_vectors)} vektorer vs {graph_node_count} graf-noder (diff: {diff})")

    except Exception as e:
        LOGGER.error(f"Kunde inte läsa ChromaDB: {e}")
        print(f"❌ KRITISKT FEL: Kunde inte läsa ChromaDB: {e}")

def rensa_gammal_logg():
    """Rensar loggfilen på rader äldre än 24 timmar."""
    print_header("3. LOGG-RENSNING")
    
    if not os.path.exists(LOG_FILE):
        print(f"⚠️ Loggfil finns inte: {LOG_FILE}")
        return
    
    try:
        now = datetime.datetime.now()
        cutoff = now - datetime.timedelta(hours=24)
        
        # Läs alla rader
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        original_count = len(lines)
        kept_lines = []
        
        # Logg-format: "2025-12-11 14:06:33,526 - TRANS - INFO - ..."
        timestamp_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
        
        for line in lines:
            match = timestamp_pattern.match(line)
            if match:
                try:
                    line_time = datetime.datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
                    if line_time >= cutoff:
                        kept_lines.append(line)
                except ValueError as e:
                    # Kunde inte parsa tidsstämpel, behåll raden
                    LOGGER.debug(f"Kunde inte parsa tidsstämpel: {e}")
                    kept_lines.append(line)
            else:
                # Rad utan tidsstämpel (t.ex. fortsättning av felmeddelande), behåll
                kept_lines.append(line)
        
        removed_count = original_count - len(kept_lines)
        
        if removed_count > 0:
            # Skriv tillbaka de kvarvarande raderna
            with open(LOG_FILE, 'w', encoding='utf-8') as f:
                f.writelines(kept_lines)
            print(f"🧹 Rensade {removed_count} rader äldre än 24h")
            print(f"   Innan: {original_count} rader → Efter: {len(kept_lines)} rader")
        else:
            print(f"✅ Ingen rensning behövdes ({original_count} rader, alla inom 24h)")
            
    except Exception as e:
        LOGGER.error(f"Fel vid loggrensning: {e}")
        print(f"❌ Fel vid loggrensning: {e}")

def run_startup_checks():
    """
    Kör alla valideringar och returnerar health_info för auto_repair.
    Används av start_services.py vid uppstart.
    """
    print("=== MyMem System Validator ===")

    lake_c = validera_filer()
    lake_ids = get_lake_ids() if lake_c > 0 else {}

    # Hämta counts för health_info
    vector_count = 0

    if lake_c > 0:
        # Chroma (via VectorService för konsistent collection-namn)
        try:
            vector_service = get_vector_service("knowledge_base")
            vector_count = vector_service.count()
            validera_chroma(lake_c, lake_ids)
        except Exception as e:
            LOGGER.error(f"Kunde inte läsa ChromaDB: {e}")
            print(f"❌ KRITISKT FEL: Kunde inte läsa ChromaDB: {e}")
    else:
        print("\nIngen data att validera i databaserna.")

    # Rensa gammal logg
    rensa_gammal_logg()

    # Returnera health_info för auto_repair
    return {
        'lake_count': lake_c,
        'vector_count': vector_count,
        'lake_store': LAKE_STORE,
        'chroma_path': CHROMA_PATH,
        'lake_ids': lake_ids
    }

if __name__ == "__main__":
    run_startup_checks()
