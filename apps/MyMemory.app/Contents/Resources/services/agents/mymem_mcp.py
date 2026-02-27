import os
import sys
import re
import signal
import time
import yaml
import json
import logging
import uuid
import asyncio
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Path setup
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- SIGTERM handler för graceful shutdown ---
# OBS: os._exit() för att undvika cleanup-problem - se poc/process/signal_logging_poc.py
def _handle_sigterm(signum, frame):
    os._exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# --- LOGGING: Endast FileHandler, ingen terminal-output ---
# MCP-servrar använder stdout för protokoll, stderr läcker till terminal
_log_file = os.path.expanduser('~/MyMemory/Logs/my_mem_system.log')
os.makedirs(os.path.dirname(_log_file), exist_ok=True)

_root = logging.getLogger()
_root.setLevel(logging.INFO)
for _h in _root.handlers[:]:
    _root.removeHandler(_h)

_fh = logging.FileHandler(_log_file)
_fh.setFormatter(logging.Formatter('%(asctime)s - MCP_SEARCH - %(levelname)s - %(message)s'))
_root.addHandler(_fh)

from mcp.server.fastmcp import FastMCP
from services.utils.graph_service import GraphService
from services.utils.vector_service import vector_scope
from services.utils.shared_lock import resource_lock

# Tysta tredjepartsloggers EFTER import
for _name in ['httpx', 'httpcore', 'mcp', 'anyio']:
    logging.getLogger(_name).setLevel(logging.WARNING)

# --- CONFIG LOADING ---
from services.utils.config_loader import get_config

try:
    CONFIG = get_config()
except FileNotFoundError as e:
    logging.error(f"Config load failed: {e}")
    CONFIG = {}
PATHS = CONFIG.get('paths', {})
SEARCH_CONFIG = CONFIG.get('search', {})

GRAPH_PATH = os.path.expanduser(PATHS.get('graph_db', '~/MyMemory/Index/GraphDB'))
LAKE_PATH = os.path.expanduser(PATHS.get('lake_dir', '~/MyMemory/Lake'))
VECTOR_PATH = os.path.expanduser(PATHS.get('vector_db', '~/MyMemory/Index/VectorDB'))
AI_GENERATED_PATH = os.path.expanduser(PATHS.get('asset_ai_generated', '~/MyMemory/Assets/AIGenerated'))

# Aktuella sökvägar (kan bytas runtime)
_current_paths = {
    "graph": GRAPH_PATH,
    "lake": LAKE_PATH,
    "vector": VECTOR_PATH,
    "label": "default"
}

def _get_graph_path():
    return _current_paths["graph"]

def _get_lake_path():
    return _current_paths["lake"]

# Search limits och tröskelvärden från config
GRAPH_SEARCH_LIMIT = SEARCH_CONFIG.get('graph_limit', 15)
VECTOR_DISTANCE_STRONG = SEARCH_CONFIG.get('distance_strong', 0.8)
VECTOR_DISTANCE_WEAK = SEARCH_CONFIG.get('distance_weak', 1.2)
SLOW_QUERY_THRESHOLD = SEARCH_CONFIG.get('slow_query_threshold_ms', 2000)


def _log_tool_time(tool_name: str, t0: float):
    """Loggar exekveringstid för ett MCP-verktyg.

    Loggar alltid vid INFO-nivå. Varnar om tiden överskrider SLOW_QUERY_THRESHOLD.
    """
    elapsed_ms = (time.monotonic() - t0) * 1000
    if elapsed_ms > SLOW_QUERY_THRESHOLD:
        logging.warning(f"SLOW {tool_name}: {elapsed_ms:.0f}ms")
    else:
        logging.info(f"{tool_name}: {elapsed_ms:.0f}ms")


mcp = FastMCP("MyMemory")

# --- MEETING TRANSCRIBER STATE ---
# Subprocess och buffer-hantering för mötesbevakning

MEETING_CONFIG = CONFIG.get('meeting_transcriber', {})
MEETING_BUFFER_DIR = Path(os.path.expanduser(MEETING_CONFIG.get('buffer_dir', '~/MyMemory/MeetingBuffer')))
MEETING_CHUNKS_DIR = MEETING_BUFFER_DIR / "chunks"
MEETING_PROCESSED_FILE = MEETING_BUFFER_DIR / "processed.txt"
MEETING_STATE_FILE = MEETING_BUFFER_DIR / "watch_state.json"

# Säkerställ att mappar finns
MEETING_BUFFER_DIR.mkdir(parents=True, exist_ok=True)
MEETING_CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

# Global transcriber process reference
_transcriber_process: subprocess.Popen = None


def _get_meeting_processed_chunks() -> set:
    """Läs lista på redan processade chunks."""
    if not MEETING_PROCESSED_FILE.exists():
        return set()
    return set(MEETING_PROCESSED_FILE.read_text().strip().split("\n"))


def _mark_meeting_chunk_processed(chunk_file: str):
    """Markera chunk som processad."""
    with open(MEETING_PROCESSED_FILE, "a") as f:
        f.write(f"{chunk_file}\n")


def _get_meeting_unread_chunks() -> list:
    """Hämta alla olästa chunks, sorterade efter tid."""
    processed = _get_meeting_processed_chunks()
    chunks = []

    for chunk_file in sorted(MEETING_CHUNKS_DIR.glob("*.txt")):
        if chunk_file.name not in processed:
            content = chunk_file.read_text()
            chunks.append((chunk_file.name, content))

    return chunks


def _get_meeting_watch_state() -> dict:
    """Läs bevakningsstate."""
    if not MEETING_STATE_FILE.exists():
        return {"active": False, "started_at": None, "chunks_processed": 0}
    try:
        return json.loads(MEETING_STATE_FILE.read_text())
    except json.JSONDecodeError:
        return {"active": False, "started_at": None, "chunks_processed": 0}


def _set_meeting_watch_state(active: bool, **kwargs):
    """Uppdatera bevakningsstate."""
    state = _get_meeting_watch_state()
    state["active"] = active
    state.update(kwargs)
    MEETING_STATE_FILE.write_text(json.dumps(state, indent=2))


def _start_transcriber_process() -> bool:
    """Starta transcriber som subprocess."""
    global _transcriber_process

    # Kolla om redan igång
    if _transcriber_process and _transcriber_process.poll() is None:
        logging.info("Transcriber already running")
        return True

    transcriber_script = Path(project_root) / "services" / "meeting_transcriber" / "daemon.py"

    if not transcriber_script.exists():
        logging.error(f"Transcriber script not found: {transcriber_script}")
        return False

    try:
        _transcriber_process = subprocess.Popen(
            [sys.executable, str(transcriber_script)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        logging.info(f"Started transcriber process (PID: {_transcriber_process.pid})")
        return True
    except Exception as e:
        logging.error(f"Failed to start transcriber: {e}")
        return False


def _stop_transcriber_process():
    """Stoppa transcriber subprocess."""
    global _transcriber_process

    if _transcriber_process:
        try:
            if _transcriber_process.poll() is None:
                _transcriber_process.terminate()
                _transcriber_process.wait(timeout=5)
                logging.info("Transcriber stopped")
        except subprocess.TimeoutExpired:
            _transcriber_process.kill()
            logging.warning("Transcriber killed (did not stop gracefully)")
        except Exception as e:
            logging.error(f"Error stopping transcriber: {e}")
        finally:
            _transcriber_process = None

# --- TOOL 0: INDEX SWITCHING ---

@mcp.tool()
def switch_index(backup_path: str = None) -> str:
    """
    Byt vilket index som söks – för att jämföra backup med nuvarande.

    ANVÄNDNING:
    - switch_index("/Users/jekman/MyMemory_backup_20260117_2100") → Byt till backup
    - switch_index() → Återgå till default

    Efter byte påverkas ALLA sökverktyg (graf, vektor, lake).
    Perfekt för kvalitetsjämförelser före/efter rebuild.

    Args:
        backup_path: Sökväg till MyMemory-backup (eller None för att återställa default)
    """
    global _current_paths

    if backup_path is None:
        # Återställ till default
        _current_paths["graph"] = GRAPH_PATH
        _current_paths["lake"] = LAKE_PATH
        _current_paths["vector"] = VECTOR_PATH
        _current_paths["label"] = "default"
        _lake_cache.invalidate()
        return f"✅ Återställt till DEFAULT index:\n  Graf: {GRAPH_PATH}\n  Lake: {LAKE_PATH}\n  Vektor: {VECTOR_PATH}"

    # Expandera och validera sökvägen
    backup_path = os.path.expanduser(backup_path)

    if not os.path.exists(backup_path):
        return f"❌ Sökvägen finns inte: {backup_path}"

    # Sök efter index-mappar i backup
    graph_path = os.path.join(backup_path, "Index", "GraphDB")
    lake_path = os.path.join(backup_path, "Lake")
    vector_path = os.path.join(backup_path, "Index", "VectorDB")

    missing = []
    if not os.path.exists(graph_path):
        missing.append(f"Graf: {graph_path}")
    if not os.path.exists(lake_path):
        missing.append(f"Lake: {lake_path}")
    if not os.path.exists(vector_path):
        missing.append(f"Vektor: {vector_path}")

    if missing:
        return f"❌ Backup saknar komponenter:\n  " + "\n  ".join(missing)

    # Byt till backup
    _current_paths["graph"] = graph_path
    _current_paths["lake"] = lake_path
    _current_paths["vector"] = vector_path
    _current_paths["label"] = os.path.basename(backup_path)
    _lake_cache.invalidate()

    return (
        f"✅ Bytte till BACKUP index ({_current_paths['label']}):\n"
        f"  Graf: {graph_path}\n"
        f"  Lake: {lake_path}\n"
        f"  ⚠️ Vektor: Använder fortfarande default (singleton-begränsning)\n"
        f"\nAnvänd search_graph_nodes och get_graph_statistics för jämförelser."
    )


@mcp.tool()
def get_current_index() -> str:
    """
    Visa vilket index som är aktivt just nu.
    """
    return f"Aktivt index: {_current_paths['label']}\n  Graf: {_current_paths['graph']}\n  Lake: {_current_paths['lake']}\n  Vektor: {_current_paths['vector']}"


# --- HELPERS ---

def _parse_frontmatter(file_path: str) -> Dict:
    """Läser YAML-frontmatter från en markdown-fil.

    Läser rad-för-rad istället för hela filen - viktigt för stora
    transkript/dokument där frontmatter bara är ~20 rader i toppen.
    """
    try:
        lines = []
        in_frontmatter = False
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if not first_line.startswith('---'):
                return {}
            in_frontmatter = True
            for line in f:
                if line.startswith('---'):
                    break
                lines.append(line)
            else:
                # Nådde EOF utan avslutande ---
                return {}
        if not lines:
            return {}
        return yaml.safe_load(''.join(lines)) or {}
    except Exception:
        return {}


# --- LAKE METADATA CACHE ---
# Cachar frontmatter för alla Lake-filer så att search_by_date_range
# och search_lake_metadata inte behöver läsa varje fil vid varje sökning.

class _LakeMetadataCache:
    """In-memory cache av Lake-filernas frontmatter.

    Laddar alla filer vid första anrop, sedan inkrementellt:
    - Nya filer (finns på disk men inte i cache) läggs till
    - Borttagna filer (finns i cache men inte på disk) rensas
    Invalideras helt om index byts (switch_index).
    """

    def __init__(self):
        self._cache: Dict[str, Dict] = {}  # filename -> frontmatter
        self._loaded = False
        self._lake_path: Optional[str] = None

    def invalidate(self):
        """Rensa hela cachen (t.ex. vid index-byte)."""
        self._cache.clear()
        self._loaded = False
        self._lake_path = None

    def _ensure_loaded(self):
        """Ladda eller uppdatera cachen inkrementellt."""
        lake_path = _get_lake_path()

        # Om lake-sökväg ändrats → full reload
        if self._lake_path != lake_path:
            self._cache.clear()
            self._loaded = False
            self._lake_path = lake_path

        if not os.path.exists(lake_path):
            return

        current_files = {f for f in os.listdir(lake_path) if f.endswith('.md')}
        cached_files = set(self._cache.keys())

        if self._loaded:
            # Inkrementell uppdatering
            new_files = current_files - cached_files
            removed_files = cached_files - current_files

            for filename in removed_files:
                del self._cache[filename]

            for filename in new_files:
                full_path = os.path.join(lake_path, filename)
                self._cache[filename] = _parse_frontmatter(full_path)
        else:
            # Full load
            import time as _time
            t0 = _time.monotonic()
            for filename in current_files:
                if filename not in self._cache:
                    full_path = os.path.join(lake_path, filename)
                    self._cache[filename] = _parse_frontmatter(full_path)
            elapsed = _time.monotonic() - t0
            logging.info(f"Lake metadata cache loaded: {len(self._cache)} files in {elapsed:.2f}s")
            self._loaded = True

    def get_all(self) -> Dict[str, Dict]:
        """Returnera {filename: frontmatter} för alla Lake-filer."""
        self._ensure_loaded()
        return self._cache


_lake_cache = _LakeMetadataCache()

# --- TOOL 1: GRAPH (Structure) ---

@mcp.tool()
def search_graph_nodes(query: str, node_type: str = None) -> str:
    """
    Exakt sökning i grafstrukturen – hittar specifika entiteter via namn, alias eller ID.

    ANVÄND FÖR:
    - Hitta en specifik person: query="Johan"
    - Kolla om en organisation finns: query="Acme AB", node_type="Organization"
    - Söka på e-post eller andra properties: query="johan@example.com"

    Söker i: id, aliases, och hela properties (name, email, context_summary, etc.)

    SKILLNAD MOT query_vector_memory:
    - search_graph_nodes = "Finns noden X?" (exakt matchning)
    - query_vector_memory = "Vem jobbar med AI-projekt?" (semantisk sökning)

    TIPS: Börja ofta här för att hitta rätt node_id,
    använd sedan get_entity_summary eller get_neighbor_network för detaljer.
    """
    _t0 = time.monotonic()
    try:
        # Validera node_type mot schemat om angivet
        if node_type:
            schema_path = os.path.expanduser(CONFIG.get('paths', {}).get('graph_schema', ''))
            if schema_path and os.path.exists(schema_path):
                with open(schema_path, 'r') as sf:
                    schema = json.load(sf)
                valid_types = [nt['type'] for nt in schema.get('node_types', [])]
                if node_type not in valid_types:
                    return f"Ogiltig node_type: '{node_type}'. Giltiga: {', '.join(sorted(valid_types))}"

        with resource_lock("graph", exclusive=False, timeout=10.0):
            graph = GraphService(_get_graph_path(), read_only=True)
            limit = GRAPH_SEARCH_LIMIT

            sql = "SELECT id, type, aliases, properties FROM nodes WHERE (id ILIKE ? OR aliases ILIKE ? OR properties ILIKE ?)"
            params = [f"%{query}%", f"%{query}%", f"%{query}%"]

            if node_type:
                sql += " AND type = ?"
                params.append(node_type)

            sql += " LIMIT ?"
            params.append(limit)

            rows = graph.conn.execute(sql, params).fetchall()
            graph.close()

        if not rows:
            return f"GRAF: Inga träffar för '{query}'" + (f" (Typ: {node_type})" if node_type else "")

        output = [f"=== GRAF RESULTAT ({len(rows)}) ==="]
        for r in rows:
            node_id, n_type, aliases_raw, props_raw = r
            props = json.loads(props_raw) if props_raw else {}
            aliases = json.loads(aliases_raw) if aliases_raw else []

            name = props.get('name', node_id)
            ctx_summary = props.get('context_summary', '')
            ctx_str = f"Identitet: {ctx_summary}" if ctx_summary else "Identitet: SAKNAS"
            alias_str = f"Aliases: {len(aliases)}" if aliases else ""

            output.append(f"• [{n_type}] {name}")
            output.append(f"  ID: {node_id}")
            if alias_str: output.append(f"  {alias_str}")
            output.append(f"  {ctx_str}")

        return "\n".join(output)
    except TimeoutError:
        return "Grafsökning misslyckades: Databasen är upptagen (ingestion pågår). Försök igen om en stund."
    except Exception as e:
        return f"Grafsökning misslyckades: {e}"
    finally:
        _log_tool_time("search_graph_nodes", _t0)

# --- TOOL 2: VECTOR (Semantics) ---

@mcp.tool()
def query_vector_memory(query_text: str, n_results: int = 5) -> str:
    """
    Semantisk sökning i kunskapsgrafen – hittar entiteter baserat på MENING, inte bara nyckelord.

    Varje entitet har en "context_summary" som beskriver:
    - Vem/vad entiteten ÄR (kompakt identitet)
    - Relationer och kontext via relation_context på kanter

    EXEMPEL PÅ CONTEXT_SUMMARY:
    - "IT-konsultbolag specialiserat på digital transformation"
    - "Projektledare på Digitalist, ansvarig för AI-initiativ"
    - "Internt projekt för kunskapshantering"

    SÖK PÅ KONCEPT, INTE NAMN:
    ✅ "vem arbetar med upphandlingar"
    ✅ "projekt med faktureringsproblem"
    ✅ "personer som haft kundmöten"
    ❌ "Johan" (använd search_graph_nodes för exakta namn)

    Returnerar: Entiteter rankade efter semantisk likhet med din fråga.
    """
    _t0 = time.monotonic()
    try:
        with vector_scope(exclusive=False, timeout=10.0) as vs:
            results = vs.search(query_text=query_text, limit=n_results)
            model_name = vs.model_name

        if not results:
            return f"VEKTOR: Inga semantiska matchningar för '{query_text}'."

        output = [f"=== VEKTOR RESULTAT ('{query_text}') ==="]
        output.append(f"Modell: {model_name}")
        output.append("-" * 30)

        for i, item in enumerate(results):
            dist = item['distance']
            meta = item['metadata']
            content = item['document']
            uid = item['id']

            content_preview = content.replace('\n', ' ')[:150] + "..."

            quality = "🔥 Stark" if dist < VECTOR_DISTANCE_STRONG else "❄️ Svag" if dist > VECTOR_DISTANCE_WEAK else "☁️ Medel"

            output.append(f"{i+1}. [{quality} Match] (Dist: {dist:.3f})")
            output.append(f"   Fil: {meta.get('filename', 'Unknown')}")
            output.append(f"   Content: \"{content_preview}\"")
            output.append(f"   ID: {uid}")
            output.append("---")

        return "\n".join(output)

    except TimeoutError:
        return "⚠️ VEKTOR-FEL: Databasen är upptagen (ingestion pågår). Försök igen om en stund."
    except (OSError, RuntimeError) as e:
        return f"⚠️ VEKTOR-FEL: {str(e)}"
    finally:
        _log_tool_time("query_vector_memory", _t0)

# --- TOOL 3: LAKE (Metadata) ---

@mcp.tool()
def search_by_date_range(
    start_date: str,
    end_date: str,
    date_field: str = "content"
) -> str:
    """
    Tidssökning – hitta dokument och händelser från en specifik period.

    TRE DATUMFÄLT:
    - "content": När händelsen faktiskt inträffade (default)
    - "ingestion": När filen lades till i systemet
    - "updated": Senaste semantiska uppdatering

    EXEMPEL:
    - Vad hände förra veckan? → parse_relative_date först, sedan denna
    - Alla dokument från Q4 2024 → start="2024-10-01", end="2024-12-31"

    KOMBINERA MED ANDRA VERKTYG:
    1. search_by_date_range → Hitta dokument från perioden
    2. read_document_content → Läs intressanta dokument
    3. query_vector_memory → Hitta relaterade entiteter

    Args:
        start_date: Startdatum (YYYY-MM-DD)
        end_date: Slutdatum (YYYY-MM-DD)
        date_field: "content" (default), "ingestion", eller "updated"
    """
    _t0 = time.monotonic()
    from datetime import datetime

    # Mappa date_field till frontmatter-nyckel
    field_map = {
        "content": "timestamp_content",
        "ingestion": "timestamp_ingestion",
        "updated": "timestamp_updated"
    }

    if date_field not in field_map:
        return f"⚠️ Ogiltigt date_field: '{date_field}'. Använd: content, ingestion, updated"

    timestamp_key = field_map[date_field]

    try:
        # Parsa datum
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    except ValueError as e:
        return f"⚠️ Ogiltigt datumformat: {e}. Använd YYYY-MM-DD"

    if not os.path.exists(_get_lake_path()):
        return f"⚠️ LAKE-FEL: Mappen {LAKE_PATH} finns inte."

    matches = []
    skipped_unknown = 0

    try:
        all_metadata = _lake_cache.get_all()

        for filename, frontmatter in all_metadata.items():
            timestamp_str = frontmatter.get(timestamp_key)

            # Hantera UNKNOWN och None
            if not timestamp_str or timestamp_str == "UNKNOWN":
                skipped_unknown += 1
                continue

            try:
                # Parsa ISO-format (med eller utan tid)
                if 'T' in timestamp_str:
                    file_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    # Ta bort timezone för jämförelse
                    file_dt = file_dt.replace(tzinfo=None)
                else:
                    file_dt = datetime.strptime(timestamp_str[:10], "%Y-%m-%d")

                # Kolla om inom intervall
                if start_dt <= file_dt <= end_dt:
                    source_type = frontmatter.get('source_type', 'Unknown')
                    summary = frontmatter.get('context_summary', '')[:80]
                    matches.append({
                        'filename': filename,
                        'date': file_dt,
                        'source_type': source_type,
                        'summary': summary
                    })

            except (ValueError, TypeError) as e:
                logging.debug(f"Kunde inte parsa datum i {filename}: {e}")
                continue

        # Sortera efter datum
        matches.sort(key=lambda x: x['date'])

        if not matches:
            msg = f"DATUM: Inga träffar för {start_date} - {end_date} (fält: {date_field})"
            if skipped_unknown > 0:
                msg += f"\n⚠️ {skipped_unknown} filer har {timestamp_key}=UNKNOWN och exkluderades"
            return msg

        output = [f"=== DATUM RESULTAT ({len(matches)} träffar) ==="]
        output.append(f"Intervall: {start_date} → {end_date}")
        output.append(f"Fält: {timestamp_key}")
        if skipped_unknown > 0:
            output.append(f"⚠️ {skipped_unknown} filer med UNKNOWN exkluderade")
        output.append("-" * 30)

        for m in matches:
            date_str = m['date'].strftime("%Y-%m-%d %H:%M")
            output.append(f"📄 [{date_str}] {m['filename']}")
            output.append(f"   Typ: {m['source_type']}")
            if m['summary']:
                output.append(f"   {m['summary']}...")

        return "\n".join(output)

    except Exception as e:
        return f"Datumsökning misslyckades: {e}"
    finally:
        _log_tool_time("search_by_date_range", _t0)


@mcp.tool()
def search_lake_metadata(keyword: str, field: str = None) -> str:
    """
    Sök i dokumentens YAML-metadata – hitta filer baserat på taggning.

    SÖKER I FRONTMATTER:
    - source_type: Dokumenttyp — "Slack Log", "Email Thread", "Calendar Event", "Transcript", "Document"
    - document_keywords: AI-extraherade nyckelord (lista)
    - owner: Dokumentets ägare
    - context_summary: AI-genererad sammanfattning
    - relations_summary: AI-genererad sammanfattning av relationer
    - original_filename: Originalfilnamn (t.ex. "Slack_sälj_sales_2026-01-13_uuid.txt")

    EXEMPEL:
    - Alla Slack-loggar: keyword="Slack Log", field="source_type"
    - Alla mail: keyword="Email Thread", field="source_type"
    - Dokument om upphandling: keyword="upphandling", field="document_keywords"
    - Fritextsökning i alla fält: keyword="Besqab" (utan field)

    KOMBINERA MED read_document_content för att läsa matchande filer.
    """
    _t0 = time.monotonic()
    metadata_max_results = SEARCH_CONFIG.get('lake_metadata_max_results', 10)
    matches = []

    try:
        if not os.path.exists(_get_lake_path()):
             return f"⚠️ LAKE-FEL: Mappen {LAKE_PATH} finns inte."

        all_metadata = _lake_cache.get_all()
        keyword_lower = keyword.lower()

        for filename, frontmatter in all_metadata.items():
            found = False
            hit_details = []

            # Söklogik
            for k, v in frontmatter.items():
                # Om användaren specificerat fält, hoppa över andra
                if field and k != field:
                    continue

                # Sök i listor (t.ex. mentions, keywords)
                if isinstance(v, list):
                    for item in v:
                        if keyword_lower in str(item).lower():
                            found = True
                            hit_details.append(f"{k}: ...{item}...")
                # Sök i strängar (t.ex. summary, title)
                elif isinstance(v, str):
                    if keyword_lower in v.lower():
                        found = True
                        hit_details.append(f"{k}: {v[:50]}...")

            if found:
                matches.append(f"📄 {filename} -> [{', '.join(hit_details)}]")
                if len(matches) >= metadata_max_results:
                    break

        if not matches:
            return f"LAKE: Inga metadata-träffar för '{keyword}' (Skannade {len(all_metadata)} filer)."

        output = [f"=== LAKE METADATA ({len(matches)} träffar) ==="]
        output.extend(matches)
        return "\n".join(output)

    except Exception as e:
        return f"Lake-sökning misslyckades: {e}"
    finally:
        _log_tool_time("search_lake_metadata", _t0)


# --- HELPERS: TEMPORAL FILTERING ---

def _in_date_range(timestamp: str, start_date: str = None, end_date: str = None) -> bool:
    """Check if an ISO timestamp prefix falls within a date range.

    Handles both YYYY-MM-DD and YYYY-MM formats.
    UNKNOWN timestamps are excluded when filtering.
    """
    if not timestamp or timestamp == "UNKNOWN":
        return False
    ts = timestamp[:10]  # YYYY-MM-DD or YYYY-MM
    if start_date and ts < start_date:
        return False
    if end_date and ts > end_date:
        return False
    return True


def _format_relation_context(rc_entries: list, start_date: str = None,
                              end_date: str = None, max_entries: int = 3) -> list:
    """Format relation_context entries as output lines, optionally filtered by date."""
    if not rc_entries:
        return []
    if start_date or end_date:
        rc_entries = [e for e in rc_entries if _in_date_range(
            e.get("timestamp", ""), start_date, end_date)]
    lines = []
    for entry in rc_entries[-max_entries:]:
        ts = entry.get("timestamp", "?")
        text = entry.get("text", "")
        if text:
            lines.append(f"      [{ts}] {text}")
    return lines


# --- TOOL 5: RELATIONSHIP EXPLORER ---

@mcp.tool()
def get_neighbor_network(node_id: str, start_date: str = None, end_date: str = None) -> str:
    """
    Kartlägg relationerna kring en entitet – vem/vad är den kopplad till?

    RETURNERAR:
    - Utgående relationer: "Person → ARBETAR_PÅ → Organisation"
    - Inkommande relationer: "Projekt → HAR_DELTAGARE → Person"
    - Relationstyper och riktning

    ANVÄNDNINGSFLÖDE:
    1. search_graph_nodes("Johan") → node_id
    2. get_neighbor_network(node_id) → Se alla kopplingar
    3. get_entity_summary(granne_id) → Djupdyk i intressant koppling

    BRA FÖR:
    - "Vilka projekt är personen inblandad i?"
    - "Vilka personer jobbar med projektet?"
    - "Hur hänger dessa entiteter ihop?"
    """
    _t0 = time.monotonic()
    try:
        with resource_lock("graph", exclusive=False, timeout=10.0):
            graph = GraphService(_get_graph_path(), read_only=True)

            center_node = graph.get_node(node_id)
            if not center_node:
                graph.close()
                return f"Noden '{node_id}' hittades inte."

            out_edges = graph.get_edges_from(node_id)
            in_edges = graph.get_edges_to(node_id)

            neighbor_ids = set()
            for e in out_edges:
                neighbor_ids.add(e['target'])
            for e in in_edges:
                neighbor_ids.add(e['source'])

            neighbor_map = {}
            for nid in neighbor_ids:
                n = graph.get_node(nid)
                if n:
                    props = n.get('properties', {})
                    neighbor_map[nid] = props.get('name', nid)
                else:
                    neighbor_map[nid] = nid

            graph.close()

        c_props = center_node.get('properties', {})
        c_name = c_props.get('name', node_id)

        output = [f"=== NÄTVERK: {c_name} ({center_node['type']}) ==="]
        if start_date or end_date:
            output.append(f"Tidsfilter: {start_date or '...'} → {end_date or '...'}")

        if out_edges:
            output.append("\n--> UTGÅENDE:")
            for e in out_edges:
                target_name = neighbor_map.get(e['target'], e['target'])
                output.append(f"   [{e['type']}] -> {target_name}")
                rc = e.get("properties", {}).get("relation_context", [])
                rc_lines = _format_relation_context(rc, start_date, end_date)
                output.extend(rc_lines)

        if in_edges:
            output.append("\n<-- INKOMMANDE:")
            for e in in_edges:
                source_name = neighbor_map.get(e['source'], e['source'])
                output.append(f"   {source_name} -> [{e['type']}]")
                rc = e.get("properties", {}).get("relation_context", [])
                rc_lines = _format_relation_context(rc, start_date, end_date)
                output.extend(rc_lines)

        if not out_edges and not in_edges:
            output.append("   (Inga kopplingar - Isolerad nod)")

        return "\n".join(output)

    except TimeoutError:
        return "Nätverksutforskning misslyckades: Databasen är upptagen. Försök igen om en stund."
    except Exception as e:
        return f"Nätverksutforskning misslyckades: {e}"
    finally:
        _log_tool_time("get_neighbor_network", _t0)


# --- TOOL 6: ENTITY SUMMARY ---

@mcp.tool()
def get_entity_summary(node_id: str, start_date: str = None, end_date: str = None) -> str:
    """
    Djupdyk i EN entitet – hämtar allt systemet vet om den.

    RETURNERAR:
    - context_summary: Kompakt identitetsbeskrivning
    - relation_context: Kronologiska händelser per relation
    - Relationer: Vilka andra entiteter den är kopplad till
    - Metadata: Typ, alias, properties
    - Konfidens: Hur säker systemet är på informationen

    ANVÄNDNING:
    1. Hitta entitet med search_graph_nodes: "Johan" → node_id="abc123"
    2. Hämta allt om Johan: get_entity_summary(node_id="abc123")

    Perfekt för att svara på "Berätta allt du vet om X".
    """
    _t0 = time.monotonic()
    try:
        with resource_lock("graph", exclusive=False, timeout=10.0):
            graph = GraphService(_get_graph_path(), read_only=True)
            node = graph.get_node(node_id)

            if not node:
                graph.close()
                return f"Noden '{node_id}' hittades inte."

            props = node.get('properties', {})
            name = props.get('name', node_id)
            aliases = node.get('aliases', [])
            # Fetch edges for relation_context display
            out_edges = graph.get_edges_from(node_id)
            in_edges = graph.get_edges_to(node_id)

            # Build neighbor name map
            neighbor_map = {}
            for e in out_edges:
                nid = e['target']
                if nid not in neighbor_map:
                    n = graph.get_node(nid)
                    neighbor_map[nid] = n.get('properties', {}).get('name', nid) if n else nid
            for e in in_edges:
                nid = e['source']
                if nid not in neighbor_map:
                    n = graph.get_node(nid)
                    neighbor_map[nid] = n.get('properties', {}).get('name', nid) if n else nid

            graph.close()

        output = [f"=== SUMMERING: {name} ==="]
        output.append(f"Typ: {node['type']}")
        output.append(f"ID: {node_id}")
        if start_date or end_date:
            output.append(f"Tidsfilter: {start_date or '...'} \u2192 {end_date or '...'}")

        # Identity section (context_summary)
        ctx_summary = props.get('context_summary', '')
        if ctx_summary:
            output.append(f"\n--- IDENTITET ---")
            output.append(ctx_summary)

        output.append(f"\nKonfidens: {props.get('confidence', 'N/A')}")
        output.append(f"Status: {props.get('status', 'N/A')}")

        retrieved = props.get('retrieved_times', 0)
        last_seen = props.get('last_retrieved_at', 'Aldrig')
        output.append(f"Popularitet: {retrieved} visningar | Sist sedd: {last_seen}")

        if aliases:
            output.append(f"Aliases: {', '.join(aliases[:5])}" + (" ..." if len(aliases) > 5 else ""))

        # Relation context from edges
        non_mentions = [e for e in out_edges + in_edges if e.get('type') != 'MENTIONS']
        has_rc = False
        for e in non_mentions:
            rc = e.get("properties", {}).get("relation_context", [])
            rc_lines = _format_relation_context(rc, start_date, end_date, max_entries=5)
            if rc_lines:
                if not has_rc:
                    output.append("\n--- HÄNDELSER (relation_context) ---")
                    has_rc = True
                other_id = e['target'] if e.get('source') == node_id else e['source']
                other_name = neighbor_map.get(other_id, other_id)
                output.append(f"  [{e['type']}] {other_name}:")
                output.extend(rc_lines)

        return "\n".join(output)

    except TimeoutError:
        return "Summering misslyckades: Databasen är upptagen. Försök igen om en stund."
    except Exception as e:
        return f"Summering misslyckades: {e}"
    finally:
        _log_tool_time("get_entity_summary", _t0)


# --- TOOL 7: GRAPH STATISTICS ---

@mcp.tool()
def get_graph_statistics() -> str:
    """
    Hämtar övergripande statistik om kunskapsgrafen.
    Visar antal noder och kanter per typ.
    """
    _t0 = time.monotonic()
    try:
        with resource_lock("graph", exclusive=False, timeout=10.0):
            graph = GraphService(_get_graph_path(), read_only=True)
            stats = graph.get_stats()
            graph.close()

        output = ["=== GRAF STATISTIK ==="]
        output.append(f"Totalt antal noder: {stats['total_nodes']}")
        output.append(f"Totalt antal kanter: {stats['total_edges']}")

        output.append("\n--- Noder per Typ ---")
        for k, v in stats.get('nodes', {}).items():
            output.append(f"  {k}: {v}")

        output.append("\n--- Kanter per Typ ---")
        for k, v in stats.get('edges', {}).items():
            output.append(f"  {k}: {v}")

        return "\n".join(output)

    except TimeoutError:
        return "Kunde inte hämta statistik: Databasen är upptagen. Försök igen om en stund."
    except Exception as e:
        return f"Kunde inte hämta statistik: {e}"
    finally:
        _log_tool_time("get_graph_statistics", _t0)


# --- TOOL 8: RELATIVE DATE PARSER ---

@mcp.tool()
def parse_relative_date(expression: str) -> str:
    """
    Översätt mänskliga tidsuttryck till datum för search_by_date_range.

    EXEMPEL:
    - "förra veckan" → {"start_date": "2025-01-06", "end_date": "2025-01-12"}
    - "igår" → {"start_date": "2025-01-16", "end_date": "2025-01-16"}
    - "senaste månaden" → {...}
    - "3 dagar sedan" → {...}

    ANVÄNDNING:
    1. parse_relative_date("förra veckan")
    2. search_by_date_range(start_date=..., end_date=...)

    Returnerar JSON med start_date och end_date i YYYY-MM-DD format.
    """
    today = datetime.now().date()
    result = {
        "today": today.isoformat(),
        "expression": expression,
        "start_date": None,
        "end_date": None,
        "description": ""
    }

    expr_lower = expression.lower().strip()

    # TODAY
    if expr_lower in ["today", "idag"]:
        result["start_date"] = today.isoformat()
        result["end_date"] = today.isoformat()
        result["description"] = f"Idag ({today.isoformat()})"

    # YESTERDAY
    elif expr_lower in ["yesterday", "igår"]:
        yesterday = today - timedelta(days=1)
        result["start_date"] = yesterday.isoformat()
        result["end_date"] = yesterday.isoformat()
        result["description"] = f"Igår ({yesterday.isoformat()})"

    # THIS WEEK
    elif expr_lower in ["this week", "denna veckan", "den här veckan"]:
        monday = today - timedelta(days=today.weekday())
        result["start_date"] = monday.isoformat()
        result["end_date"] = today.isoformat()
        result["description"] = f"Denna vecka ({monday.isoformat()} - {today.isoformat()})"

    # LAST WEEK
    elif expr_lower in ["last week", "förra veckan"]:
        prev_monday = today - timedelta(days=today.weekday() + 7)
        prev_sunday = prev_monday + timedelta(days=6)
        result["start_date"] = prev_monday.isoformat()
        result["end_date"] = prev_sunday.isoformat()
        result["description"] = f"Förra veckan ({prev_monday.isoformat()} - {prev_sunday.isoformat()})"

    # THIS MONTH
    elif expr_lower in ["this month", "denna månaden", "den här månaden"]:
        first_of_month = today.replace(day=1)
        result["start_date"] = first_of_month.isoformat()
        result["end_date"] = today.isoformat()
        result["description"] = f"Denna månad ({first_of_month.isoformat()} - {today.isoformat()})"

    # LAST MONTH
    elif expr_lower in ["last month", "förra månaden"]:
        first_of_this_month = today.replace(day=1)
        last_day_prev_month = first_of_this_month - timedelta(days=1)
        first_of_prev_month = last_day_prev_month.replace(day=1)
        result["start_date"] = first_of_prev_month.isoformat()
        result["end_date"] = last_day_prev_month.isoformat()
        result["description"] = f"Förra månaden ({first_of_prev_month.isoformat()} - {last_day_prev_month.isoformat()})"

    # X DAYS AGO
    elif match := re.search(r'(\d+)\s*(?:days?|dagar?)\s*(?:ago|sedan)', expr_lower):
        days = int(match.group(1))
        target_date = today - timedelta(days=days)
        result["start_date"] = target_date.isoformat()
        result["end_date"] = target_date.isoformat()
        result["description"] = f"{days} dagar sedan ({target_date.isoformat()})"

    # X WEEKS AGO
    elif match := re.search(r'(\d+)\s*(?:weeks?|veckor?)\s*(?:ago|sedan)', expr_lower):
        weeks = int(match.group(1))
        target_monday = today - timedelta(days=today.weekday() + (weeks * 7))
        target_sunday = target_monday + timedelta(days=6)
        result["start_date"] = target_monday.isoformat()
        result["end_date"] = target_sunday.isoformat()
        result["description"] = f"{weeks} vecka/or sedan ({target_monday.isoformat()} - {target_sunday.isoformat()})"

    # RECENT
    elif expr_lower in ["recent", "recently", "nyligen"]:
        start = today - timedelta(days=7)
        result["start_date"] = start.isoformat()
        result["end_date"] = today.isoformat()
        result["description"] = f"Senaste 7 dagarna ({start.isoformat()} - {today.isoformat()})"

    # FALLBACK
    else:
        result["description"] = f"Okänt uttryck: '{expression}'. Prova 'förra veckan', 'igår', '3 dagar sedan'."

    return json.dumps(result, indent=2, ensure_ascii=False)


# --- TOOL 9: READ DOCUMENT CONTENT (Smart Truncation) ---

def _smart_truncate(content: str, max_length: int, tail_ratio: float = 0.2) -> tuple:
    """
    Intelligent trunkering som bevarar frontmatter + head + tail.
    """
    if len(content) <= max_length:
        return content, False

    frontmatter = ""
    body = content

    if content.startswith('---'):
        end_idx = content.find('\n---', 3)
        if end_idx != -1:
            frontmatter = content[:end_idx + 4]
            body = content[end_idx + 4:].lstrip('\n')

    available = max_length - len(frontmatter) - 100

    if available <= 0:
        return content[:max_length] + "\n\n... [TRUNKERAD]", True

    tail_size = int(available * tail_ratio)
    head_size = available - tail_size

    body_head = body[:head_size].rstrip()
    body_tail = body[-tail_size:].lstrip() if tail_size > 0 else ""

    # Klipp vid radbrytning för renare output
    if len(body_head) > 200:
        last_break = body_head.rfind('\n', len(body_head) - 200)
        if last_break > len(body_head) * 0.7:
            body_head = body_head[:last_break]

    if len(body_tail) > 200:
        first_break = body_tail.find('\n', 0, 200)
        if first_break > 0:
            body_tail = body_tail[first_break + 1:]

    omitted = len(body) - len(body_head) - len(body_tail)
    truncated = f"{frontmatter}\n{body_head}\n\n[... {omitted:,} tecken utelämnade ...]\n\n{body_tail}"

    return truncated, True


@mcp.tool()
def read_document_content(doc_id: str, max_length: int = 8000, section: str = "smart") -> str:
    """
    Läs källdokument – hämta originaltext från Lake.

    SMART TRUNKERING (default):
    - Bevarar YAML frontmatter (metadata)
    - ~80% från dokumentets början
    - ~20% från slutet (fångar org.nr, signaturer, fotnoter)

    LÄSLÄGEN:
    - "smart": Rekommenderat – head + tail med metadata (default)
    - "head": Endast början
    - "tail": Endast slutet
    - "full": Hela dokumentet (varning: kan bli stort!)

    ANVÄNDNING:
    1. Hitta dokument via search_by_date_range eller search_lake_metadata
    2. read_document_content(doc_id="uuid-eller-filnamn")

    BRA FÖR: Verifiera information, hitta exakta citat, förstå kontext.

    Args:
        doc_id: Dokumentets UUID eller filnamn
        max_length: Max antal tecken (default 8000)
        section: "smart" (default), "head", "tail", eller "full"
    """
    _t0 = time.monotonic()
    try:
        # Hitta filen
        filepath = None

        # Försök hitta via UUID i filnamn
        if os.path.exists(_get_lake_path()):
            for f in os.listdir(_get_lake_path()):
                if f.endswith('.md') and doc_id in f:
                    filepath = os.path.join(_get_lake_path(), f)
                    break

        # Fallback: sök i frontmatter
        if not filepath and os.path.exists(_get_lake_path()):
            for f in os.listdir(_get_lake_path()):
                if f.endswith('.md'):
                    full_path = os.path.join(_get_lake_path(), f)
                    fm = _parse_frontmatter(full_path)
                    if fm.get('uuid') == doc_id:
                        filepath = full_path
                        break

        if not filepath:
            return f"DOKUMENT EJ HITTAT: Kunde inte hitta fil för ID '{doc_id}' i Lake."

        # Säkerhetskontroll: verifiera att filepath ligger inom Lake-mappen
        lake_real = os.path.realpath(_get_lake_path())
        file_real = os.path.realpath(filepath)
        if not file_real.startswith(lake_real + os.sep):
            LOGGER.warning(f"Path traversal blockerad: {file_real} utanför {lake_real}")
            return "FEL: Filen ligger utanför Lake-mappen."

        # Läs filen
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            return f"LÄSFEL: Kunde inte läsa {filepath}: {e}"

        # Applicera trunkering
        filename = os.path.basename(filepath)
        full_length = len(content)
        header = f"=== DOKUMENT: {filename} ({full_length:,} tecken) ==="

        if section == "full" or full_length <= max_length:
            return f"{header}\n\n{content}"

        elif section == "head":
            truncated = content[:max_length].rstrip()
            return f"{header}\n[LÄGE: head - visar första {max_length:,} tecken]\n\n{truncated}\n\n... [TRUNKERAD - {full_length - max_length:,} tecken kvar]"

        elif section == "tail":
            truncated = content[-max_length:].lstrip()
            return f"{header}\n[LÄGE: tail - visar sista {max_length:,} tecken]\n\n[TRUNKERAD - {full_length - max_length:,} tecken före detta ...]\n\n{truncated}"

        else:  # "smart"
            truncated, was_truncated = _smart_truncate(content, max_length)
            if was_truncated:
                return f"{header}\n[LÄGE: smart - frontmatter + head + tail bevarade]\n\n{truncated}"
            return f"{header}\n\n{truncated}"

    except Exception as e:
        logging.error(f"read_document_content: Fel för {doc_id}: {e}")
        return f"Dokumentläsning misslyckades: {e}"
    finally:
        _log_tool_time("read_document_content", _t0)


# --- TOOL 10: INGEST CONTENT ---

@mcp.tool()
def ingest_content(
    filename: str,
    content: str,
    source: str = "mcp_ingest",
    metadata: dict = None
) -> str:
    """
    Skapar ett nytt dokument direkt från innehåll och ingesterar det.

    Perfekt för AI-genererat innehåll som mötesanteckningar, sammanfattningar,
    eller annat material som skapas i konversationen.

    ANVÄNDNING:
    - ingest_content("mote_2026-01-18.md", "# Mötesanteckningar\\n\\nVi diskuterade...")
    - ingest_content("sammanfattning.md", content, source="claude", metadata={"topic": "projekt"})

    INNEHÅLLSFORMAT:
    - Ren markdown fungerar bra
    - YAML frontmatter (---) är valfritt - systemet lägger till egen metadata

    Args:
        filename: Önskat filnamn (t.ex. "anteckningar.md")
        content: Dokumentets innehåll (markdown)
        source: Källmarkering för spårbarhet (default: "mcp_ingest")
        metadata: Extra metadata att inkludera i frontmatter (valfritt)

    Returnerar: Status med UUID och sökväg.
    """
    try:
        # Validera input
        if not filename:
            return "❌ FEL: Filnamn krävs"

        if not content or len(content.strip()) < 10:
            return "❌ FEL: Innehållet är för kort (minst 10 tecken)"

        # Hantera filändelse
        name_part, ext = os.path.splitext(filename)
        if not ext:
            ext = ".md"  # Default till markdown
            filename = f"{filename}{ext}"

        valid_extensions = ['.md', '.txt']
        if ext.lower() not in valid_extensions:
            return f"❌ FEL: Endast {', '.join(valid_extensions)} stöds för innehållsingestion"

        # Generera UUID och filnamn
        file_uuid = str(uuid.uuid4())
        new_filename = f"{name_part}_{file_uuid}{ext}"

        # Bygg innehåll med header om det inte redan har frontmatter
        final_content = content
        if not content.strip().startswith('---'):
            # Lägg till minimal header för spårbarhet
            header_lines = [
                "---",
                f"source: {source}",
                f"created_via: mcp_ingest",
                f"created_at: {datetime.now().isoformat()}",
            ]
            if metadata:
                for key, value in metadata.items():
                    if isinstance(value, (str, int, float, bool)):
                        header_lines.append(f"{key}: {value}")
                    elif isinstance(value, list):
                        header_lines.append(f"{key}: {json.dumps(value, ensure_ascii=False)}")
            header_lines.append("---")
            header_lines.append("")
            final_content = "\n".join(header_lines) + content

        # Säkerställ att målmappen finns
        os.makedirs(AI_GENERATED_PATH, exist_ok=True)

        # Skriv filen
        target_path = os.path.join(AI_GENERATED_PATH, new_filename)
        with open(target_path, 'w', encoding='utf-8') as f:
            f.write(final_content)

        logging.info(f"ingest_content: Skapade {new_filename} ({len(content)} tecken)")

        return (
            f"✅ Innehåll ingesterat!\n"
            f"  Filnamn: {new_filename}\n"
            f"  UUID: {file_uuid}\n"
            f"  Storlek: {len(content):,} tecken\n"
            f"  Källa: {source}\n"
            f"  Placerad i: {target_path}\n"
            f"  Status: Väntar på Ingestion Engine"
        )

    except Exception as e:
        logging.error(f"ingest_content: Fel vid skapande av {filename}: {e}")
        return f"❌ FEL: Kunde inte skapa dokumentet: {e}"


# --- TOOL 11-14: MEETING TRANSCRIBER ---

@mcp.tool()
def watch_meeting(action: str = "check") -> str:
    """
    Bevaka möte och hämta nya transkript-chunks.

    VIKTIGT FÖR CLAUDE: När denna tool returnerar instruktioner att
    "anropa watch_meeting igen", GÖR DET DIREKT i nästa svar.
    Detta skapar en polling-loop som ersätter sampling.

    Args:
        action: "start" | "check" | "stop"
               - start: Börja bevaka, rensa state
               - check: Hämta nya chunks (default)
               - stop: Avsluta bevakning
    """
    logging.info(f"watch_meeting({action})")

    if action == "stop":
        state = _get_meeting_watch_state()
        chunks_processed = state.get("chunks_processed", 0)
        _set_meeting_watch_state(active=False)
        _stop_transcriber_process()
        return f"""## Mötesbevakning avslutad

Totalt analyserade chunks: {chunks_processed}

Bevakningen är stoppad. Säg "bevaka mötet" för att starta igen.
"""

    if action == "start":
        # Rensa processed-lista för ny session
        if MEETING_PROCESSED_FILE.exists():
            MEETING_PROCESSED_FILE.unlink()

        # Starta transcriber
        started = _start_transcriber_process()

        _set_meeting_watch_state(
            active=True,
            started_at=datetime.now().isoformat(),
            chunks_processed=0
        )

        if started:
            logging.info("Meeting watch started with transcriber")
        else:
            logging.warning("Meeting watch started but transcriber failed to start")

        # Fall through to check

    # Hämta state
    state = _get_meeting_watch_state()
    if not state.get("active") and action != "start":
        return """## Bevakning ej aktiv

Säg "bevaka mötet" för att starta mötesbevakning.
Jag kommer då loopa och hämta nya transkript-chunks.
"""

    # Hämta olästa chunks
    unread = _get_meeting_unread_chunks()

    if unread:
        # Batcha ALLA olästa chunks i ett svar
        all_chunks_text = []
        for chunk_file, chunk_content in unread:
            _mark_meeting_chunk_processed(chunk_file)
            all_chunks_text.append(f"### [{chunk_file}]\n\n{chunk_content}")

        # Uppdatera state
        state["chunks_processed"] = state.get("chunks_processed", 0) + len(unread)
        _set_meeting_watch_state(**state)

        logging.info(f"Returning {len(unread)} chunks")

        combined = "\n\n---\n\n".join(all_chunks_text)

        return f"""## Ny mötestext ({len(unread)} chunks)

{combined}

---
**ANALYSERA** texten ovan. Sök i MyMemory efter personer, företag, projekt.
Ge en kort whisper om du hittar relevant kontext.
**ANROPA SEDAN** `watch_meeting("check")` igen.
"""

    else:
        # Ingen ny text - kort svar
        return "[Väntar på mötestext...]\n\n→ Anropa `watch_meeting(\"check\")` igen."


@mcp.tool()
def add_test_chunk(text: str, context: str = "Testmöte") -> str:
    """
    Lägg till en test-chunk manuellt (för debugging).

    Args:
        text: Mötestext att lägga till
        context: Möteskontext (visas som header)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    chunk_file = MEETING_CHUNKS_DIR / f"{timestamp}.txt"

    content = f"""MÖTESKONTEXT: {context}
TIDPUNKT: {datetime.now().strftime("%Y-%m-%d %H:%M")}

---

{text}
"""

    chunk_file.write_text(content)
    logging.info(f"Added test chunk: {chunk_file.name}")

    return f"Chunk tillagd: {chunk_file.name}\n\nAnropa watch_meeting('check') för att läsa den."


@mcp.tool()
def get_buffer_status() -> str:
    """Visa status för chunk-buffern."""
    global _transcriber_process

    state = _get_meeting_watch_state()
    processed = _get_meeting_processed_chunks()
    all_chunks = list(MEETING_CHUNKS_DIR.glob("*.txt"))
    unread = [c for c in all_chunks if c.name not in processed]

    # Kolla transcriber-status
    transcriber_status = "Ej startad"
    if _transcriber_process:
        if _transcriber_process.poll() is None:
            transcriber_status = f"Kör (PID: {_transcriber_process.pid})"
        else:
            transcriber_status = f"Avslutad (kod: {_transcriber_process.returncode})"

    status_lines = [
        "## Meeting Buffer Status",
        "",
        f"**Bevakning aktiv:** {'Ja' if state.get('active') else 'Nej'}",
        f"**Transcriber:** {transcriber_status}",
        f"**Startad:** {state.get('started_at', '-')}",
        f"**Chunks processade:** {state.get('chunks_processed', 0)}",
        "",
        f"**Totalt chunks i buffer:** {len(all_chunks)}",
        f"**Olästa chunks:** {len(unread)}",
        f"**Lästa chunks:** {len(processed)}",
        "",
        "**Senaste chunks:**",
    ]

    for chunk in sorted(all_chunks, reverse=True)[:5]:
        mark = "[ ]" if chunk.name not in processed else "[x]"
        status_lines.append(f"  {mark} {chunk.name}")

    return "\n".join(status_lines)


@mcp.tool()
def clear_buffer() -> str:
    """Rensa hela chunk-buffern (för debugging)."""
    # Ta bort alla chunks
    for chunk in MEETING_CHUNKS_DIR.glob("*.txt"):
        chunk.unlink()

    # Rensa processed-lista
    if MEETING_PROCESSED_FILE.exists():
        MEETING_PROCESSED_FILE.unlink()

    # Återställ state
    _set_meeting_watch_state(active=False, started_at=None, chunks_processed=0)

    logging.info("Meeting buffer cleared")
    return "Buffer rensad. Alla chunks borttagna."


def _is_broken_pipe(exc):
    """Rekursivt kolla om BrokenPipeError finns i exception chain."""
    if isinstance(exc, BrokenPipeError):
        return True
    if isinstance(exc, BaseExceptionGroup):
        return any(_is_broken_pipe(e) for e in exc.exceptions)
    return False


if __name__ == "__main__":
    try:
        mcp.run()
    except BaseException as e:
        # BrokenPipeError = parent dog, normal shutdown
        # Se poc/process/mcp_shutdown_poc.py
        if _is_broken_pipe(e):
            os._exit(0)
        logging.critical(f"Server Crash: {e}")
        sys.exit(1)