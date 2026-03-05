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
_log_file = os.path.expanduser('~/Library/Logs/MyMemory/my_mem_system.log')
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
from services.utils.schema_validator import SchemaValidator

try:
    CONFIG = get_config()
except FileNotFoundError as e:
    logging.error(f"Config load failed: {e}")
    CONFIG = {}
PATHS = CONFIG.get('paths', {})
SEARCH_CONFIG = CONFIG.get('search', {})

# System date override (för demo-index med fiktiva datum)
_system_date_override = None
_raw_date_override = CONFIG.get('validation', {}).get('system_date_override')
if _raw_date_override:
    try:
        _system_date_override = datetime.fromisoformat(str(_raw_date_override)).date()
    except (ValueError, TypeError):
        pass

_SCHEMA_VALIDATOR = None

def _get_schema_validator() -> SchemaValidator:
    global _SCHEMA_VALIDATOR
    if _SCHEMA_VALIDATOR is None:
        _SCHEMA_VALIDATOR = SchemaValidator()
    return _SCHEMA_VALIDATOR

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

def _get_vector_path():
    return _current_paths["vector"]

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
        f"✅ Bytte till index ({_current_paths['label']}):\n"
        f"  Graf: {graph_path}\n"
        f"  Lake: {lake_path}\n"
        f"  Vektor: {vector_path}\n"
        f"\nAlla sökverktyg pekar nu på detta index."
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
    Sök specifika entiteter i grafen via namn, alias, e-post eller ID.

    SÖKMETOD: ILIKE-matchning (%query%) — söker i id, aliases och hela
    properties-objektet (name, email, context_summary, etc.). Hittar
    delmatchningar, t.ex. "Johan" matchar "Johan Ekman".

    ANVÄND FÖR:
    - Hitta en specifik person: query="Johan"
    - Kolla om en organisation finns: query="Acme AB", node_type="Organization"
    - Söka på e-post: query="johan@example.com"
    - Söka i identitetsbeskrivning: query="projektledare"

    FILTRERA PÅ TYP (node_type):
    Person, Organization, Group, Project, Event, Roles, Source

    SKILLNAD MOT query_vector_memory:
    - search_graph_nodes = "Finns noden X?" (textsökning i namn/properties)
    - query_vector_memory = "Vem jobbar med AI-projekt?" (semantisk sökning)

    TIPS: Börja här för att hitta rätt node_id,
    använd sedan get_entity_summary eller get_neighbor_network för detaljer.
    """
    _t0 = time.monotonic()
    try:
        # Validera node_type mot schemat om angivet
        if node_type:
            valid_types = _get_schema_validator().get_node_types()
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
            confidence = props.get('confidence', 'N/A')
            ctx_summary = props.get('context_summary', '')
            ctx_str = f"Identitet: {ctx_summary}" if ctx_summary else "Identitet: SAKNAS"
            alias_str = f"Aliases: {len(aliases)}" if aliases else ""

            output.append(f"• [{n_type}] {name}")
            output.append(f"  ID: {node_id}")
            output.append(f"  Confidence: {confidence}")
            if alias_str: output.append(f"  {alias_str}")
            output.append(f"  {ctx_str}")

        return "\n".join(output)
    except TimeoutError:
        # Vektor-fallback: grafens ILIKE kan approximeras via semantisk sökning
        fallback_msg = "Grafen är upptagen (ingestion/dreamer pågår).\nFörsöker vektor-sökning som fallback..."
        try:
            with vector_scope(exclusive=False, timeout=10.0, db_path=_get_vector_path()) as vs:
                results = vs.search(query_text=query, limit=GRAPH_SEARCH_LIMIT)
            if results:
                lines = [f"=== VEKTOR-FALLBACK ({len(results)} träffar, graf upptagen) ==="]
                for item in results:
                    meta = item['metadata']
                    content = item['document'].replace('\n', ' ')[:100]
                    lines.append(f"• {meta.get('filename', item['id'])}: {content}...")
                fallback_msg += "\n" + "\n".join(lines)
            else:
                fallback_msg += "\nInga vektor-träffar heller."
            fallback_msg += "\n\nGraf-verktyg: Försök igen om 30 sekunder."
        except Exception:
            fallback_msg += "\nVektor-fallback misslyckades också. Försök igen om 30 sekunder."
        return fallback_msg
    except Exception as e:
        return f"Grafsökning misslyckades: {e}"
    finally:
        _log_tool_time("search_graph_nodes", _t0)

# --- TOOL 2: VECTOR (Semantics) ---

@mcp.tool()
def query_vector_memory(query_text: str, n_results: int = 5, node_type: str = None) -> str:
    """
    Semantisk sökning – hittar dokument OCH graf-entiteter baserat på MENING.

    Söker bland:
    - Dokument (mail, Slack, transkript, etc.) — matchas via innehåll
    - Graf-noder (Person, Organization, etc.) — matchas via context_summary
      och relationsdata (t.ex. "vem jobbar på Digitalist" hittar personer
      via deras BELONGS_TO-relationer inbakade i vektorn)

    RESULTATTYPER (skilj på source i metadata):
    - source="graph_node" → En entitet i grafen
    - Annars → Ett dokument i Lake

    SÖK PÅ KONCEPT, INTE NAMN:
    ✅ "vem arbetar med upphandlingar"
    ✅ "projekt med faktureringsproblem"
    ✅ "personer som haft kundmöten"
    ✅ "vem jobbar på Digitalist" (hittar via relationsdata)
    ❌ "Johan" (använd search_graph_nodes för exakta namn)

    FILTRERA PÅ NODTYP (node_type):
    Person, Organization, Group, Project, Event, Roles
    Filtrerar till enbart graf-noder av angiven typ.

    SKILLNAD MOT search_graph_nodes:
    - search_graph_nodes = "Finns noden X?" (textsökning)
    - query_vector_memory = "Vem/vad matchar konceptet Y?" (semantisk)
    """
    _t0 = time.monotonic()
    try:
        # Bygg where-filter
        where_filter = None
        if node_type:
            sv = _get_schema_validator()
            doc_type = sv.get_document_node_type()
            valid_types = sv.get_node_types() - {doc_type}
            if node_type not in valid_types:
                return f"Ogiltig node_type: '{node_type}'. Giltiga: {', '.join(sorted(valid_types))}"
            where_filter = {"$and": [{"type": node_type}, {"source": "graph_node"}]}

        with vector_scope(exclusive=False, timeout=10.0, db_path=_get_vector_path()) as vs:
            results = vs.search(query_text=query_text, limit=n_results, where=where_filter)
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

    3-TIMESTAMP-SYSTEMET:
    - "content" (default): När innehållet faktiskt hände (möte, mail skickades)
    - "ingestion": När filen lades till i systemet (indexeringstidpunkt)
    - "updated": Senaste förädling av Enrichment/Dreamer

    DOKUMENTTYPER (source_type i resultat):
    Document, Slack Log, Email Thread, Calendar Event, Transcript

    EXEMPEL:
    - Vad hände förra veckan? → parse_relative_date först, sedan denna
    - Alla dokument från Q4 2024 → start="2024-10-01", end="2024-12-31"
    - Nyligen indexerade filer → date_field="ingestion"

    KOMBINERA MED ANDRA VERKTYG:
    1. search_by_date_range → Hitta dokument från perioden
    2. read_document_content → Läs intressanta dokument
    3. get_source_connections(doc_id=...) → Vilka entiteter nämns i dokumentet

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
                        'summary': summary  # noqa: SD — local dict key, not schema property
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
            if m['summary']:  # noqa: SD — local dict key, not schema property
                output.append(f"   {m['summary']}...")  # noqa: SD — local dict key

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
    - source_type: "Document", "Slack Log", "Email Thread", "Calendar Event", "Transcript"
    - document_keywords: AI-extraherade nyckelord (lista)
    - owner: Dokumentets ägare
    - context_summary: AI-genererad sammanfattning
    - relations_summary: AI-genererad sammanfattning av relationer
    - original_filename: Originalfilnamn

    EXEMPEL:
    - Alla Slack-loggar: keyword="Slack Log", field="source_type"
    - Alla mail: keyword="Email Thread", field="source_type"
    - Alla transkript: keyword="Transcript", field="source_type"
    - Dokument om upphandling: keyword="upphandling", field="document_keywords"
    - Fritextsökning i alla fält: keyword="Besqab" (utan field)

    KOMBINERA MED:
    - read_document_content(doc_id) för att läsa matchande filer
    - get_source_connections(doc_id=...) för att se vilka entiteter som nämns
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
    - Utgående relationer med confidence och relation_context
    - Inkommande relationer med confidence och relation_context
    - relation_context: Kronologiska händelser per relation (vad som sagts/hänt)

    RELATIONSTYPER:
    MENTIONS (Source→entitet), ATTENDED (Person→Event),
    BELONGS_TO (Person/Group→Organization/Group),
    HAS_BUSINESS_RELATION (Organization→Organization),
    WORKS_ON (Person→Project), HAS_ROLE (Person→Roles)

    TIDSFILTRERING (start_date/end_date):
    Filtrerar relation_context-händelser till angiven period.

    ANVÄNDNINGSFLÖDE:
    1. search_graph_nodes("Johan") → node_id
    2. get_neighbor_network(node_id) → Se alla kopplingar
    3. get_entity_summary(granne_id) → Djupdyk i intressant koppling
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
                edge_conf = e.get("properties", {}).get("confidence", "")
                conf_str = f" (conf: {edge_conf})" if edge_conf else ""
                output.append(f"   [{e['type']}] -> {target_name}{conf_str}")
                rc = e.get("properties", {}).get("relation_context", [])
                rc_lines = _format_relation_context(rc, start_date, end_date)
                output.extend(rc_lines)

        if in_edges:
            output.append("\n<-- INKOMMANDE:")
            for e in in_edges:
                source_name = neighbor_map.get(e['source'], e['source'])
                edge_conf = e.get("properties", {}).get("confidence", "")
                conf_str = f" (conf: {edge_conf})" if edge_conf else ""
                output.append(f"   {source_name} -> [{e['type']}]{conf_str}")
                rc = e.get("properties", {}).get("relation_context", [])
                rc_lines = _format_relation_context(rc, start_date, end_date)
                output.extend(rc_lines)

        if not out_edges and not in_edges:
            output.append("   (Inga kopplingar - Isolerad nod)")

        return "\n".join(output)

    except TimeoutError:
        return "Nätverksutforskning misslyckades: Grafen är upptagen (ingestion/dreamer pågår). Försök igen om 30 sekunder."
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
    - context_summary: AI-genererad identitetsbeskrivning (vem/vad ÄR detta)
    - Confidence: Systemets säkerhet (0.0-1.0, >0.8 = hög, <0.5 = låg)
    - relation_context: Kronologiska händelser per relation (vad som hänt)
    - Relationer: Vilka andra entiteter den är kopplad till (exkl. MENTIONS)
    - Metadata: Typ, alias, popularitet

    TIDSFILTRERING (start_date/end_date):
    Filtrerar relation_context-händelser till angiven period.
    Format: YYYY-MM-DD

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
        source_edges = _get_schema_validator().get_source_edge_types()
        non_mentions = [e for e in out_edges + in_edges if e.get('type') not in source_edges]
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
        return "Summering misslyckades: Grafen är upptagen (ingestion/dreamer pågår). Försök igen om 30 sekunder."
    except Exception as e:
        return f"Summering misslyckades: {e}"
    finally:
        _log_tool_time("get_entity_summary", _t0)


# --- TOOL 7: GRAPH STATISTICS ---

@mcp.tool()
def get_graph_statistics() -> str:
    """
    Hämtar övergripande statistik och datakvalitet för kunskapsgrafen.

    RETURNERAR:
    - Antal noder och kanter per typ
    - Datakvalitet: context_summary-täckning per nodtyp
    - Confidence-fördelning per nodtyp (låg/medel/hög + genomsnitt)
    - Antal orphan-noder (utan kopplingar)
    - Senaste Dreamer-körning
    """
    _t0 = time.monotonic()
    try:
        with resource_lock("graph", exclusive=False, timeout=10.0):
            graph = GraphService(_get_graph_path(), read_only=True)
            stats = graph.get_stats()
            quality = graph.get_quality_stats()
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

        # Datakvalitet
        output.append("\n--- Datakvalitet ---")
        output.append("Context Summary:")
        for ntype, cov in quality.get('context_summary_coverage', {}).items():
            output.append(f"  {ntype}: {cov['with_summary']}/{cov['total']} ({cov['pct']:.0f}%)")

        output.append("\nConfidence:")
        for ntype, dist in quality.get('confidence_distribution', {}).items():
            output.append(f"  {ntype}: avg {dist['avg']:.2f} ({dist['low']} låg, {dist['medium']} medel, {dist['high']} hög)")

        output.append(f"\nOrphaner: {quality.get('orphan_count', 'N/A')} noder utan kopplingar")
        output.append(f"Senaste Dreamer-körning: {quality.get('last_dreamer_run', 'N/A')}")

        return "\n".join(output)

    except TimeoutError:
        return "Kunde inte hämta statistik: Databasen är upptagen (ingestion/dreamer pågår). Försök igen om 30 sekunder."
    except Exception as e:
        return f"Kunde inte hämta statistik: {e}"
    finally:
        _log_tool_time("get_graph_statistics", _t0)


# --- TOOL 7b: SYSTEM HEALTH ---

@mcp.tool()
def get_system_health() -> str:
    """
    Visa systemets hälsa och status.

    RETURNERAR:
    - Datatillgänglighet (graf, vektor, Lake)
    - Antal noder, kanter, dokument, vektorer
    - Senaste Dreamer-körning
    - Andel noder som aldrig förädlats

    Användbart för felsökning och överblick.
    """
    _t0 = time.monotonic()
    output = ["=== SYSTEMHÄLSA ==="]

    # Graf
    try:
        with resource_lock("graph", exclusive=False, timeout=10.0):
            graph = GraphService(_get_graph_path(), read_only=True)
            health = graph.get_system_health()
            graph.close()
        output.append(f"\nGraf: Tillgänglig")
        output.append(f"  Noder: {health.get('total_nodes', '?')}")
        output.append(f"  Kanter: {health.get('total_edges', '?')}")
        output.append(f"  Senaste Dreamer: {health.get('last_dreamer_run', 'N/A')}")
        output.append(f"  Aldrig förädlade: {health.get('nodes_never_refined', '?')} noder")
    except TimeoutError:
        output.append(f"\nGraf: Upptagen (ingestion/dreamer pågår)")
    except Exception as e:
        output.append(f"\nGraf: FEL ({e})")

    # Lake
    try:
        all_docs = _lake_cache.get_all()
        output.append(f"\nLake: Tillgänglig")
        output.append(f"  Dokument: {len(all_docs)}")
    except Exception as e:
        output.append(f"\nLake: FEL ({e})")

    # Vektor
    try:
        with vector_scope(exclusive=False, timeout=10.0, db_path=_get_vector_path()) as vs:
            count = vs.count()
        output.append(f"\nVektor: Tillgänglig")
        output.append(f"  Vektorer: {count}")
    except TimeoutError:
        output.append(f"\nVektor: Upptagen")
    except Exception as e:
        output.append(f"\nVektor: FEL ({e})")

    output.append(f"\nAktivt index: {_current_paths['label']}")

    return "\n".join(output)


# --- TOOL 7c: DATA MODEL ---

@mcp.tool()
def get_data_model() -> str:
    """
    Visa systemets datamodell — nodtyper, relationer och källprofiler.

    Användbar för att förstå:
    - Vilka typer av entiteter som finns (Person, Organization, etc.)
    - Hur de relaterar till varandra (BELONGS_TO, WORKS_ON, etc.)
    - Hur olika källtyper (Email, Slack, Transcript) tolkas

    Anropa detta verktyg FÖRST om du är osäker på hur systemet fungerar
    eller vilka sökstrategier som är mest effektiva.
    """
    _t0 = time.monotonic()
    try:
        schema = _get_schema_validator().get_llm_readable_schema()

        output = [f"=== DATAMODELL (schema v{schema.get('schema_version', '?')}) ==="]

        # Nodtyper
        output.append("\n--- NODTYPER ---")
        for nt in schema.get('node_types', []):
            output.append(f"  {nt['type']}: {nt.get('description', '')}")

        # Relationer
        output.append("\n--- RELATIONER ---")
        for et in schema.get('edge_types', []):
            src = ', '.join(et.get('source_types', []))
            tgt = ', '.join(et.get('target_types', []))
            desc = et.get('description') or ''
            output.append(f"  {et['type']}: {src} → {tgt}")
            if desc:
                output.append(f"    {desc}")

        # Källprofiler
        output.append("\n--- KÄLLPROFILER ---")
        for sp in schema.get('source_type_profiles', []):
            creates = ', '.join(sp.get('allow_create', [])) or 'Inga (LINK-only)'
            output.append(f"  {sp['type']}: {sp.get('description', '')}")
            output.append(f"    Kan skapa: {creates}")

        # Söktips
        output.append("\n--- SÖKTIPS ---")
        output.append("• Specifik person/org → search_graph_nodes(\"namn\")")
        output.append("• Koncept/roll/ämne → query_vector_memory(\"beskrivning\")")
        output.append("• Tidsperiod → parse_relative_date + search_by_date_range")
        output.append("• Djupdyk i entitet → get_entity_summary(node_id)")
        output.append("• Kartlägg relationer → get_neighbor_network(node_id)")
        output.append("• Läs källdokument → search_lake_metadata + read_document_content")
        output.append("• Dokument↔entiteter → get_source_connections(entity_id/doc_id)")

        return "\n".join(output)
    except Exception as e:
        return f"Kunde inte hämta datamodell: {e}"
    finally:
        _log_tool_time("get_data_model", _t0)


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
    today = _system_date_override or datetime.now().date()
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


# --- TOOL 9b: SOURCE CONNECTIONS ---

@mcp.tool()
def get_source_connections(entity_id: str = None, doc_id: str = None) -> str:
    """
    Navigera mellan dokument och entiteter via MENTIONS-kanter.

    TVÅ RIKTNINGAR:
    - entity_id → "Vilka dokument nämner denna person/organisation?"
    - doc_id → "Vilka entiteter nämns i detta dokument?"

    Exakt en av entity_id eller doc_id måste anges.

    ANVÄNDNINGSFLÖDE:
    1. search_graph_nodes("Kajsa") → node_id
    2. get_source_connections(entity_id=node_id) → Lista dokument
    3. read_document_content(doc_id) → Läs dokumentet

    ELLER:
    1. search_by_date_range(...) → filnamn
    2. get_source_connections(doc_id="filnamn-uuid") → Vilka entiteter nämns
    3. get_entity_summary(entity_id) → Djupdyk

    Args:
        entity_id: Node-ID för entiteten (Person, Organization, etc.)
        doc_id: Filnamn eller UUID för dokumentet (matchar Source-nodens uuid-property)
    """
    _t0 = time.monotonic()
    try:
        # Validera att exakt en parameter anges
        if entity_id and doc_id:
            return "FEL: Ange antingen entity_id ELLER doc_id, inte båda."
        if not entity_id and not doc_id:
            return "FEL: Ange entity_id eller doc_id."

        sv = _get_schema_validator()
        source_edge_types = sv.get_source_edge_types()
        doc_node_type = sv.get_document_node_type()

        with resource_lock("graph", exclusive=False, timeout=10.0):
            graph = GraphService(_get_graph_path(), read_only=True)

            if entity_id:
                # Riktning: entitet → vilka dokument nämner den?
                entity_node = graph.get_node(entity_id)
                if not entity_node:
                    graph.close()
                    return f"Noden '{entity_id}' hittades inte."

                in_edges = graph.get_edges_to(entity_id)
                mentions = [e for e in in_edges if e.get('type') in source_edge_types]

                # Hämta Source-noders properties
                sources = []
                for e in mentions:
                    source_node = graph.get_node(e['source'])
                    if source_node:
                        s_props = source_node.get('properties', {})
                        edge_conf = e.get('properties', {}).get('confidence', '')
                        sources.append({
                            'id': e['source'],
                            'title': s_props.get('title', e['source']),
                            'source_type': s_props.get('source_type', ''),
                            'confidence': edge_conf
                        })

                graph.close()

                e_name = entity_node.get('properties', {}).get('name', entity_id)
                output = [f"=== DOKUMENT SOM NÄMNER: {e_name} ({len(sources)} träffar) ==="]

                if not sources:
                    output.append("Inga dokument hittade via Source-kanter.")
                    return "\n".join(output)

                for s in sources:
                    conf_str = f" (conf: {s['confidence']})" if s['confidence'] else ""
                    output.append(f"  [{s['source_type']}] {s['title']}{conf_str}")
                    output.append(f"    ID: {s['id']}")

                return "\n".join(output)

            else:
                # Riktning: dokument → vilka entiteter nämns?
                # Hitta Source-noden via uuid-property
                sql = "SELECT id, properties FROM nodes WHERE type = ? AND (id ILIKE ? OR properties ILIKE ?)"
                params = [doc_node_type, f"%{doc_id}%", f"%{doc_id}%"]
                rows = graph.conn.execute(sql, params).fetchall()

                if not rows:
                    graph.close()
                    return f"Ingen {doc_node_type}-nod hittades för '{doc_id}'."

                source_node_id = rows[0][0]
                source_props = json.loads(rows[0][1]) if rows[0][1] else {}

                # Hämta Source→entitet-kanter
                out_edges = graph.get_edges_from(source_node_id)
                mentions = [e for e in out_edges if e.get('type') in source_edge_types]

                # Hämta target-noder
                targets = []
                for e in mentions:
                    target_node = graph.get_node(e['target'])
                    if target_node:
                        t_props = target_node.get('properties', {})
                        edge_conf = e.get('properties', {}).get('confidence', '')
                        targets.append({
                            'id': e['target'],
                            'type': target_node['type'],
                            'name': t_props.get('name', e['target']),
                            'confidence': edge_conf
                        })

                graph.close()

                title = source_props.get('title', source_node_id)
                output = [f"=== ENTITETER I: {title} ({len(targets)} träffar) ==="]

                if not targets:
                    output.append("Inga entiteter hittade via MENTIONS-kanter.")
                    return "\n".join(output)

                for t in targets:
                    conf_str = f" (conf: {t['confidence']})" if t['confidence'] else ""
                    output.append(f"  [{t['type']}] {t['name']}{conf_str}")
                    output.append(f"    ID: {t['id']}")

                return "\n".join(output)

    except TimeoutError:
        return "Source-navigering misslyckades: Databasen är upptagen (ingestion/dreamer pågår). Försök igen om 30 sekunder."
    except Exception as e:
        return f"Source-navigering misslyckades: {e}"
    finally:
        _log_tool_time("get_source_connections", _t0)


# --- TOOL 10: INGEST CONTENT ---

@mcp.tool()
def ingest_content(
    filename: str,
    content: str,
    source: str = "mcp_ingest",
    metadata: dict = None,
    unit_id: str = None
) -> str:
    """
    Skapar ett nytt dokument direkt från innehåll och ingesterar det.
    Om unit_id anges uppdateras ett befintligt dokument istället.

    Perfekt för AI-genererat innehåll som mötesanteckningar, sammanfattningar,
    eller annat material som skapas i konversationen.

    ANVÄNDNING:
    - ingest_content("mote_2026-01-18.md", "# Mötesanteckningar\\n\\nVi diskuterade...")
    - ingest_content("sammanfattning.md", content, source="claude", metadata={"topic": "projekt"})
    - ingest_content("rapport.md", updated_content, unit_id="550e8400-...") ← uppdaterar befintligt

    INNEHÅLLSFORMAT:
    - Ren markdown fungerar bra
    - YAML frontmatter (---) är valfritt - systemet lägger till egen metadata

    Args:
        filename: Önskat filnamn (t.ex. "anteckningar.md")
        content: Dokumentets innehåll (markdown)
        source: Källmarkering för spårbarhet (default: "mcp_ingest")
        metadata: Extra metadata att inkludera i frontmatter (valfritt)
        unit_id: UUID för befintligt dokument att uppdatera (valfritt). Om angivet skrivs
                 befintlig Asset-fil över och Ingestion Engine triggar re-ingest automatiskt.

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

        # --- Uppdatering av befintligt dokument ---
        if unit_id:
            target_path = _find_asset_by_unit_id(unit_id)
            if not target_path:
                return f"❌ FEL: Inget dokument med unit_id '{unit_id}' hittades"

            # Bygg innehåll med header
            final_content = _build_content_with_header(content, source, metadata)

            with open(target_path, 'w', encoding='utf-8') as f:
                f.write(final_content)

            logging.info(f"ingest_content: Uppdaterade {os.path.basename(target_path)} ({len(content)} tecken)")

            return (
                f"✅ Dokument uppdaterat!\n"
                f"  Filnamn: {os.path.basename(target_path)}\n"
                f"  UUID: {unit_id}\n"
                f"  Storlek: {len(content):,} tecken\n"
                f"  Källa: {source}\n"
                f"  Status: Väntar på re-ingest via Ingestion Engine"
            )

        # --- Nytt dokument ---
        file_uuid = str(uuid.uuid4())
        new_filename = f"{name_part}_{file_uuid}{ext}"

        final_content = _build_content_with_header(content, source, metadata)

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


def _build_content_with_header(content, source, metadata):
    """Bygg innehåll med frontmatter-header om det saknas."""
    if content.strip().startswith('---'):
        return content
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
    return "\n".join(header_lines) + content


def _find_asset_by_unit_id(unit_id):
    """Hitta befintlig Asset-fil via Lake-frontmatter."""
    lake_path = _get_lake_path()
    if not os.path.isdir(lake_path):
        return None

    # Sök Lake-fil med matchande UUID
    for f in os.listdir(lake_path):
        if unit_id in f and f.endswith('.md'):
            full_path = os.path.join(lake_path, f)
            try:
                with open(full_path, 'r', encoding='utf-8') as fh:
                    content = fh.read()
                if not content.startswith('---'):
                    continue
                parts = content.split('---', 2)
                if len(parts) < 3:
                    continue
                import yaml
                meta = yaml.safe_load(parts[1]) or {}
                original_filename = meta.get('original_filename', '')
                if not original_filename:
                    continue

                # Sök Asset-fil i alla Asset-undermappar
                asset_store = os.path.expanduser(PATHS.get('asset_store', '~/MyMemory/Assets'))
                for dirpath, _, filenames in os.walk(asset_store):
                    if original_filename in filenames:
                        return os.path.join(dirpath, original_filename)
            except Exception:
                continue

    return None


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