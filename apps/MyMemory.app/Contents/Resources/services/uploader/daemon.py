"""
Uploader Daemon — bevakar lokala Assets och pushar pre-parsad text till
upload_server (#165) på Hetzner. Kontrakt enligt #179.

Status-flöde i SQLite (~/Library/Application Support/MyMemory/uploader_state.db):
    pending     ← upptäckt i Assets (watchdog eller bootstrap-scan)
    extracting  ← text-extraktion pågår
    uploaded    ← POST 202 från servern
    done        ← server-status='done' (bekräftad via GET)
    failed      ← extraktion misslyckades, eller upload 4xx/5xx efter retries

Retry-strategi:
    Nätverksfel (connection refused/DNS): retry utan att räkna upp retry_count
    Server 5xx: räkna upp retry_count, exp. backoff (5s, 30s, 5min, 30min, 2h)
    Server 4xx (ej 409): permanent failed, ingen retry
    Server 409 (UUID exists): markera done direkt — idempotency

Environment variables:
    MYMEMORY_CONFIG               Sökväg till my_mem_config.yaml
    MYMEMORY_UPLOAD_URL           Override för cloud.api_url
    MYMEMORY_PAT                  Override för PAT (annars läses från config)
    UPLOADER_INTERVAL             Polling-intervall i sekunder (default 5)
    UPLOADER_LOG_FILE             Default ~/Library/Logs/MyMemory/uploader.log
"""

import atexit
import json
import logging
import os
import re
import shutil
import signal
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Path setup
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- LOGGING ---
from services.utils.config_loader import get_config
_config = get_config()
_default_log_dir = os.path.dirname(os.path.expanduser(
    _config.get('logging', {}).get('system_log', '~/Library/Logs/MyMemory/system.log')
))
_log_file = os.environ.get(
    'UPLOADER_LOG_FILE',
    os.path.join(_default_log_dir, 'uploader.log')
)
os.makedirs(os.path.dirname(_log_file), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - UPLOADER - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(_log_file),
        logging.StreamHandler(sys.stderr),
    ]
)
LOGGER = logging.getLogger('UPLOADER')

for _name in ['urllib3', 'httpx', 'httpcore', 'watchdog']:
    logging.getLogger(_name).setLevel(logging.WARNING)


import urllib.error
import urllib.request

# Watchdog är optional — bootstrap-scan + polling fungerar utan
try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    _WATCHDOG_AVAILABLE = True
except ImportError as _e:
    LOGGER.warning(f"watchdog ej tillgängligt ({_e}) — endast polling-läge")
    FileSystemEventHandler = object
    Observer = None
    _WATCHDOG_AVAILABLE = False

from services.processors.text_extractor import extract_text
from services.utils.config_loader import get_config


# --- CONFIG ---

UUID_SUFFIX_PATTERN = re.compile(
    r'_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.'
    r'(txt|md|pdf|docx|csv|xlsx|eml|json)$',
    re.IGNORECASE,
)

POLL_INTERVAL = int(os.environ.get('UPLOADER_INTERVAL', '5'))

# Default state-DB i macOS Application Support
_STATE_DB_DEFAULT = os.path.expanduser(
    '~/Library/Application Support/MyMemory/uploader_state.db'
)

# Pid-fil — menubar-appen läser denna och skapar DispatchSource.makeProcessSource
# för att få OS-nivå-event när daemonen avslutas (#213). Liggsi samma katalog
# som state-DB:n så båda är portabla tillsammans.
_PID_FILE_DEFAULT = os.path.expanduser(
    '~/Library/Application Support/MyMemory/uploader.pid'
)

# Backoff-trappa (sekunder) per retry_count
RETRY_BACKOFF = [5, 30, 300, 1800, 7200]

# HTTP-timeouts
UPLOAD_TIMEOUT = 60
STATUS_TIMEOUT = 10


def _get_uploader_config() -> dict:
    """Läs uploader-relevant config + env-overrides."""
    cfg = get_config()
    cloud = cfg.get('cloud', {})

    api_url = os.environ.get('MYMEMORY_UPLOAD_URL') or cloud.get('api_url', '')
    if not api_url:
        raise RuntimeError(
            "HARDFAIL: cloud.api_url saknas i config (eller MYMEMORY_UPLOAD_URL env)"
        )
    # Detektera orörd template-placeholder så användaren får tydlig
    # vägledning istället för cryptic connection error (#188).
    if api_url.startswith('__') and api_url.endswith('__'):
        from services.utils.config_loader import get_config_path
        config_path = get_config_path()
        raise RuntimeError(
            f"HARDFAIL: cloud.api_url i config är en orörd template-"
            f"placeholder ({api_url}). Sätt riktig URL i "
            f"{config_path} eller via MYMEMORY_UPLOAD_URL env-variabel."
        )
    api_url = api_url.rstrip('/')

    # PAT-prioritet: env > Keychain > config (config fallback för dev/Linux).
    # macOS: använd Keychain via `services.utils.keychain` (set via menubar).
    pat = os.environ.get('MYMEMORY_PAT')
    if not pat:
        try:
            from services.utils.keychain import get_pat
            pat = get_pat()
        except ImportError as e:
            LOGGER.debug(f"keychain import failed: {e}")
    if not pat:
        pat = cloud.get('pat', '')
    if not pat:
        raise RuntimeError(
            "HARDFAIL: PAT saknas. Sätt via menubar (Keychain), "
            "MYMEMORY_PAT env, eller cloud.pat i config."
        )

    asset_store = os.path.expanduser(cfg['paths']['asset_store'])
    failed_dir = os.path.expanduser(
        cfg['paths'].get('asset_failed', os.path.join(asset_store, 'Failed'))
    )

    state_db = os.path.expanduser(
        cloud.get('uploader_state_db', _STATE_DB_DEFAULT)
    )

    return {
        'api_url': api_url,
        'pat': pat,
        'asset_store': asset_store,
        'failed_dir': failed_dir,
        'state_db': state_db,
        'verify_tls': cloud.get('verify_tls', True),
        'owner': cfg.get('owner', {}).get('profile', {}).get(
            'full_name', 'unknown'
        ),
    }


# --- SQLite STATE ---

_SCHEMA = """
CREATE TABLE IF NOT EXISTS assets (
    uuid TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    asset_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP,
    uploaded_at TIMESTAMP,
    confirmed_at TIMESTAMP,
    last_error TEXT,
    retry_count INTEGER DEFAULT 0,
    next_retry_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_assets_status ON assets(status);
CREATE INDEX IF NOT EXISTS idx_assets_next_retry ON assets(status, next_retry_at);
"""


class UploaderState:
    """Wrapper kring SQLite state-DB. Trådsäker via threading.Lock."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self):
        with self._lock, self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    def upsert_pending(self, uuid: str, source_type: str,
                       original_filename: str, asset_path: str) -> bool:
        """Lägg in en ny pending asset om den inte redan finns.
        Returnerar True om raden är ny."""
        with self._lock, self._connect() as conn:
            cur = conn.execute("SELECT status FROM assets WHERE uuid = ?", (uuid,))
            existing = cur.fetchone()
            if existing:
                return False
            conn.execute(
                "INSERT INTO assets (uuid, source_type, original_filename, "
                "asset_path, status) VALUES (?, ?, ?, ?, 'pending')",
                (uuid, source_type, original_filename, asset_path),
            )
            conn.commit()
            return True

    def fetch_due_pending(self, limit: int = 5) -> list:
        """Hämta pending/failed-rader vars next_retry_at är nådd."""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM assets "
                "WHERE status IN ('pending', 'failed') "
                "  AND (next_retry_at IS NULL OR next_retry_at <= ?) "
                "  AND retry_count < ? "
                "ORDER BY discovered_at ASC LIMIT ?",
                (now, len(RETRY_BACKOFF), limit),
            )
            return [dict(r) for r in cur.fetchall()]

    def lookup(self, uuid: str) -> Optional[dict]:
        """Hämta en rad efter UUID. Används av SSE-consumer vid events."""
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM assets WHERE uuid = ?", (uuid,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def update_status(self, uuid: str, status: str, **fields):
        """Uppdatera status + valfria timestamps/error på en rad."""
        cols = ['status = ?']
        values = [status]
        for k, v in fields.items():
            cols.append(f"{k} = ?")
            values.append(v)
        values.append(uuid)
        with self._lock, self._connect() as conn:
            conn.execute(
                f"UPDATE assets SET {', '.join(cols)} WHERE uuid = ?",
                values,
            )
            conn.commit()

    def increment_retry(self, uuid: str, error: str) -> bool:
        """Räkna upp retry_count och beräkna next_retry_at via backoff.

        Returnerar True om detta var sista försöket (permanent failed — inga
        fler retries kommer plockas upp av fetch_due_pending).
        """
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "SELECT retry_count FROM assets WHERE uuid = ?", (uuid,)
            )
            row = cur.fetchone()
            if not row:
                return False
            new_count = (row['retry_count'] or 0) + 1
            backoff_idx = min(new_count - 1, len(RETRY_BACKOFF) - 1)
            next_retry = datetime.now(timezone.utc).timestamp() + RETRY_BACKOFF[backoff_idx]
            next_retry_iso = datetime.fromtimestamp(
                next_retry, timezone.utc
            ).isoformat()
            conn.execute(
                "UPDATE assets SET retry_count = ?, last_error = ?, "
                "next_retry_at = ?, status = 'failed' WHERE uuid = ?",
                (new_count, error[:1024], next_retry_iso, uuid),
            )
            conn.commit()
            return new_count >= len(RETRY_BACKOFF)

    def stats(self) -> dict:
        """Returnera count per status (för menubar / status-API)."""
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "SELECT status, COUNT(*) AS n FROM assets GROUP BY status"
            )
            return {r['status']: r['n'] for r in cur.fetchall()}


# --- SOURCE TYPE DETECTION ---

def _load_source_mappings():
    """Läs source_mappings + default från schema (SSOT)."""
    from services.utils.schema_validator import SchemaValidator
    sv = SchemaValidator()
    return sv.get_source_type_mappings(), sv.get_default_source_type()

_SOURCE_MAPPINGS, _DEFAULT_SOURCE_TYPE = _load_source_mappings()


def detect_source_type(asset_path: str) -> str:
    """Härled source_type från asset-pathen (Asset/Documents/, Mail/, etc.)."""
    lower = asset_path.lower()
    for keyword, source_type in _SOURCE_MAPPINGS.items():
        if keyword in lower:
            return source_type
    return _DEFAULT_SOURCE_TYPE


def extract_uuid(filename: str) -> Optional[str]:
    """Hämta UUID-suffix från filnamn (mönster: name_<UUID>.<ext>)."""
    match = UUID_SUFFIX_PATTERN.search(filename)
    return match.group(1).lower() if match else None


# --- TRANSPORT ---

class UploadError(Exception):
    """Generiskt upload-fel — wrappar nätverk/HTTP-fel."""

    def __init__(self, message: str, *, network: bool = False,
                 status_code: Optional[int] = None):
        super().__init__(message)
        self.network = network
        self.status_code = status_code


def _explain_url_error(err: urllib.error.URLError, verify_tls: bool) -> str:
    """Formattera URLError med hjälpsam kontext för vanliga fel.

    SSL-verifikationsfel är särskilt förvirrande för användare — om servern
    kör self-signed cert och `cloud.verify_tls` är True i config får man
    'CERTIFICATE_VERIFY_FAILED' utan vägledning. Ge tydlig hint om vad som
    behöver ändras.
    """
    reason = str(err.reason)
    if verify_tls and "CERTIFICATE_VERIFY_FAILED" in reason:
        return (
            f"TLS-verifiering misslyckades ({reason}). "
            "Servern använder troligen self-signed cert. "
            "Sätt `cloud.verify_tls: false` i my_mem_config.yaml, "
            "eller installera serverns CA-cert."
        )
    return f"Network error: {reason}"


def post_payload(api_url: str, pat: str, payload: dict,
                 verify_tls: bool = True) -> tuple:
    """POST /api/v1/ingest. Returnerar (status_code, response_body_dict).

    Raises UploadError vid nätverksfel (network=True) eller server 5xx.
    4xx returneras som (status, body) — caller hanterar (409 = idempotency etc.).
    """
    url = f"{api_url}/api/v1/ingest"
    body = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        url, data=body, method='POST',
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {pat}',
        },
    )

    ctx = None
    if not verify_tls:
        import ssl
        ctx = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(req, timeout=UPLOAD_TIMEOUT, context=ctx) as resp:
            return resp.status, json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        # 4xx/5xx — kolla status
        try:
            err_body = json.loads(e.read().decode('utf-8'))
        except (ValueError, UnicodeDecodeError):
            err_body = {"error": "unparsable_response"}
        if e.code >= 500:
            raise UploadError(
                f"Server {e.code}: {err_body}", status_code=e.code
            ) from e
        return e.code, err_body
    except urllib.error.URLError as e:
        raise UploadError(_explain_url_error(e, verify_tls), network=True) from e
    except (TimeoutError, ConnectionError) as e:
        raise UploadError(f"Connection error: {e}", network=True) from e


# get_status borttagen (#214) — ersatt av SSE-consumer (_run_sse_consumer).


# --- PROCESSING ---

def build_payload(*, uuid: str, source_type: str, original_filename: str,
                  extracted_text: str, owner: str,
                  source_specific: Optional[dict] = None) -> dict:
    """Bygg upload-payload enligt schema upload_api_v1.schema.json."""
    payload = {
        'schema_version': 1,
        'uuid': uuid,
        'source_type': source_type,
        'original_filename': original_filename,
        'extracted_text': extracted_text,
        'metadata': {
            'timestamp_content': 'UNKNOWN',  # härleds server-side om möjligt
            'owner': owner,
        },
    }
    if source_specific:
        payload['metadata']['source_specific'] = source_specific
    return payload


def _move_to_failed(asset_path: str, failed_dir: str, filename: str) -> None:
    """Flytta permanent-failade filer till Failed-mappen.

    Tyst no-op om filen redan saknas (kan hända om användaren städat manuellt
    eller om 'Asset saknas'-retry-grenen når permanent). Felhanterar
    move-fel som WARNING — failed-flytt får aldrig blockera SQLite-uppdatering.
    """
    if not os.path.exists(asset_path):
        return
    try:
        os.makedirs(failed_dir, exist_ok=True)
        dest = os.path.join(failed_dir, os.path.basename(asset_path))
        shutil.move(asset_path, dest)
        LOGGER.info(f"{filename}: flyttad till {failed_dir} (permanent failed)")
    except OSError as e:
        LOGGER.warning(f"{filename}: kunde inte flytta till Failed: {e}")


def process_asset(state: UploaderState, cfg: dict, row: dict) -> str:
    """Processa en pending asset: extrahera → upload. Returnerar ny status.

    Uppdaterar SQLite-raden med varje steg. Vid fel: increment_retry()
    och returnera 'failed'. Vid 409: markera done direkt (idempotency).
    """
    uuid_str = row['uuid']
    asset_path = row['asset_path']
    source_type = row['source_type']
    filename = row['original_filename']

    # 1. Extrahera text
    if not os.path.exists(asset_path):
        msg = f"Asset saknas: {asset_path}"
        LOGGER.error(f"{filename}: {msg}")
        state.increment_retry(uuid_str, msg)
        # Ingen fil att flytta — den finns ju inte
        return 'failed'

    state.update_status(uuid_str, 'extracting')
    try:
        text = extract_text(asset_path)
    except (RuntimeError, OSError) as e:
        msg = f"extract_text failed: {e}"
        LOGGER.error(f"{filename}: {msg}")
        if state.increment_retry(uuid_str, msg):
            _move_to_failed(asset_path, cfg['failed_dir'], filename)
        return 'failed'

    if not text or len(text.strip()) < 10:
        msg = f"Tom text-extraktion ({len(text)} chars)"
        LOGGER.warning(f"{filename}: {msg}")
        if state.increment_retry(uuid_str, msg):
            _move_to_failed(asset_path, cfg['failed_dir'], filename)
        return 'failed'

    state.update_status(
        uuid_str,
        'extracting',
        extracted_at=datetime.now(timezone.utc).isoformat(),
    )

    # 2. Bygg payload + upload
    payload = build_payload(
        uuid=uuid_str,
        source_type=source_type,
        original_filename=filename,
        extracted_text=text,
        owner=cfg['owner'],
    )

    try:
        status_code, body = post_payload(
            cfg['api_url'], cfg['pat'], payload,
            verify_tls=cfg['verify_tls'],
        )
    except UploadError as e:
        if e.network:
            # Nätverksfel — räkna inte upp retry_count, försök igen senare
            LOGGER.warning(f"{filename}: nätverksfel, försöker igen senare: {e}")
            state.update_status(
                uuid_str, 'pending',
                next_retry_at=datetime.fromtimestamp(
                    datetime.now(timezone.utc).timestamp() + RETRY_BACKOFF[0],
                    timezone.utc,
                ).isoformat(),
            )
            return 'pending'
        # Server 5xx — räkna upp retry
        msg = f"server_error: {e}"
        LOGGER.error(f"{filename}: {msg}")
        if state.increment_retry(uuid_str, msg):
            _move_to_failed(asset_path, cfg['failed_dir'], filename)
        return 'failed'

    if status_code == 202:
        LOGGER.info(f"{filename}: uploaded ({uuid_str})")
        state.update_status(
            uuid_str, 'uploaded',
            uploaded_at=datetime.now(timezone.utc).isoformat(),
        )
        return 'uploaded'

    if status_code == 409:
        # Idempotency — UUID fanns redan, markera done
        LOGGER.info(f"{filename}: already on server (409), marking done")
        state.update_status(
            uuid_str, 'done',
            confirmed_at=datetime.now(timezone.utc).isoformat(),
        )
        return 'done'

    # 4xx (utom 409): permanent fel
    msg = f"client_error_{status_code}: {body}"
    LOGGER.error(f"{filename}: {msg}")
    state.update_status(
        uuid_str, 'failed',
        last_error=msg[:1024],
        retry_count=len(RETRY_BACKOFF),  # markera som permanent — ingen mer retry
    )
    _move_to_failed(asset_path, cfg['failed_dir'], filename)
    return 'failed'


# confirm_uploaded borttagen (#214) — ersatt av SSE-consumer nedan.


# --- SSE CONSUMER (#214) ---

def _apply_server_event(state: 'UploaderState', cfg: dict, event: dict) -> None:
    """Hantera ett ingestion_done/ingestion_failed-event från servern.

    Översätter SSE-eventet till en update_status-operation i SQLite.
    Ignorerar events för UUID:er vi inte känner till (kan hända om
    klienten startat efter en ingestion på annan enhet — se #216).
    """
    uuid_str = event.get('uuid')
    status = event.get('status')
    if not uuid_str or status not in ('done', 'failed'):
        return

    row = state.lookup(uuid_str)
    if row is None:
        LOGGER.debug(f"SSE-event för okänd uuid {uuid_str} — ignorerar")
        return

    filename = row.get('original_filename') or event.get('filename') or uuid_str

    if status == 'done':
        LOGGER.info(f"{filename}: bekräftat done (SSE)")
        state.update_status(
            uuid_str, 'done',
            confirmed_at=datetime.now(timezone.utc).isoformat(),
        )
    else:  # failed
        err = event.get('error', 'server-side ingestion failed')
        LOGGER.error(f"{filename}: server-side failed (SSE): {err}")
        state.update_status(
            uuid_str, 'failed',
            last_error=err[:1024],
            retry_count=len(RETRY_BACKOFF),
        )
        asset_path = row.get('asset_path', '')
        if asset_path:
            _move_to_failed(asset_path, cfg['failed_dir'], filename)


def _run_sse_consumer(state: 'UploaderState', cfg: dict) -> None:
    """Bakgrundstråd: håll persistent SSE-anslutning mot
    GET {api_url}/api/v1/events och applicera events på SQLite-state.

    Reconnectar automatiskt vid fel med exponentiell backoff
    (1s → 2s → 5s → 15s → 60s max). Körs tills _running sätts till False.
    """
    url = f"{cfg['api_url']}/api/v1/events"
    backoff = 1
    backoff_sequence = [1, 2, 5, 15, 60]

    ctx = None
    if not cfg['verify_tls']:
        import ssl
        ctx = ssl._create_unverified_context()

    while _running:
        try:
            req = urllib.request.Request(
                url, method='GET',
                headers={
                    'Authorization': f"Bearer {cfg['pat']}",
                    'Accept': 'text/event-stream',
                },
            )
            LOGGER.info(f"SSE: ansluter till {url}")
            with urllib.request.urlopen(req, timeout=None, context=ctx) as resp:
                backoff = 1  # reset efter lyckad anslutning
                LOGGER.info("SSE: ansluten")

                current_event = None
                for raw in resp:
                    if not _running:
                        break
                    line = raw.decode('utf-8', errors='replace').rstrip('\n')
                    if line == '':
                        # Tom rad = event-delimiter — men vi har redan
                        # hanterat data-raden direkt, så inget att göra här
                        current_event = None
                        continue
                    if line.startswith(':'):
                        continue  # keepalive-kommentar
                    if line.startswith('event: '):
                        current_event = line[7:].strip()
                        continue
                    if line.startswith('data: '):
                        try:
                            payload = json.loads(line[6:])
                        except json.JSONDecodeError as e:
                            LOGGER.warning(f"SSE: ogiltig JSON: {e}")
                            continue
                        if current_event in ('ingestion_done', 'ingestion_failed'):
                            _apply_server_event(state, cfg, payload)

        except urllib.error.HTTPError as e:
            LOGGER.error(f"SSE: HTTP {e.code} — väntar {backoff}s innan retry")
        except urllib.error.URLError as e:
            LOGGER.warning(f"SSE: nätverksfel ({e.reason}) — retry om {backoff}s")
        except (TimeoutError, ConnectionError, OSError) as e:
            LOGGER.warning(f"SSE: connection error ({e}) — retry om {backoff}s")

        # Backoff innan retry
        for _ in range(backoff):
            if not _running:
                break
            time.sleep(1)
        idx = min(backoff_sequence.index(backoff) + 1, len(backoff_sequence) - 1) \
            if backoff in backoff_sequence else 0
        backoff = backoff_sequence[idx]


# --- BOOTSTRAP + WATCHDOG ---

def discover_assets(state: UploaderState, asset_store: str) -> int:
    """Walk Assets/ och INSERT pending för okända filer. Returnerar antal nya."""
    if not os.path.isdir(asset_store):
        LOGGER.warning(f"Asset store saknas: {asset_store}")
        return 0

    new_count = 0
    for root, _dirs, files in os.walk(asset_store):
        for fname in files:
            uuid_str = extract_uuid(fname)
            if not uuid_str:
                continue
            full_path = os.path.join(root, fname)
            source_type = detect_source_type(full_path)
            if state.upsert_pending(uuid_str, source_type, fname, full_path):
                new_count += 1
                LOGGER.info(f"Bootstrap discovered: {fname} ({source_type})")
    return new_count


class AssetEventHandler(FileSystemEventHandler):
    """Watchdog-handler: lägg in nya filer som pending."""

    def __init__(self, state: UploaderState):
        self.state = state

    def on_created(self, event):
        if event.is_directory:
            return
        self._enqueue(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        self._enqueue(event.dest_path)

    def _enqueue(self, path: str):
        fname = os.path.basename(path)
        uuid_str = extract_uuid(fname)
        if not uuid_str:
            return
        source_type = detect_source_type(path)
        if self.state.upsert_pending(uuid_str, source_type, fname, path):
            LOGGER.info(f"Watchdog enqueued: {fname} ({source_type})")


# --- MAIN LOOP ---

_running = True


def _signal_handler(signum, _frame):
    global _running
    LOGGER.info(f"Signal {signum} mottagen — avslutar")
    _running = False


def _write_pid_file(pid_file: str) -> None:
    """Skriv nuvarande PID till pid-fil + registrera atexit-cleanup.

    Menubar-appen läser filen och skapar en DispatchSource.makeProcessSource
    för att få event vid daemon-exit (#213). Cleanup sker via atexit (normal
    exit) + signal-handler (SIGTERM/SIGINT). Hård krasch (SIGKILL/crash)
    lämnar stale fil — menubar får då omedelbar .exit-event på den döda
    PID:en vilket tolkas korrekt som 'stoppad'.
    """
    try:
        os.makedirs(os.path.dirname(pid_file), exist_ok=True)
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
        atexit.register(_remove_pid_file, pid_file)
    except OSError as e:
        LOGGER.warning(f"Kunde inte skriva pid-fil {pid_file}: {e}")


def _remove_pid_file(pid_file: str) -> None:
    """Ta bort pid-filen om den tillhör oss."""
    try:
        with open(pid_file, 'r') as f:
            owner_pid = int(f.read().strip())
        if owner_pid == os.getpid():
            os.unlink(pid_file)
    except (OSError, ValueError):
        pass


def run_daemon():
    """Huvudloop: bootstrap → watchdog + polling-loop."""
    cfg = _get_uploader_config()
    state = UploaderState(cfg['state_db'])

    _write_pid_file(_PID_FILE_DEFAULT)

    LOGGER.info(
        f"Starting uploader: api={cfg['api_url']}, "
        f"assets={cfg['asset_store']}, state={cfg['state_db']}"
    )

    # 1. Bootstrap-scan
    new = discover_assets(state, cfg['asset_store'])
    if new:
        LOGGER.info(f"Bootstrap: {new} nya assets upptäckta")

    # 2. Watchdog
    observer = None
    if _WATCHDOG_AVAILABLE:
        observer = Observer()
        observer.schedule(
            AssetEventHandler(state), cfg['asset_store'], recursive=True
        )
        observer.start()
        LOGGER.info(f"Watchdog aktiv på {cfg['asset_store']}")
    else:
        LOGGER.warning("Watchdog ej installerat — endast bootstrap-scan + polling")

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # 3. SSE-consumer: server pushar done/failed-events (#214) istället för
    #    klient-polling av GET /api/v1/ingest/{uuid}. Körs i dedikerad tråd
    #    så upload-loopen inte blockeras av SSE-reconnects.
    sse_thread = threading.Thread(
        target=_run_sse_consumer,
        args=(state, cfg),
        name='SSE-consumer',
        daemon=True,
    )
    sse_thread.start()

    # 4. Upload-loop: plockar pending, extraherar, uppladdar. Inget
    #    confirm-steg längre — serverns NOTIFY hanterar done-övergången.
    while _running:
        try:
            for row in state.fetch_due_pending(limit=5):
                if not _running:
                    break
                process_asset(state, cfg, row)
        except sqlite3.OperationalError as e:
            LOGGER.error(f"SQLite error: {e}")

        # Sov — bryt tidigt om signal kommer
        for _ in range(POLL_INTERVAL):
            if not _running:
                break
            time.sleep(1)

    if observer:
        observer.stop()
        observer.join(timeout=5)

    stats = state.stats()
    LOGGER.info(f"Uploader stoppad. Stats: {stats}")


if __name__ == "__main__":
    run_daemon()
