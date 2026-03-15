"""
peer_server.py — StreamableHTTP MCP-server for Signal Feed.

Wraps ALL mymem_mcp tools over HTTP so the broker agent gets the same
access as the user via Claude Desktop. Adds three signal-feed-specific
tools: get_feed, receive_signal, get_signals.

Startup:
    Managed by start_services.py when signal_feed.enabled = true.
    Can also be started standalone for testing:

    MYMEMORY_HOME=~/MyMemory python services/agents/peer_server.py
"""

import json
import logging
import os
import sys
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# Path setup
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import mymem_mcp — this initializes config, logging, graph/vector paths,
# signal handlers, and all tool functions. We piggyback on that setup.
from services.agents import mymem_mcp

from mcp.server.fastmcp import FastMCP
from fastmcp.server.auth import TokenVerifier, AccessToken

# Reconfigure logging prefix for peer server
LOGGER = logging.getLogger("PeerServer")

# --- CONFIG (from mymem_mcp's already-loaded config) ---
CONFIG = mymem_mcp.CONFIG
SIGNAL_FEED_CONFIG = CONFIG.get('signal_feed', {})

PEER_ID = CONFIG.get('owner', {}).get('id', 'unknown')
PEER_PORT = SIGNAL_FEED_CONFIG.get('port', 8100)
BROKER_URL = SIGNAL_FEED_CONFIG.get('broker_url', '')
INTENT_PEER_SECRET = SIGNAL_FEED_CONFIG.get('intent_peer_secret', '')
HEARTBEAT_INTERVAL = SIGNAL_FEED_CONFIG.get('heartbeat_interval_seconds', 300)

from services.utils.config_loader import get_mymemory_home
HOME_PATH = str(get_mymemory_home())
SIGNALS_DIR = os.path.join(HOME_PATH, "Signals")
INBOX_FILE = os.path.join(SIGNALS_DIR, "inbox.json")

# Feed cache (memory + disk)
_feed_cache = None
_feed_cache_time = 0.0
FEED_CACHE_TTL = SIGNAL_FEED_CONFIG.get('feed_cache_ttl_seconds', 3600)
FEED_FILE = os.path.join(SIGNALS_DIR, "feed.json")
FEED_HISTORY_DIR = os.path.join(SIGNALS_DIR, "feed_history")
FEED_RETENTION_DAYS = 7


def _save_feed_to_disk(feed: dict):
    """Save feed to disk: current + timestamped history."""
    os.makedirs(SIGNALS_DIR, exist_ok=True)
    os.makedirs(FEED_HISTORY_DIR, exist_ok=True)

    # Current feed (overwritten each time)
    with open(FEED_FILE, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)

    # Timestamped copy for history
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%S")
    history_file = os.path.join(FEED_HISTORY_DIR, f"feed_{ts}.json")
    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False)

    _clean_old_feeds()
    LOGGER.info(f"Feed saved to disk ({FEED_FILE})")


def _load_feed_from_disk() -> dict | None:
    """Load last saved feed from disk."""
    if not os.path.exists(FEED_FILE):
        return None
    try:
        with open(FEED_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        LOGGER.warning(f"Could not load cached feed: {e}")
        return None


def _clean_old_feeds():
    """Remove feed history files older than FEED_RETENTION_DAYS."""
    if not os.path.isdir(FEED_HISTORY_DIR):
        return
    cutoff = time.time() - (FEED_RETENTION_DAYS * 86400)
    for filename in os.listdir(FEED_HISTORY_DIR):
        filepath = os.path.join(FEED_HISTORY_DIR, filename)
        try:
            if os.path.getmtime(filepath) < cutoff:
                os.remove(filepath)
        except OSError:
            pass


# --- Auth ---

class IntentTokenVerifier(TokenVerifier):
    """Validate Bearer token against shared secret from config."""

    async def verify_token(self, token: str) -> AccessToken | None:
        if token != INTENT_PEER_SECRET:
            LOGGER.warning("Invalid Bearer token received")
            return None
        return AccessToken(
            token=token,
            client_id="intent",
            scopes=["full"],
        )


if not INTENT_PEER_SECRET:
    LOGGER.error("intent_peer_secret not set — peer server will not start without auth")
    print("ERROR: signal_feed.intent_peer_secret missing in config. "
          "Peer server refuses to start without auth.", file=sys.stderr)
    sys.exit(1)

_auth = IntentTokenVerifier()

# --- Peer MCP Server (StreamableHTTP) ---
peer_mcp = FastMCP(
    f"MyMemory-Peer-{PEER_ID}",
    host="0.0.0.0",
    port=PEER_PORT,
    auth=_auth,
)

# ========================================================================
# Re-register all mymem_mcp tools on the peer server.
# The broker gets the same toolset as the user.
# ========================================================================

_MYMEM_TOOLS = [
    mymem_mcp.search_graph_nodes,
    mymem_mcp.query_vector_memory,
    mymem_mcp.search_by_date_range,
    mymem_mcp.search_lake_metadata,
    mymem_mcp.get_neighbor_network,
    mymem_mcp.search_relation_context,
    mymem_mcp.get_entity_summary,
    mymem_mcp.get_graph_health,
    mymem_mcp.get_data_model,
    mymem_mcp.parse_relative_date,
    mymem_mcp.read_document_content,
    mymem_mcp.get_source_connections,
    mymem_mcp.ingest_content,
]

for _tool_func in _MYMEM_TOOLS:
    peer_mcp.tool()(_tool_func)


# ========================================================================
# Signal Feed tools (peer-specific, not in mymem_mcp)
# ========================================================================

@peer_mcp.tool()
def get_feed() -> str:
    """Return this peer's smoketrail feed.

    Contains k-means cluster centroids (384-dim embeddings) representing
    thematic gravity zones. No cleartext, no filenames, no IDs.

    The broker compares centroids between peers to find overlaps,
    then uses MCP tools on-demand to investigate further.

    Feed is cached in memory + saved to disk with 7 days retention.
    """
    global _feed_cache, _feed_cache_time
    from services.utils.feed_publisher import build_feed

    now = time.monotonic()
    if _feed_cache is not None and (now - _feed_cache_time) < FEED_CACHE_TTL:
        LOGGER.info("Returning cached feed")
        return json.dumps(_feed_cache, ensure_ascii=False)

    # Try disk cache if memory cache is empty (e.g. after restart)
    if _feed_cache is None:
        disk_feed = _load_feed_from_disk()
        if disk_feed is not None:
            _feed_cache = disk_feed
            _feed_cache_time = now
            LOGGER.info("Loaded feed from disk cache")
            return json.dumps(disk_feed, ensure_ascii=False)

    try:
        n_clusters = SIGNAL_FEED_CONFIG.get('smoketrail_clusters', 12)
        feed = build_feed(
            peer_id=PEER_ID,
            vector_db_path=mymem_mcp.VECTOR_PATH,
            n_clusters=n_clusters,
        )
        _feed_cache = feed
        _feed_cache_time = now
        _save_feed_to_disk(feed)
        return json.dumps(feed, ensure_ascii=False)
    except Exception as e:
        LOGGER.error(f"get_feed failed: {e}")
        return json.dumps({"error": f"get_feed: {e}"})


@peer_mcp.tool()
def receive_signal(message: str) -> str:
    """Receive a mediation message from the broker.

    Saves to ~/MyMemory/Signals/inbox.json.
    """
    try:
        os.makedirs(SIGNALS_DIR, exist_ok=True)

        inbox = []
        if os.path.exists(INBOX_FILE):
            with open(INBOX_FILE, "r", encoding="utf-8") as f:
                inbox = json.load(f)

        inbox.append({
            "message": message,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "read": False,
        })

        with open(INBOX_FILE, "w", encoding="utf-8") as f:
            json.dump(inbox, f, indent=2, ensure_ascii=False)

        LOGGER.info(f"Signal received and saved ({len(inbox)} total in inbox)")
        return json.dumps({"status": "ok", "inbox_count": len(inbox)})

    except Exception as e:
        LOGGER.error(f"receive_signal failed: {e}")
        return json.dumps({"error": f"receive_signal: {e}"})


@peer_mcp.tool()
def get_signals() -> str:
    """Get all received signals (messages from the broker).

    Returns the inbox and marks all messages as read.
    """
    try:
        if not os.path.exists(INBOX_FILE):
            return json.dumps({"signals": [], "count": 0})

        with open(INBOX_FILE, "r", encoding="utf-8") as f:
            inbox = json.load(f)

        unread = [s for s in inbox if not s.get("read")]
        for s in inbox:
            s["read"] = True

        with open(INBOX_FILE, "w", encoding="utf-8") as f:
            json.dump(inbox, f, indent=2, ensure_ascii=False)

        return json.dumps({
            "signals": inbox,
            "count": len(inbox),
            "unread": len(unread),
        }, ensure_ascii=False)

    except Exception as e:
        LOGGER.error(f"get_signals failed: {e}")
        return json.dumps({"error": f"get_signals: {e}"})


# ========================================================================
# Broker registration & heartbeat
# ========================================================================

def _register_with_broker():
    """POST registration to broker. Broker reads our public IP from the request."""
    if not BROKER_URL:
        return

    url = BROKER_URL.rstrip('/') + '/api/peers/register'
    tool_names = [f.__name__ for f in _MYMEM_TOOLS] + ["get_feed", "receive_signal", "get_signals"]
    payload = json.dumps({
        "peer_id": PEER_ID,
        "port": PEER_PORT,
        "tools": tool_names,
        "version": CONFIG.get('version', 'unknown'),
    }).encode('utf-8')

    try:
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            LOGGER.info(f"Registered with broker ({resp.status})")
    except (urllib.error.URLError, OSError) as e:
        LOGGER.warning(f"Broker registration failed: {e}")


def _heartbeat_loop():
    """Background thread: register at startup, then heartbeat every N seconds."""
    # Initial registration (small delay to let server start)
    time.sleep(2)
    _register_with_broker()

    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        _register_with_broker()


# ========================================================================
# Main
# ========================================================================

if __name__ == "__main__":
    LOGGER.info(f"Starting peer server '{PEER_ID}' on port {PEER_PORT}")
    LOGGER.info(f"  Tools: {len(_MYMEM_TOOLS)} from mymem_mcp + 3 signal feed")

    if BROKER_URL:
        LOGGER.info(f"  Broker: {BROKER_URL} (heartbeat every {HEARTBEAT_INTERVAL}s)")
        t = threading.Thread(target=_heartbeat_loop, daemon=True)
        t.start()
    else:
        LOGGER.info("  No broker_url configured — skipping registration")

    print(f"Peer server '{PEER_ID}' starting on http://0.0.0.0:{PEER_PORT}/mcp",
          file=sys.stderr)
    peer_mcp.run(transport="streamable-http")
