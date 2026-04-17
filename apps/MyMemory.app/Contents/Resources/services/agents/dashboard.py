"""
Dashboard — publik landing-sida för MyMemory Cloud.

Visar aggregerad hälso- och storlekssignal ("lever servern?") utan att
exponera tenant-data. Hostas som route `/` i MCP-servern (se
mcp_server.create_app). Ingen auth — ingen info som kräver skydd.

Säkerhetsöverväganden:
    - Endast aggregerade counts över alla tenants (ingen per-tenant-info)
    - Inga filnamn, innehåll, eller tenant-identifierare
    - HTML är statisk förutom siffror + timestamp (ingen användar-input)
    - Om PG är nere: "degraded" utan stacktrace

CLAUDE.md §14 respekteras (ingen ny port — routen ligger på MCP-serverns
befintliga port 8000, som redan exponeras via Caddy).
"""

import logging
from datetime import datetime, timezone

import psycopg2
from starlette.requests import Request
from starlette.responses import HTMLResponse

from services.utils.config_loader import get_config
from services.utils.graph_service import _get_pg_dsn

LOGGER = logging.getLogger('DASHBOARD')


# --- PG-AGGREGAT ---

_COUNT_QUERIES = [
    ("tenants", "SELECT COUNT(*) FROM tenants"),
    ("documents", "SELECT COUNT(*) FROM documents"),
    ("nodes", "SELECT COUNT(*) FROM nodes"),
    ("edges", "SELECT COUNT(*) FROM edges"),
    ("vectors", "SELECT COUNT(*) FROM vectors"),
]


def _fetch_stats() -> dict:
    """Hämta aggregerade counts + senaste ingestion-timestamp.

    Returnerar dict med:
        pg_ok: bool
        counts: {tabellnamn: int} (saknas om pg_ok=False)
        latest_ingestion: ISO8601 eller None
        error: str (endast vid pg_ok=False)
    """
    try:
        conn = psycopg2.connect(_get_pg_dsn(), connect_timeout=3)
    except (psycopg2.OperationalError, psycopg2.InterfaceError, OSError) as e:
        LOGGER.warning(f"Dashboard: PG unavailable: {e}")
        return {"pg_ok": False, "error": "database offline"}

    counts = {}
    latest = None
    try:
        with conn.cursor() as cur:
            for key, sql in _COUNT_QUERIES:
                cur.execute(sql)
                counts[key] = cur.fetchone()[0]
            cur.execute(
                "SELECT MAX(timestamp_ingestion) FROM documents"
            )
            row = cur.fetchone()
            if row and row[0]:
                latest = row[0].astimezone(timezone.utc).isoformat()
    finally:
        conn.close()

    return {
        "pg_ok": True,
        "counts": counts,
        "latest_ingestion": latest,
    }


# --- HTML-RENDERING ---

def _render_html(stats: dict, server_version: str) -> str:
    """Bygg dashboard-HTML. Inline CSS (ingen ny statisk-asset-infra)."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    status_label = "Levande" if stats["pg_ok"] else "Nedgraderad"
    status_class = "ok" if stats["pg_ok"] else "degraded"

    if stats["pg_ok"]:
        c = stats["counts"]
        cards = f"""
        <div class="cards">
            <div class="card"><span class="num">{c['tenants']:,}</span><span class="lbl">Minnen</span></div>
            <div class="card"><span class="num">{c['documents']:,}</span><span class="lbl">Dokument</span></div>
            <div class="card"><span class="num">{c['nodes']:,}</span><span class="lbl">Noder</span></div>
            <div class="card"><span class="num">{c['edges']:,}</span><span class="lbl">Kanter</span></div>
            <div class="card"><span class="num">{c['vectors']:,}</span><span class="lbl">Vektorer</span></div>
        </div>
        """
        latest = stats.get("latest_ingestion")
        latest_line = (
            f'<p class="latest">Senaste ingestion: <time>{latest}</time></p>'
            if latest else
            '<p class="latest">Ingen ingestion registrerad ännu.</p>'
        )
    else:
        cards = '<p class="error">Kunde inte läsa aggregat — databasen svarar inte.</p>'
        latest_line = ""

    return f"""<!DOCTYPE html>
<html lang="sv">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="refresh" content="30">
    <title>Digitalist Memory</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Lexend+Deca:wght@300;500;700&display=swap" rel="stylesheet">
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        html, body {{ height: 100%; }}
        body {{
            font-family: 'Lexend Deca', -apple-system, BlinkMacSystemFont, sans-serif;
            background: radial-gradient(ellipse at top, #1a1d2e 0%, #0a0b14 70%);
            color: #f0ecd9;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 2rem;
        }}
        .wrap {{ max-width: 960px; width: 100%; }}
        header {{
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            margin-bottom: 3rem;
            flex-wrap: wrap;
            gap: 1rem;
        }}
        h1 {{
            font-size: 1.8rem;
            font-weight: 300;
            letter-spacing: 0.02em;
        }}
        h1 .accent {{ color: #c9a96e; font-weight: 500; }}
        .status {{
            font-size: 0.85rem;
            padding: 0.35rem 0.9rem;
            border-radius: 999px;
            font-weight: 500;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }}
        .status.ok {{
            background: rgba(132, 199, 122, 0.12);
            color: #84c77a;
            border: 1px solid rgba(132, 199, 122, 0.3);
        }}
        .status.degraded {{
            background: rgba(230, 120, 120, 0.12);
            color: #e67878;
            border: 1px solid rgba(230, 120, 120, 0.3);
        }}
        .cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
            margin-bottom: 2.5rem;
        }}
        .card {{
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(201, 169, 110, 0.15);
            border-radius: 12px;
            padding: 1.5rem 1rem;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 0.4rem;
        }}
        .card .num {{
            font-size: 2.2rem;
            font-weight: 500;
            color: #c9a96e;
            font-variant-numeric: tabular-nums;
        }}
        .card .lbl {{
            font-size: 0.8rem;
            color: rgba(240, 236, 217, 0.6);
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }}
        .latest, .error {{
            color: rgba(240, 236, 217, 0.55);
            font-size: 0.9rem;
            text-align: center;
            margin-bottom: 0.5rem;
        }}
        .error {{ color: #e67878; }}
        time {{ color: rgba(240, 236, 217, 0.8); }}
        footer {{
            margin-top: 2rem;
            text-align: center;
            color: rgba(240, 236, 217, 0.35);
            font-size: 0.75rem;
            letter-spacing: 0.05em;
        }}
    </style>
</head>
<body>
    <div class="wrap">
        <header>
            <h1>Digitalist <span class="accent">Memory</span></h1>
            <span class="status {status_class}">{status_label}</span>
        </header>
        {cards}
        {latest_line}
        <footer>
            Version {server_version} &middot; uppdaterad {now} &middot; auto-refresh 30s
        </footer>
    </div>
</body>
</html>"""


# --- ROUTE ---

async def dashboard_route(request: Request) -> HTMLResponse:
    """GET / — publik dashboard. Read-only, ingen auth."""
    stats = _fetch_stats()
    server_version = get_config().get('version', 'unknown')
    html = _render_html(stats, server_version)
    code = 200 if stats["pg_ok"] else 503
    return HTMLResponse(html, status_code=code)
