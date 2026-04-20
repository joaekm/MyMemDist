"""Status-CLI för uploader-daemonen — läser SQLite-state och printar JSON.

Tänkt att anropas av menubar-appen via subprocess. Returnerar:
    {
        "stats": {"pending": 3, "uploaded": 1, "done": 42, "failed": 2},
        "recent_failures": [
            {"filename": "doc.pdf", "error": "...", "retry_count": 3}
        ]
    }

Usage:
    python -m client.uploader.status_cli
    python -m client.uploader.status_cli --db /path/to/state.db
"""

import argparse
import json
import sqlite3
import sys

from client.uploader.daemon import _STATE_DB_DEFAULT


def get_status(db_path: str) -> dict:
    """Läs stats + senaste failures från SQLite."""
    conn = sqlite3.connect(db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        # Stats per status
        cur = conn.execute(
            "SELECT status, COUNT(*) AS n FROM assets GROUP BY status"
        )
        stats = {r['status']: r['n'] for r in cur.fetchall()}

        # Senaste 5 failures
        cur = conn.execute(
            "SELECT original_filename, last_error, retry_count "
            "FROM assets WHERE status = 'failed' "
            "ORDER BY discovered_at DESC LIMIT 5"
        )
        failures = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    return {
        'stats': stats,
        'recent_failures': failures,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--db', default=_STATE_DB_DEFAULT,
        help='Sökväg till state-DB (default: macOS Application Support)',
    )
    args = parser.parse_args()

    try:
        result = get_status(args.db)
    except sqlite3.OperationalError as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
