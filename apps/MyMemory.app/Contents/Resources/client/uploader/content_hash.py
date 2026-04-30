"""
content_hash.py — Body-deterministisk hash för upsert-routing (#249).

Klienten beräknar en versionsstämplad SHA-256 över "body-bytes" innan POST
till /api/v1/ingest. Servern jämför mot lagrad hash inom samma algo-version
och routar:
    - samma hash + samma algo  → 200 unchanged (no-op)
    - olika hash, samma algo   → 202 updated (re-ingest)
    - olika algo               → 200 algorithm_bumped (lagra, ingen re-ingest)
    - ingen lagrad hash        → 200 hash_stored (lazy backfill)

Body-extraktion är per source_type:
    - Document / Transcript:                 hela filen
    - Email Thread / Calendar Event /
      Slack Log:                              allt efter metadata-blocket

Metadata-blocket har formatet (matchar Swift watchers + legacy Python collectors):
    ================================================================================
    METADATA FRÅN <KÄLLA>
    ================================================================================
    NYCKEL:        värde
    ...
    ================================================================================

    <body>

Tre separator-rader (80x "="). Body börjar efter den tredje. Ändras inte
metadata-blocket men body modifieras (t.ex. nytt event tillkommer i en
kalender-digest), ger hash:en samma värde *bara* om bodyn är identisk.
Det är meningen — då är det ingen verklig ändring.

Versionsstämpel:
    HASH_ALGO byts vid algoritm-byte (sha256-body-v1 → blake3-body-v2).
    Server lagrar hashen men triggar inte re-ingest vid algo-byte —
    skydd mot algoritm-storm vid flotta-uppdatering.
"""

from __future__ import annotations

import hashlib

# Versionsstämpel. Format: <algorithm>-<scope>-<version>.
# - algorithm: sha256 (kollisions-resistens ~2^128 över body-bytes)
# - scope:     body (skippar metadata-block)
# - version:   v1 (höjs vid byte av algo eller body-extraktionslogik)
HASH_ALGO = "sha256-body-v1"

# 80x "=" + LF (matchar Swift CalendarWatcher/MailWatcher + legacy Python).
_SEPARATOR = b"=" * 80 + b"\n"

# Source-types vars hela filinnehåll är "body" — ingen metadata att skippa.
# (Schema-enum: "Document", "Slack Log", "Email Thread", "Calendar Event",
# "Transcript". Slack/Mail/Calendar har metadata-block, övriga inte.)
_FULLFILE_SOURCE_TYPES = frozenset({"Document", "Transcript"})


def extract_body(asset_path: str, source_type: str) -> bytes:
    """Returnera body-bytes för en asset, deterministiskt per source_type.

    Args:
        asset_path: Absolut sökväg till filen i Assets/.
        source_type: Schema-enum-värde ("Document", "Calendar Event", etc.).

    Returns:
        Body-bytes redo att hashas.

    Raises:
        OSError: Om filen inte kan läsas.
    """
    with open(asset_path, 'rb') as f:
        raw = f.read()

    if source_type in _FULLFILE_SOURCE_TYPES:
        return raw

    # Mail/Calendar/Slack: split på SEPARATOR, ta allt efter tredje
    # förekomsten. maxsplit=3 ger upp till 4 parts; parts[3] är resten av
    # filen (inkluderar eventuella ===-rader i bodyn själv).
    parts = raw.split(_SEPARATOR, 3)
    if len(parts) >= 4:
        return parts[3].lstrip(b"\r\n")

    # Fallback: ingen metadata-block hittad → behandla hela filen som body.
    # Säkerställer att hash alltid kan beräknas; skulle fel format dyka upp
    # blir det "annan hash" → re-ingest, inte krasch.
    return raw


def compute_content_hash(asset_path: str, source_type: str) -> tuple[str, str]:
    """Beräkna (hash_hex, algo_version) för asset.

    Args:
        asset_path: Absolut sökväg till filen i Assets/.
        source_type: Schema-enum-värde.

    Returns:
        (sha256_hex, "sha256-body-v1")
    """
    body = extract_body(asset_path, source_type)
    return hashlib.sha256(body).hexdigest(), HASH_ALGO
