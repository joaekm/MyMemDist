"""
Broker-auth för klient-processer.

Efter #234 (plan B) äger menubar-processen refresh-token-rotationen och
skriver färska access-tokens atomiskt till en delad cache-fil. Klient-
processer (uploader-daemon, SSE-consumer, framtida komponenter) läser
filen vid varje request.

Kontrakt:
  - Fil: ~/Library/Caches/MyMemory/access_token (0600, UTF-8)
  - Innehåll: en (1) JWT-sträng, inget whitespace utanför
  - Roterar: var ~10 min av TokenBrokerManager.swift
  - Saknas: menubar är inte igång, eller användaren inte inloggad

Funktioner:
  get_access_token()       — läser filen, returnerar None om saknas
  reload_access_token()    — dito, men markerat för användning efter 401
                             (syntax-parallell för läsbarhet i daemon-kod)

Plattforms-agnostiska: på Linux/dev används MYMEMORY_ACCESS_TOKEN-env
som override.
"""

import logging
import os
from pathlib import Path
from typing import Optional

LOGGER = logging.getLogger("BROKER_AUTH")

ACCESS_TOKEN_PATH = Path(
    os.path.expanduser("~/Library/Caches/MyMemory/access_token")
)


def get_access_token() -> Optional[str]:
    """Läs färsk access-token. None om broker inte skrivit filen än.

    Ordning: MYMEMORY_ACCESS_TOKEN-env > broker-fil. Env-overriden är för
    dev/test-scenarier där menubar-brokern inte kör.

    Retur-semantik:
      - str: färsk access-token (caller använder som Bearer)
      - None: normal "waiting"-situation (fil saknas, menubar inte igång)

    Läsfel på en fil som faktiskt finns är oväntat och loggas — men
    returnerar ändå None för att caller ska kunna mjuk-fallback till
    "försök igen senare" istället för att krascha uploadern.
    """
    env = os.environ.get("MYMEMORY_ACCESS_TOKEN")
    if env:
        return env

    if not ACCESS_TOKEN_PATH.exists():
        # Broker inte igång / användare inte inloggad. Normal situation,
        # logga inte — skulle dränka loggen i "waiting"-skrik.
        return None

    try:
        token = ACCESS_TOKEN_PATH.read_text(encoding="utf-8").strip()
    except OSError as exc:
        LOGGER.warning(
            f"Broker-fil finns men kunde inte läsas ({ACCESS_TOKEN_PATH}): {exc}"
        )
        return None

    return token if token else None


def reload_access_token() -> Optional[str]:
    """Alias för get_access_token — läsbarhet vid 401-retry i daemon-kod.

    Brokern skriver atomiskt (replace_item), så en enkel re-read räcker —
    ingen cache-invalidering behövs i klienten.
    """
    return get_access_token()
