"""
Central config loader för klient-sidan.

Princip 14 (Modellen är sanningen — serialisering är en projektion):
config persisteras som JSON i `~/Library/Application Support/MyMemory/
config.json`. Inga handskrivna fält-mappar — `json.load` returnerar
hela strukturen.

Princip 1 (två separata system, ingen shared-yta): klient-koden får
inte importera från server.utils.

Söker config i följande ordning:
1. MYMEMORY_CONFIG environment variable (full sökväg till config-fil)
2. ~/Library/Application Support/MyMemory/config.json (standardplats)

HARDFAIL om filen saknas. Ingen tyst fallback till legacy-yaml-pathen
— migration hanteras explicit av `client/utils/config_upgrade.py`,
triggad av Mac-appen vid version-bump.

Exempel:
    from client.utils.config_loader import get_config, get_config_path

    config = get_config()  # Returnerar dict
    path = get_config_path()  # Returnerar sökväg som str
"""

import json
import logging
import os
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

# Standardplats — princip 14 + observer-migration commit 7.
DEFAULT_CONFIG_PATH = Path.home() / "Library" / "Application Support" / "MyMemory" / "config.json"


def _find_config_path() -> Path:
    """
    Hittar config-fil enligt prioritetsordning.

    Returns:
        Path till config-fil

    Raises:
        FileNotFoundError om ingen config hittas
    """
    # 1. Explicit config-sökväg via env var
    if config_path := os.environ.get("MYMEMORY_CONFIG"):
        path = Path(config_path).expanduser()
        if path.exists():
            return path
        # HARDFAIL — princip 6. Om env-var pekar på saknad fil är det
        # konfigfel hos användaren, inte tillfälle för fallback.
        raise FileNotFoundError(
            f"MYMEMORY_CONFIG={config_path} pekar på fil som inte finns."
        )

    # 2. Standardplats
    if DEFAULT_CONFIG_PATH.exists():
        return DEFAULT_CONFIG_PATH

    raise FileNotFoundError(
        f"Kunde inte hitta config.json. Förväntade platser:\n"
        f"  1. $MYMEMORY_CONFIG\n"
        f"  2. {DEFAULT_CONFIG_PATH}\n"
        f"\n"
        f"Migration från legacy yaml-config sker via "
        f"`client/utils/config_upgrade.py`. Den triggas normalt av "
        f"Mac-appens SetupManager vid version-bump."
    )


@lru_cache(maxsize=1)
def get_config_path() -> str:
    """
    Returnerar sökväg till config-fil.

    Cachelagras för att undvika upprepade filsystem-operationer.
    """
    return str(_find_config_path())


@lru_cache(maxsize=1)
def get_config() -> dict:
    """
    Läser och returnerar config som dict.

    Cachelagras för att undvika upprepade fil-läsningar.

    Returns:
        dict med config-data

    Raises:
        FileNotFoundError om config saknas
        json.JSONDecodeError om config är felformaterad
    """
    config_path = get_config_path()
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    logger.debug(f"Config laddad från {config_path}")
    return config


def reload_config() -> dict:
    """
    Tvingar omladdning av config (rensar cache).

    Användbart om config ändrats under körning.
    """
    get_config.cache_clear()
    get_config_path.cache_clear()
    return get_config()
