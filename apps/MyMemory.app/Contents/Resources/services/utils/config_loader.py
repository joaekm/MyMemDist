"""
Central config loader för MyMemory.

Söker config i följande ordning:
1. MYMEMORY_CONFIG environment variable (full sökväg till config-fil)
2. MYMEMORY_HOME environment variable + /Settings/my_mem_config.yaml
3. ~/MyMemory/Settings/my_mem_config.yaml (standardplats)

Exempel:
    from services.utils.config_loader import get_config, get_config_path

    config = get_config()  # Returnerar dict
    path = get_config_path()  # Returnerar sökväg som str
"""

import os
import yaml
import logging
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

# Standardplatser
DEFAULT_HOME = Path.home() / "MyMemory"
DEFAULT_CONFIG_FILENAME = "my_mem_config.yaml"
DEFAULT_SETTINGS_DIR = "Settings"


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
        logger.warning(f"MYMEMORY_CONFIG={config_path} finns inte, provar nästa")

    # 2. Home-katalog via env var
    if home := os.environ.get("MYMEMORY_HOME"):
        path = Path(home).expanduser() / DEFAULT_SETTINGS_DIR / DEFAULT_CONFIG_FILENAME
        if path.exists():
            return path
        logger.warning(f"MYMEMORY_HOME={home} finns, men config saknas på {path}")

    # 3. Standardplats
    path = DEFAULT_HOME / DEFAULT_SETTINGS_DIR / DEFAULT_CONFIG_FILENAME
    if path.exists():
        return path

    # 4. Fallback för utveckling/bakåtkompatibilitet
    # Kolla project root (där services-mappen ligger)
    project_root = Path(__file__).parent.parent.parent
    legacy_path = project_root / "config" / DEFAULT_CONFIG_FILENAME
    if legacy_path.exists():
        logger.info(f"Använder legacy config-plats: {legacy_path}")
        return legacy_path

    raise FileNotFoundError(
        f"Kunde inte hitta {DEFAULT_CONFIG_FILENAME}. "
        f"Förväntade platser:\n"
        f"  1. $MYMEMORY_CONFIG\n"
        f"  2. $MYMEMORY_HOME/Settings/{DEFAULT_CONFIG_FILENAME}\n"
        f"  3. {DEFAULT_HOME / DEFAULT_SETTINGS_DIR / DEFAULT_CONFIG_FILENAME}\n"
        f"  4. {legacy_path} (legacy)"
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
        yaml.YAMLError om config är felformaterad
    """
    config_path = get_config_path()
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

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


def get_mymemory_home() -> Path:
    """
    Returnerar MyMemory home-katalog.

    Används för att hitta andra resurser relativt till home.
    """
    if home := os.environ.get("MYMEMORY_HOME"):
        return Path(home).expanduser()
    return DEFAULT_HOME


# Convenience-funktioner för vanliga paths
def get_settings_path() -> Path:
    """Returnerar Settings-mappen."""
    return get_mymemory_home() / DEFAULT_SETTINGS_DIR


def get_index_path() -> Path:
    """Returnerar Index-mappen."""
    return get_mymemory_home() / "Index"


def get_lake_path() -> Path:
    """Returnerar Lake-mappen."""
    return get_mymemory_home() / "Lake"


def get_assets_path() -> Path:
    """Returnerar Assets-mappen."""
    return get_mymemory_home() / "Assets"


@lru_cache(maxsize=1)
def get_prompts() -> dict:
    """
    Läser och returnerar prompts-config.

    Prompts ligger alltid relativt till main config:
    - Om config är i ~/MyMemory/Settings/ → ~/MyMemory/Settings/services_prompts.yaml
    - Om config är i project/config/ → project/config/services_prompts.yaml
    """
    config_path = Path(get_config_path())
    config_dir = config_path.parent

    # Sök efter prompts-fil
    for name in ['services_prompts.yaml', 'service_prompts.yaml']:
        prompts_path = config_dir / name
        if prompts_path.exists():
            with open(prompts_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)

    logger.warning(f"Prompts-fil saknas i {config_dir}")
    return {}


def get_expanded_paths() -> dict:
    """
    Returnerar paths från config med expanderade ~-sökvägar.

    Convenience-funktion för kod som behöver färdig-expanderade paths.
    """
    config = get_config()
    paths = config.get('paths', {})
    return {k: os.path.expanduser(v) for k, v in paths.items()}
