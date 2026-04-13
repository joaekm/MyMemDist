"""
Keychain helper för macOS — säker lagring av PAT (Personal Access Token).

Använder `security` CLI istället för pyobjc keychain-bindings för att
slippa extra dependency. Funktionerna är no-ops på Linux (returnerar None).

Usage:
    from services.utils.keychain import get_pat, set_pat, delete_pat

    set_pat("eyJhbGc...")          # spara
    pat = get_pat()                # läs (None om saknas)
    delete_pat()                   # rensa
"""

import logging
import shutil
import subprocess
import sys

LOGGER = logging.getLogger("Keychain")

SERVICE_NAME = "MyMemory"
ACCOUNT_NAME = "uploader_pat"


def _is_macos() -> bool:
    return sys.platform == "darwin"


def _security_cli() -> str | None:
    """Returnera sökväg till `security` CLI om tillgänglig, annars None."""
    if not _is_macos():
        return None
    return shutil.which("security")


def get_pat() -> str | None:
    """Hämta PAT från Keychain. Returnerar None om saknas eller på Linux."""
    cli = _security_cli()
    if not cli:
        return None
    try:
        result = subprocess.run(
            [cli, "find-generic-password", "-s", SERVICE_NAME,
             "-a", ACCOUNT_NAME, "-w"],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        LOGGER.warning(f"Keychain lookup failed: {e}")
        return None

    if result.returncode != 0:
        # Item not found ger returncode=44, andra fel kan ge annat
        return None
    return result.stdout.strip() or None


def set_pat(token: str) -> bool:
    """Spara PAT i Keychain. Skriver över befintlig. Returnerar True vid lyckad."""
    cli = _security_cli()
    if not cli:
        LOGGER.warning("Keychain inte tillgängligt — set_pat ignored")
        return False
    if not token:
        LOGGER.warning("set_pat: tom token, ignored")
        return False
    try:
        result = subprocess.run(
            [cli, "add-generic-password", "-U",  # -U = update om finns
             "-s", SERVICE_NAME, "-a", ACCOUNT_NAME, "-w", token],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        LOGGER.error(f"Keychain set failed: {e}")
        return False

    if result.returncode != 0:
        LOGGER.error(f"Keychain set returnerade {result.returncode}: {result.stderr}")
        return False
    LOGGER.info("PAT lagrad i Keychain")
    return True


def delete_pat() -> bool:
    """Ta bort PAT från Keychain. Returnerar True om raderad eller saknades."""
    cli = _security_cli()
    if not cli:
        return False
    try:
        result = subprocess.run(
            [cli, "delete-generic-password",
             "-s", SERVICE_NAME, "-a", ACCOUNT_NAME],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        LOGGER.warning(f"Keychain delete failed: {e}")
        return False

    if result.returncode == 0:
        LOGGER.info("PAT borttagen från Keychain")
        return True
    if result.returncode == 44:
        # 44 = item not found, betraktas som lyckad delete
        return True
    LOGGER.warning(f"Keychain delete returnerade {result.returncode}: {result.stderr}")
    return False
