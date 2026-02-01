#!/usr/bin/env python3
"""
MyMemory Setup Script (Standalone)

Fristående installationsscript — laddar ner enbart runtime-filer
från GitHub och konfigurerar MyMemory.

Användning:
    curl -sL https://raw.githubusercontent.com/joaekm/MyMemDist/main/setup_mymemory.py -o setup_mymemory.py
    python3 setup_mymemory.py

Kräver: Python 3.12+, internetåtkomst
Dependencies: Inga (använder enbart Python stdlib)
"""

import os
import sys
import shutil
import subprocess
import tarfile
import tempfile
import webbrowser
import json
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# --- CONSTANTS ---

# GitHub-repo för distribution (publikt).
# Ändra REPO_OWNER/REPO_NAME om repot byter namn.
REPO_OWNER = "joaekm"
REPO_NAME = "MyMemDist"
GITHUB_TARBALL_URL = f"https://github.com/{REPO_OWNER}/{REPO_NAME}/archive/refs/heads/main.tar.gz"
TARBALL_PREFIX = f"{REPO_NAME}-main/"  # GitHub prepends this to all paths in the archive

MYMEMORY_DATA = Path.home() / "MyMemory"

DATA_DIRECTORIES = [
    MYMEMORY_DATA / "Assets" / "Documents",
    MYMEMORY_DATA / "Assets" / "Recordings",
    MYMEMORY_DATA / "Assets" / "Transcripts",
    MYMEMORY_DATA / "Assets" / "Slack",
    MYMEMORY_DATA / "Assets" / "Calendar",
    MYMEMORY_DATA / "Assets" / "Mail",
    MYMEMORY_DATA / "Assets" / "AIGenerated",
    MYMEMORY_DATA / "Assets" / "Failed",
    MYMEMORY_DATA / "Index" / "VectorDB",
    MYMEMORY_DATA / "Index" / "GraphDB",
    MYMEMORY_DATA / "Lake",
    MYMEMORY_DATA / "Logs",
    MYMEMORY_DATA / "MeetingBuffer",
    MYMEMORY_DATA / "Credentials",
    MYMEMORY_DATA / "MemoryDrop",
]

# Vilka toppnivå-filer och kataloger som ska extraheras.
# Allt under services/ inkluderas automatiskt.
RUNTIME_TOP_FILES = {
    "start_services.py",
    "requirements.txt",
}

RUNTIME_DIRS = {
    "config/",       # Alla config-filer
    "services/",     # All runtime-kod
}


# --- UTILITIES ---

def print_header(text):
    width = 60
    print(f"\n{'=' * width}")
    print(f"  {text}")
    print(f"{'=' * width}\n")


def print_step(number, total, text):
    print(f"\n--- Steg {number}/{total}: {text} ---\n")


def ask(prompt, default=""):
    if default:
        result = input(f"{prompt} [{default}]: ").strip()
        return result if result else default
    return input(f"{prompt}: ").strip()


def ask_yes_no(prompt, default=True):
    suffix = "[J/n]" if default else "[j/N]"
    result = input(f"{prompt} {suffix}: ").strip().lower()
    if not result:
        return default
    return result in ("j", "ja", "y", "yes")


def success(text):
    print(f"  OK  {text}")


def warn(text):
    print(f"  !   {text}")


def fail(text):
    print(f"  X   {text}")


# --- STEP 0: System Check ---

def check_system():
    print_step(0, 9, "Systemkontroll")

    # Python version
    major, minor = sys.version_info[:2]
    if major < 3 or (major == 3 and minor < 12):
        fail(f"Python {major}.{minor} — kräver 3.12+")
        print("    Installera Python 3.12: https://www.python.org/downloads/")
        sys.exit(1)
    success(f"Python {major}.{minor}")

    # ffmpeg (optional)
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path:
        success(f"ffmpeg: {ffmpeg_path}")
    else:
        warn("ffmpeg saknas — transkribering av ljudfiler fungerar inte")
        print("    macOS: brew install ffmpeg")
        print("    Linux: sudo apt-get install ffmpeg")


# --- STEP 1: Download Code ---

def choose_install_dir():
    """Fråga användaren var koden ska installeras."""
    default = Path.cwd() / "MyMemory"
    path_str = ask("  Installationskatalog", str(default))
    return Path(path_str).resolve()


def download_and_extract(install_dir):
    print_step(1, 9, "Ladda ner kod")

    if install_dir.exists() and any(install_dir.iterdir()):
        if not ask_yes_no(f"  {install_dir} finns redan. Skriva över kod-filer?", default=False):
            success("Behåller befintlig kod")
            return install_dir

    print(f"  Laddar ner från GitHub...")

    # Download tarball to temp file
    tmp_file = None
    try:
        req = Request(GITHUB_TARBALL_URL, headers={"User-Agent": "MyMemory-Setup/1.0"})
        response = urlopen(req, timeout=60)
        tmp_fd, tmp_file = tempfile.mkstemp(suffix=".tar.gz")
        with os.fdopen(tmp_fd, 'wb') as f:
            total = 0
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                f.write(chunk)
                total += len(chunk)
        success(f"Nedladdat ({total // 1024} KB)")
    except (URLError, HTTPError) as e:
        fail(f"Kunde inte ladda ner: {e}")
        print("    Kontrollera internetanslutningen och försök igen.")
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)
        sys.exit(1)

    # Extract only runtime files
    print("  Extraherar runtime-filer...")
    extracted = 0
    try:
        with tarfile.open(tmp_file, "r:gz") as tar:
            for member in tar.getmembers():
                # Strip GitHub prefix (e.g. "MyMemory-main/services/utils/...")
                if not member.name.startswith(TARBALL_PREFIX):
                    continue
                relative_path = member.name[len(TARBALL_PREFIX):]

                # Skip empty path (the root dir itself)
                if not relative_path:
                    continue

                # Skip directories — we create them on demand
                if member.isdir():
                    continue

                # Check if this file should be included
                if not _should_include(relative_path):
                    continue

                # Determine output path
                output_path = install_dir / relative_path
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Extract file content
                src = tar.extractfile(member)
                if src is None:
                    continue
                output_path.write_bytes(src.read())
                extracted += 1

        success(f"{extracted} filer extraherade till {install_dir}")

    except tarfile.TarError as e:
        fail(f"Kunde inte extrahera: {e}")
        sys.exit(1)
    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)

    # Verify critical files exist
    for f in ["start_services.py", "requirements.txt", "config/my_mem_config.template.yaml"]:
        if not (install_dir / f).exists():
            fail(f"Kritisk fil saknas: {f}")
            sys.exit(1)

    return install_dir


def _should_include(relative_path):
    """Avgör om en fil från arkivet ska extraheras."""
    # Top-level files
    if relative_path in RUNTIME_TOP_FILES:
        return True

    # Files under runtime directories
    for d in RUNTIME_DIRS:
        if relative_path.startswith(d):
            return True

    return False


# --- STEP 2: Virtual Environment ---

def setup_venv(install_dir):
    print_step(2, 9, "Virtual Environment")

    venv_path = install_dir / "venv"
    requirements_path = install_dir / "requirements.txt"

    if not requirements_path.exists():
        fail(f"requirements.txt saknas i {install_dir}")
        sys.exit(1)

    if venv_path.exists():
        success(f"venv finns redan: {venv_path}")
    else:
        print(f"  Skapar venv i {venv_path}...")
        subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)
        success("venv skapad")

    # Determine pip/python paths
    if sys.platform == "win32":
        pip_path = venv_path / "Scripts" / "pip"
        python_path = venv_path / "Scripts" / "python"
    else:
        pip_path = venv_path / "bin" / "pip"
        python_path = venv_path / "bin" / "python"

    # Install requirements
    print("  Installerar dependencies (kan ta ett par minuter)...")
    result = subprocess.run(
        [str(pip_path), "install", "-r", str(requirements_path), "-q"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        fail("pip install misslyckades")
        print(result.stderr[-500:] if len(result.stderr) > 500 else result.stderr)
        sys.exit(1)
    success("Dependencies installerade")

    # Verify critical imports
    for module in ["anthropic", "chromadb", "duckdb", "yaml"]:
        result = subprocess.run(
            [str(python_path), "-c", f"import {module}"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            success(f"Import: {module}")
        else:
            fail(f"Import: {module} — {result.stderr.strip()[:100]}")

    return str(python_path)


# --- STEP 3: Data Directory Structure ---

def create_directories():
    print_step(3, 9, "Datamappar")

    created = 0
    for d in DATA_DIRECTORIES:
        if d.exists():
            continue
        d.mkdir(parents=True, exist_ok=True)
        created += 1

    if created > 0:
        success(f"{created} mappar skapade under {MYMEMORY_DATA}")
    else:
        success(f"Alla mappar finns redan under {MYMEMORY_DATA}")


# --- STEP 4: Owner Profile ---

def get_owner_profile():
    print_step(4, 9, "Ägarprofil")
    print("  Denna information hjälper AI:n att förstå kontext.\n")

    name = ask("  Ditt namn", "")
    while not name:
        name = ask("  Ditt namn (obligatoriskt)")

    role = ask("  Din roll", "")
    org = ask("  Din organisation", "")
    location = ask("  Din plats", "Stockholm, Sverige")

    owner_id = name.lower().replace(" ", ".")

    return {
        "__OWNER_ID__": owner_id,
        "__OWNER_NAME__": name,
        "__OWNER_ROLE__": role,
        "__OWNER_ORG__": org,
        "__OWNER_LOCATION__": location,
    }


# --- STEP 5: API Keys ---

def get_api_keys(python_path):
    print_step(5, 9, "API-nycklar (obligatoriska)")

    replacements = {}

    # Anthropic
    print("\n  Anthropic (Claude) — https://console.anthropic.com/settings/keys")
    while True:
        key = ask("  Anthropic API-nyckel")
        if not key:
            warn("Anthropic-nyckel krävs för entity extraction och metadata")
            continue
        if _validate_key_via_venv(python_path, "anthropic", key):
            replacements["__ANTHROPIC_API_KEY__"] = key
            break

    # Gemini
    print("\n  Google Gemini — https://aistudio.google.com/apikey")
    while True:
        key = ask("  Gemini API-nyckel")
        if not key:
            warn("Gemini-nyckel krävs för transkribering")
            continue
        if _validate_key_via_venv(python_path, "gemini", key):
            replacements["__GEMINI_API_KEY__"] = key
            break

    return replacements


def _validate_key_via_venv(python_path, provider, key):
    """Validera API-nyckel genom att köra test i venv-python."""
    if provider == "anthropic":
        code = (
            "import anthropic; "
            f"c = anthropic.Anthropic(api_key='{key}'); "
            "c.messages.create(model='claude-haiku-4-5-20251001', max_tokens=10, "
            "messages=[{'role': 'user', 'content': 'hi'}]); "
            "print('OK')"
        )
    elif provider == "gemini":
        code = (
            "from google import genai; "
            f"c = genai.Client(api_key='{key}'); "
            "c.models.generate_content(model='gemini-2.0-flash', contents='hi'); "
            "print('OK')"
        )
    else:
        return True

    result = subprocess.run(
        [python_path, "-c", code],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode == 0 and "OK" in result.stdout:
        success(f"{provider.capitalize()}-nyckel fungerar")
        return True
    else:
        error_msg = result.stderr.strip().split('\n')[-1] if result.stderr else "Okänt fel"
        fail(f"{provider.capitalize()}-nyckel ogiltig: {error_msg[:100]}")
        if ask_yes_no("  Spara ändå?", default=False):
            return True
        return False


# --- STEP 6: Optional Integrations ---

def get_optional_integrations(python_path):
    print_step(6, 9, "Valfria integrationer")

    replacements = {}

    # Slack
    print("\n  Slack-integration samlar meddelanden från utvalda kanaler.")
    if ask_yes_no("  Aktivera Slack?", default=False):
        replacements.update(_setup_slack(python_path))
    else:
        replacements["__SLACK_BOT_TOKEN__"] = ""
        success("Slack inaktiverad")

    # Google (Gmail + Calendar)
    print("\n  Google-integration samlar mail (Gmail) och kalenderhändelser.")
    if ask_yes_no("  Aktivera Gmail & Kalender?", default=False):
        replacements.update(_setup_google())
    else:
        replacements["__GOOGLE_CREDENTIALS_PATH__"] = ""
        replacements["__GOOGLE_TOKEN_PATH__"] = ""
        replacements["__GMAIL_LABEL__"] = ""
        success("Gmail & Kalender inaktiverade")

    return replacements


def _setup_slack(python_path):
    replacements = {}
    print("\n  Skapa en Slack-app: https://api.slack.com/apps")
    print("  OAuth scopes: channels:history, channels:read, users:read")
    if ask_yes_no("  Öppna Slack API i webbläsaren?", default=False):
        webbrowser.open("https://api.slack.com/apps")

    token = ask("  Slack Bot Token (xoxb-...)")
    if token:
        # Validate via venv
        code = (
            "from slack_sdk import WebClient; "
            f"c = WebClient(token='{token}'); "
            "r = c.auth_test(); "
            "print(f\"OK {r.get('team', '?')}\")"
        )
        result = subprocess.run(
            [python_path, "-c", code],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0 and "OK" in result.stdout:
            team = result.stdout.strip().replace("OK ", "")
            success(f"Slack-token fungerar (workspace: {team})")
            replacements["__SLACK_BOT_TOKEN__"] = token
        else:
            fail("Slack-token ogiltig")
            if ask_yes_no("  Spara ändå?", default=False):
                replacements["__SLACK_BOT_TOKEN__"] = token
            else:
                replacements["__SLACK_BOT_TOKEN__"] = ""
    else:
        replacements["__SLACK_BOT_TOKEN__"] = ""

    return replacements


def _setup_google():
    replacements = {}

    print("\n  Google OAuth kräver manuell konfiguration:")
    print("  1. Skapa projekt i Google Cloud Console")
    print("  2. Aktivera Gmail API + Calendar API")
    print("  3. Konfigurera OAuth consent screen")
    print("  4. Skapa OAuth 2.0 credentials (Desktop app)")
    print("  5. Ladda ner client_secret.json")

    if ask_yes_no("  Öppna Google Cloud Console i webbläsaren?", default=False):
        webbrowser.open("https://console.cloud.google.com/apis/credentials")

    creds_source = ask("  Sökväg till din client_secret.json (eller Enter för att skippa)")
    if creds_source and os.path.exists(creds_source):
        dest = MYMEMORY_DATA / "Credentials" / "client_secret.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(creds_source, dest)
        success(f"Kopierad till {dest}")
        replacements["__GOOGLE_CREDENTIALS_PATH__"] = "~/MyMemory/Credentials/client_secret.json"
        replacements["__GOOGLE_TOKEN_PATH__"] = "~/MyMemory/Credentials/token.json"
    else:
        if creds_source:
            warn(f"Filen finns inte: {creds_source}")
        replacements["__GOOGLE_CREDENTIALS_PATH__"] = ""
        replacements["__GOOGLE_TOKEN_PATH__"] = ""
        warn("Google-integration inaktiverad. Konfigurera manuellt senare.")

    label = ask("  Gmail-label att bevaka", "MyMemory") if replacements.get("__GOOGLE_CREDENTIALS_PATH__") else ""
    replacements["__GMAIL_LABEL__"] = label

    return replacements


# --- STEP 7: Generate Config ---

def generate_config(install_dir, replacements):
    print_step(7, 9, "Generera konfiguration")

    template_path = install_dir / "config" / "my_mem_config.template.yaml"
    config_path = install_dir / "config" / "my_mem_config.yaml"

    if not template_path.exists():
        fail(f"Config-template saknas: {template_path}")
        sys.exit(1)

    if config_path.exists():
        if not ask_yes_no(f"  Config finns redan. Skriva över?", default=False):
            warn("Behåller befintlig config")
            return

    template = template_path.read_text(encoding="utf-8")

    for placeholder, value in replacements.items():
        template = template.replace(placeholder, value)

    config_path.write_text(template, encoding="utf-8")
    success(f"Config sparad: {config_path}")

    # Summary
    print("\n  Konfigurationssammanfattning:")
    print(f"    Ägare:     {replacements.get('__OWNER_NAME__', '?')}")
    print(f"    Anthropic: {'Konfigurerad' if replacements.get('__ANTHROPIC_API_KEY__') else 'Saknas'}")
    print(f"    Gemini:    {'Konfigurerad' if replacements.get('__GEMINI_API_KEY__') else 'Saknas'}")
    print(f"    Slack:     {'Konfigurerad' if replacements.get('__SLACK_BOT_TOKEN__') else 'Inaktiv'}")
    google_creds = replacements.get('__GOOGLE_CREDENTIALS_PATH__', '')
    print(f"    Google:    {'Konfigurerad' if google_creds else 'Inaktiv'}")


# --- STEP 8: Claude Desktop MCP ---

def setup_claude_desktop(install_dir):
    print_step(8, 9, "Claude Desktop MCP (valfritt)")

    if not ask_yes_no("  Konfigurera Claude Desktop MCP-integration?", default=True):
        return

    mcp_script = install_dir / "services" / "agents" / "mymem_mcp.py"

    # Use venv python
    if sys.platform == "win32":
        python_path = str(install_dir / "venv" / "Scripts" / "python")
    else:
        python_path = str(install_dir / "venv" / "bin" / "python")

    config_entry = {
        "mcpServers": {
            "MyMemory": {
                "command": python_path,
                "args": [str(mcp_script)]
            }
        }
    }

    print(f"\n  Lägg till detta i din Claude Desktop config:\n")
    print(json.dumps(config_entry, indent=2))

    # macOS: erbjud att skriva direkt
    if sys.platform == "darwin":
        claude_config_path = Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
        if ask_yes_no(f"\n  Skriva direkt till {claude_config_path}?", default=False):
            try:
                existing = {}
                if claude_config_path.exists():
                    existing = json.loads(claude_config_path.read_text())

                if "mcpServers" not in existing:
                    existing["mcpServers"] = {}

                existing["mcpServers"]["MyMemory"] = config_entry["mcpServers"]["MyMemory"]

                claude_config_path.parent.mkdir(parents=True, exist_ok=True)
                claude_config_path.write_text(json.dumps(existing, indent=2))
                success("Claude Desktop config uppdaterad")
                print("    Starta om Claude Desktop för att aktivera.")
            except Exception as e:
                fail(f"Kunde inte skriva: {e}")
                print("    Kopiera JSON-snippeten ovan manuellt.")


# --- STEP 9: Validation ---

def validate_setup(install_dir):
    print_step(9, 9, "Validering")

    # Check critical files
    critical = [
        "start_services.py",
        "requirements.txt",
        "config/my_mem_config.template.yaml",
        "config/graph_schema_template.json",
        "config/services_prompts.yaml",
        "services/engines/ingestion_engine.py",
        "services/agents/mymem_mcp.py",
        "services/utils/graph_service.py",
    ]
    missing_code = [f for f in critical if not (install_dir / f).exists()]
    if missing_code:
        fail(f"{len(missing_code)} kritiska filer saknas:")
        for f in missing_code:
            print(f"      {f}")
    else:
        success("Alla kritiska filer finns")

    # Check data directories
    missing_dirs = [d for d in DATA_DIRECTORIES if not d.exists()]
    if missing_dirs:
        fail(f"{len(missing_dirs)} datamappar saknas")
    else:
        success("Alla datamappar finns")

    # Check config
    config_path = install_dir / "config" / "my_mem_config.yaml"
    if config_path.exists():
        success("Config-fil finns")
        content = config_path.read_text()
        placeholders = [p for p in ["__ANTHROPIC_API_KEY__", "__GEMINI_API_KEY__"] if p in content]
        if placeholders:
            fail(f"Oersatta placeholders: {', '.join(placeholders)}")
    else:
        fail("Config-fil saknas!")

    # Next steps
    print("\n" + "=" * 60)
    print("  Setup klar!")
    print("=" * 60)
    print(f"""
  Nästa steg:

  1. Aktivera din venv:
     source {install_dir}/venv/bin/activate

  2. Starta MyMemory:
     cd {install_dir}
     python start_services.py

  3. Droppa filer i ~/MyMemory/MemoryDrop/ för ingestion

  4. Använd Claude Desktop med MCP för att söka i ditt minne
""")


# --- MAIN ---

def main():
    print_header("MyMemory Setup")
    print("  Välkommen! Detta script installerar och konfigurerar MyMemory.")
    print("  Laddar ner enbart de filer som behövs för att köra systemet.\n")

    # Step 0: System check
    check_system()

    # Choose install directory
    install_dir = choose_install_dir()

    # Step 1: Download
    download_and_extract(install_dir)

    # Step 2: Virtual environment
    python_path = setup_venv(install_dir)

    # Step 3: Data directories
    create_directories()

    # Step 4-6: Configuration
    replacements = {}
    replacements.update(get_owner_profile())
    replacements.update(get_api_keys(python_path))
    replacements.update(get_optional_integrations(python_path))

    # Step 7: Generate config
    generate_config(install_dir, replacements)

    # Step 8: Claude Desktop
    setup_claude_desktop(install_dir)

    # Step 9: Validate
    validate_setup(install_dir)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Avbrutet av användare.")
        sys.exit(0)
