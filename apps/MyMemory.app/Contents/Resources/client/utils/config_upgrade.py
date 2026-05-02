#!/usr/bin/env python3
"""
Config Upgrade — merge user values into JSON template.

Princip 14 (Modellen är sanningen — serialisering är en projektion):
config persisteras som JSON. Upgrade-flödet är ren dict-merge. Den
gamla yaml-text-manipulationen (placeholder-replace, _inject_list,
_inject_line_value) är riven — listor och nested-strukturer hanteras
naturligt av deep-merge.

Hanterar tre scenarier:
  1. Bara JSON finns på target → dict-merge med template, skriv tillbaka
  2. Bara legacy YAML finns → engångs-migration: läs yaml, merge med
     template, skriv som JSON på target. YAML lämnas orörd för commit 8
     (~/MyMemory-rensning).
  3. Inget finns → första install? Skriv template som JSON. (Vanligtvis
     hanterad av SetupManager.generateConfig istället.)

HARDFAIL i alla felfall — princip 6.

Usage:
    python config_upgrade.py \\
        --template PATH/to/template.json \\
        --config PATH/to/target.json \\
        --legacy-yaml PATH/to/legacy.yaml \\
        --version X.Y.Z
"""

import argparse
import json
import logging
import os
import shutil
import sys
from typing import Any

LOGGER = logging.getLogger("ConfigUpgrade")


def _deep_merge(template: dict, user: dict) -> dict:
    """
    Deep-merge user-värden in i template.

    Princip:
    - Template definierar struktur + defaults
    - User-värden överskrider template där de finns
    - Listor från user bevaras (inte template-defaults), eftersom användaren
      kan ha ändrat dem (t.ex. selected_calendars, slack.channels)
    - Nya nycklar från template läggs in (det är poängen med upgrade)
    - Nycklar som finns i user men inte i template ignoreras (de är borta
      ur modellen)

    Returnerar nytt dict, modifierar inget input.
    """
    result: dict[str, Any] = {}
    for key, template_value in template.items():
        if key not in user:
            # Nyckel finns bara i template → använd template-värdet
            result[key] = template_value
            continue

        user_value = user[key]

        if isinstance(template_value, dict) and isinstance(user_value, dict):
            # Båda är dicts → rekursiv merge
            result[key] = _deep_merge(template_value, user_value)
        else:
            # User-värdet vinner (inkl. listor, scalars, None)
            result[key] = user_value
    return result


def _load_user_config(json_path: str, legacy_yaml_path: str | None) -> tuple[dict, str]:
    """
    Hitta och läs användarens nuvarande config.

    Returnerar (dict, source) där source är "json" eller "yaml-legacy" eller
    "none". HARDFAIL om en path finns men inte kan parsas.
    """
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                return json.load(f), "json"
            except json.JSONDecodeError as e:
                raise SystemExit(
                    f"FAIL: existerande config på {json_path} är inte giltig "
                    f"JSON: {e}. Kör inte upgrade — användaren måste fixa "
                    f"manuellt eller restaurera från .pre-upgrade-backup."
                )

    if legacy_yaml_path and os.path.exists(legacy_yaml_path):
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError:
            raise SystemExit(
                "FAIL: legacy yaml-migration kräver PyYAML. Installera via "
                "venv eller migrera manuellt."
            )
        with open(legacy_yaml_path, "r", encoding="utf-8") as f:
            try:
                user_config = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                raise SystemExit(
                    f"FAIL: legacy yaml på {legacy_yaml_path} kan inte parsas: "
                    f"{e}"
                )
        return user_config, "yaml-legacy"

    return {}, "none"


def upgrade_config(
    template_path: str,
    config_path: str,
    legacy_yaml_path: str | None,
    version: str,
) -> bool:
    """
    Merga template + användar-config, skriv resultat som JSON.

    Princip 14: ingen text-manipulation, ingen handskriven fält-mappning.
    Bara dict-merge mellan två giltiga JSON-strukturer.

    Returnerar True vid lyckad körning, raise SystemExit vid HARDFAIL.
    """
    # Read template
    if not os.path.exists(template_path):
        raise SystemExit(f"FAIL: template saknas på {template_path}")

    with open(template_path, "r", encoding="utf-8") as f:
        try:
            template = json.load(f)
        except json.JSONDecodeError as e:
            raise SystemExit(f"FAIL: template är inte giltig JSON: {e}")

    user_config, source = _load_user_config(config_path, legacy_yaml_path)
    old_version = user_config.get("version", "?") if user_config else "?"

    # Merge: template definierar struktur, user-värden vinner
    merged = _deep_merge(template, user_config)

    # Sätt nya version-strängen
    merged["version"] = version

    # Backup existerande JSON om den finns (innan vi skriver över)
    if os.path.exists(config_path):
        backup_path = config_path + ".pre-upgrade"
        try:
            shutil.copy2(config_path, backup_path)
        except OSError as e:
            raise SystemExit(f"FAIL: kunde inte skapa backup: {e}")

    # Säkerställ target-katalogen
    config_dir = os.path.dirname(config_path)
    if config_dir:
        os.makedirs(config_dir, exist_ok=True)

    # Skriv som pretty JSON (deterministisk ordning för diff-läsbarhet)
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, sort_keys=True, ensure_ascii=False)
            f.write("\n")
    except OSError as e:
        raise SystemExit(f"FAIL: kunde inte skriva config: {e}")

    if source == "yaml-legacy":
        print(
            f"Migrated yaml→json + upgraded {old_version} → {version} "
            f"(target={config_path}, source={legacy_yaml_path})"
        )
    elif source == "json":
        print(f"Upgraded {old_version} → {version} (target={config_path})")
    else:
        print(f"Created fresh config from template ({version}) at {config_path}")

    return True


def main():
    parser = argparse.ArgumentParser(description="Merge template into user config (JSON)")
    parser.add_argument("--template", required=True, help="Path to template JSON")
    parser.add_argument(
        "--config", required=True, help="Path to target config JSON"
    )
    parser.add_argument(
        "--legacy-yaml",
        required=False,
        default=None,
        help="Path to legacy yaml for one-time migration (optional)",
    )
    parser.add_argument("--version", required=True, help="New version string")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    upgrade_config(args.template, args.config, args.legacy_yaml, args.version)
    sys.exit(0)


if __name__ == "__main__":
    main()
