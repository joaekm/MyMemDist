#!/usr/bin/env python3
"""
Config Upgrade — populate template with protected values from existing config.

Called by SetupManager.swift during app upgrade to migrate user config
to new template structure while preserving API keys, owner info, etc.

Usage:
    python config_upgrade.py --template PATH --config PATH --version X.Y.Z
"""

import argparse
import logging
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML required")
    sys.exit(1)

LOGGER = logging.getLogger("ConfigUpgrade")

# --- Protected value extraction ---

# Placeholder → key path in old config
PLACEHOLDER_MAP = {
    "__APP_VERSION__": None,  # from --version arg
    "__OWNER_ID__": ("owner", "id"),
    "__OWNER_NAME__": ("owner", "profile", "full_name"),
    "__OWNER_ROLE__": ("owner", "profile", "role"),
    "__OWNER_ORG__": ("owner", "profile", "organization"),
    "__OWNER_LOCATION__": ("owner", "profile", "location"),
    "__ANTHROPIC_API_KEY__": ("ai_engine", "anthropic", "api_key"),
    "__GEMINI_API_KEY__": ("ai_engine", "gemini", "api_key"),
    "__SLACK_BOT_TOKEN__": ("slack", "bot_token"),
    "__GMAIL_LABEL__": ("google", "gmail", "target_label"),
}

# Non-placeholder protected values: key path in old config → (section_marker, key, indent)
PROTECTED_LINE_VALUES = {
    ("signal_feed", "intent_peer_secret"): ("signal_feed:", "intent_peer_secret", "  "),
}


def _get_nested(d: dict, path: tuple) -> Any:
    """Safely get a nested value from a dict."""
    current = d
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _replace_placeholders(template_text: str, old_config: dict, version: str) -> str:
    """Replace __PLACEHOLDER__ strings with values from old config."""
    result = template_text
    preserved = 0

    for placeholder, key_path in PLACEHOLDER_MAP.items():
        if key_path is None:
            # Version comes from CLI arg
            result = result.replace(placeholder, version)
            continue

        value = _get_nested(old_config, key_path)
        if value is not None and str(value).strip():
            result = result.replace(placeholder, str(value))
            preserved += 1
        else:
            # Replace with empty string to avoid leftover placeholders
            result = result.replace(placeholder, "")

    return result, preserved


def _inject_list(text: str, pattern: str, items: list, indent: str) -> str:
    """Replace a YAML list placeholder with actual items.

    Handles both empty list (key: []) and populated list formats.
    """
    if not items:
        return text

    # Build YAML list
    yaml_lines = []
    for item in items:
        # Quote strings that contain special YAML chars
        if isinstance(item, str):
            yaml_lines.append(f'{indent}  - "{item}"')
        else:
            yaml_lines.append(f'{indent}  - {item}')
    yaml_block = "\n".join(yaml_lines)

    # Replace empty list: "key: []"
    empty_pattern = f'{indent}{pattern}: []'
    if empty_pattern in text:
        text = text.replace(empty_pattern, f'{indent}{pattern}:\n{yaml_block}')
        return text

    # Replace populated default list (multi-line)
    regex = re.compile(
        rf'^({re.escape(indent)}{re.escape(pattern)}:\s*)\n'
        rf'((?:{re.escape(indent)}  - .+\n?)+)',
        re.MULTILINE
    )
    match = regex.search(text)
    if match:
        text = text[:match.start()] + f'{indent}{pattern}:\n{yaml_block}\n' + text[match.end():]

    return text


def _inject_line_value(text: str, section_marker: str, key: str, value: str, indent: str) -> str:
    """Replace a specific key's value in a YAML section."""
    if value is None or str(value).strip() == "":
        return text

    # Find the section, then find the key within it
    lines = text.split('\n')
    in_section = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == section_marker or stripped.startswith(section_marker):
            in_section = True
            continue
        if in_section:
            # Detect section end (non-indented line that isn't empty/comment)
            if stripped and not stripped.startswith('#') and not line.startswith(indent):
                in_section = False
                continue
            # Match the key
            key_pattern = f'{key}:'
            if stripped.startswith(key_pattern):
                # Preserve inline comment
                comment = ""
                if '#' in line:
                    comment_idx = line.index('#')
                    comment = "  " + line[comment_idx:]

                # Determine actual indentation of this line
                line_indent = line[:len(line) - len(line.lstrip())]
                # Quote value if it contains special chars
                if isinstance(value, str) and value:
                    lines[i] = f'{line_indent}{key}: "{value}"{comment}'
                else:
                    lines[i] = f'{line_indent}{key}: ""{comment}'
                return '\n'.join(lines)

    return text


def _inject_aliases(text: str, aliases: list) -> str:
    """Insert owner aliases after the owner.id line."""
    if not aliases:
        return text

    yaml_lines = [f'    - "{a}"' for a in aliases]
    alias_block = "  aliases:\n" + "\n".join(yaml_lines)

    # Find owner.id line and insert after it
    lines = text.split('\n')
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('id:') and i > 0:
            # Check we're in the owner section
            for j in range(i - 1, max(0, i - 5), -1):
                if lines[j].strip() == 'owner:' or lines[j].strip().startswith('owner:'):
                    # Insert aliases after the id line
                    lines.insert(i + 1, alias_block)
                    return '\n'.join(lines)

    return text


def _inject_protected_values(text: str, old_config: dict) -> tuple:
    """Inject non-placeholder protected values into template text."""
    count = 0

    # Owner skills
    old_skills = _get_nested(old_config, ("owner", "profile", "skills"))
    if old_skills and isinstance(old_skills, list):
        text = _inject_list(text, "skills", old_skills, "    ")
        count += 1

    # Owner aliases
    old_aliases = _get_nested(old_config, ("owner", "aliases"))
    if old_aliases and isinstance(old_aliases, list):
        text = _inject_aliases(text, old_aliases)
        count += 1

    # Slack channels
    old_channels = _get_nested(old_config, ("slack", "channels"))
    if old_channels and isinstance(old_channels, list):
        text = _inject_list(text, "channels", old_channels, "  ")
        count += 1

    # Gemini personal API key
    old_gemini_personal = _get_nested(old_config, ("ai_engine", "gemini", "api_key_personal"))
    if old_gemini_personal:
        text = _inject_line_value(text, "gemini:", "api_key_personal", old_gemini_personal, "    ")
        count += 1

    # OpenAI API key
    old_openai = _get_nested(old_config, ("ai_engine", "openai", "api_key"))
    if old_openai:
        text = _inject_line_value(text, "openai:", "api_key", old_openai, "    ")
        count += 1

    # Generic protected line values (secrets, tokens)
    for key_path, (section, key, indent) in PROTECTED_LINE_VALUES.items():
        old_value = _get_nested(old_config, key_path)
        if old_value:
            text = _inject_line_value(text, section, key, old_value, indent)
            count += 1

    return text, count


def upgrade_config(template_path: str, config_path: str, version: str) -> bool:
    """Upgrade config by populating template with protected values from existing config.

    1. Read template text (preserves comments)
    2. Read old config as YAML dict
    3. Replace placeholders with old values
    4. Inject protected lists/complex values
    5. Validate result
    6. Backup old config
    7. Write populated template as new config

    Returns True on success, False on failure.
    """
    # Read template
    if not os.path.exists(template_path):
        LOGGER.error(f"Template not found: {template_path}")
        return False

    with open(template_path, 'r', encoding='utf-8') as f:
        template_text = f.read()

    # Read old config
    if not os.path.exists(config_path):
        LOGGER.error(f"Config not found: {config_path}")
        return False

    with open(config_path, 'r', encoding='utf-8') as f:
        old_text = f.read()

    try:
        old_config = yaml.safe_load(old_text) or {}
    except yaml.YAMLError as e:
        LOGGER.error(f"Failed to parse old config: {e}")
        return False

    old_version = old_config.get("version", "?")

    # Phase 1: Replace placeholders
    result_text, placeholder_count = _replace_placeholders(template_text, old_config, version)

    # Phase 2: Inject protected values
    result_text, inject_count = _inject_protected_values(result_text, old_config)

    total_preserved = placeholder_count + inject_count

    # Phase 3: Validate
    try:
        result_config = yaml.safe_load(result_text)
        if not isinstance(result_config, dict):
            LOGGER.error("Validation failed: result is not a dict")
            return False
    except yaml.YAMLError as e:
        LOGGER.error(f"Validation failed: {e}")
        return False

    # Verify version was set
    if result_config.get("version") != version:
        LOGGER.error(f"Version mismatch: expected {version}, got {result_config.get('version')}")
        return False

    # Phase 4: Backup old config
    backup_path = config_path + ".pre-upgrade"
    try:
        shutil.copy2(config_path, backup_path)
    except OSError as e:
        LOGGER.error(f"Failed to create backup: {e}")
        return False

    # Phase 5: Write new config
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(result_text)
    except OSError as e:
        LOGGER.error(f"Failed to write config: {e}")
        # Restore backup
        try:
            shutil.copy2(backup_path, config_path)
        except OSError as restore_err:
            LOGGER.error(f"Failed to restore backup: {restore_err}")
        return False

    print(f"Upgraded config {old_version} -> {version} ({total_preserved} values preserved)")
    return True


def main():
    parser = argparse.ArgumentParser(description="Upgrade config from template")
    parser.add_argument("--template", required=True, help="Path to template YAML")
    parser.add_argument("--config", required=True, help="Path to existing config YAML")
    parser.add_argument("--version", required=True, help="New version string")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    success = upgrade_config(args.template, args.config, args.version)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
