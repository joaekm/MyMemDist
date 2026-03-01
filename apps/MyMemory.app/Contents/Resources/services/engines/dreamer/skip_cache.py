"""
Merge skip-cache management for Dreamer.

Persists LLM "don't merge" decisions to avoid re-evaluating
unchanged node pairs. Cache entries are invalidated when either
node's context_summary or edge_count changes.
"""

import hashlib
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


def load(config: dict) -> dict:
    """Load skip-cache from JSON file."""
    cache_file = os.path.expanduser(
        config.get('merge', {}).get(
            'skip_cache_file', '~/Library/Application Support/MyMemory/merge_skip_cache.json')
    )
    if not os.path.exists(cache_file):
        return {}
    try:
        with open(cache_file, 'r') as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        LOGGER.warning(f"Could not load merge skip-cache: {e}")
        return {}


def save(config: dict, cache: dict):
    """Save skip-cache to JSON file."""
    cache_file = os.path.expanduser(
        config.get('merge', {}).get(
            'skip_cache_file', '~/Library/Application Support/MyMemory/merge_skip_cache.json')
    )
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except OSError as e:
        LOGGER.error(f"Could not save merge skip-cache: {e}")


def compute_node_hash(node: dict, edge_count: int) -> str:
    """Compute hash for a node based on context_summary + edge_count."""
    props = node.get("properties", {})
    ctx = props.get("context_summary", "")
    raw = f"{ctx}|{edge_count}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def compute_pair_key(id_a: str, id_b: str) -> str:
    """Compute deterministic pair key (sorted)."""
    return "|".join(sorted([id_a, id_b]))
