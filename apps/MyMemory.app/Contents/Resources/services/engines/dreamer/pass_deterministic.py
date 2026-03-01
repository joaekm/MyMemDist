"""
Dreamer Pass 1: DETERMINISTIC cleanup (no LLM).

Removes schema-invalid edges, self-aliases, and UUID-aliases.
Free operations that improve graph quality without LLM cost.
"""

import json
import logging
import re

LOGGER = logging.getLogger(__name__)


def count_issues(graph_service, schema_validator, config: dict) -> dict:
    """Count deterministic issues without modifying the graph (for dry-run)."""
    result = {"invalid_edges": 0, "self_aliases": 0, "uuid_aliases": 0}

    if config.get('deterministic', {}).get('invalid_edge_cleanup', True):
        result["invalid_edges"] = _count_invalid_edges(graph_service, schema_validator)

    if config.get('deterministic', {}).get('self_alias_cleanup', True):
        result["self_aliases"] = _count_self_aliases(graph_service)

    if config.get('deterministic', {}).get('uuid_alias_cleanup', True):
        result["uuid_aliases"] = _count_uuid_aliases(graph_service)

    return result


def run(graph_service, schema_validator, config: dict, dry_run: bool) -> dict:
    """Run deterministic cleanup pass. Returns stats dict."""
    stats = {"invalid_edges": 0, "self_aliases": 0, "uuid_aliases": 0}

    if dry_run:
        stats = count_issues(graph_service, schema_validator, config)
        LOGGER.info(f"  [DRY] Deterministic: {stats['invalid_edges']} invalid edges, "
                    f"{stats['self_aliases']} self-aliases, {stats['uuid_aliases']} UUID-aliases")
        return stats

    if config.get('deterministic', {}).get('invalid_edge_cleanup', True):
        stats["invalid_edges"] = _cleanup_invalid_edges(graph_service, schema_validator)

    if config.get('deterministic', {}).get('self_alias_cleanup', True):
        stats["self_aliases"] = _cleanup_self_aliases(graph_service)

    if config.get('deterministic', {}).get('uuid_alias_cleanup', True):
        stats["uuid_aliases"] = _cleanup_uuid_aliases(graph_service)

    return stats


# --- Internal helpers ---

def _count_invalid_edges(graph_service, schema_validator) -> int:
    count = 0
    rows = graph_service.conn.execute(
        "SELECT source, target, edge_type FROM edges WHERE edge_type != 'MENTIONS' AND edge_type != 'DEALS_WITH'"
    ).fetchall()
    for source_id, target_id, edge_type in rows:
        source_node = graph_service.get_node(source_id)
        target_node = graph_service.get_node(target_id)
        if not source_node or not target_node:
            count += 1
            continue
        nodes_map = {
            source_id: source_node.get("type", "Unknown"),
            target_id: target_node.get("type", "Unknown"),
        }
        edge = {"source": source_id, "target": target_id, "type": edge_type}
        ok, msg = schema_validator.validate_edge_structure(edge, nodes_map)
        if not ok:
            count += 1

    # MENTIONS edges
    mentions_rows = graph_service.conn.execute(
        "SELECT source, target, edge_type FROM edges WHERE edge_type = 'MENTIONS'"
    ).fetchall()
    valid_mention_targets = schema_validator.get_source_edge_target_types()
    for source_id, target_id, edge_type in mentions_rows:
        target_node = graph_service.get_node(target_id)
        if not target_node or target_node.get("type") not in valid_mention_targets:
            count += 1

    return count


def _count_self_aliases(graph_service) -> int:
    rows = graph_service.conn.execute(
        "SELECT id, aliases, properties FROM nodes WHERE aliases IS NOT NULL AND aliases != '[]'"
    ).fetchall()
    count = 0
    for node_id, aliases_str, props_str in rows:
        aliases = json.loads(aliases_str) if aliases_str else []
        props = json.loads(props_str) if props_str else {}
        name = props.get("name", node_id)
        count += sum(1 for a in aliases if a == name)
    return count


def _count_uuid_aliases(graph_service) -> int:
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
    rows = graph_service.conn.execute(
        "SELECT id, aliases FROM nodes WHERE aliases IS NOT NULL AND aliases != '[]'"
    ).fetchall()
    count = 0
    for node_id, aliases_str in rows:
        aliases = json.loads(aliases_str) if aliases_str else []
        count += sum(1 for a in aliases if uuid_pattern.match(a))
    return count


def _cleanup_invalid_edges(graph_service, schema_validator) -> int:
    rows = graph_service.conn.execute(
        "SELECT source, target, edge_type FROM edges WHERE edge_type != 'MENTIONS' AND edge_type != 'DEALS_WITH'"
    ).fetchall()

    removed = 0
    for source_id, target_id, edge_type in rows:
        source_node = graph_service.get_node(source_id)
        target_node = graph_service.get_node(target_id)
        if not source_node or not target_node:
            graph_service.delete_edge(source_id, target_id, edge_type)
            removed += 1
            continue
        nodes_map = {
            source_id: source_node.get("type", "Unknown"),
            target_id: target_node.get("type", "Unknown"),
        }
        edge = {"source": source_id, "target": target_id, "type": edge_type}
        ok, msg = schema_validator.validate_edge_structure(edge, nodes_map)
        if not ok:
            graph_service.delete_edge(source_id, target_id, edge_type)
            removed += 1

    # MENTIONS edges
    mentions_rows = graph_service.conn.execute(
        "SELECT source, target, edge_type FROM edges WHERE edge_type = 'MENTIONS'"
    ).fetchall()
    valid_mention_targets = schema_validator.get_source_edge_target_types()
    for source_id, target_id, edge_type in mentions_rows:
        target_node = graph_service.get_node(target_id)
        if not target_node:
            graph_service.delete_edge(source_id, target_id, edge_type)
            removed += 1
            continue
        if target_node.get("type", "") not in valid_mention_targets:
            graph_service.delete_edge(source_id, target_id, edge_type)
            removed += 1

    if removed:
        LOGGER.info(f"Deterministic: removed {removed} invalid edges")
    return removed


def _cleanup_self_aliases(graph_service) -> int:
    rows = graph_service.conn.execute(
        "SELECT id, type, aliases, properties FROM nodes WHERE aliases IS NOT NULL AND aliases != '[]'"
    ).fetchall()

    cleaned = 0
    for node_id, node_type, aliases_str, props_str in rows:
        aliases = json.loads(aliases_str) if aliases_str else []
        props = json.loads(props_str) if props_str else {}
        name = props.get("name", node_id)
        new_aliases = [a for a in aliases if a != name]
        if len(new_aliases) < len(aliases):
            graph_service.upsert_node(node_id, node_type, new_aliases, {})
            cleaned += len(aliases) - len(new_aliases)

    if cleaned:
        LOGGER.info(f"Deterministic: removed {cleaned} self-aliases")
    return cleaned


def _cleanup_uuid_aliases(graph_service) -> int:
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
    rows = graph_service.conn.execute(
        "SELECT id, type, aliases, properties FROM nodes WHERE aliases IS NOT NULL AND aliases != '[]'"
    ).fetchall()

    cleaned = 0
    for node_id, node_type, aliases_str, props_str in rows:
        aliases = json.loads(aliases_str) if aliases_str else []
        new_aliases = [a for a in aliases if not uuid_pattern.match(a)]
        if len(new_aliases) < len(aliases):
            graph_service.upsert_node(node_id, node_type, new_aliases, {})
            cleaned += len(aliases) - len(new_aliases)

    if cleaned:
        LOGGER.info(f"Deterministic: removed {cleaned} UUID aliases")
    return cleaned
