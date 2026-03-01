"""
Dreamer Pass 5: RECATEGORIZE — reads quality_flags, LLM evaluation, clear flag.

Processes nodes flagged with 'wrong_type' by enrichment.
LLM decides the correct type and validates edge compatibility.
"""

import json
import logging
from typing import Dict, Optional

from services.utils.llm_json import call_llm_json
from services.utils.terminal_status import short_id, node_name
from services.engines.dreamer.pass_merge import _build_edges_text

LOGGER = logging.getLogger(__name__)


def run(engine, dry_run: bool, limit: int) -> dict:
    """Run recategorize pass. Returns stats dict."""
    config = engine.dreamer_config
    stats = {"candidates": 0, "recategorized": 0, "skipped": 0, "decisions": []}

    LOGGER.info("Pass 5: RECATEGORIZE (quality_flags)")

    recat_config = config.get('recategorize', {})
    max_recat = limit or recat_config.get('max_candidates_per_run', 10)

    rows = engine.graph_service.conn.execute(
        "SELECT id, properties FROM nodes WHERE "
        "json_extract(properties, '$.quality_flags') IS NOT NULL"
    ).fetchall()

    candidates = []
    for node_id, props_str in rows:
        if len(candidates) >= max_recat:
            break
        props = json.loads(props_str) if props_str else {}
        flags = props.get("quality_flags", [])
        if "wrong_type" in flags:
            node = engine.graph_service.get_node(node_id)
            if node:
                flag_reason = props.get("quality_flag_reason", {}).get("wrong_type", "Flagged by enrichment")
                candidates.append((node, flag_reason))

    stats["candidates"] = len(candidates)

    if not dry_run and candidates:
        threshold = config.get('thresholds', {}).get('recategorize', 0.90)

        for node, flag_reason in candidates:
            result = _evaluate(engine, node, flag_reason)
            if not result:
                _clear_flag(engine, node["id"])
                continue

            decision = result.get("decision", "SKIP")
            conf = result.get("confidence", 0)
            new_type = result.get("new_type")

            decision_record = {
                "node_name": node_name(node), "current_type": node.get("type", "?"),
                "flag_reason": flag_reason, "decision": decision,
                "confidence": conf, "reason": result.get("reason", ""),
            }

            if decision == "RECATEGORIZE" and conf >= threshold and new_type:
                LOGGER.info(f"RECAT: {node_name(node)} {node.get('type')} -> {new_type} (conf={conf:.2f})")
                engine.graph_service.record_dreamer_decision(
                    "recategorize", "EXECUTE", node["id"], node_name(node),
                    conf, result.get("reason", ""), {
                        "old_type": node.get("type", "?"), "new_type": new_type,
                    })
                _execute(engine, node["id"], new_type)
                decision_record["new_type"] = new_type
                stats["recategorized"] += 1
            else:
                engine.graph_service.record_dreamer_decision(
                    "recategorize", "SKIP", node["id"], node_name(node),
                    conf, result.get("reason", ""), {
                        "old_type": node.get("type", "?"), "flag_reason": flag_reason,
                    })
                stats["skipped"] += 1

            stats["decisions"].append(decision_record)
            _clear_flag(engine, node["id"])

    elif dry_run and candidates:
        details = []
        for node, flag_reason in candidates:
            details.append({"name": node_name(node), "type": node.get("type", "?"), "reason": flag_reason})
            LOGGER.info(f"  [DRY] RECAT candidate: {node_name(node)} ({node.get('type')}) -- {flag_reason}")
        stats["details"] = details

    return stats


# --- Evaluation ---

def _evaluate(engine, node: dict, flag_reason: str) -> Optional[Dict]:
    """Evaluate a recategorize candidate via LLM."""
    template = engine.prompts.get("recategorize_evaluation", "")
    if not template:
        LOGGER.error("Missing recategorize_evaluation prompt")
        return None

    props = node.get("properties", {})
    allowed_types = list(engine.schema.get("nodes", {}).keys())

    prompt = template.format(
        node_name=props.get("name", node["id"]),
        current_type=node.get("type", "?"),
        context_summary=props.get("context_summary", "(saknas)"),
        edges_text=_build_edges_text(engine, node["id"]),
        allowed_types=", ".join(allowed_types),
        flag_reason=flag_reason,
    )

    return call_llm_json(
        engine.llm_service, prompt, engine.model_id,
        f"recat_eval({short_id(node['id'])})",
        engine.max_output_tokens
    )


# --- Execution ---

def _execute(engine, node_id: str, new_type: str) -> bool:
    """Execute a recategorize operation."""
    try:
        # Validate edges before changing type
        valid_edges, invalid_edges = _validate_edges_for_type(engine, node_id, new_type)
        for edge in invalid_edges:
            engine.graph_service.delete_edge(edge["source"], edge["target"], edge["type"])

        engine.graph_service.recategorize_node(node_id, new_type)
        recat_node = engine.graph_service.get_node(node_id)
        if recat_node:
            engine.vector_service.upsert_node(recat_node)
        if invalid_edges:
            LOGGER.info(f"Recategorize {short_id(node_id)} -> {new_type}: "
                        f"removed {len(invalid_edges)} invalid edges")
        return True
    except Exception as e:
        LOGGER.error(f"Recategorize execution failed: {e}")
        return False


def _validate_edges_for_type(engine, node_id: str, new_type: str) -> tuple:
    """Validate edges for hypothetical type change."""
    edges_out = engine.graph_service.get_edges_from(node_id)
    edges_in = engine.graph_service.get_edges_to(node_id)
    all_edges = edges_out + edges_in

    if not all_edges:
        return ([], [])

    valid_edges = []
    invalid_edges = []

    for edge in all_edges:
        source_type = new_type if edge["source"] == node_id else _get_node_type(engine, edge["source"])
        target_type = new_type if edge["target"] == node_id else _get_node_type(engine, edge["target"])

        nodes_map = {edge["source"]: source_type, edge["target"]: target_type}
        ok, msg = engine.schema_validator.validate_edge_structure(edge, nodes_map)

        if ok:
            valid_edges.append(edge)
        else:
            invalid_edges.append(edge)

    return (valid_edges, invalid_edges)


def _get_node_type(engine, node_id: str) -> str:
    """Get type for a node from the graph."""
    node = engine.graph_service.get_node(node_id)
    return node.get("type", "Unknown") if node else "Unknown"


def _clear_flag(engine, node_id: str):
    """Remove 'wrong_type' quality flag from a node."""
    node = engine.graph_service.get_node(node_id)
    if not node:
        return
    props = node.get("properties", {})
    flags = props.get("quality_flags", [])
    if "wrong_type" in flags:
        flags.remove("wrong_type")
        props["quality_flags"] = flags
        if not flags:
            props.pop("quality_flags", None)
            props.pop("quality_flag_reason", None)
        engine.graph_service.update_node_properties(node_id, props)
