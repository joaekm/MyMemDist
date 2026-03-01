"""
Dreamer Pass 4: SPLIT — reads quality_flags from enrichment, LLM evaluation, clear flag.

Processes nodes flagged with 'possible_split' by enrichment.
LLM decides whether to split and defines clusters with edge assignments.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple

from services.utils.llm_json import call_llm_json
from services.utils.terminal_status import short_id, node_name
from services.engines.dreamer.pass_merge import _get_ordered_edges

LOGGER = logging.getLogger(__name__)


def run(engine, dry_run: bool, limit: int) -> dict:
    """Run split pass. Returns stats dict."""
    config = engine.dreamer_config
    stats = {"candidates": 0, "split": 0, "skipped": 0, "decisions": []}

    LOGGER.info("Pass 4: SPLIT (quality_flags)")

    split_config = config.get('split', {})
    max_split = limit or split_config.get('max_candidates_per_run', 10)

    # Find flagged nodes
    rows = engine.graph_service.conn.execute(
        "SELECT id, properties FROM nodes WHERE "
        "json_extract(properties, '$.quality_flags') IS NOT NULL"
    ).fetchall()

    candidates = []
    for node_id, props_str in rows:
        if len(candidates) >= max_split:
            break
        props = json.loads(props_str) if props_str else {}
        flags = props.get("quality_flags", [])
        if "possible_split" in flags:
            node = engine.graph_service.get_node(node_id)
            if node:
                flag_reason = props.get("quality_flag_reason", {}).get("possible_split", "Flagged by enrichment")
                candidates.append((node, flag_reason))

    stats["candidates"] = len(candidates)

    if not dry_run and candidates:
        threshold = config.get('thresholds', {}).get('split', 0.90)

        for node, flag_reason in candidates:
            result, ordered_edges = _evaluate(engine, node, flag_reason)
            if not result:
                _clear_flag(engine, node["id"])
                continue

            decision = result.get("decision", "SKIP")
            conf = result.get("confidence", 0)

            decision_record = {
                "node_name": node_name(node), "flag_reason": flag_reason,
                "decision": decision, "confidence": conf,
                "reason": result.get("reason", ""),
            }

            if decision == "SPLIT" and conf >= threshold:
                clusters = result.get("split_clusters", [])
                if clusters:
                    LOGGER.info(f"SPLIT: {node_name(node)} -> {len(clusters)} clusters (conf={conf:.2f})")
                    engine.graph_service.record_dreamer_decision(
                        "split", "EXECUTE", node["id"], node_name(node),
                        conf, result.get("reason", ""), {
                            "clusters": clusters,
                        })
                    _execute(engine, node["id"], clusters, ordered_edges)
                    decision_record["clusters"] = clusters
                    stats["split"] += 1
            else:
                engine.graph_service.record_dreamer_decision(
                    "split", "SKIP", node["id"], node_name(node),
                    conf, result.get("reason", ""), {
                        "flag_reason": flag_reason,
                    })
                stats["skipped"] += 1

            stats["decisions"].append(decision_record)
            _clear_flag(engine, node["id"])

    elif dry_run and candidates:
        details = []
        for node, flag_reason in candidates:
            details.append({"name": node_name(node), "reason": flag_reason})
            LOGGER.info(f"  [DRY] SPLIT candidate: {node_name(node)} -- {flag_reason}")
        stats["details"] = details

    return stats


# --- Evaluation ---

def _build_numbered_edges_text(engine, node_id: str) -> Tuple[str, List[dict]]:
    """Build numbered edge text for split evaluation.

    Returns (text, filtered_edges) where index in text matches list position.
    Edges with missing counterparts are filtered out.
    """
    ordered = _get_ordered_edges(engine, node_id)

    filtered = []
    lines = []
    idx = 0
    for e in ordered:
        if e["direction"] == "out":
            other = engine.graph_service.get_node(e["target"])
            if not other:
                continue
            name = other.get("properties", {}).get("name", e["target"])
            lines.append(f"[{idx}] \u2192[{e['type']}]\u2192 {name} ({other.get('type', '?')})")
        else:
            other = engine.graph_service.get_node(e["source"])
            if not other:
                continue
            name = other.get("properties", {}).get("name", e["source"])
            lines.append(f"[{idx}] \u2190[{e['type']}]\u2190 {name} ({other.get('type', '?')})")

        rc = e.get("properties", {}).get("relation_context", [])
        for entry in rc[-3:]:
            text = entry.get("text", "") if isinstance(entry, dict) else str(entry)
            if text:
                lines.append(f"    {text}")

        filtered.append(e)
        idx += 1

    text = "\n".join(lines) if lines else "(Inga relationer)"
    return text, filtered


def _evaluate(engine, node: dict, flag_reason: str) -> Tuple[Optional[Dict], List[dict]]:
    """Evaluate a split candidate via LLM. Returns (result, ordered_edges)."""
    template = engine.prompts.get("split_evaluation", "")
    if not template:
        LOGGER.error("Missing split_evaluation prompt")
        return None, []

    edges_text, ordered_edges = _build_numbered_edges_text(engine, node["id"])

    props = node.get("properties", {})
    prompt = template.format(
        node_name=props.get("name", node["id"]),
        node_type=node.get("type", "?"),
        context_summary=props.get("context_summary", "(saknas)"),
        aliases=node.get("aliases", []),
        edges_text=edges_text,
        flag_reason=flag_reason,
    )

    result = call_llm_json(
        engine.llm_service, prompt, engine.model_id,
        f"split_eval({short_id(node['id'])})",
        engine.max_output_tokens
    )
    return result, ordered_edges


# --- Execution ---

def _execute(engine, node_id: str, clusters: list, ordered_edges: list) -> bool:
    """Execute a split operation with directed edge assignment."""
    try:
        source_edge_types = engine.schema_validator.get_source_edge_types()

        # Build edge_assignment: cluster_index -> list of edges
        edge_assignment = {}
        assigned_indices = set()

        for ci, cluster in enumerate(clusters):
            edge_assignment[ci] = []
            context_indices = cluster.get("context_indices", [])
            for idx in context_indices:
                if not isinstance(idx, int):
                    continue
                if idx < 0 or idx >= len(ordered_edges):
                    LOGGER.warning(f"Split edge index {idx} out of range (0-{len(ordered_edges)-1}), skipping")
                    continue
                edge_assignment[ci].append(ordered_edges[idx])
                assigned_indices.add(idx)

        # Unassigned edges -> largest cluster
        unassigned = [ordered_edges[i] for i in range(len(ordered_edges)) if i not in assigned_indices]
        if unassigned:
            largest_ci = max(range(len(clusters)),
                            key=lambda ci: len(edge_assignment.get(ci, [])))
            LOGGER.info(f"Split: {len(unassigned)} unassigned edges -> cluster {largest_ci}")
            edge_assignment[largest_ci].extend(unassigned)

        new_nodes = engine.graph_service.split_node(
            node_id, clusters,
            edge_assignment=edge_assignment,
            source_edge_types=source_edge_types,
        )
        if new_nodes:
            for new_nid in new_nodes:
                new_node = engine.graph_service.get_node(new_nid)
                if new_node:
                    engine.vector_service.upsert_node(new_node)
        return True
    except Exception as e:
        LOGGER.error(f"Split execution failed: {e}")
        return False


def _clear_flag(engine, node_id: str):
    """Remove 'possible_split' quality flag from a node."""
    node = engine.graph_service.get_node(node_id)
    if not node:
        return
    props = node.get("properties", {})
    flags = props.get("quality_flags", [])
    if "possible_split" in flags:
        flags.remove("possible_split")
        props["quality_flags"] = flags
        if not flags:
            props.pop("quality_flags", None)
            props.pop("quality_flag_reason", None)
        engine.graph_service.update_node_properties(node_id, props)
