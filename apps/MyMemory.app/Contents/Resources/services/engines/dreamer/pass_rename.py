"""
Dreamer Pass 3: RENAME — regex discovery, LLM evaluation, flag-as-state.

Discovers single-token Person names that have full names in their
context_summary. LLM evaluates and may suggest a better name.
"""

import json
import logging
import re
from typing import Dict, List, Optional, Tuple

from services.utils.llm_json import call_llm_json
from services.utils.name_matching import is_single_token, is_weak_name
from services.utils.terminal_status import short_id, node_name
from services.engines.dreamer.pass_merge import _build_edges_text

LOGGER = logging.getLogger(__name__)


def run(engine, dry_run: bool, limit: int) -> dict:
    """Run rename pass. Returns stats dict."""
    config = engine.dreamer_config
    stats = {"candidates": 0, "renamed": 0, "skipped": 0, "decisions": []}

    rename_config = config.get('rename', {})
    if not rename_config.get('enabled', True):
        return stats

    LOGGER.info("Pass 3: RENAME")

    if dry_run:
        candidates = _discover_candidates(engine, max_candidates=limit if limit else 0)
    else:
        candidates = _discover_candidates(engine, max_candidates=limit if limit else None)

    stats["candidates"] = len(candidates)

    if not dry_run and candidates:
        thresholds = config.get('thresholds', {})
        threshold_normal = thresholds.get('rename_normal', 0.95)
        threshold_weak = thresholds.get('rename_weak', 0.70)

        for node, suggested, reason in candidates:
            result = _evaluate(engine, node, suggested, reason)
            if not result:
                continue

            decision = result.get("decision", "SKIP")
            conf = result.get("confidence", 0)
            new_name = result.get("new_name")

            is_weak = is_weak_name(node_name(node))
            threshold = threshold_weak if is_weak else threshold_normal

            decision_record = {
                "current_name": node_name(node), "decision": decision,
                "confidence": conf, "reason": result.get("reason", ""),
            }

            if decision == "RENAME" and conf >= threshold and new_name:
                LOGGER.info(f"RENAME: {node_name(node)} -> {new_name} (conf={conf:.2f})")
                engine.graph_service.record_dreamer_decision(
                    "rename", "EXECUTE", node["id"], node_name(node),
                    conf, result.get("reason", ""), {
                        "old_name": node_name(node), "new_name": new_name,
                    })
                _execute(engine, node["id"], new_name)
                decision_record["new_name"] = new_name
                stats["renamed"] += 1
            else:
                LOGGER.info(f"RENAME SKIP: {node_name(node)} (conf={conf:.2f})")
                engine.graph_service.record_dreamer_decision(
                    "rename", "SKIP", node["id"], node_name(node),
                    conf, result.get("reason", ""), {
                        "old_name": node_name(node),
                    })
                stats["skipped"] += 1

            stats["decisions"].append(decision_record)

    elif dry_run and candidates:
        details = []
        for node, suggested, reason in candidates:
            details.append({
                "current": node_name(node), "suggested": suggested, "reason": reason,
            })
            LOGGER.info(f"  [DRY] RENAME candidate: {node_name(node)} -> {suggested} ({reason})")
        stats["details"] = details

    return stats


# --- Discovery ---

def _discover_candidates(engine, max_candidates=None) -> List[Tuple[dict, str, str]]:
    """Find rename candidates: single-token names with full name in context."""
    config = engine.dreamer_config
    rename_config = config.get('rename', {})
    if max_candidates is None:
        max_candidates = rename_config.get('max_candidates_per_run', 30)

    rows = engine.graph_service.conn.execute(
        "SELECT id, type, properties FROM nodes WHERE type = 'Person'"
    ).fetchall()

    candidates = []
    for r in rows:
        if max_candidates and len(candidates) >= max_candidates:
            break

        props = json.loads(r[2]) if r[2] else {}
        name = props.get("name", r[0])

        if not is_single_token(name):
            continue

        ctx = props.get("context_summary", "")
        if not ctx:
            continue

        pattern = re.compile(
            rf'\b{re.escape(name)}\s+([A-Z\u00c5\u00c4\u00d6][a-z\u00e5\u00e4\u00f6\u00e9]+(?:\s+[A-Z\u00c5\u00c4\u00d6][a-z\u00e5\u00e4\u00f6\u00e9]+)*)',
            re.UNICODE
        )
        match = pattern.search(ctx)
        if match:
            suggested = f"{name} {match.group(1)}"
            node = engine.graph_service.get_node(r[0])
            if node:
                candidates.append((node, suggested, "single_token_with_full_name_in_context"))

    LOGGER.info(f"Rename discovery: {len(candidates)} candidates")
    return candidates


# --- Evaluation ---

def _evaluate(engine, node: dict, suggested_name: str, reason: str) -> Optional[Dict]:
    """Evaluate a rename candidate via LLM."""
    template = engine.prompts.get("rename_evaluation", "")
    if not template:
        LOGGER.error("Missing rename_evaluation prompt")
        return None

    props = node.get("properties", {})
    prompt = template.format(
        current_name=props.get("name", node["id"]),
        node_type=node.get("type", "?"),
        context_summary=props.get("context_summary", "(saknas)"),
        suggested_name=suggested_name,
        reason=reason,
        edges_text=_build_edges_text(engine, node["id"]),
    )

    return call_llm_json(
        engine.llm_service, prompt, engine.model_id,
        f"rename_eval({short_id(node['id'])})",
        engine.max_output_tokens
    )


# --- Execution ---

def _execute(engine, node_id: str, new_name: str) -> bool:
    """Execute a rename operation."""
    try:
        engine.graph_service.rename_node(node_id, new_name)
        renamed_node = engine.graph_service.get_node(new_name)
        if renamed_node:
            engine.vector_service.upsert_node(renamed_node)
        return True
    except Exception as e:
        LOGGER.error(f"Rename execution failed: {e}")
        return False
