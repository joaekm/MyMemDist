"""
Dreamer Pass 2: MERGE — name-based discovery, LLM evaluation, skip-cache.

Discovers potential duplicate nodes via token-subset, diacritics-fold,
and fuzzy matching strategies. LLM evaluates each pair. Results are
cached to avoid re-evaluating unchanged pairs.
"""

import difflib
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from services.utils.llm_json import call_llm_json
from services.utils.name_matching import fold_diacritics, tokenize
from services.utils.terminal_status import short_id, node_name
from services.engines.dreamer import skip_cache

LOGGER = logging.getLogger(__name__)


def run(engine, dry_run: bool, limit: int) -> dict:
    """Run merge pass. Returns stats dict."""
    config = engine.dreamer_config
    stats = {"candidates": 0, "merged": 0, "skipped": 0, "cached": 0, "decisions": []}

    merge_config = config.get('merge', {})
    if not merge_config.get('enabled', True):
        return stats

    LOGGER.info("Pass 2: MERGE")

    if dry_run:
        candidates = _discover_candidates(engine, max_candidates=limit if limit else 0)
    else:
        candidates = _discover_candidates(engine, max_candidates=limit if limit else None)

    stats["candidates"] = len(candidates)

    if not dry_run and candidates:
        cache = skip_cache.load(config)
        threshold = config.get('thresholds', {}).get('merge', 0.90)

        for node_a, node_b, strategy in candidates:
            pair_key = skip_cache.compute_pair_key(node_a["id"], node_b["id"])
            hash_a = skip_cache.compute_node_hash(node_a, _get_edge_count(engine, node_a["id"]))
            hash_b = skip_cache.compute_node_hash(node_b, _get_edge_count(engine, node_b["id"]))

            # Check skip-cache
            cached = cache.get(pair_key)
            if cached:
                if cached.get("hash_a") == hash_a and cached.get("hash_b") == hash_b:
                    stats["cached"] += 1
                    continue

            result = _evaluate(engine, node_a, node_b)
            if not result:
                continue

            decision = result.get("decision", "SKIP")
            conf = result.get("confidence", 0)
            reason = result.get("reason", "")

            decision_record = {
                "node_a": node_name(node_a), "node_b": node_name(node_b),
                "strategy": strategy, "decision": decision,
                "confidence": conf, "reason": reason,
            }

            if decision == "MERGE" and conf >= threshold:
                # Map LLM's primary choice back to known UUIDs by name.
                # Never trust LLM to echo UUIDs correctly.
                llm_primary = result.get("primary_node", "")
                name_b = node_b.get("properties", {}).get("name", "")
                if llm_primary == name_b:
                    primary, secondary = node_b["id"], node_a["id"]
                else:
                    primary, secondary = node_a["id"], node_b["id"]
                LOGGER.info(f"MERGE: {short_id(secondary)} -> {short_id(primary)} "
                            f"(conf={conf:.2f}, strategy={strategy})")
                primary_node = node_a if primary == node_a["id"] else node_b
                secondary_node = node_b if primary == node_a["id"] else node_a
                engine.graph_service.record_dreamer_decision(
                    "merge", "EXECUTE", primary, node_name(primary_node),
                    conf, reason, {
                        "node_id_secondary": secondary,
                        "node_name_secondary": node_name(secondary_node),
                        "strategy": strategy,
                    })
                _execute(engine, primary, secondary)
                decision_record["primary"] = node_name(primary_node)
                stats["merged"] += 1
                cache.pop(pair_key, None)
            else:
                LOGGER.info(f"MERGE SKIP: {node_name(node_a)} <-> {node_name(node_b)} "
                            f"(conf={conf:.2f})")
                engine.graph_service.record_dreamer_decision(
                    "merge", "SKIP", node_a["id"], node_name(node_a),
                    conf, reason, {
                        "node_id_secondary": node_b["id"],
                        "node_name_secondary": node_name(node_b),
                        "strategy": strategy,
                    })
                cache[pair_key] = {
                    "hash_a": hash_a, "hash_b": hash_b,
                    "decision": decision, "confidence": conf,
                    "timestamp": datetime.now().isoformat(),
                }
                stats["skipped"] += 1

            stats["decisions"].append(decision_record)

        skip_cache.save(config, cache)

    elif dry_run and candidates:
        details = []
        for node_a, node_b, strategy in candidates:
            details.append({
                "a": node_name(node_a), "b": node_name(node_b),
                "type": node_a.get("type", "?"), "strategy": strategy,
            })
            LOGGER.info(f"  [DRY] MERGE candidate: {node_name(node_a)} <-> {node_name(node_b)} ({strategy})")
        stats["details"] = details

    return stats


# --- Discovery ---

def _discover_candidates(engine, max_candidates=None) -> List[Tuple[dict, dict, str]]:
    """Find merge candidates using name-based strategies."""
    config = engine.dreamer_config
    merge_config = config.get('merge', {})
    strategies = merge_config.get('strategies', ['token_subset', 'diacritics_fold', 'fuzzy'])
    fuzzy_cutoff = merge_config.get('fuzzy_cutoff', 0.80)
    if max_candidates is None:
        max_candidates = merge_config.get('max_candidates_per_run', 20)

    schema = engine.schema
    valid_types = set(schema.get("nodes", {}).keys())
    rows = engine.graph_service.conn.execute(
        "SELECT id, type, properties FROM nodes WHERE type NOT IN ('Source', 'Event')"
    ).fetchall()

    nodes_by_type = {}
    for r in rows:
        if r[1] not in valid_types:
            continue
        node = engine.graph_service.get_node(r[0])
        if node:
            node_type = node.get("type", "")
            if node_type not in nodes_by_type:
                nodes_by_type[node_type] = []
            nodes_by_type[node_type].append(node)

    candidates = []
    seen_pairs = set()

    for node_type, nodes in nodes_by_type.items():
        for i, node_a in enumerate(nodes):
            name_a = node_name(node_a)
            tokens_a = tokenize(name_a)
            folded_a = fold_diacritics(name_a)

            for node_b in nodes[i + 1:]:
                if max_candidates and len(candidates) >= max_candidates:
                    break

                name_b = node_name(node_b)
                pair_key = skip_cache.compute_pair_key(node_a["id"], node_b["id"])
                if pair_key in seen_pairs:
                    continue

                matched_strategy = None
                tokens_b = tokenize(name_b)
                folded_b = fold_diacritics(name_b)

                # Strategy 1: Token subset
                if 'token_subset' in strategies:
                    if tokens_a and tokens_b:
                        smaller, larger = (tokens_a, tokens_b) if len(tokens_a) <= len(tokens_b) else (tokens_b, tokens_a)
                        if smaller.issubset(larger) and smaller != larger:
                            matched_strategy = "token_subset"

                # Strategy 2: Diacritics fold
                if not matched_strategy and 'diacritics_fold' in strategies:
                    if folded_a == folded_b and name_a != name_b:
                        matched_strategy = "diacritics_fold"

                # Strategy 3a: Fuzzy first-token
                if not matched_strategy and 'fuzzy' in strategies:
                    if len(tokens_a) == 1 and len(tokens_b) > 1:
                        first_b = folded_b.split()[0]
                        ratio = difflib.SequenceMatcher(None, folded_a, first_b).ratio()
                        if ratio >= fuzzy_cutoff:
                            matched_strategy = "fuzzy_first_token"
                    elif len(tokens_b) == 1 and len(tokens_a) > 1:
                        first_a = folded_a.split()[0]
                        ratio = difflib.SequenceMatcher(None, folded_b, first_a).ratio()
                        if ratio >= fuzzy_cutoff:
                            matched_strategy = "fuzzy_first_token"

                # Strategy 3b: Fuzzy full name
                if not matched_strategy and 'fuzzy' in strategies:
                    if len(tokens_a) == len(tokens_b) and len(tokens_a) >= 2:
                        ratio = difflib.SequenceMatcher(None, folded_a, folded_b).ratio()
                        if ratio >= fuzzy_cutoff and name_a != name_b:
                            matched_strategy = "fuzzy_full"

                if matched_strategy:
                    seen_pairs.add(pair_key)
                    candidates.append((node_a, node_b, matched_strategy))

            if max_candidates and len(candidates) >= max_candidates:
                break

    LOGGER.info(f"Merge discovery: {len(candidates)} candidates from {sum(len(v) for v in nodes_by_type.values())} nodes")
    return candidates


# --- Evaluation ---

def _evaluate(engine, node_a: dict, node_b: dict) -> Optional[Dict]:
    """Evaluate a merge candidate pair via LLM."""
    template = engine.prompts.get("merge_evaluation", "")
    if not template:
        LOGGER.error("Missing merge_evaluation prompt")
        return None

    props_a = node_a.get("properties", {})
    props_b = node_b.get("properties", {})

    prompt = template.format(
        name_a=props_a.get("name", node_a["id"]),
        type_a=node_a.get("type", "?"),
        context_a=props_a.get("context_summary", "(saknas)"),
        edges_a=_build_edges_text(engine, node_a["id"]),
        aliases_a=node_a.get("aliases", []),
        name_b=props_b.get("name", node_b["id"]),
        type_b=node_b.get("type", "?"),
        context_b=props_b.get("context_summary", "(saknas)"),
        edges_b=_build_edges_text(engine, node_b["id"]),
        aliases_b=node_b.get("aliases", []),
    )

    return call_llm_json(
        engine.llm_service, prompt, engine.model_id,
        f"merge_eval({short_id(node_a['id'])}|{short_id(node_b['id'])})",
        engine.max_output_tokens
    )


# --- Execution ---

def _execute(engine, primary_id: str, secondary_id: str) -> bool:
    """Execute a merge operation."""
    try:
        engine.graph_service.merge_nodes(primary_id, secondary_id)
        engine.vector_service.delete(secondary_id)
        merged_node = engine.graph_service.get_node(primary_id)
        if merged_node:
            engine.vector_service.upsert_node(merged_node)
        return True
    except Exception as e:
        LOGGER.error(f"Merge execution failed: {e}")
        return False


# --- Shared helpers ---

def _get_ordered_edges(engine, node_id: str) -> list:
    """Return non-source edges for a node in stable order (out first, then in).

    Each entry is a dict with keys from the edge plus 'direction' ("out"/"in").
    """
    edges_out = engine.graph_service.get_edges_from(node_id)
    edges_in = engine.graph_service.get_edges_to(node_id)
    source_edges = engine.schema_validator.get_source_edge_types()

    ordered = []
    for e in edges_out:
        if e["type"] not in source_edges:
            ordered.append({**e, "direction": "out"})
    for e in edges_in:
        if e["type"] not in source_edges:
            ordered.append({**e, "direction": "in"})
    return ordered


def _build_edges_text(engine, node_id: str) -> str:
    """Build human-readable edge summary for a node."""
    ordered = _get_ordered_edges(engine, node_id)

    lines = []
    for e in ordered:
        if e["direction"] == "out":
            other = engine.graph_service.get_node(e["target"])
            if other:
                name = other.get("properties", {}).get("name", e["target"])
                lines.append(f"\u2192[{e['type']}]\u2192 {name} ({other.get('type', '?')})")
                rc = e.get("properties", {}).get("relation_context", [])
                for entry in rc[-3:]:
                    text = entry.get("text", "") if isinstance(entry, dict) else str(entry)
                    if text:
                        lines.append(f"    {text}")
        else:
            other = engine.graph_service.get_node(e["source"])
            if other:
                name = other.get("properties", {}).get("name", e["source"])
                lines.append(f"\u2190[{e['type']}]\u2190 {name} ({other.get('type', '?')})")
                rc = e.get("properties", {}).get("relation_context", [])
                for entry in rc[-3:]:
                    text = entry.get("text", "") if isinstance(entry, dict) else str(entry)
                    if text:
                        lines.append(f"    {text}")

    return "\n".join(lines) if lines else "(Inga relationer)"


def _get_edge_count(engine, node_id: str) -> int:
    """Count non-source edges for a node."""
    edges_out = engine.graph_service.get_edges_from(node_id)
    edges_in = engine.graph_service.get_edges_to(node_id)
    source_edges = engine.schema_validator.get_source_edge_types()
    return len([e for e in edges_out + edges_in if e["type"] not in source_edges])
