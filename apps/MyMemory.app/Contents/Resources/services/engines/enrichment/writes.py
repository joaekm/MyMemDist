"""
Enrichment write operations — apply LLM results to graph, Lake, vector.

Two write functions:
1. execute_enrich_writes() — node/edge enrichments + Lake updates
2. write_quality_flags() — quality flags for Dreamer (OBJEKT-107)
"""

import logging
from datetime import datetime
from typing import Dict

from services.utils.terminal_status import short_id
from services.utils.terminal_status import status as terminal_status
from services.utils.lake_service import LakeService

LOGGER = logging.getLogger(__name__)


def execute_enrich_writes(engine, enrich_result: Dict) -> Dict:
    """Apply enrichment results to graph, Lake, and vector.

    Returns stats dict with counts of operations performed.
    """
    system_properties = set(engine.enrichment_config.get('system_properties', []))
    threshold_enrich = engine.enrichment_config.get('thresholds', {}).get('enrich', 0.6)

    stats = {"nodes_updated": 0, "edges_updated": 0, "lake_updated": 0,
             "nodes_skipped": 0, "edges_skipped": 0, "decisions": []}
    enriched_node_ids = set()

    # --- Node enrichments ---
    for ne in enrich_result.get("node_enrichments", []):
        node_id = ne.get("node_id", "")
        confidence = ne.get("confidence", 0)
        reason = ne.get("reason", "")

        if confidence < threshold_enrich:
            LOGGER.info(f"Enrich skip (conf {confidence:.2f} < {threshold_enrich}): {short_id(node_id)}")
            stats["nodes_skipped"] += 1
            stats["decisions"].append({
                "node_id": short_id(node_id), "decision": "SKIP",
                "confidence": confidence, "reason": f"conf < {threshold_enrich}",
            })
            continue

        node = engine.graph_service.get_node(node_id)
        if not node:
            LOGGER.warning(f"Enrich skip: node {short_id(node_id)} not found in graph")
            stats["nodes_skipped"] += 1
            continue

        node_name = node.get("properties", {}).get("name", short_id(node_id))

        # Clean system properties from LLM output
        updated_props = {k: v for k, v in ne.get("updated_properties", {}).items()
                         if k not in system_properties}
        new_aliases = ne.get("new_aliases", [])

        made_changes = False

        # Update properties
        if updated_props:
            engine.graph_service.upsert_node(node_id, node["type"], node.get("aliases"), updated_props)
            made_changes = True

        # Update aliases (LLM returns full list)
        if new_aliases:
            engine.graph_service.upsert_node(node_id, node["type"], new_aliases, {})
            made_changes = True

        if made_changes:
            # Set last_refined_at
            now_ts = datetime.now().isoformat()
            current_node = engine.graph_service.get_node(node_id)
            if current_node:
                props = current_node.get("properties", {})
                props["last_refined_at"] = now_ts
                engine.graph_service.update_node_properties(node_id, props)

            # Vector re-index
            updated_node = engine.graph_service.get_node(node_id)
            if updated_node:
                engine.vector_service.upsert_node(updated_node)

            enriched_node_ids.add(node_id)
            stats["nodes_updated"] += 1
            stats["decisions"].append({
                "node_name": node_name, "decision": "ENRICH",
                "confidence": confidence, "reason": reason,
                "updated_properties": list(updated_props.keys()),
                "new_aliases": new_aliases or [],
            })
            terminal_status("enrichment", f"ENRICH: [{short_id(node_id)}] {reason[:60]}", "done")

    # --- Edge enrichments ---
    for ee in enrich_result.get("edge_enrichments", []):
        confidence = ee.get("confidence", 0)
        if confidence < threshold_enrich:
            stats["edges_skipped"] += 1
            continue

        source = ee.get("source", "")
        target = ee.get("target", "")
        edge_type = ee.get("edge_type", "")

        # Self-loop guard
        if source == target:
            LOGGER.warning(
                f"Dropped self-loop edge ({edge_type}): "
                f"source == target ({source[:12]})"
            )
            continue

        # relation_context is append-only from ingestion — Dreamer must not overwrite
        updated_props = {k: v for k, v in ee.get("updated_properties", {}).items()
                         if k not in system_properties and k != "relation_context"}

        if not updated_props:
            continue

        # Read-merge-write pattern (upsert_edge handles list-merge)
        existing_edge = engine.graph_service.get_edge(source, target, edge_type)
        if existing_edge:
            merged_props = {**existing_edge.get("properties", {}), **updated_props}
        else:
            merged_props = updated_props

        # Warn if relation_context is getting large
        existing_rc = existing_edge.get("properties", {}).get("relation_context", []) if existing_edge else []
        if len(existing_rc) > 30:
            LOGGER.warning(f"Large relation_context ({len(existing_rc)} entries) on "
                           f"{source} -[{edge_type}]-> {target}")

        try:
            engine.graph_service.upsert_edge(source, target, edge_type, merged_props)
            stats["edges_updated"] += 1
        except ValueError as e:
            LOGGER.warning(f"Edge skipped (schema guard): {e}")

    # --- Lake updates ---
    lake_path = engine._get_lake_path()
    if lake_path:
        lake_service = LakeService(lake_path)
        for lu in enrich_result.get("lake_updates", []):
            unit_id = lu.get("unit_id", "")
            filepath = engine._find_lake_file(lake_path, unit_id)
            if not filepath:
                continue

            success = lake_service.update_semantics(
                filepath,
                context_summary=lu.get("context_summary"),
                relations_summary=lu.get("relations_summary"),
                document_keywords=lu.get("document_keywords"),
                set_timestamp_updated=True
            )
            if success:
                stats["lake_updated"] += 1

    LOGGER.info(f"Enrich writes: {stats['nodes_updated']} nodes, {stats['edges_updated']} edges, "
                 f"{stats['lake_updated']} lake ({stats['nodes_skipped']} skipped)")
    return stats


def write_quality_flags(engine, enrich_result: Dict) -> int:
    """Write quality_flags from enrichment LLM response to graph nodes.

    Flags are written to nodes.properties.quality_flags as a list of strings.
    Graph Sweep reads these flags to decide on SPLIT/RECATEGORIZE actions.

    Returns count of nodes flagged.
    """
    flags_enabled = engine.enrichment_config.get('quality_flags', {}).get('enabled', True)
    if not flags_enabled:
        return 0

    flagged_count = 0
    for ne in enrich_result.get("node_enrichments", []):
        quality_flags = ne.get("quality_flags", [])
        flag_reason = ne.get("flag_reason", {})

        if not quality_flags:
            continue

        node_id = ne.get("node_id", "")
        node = engine.graph_service.get_node(node_id)
        if not node:
            continue

        props = node.get("properties", {})
        props["quality_flags"] = quality_flags
        if flag_reason:
            props["quality_flag_reason"] = flag_reason
        engine.graph_service.update_node_properties(node_id, props)
        flagged_count += 1
        LOGGER.info(f"Quality flags set on {short_id(node_id)}: {quality_flags}")

    return flagged_count
