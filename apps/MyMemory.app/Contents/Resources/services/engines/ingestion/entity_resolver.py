"""
Entity resolution against existing graph.

Resolves extracted entities to either LINK (existing node) or CREATE (new node).
Handles relation edge construction with canonical names.
"""

import logging
import os
import uuid
from collections import Counter
from typing import Dict, Any, List

from services.utils.graph_service import GraphService
from services.engines.ingestion import _shared
from services.engines.ingestion._shared import _get_schema_validator, LOGGER


def resolve_entities(nodes: List[Dict], edges: List[Dict], source_type: str, filename: str) -> List[Dict]:
    """
    Resolve extracted entities against existing graph.
    Returns list of mentions (actions to perform).

    Includes canonical_name for each entity:
    - LINK: canonical_name from graph (the authoritative name)
    - CREATE: canonical_name = input name
    """
    # Load schema to identify type-specific properties and matching config
    validator = _get_schema_validator()
    schema = validator.schema
    base_prop_names = set(schema.get('base_properties', {}).get('properties', {}).keys())
    schema_nodes = schema.get('nodes', {})

    mentions = []
    name_to_uuid = {}
    name_to_canonical = {}  # Maps input name -> canonical name
    name_to_type = {}  # Maps input name -> node type

    # Check if database exists before opening in read-only mode
    # After hard reset, graph is empty - all entities will be CREATE
    graph_path = _shared.GRAPH_DB_PATH
    if not os.path.exists(graph_path):
        LOGGER.info(f"Graph DB not found at {graph_path}, all entities will be CREATE")
        graph = None
    else:
        graph = GraphService(graph_path, read_only=True)
    seen_candidates = set()

    for node in nodes:
        name = node.get('name')
        type_str = node.get('type')
        confidence = node.get('confidence', 0.5)
        if not name or not type_str:
            continue

        # Noise filter based on confidence
        if confidence < 0.3:
            continue

        key = f"{name}|{type_str}"
        if key in seen_candidates:
            continue
        seen_candidates.add(key)

        # Boost confidence for trusted sources (schema: trusted_source_confidence_floor)
        source_mappings = _get_schema_validator().get_source_type_mappings()
        trusted_types = {source_mappings.get('slack', ''), source_mappings.get('mail', '')}
        if source_type in trusted_types:
            confidence = max(confidence, 0.8)

        # Entity resolution: schema-driven creation_policy
        node_type_def = schema_nodes.get(type_str, {})
        creation_policy = node_type_def.get('creation_policy', 'STRICT_LOOKUP')

        if creation_policy == 'ALWAYS_CREATE':
            # Skip graph lookup — always create new node (Event, Source)
            action = "CREATE"
            target_uuid = str(uuid.uuid4())
            canonical_name = name
            LOGGER.debug(f"resolve: ALWAYS_CREATE for {type_str} '{name}'")
        else:
            # STRICT_LOOKUP: attempt to find existing node with schema-driven matching
            existing_uuid = None
            matching_config = node_type_def.get('matching')
            if graph is not None:
                existing_uuid = graph.find_node_by_name(
                    type_str, name, fuzzy=True,
                    matching_config=matching_config
                )

            if existing_uuid:
                action = "LINK"
                target_uuid = existing_uuid
                # Hämta kanoniskt namn från grafen
                existing_node = graph.get_node(existing_uuid)
                canonical_name = (
                    existing_node.get("properties", {}).get("name", name)
                    if existing_node else name
                )
            else:
                action = "CREATE"
                target_uuid = str(uuid.uuid4())
                canonical_name = name  # Vid CREATE = input-namn

        name_to_uuid[name] = target_uuid
        name_to_canonical[name] = canonical_name
        name_to_type[name] = type_str

        # Extract type-specific properties from LLM output
        type_specific_props = {}
        node_type_def = schema.get('nodes', {}).get(type_str, {})
        allowed_type_props = set(node_type_def.get('properties', {}).keys()) - base_prop_names
        for prop_name in allowed_type_props:
            if prop_name in node and prop_name != 'name':
                type_specific_props[prop_name] = node[prop_name]
        mention = {
            "action": action,
            "target_uuid": target_uuid,
            "type": type_str,
            "label": name,
            "canonical_name": canonical_name,
            "confidence": confidence
        }
        if type_specific_props:
            mention["properties"] = type_specific_props

        mentions.append(mention)

    if graph is not None:
        graph.close()

    # Log resolution summary per type (used for iterative tuning)
    link_counts = Counter(m['type'] for m in mentions if m.get('action') == 'LINK')
    create_counts = Counter(m['type'] for m in mentions if m.get('action') == 'CREATE')
    all_types = sorted(set(list(link_counts.keys()) + list(create_counts.keys())))
    parts = []
    for t in all_types:
        l_count = link_counts.get(t, 0)
        c_count = create_counts.get(t, 0)
        parts.append(f"{t}:{l_count}L/{c_count}C")
    total_link = sum(link_counts.values())
    total_create = sum(create_counts.values())
    LOGGER.info(f"Resolve: {total_link} LINK, {total_create} CREATE [{', '.join(parts)}]")

    # Handle relations - use canonical names for source_text
    for edge in edges:
        source_name = edge.get('source')
        target_name = edge.get('target')
        rel_type = edge.get('type')
        rel_conf = edge.get('confidence', 0.5)
        relation_context_text = edge.get('relation_context', '')

        if source_name in name_to_uuid and target_name in name_to_uuid:
            source_uuid = name_to_uuid[source_name]
            target_uuid = name_to_uuid[target_name]

            # Self-loop guard: skip edges where source == target
            if source_uuid == target_uuid:
                LOGGER.warning(
                    f"Dropped self-loop edge ({rel_type}): "
                    f"'{source_name}' -> '{target_name}' (both resolved to {source_uuid[:12]})"
                )
                continue

            # Använd canonical names i source_text
            source_canonical = name_to_canonical.get(source_name, source_name)
            target_canonical = name_to_canonical.get(target_name, target_name)
            # Hämta typer för terminal-output
            src_type = name_to_type.get(source_name, "?")
            tgt_type = name_to_type.get(target_name, "?")

            # Extract type-specific edge properties from LLM output
            edge_props = {}
            edge_type_def = schema.get('edges', {}).get(rel_type, {})
            allowed_edge_props = set(edge_type_def.get('properties', {}).keys()) - {'confidence', 'relation_context'}
            for prop_name in allowed_edge_props:
                if prop_name in edge:
                    edge_props[prop_name] = edge[prop_name]

            edge_mention = {
                "action": "CREATE_EDGE",
                "source_uuid": source_uuid,
                "target_uuid": target_uuid,
                "edge_type": rel_type,
                "confidence": rel_conf,
                "source_text": f"{source_canonical} -> {target_canonical}",
                "source_name": source_canonical,
                "source_type": src_type,
                "target_name": target_canonical,
                "target_type": tgt_type,
                "relation_context_text": relation_context_text
            }
            if edge_props:
                edge_mention["edge_properties"] = edge_props

            mentions.append(edge_mention)
        else:
            missing = [x for x in [source_name, target_name] if x not in name_to_uuid]
            LOGGER.debug(f"Dropped edge ({rel_type}): endpoint(s) {missing} not resolved")

    return mentions
