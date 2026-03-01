"""
Source type profiles for ingestion.

Controls which node types and edge types are allowed for each source type
(e.g., Calendar vs Slack vs Documents).
"""

import logging
from typing import Dict, Any, List

from services.engines.ingestion._shared import _get_schema_validator, LOGGER


def _build_default_source_profile() -> Dict[str, Any]:
    """Build default source profile from schema (full access for unknown source types)."""
    validator = _get_schema_validator()
    doc_type = validator.get_document_node_type()
    all_nodes = validator.get_node_types() - {doc_type}
    all_edges = validator.get_edge_types() - validator.get_source_edge_types()
    return {
        "allow_create": sorted(all_nodes),
        "allow_edges": sorted(all_edges),
        "skip_critic": False,
        "prompt_key": None
    }


def get_source_profile(source_type: str) -> Dict[str, Any]:
    """
    Load extraction profile for a source type from graph schema.

    Returns profile dict with keys:
        allow_create: list of node types allowed to CREATE
        allow_edges: list of edge types allowed to create
        skip_critic: bool — skip critic LLM call entirely
        prompt_key: str — key in services_prompts.yaml for source context instruction
    """
    schema = _get_schema_validator().schema
    profiles = schema.get('source_type_profiles', {})
    profile = profiles.get(source_type)
    if not profile:
        LOGGER.warning(f"No source_type_profile for '{source_type}', using default (full access)")
        return _build_default_source_profile()
    return {
        "allow_create": profile.get("allow_create", []),
        "allow_edges": profile.get("allow_edges", []),
        "skip_critic": profile.get("skip_critic", False),
        "prompt_key": profile.get("prompt_key")
    }


def apply_source_profile(ingestion_payload: List[Dict], profile: Dict[str, Any]) -> List[Dict]:
    """
    Filter ingestion_payload based on source type profile.

    - CREATE mentions: keep only if node type is in allow_create
    - CREATE_EDGE mentions: keep only if edge type is in allow_edges
      AND both endpoints survive filtering
    - LINK mentions: always kept (linking to existing nodes is always allowed)

    Returns filtered payload.
    """
    allow_create = set(profile.get("allow_create", []))
    allow_edges = set(profile.get("allow_edges", []))

    # Pass 1: filter nodes
    filtered = []
    removed_uuids = set()
    for m in ingestion_payload:
        action = m.get("action")
        if action == "CREATE":
            node_type = m.get("type")
            if node_type not in allow_create:
                removed_uuids.add(m.get("target_uuid"))
                LOGGER.debug(f"Profile blocked CREATE: {m.get('label')} ({node_type}) — not in allow_create")
                continue
        if action == "CREATE_EDGE":
            continue  # Handle edges in pass 2
        filtered.append(m)

    # Pass 2: filter edges
    for m in ingestion_payload:
        if m.get("action") != "CREATE_EDGE":
            continue
        edge_type = m.get("edge_type")
        if edge_type not in allow_edges:
            LOGGER.debug(f"Profile blocked edge: {edge_type} — not in allow_edges")
            continue
        if m.get("source_uuid") in removed_uuids or m.get("target_uuid") in removed_uuids:
            LOGGER.debug(f"Profile dropped edge: {edge_type} — endpoint removed by profile")
            continue
        filtered.append(m)

    blocked_nodes = len([m for m in ingestion_payload if m.get("action") == "CREATE"]) - \
                    len([m for m in filtered if m.get("action") == "CREATE"])
    blocked_edges = len([m for m in ingestion_payload if m.get("action") == "CREATE_EDGE"]) - \
                    len([m for m in filtered if m.get("action") == "CREATE_EDGE"])
    if blocked_nodes > 0 or blocked_edges > 0:
        LOGGER.info(f"Source profile filtered: {blocked_nodes} CREATE nodes, {blocked_edges} edges removed")

    return filtered
