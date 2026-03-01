"""
LLM-based critic filtering for extracted entities.

Sends entity candidates to a fast LLM (Haiku) for quality assessment.
Separates approved from rejected entities before graph writes.
"""

import json
import logging
from typing import Dict, List

from services.utils.json_parser import parse_llm_json
from services.engines.ingestion._shared import (
    _get_llm_service, _get_schema_validator, get_prompt, LOGGER,
)
from services.engines.ingestion import _shared


def _call_critic_llm(entities_for_review: List[Dict]) -> Dict:
    """
    Skicka entiteter till critic-LLM för kvalitetsfiltrering.

    Args:
        entities_for_review: [{"name": "...", "type": "...", "confidence": 0.x}]

    Returns:
        {"approved": [{"name", "type", "reason"}], "rejected": [{"name", "type", "reason"}]}
        Vid fel: {"approved": entities_for_review, "rejected": []}
    """
    if not entities_for_review:
        return {"approved": [], "rejected": []}

    prompt_template = get_prompt('doc_converter', 'entity_critic')
    if not prompt_template:
        LOGGER.warning("entity_critic prompt saknas - hoppar över Critic-steget")
        return {"approved": entities_for_review, "rejected": []}

    # Bygg typdefinitioner från schemat (dynamisk injektion)
    validator = _get_schema_validator()
    schema = validator.schema
    type_definitions = []
    doc_type = validator.get_document_node_type()
    for node_type, node_def in schema.get('nodes', {}).items():
        if node_type == doc_type:
            continue  # Intern typ, inte för extraktion
        desc = node_def.get('description', '')
        type_definitions.append(f"- {node_type}: {desc}")
    type_definitions_str = "\n".join(type_definitions)

    prompt = prompt_template.format(
        entities_json=json.dumps(entities_for_review, indent=2, ensure_ascii=False),
        type_definitions=type_definitions_str
    )

    llm = _get_llm_service()
    # Anthropic Haiku för snabb validering (OBJEKT-85)
    response = llm.generate(prompt, provider='anthropic', model=llm.models['lite'])

    if not response.success:
        LOGGER.error(f"Critic LLM failed: {response.error}")
        return {"approved": entities_for_review, "rejected": []}

    result = parse_llm_json(response.text)
    return result


def critic_filter_entities(nodes: List[Dict]) -> tuple:
    """
    LLM-baserad filtrering av extraherade entiteter (pre-resolve).
    Returnerar endast godkända noder.

    Fallback: Returnerar alla noder om prompt saknas eller LLM misslyckas.
    """
    if not nodes:
        return [], [], []

    entities_for_review = [
        {"name": n.get("name"), "type": n.get("type"), "confidence": n.get("confidence", 0.5)}
        for n in nodes if n.get("name") and n.get("type")
    ]

    if not entities_for_review:
        return [], [], []

    result = _call_critic_llm(entities_for_review)
    approved_names = {e["name"] for e in result.get("approved", [])}

    # Logga statistik
    rejected = result.get("rejected", [])
    if rejected:
        LOGGER.info(f"Critic: {len(approved_names)} godkända, {len(rejected)} avvisade")
        for rej in rejected[:5]:  # Logga max 5 avvisade
            LOGGER.debug(f"  Avvisad: {rej.get('name')} ({rej.get('type')}) - {rej.get('reason', 'N/A')}")

    # Filtrera original-noder baserat på godkända namn
    approved_nodes = [n for n in nodes if n.get("name") in approved_names]
    return approved_nodes, result.get("approved", []), result.get("rejected", [])


def critic_filter_resolved(ingestion_payload: List[Dict]) -> List[Dict]:
    """
    LLM-baserad kvalitetsfiltrering av resolvade entiteter.

    LINK-entiteter auto-godkänns (grafen bekräftar existens).
    CREATE-entiteter skickas till critic-LLM för filtrering.
    Edges som refererar avvisade CREATE-noder tas bort.

    Populerar _shared._last_critic_approved and _shared._last_critic_rejected.

    Returns:
        Filtrerad ingestion_payload.
    """
    if not ingestion_payload:
        _shared._last_critic_approved = []
        _shared._last_critic_rejected = []
        return []

    # Separera per action-typ
    link_mentions = [m for m in ingestion_payload if m.get("action") == "LINK"]
    create_mentions = [m for m in ingestion_payload if m.get("action") == "CREATE"]
    edge_mentions = [m for m in ingestion_payload if m.get("action") == "CREATE_EDGE"]

    # Auto-godkänn alla LINK-mentions
    link_approved = [
        {"name": m["label"], "type": m["type"], "reason": "LINK (auto-approved)"}
        for m in link_mentions
    ]

    if not create_mentions:
        # Inget att granska — alla är LINK
        _shared._last_critic_approved = link_approved
        _shared._last_critic_rejected = []
        LOGGER.info(f"Critic: {len(link_mentions)} LINK (auto), 0 CREATE")
        return link_mentions + edge_mentions

    # Skicka CREATE-mentions till critic-LLM
    entities_for_review = [
        {"name": m["label"], "type": m["type"], "confidence": m.get("confidence", 0.5)}
        for m in create_mentions
    ]

    result = _call_critic_llm(entities_for_review)
    approved_names = {e["name"] for e in result.get("approved", [])}

    # Filtrera CREATE-mentions
    approved_creates = [m for m in create_mentions if m["label"] in approved_names]
    rejected_creates = [m for m in create_mentions if m["label"] not in approved_names]

    # Samla UUID:n från avvisade noder för edge-cleanup
    rejected_uuids = {m["target_uuid"] for m in rejected_creates}

    # Filtrera bort edges som refererar avvisade noder
    if rejected_uuids:
        filtered_edges = [
            e for e in edge_mentions
            if e.get("source_uuid") not in rejected_uuids
            and e.get("target_uuid") not in rejected_uuids
        ]
        edges_removed = len(edge_mentions) - len(filtered_edges)
        if edges_removed > 0:
            LOGGER.info(f"Critic: Removed {edges_removed} edges referencing rejected nodes")
    else:
        filtered_edges = edge_mentions

    # Populera shared state för testkompatibilitet (test_ingestion_cycle.py)
    _shared._last_critic_approved = link_approved + result.get("approved", [])
    _shared._last_critic_rejected = result.get("rejected", [])

    # Logga statistik
    LOGGER.info(
        f"Critic: {len(link_mentions)} LINK (auto), "
        f"{len(approved_creates)} CREATE approved, "
        f"{len(rejected_creates)} CREATE rejected"
    )
    for rej in result.get("rejected", [])[:5]:
        LOGGER.debug(f"  Avvisad: {rej.get('name')} ({rej.get('type')}) - {rej.get('reason', 'N/A')}")

    return link_mentions + approved_creates + filtered_edges
