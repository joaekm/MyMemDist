"""
MetadataService - Genererar semantisk metadata (context_summary, relations_summary, document_keywords).

Används av:
- ingestion_engine.py (nygenering vid initial ingestion)
- dreamer.py (förädling efter graf-operationer)

Designprinciper:
- En funktion, en prompt, samma output oavsett anropare
- Schema-driven graf-berikning (ingen hårdkodning av edge_types)
- graph_schema_template.json är SSOT
"""

import os
import json
import yaml
import logging
from typing import Dict, Any, List, Optional

from services.utils.llm_service import LLMService
from services.utils.json_parser import parse_llm_json
from services.utils.graph_service import GraphService
from services.utils.schema_validator import SchemaValidator

LOGGER = logging.getLogger("MetadataService")

# Lazy-loaded singletons
_LLM_SERVICE = None
_SCHEMA = None
_CONFIG = None
_PROMPTS = None
_USER_PROFILE = None


def _get_llm_service() -> LLMService:
    """Get or create LLMService singleton."""
    global _LLM_SERVICE
    if _LLM_SERVICE is None:
        _LLM_SERVICE = LLMService()
    return _LLM_SERVICE


def _get_schema() -> dict:
    """Get or load schema from graph_schema_template.json."""
    global _SCHEMA
    if _SCHEMA is None:
        validator = SchemaValidator()
        _SCHEMA = validator.schema
    return _SCHEMA


def _load_config() -> tuple:
    """Load config and prompts."""
    global _CONFIG, _PROMPTS
    if _CONFIG is None:
        from services.utils.config_loader import get_config, get_prompts, get_expanded_paths

        raw_config = get_config()
        _CONFIG = dict(raw_config)
        _CONFIG['paths'] = get_expanded_paths()
        _PROMPTS = get_prompts()

    return _CONFIG, _PROMPTS


def _get_prompt(agent: str, key: str) -> str:
    """Get prompt from config."""
    _, prompts = _load_config()
    if prompts and 'prompts' in prompts:
        return prompts['prompts'].get(agent, {}).get(key, "")
    elif prompts:
        return prompts.get(agent, {}).get(key, "")
    return ""


def _get_user_profile() -> Dict[str, Any]:
    """
    Läs user_profile.yaml för ägarinformation.

    Returns:
        {
            "name": "Joakim Ekman",
            "id": "joakim.ekman",
            "role": "Enhetschef för Drive-enheten",
            "company": "Digitalist Open Tech AB"
        }
    """
    global _USER_PROFILE
    if _USER_PROFILE is not None:
        return _USER_PROFILE

    config, _ = _load_config()

    # user_profile.yaml ligger i Index-mappen (härleds från graph_db)
    graph_db = config['paths'].get('graph_db', '')
    if not graph_db:
        raise RuntimeError("HARDFAIL: graph_db path saknas i config")
    index_path = os.path.dirname(os.path.expanduser(graph_db))

    profile_path = os.path.join(index_path, 'user_profile.yaml')

    if not os.path.exists(profile_path):
        LOGGER.warning(f"user_profile.yaml not found at {profile_path}")
        _USER_PROFILE = {}
        return _USER_PROFILE

    with open(profile_path, 'r') as f:
        profile = yaml.safe_load(f)

    identity = profile.get('identity', {})
    _USER_PROFILE = {
        "name": identity.get('name', ''),
        "id": identity.get('id', ''),
        "role": identity.get('role', ''),
        "company": identity.get('company', '')
    }
    LOGGER.debug(f"Loaded user profile: {_USER_PROFILE.get('name')}")

    return _USER_PROFILE


def get_owner_name() -> str:
    """
    Returnera ägarens namn för Lake-filer.

    Returns:
        Ägarens namn (t.ex. "Joakim Ekman") eller tom sträng om profil saknas.
    """
    profile = _get_user_profile()
    return profile.get('name', '')


def _get_relevant_edge_types_for_node(schema: dict, node_type: str) -> List[dict]:
    """
    Schema-driven: Hitta alla edge_types där node_type är source.

    Returns:
        [
            {
                "edge_type": "BELONGS_TO",
                "target_types": ["Organization", "Group"],
                "properties": ["job_title", "job_function", ...]
            },
            ...
        ]
    """
    relevant = []
    for edge_type, edge_def in schema.get('edges', {}).items():
        # Skippa MENTIONS - det är dokument → entitet, inte entitet → entitet
        if edge_type == "MENTIONS":
            continue

        source_types = edge_def.get('source_type', [])
        if node_type in source_types:
            # Hämta property-namn (exkludera confidence)
            props = edge_def.get('properties', {})
            prop_names = [p for p in props.keys() if p != 'confidence']

            relevant.append({
                "edge_type": edge_type,
                "target_types": edge_def.get('target_type', []),
                "properties": prop_names,
                "description": edge_def.get('description', '')
            })

    return relevant


def _enrich_entities_from_graph(
    resolved_entities: List[Dict],
    graph_path: str
) -> Dict[str, Dict]:
    """
    Hämta graf-relationer för varje entity.

    Schema-driven: Läser graph_schema_template.json för att hitta relevanta edge_types.

    Returns:
        {
            "uuid-123": {
                "name": "Marc",
                "type": "Person",
                "relations": [
                    {
                        "edge_type": "BELONGS_TO",
                        "target_name": "Digitalist",
                        "target_type": "Organization",
                        "job_title": "VD"
                    },
                    ...
                ]
            }
        }
    """
    if not resolved_entities:
        return {}

    # Kolla om graf finns
    if not os.path.exists(graph_path):
        LOGGER.debug(f"Graph not found at {graph_path}, skipping enrichment")
        return {}

    schema = _get_schema()
    enriched = {}

    try:
        graph = GraphService(graph_path, read_only=True)

        for entity in resolved_entities:
            action = entity.get("action")
            if action not in ["LINK", "CREATE"]:
                continue

            uuid = entity.get("target_uuid")
            node_type = entity.get("type")
            name = entity.get("canonical_name", entity.get("label", ""))

            if not uuid or not node_type:
                continue

            # Hitta relevanta edge_types för denna nodtyp (schema-driven)
            relevant_edges = _get_relevant_edge_types_for_node(schema, node_type)

            if not relevant_edges:
                # Denna nodtyp har inga utgående relationer
                enriched[uuid] = {
                    "name": name,
                    "type": node_type,
                    "relations": []
                }
                continue

            entity_data = {
                "name": name,
                "type": node_type,
                "relations": []
            }

            # Hämta alla edges från denna nod
            edges = graph.get_edges_from(uuid)

            for edge in edges:
                edge_type = edge.get("type")

                # Kolla om denna edge_type är relevant enligt schemat
                edge_schema = next(
                    (e for e in relevant_edges if e["edge_type"] == edge_type),
                    None
                )
                if not edge_schema:
                    continue

                # Hämta target node
                target_id = edge.get("target")
                target = graph.get_node(target_id)
                if not target:
                    continue

                target_props = target.get("properties", {})
                edge_props = edge.get("properties", {})

                # Bygg relation-data
                relation = {
                    "edge_type": edge_type,
                    "target_name": target_props.get("name", ""),
                    "target_type": target.get("type", ""),
                }

                # Inkludera edge properties som finns i schemat
                for prop in edge_schema["properties"]:
                    if prop in edge_props:
                        relation[prop] = edge_props[prop]

                # Inkludera target node_context (första texten, max 200 tecken)
                node_context = target_props.get("node_context", [])
                if node_context and isinstance(node_context, list) and len(node_context) > 0:
                    first_ctx = node_context[0]
                    if isinstance(first_ctx, dict):
                        ctx_text = first_ctx.get("text", "")
                        if ctx_text:
                            relation["target_context"] = ctx_text[:200]

                entity_data["relations"].append(relation)

            enriched[uuid] = entity_data

        graph.close()
    except (OSError, IOError) as e:
        # Graf-fil kan saknas eller vara låst - returnera tom dict
        LOGGER.warning(f"Could not enrich entities (graph unavailable): {e}")

    return enriched


def _build_owner_context_string() -> str:
    """
    Bygg ägar-kontext för LLM-prompt.

    Informerar LLM om att dokumentet tillhör ägaren, så att
    sammanfattningar skrivs ur ägarens perspektiv och inte
    upprepar ägarens namn i varje mening.

    Returns:
        En instruktionssträng för LLM, t.ex.:
        "DOKUMENTÄGARE: Joakim Ekman (Enhetschef för Drive-enheten, Digitalist Open Tech AB)
         Skriv sammanfattningar ur ägarens perspektiv. Nämn inte ägaren explicit i varje mening."
    """
    profile = _get_user_profile()

    if not profile.get('name'):
        return ""

    name = profile.get('name', '')
    role = profile.get('role', '')
    company = profile.get('company', '')

    # Bygg beskrivning
    desc_parts = []
    if role:
        desc_parts.append(role)
    if company:
        desc_parts.append(company)

    desc = f" ({', '.join(desc_parts)})" if desc_parts else ""

    return f"""DOKUMENTÄGARE: {name}{desc}
Detta är {name}s personliga dokument. Skriv sammanfattningar ur ägarens perspektiv.
- Nämn INTE "{name}" explicit i context_summary eller relations_summary om det inte är relevant för dokumentets innehåll.
- Fokusera på VAD som hände, VEM ANDRA som var inblandade, och VILKA beslut som togs.
- Ägaren är implicit närvarande - du behöver inte upprepa det."""


def _build_entity_context_string(
    resolved_entities: List[Dict],
    enriched: Dict[str, Dict]
) -> str:
    """
    Bygg rik kontext-sträng för LLM-prompt.

    Exempel output:

    KÄNDA ENTITETER MED KONTEXT:

    - Marc Tersmantoll (Person)
      → BELONGS_TO: Digitalist (Organization) [job_title: VD]
      → HAS_ROLE: Styrgrupp (Roles)

    - Digitalist (Organization)
      → HAS_BUSINESS_RELATION: Industritorget (Organization) [relation_type: Kund]
    """
    if not resolved_entities:
        return "Inga kända entiteter i dokumentet."

    lines = ["KÄNDA ENTITETER MED KONTEXT:"]

    for entity in resolved_entities:
        action = entity.get("action")
        if action not in ["LINK", "CREATE"]:
            continue

        uuid = entity.get("target_uuid")
        name = entity.get("canonical_name", entity.get("label", ""))
        etype = entity.get("type", "")

        entity_line = f"\n- {name} ({etype})"

        # Lägg till relationer om de finns
        if uuid in enriched:
            relations = enriched[uuid].get("relations", [])
            for rel in relations:
                edge_type = rel["edge_type"]
                target_name = rel["target_name"]
                target_type = rel["target_type"]

                # Bygg relation-sträng
                rel_str = f"  → {edge_type}: {target_name} ({target_type})"

                # Lägg till edge properties
                extra_props = []
                for key, value in rel.items():
                    if key not in ["edge_type", "target_name", "target_type", "target_context"]:
                        if value:
                            extra_props.append(f"{key}: {value}")

                if extra_props:
                    rel_str += f" [{', '.join(extra_props)}]"

                entity_line += f"\n{rel_str}"

        lines.append(entity_line)

    return "\n".join(lines)


def generate_semantic_metadata(
    text: str,
    resolved_entities: List[Dict] = None,
    current_meta: Dict = None,
    filename: str = ""
) -> Dict[str, Any]:
    """
    Generera eller förädla semantisk metadata (context_summary, relations_summary, document_keywords).

    Används av:
    - ingestion_engine: current_meta=None → nygenering
    - dreamer: current_meta={...} → förädling av befintlig metadata

    Args:
        text: Dokumentets text (ingen begränsning)
        resolved_entities: Lista med resolved entities från ingestion
            [{"target_uuid": "...", "type": "Person", "canonical_name": "Marc", "action": "LINK"}, ...]
        current_meta: Befintlig metadata (för förädling). Om None → nygenering.
            {"context_summary": "...", "relations_summary": "...", "document_keywords": [...]}
        filename: Filnamn (för logging och prompt-kontext)

    Returns:
        {
            "context_summary": "...",
            "relations_summary": "...",
            "document_keywords": [...],
            "ai_model": "..."
        }
    """
    config, _ = _load_config()
    graph_path = config['paths']['graph_db']

    # Hämta graf-kontext (schema-driven)
    enriched = {}
    if resolved_entities:
        enriched = _enrich_entities_from_graph(resolved_entities, graph_path)

    # Bygg entity-kontext sträng
    entity_context = _build_entity_context_string(resolved_entities or [], enriched)

    # Hämta ägar-kontext för perspektiv
    owner_context = _build_owner_context_string()

    # Välj prompt baserat på läge
    if current_meta:
        # Förädlingsläge (dreamer)
        prompt_template = _get_prompt('dreamer', 'semantic_update')
        if not prompt_template:
            LOGGER.warning("semantic_update prompt missing, using current metadata")
            return {
                "context_summary": current_meta.get("context_summary", ""),
                "relations_summary": current_meta.get("relations_summary", ""),
                "document_keywords": current_meta.get("document_keywords", []),
                "ai_model": "SKIPPED"
            }

        # Injicera ägar-kontext i graf-insikten
        full_context = f"{owner_context}\n\n{entity_context}" if owner_context else entity_context

        prompt = prompt_template.format(
            filename=filename,
            current_context=current_meta.get("context_summary", ""),
            current_relations=current_meta.get("relations_summary", ""),
            current_keywords=json.dumps(current_meta.get("document_keywords", []), ensure_ascii=False),
            node_context=full_context
        )
    else:
        # Nygenereringsläge (ingestion)
        prompt_template = _get_prompt('doc_converter', 'doc_summary_prompt')
        if not prompt_template:
            LOGGER.warning("doc_summary_prompt missing, returning empty metadata")
            return {
                "context_summary": "",
                "relations_summary": "",
                "document_keywords": [],
                "ai_model": "FAILED"
            }

        # Injicera entity-kontext och ägar-kontext i texten
        context_parts = [text]
        if owner_context:
            context_parts.append(owner_context)
        if entity_context:
            context_parts.append(entity_context)

        enriched_text = "\n\n".join(context_parts)
        prompt = prompt_template.format(text=enriched_text)

    # Anropa LLM (Anthropic Sonnet för enrichment - OBJEKT-85)
    llm = _get_llm_service()
    response = llm.generate(prompt, provider='anthropic', model=llm.models['fast'])

    if not response.success:
        LOGGER.warning(f"Semantic metadata generation failed: {response.error}")
        if current_meta:
            # Vid förädling, behåll befintlig
            return {
                "context_summary": current_meta.get("context_summary", ""),
                "relations_summary": current_meta.get("relations_summary", ""),
                "document_keywords": current_meta.get("document_keywords", []),
                "ai_model": "FAILED"
            }
        return {
            "context_summary": "",
            "relations_summary": "",
            "document_keywords": [],
            "ai_model": "FAILED"
        }

    # Parsa JSON-svar
    data = parse_llm_json(response.text)

    return {
        "context_summary": data.get("context_summary", ""),
        "relations_summary": data.get("relations_summary", ""),
        "document_keywords": data.get("document_keywords", []) or data.get("keywords", []),
        "ai_model": response.model
    }
