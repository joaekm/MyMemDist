"""
Enrichment prompt construction.

Builds the enrichment LLM prompt with token budget management.
Assembles entity data, edges, neighbor summaries, schema reference, and Lake docs.
"""

import json
import logging
from typing import Dict, List, Optional

from services.utils.schema_validator import SchemaValidator

LOGGER = logging.getLogger(__name__)


def count_tokens(text: str) -> int:
    """Count tokens in text. ~4 chars/token heuristic."""
    return len(text) // 4


def build_schema_reference(entities: List[Dict], edges: List[Dict],
                           schema: dict, enrichment_config: dict) -> str:
    """Build schema reference with full enum values and descriptions."""
    system_properties = enrichment_config.get('system_properties', [])

    section = "## SCHEMA-REFERENS\n\n"
    section += "### SYSTEM-PROPERTIES (RÖR EJ — returnera ALDRIG dessa)\n"
    section += f"{system_properties}\n\n"

    section += "### Nodtyper och deras properties\n\n"
    seen_types = set(e.get("type", "") for e in entities)
    for node_type in sorted(seen_types):
        node_schema = schema.get("nodes", {}).get(node_type, {})
        if not node_schema:
            continue
        section += f"**{node_type}**: {node_schema.get('description', '')}\n"
        for prop_name, prop_def in node_schema.get("properties", {}).items():
            ptype = prop_def.get("type", "string")
            desc = prop_def.get("description", "")
            if ptype == "enum":
                values = prop_def.get("values", [])
                section += f"  - `{prop_name}` (enum: {values}) {desc}\n"
            else:
                section += f"  - `{prop_name}` ({ptype}) {desc}\n"
        section += "\n"

    edge_types_present = set(e["type"] for e in edges)
    if edge_types_present:
        section += "### Edge-typer och deras properties\n\n"
        for et in sorted(edge_types_present):
            es = schema.get("edges", {}).get(et, {})
            if not es:
                continue
            section += f"**{et}**: {es.get('description', '')}\n"
            for prop_name, prop_def in es.get("properties", {}).items():
                ptype = prop_def.get("type", "string")
                if ptype == "enum":
                    values = prop_def.get("values", [])
                    section += f"  - `{prop_name}` (enum: {values})\n"
                else:
                    section += f"  - `{prop_name}` ({ptype})\n"
            section += "\n"

    return section


def format_entity(ent: Dict, neighbor_summaries: Dict, schema: dict) -> str:
    """Format one entity as prompt text."""
    props = ent.get("properties", {})
    node_type = ent.get("type", "Unknown")
    name = props.get("name", ent.get("id", "?"))

    section = f"### [{node_type}] {name}\n"
    section += f"- ID: {ent['id']}\n"
    section += f"- Aliases: {ent.get('aliases', [])}\n"

    # Type-specific properties (skip system props)
    schema_node = schema.get("nodes", {}).get(node_type, {})
    type_props = schema_node.get("properties", {})
    if type_props:
        filled = {k: props[k] for k in type_props if k in props and props[k]}
        missing = [k for k in type_props if k not in props or not props[k]]
        if filled:
            section += f"- Fyllda: {json.dumps(filled, ensure_ascii=False)}\n"
        if missing:
            section += f"- SAKNAS: {missing}\n"

    # Compact neighbor references
    if neighbor_summaries and ent["id"] in neighbor_summaries:
        refs = neighbor_summaries[ent["id"]]
        section += f"- Grannskap ({len(refs)} relationer):\n"
        for ref in refs[:15]:
            section += f"  - {ref}\n"

    section += "\n"
    return section


def format_lake_doc(doc: Dict) -> str:
    """Format one Lake document as prompt text."""
    section = f"### {doc['filename']}\n"
    section += f"- unit_id: {doc['unit_id']}\n"
    section += f"- Typ: {doc['source_type']}\n"
    section += f"- Nyckelord: {', '.join(doc.get('document_keywords', []))}\n\n"
    return section


def format_chunk_context(chunks: List[Dict], entity_names: Dict[str, str]) -> str:
    """Format retrieved chunks as prompt context.

    Args:
        chunks: List of chunk dicts from _get_chunks_for_entities()
        entity_names: Mapping entity_id -> display name

    Returns:
        Formatted text section for the enrichment prompt.
    """
    if not chunks:
        return ""

    text = ""
    for chunk in chunks:
        source_type = chunk.get("source_type", "unknown")
        filename = chunk.get("filename", "?")
        part = chunk.get("part_number", "")
        part_str = f" (del {part})" if part != "" else ""

        # Map entity_ids to names
        eids = chunk.get("entity_ids", [])
        names = [entity_names.get(eid, eid[:12]) for eid in eids]
        mentions_str = ", ".join(names)

        doc_text = chunk.get("document_text", "")

        text += f"### [{source_type}] {filename}{part_str}\n"
        if mentions_str:
            text += f"Nämner: {mentions_str}\n"
        text += "---\n"
        text += doc_text + "\n\n"

    return text


def build_neighbor_summaries(engine, entity_ids: set, budget: int) -> Dict[str, List[str]]:
    """Build compact neighbor summary strings for entities."""
    from services.utils.schema_validator import SchemaValidator

    neighbor_summaries = {}
    remaining_budget = budget

    schema_validator = SchemaValidator()
    source_edges = schema_validator.get_source_edge_types()

    for eid in entity_ids:
        edges_out = engine.graph_service.get_edges_from(eid)
        edges_in = engine.graph_service.get_edges_to(eid)
        edges_out = [e for e in edges_out if e["type"] not in source_edges]
        edges_in = [e for e in edges_in if e["type"] not in source_edges]

        summaries = []
        for e in edges_out:
            other = engine.graph_service.get_node(e["target"])
            if other:
                name = other.get("properties", {}).get("name", e["target"])
                summaries.append(f"\u2192[{e['type']}]\u2192 {name} ({other.get('type', '?')})")
                rc = e.get("properties", {}).get("relation_context", [])
                for entry in rc[-3:]:
                    ts = entry.get("timestamp", "?")
                    text = entry.get("text", "")
                    if text:
                        summaries.append(f"    [{ts}] {text}")
        for e in edges_in:
            other = engine.graph_service.get_node(e["source"])
            if other:
                name = other.get("properties", {}).get("name", e["source"])
                summaries.append(f"\u2190[{e['type']}]\u2190 {name} ({other.get('type', '?')})")
                rc = e.get("properties", {}).get("relation_context", [])
                for entry in rc[-3:]:
                    ts = entry.get("timestamp", "?")
                    text = entry.get("text", "")
                    if text:
                        summaries.append(f"    [{ts}] {text}")

        if summaries:
            neighbor_text = f"- Grannskap ({len(summaries)} relationer):\n"
            for ref in summaries[:15]:
                neighbor_text += f"  - {ref}\n"
            n_tokens = count_tokens(neighbor_text)
            if n_tokens <= remaining_budget:
                neighbor_summaries[eid] = summaries
                remaining_budget -= n_tokens

    return neighbor_summaries


def build_enrich_prompt(engine, all_nodes: List[Dict],
                        token_budget: Optional[int] = None) -> tuple:
    """Build enrich prompt incrementally with token budget.

    Returns: (prompt_text, stats_dict, selected_entities, edges, neighbor_summaries)
    """
    enrichment_config = engine.enrichment_config

    if token_budget is None:
        token_budget = enrichment_config.get('token_budget', 22500)

    entity_ceiling_pct = enrichment_config.get('entity_ceiling_pct', 0.60)
    neighbor_budget_pct = enrichment_config.get('neighbor_budget_pct', 0.40)

    instruction = engine.prompts.get("enrichment_prompt", "")
    if not instruction:
        LOGGER.error("Missing enrichment_prompt in config")
        return "", {}, [], [], {}

    stats = {"entities": 0, "entities_skipped": 0, "edges": 0, "docs": 0,
             "docs_skipped": 0, "neighbors": 0, "chunks": 0, "chunks_skipped": 0}

    instruction_tokens = count_tokens(instruction)
    tokens_used = instruction_tokens

    # Reserve for schema
    schema_reserve = 2000
    tokens_used += schema_reserve

    # --- Priority 1: Entities ---
    entity_header = "## BERÖRDA ENTITETER\n\n"
    tokens_used += count_tokens(entity_header)

    selected_entities = []
    entity_ids = set()
    schema = engine.schema_validator.schema

    entity_ceiling = int(token_budget * entity_ceiling_pct)
    for node in all_nodes:
        ent_text = format_entity(node, {}, schema)
        ent_tokens = count_tokens(ent_text)

        if tokens_used + ent_tokens > entity_ceiling and selected_entities:
            stats["entities_skipped"] += 1
            continue

        tokens_used += ent_tokens
        selected_entities.append(node)
        entity_ids.add(node["id"])
        stats["entities"] += 1

    # --- Priority 2: Edges ---
    edges = engine._get_edges_between(entity_ids)
    edge_section = "## RELATIONER MELLAN ENTITETER\n\n"
    if edges:
        for e in edges:
            edge_section += f"- {e['source']} \u2014[{e['type']}]\u2192 {e['target']}\n"
            if e.get("properties"):
                edge_section += f"  Properties: {json.dumps(e['properties'], ensure_ascii=False)}\n"
    else:
        edge_section += "(Inga relationer mellan dessa entiteter)\n"

    tokens_used += count_tokens(edge_section)
    stats["edges"] = len(edges)

    # --- Priority 3: Neighbor summaries ---
    neighbor_budget = int((token_budget - tokens_used) * neighbor_budget_pct)
    neighbor_summaries = build_neighbor_summaries(engine, entity_ids, neighbor_budget)
    for eid, refs in neighbor_summaries.items():
        stats["neighbors"] += len(refs)
        tokens_used += count_tokens("\n".join(refs))

    # Re-build entity texts with neighbor info
    entity_texts_final = []
    for node in selected_entities:
        entity_texts_final.append(format_entity(node, neighbor_summaries, schema))

    # --- Build schema reference ---
    schema_section = build_schema_reference(selected_entities, edges, schema, enrichment_config)
    actual_schema_tokens = count_tokens(schema_section)
    tokens_used = tokens_used - schema_reserve + actual_schema_tokens

    # --- Priority 4: Context from source documents ---
    chunk_config = enrichment_config.get('chunk_context', {})
    use_chunks = chunk_config.get('enabled', False)

    context_section = ""
    if use_chunks:
        # New approach: original text chunks from VectorDB
        chunks = engine._get_chunks_for_entities(entity_ids)
        max_total = chunk_config.get('max_total_chunks', 10)

        # Build entity name mapping for display
        entity_name_map = {}
        for node in selected_entities:
            props = node.get("properties", {})
            entity_name_map[node["id"]] = props.get("name", node["id"][:12])

        context_header = "## KÄLLTEXT-KONTEXT (originaltext från dokument)\n\n"
        context_budget = token_budget - tokens_used - count_tokens(context_header)

        selected_chunks = []
        for chunk in chunks:
            chunk_text = format_chunk_context([chunk], entity_name_map)
            chunk_tokens = count_tokens(chunk_text)
            if chunk_tokens > context_budget and selected_chunks:
                stats["chunks_skipped"] += 1
                continue
            selected_chunks.append(chunk_text)
            context_budget -= chunk_tokens
            tokens_used += chunk_tokens
            stats["chunks"] += 1

        if selected_chunks:
            context_section = context_header + "".join(selected_chunks)

        stats["docs"] = 0
    else:
        # Legacy approach: Lake metadata only (rollback)
        all_docs = engine._get_lake_docs_for_entities(entity_ids)
        doc_header = "## NYLIGEN INGESTADE DOKUMENT\n\n"
        doc_texts = []
        doc_budget = token_budget - tokens_used - count_tokens(doc_header)

        for doc in all_docs:
            doc_text = format_lake_doc(doc)
            doc_tokens = count_tokens(doc_text)
            if doc_tokens > doc_budget and doc_texts:
                stats["docs_skipped"] += 1
                continue
            doc_texts.append(doc_text)
            doc_budget -= doc_tokens
            tokens_used += doc_tokens
            stats["docs"] += 1

        if doc_texts:
            context_section = doc_header + "".join(doc_texts)

    # --- Assemble ---
    prompt = ""
    if context_section:
        prompt += context_section
    prompt += entity_header + "".join(entity_texts_final)
    prompt += edge_section
    prompt += schema_section
    prompt += instruction

    actual_tokens = count_tokens(prompt)
    stats["total_tokens"] = actual_tokens
    stats["budget"] = token_budget
    stats["utilization"] = actual_tokens / token_budget if token_budget > 0 else 0

    return prompt, stats, selected_entities, edges, neighbor_summaries
