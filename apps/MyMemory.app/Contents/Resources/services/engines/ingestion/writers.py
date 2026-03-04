"""
Storage writers for ingestion: Lake, Graph, Vector.

Write order is important: Graph -> Vector -> Lake (Lake = commit receipt).
"""

import logging
import os
import yaml
from typing import Dict, List

from services.utils.graph_service import GraphService
from services.utils.metadata_service import get_owner_name
from services.utils.terminal_status import entity_detail
from services.engines.ingestion import _shared
from services.engines.ingestion._shared import (
    CONFIG, LAKE_STORE, _get_schema_validator, LOGGER,
)
def write_lake(unit_id: str, filename: str, raw_text: str, source_type: str,
               semantic_metadata: Dict, ingestion_payload: List,
               timestamp_ingestion: str = None, timestamp_content: str = None) -> str:
    """Write document to Lake with frontmatter."""
    base_name = os.path.splitext(filename)[0]
    lake_file = os.path.join(LAKE_STORE, f"{base_name}.md")

    # HARDFAIL if semantic metadata is incomplete — Lake is the commit receipt
    ai_model = semantic_metadata.get("ai_model")
    if not ai_model:
        raise RuntimeError(f"HARDFAIL: write_lake({filename}) — semantic_metadata missing 'ai_model'. metadata_service likely failed.")

    if not timestamp_ingestion or not timestamp_content:
        raise RuntimeError(f"HARDFAIL: write_lake({filename}) — timestamps must be provided by caller")

    default_access_level = CONFIG.get('security', {}).get('default_access_level', 5)

    frontmatter = {
        "unit_id": unit_id,
        "source_ref": lake_file,
        "original_filename": filename,
        "timestamp_ingestion": timestamp_ingestion,
        "timestamp_content": timestamp_content,
        "timestamp_updated": None,
        "source_type": source_type,
        "access_level": default_access_level,
        "owner": get_owner_name(),
        "context_summary": semantic_metadata.get("context_summary", ""),
        "relations_summary": semantic_metadata.get("relations_summary", ""),
        "document_keywords": semantic_metadata.get("document_keywords", []),
        "ai_model": ai_model,
    }

    fm_str = yaml.dump(frontmatter, sort_keys=False, allow_unicode=True)
    with open(lake_file, 'w', encoding='utf-8') as f:
        f.write(f"---\n{fm_str}---\n\n# {filename}\n\n{raw_text}")

    LOGGER.info(f"Lake: {filename} ({source_type}) -> {len(ingestion_payload)} mentions")
    return lake_file


def write_graph(unit_id: str, filename: str, ingestion_payload: List,
                source_type: str = None, timestamp_content: str = None,
                vector_service=None) -> tuple:
    """Write entities and edges to graph, and index nodes to vector."""
    if source_type is None:
        source_type = _get_schema_validator().get_default_source_type()
    graph = GraphService(_shared.GRAPH_DB_PATH)
    if vector_service is None:
        from services.utils.vector_service import get_vector_service
        vector_service = get_vector_service("knowledge_base")

    # Skapa Source-nod för källdokumentet (krävs för source→entity-kanter)
    doc_node_type = _get_schema_validator().get_document_node_type()
    graph.upsert_node(
        id=unit_id,
        type=doc_node_type,
        properties={
            "name": filename,
            "status": "ACTIVE",
            "source_system": "IngestionEngine",
            "source_type": source_type
        }
    )

    nodes_written = 0
    edges_written = 0

    for entity in ingestion_payload:
        action = entity.get("action")

        if action in ["CREATE", "LINK"]:
            target_uuid = entity.get("target_uuid")
            node_type = entity.get("type")
            label = entity.get("label")
            confidence = entity.get("confidence", 0.5)

            if not target_uuid or not node_type:
                continue
            if not label:
                LOGGER.error(f"HARDFAIL: Entity missing label: {entity}")
                continue

            props = {
                "name": label,
                "confidence": confidence,
                "source_system": "IngestionEngine"
            }

            # Add type-specific properties from LLM extraction
            type_props = entity.get("properties", {})
            if type_props:
                props.update(type_props)

            graph.upsert_node(
                id=target_uuid,
                type=node_type,
                properties=props
            )

            # Index node to vector immediately (not wait for Dreamer)
            vector_service.upsert_node({
                'id': target_uuid,
                'type': node_type,
                'properties': props,
                'aliases': []
            })
            nodes_written += 1

            # OBJEKT-107: Create source→entity edge (schema-driven type)
            source_edge_types = _get_schema_validator().get_source_edge_types()
            source_edge_def = {
                et: _get_schema_validator().schema['edges'][et]
                for et in source_edge_types
            }
            # Check if this node_type is a valid target for any source edge
            valid_edge = next(
                (et for et, ed in source_edge_def.items()
                 if node_type in ed.get('target_type', [])),
                None
            )
            if valid_edge:
                try:
                    graph.upsert_edge(
                        source=unit_id,
                        target=target_uuid,
                        edge_type=valid_edge,
                        properties={"confidence": confidence}
                    )
                    edges_written += 1
                except ValueError as e:
                    LOGGER.warning(f"Edge skipped (schema guard): {e}")

        elif action == "CREATE_EDGE":
            source_uuid = entity.get("source_uuid")
            target_uuid = entity.get("target_uuid")
            edge_type = entity.get("edge_type")
            edge_conf = entity.get("confidence", 0.5)

            if source_uuid and target_uuid and edge_type:
                # Self-loop guard (defense in depth)
                if source_uuid == target_uuid:
                    LOGGER.warning(
                        f"Self-loop blocked in write_graph: "
                        f"{entity.get('source_name', '?')} -[{edge_type}]-> "
                        f"{entity.get('target_name', '?')} ({source_uuid[:12]})"
                    )
                    continue

                edge_props = {"confidence": edge_conf}
                extra_edge_props = entity.get("edge_properties", {})
                if extra_edge_props:
                    edge_props.update(extra_edge_props)

                # Bygg relation_context entry om text finns
                rc_text = entity.get("relation_context_text", "")
                if rc_text and timestamp_content and timestamp_content != "UNKNOWN":
                    edge_props["relation_context"] = [{
                        "text": rc_text,
                        "origin": unit_id,
                        "timestamp": timestamp_content
                    }]
                elif rc_text:
                    LOGGER.warning(
                        f"relation_context dropped — invalid timestamp "
                        f"'{timestamp_content}' for edge "
                        f"{entity.get('source_name', '?')} -[{edge_type}]-> "
                        f"{entity.get('target_name', '?')}"
                    )

                # Quality gate: drop edges that only have confidence AND require
                # additional properties to be meaningful.
                semantic_keys = set(edge_props.keys()) - {"confidence"}
                if not semantic_keys:
                    schema = _get_schema_validator().schema
                    edge_def = schema.get('edges', {}).get(edge_type, {})
                    schema_props = edge_def.get('properties', {})
                    extra_required = [
                        p for p, d in schema_props.items()
                        if isinstance(d, dict) and d.get('required') and p != 'confidence'
                    ]
                    if extra_required:
                        source_name = entity.get("source_name", "?")
                        target_name = entity.get("target_name", "?")
                        LOGGER.warning(
                            f"Edge dropped (no semantic properties, "
                            f"missing required: {extra_required}): "
                            f"{source_name} -[{edge_type}]-> {target_name}"
                        )
                        continue

                try:
                    graph.upsert_edge(
                        source=source_uuid,
                        target=target_uuid,
                        edge_type=edge_type,
                        properties=edge_props
                    )
                    edges_written += 1
                except ValueError as e:
                    LOGGER.warning(f"Edge skipped (schema guard): {e}")
                    continue

                # Terminal output: visa sparad relation
                entity_detail(
                    source_name=entity.get("source_name", "?"),
                    source_type=entity.get("source_type", "?"),
                    edge_type=edge_type,
                    target_name=entity.get("target_name", "?"),
                    target_type=entity.get("target_type", "?")
                )

    graph.close()
    LOGGER.info(f"Graph: {filename} -> {nodes_written} nodes, {edges_written} edges")
    return nodes_written, edges_written


def write_vector(unit_id: str, filename: str, raw_text: str, source_type: str,
                 semantic_metadata: Dict, timestamp_ingestion: str, vector_service=None):
    """
    Write document to vector index.

    For transcripts with Rich Transcriber Del-struktur:
    - Indexes each part as a separate chunk for precise semantic search

    For other documents:
    - Applies content chunking for long documents
    - Falls back to single document indexing
    """
    if vector_service is None:
        from services.utils.vector_service import get_vector_service
        vector_service = get_vector_service("knowledge_base")

    from services.utils.parts_parser_service import (
        has_transcript_parts,
        extract_transcript_parts,
        build_chunk_text,
        chunk_document,
        build_overview_chunk_text
    )

    # Check if transcript with parts structure
    transcript_type = _get_schema_validator().get_source_type_mappings().get('transcripts', '')
    if source_type == transcript_type and has_transcript_parts(raw_text):
        parts = extract_transcript_parts(raw_text)
        if parts:
            LOGGER.info(f"Vector: {filename} -> {len(parts)} chunks (transcript parts)")

            for part in parts:
                chunk_id = f"{unit_id}__part_{part.part_number}"
                chunk_text = build_chunk_text(part)

                vector_service.upsert(
                    id=chunk_id,
                    text=chunk_text,
                    metadata={
                        "timestamp": timestamp_ingestion,
                        "filename": filename,
                        "source_type": source_type,
                        "parent_id": unit_id,
                        "part_number": part.part_number,
                        "title": part.title,
                        "time_start": part.time_start or "",
                        "time_end": part.time_end or ""
                    }
                )
            return

    # Try document chunking for long non-transcript docs (OBJEKT-100)
    chunking_cfg = CONFIG.get('processing', {}).get('chunking', {})
    chunk_size = chunking_cfg.get('chunk_size', 2000)
    chunk_overlap = chunking_cfg.get('chunk_overlap', 200)
    chunk_threshold = chunking_cfg.get('chunk_threshold', 4000)
    overview_content_chars = chunking_cfg.get('overview_content_chars', 500)

    ctx_summary = semantic_metadata.get("context_summary", "")
    rel_summary = semantic_metadata.get("relations_summary", "")

    parts = chunk_document(raw_text, source_type, chunk_size, chunk_overlap, chunk_threshold)

    if parts:
        # Part 0: Overview chunk
        overview_text = build_overview_chunk_text(
            filename, ctx_summary, rel_summary, raw_text, overview_content_chars
        )
        vector_service.upsert(
            id=f"{unit_id}__part_0",
            text=overview_text,
            metadata={
                "timestamp": timestamp_ingestion,
                "filename": filename,
                "source_type": source_type,
                "parent_id": unit_id,
                "part_number": 0,
                "title": "Overview",
            }
        )

        # Parts 1..N: Content chunks
        for part in parts:
            chunk_id = f"{unit_id}__part_{part.part_number}"
            chunk_text = build_chunk_text(part)
            vector_service.upsert(
                id=chunk_id,
                text=chunk_text,
                metadata={
                    "timestamp": timestamp_ingestion,
                    "filename": filename,
                    "source_type": source_type,
                    "parent_id": unit_id,
                    "part_number": part.part_number,
                    "title": part.title or "",
                    "time_start": part.time_start or "",
                    "time_end": part.time_end or "",
                }
            )

        total = len(parts) + 1
        LOGGER.info(f"Vector: {filename} -> {total} chunks ({source_type} chunking)")
        return

    # Standard document indexing (short docs or fallback)
    vector_text = f"FILENAME: {filename}\nSUMMARY: {ctx_summary}\nRELATIONS: {rel_summary}\n\nCONTENT:\n{raw_text[:8000]}"

    vector_service.upsert(
        id=unit_id,
        text=vector_text,
        metadata={
            "timestamp": timestamp_ingestion,
            "filename": filename,
            "source_type": source_type
        }
    )
    LOGGER.info(f"Vector: {filename} -> ChromaDB")


def clean_before_reingest(unit_id: str, filename: str, lake_file: str, vector_service=None):
    """
    Clean stale data from Graph + Vector before re-ingestion.

    Deletes orphan entities (no edges besides MENTIONS from this document),
    removes Source node + MENTIONS edges, and clears vector entries.

    Does NOT touch Assets (they're the source of truth for re-ingest).
    Lake file is deleted so write_lake() can recreate it as the final "commit".
    """
    graph = GraphService(_shared.GRAPH_DB_PATH)
    if vector_service is None:
        from services.utils.vector_service import get_vector_service
        vs = get_vector_service("knowledge_base")
    else:
        vs = vector_service

    try:
        # 1. Check for orphan entities (only connected via this document)
        edges = graph.get_edges_from(unit_id)
        source_edges = _get_schema_validator().get_source_edge_types()
        mentions = [e for e in edges if e.get("type") in source_edges]

        entities_deleted = 0

        for edge in mentions:
            entity_id = edge["target"]
            other_incoming = [e for e in graph.get_edges_to(entity_id) if e["source"] != unit_id]
            other_outgoing = graph.get_edges_from(entity_id)

            if not other_incoming and not other_outgoing:
                graph.delete_node(entity_id)
                vs.delete(entity_id)
                entities_deleted += 1

        # 2. Delete Source node + MENTIONS edges
        graph.delete_node(unit_id)

        # 3. Delete vector entries (document + transcript chunks)
        vs.delete(unit_id)
        vs.delete_by_parent(unit_id)

    finally:
        graph.close()

    # 4. Delete Lake file (will be recreated as final "commit" step)
    try:
        os.remove(lake_file)
    except OSError as e:
        LOGGER.warning(f"Could not remove Lake file during re-ingest: {e}")

    LOGGER.info(
        f"Re-ingest cleanup: {filename} -> {entities_deleted} orphans deleted"
    )
