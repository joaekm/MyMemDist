"""
DocumentManager — Lista, preview och radera dokument ur alla tre lager.

Används av:
- DocumentManagementView.swift (menubar-app)
- test_ingestion_cycle.py (E2E-test)

Logik:
    1. Graph — rensa node_context entries med origin == unit_id, orphan-entities tas bort helt
    2. Graph — delete Document-nod + MENTIONS-kanter
    3. Vector — delete dokument + transcript-chunks
    4. Lake — ta bort .md-fil
    5. Assets — flytta till Rejected/
"""

import os
import json
import shutil
import logging
import yaml

from services.utils.config_loader import get_config
from services.utils.graph_service import GraphService
from services.utils.vector_service import vector_scope
from services.utils.shared_lock import resource_lock

LOGGER = logging.getLogger("DocumentManager")


def _load_paths():
    config = get_config()
    paths = config.get('paths', {})
    return config, paths


def _get_lake_store(paths):
    return os.path.expanduser(paths.get('lake_store'))


def _get_graph_db_path(paths):
    return os.path.expanduser(paths.get('graph_db'))


def _get_asset_folders(paths):
    folders = [
        paths.get('asset_documents'),
        paths.get('asset_slack'),
        paths.get('asset_mail'),
        paths.get('asset_calendar'),
        paths.get('asset_transcripts'),
        paths.get('asset_ai_generated'),
    ]
    return [os.path.expanduser(f) for f in folders if f]


def _get_rejected_dir(paths):
    return os.path.expanduser(paths.get('asset_rejected', '~/MyMemory/Assets/Rejected'))


def parse_frontmatter(file_path: str) -> dict:
    """Läser YAML-frontmatter från en markdown-fil."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            if content.startswith('---'):
                parts = content.split('---', 2)
                if len(parts) >= 3:
                    return yaml.safe_load(parts[1]) or {}
        return {}
    except (OSError, yaml.YAMLError) as e:
        LOGGER.warning(f"Could not parse frontmatter for {file_path}: {e}")
        return {}


def find_lake_file(doc_id: str) -> str | None:
    """Hitta Lake-fil via unit_id eller filnamn."""
    _, paths = _load_paths()
    lake_store = _get_lake_store(paths)

    if not os.path.exists(lake_store):
        return None

    # Sök i filnamn
    for f in os.listdir(lake_store):
        if f.endswith('.md') and doc_id in f:
            return os.path.join(lake_store, f)

    # Sök i frontmatter unit_id
    for f in os.listdir(lake_store):
        if f.endswith('.md'):
            full_path = os.path.join(lake_store, f)
            fm = parse_frontmatter(full_path)
            if fm.get('unit_id') == doc_id:
                return full_path

    return None


def find_asset_file(original_filename: str) -> str | None:
    """Hitta Asset-fil i bevakade mappar."""
    if not original_filename:
        return None
    _, paths = _load_paths()
    asset_folders = _get_asset_folders(paths)

    for folder in asset_folders:
        if not os.path.exists(folder):
            continue
        candidate = os.path.join(folder, original_filename)
        if os.path.exists(candidate):
            return candidate
    return None


def list_documents(limit: int = 20) -> list[dict]:
    """Lista Lake-dokument sorterade efter timestamp_ingestion (nyast först)."""
    _, paths = _load_paths()
    lake_store = _get_lake_store(paths)

    if not os.path.exists(lake_store):
        return []

    docs = []
    for f in os.listdir(lake_store):
        if not f.endswith('.md'):
            continue
        full_path = os.path.join(lake_store, f)
        fm = parse_frontmatter(full_path)
        if not fm:
            continue

        docs.append({
            "unit_id": fm.get("unit_id", ""),
            "filename": f,
            "original_filename": fm.get("original_filename", ""),
            "source_type": fm.get("source_type", ""),
            "timestamp_ingestion": fm.get("timestamp_ingestion", ""),
            "context_summary": fm.get("context_summary", "")[:200],
        })

    docs.sort(key=lambda d: d.get("timestamp_ingestion", ""), reverse=True)
    return docs[:limit]


def collect_impact(unit_id: str, graph: GraphService) -> dict:
    """Samla information om vad deletion påverkar."""
    impact = {
        "document_node": False,
        "mentions_edges": 0,
        "entities_affected": [],
        "vector_entries": 0,
    }

    doc_node = graph.get_node(unit_id)
    if doc_node:
        impact["document_node"] = True

    edges = graph.get_edges_from(unit_id)
    mentions = [e for e in edges if e.get("type") == "MENTIONS"]
    impact["mentions_edges"] = len(mentions)

    for edge in mentions:
        entity_id = edge["target"]
        entity = graph.get_node(entity_id)
        if not entity:
            continue

        props = entity.get("properties", {})
        name = props.get("name", entity_id)
        node_type = entity.get("type", "Unknown")
        node_context = props.get("node_context", [])

        origins = set()
        for nc in node_context:
            if isinstance(nc, dict):
                origin = nc.get("origin", "")
                if origin:
                    origins.add(origin)

        other_sources = len(origins - {unit_id})

        other_incoming = graph.get_edges_to(entity_id)
        other_outgoing = graph.get_edges_from(entity_id)
        non_doc_edges = len([e for e in other_incoming if e["source"] != unit_id])
        non_doc_edges += len(other_outgoing)

        will_delete = (other_sources == 0 and non_doc_edges == 0)

        impact["entities_affected"].append({
            "id": entity_id,
            "name": name,
            "type": node_type,
            "will_delete": will_delete,
            "other_sources": other_sources,
        })

    try:
        with vector_scope(exclusive=False, timeout=10.0) as vs:
            chunks = vs.collection.get(where={"parent_id": unit_id})
            chunk_count = len(chunks['ids']) if chunks and chunks['ids'] else 0
        impact["vector_entries"] = 1 + chunk_count
    except (TimeoutError, OSError, RuntimeError):
        impact["vector_entries"] = -1  # Unknown

    return impact


def preview_deletion(doc_id: str) -> dict:
    """Visa vad som kommer tas bort utan att göra det."""
    _, paths = _load_paths()
    graph_db_path = _get_graph_db_path(paths)

    lake_file = find_lake_file(doc_id)
    if not lake_file:
        return {"status": "ERROR", "message": f"Dokument ej hittat: {doc_id}"}

    fm = parse_frontmatter(lake_file)
    unit_id = fm.get("unit_id", doc_id)
    original_filename = fm.get("original_filename", "")
    asset_file = find_asset_file(original_filename)

    graph = GraphService(graph_db_path, read_only=True)
    try:
        impact = collect_impact(unit_id, graph)
    finally:
        graph.close()

    return {
        "status": "PREVIEW",
        "unit_id": unit_id,
        "lake_file": os.path.basename(lake_file),
        "original_filename": original_filename,
        "source_type": fm.get("source_type", ""),
        "asset_file": asset_file,
        "asset_action": "move to Rejected" if asset_file else "not found",
        "impact": impact,
    }


def execute_deletion(doc_id: str) -> dict:
    """Utför deletion från alla tre lager."""
    _, paths = _load_paths()
    graph_db_path = _get_graph_db_path(paths)
    rejected_dir = _get_rejected_dir(paths)

    lake_file = find_lake_file(doc_id)
    if not lake_file:
        return {"status": "ERROR", "message": f"Dokument ej hittat: {doc_id}"}

    fm = parse_frontmatter(lake_file)
    unit_id = fm.get("unit_id", doc_id)
    original_filename = fm.get("original_filename", "")
    lake_filename = os.path.basename(lake_file)

    result = {
        "status": "OK",
        "unit_id": unit_id,
        "lake_file": lake_filename,
        "actions": [],
    }

    with resource_lock("graph", exclusive=True):
        with vector_scope(exclusive=True) as vs:
            graph = GraphService(graph_db_path)

            try:
                # 1. Rensa entity node_context
                edges = graph.get_edges_from(unit_id)
                mentions = [e for e in edges if e.get("type") == "MENTIONS"]

                entities_cleaned = 0
                entities_deleted = 0

                for edge in mentions:
                    entity_id = edge["target"]
                    entity = graph.get_node(entity_id)
                    if not entity:
                        continue

                    props = entity.get("properties", {})
                    node_context = props.get("node_context", [])

                    new_context = [
                        nc for nc in node_context
                        if not (isinstance(nc, dict) and nc.get("origin") == unit_id)
                    ]

                    if not new_context:
                        other_incoming = [e for e in graph.get_edges_to(entity_id) if e["source"] != unit_id]
                        other_outgoing = graph.get_edges_from(entity_id)

                        if not other_incoming and not other_outgoing:
                            graph.delete_node(entity_id)
                            vs.delete(entity_id)
                            entities_deleted += 1
                            continue

                    props["node_context"] = new_context
                    graph.update_node_properties(entity_id, props)

                    vs.upsert_node({
                        'id': entity_id,
                        'type': entity.get('type'),
                        'properties': props,
                        'aliases': entity.get('aliases', [])
                    })
                    entities_cleaned += 1

                result["actions"].append(
                    f"Entities: {entities_cleaned} cleaned, {entities_deleted} deleted"
                )

                # 2. Ta bort Document-noden (+ kvarvarande MENTIONS-kanter)
                doc_deleted = graph.delete_node(unit_id)
                result["actions"].append(
                    f"Document node: {'deleted' if doc_deleted else 'not found'}"
                )

                # 3. Ta bort vector-entries
                vs.delete(unit_id)
                chunks_deleted = vs.delete_by_parent(unit_id)
                result["actions"].append(
                    f"Vector: document + {chunks_deleted} chunks deleted"
                )

            finally:
                graph.close()

    # 4. Ta bort Lake-fil
    try:
        os.remove(lake_file)
        result["actions"].append(f"Lake: {lake_filename} deleted")
    except OSError as e:
        LOGGER.error(f"Failed to delete Lake file {lake_filename}: {e}")
        result["actions"].append(f"Lake: failed to delete ({e})")

    # 5. Flytta Asset-fil till Rejected
    asset_file = find_asset_file(original_filename)
    if asset_file:
        os.makedirs(rejected_dir, exist_ok=True)
        dest = os.path.join(rejected_dir, os.path.basename(asset_file))
        try:
            shutil.move(asset_file, dest)
            result["actions"].append("Asset: moved to Rejected/")
        except OSError as e:
            LOGGER.error(f"Failed to move asset {original_filename} to Rejected: {e}")
            result["actions"].append(f"Asset: failed to move ({e})")
    else:
        result["actions"].append("Asset: not found (skipped)")

    LOGGER.info(f"Document deleted: {unit_id} ({lake_filename})")
    return result
