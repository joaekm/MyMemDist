"""
DocumentManager — Lista, preview och radera dokument ur alla tre lager.

Används av:
- DocumentManagementView.swift (menubar-app)
- test_ingestion_cycle.py (E2E-test)

Logik:
    1. Graph — rensa orphan-entities (inga kanter till andra Source-noder)
    1b. Graph — purge relation_context entries med origin=doc_id på kvarvarande entiteters kanter
    2. Graph — delete Source-nod + MENTIONS-kanter
    3. Vector — delete dokument + transcript-chunks
    4. Lake — ta bort .md-fil (lokal-mode) ELLER DELETE FROM documents (cloud-mode)
    5. Assets — flytta till Rejected/ (lokal-mode, skippas cloud-mode)

Mode-detektion:
    Om PG har en rad för doc_id i documents-tabellen → cloud-mode (source of
    truth i PG, ingen Lake-fil eller Asset-mapp server-side). Annars faller
    helpern tillbaka på Lake-filer (lokal-mode för main-branchen).
"""

import os
import json
import shutil
import logging
import yaml

import psycopg2

from services.utils.config_loader import get_config
from services.utils.graph_service import graph_scope, GraphService, _get_pg_dsn
from services.utils.vector_service import vector_scope
from services.utils.schema_validator import SchemaValidator

LOGGER = logging.getLogger("DocumentManager")

_SCHEMA_VALIDATOR = None

def _get_schema_validator() -> SchemaValidator:
    global _SCHEMA_VALIDATOR
    if _SCHEMA_VALIDATOR is None:
        _SCHEMA_VALIDATOR = SchemaValidator()
    return _SCHEMA_VALIDATOR


def _load_paths():
    config = get_config()
    paths = config.get('paths', {})
    return config, paths


def _get_tenant_id() -> str | None:
    """Hämta tenant_id från config. Returnerar None om ej satt (t.ex. ren
    klient-installation utan cloud-koppling)."""
    config = get_config()
    return config.get('database', {}).get('tenant_id') or config.get('tenant_id')


def _fetch_pg_document(doc_id: str) -> dict | None:
    """Slå upp dokument i PG documents-tabellen. Returnerar None om ej
    PG-uppkoppling eller raden saknas (då använder caller Lake-fallback)."""
    tenant_id = _get_tenant_id()
    if not tenant_id:
        return None
    try:
        conn = psycopg2.connect(_get_pg_dsn(), connect_timeout=3)
    except psycopg2.Error as e:
        LOGGER.debug(f"PG unavailable for document lookup: {e}")
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, original_filename, source_type, "
                "       timestamp_ingestion, metadata "
                "FROM documents WHERE id = %s AND tenant_id = %s",
                [doc_id, tenant_id],
            )
            row = cur.fetchone()
            if not row:
                return None
            meta = row[4] or {}
            return {
                "unit_id": row[0],
                "original_filename": row[1] or "",
                "source_type": row[2] or "",
                "timestamp_ingestion": (
                    row[3].isoformat() if row[3] else ""
                ),
                "context_summary": meta.get("context_summary", "")[:200] if meta else "",
                "_source": "pg",
            }
    finally:
        conn.close()


def _delete_pg_document(doc_id: str) -> bool:
    """DELETE FROM documents. Returnerar True om rad togs bort."""
    tenant_id = _get_tenant_id()
    if not tenant_id:
        return False
    conn = psycopg2.connect(_get_pg_dsn())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM documents WHERE id = %s AND tenant_id = %s",
                [doc_id, tenant_id],
            )
            deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


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
    rejected = paths.get('asset_failed')
    if not rejected:
        raise RuntimeError("HARDFAIL: 'asset_failed' saknas i config paths")
    return os.path.expanduser(rejected)


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
    """Lista dokument sorterade efter timestamp_ingestion (nyast först).

    Cloud-mode: läser från PG documents-tabellen (enda source of truth).
    Lokal-mode (main-branch): läser Lake-filer på disk som fallback.
    """
    # Försök PG först
    tenant_id = _get_tenant_id()
    if tenant_id:
        try:
            conn = psycopg2.connect(_get_pg_dsn(), connect_timeout=3)
        except psycopg2.Error as e:
            LOGGER.debug(f"PG unavailable, fallback to Lake: {e}")
            conn = None

        if conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, original_filename, source_type, "
                        "       timestamp_ingestion, metadata "
                        "FROM documents "
                        "WHERE tenant_id = %s AND processing_status = 'done' "
                        "ORDER BY timestamp_ingestion DESC NULLS LAST "
                        "LIMIT %s",
                        [tenant_id, limit],
                    )
                    rows = cur.fetchall()
                    return [{
                        "unit_id": r[0],
                        "filename": r[1] or "",
                        "original_filename": r[1] or "",
                        "source_type": r[2] or "",
                        "timestamp_ingestion": (
                            r[3].isoformat() if r[3] else ""
                        ),
                        "context_summary": (
                            (r[4] or {}).get("context_summary", "")[:200]
                        ),
                    } for r in rows]
            finally:
                conn.close()

    # Lokal-mode fallback: Lake-filer
    _, paths = _load_paths()
    lake_store = _get_lake_store(paths)

    if not lake_store or not os.path.exists(lake_store):
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
    source_edges = _get_schema_validator().get_source_edge_types()
    mentions = [e for e in edges if e.get("type") in source_edges]
    impact["mentions_edges"] = len(mentions)

    for edge in mentions:
        entity_id = edge["target"]
        entity = graph.get_node(entity_id)
        if not entity:
            continue

        props = entity.get("properties", {})
        name = props.get("name", entity_id)
        node_type = entity.get("type", "Unknown")

        # Check if entity has edges from other Source-nods (non-orphan detection)
        other_incoming = graph.get_edges_to(entity_id)
        other_outgoing = graph.get_edges_from(entity_id)
        non_doc_edges = len([e for e in other_incoming if e["source"] != unit_id])
        non_doc_edges += len(other_outgoing)

        will_delete = (non_doc_edges == 0)

        impact["entities_affected"].append({
            "id": entity_id,
            "name": name,
            "type": node_type,
            "will_delete": will_delete,
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
    """Visa vad som kommer tas bort utan att göra det.

    Cloud-mode: hämtar metadata från PG documents-tabellen.
    Lokal-mode: fallback till Lake-fil-frontmatter.
    """
    # Försök PG först
    pg_doc = _fetch_pg_document(doc_id)
    if pg_doc:
        unit_id = pg_doc["unit_id"]
        original_filename = pg_doc["original_filename"]
        source_type = pg_doc["source_type"]
        lake_file_name = None
        asset_file = None  # Inga lokala asset-mappar server-side
    else:
        # Lokal fallback
        lake_file = find_lake_file(doc_id)
        if not lake_file:
            return {"status": "ERROR", "message": f"Dokument ej hittat: {doc_id}"}
        fm = parse_frontmatter(lake_file)
        unit_id = fm.get("unit_id", doc_id)
        original_filename = fm.get("original_filename", "")
        source_type = fm.get("source_type", "")
        lake_file_name = os.path.basename(lake_file)
        asset_file = find_asset_file(original_filename)

    with graph_scope(exclusive=False) as graph:
        impact = collect_impact(unit_id, graph)

    return {
        "status": "PREVIEW",
        "unit_id": unit_id,
        "lake_file": lake_file_name,
        "original_filename": original_filename,
        "source_type": source_type,
        "asset_file": asset_file,
        "asset_action": (
            "move to Rejected" if asset_file
            else ("not applicable (cloud-mode)" if pg_doc else "not found")
        ),
        "impact": impact,
    }


def _purge_relation_context(graph: GraphService, entity_id: str, origin_id: str) -> int:
    """Remove relation_context entries with matching origin from all edges on an entity.

    Använder PG-syntax (%s, source_id/target_id, tenant_id) eftersom
    graph_service är PostgreSQL-backad efter cloud-migreringen.

    Returns count of entries removed.
    """
    purged = 0
    all_edges = graph.get_edges_from(entity_id) + graph.get_edges_to(entity_id)

    for edge in all_edges:
        props = edge.get("properties", {})
        rc_list = props.get("relation_context")
        if not rc_list or not isinstance(rc_list, list):
            continue

        filtered = [entry for entry in rc_list if entry.get("origin") != origin_id]
        removed_count = len(rc_list) - len(filtered)
        if removed_count == 0:
            continue

        props["relation_context"] = filtered
        props_json = json.dumps(props, ensure_ascii=False)
        with graph.conn.cursor() as cur:
            cur.execute(
                "UPDATE edges SET properties = %s::jsonb "
                "WHERE source_id = %s AND target_id = %s "
                "  AND edge_type = %s AND tenant_id = %s",
                [props_json, edge["source"], edge["target"],
                 edge["type"], graph.tenant_id],
            )
        graph.conn.commit()
        purged += removed_count

    return purged


def execute_deletion(doc_id: str) -> dict:
    """Utför deletion från alla tre lager (graph, vector, storage).

    Storage-steget skiljer sig per mode:
    - Cloud: DELETE FROM documents (PG är source of truth)
    - Lokal: rm Lake-fil + flytta Asset-fil till Rejected/
    """
    # Försök PG först
    pg_doc = _fetch_pg_document(doc_id)
    is_cloud_mode = pg_doc is not None

    if is_cloud_mode:
        unit_id = pg_doc["unit_id"]
        original_filename = pg_doc["original_filename"]
        lake_filename = None
    else:
        _, paths = _load_paths()
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

    with graph_scope(exclusive=True) as graph:
        with vector_scope(exclusive=True) as vs:
            # 1. Rensa orphan entities
            edges = graph.get_edges_from(unit_id)
            source_edges = _get_schema_validator().get_source_edge_types()
            mentions = [e for e in edges if e.get("type") in source_edges]

            entities_cleaned = 0
            entities_deleted = 0
            rc_purged = 0

            for edge in mentions:
                entity_id = edge["target"]
                entity = graph.get_node(entity_id)
                if not entity:
                    continue

                # Check if entity is orphan (no edges from other sources)
                other_incoming = [e for e in graph.get_edges_to(entity_id) if e["source"] != unit_id]
                other_outgoing = graph.get_edges_from(entity_id)

                if not other_incoming and not other_outgoing:
                    graph.delete_node(entity_id)
                    # Entity-nodens vektor lagras via upsert_node med
                    # doc_id = node_id → delete_by_parent(entity_id)
                    # rensar den.
                    vs.delete_by_parent(entity_id)
                    entities_deleted += 1
                else:
                    # Purge relation_context entries referencing deleted document
                    rc_purged += _purge_relation_context(graph, entity_id, unit_id)
                    entities_cleaned += 1

            result["actions"].append(
                f"Entities: {entities_cleaned} cleaned, {entities_deleted} deleted"
            )
            if rc_purged:
                result["actions"].append(
                    f"Relation context: {rc_purged} entries purged from edges"
                )

            # 2. Ta bort Source-noden (+ kvarvarande MENTIONS-kanter)
            doc_deleted = graph.delete_node(unit_id)
            result["actions"].append(
                f"Document node: {'deleted' if doc_deleted else 'not found'}"
            )

            # 3. Ta bort vector-entries — dokumentets overview (chunk_index=0)
            # och alla text-chunks delar doc_id = unit_id, så en enda
            # delete_by_parent täcker båda.
            chunks_deleted = vs.delete_by_parent(unit_id)
            result["actions"].append(
                f"Vector: {chunks_deleted} rader (overview + chunks) deleted"
            )

    # 4. Storage-cleanup
    if is_cloud_mode:
        # PG är source of truth — DELETE FROM documents
        if _delete_pg_document(unit_id):
            result["actions"].append(f"PG documents: {unit_id} deleted")
        else:
            result["actions"].append(
                f"PG documents: no row found (already gone?)"
            )
    else:
        # Lokal-mode: ta bort Lake-fil
        try:
            os.remove(lake_file)
            result["actions"].append(f"Lake: {lake_filename} deleted")
        except OSError as e:
            LOGGER.error(f"Failed to delete Lake file {lake_filename}: {e}")
            result["actions"].append(f"Lake: failed to delete ({e})")

    # 5. Flytta Asset-fil till Rejected (lokal-mode only — inga Asset-mappar
    # på servern eftersom klienten laddar upp direkt till PG).
    if is_cloud_mode:
        result["actions"].append("Asset: not applicable (cloud-mode)")
    else:
        rejected_dir = _get_rejected_dir(paths)
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

    LOGGER.info(f"Document deleted: {unit_id} ({lake_filename or 'cloud'})")
    return result
