"""Delad helper för att skriva nya dokument till PG documents-tabellen.

Används av både:
- services.agents.service_api (POST /api/v1/ingest från klient-uploader)
- services.agents.mymem_mcp (ingest_content-verktyget från Claude Desktop etc.)

En enda kodbas för insert + NOTIFY så ingestion-workern plockar upp
raden och SSE-klienter får bekräftelse via samma kedja oavsett källa.
"""

from __future__ import annotations

import json
from typing import Literal

import psycopg2

from services.utils.graph_service import _get_pg_dsn


InsertResult = Literal['inserted', 'exists']


def insert_document(
    *,
    uuid: str,
    tenant_id: str,
    source_type: str,
    original_filename: str,
    content: str,
    metadata: dict,
    timestamp_content: str | None = None,
) -> InsertResult:
    """Skapa ny rad i documents-tabellen + trigga document_events NOTIFY.

    Args:
        uuid: Dokument-UUID (klient-genererad, används som PK)
        tenant_id: Ägarens tenant-ID
        source_type: T.ex. 'document', 'mail', 'slack', 'mcp_ingest'
        original_filename: Ursprungligt filnamn
        content: Textinnehåll (markdown eller plain text)
        metadata: JSON-metadata att lagra i documents.metadata
        timestamp_content: ISO-datum eller 'UNKNOWN'/None

    Returns:
        'inserted' om ny rad skapades, 'exists' om UUID redan fanns.
    """
    # timestamp_content kan vara 'UNKNOWN' enligt spec — då NULL i kolumnen
    ts_value = None
    if timestamp_content and timestamp_content != "UNKNOWN":
        ts_value = timestamp_content

    sql_insert = """
        INSERT INTO documents (
            id, tenant_id, source_type, original_filename,
            content, metadata, timestamp_content, processing_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    """

    # NOTIFY skickas INTE för pending — ingestion-workern plockar upp via
    # polling av documents-tabellen (_claim_pending_batch). NOTIFY sker först
    # vid status-övergång till done/failed (se writers.write_lake och
    # ingestion_worker._mark_failed).
    conn = psycopg2.connect(_get_pg_dsn())
    try:
        with conn.cursor() as cur:
            cur.execute(sql_insert, (
                uuid, tenant_id, source_type, original_filename,
                content, json.dumps(metadata), ts_value,
            ))
            row = cur.fetchone()
        conn.commit()
        return 'inserted' if row is not None else 'exists'
    finally:
        conn.close()
