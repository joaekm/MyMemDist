"""
Ingestion orchestrator — process_document() and supporting functions.

Main entry point for document processing. Coordinates the full pipeline:
text extraction -> entity extraction -> resolve -> critic -> post-process -> write.
"""

import copy
import json
import logging
import os
from typing import List

from services.processors.text_extractor import extract_text
from services.utils.metadata_service import generate_semantic_metadata
from services.utils.shared_lock import resource_lock
from services.utils.terminal_status import status as terminal_status

from services.engines.ingestion._shared import (
    CONFIG, LAKE_STORE,
    ENRICHMENT_STATE_FILE, ENRICHMENT_STATE_LOCK,
    UUID_SUFFIX_PATTERN, PROCESSED_FILES, PROCESS_LOCK,
    _get_llm_service, _get_schema_validator, LOGGER,
    _LLM_SERVICE,
)
from services.engines.ingestion import _shared
from services.engines.ingestion.mcp_extractor import extract_entities_mcp
from services.engines.ingestion.entity_resolver import resolve_entities
from services.engines.ingestion.critic_filter import critic_filter_resolved
from services.engines.ingestion.source_profile import get_source_profile, apply_source_profile
from services.engines.ingestion.edge_postprocessor import post_process_edges
from services.utils.date_service import get_content_timestamp
from services.engines.ingestion.writers import (
    write_lake, write_graph, write_vector, clean_before_reingest,
)


def _increment_enrichment_node_counter(nodes_added: int):
    """
    Increment the Enrichment daemon node counter (OBJEKT-76).

    This signals to the daemon that new graph nodes have been created,
    allowing threshold-based triggering of Enrichment cycles.
    """
    if nodes_added <= 0:
        return

    with ENRICHMENT_STATE_LOCK:
        try:
            # Load existing state
            state = {'nodes_since_last_run': 0, 'last_run_timestamp': None}
            if os.path.exists(ENRICHMENT_STATE_FILE):
                with open(ENRICHMENT_STATE_FILE, 'r') as f:
                    state = json.load(f)

            # Increment counter
            state['nodes_since_last_run'] = state.get('nodes_since_last_run', 0) + nodes_added

            # Save state
            os.makedirs(os.path.dirname(ENRICHMENT_STATE_FILE), exist_ok=True)
            with open(ENRICHMENT_STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2, default=str)

            LOGGER.debug(f"Enrichment counter: +{nodes_added} -> {state['nodes_since_last_run']} total")

        except (OSError, json.JSONDecodeError) as e:
            LOGGER.error(f"HARDFAIL: Could not update Enrichment state: {e}")
            raise RuntimeError(f"Failed to update Enrichment counter: {e}") from e


def reset_enrichment_counter():
    """
    Reset the Enrichment daemon node counter to zero.

    Called by rebuild orchestrator to prevent daemon from triggering
    during rebuild (since orchestrator runs Enrichment manually after each day).
    """
    with ENRICHMENT_STATE_LOCK:
        try:
            state = {'nodes_since_last_run': 0, 'last_run_timestamp': None}
            if os.path.exists(ENRICHMENT_STATE_FILE):
                with open(ENRICHMENT_STATE_FILE, 'r') as f:
                    state = json.load(f)

            state['nodes_since_last_run'] = 0

            os.makedirs(os.path.dirname(ENRICHMENT_STATE_FILE), exist_ok=True)
            with open(ENRICHMENT_STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2, default=str)

            LOGGER.info("Enrichment counter reset to 0")

        except (OSError, json.JSONDecodeError) as e:
            LOGGER.warning(f"Could not reset Enrichment state: {e}")


def _needs_reingest(filepath: str, lake_file: str) -> bool:
    """Check if Asset file is newer than Lake file (content was updated)."""
    try:
        asset_mtime = os.path.getmtime(filepath)
        lake_mtime = os.path.getmtime(lake_file)
        return asset_mtime > lake_mtime
    except OSError as e:
        LOGGER.warning(f"Could not compare timestamps for {filepath}: {e}")
        return False


def process_document(filepath: str, filename: str, _lock_held: bool = False):
    """
    Main document processing function.
    Orchestrates the full ingestion pipeline.

    Args:
        filepath: Full path to source file
        filename: Filename (used for UUID extraction)
        _lock_held: If True, caller already holds resource locks (e.g., rebuild).
                    If False, this function acquires locks per document.
    """
    with PROCESS_LOCK:
        if filename in PROCESSED_FILES:
            return
        PROCESSED_FILES.add(filename)

    match = UUID_SUFFIX_PATTERN.search(filename)
    if not match:
        return
    unit_id = match.group(1)
    base_name = os.path.splitext(filename)[0]
    lake_file = os.path.join(LAKE_STORE, f"{base_name}.md")

    is_reingest = False
    if os.path.exists(lake_file):
        if _needs_reingest(filepath, lake_file):
            is_reingest = True
            LOGGER.info(f"Re-ingest triggered: {filename} (Asset newer than Lake)")
        else:
            LOGGER.debug(f"Skippar {filename} - redan i Lake")
            return  # Idempotent

    LOGGER.info(f"{'Re-processing' if is_reingest else 'Processing'}: {filename}")
    terminal_status("ingestion", filename, "processing")

    def _prepare(vector_service=None):
        """Phase 1: Extract text, entities, resolve — no exclusive locks needed.

        Returns all data needed for writing, or None if document should be skipped.
        For _lock_held (rebuild) scenario, also handles re-ingest cleanup.
        """
        # Reset token counter for per-document tracking
        llm_svc = _shared._LLM_SERVICE
        if llm_svc:
            llm_svc.reset_token_usage()

        # 0. Clean stale data before re-ingestion (needs locks if held by caller)
        if is_reingest and _lock_held:
            terminal_status("ingestion", filename, "re-ingest cleanup")
            clean_before_reingest(unit_id, filename, lake_file, vector_service=vector_service)

        # 1. Extract text (via text_extractor) — no DB dependency
        raw_text = extract_text(filepath)

        if not raw_text or len(raw_text) < 10:
            LOGGER.debug(f"File {filename} appears incomplete ({len(raw_text) if raw_text else 0} chars). Waiting for on_modified.")
            with PROCESS_LOCK:
                PROCESSED_FILES.discard(filename)
            return None

        # 2. Determine source type (schema-driven via processing_policy.source_mappings)
        validator = _get_schema_validator()
        source_mappings = validator.get_source_type_mappings()
        source_type = validator.get_default_source_type()
        fp_lower = filepath.lower()
        for keyword, profile_name in source_mappings.items():
            if keyword in fp_lower:
                source_type = profile_name
                break

        # 2b. Load source type extraction profile from schema
        profile = get_source_profile(source_type)
        LOGGER.info(f"Source profile: {source_type} — create={profile['allow_create']}, edges={profile['allow_edges']}, skip_critic={profile['skip_critic']}")

        # 3. Extract entities via MCP — LLM call, no DB dependency
        entity_data = extract_entities_mcp(raw_text, source_hint=source_type)
        nodes = entity_data.get('nodes', [])
        edges = entity_data.get('edges', [])

        # 4. Resolve ALLA noder mot graf FÖRST (edges behöver alla för UUID-lookup)
        # Uses graph reads — safe without exclusive lock (idempotent LINK/CREATE)
        ingestion_payload = resolve_entities(nodes, edges, source_type, filename)

        # 4b. Apply source type profile (filter CREATE/edges based on profile)
        ingestion_payload = apply_source_profile(ingestion_payload, profile)

        # 5. Critic EFTER resolve (bara CREATE, LINK auto-godkänd)
        if profile.get("skip_critic"):
            # Profile says skip critic — all surviving entries are accepted
            link_count = len([m for m in ingestion_payload if m.get("action") == "LINK"])
            create_count = len([m for m in ingestion_payload if m.get("action") == "CREATE"])
            _shared._last_critic_approved = [
                {"name": m["label"], "type": m["type"], "reason": f"{m.get('action')} (critic skipped by profile)"}
                for m in ingestion_payload if m.get("action") in ("LINK", "CREATE")
            ]
            _shared._last_critic_rejected = []
            LOGGER.info(f"Critic: skipped by profile ({link_count} LINK, {create_count} CREATE)")
        else:
            ingestion_payload = critic_filter_resolved(ingestion_payload)

        # 5b. Post-processing: deterministiska edge properties
        ingestion_payload = post_process_edges(ingestion_payload, source_type, raw_text)

        # Expose payload for test write-through verification
        _shared._last_ingestion_payload = copy.deepcopy(ingestion_payload)
        _shared._last_raw_text = raw_text

        # 6. Generate semantic metadata MED graf-berikning (via metadata_service)
        semantic_metadata = generate_semantic_metadata(
            text=raw_text,
            resolved_entities=ingestion_payload,
            current_meta=None,  # Nygenering
            filename=filename
        )

        _shared._last_semantic_metadata = copy.deepcopy(semantic_metadata) if semantic_metadata else {}

        # 7. Timestamps — create once, use everywhere
        timestamp_ingestion, timestamp_content = get_content_timestamp(raw_text, filename)

        return {
            "raw_text": raw_text,
            "source_type": source_type,
            "profile": profile,
            "ingestion_payload": ingestion_payload,
            "semantic_metadata": semantic_metadata,
            "timestamp_ingestion": timestamp_ingestion,
            "timestamp_content": timestamp_content,
        }

    def _write(prepared, vector_service=None):
        """Phase 2: Write to Graph, Vector, Lake — requires exclusive locks.

        Short duration (~2-5s), only DB writes.
        """
        raw_text = prepared["raw_text"]
        source_type = prepared["source_type"]
        ingestion_payload = prepared["ingestion_payload"]
        semantic_metadata = prepared["semantic_metadata"]
        timestamp_ingestion = prepared["timestamp_ingestion"]
        timestamp_content = prepared["timestamp_content"]

        # 0. Clean stale data before re-ingestion (realtime — needs exclusive lock)
        if is_reingest and not _lock_held:
            terminal_status("ingestion", filename, "re-ingest cleanup")
            clean_before_reingest(unit_id, filename, lake_file, vector_service=vector_service)

        # 8. Write to Graph
        nodes_written, edges_written = write_graph(
            unit_id, filename, ingestion_payload,
            source_type=source_type,
            timestamp_content=timestamp_content,
            vector_service=vector_service
        )

        # 8b. Update Enrichment daemon counter (OBJEKT-76)
        _increment_enrichment_node_counter(nodes_written)

        # 9. Write to Vector
        write_vector(unit_id, filename, raw_text, source_type, semantic_metadata, timestamp_ingestion, vector_service=vector_service)

        # 10. Write to Lake (SIST - fungerar som "commit" att allt lyckades)
        write_lake(unit_id, filename, raw_text, source_type, semantic_metadata, ingestion_payload,
                   timestamp_ingestion=timestamp_ingestion, timestamp_content=timestamp_content)

        # Done - log token usage and terminal
        action = "Re-ingested" if is_reingest else "Completed"
        token_usage = _get_llm_service().get_token_usage() if _shared._LLM_SERVICE else {}
        if token_usage:
            usage_parts = []
            for m, u in token_usage.items():
                usage_parts.append(f"{m}: {u['input_tokens']}in/{u['output_tokens']}out ({u['calls']} calls)")
            LOGGER.info(f"Token usage for {filename}: {', '.join(usage_parts)}")
        LOGGER.info(f"{action}: {filename}")
        detail = f"{nodes_written} noder, {edges_written} relationer"
        if is_reingest:
            detail = f"re-ingest: {detail}"
        terminal_status("ingestion", filename, "done", detail=detail)

    try:
        if _lock_held:
            # Caller holds locks (rebuild scenario) — run both phases inline
            prepared = _prepare()
            if prepared:
                _write(prepared)
        else:
            # Realtime scenario: prepare without exclusive locks, then write briefly
            from services.utils.vector_service import vector_scope
            prepared = _prepare()
            if prepared:
                with resource_lock("graph", exclusive=True):
                    with vector_scope(exclusive=True) as vs:
                        _write(prepared, vector_service=vs)

    except Exception as e:
        LOGGER.error(f"HARDFAIL {filename}: {e}")
        terminal_status("ingestion", filename, "failed", str(e))
        with PROCESS_LOCK:
            PROCESSED_FILES.discard(filename)
        raise RuntimeError(f"HARDFAIL: Document processing failed for {filename}: {e}") from e


class DocumentHandler:
    """Watchdog event handler for new and modified documents."""

    def on_created(self, event):
        if event.is_directory:
            return
        process_document(event.src_path, os.path.basename(event.src_path))

    def on_modified(self, event):
        if event.is_directory:
            return
        fname = os.path.basename(event.src_path)
        if not UUID_SUFFIX_PATTERN.search(fname):
            return
        with PROCESS_LOCK:
            PROCESSED_FILES.discard(fname)
        process_document(event.src_path, fname)
