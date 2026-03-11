"""
Enrichment Engine — orchestrator for 2-step enrichment cycle.

Phase 2 of the pipeline: Collect & Normalize -> Ingestion -> ENRICHMENT -> Graph Sweep

Cycle:
  1. ENRICH  (LLM) — enrich properties, edges, Lake metadata, quality_flags
  2. WRITE ENRICH — apply enrichments (graph, Lake, vector, quality_flags)
"""

import json
import logging
import os
from datetime import datetime
from typing import List, Dict

from services.utils.graph_service import GraphService
from services.utils.vector_service import VectorService
from services.utils.lake_service import LakeService
from services.utils.llm_service import LLMService
from services.utils.llm_json import call_llm_json
from services.utils.schema_validator import SchemaValidator
from services.utils.config_loader import get_config
from services.utils.terminal_status import status as terminal_status

from services.engines.enrichment.prompt_builder import build_enrich_prompt
from services.engines.enrichment.writes import execute_enrich_writes, write_quality_flags

LOGGER = logging.getLogger('Enrichment')

# Load config
try:
    _CONFIG = get_config()
except FileNotFoundError:
    LOGGER.warning("Config file not found, using empty config for Enrichment")
    _CONFIG = {}

_SCHEMA_VALIDATOR = None


def _get_schema_validator():
    global _SCHEMA_VALIDATOR
    if _SCHEMA_VALIDATOR is None:
        _SCHEMA_VALIDATOR = SchemaValidator()
    return _SCHEMA_VALIDATOR


def get_schema_validator():
    """Public accessor for schema validator."""
    return _get_schema_validator()


ENRICHMENT_CONFIG = _CONFIG.get('enrichment', {})


class Enrichment:
    """
    Enrichment Engine v4 (OBJEKT-107): 2-step enrichment cycle.

    Step 1: ENRICH (LLM) — enrich properties, edges, Lake metadata + quality_flags
    Step 2: WRITE ENRICH — apply to graph/Lake/vector + write quality_flags

    Structural operations moved to Dreamer.
    """

    def __init__(self, graph_service: GraphService, vector_service: VectorService,
                 config_path: str = "config/services_prompts.yaml"):
        self.graph_service = graph_service
        self.vector_service = vector_service
        self.llm_service = LLMService()
        self.prompts = self._load_prompts(config_path)
        self.schema_validator = _get_schema_validator()
        self.enrichment_config = ENRICHMENT_CONFIG
        self._triage_skipped_nodes: List[str] = []

    def _load_prompts(self, path: str) -> dict:
        try:
            import yaml
            with open(path, "r") as f:
                data = yaml.safe_load(f)
                prompts = data.get("enrichment", {})
                prompts.update(data.get("entity_resolver", {}))
                return prompts
        except Exception as e:
            LOGGER.error(f"Failed to load prompts from {path}: {e}")
            raise

    # =====================================================================
    # DATA GATHERING
    # =====================================================================

    def _get_candidate_nodes(self) -> List[Dict]:
        """Get enrichment candidates: never-enriched + changed neighborhoods."""
        schema = _get_schema_validator().schema
        valid_types = set(schema.get("nodes", {}).keys())
        re_enrich_threshold = ENRICHMENT_CONFIG.get('daemon', {}).get('re_enrich_threshold')
        if re_enrich_threshold is None:
            raise ValueError("Missing required config: enrichment.daemon.re_enrich_threshold")

        rows = self.graph_service.conn.execute("""
            SELECT id, type, properties FROM nodes
            ORDER BY json_extract(properties, '$.created_at') DESC
        """).fetchall()

        candidates = []
        for node_id, node_type, props_raw in rows:
            doc_type = _get_schema_validator().get_document_node_type()
            if node_type == doc_type or node_type not in valid_types:
                continue

            props = json.loads(props_raw) if props_raw else {}
            last_refined = props.get("last_refined_at", "never")

            # Rule 1: never enriched
            if last_refined == "never":
                node = self.graph_service.get_node(node_id)
                if node:
                    candidates.append(node)
                continue

            # Rule 2: count new relation_context entries since last_refined_at
            new_context_count = self._count_new_relation_context(node_id, last_refined)
            if new_context_count >= re_enrich_threshold:
                # Triage: LLM decides if new observations warrant re-enrichment
                new_entries = self._collect_new_relation_context(node_id, last_refined)
                if self._triage_re_enrich(node_id, props, new_entries):
                    node = self.graph_service.get_node(node_id)
                    if node:
                        candidates.append(node)
                        LOGGER.debug(
                            f"Re-enrich candidate: {props.get('name', node_id[:12])} "
                            f"({new_context_count} new context entries since {last_refined})"
                        )
                else:
                    # Triage said NEJ — defer last_refined_at update to write phase (#123)
                    self._triage_skipped_nodes.append(node_id)
                    LOGGER.debug(
                        f"Triage skip: {props.get('name', node_id[:12])} "
                        f"({new_context_count} entries, no significant change)"
                    )

        LOGGER.info(f"Enrichment candidates: {len(candidates)} "
                     f"(threshold: {re_enrich_threshold})")
        return candidates

    def _count_new_relation_context(self, node_id: str, since_timestamp: str) -> int:
        """Count relation_context entries newer than since_timestamp."""
        edges_out = self.graph_service.conn.execute(
            "SELECT properties FROM edges WHERE source = ?", [node_id]
        ).fetchall()
        edges_in = self.graph_service.conn.execute(
            "SELECT properties FROM edges WHERE target = ?", [node_id]
        ).fetchall()

        count = 0
        for (props_raw,) in edges_out + edges_in:
            props = json.loads(props_raw) if props_raw else {}
            relation_context = props.get("relation_context", [])
            for entry in relation_context:
                entry_ts = entry.get("timestamp", "")
                if entry_ts > since_timestamp:
                    count += 1
        return count

    def _collect_new_relation_context(self, node_id: str, since_timestamp: str) -> List[Dict]:
        """Collect new relation_context entries since timestamp for triage."""
        edges_out = self.graph_service.conn.execute(
            "SELECT source, target, edge_type, properties FROM edges WHERE source = ?", [node_id]
        ).fetchall()
        edges_in = self.graph_service.conn.execute(
            "SELECT source, target, edge_type, properties FROM edges WHERE target = ?", [node_id]
        ).fetchall()

        entries = []
        for source, target, edge_type, props_raw in edges_out + edges_in:
            props = json.loads(props_raw) if props_raw else {}
            neighbor_id = target if source == node_id else source
            neighbor = self.graph_service.get_node(neighbor_id)
            neighbor_name = neighbor.get("properties", {}).get("name", neighbor_id[:12]) if neighbor else neighbor_id[:12]

            for entry in props.get("relation_context", []):
                entry_ts = entry.get("timestamp", "")
                if entry_ts > since_timestamp:
                    entries.append({
                        "text": entry.get("text", ""),
                        "timestamp": entry_ts,
                        "origin": entry.get("origin", ""),
                        "edge_type": edge_type,
                        "neighbor_name": neighbor_name,
                    })

        entries.sort(key=lambda e: e["timestamp"], reverse=True)
        return entries

    def _triage_re_enrich(self, node_id: str, props: dict, new_entries: List[Dict]) -> bool:
        """LLM triage: do new observations warrant re-enrichment?"""
        triage_template = self.prompts.get("triage_prompt", "")
        if not triage_template:
            LOGGER.warning("Missing triage_prompt — accepting candidate without triage")
            return True

        context_summary = props.get("context_summary", "(ingen beskrivning)")
        entries_text = "\n".join(
            f"- [{e['edge_type']}] {e['neighbor_name']}: {e['text']}" for e in new_entries[:20]
        )

        prompt = triage_template.format(
            context_summary=context_summary,
            new_entries=entries_text,
        )

        model_key = ENRICHMENT_CONFIG.get('model', 'fast')
        model_id = self.llm_service.models.get(model_key, self.llm_service.models['fast'])
        response = self.llm_service.generate(prompt, model=model_id)

        if not response.success:
            LOGGER.warning(f"Triage LLM failed for {props.get('name', node_id[:12])}: {response.error}")
            return True  # fail-open: accept candidate if triage fails

        answer = response.text.strip().upper()
        return answer.startswith("JA")

    def _get_edges_between(self, entity_ids: set) -> List[Dict]:
        """Get all non-UNIT edges between a set of entities."""
        if not entity_ids:
            return []

        placeholders = ",".join(["?" for _ in entity_ids])
        ids_list = list(entity_ids)

        rows = self.graph_service.conn.execute(f"""
            SELECT source, target, edge_type, properties FROM edges
            WHERE source IN ({placeholders})
              AND target IN ({placeholders})
              AND edge_type NOT IN ('MENTIONS', 'DEALS_WITH')
        """, ids_list + ids_list).fetchall()

        edges = []
        for row in rows:
            props = json.loads(row[3]) if row[3] else {}
            edges.append({
                "source": row[0],
                "target": row[1],
                "type": row[2],
                "properties": props,
            })
        return edges

    def _get_lake_docs_for_entities(self, entity_ids: set) -> List[Dict]:
        """Reverse MENTIONS: find Lake documents that mention any of these entities."""
        if not entity_ids:
            return []

        lake_path = self._get_lake_path()
        if not lake_path:
            return []

        placeholders = ",".join(["?" for _ in entity_ids])
        rows = self.graph_service.conn.execute(f"""
            SELECT DISTINCT source FROM edges
            WHERE target IN ({placeholders}) AND edge_type = 'MENTIONS'
        """, list(entity_ids)).fetchall()

        unit_ids = set(r[0] for r in rows)

        lake_service = LakeService(lake_path)
        docs = []
        for filename in os.listdir(lake_path):
            if not filename.endswith(".md"):
                continue
            filepath = os.path.join(lake_path, filename)
            meta = lake_service.read_metadata(filepath)
            if not meta:
                continue
            uid = meta.get("unit_id", "")
            if uid in unit_ids:
                docs.append({
                    "filepath": filepath,
                    "filename": filename,
                    "unit_id": uid,
                    "timestamp_ingestion": meta.get("timestamp_ingestion", ""),
                    "source_type": meta.get("source_type", ""),
                    "document_keywords": meta.get("document_keywords", []),
                })

        return docs

    # =====================================================================
    # CHUNK CONTEXT (vector grounding for enrichment)
    # =====================================================================

    def _get_chunks_for_entities(self, entity_ids: set) -> List[Dict]:
        """Retrieve original text chunks from VectorDB for enrichment context.

        Per entity: MENTIONS → unit_ids → semantic search with parent_id filter.
        Fallback: collection.get() for unchunked documents (no parent_id).
        One vector_scope for all lookups.

        Returns list of dicts:
            [{chunk_id, document_text, parent_id, filename, source_type,
              part_number, distance, entity_ids}]
        """
        from services.utils.vector_service import vector_scope

        chunk_config = self.enrichment_config.get('chunk_context', {})
        if not chunk_config.get('enabled', False):
            return []

        max_per_entity = chunk_config.get('max_chunks_per_entity', 3)
        max_total = chunk_config.get('max_total_chunks', 10)
        max_chars = chunk_config.get('max_chunk_chars', 1500)
        search_limit = chunk_config.get('search_limit', 5)

        # Gather unit_ids per entity via MENTIONS edges
        entity_unit_ids = {}
        all_unit_ids = set()
        for eid in entity_ids:
            rows = self.graph_service.conn.execute("""
                SELECT DISTINCT source FROM edges
                WHERE target = ? AND edge_type = 'MENTIONS'
            """, [eid]).fetchall()
            uids = [r[0] for r in rows]
            entity_unit_ids[eid] = uids
            all_unit_ids.update(uids)

        if not all_unit_ids:
            return []

        # Get entity names for search queries
        entity_names = {}
        for eid in entity_ids:
            node = self.graph_service.get_node(eid)
            if node:
                props = node.get("properties", {})
                entity_names[eid] = f"{props.get('name', '')} {node.get('type', '')}"

        chunks_by_id = {}  # chunk_id -> chunk dict (deduplication)

        try:
            with vector_scope(exclusive=False, timeout=30.0) as vs:
                for eid in entity_ids:
                    if len(chunks_by_id) >= max_total:
                        break

                    uids = entity_unit_ids.get(eid, [])
                    if not uids:
                        continue

                    query = entity_names.get(eid, "")
                    if not query.strip():
                        continue

                    # Semantic search with parent_id filter (chunked docs)
                    entity_chunks = 0
                    try:
                        results = vs.search(
                            query, limit=search_limit,
                            where={"parent_id": {"$in": uids}}
                        )
                        for r in results:
                            if entity_chunks >= max_per_entity:
                                break
                            if len(chunks_by_id) >= max_total:
                                break

                            cid = r.get("id", "")
                            if cid in chunks_by_id:
                                # Already seen — add this entity to its entity_ids
                                chunks_by_id[cid]["entity_ids"].add(eid)
                                continue

                            doc_text = r.get("document", "")
                            if len(doc_text) > max_chars:
                                doc_text = doc_text[:max_chars]

                            meta = r.get("metadata", {})
                            chunks_by_id[cid] = {
                                "chunk_id": cid,
                                "document_text": doc_text,
                                "parent_id": meta.get("parent_id", ""),
                                "filename": meta.get("filename", ""),
                                "source_type": meta.get("source_type", ""),
                                "part_number": meta.get("part_number", ""),
                                "distance": r.get("distance", 0.0),
                                "entity_ids": {eid},
                            }
                            entity_chunks += 1
                    except Exception as e:
                        LOGGER.warning(f"Chunk semantic search failed for {eid[:12]}: {e}")

                    # Fallback: direct get for unchunked docs
                    if entity_chunks < max_per_entity:
                        try:
                            direct = vs.collection.get(ids=uids, include=["documents", "metadatas"])
                            if direct and direct['ids']:
                                for i, doc_id in enumerate(direct['ids']):
                                    if entity_chunks >= max_per_entity:
                                        break
                                    if len(chunks_by_id) >= max_total:
                                        break
                                    if doc_id in chunks_by_id:
                                        chunks_by_id[doc_id]["entity_ids"].add(eid)
                                        continue

                                    meta = direct['metadatas'][i] if direct['metadatas'] else {}
                                    # Skip if this is a chunked doc's entry (has parent_id)
                                    if meta.get("parent_id"):
                                        continue

                                    doc_text = direct['documents'][i] if direct['documents'] else ""
                                    if len(doc_text) > max_chars:
                                        doc_text = doc_text[:max_chars]

                                    chunks_by_id[doc_id] = {
                                        "chunk_id": doc_id,
                                        "document_text": doc_text,
                                        "parent_id": "",
                                        "filename": meta.get("filename", ""),
                                        "source_type": meta.get("source_type", ""),
                                        "part_number": "",
                                        "distance": 0.0,
                                        "entity_ids": {eid},
                                    }
                                    entity_chunks += 1
                        except Exception as e:
                            LOGGER.warning(f"Chunk fallback get failed for {eid[:12]}: {e}")

        except TimeoutError:
            LOGGER.warning("Chunk retrieval: vector_scope timeout")
            return []
        except Exception as e:
            LOGGER.error(f"Chunk retrieval failed: {e}")
            return []

        # Convert sets to lists for JSON serialization
        result = list(chunks_by_id.values())
        for chunk in result:
            chunk["entity_ids"] = list(chunk["entity_ids"])

        LOGGER.info(f"Chunk retrieval: {len(result)} chunks for {len(entity_ids)} entities")
        return result

    # =====================================================================
    # MAIN CYCLE
    # =====================================================================

    def prepare_cycle(self) -> Dict:
        """Phase 1: Gather candidates and build prompt (graph reads only).

        Returns dict with 'prompt', 'prompt_stats' keys, or {"status": "no_candidates"/"prompt_build_failed"}.
        Reads graph (can use read-only connection) and vector (via _get_chunks_for_entities).
        """
        LOGGER.info("Enrichment cycle: gathering candidates...")
        self._triage_skipped_nodes = []
        all_nodes = self._get_candidate_nodes()

        if not all_nodes:
            LOGGER.info("No candidates for enrichment cycle")
            return {"status": "no_candidates"}

        LOGGER.info(f"Enrichment cycle: {len(all_nodes)} candidate nodes")

        prompt, prompt_stats, selected_entities, edges, neighbor_summaries = build_enrich_prompt(self, all_nodes)

        if not prompt:
            LOGGER.error("Failed to build enrich prompt")
            return {"status": "prompt_build_failed"}

        chunks_str = (f", {prompt_stats.get('chunks', 0)} chunks"
                      if prompt_stats.get('chunks', 0) > 0 else
                      f", {prompt_stats['docs']} docs")
        LOGGER.info(f"Enrich prompt: {prompt_stats['entities']} entities, {prompt_stats['edges']} edges"
                     f"{chunks_str}, {prompt_stats['total_tokens']} tokens "
                     f"({prompt_stats['utilization']:.0%} of budget)")

        return {"status": "ready", "prompt": prompt, "prompt_stats": prompt_stats}

    def call_enrich_llm(self, prompt: str, prompt_stats: Dict) -> Dict:
        """Phase 2: LLM call (no DB access needed).

        Returns dict with 'enrich_result', 'usage', 'prompt_stats' keys,
        or {"status": "enrich_llm_failed", ...}.
        """
        model_key = ENRICHMENT_CONFIG.get('model', 'fast')
        model_id = self.llm_service.models.get(model_key, self.llm_service.models['fast'])
        max_output_tokens = ENRICHMENT_CONFIG.get('max_output_tokens', 32768)

        LOGGER.info(f"LLM call: ENRICH (model: {model_id})...")
        terminal_status("enrichment", "ENRICH LLM call", "processing")
        self.llm_service.reset_token_usage()
        enrich_result = call_llm_json(self.llm_service, prompt, model_id, "enrich", max_output_tokens)

        total_usage = self.llm_service.get_token_usage()
        if not enrich_result:
            LOGGER.error("Enrich LLM call failed — no writes")
            return {"status": "enrich_llm_failed", "usage": total_usage}

        n_nodes = len(enrich_result.get("node_enrichments", []))
        n_edges = len(enrich_result.get("edge_enrichments", []))
        n_lake = len(enrich_result.get("lake_updates", []))
        LOGGER.info(f"Enrich result: {n_nodes} nodes, {n_edges} edges, {n_lake} lake")

        return {
            "status": "ready",
            "enrich_result": enrich_result,
            "usage": total_usage,
            "prompt_stats": prompt_stats,
        }

    def write_cycle_results(self, enrich_result: Dict) -> Dict:
        """Phase 3: Write enrichments to graph/vector/Lake (requires exclusive locks).

        Short duration (~1-3s). Requires self.graph_service (rw) and self.vector_service.
        Returns summary dict.
        """
        LOGGER.info("Writing enrichments...")
        terminal_status("enrichment", "Writing enrichments", "processing")
        enrich_stats = execute_enrich_writes(self, enrich_result)

        # Write deferred triage-skip timestamps (#123)
        if self._triage_skipped_nodes:
            now_iso = datetime.now().isoformat()
            for node_id in self._triage_skipped_nodes:
                node = self.graph_service.get_node(node_id)
                if node:
                    props = node.get('properties', {})
                    props["last_refined_at"] = now_iso
                    self.graph_service.update_node_properties(node_id, props)
            LOGGER.info(f"Triage-skip: updated last_refined_at for {len(self._triage_skipped_nodes)} nodes")
            enrich_stats["triage_skipped"] = len(self._triage_skipped_nodes)
            self._triage_skipped_nodes = []

        flags_written = write_quality_flags(self, enrich_result)
        enrich_stats["quality_flags"] = flags_written

        summary_str = (f"Enrichment cycle complete: "
                        f"{enrich_stats.get('nodes_updated', 0)} enriched, "
                        f"{flags_written} flagged")
        LOGGER.info(summary_str)
        terminal_status("enrichment", "Enrichment cycle", "done", detail=summary_str)

        return enrich_stats

    def run_enrichment_cycle(self, dry_run: bool = False) -> Dict:
        """
        Main 2-step enrichment cycle (OBJEKT-107).

        Convenience wrapper that runs all three phases sequentially.
        Used by tests, dry-run, and rebuild (where caller holds locks).

        Step 1: LLM enrichment (properties, edges, Lake metadata, quality_flags)
        Step 2: Write enrichments + quality_flags to graph/Lake/vector
        """
        # Phase 1: Prepare
        prepare_result = self.prepare_cycle()
        if prepare_result.get("status") != "ready":
            return prepare_result

        prompt = prepare_result["prompt"]
        prompt_stats = prepare_result["prompt_stats"]

        if dry_run:
            return {"status": "dry_run", "prompt_stats": prompt_stats}

        # Phase 2: LLM call
        llm_result = self.call_enrich_llm(prompt, prompt_stats)
        if llm_result.get("status") != "ready":
            return llm_result

        enrich_result = llm_result["enrich_result"]
        total_usage = llm_result["usage"]

        # Phase 3: Write
        enrich_stats = self.write_cycle_results(enrich_result)

        return {
            "status": "completed",
            "enrich": enrich_stats,
            "prompt_stats": prompt_stats,
            "usage": total_usage,
        }

    # =====================================================================
    # HELPER METHODS
    # =====================================================================

    def _get_lake_path(self) -> str:
        """Get Lake path from config."""
        try:
            config = get_config()
            return os.path.expanduser(config['paths']['lake_store'])
        except (KeyError, FileNotFoundError) as e:
            LOGGER.error(f"Could not read config: {e}")
            return ""

    def _find_lake_file(self, lake_path: str, unit_id: str) -> str:
        """Find Lake file based on unit_id."""
        try:
            for filename in os.listdir(lake_path):
                if unit_id in filename and filename.endswith('.md'):
                    return os.path.join(lake_path, filename)
        except OSError as e:
            LOGGER.error(f"Error searching for Lake file: {e}")
        return ""

