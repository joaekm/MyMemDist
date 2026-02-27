#!/usr/bin/env python3
"""
Enrichment Engine (v4.0) — OBJEKT-107

2-step enrichment cycle for knowledge graph refinement.
Phase 2 of the pipeline: Collect & Normalize -> Ingestion -> ENRICHMENT -> Graph Sweep

Cycle:
  1. ENRICH  (LLM) — enrich properties, edges, Lake metadata, quality_flags
  2. WRITE ENRICH — apply enrichments (graph, Lake, vector, quality_flags)

Structural operations (merge/split/rename/recat) moved to dreamer.py.
"""

import logging
import json
import os
import re
from datetime import datetime
from typing import List, Dict, Optional

from services.utils.graph_service import GraphService
from services.utils.vector_service import VectorService
from services.utils.lake_service import LakeService
from services.utils.llm_service import LLMService
from services.utils.schema_validator import SchemaValidator
from services.utils.metadata_service import generate_semantic_metadata

# Load config for logging
from services.utils.config_loader import get_config
try:
    _CONFIG = get_config()
except FileNotFoundError:
    _CONFIG = {}
LOG_FILE = os.path.expanduser(_CONFIG.get('logging', {}).get('system_log', '~/MyMemory/Logs/my_mem_system.log'))

log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

# Configure root logger: file only, no console
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - ENRICHMENT - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

# Tysta tredjepartsloggers
for _name in ['httpx', 'httpcore', 'google', 'google_genai', 'anyio']:
    logging.getLogger(_name).setLevel(logging.WARNING)

LOGGER = logging.getLogger('Enrichment')

# Terminal status (visual feedback)
from services.utils.terminal_status import status as terminal_status


def _short_id(node_id: str) -> str:
    """Shorten UUID for terminal display: 'a2453abe-...' """
    if node_id and len(node_id) > 12 and '-' in node_id:
        return f"{node_id[:8]}-..."
    return node_id


def _node_name(node: dict) -> str:
    """Get node name from properties."""
    return node.get("properties", {}).get("name", node.get("id", "?"))


_SCHEMA_VALIDATOR = None

def _get_schema_validator():
    """Get cached SchemaValidator instance."""
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

    Structural operations moved to Dreamer (dreamer.py).
    """

    def __init__(self, graph_service: GraphService, vector_service: VectorService,
                 config_path: str = "config/services_prompts.yaml"):
        self.graph_service = graph_service
        self.vector_service = vector_service
        self.llm_service = LLMService()
        self.prompts = self._load_prompts(config_path)

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
            return {}

    # =====================================================================
    # DATA GATHERING
    # =====================================================================

    def _get_candidate_nodes(self, since: Optional[str] = None) -> List[Dict]:
        """Get nodes ordered by created_at DESC, filtered to valid entity types."""
        schema = _get_schema_validator().schema
        valid_types = set(schema.get("nodes", {}).keys())

        if since:
            rows = self.graph_service.conn.execute("""
                SELECT id, type, properties FROM nodes
                WHERE json_extract(properties, '$.created_at') > ?
                ORDER BY json_extract(properties, '$.created_at') DESC
            """, [since]).fetchall()
        else:
            rows = self.graph_service.conn.execute("""
                SELECT id, type, properties FROM nodes
                ORDER BY json_extract(properties, '$.created_at') DESC
            """).fetchall()

        nodes = []
        for r in rows:
            node_type = r[1]
            # Skip Source nodes (document-references) and deprecated types
            if node_type == "Source" or node_type not in valid_types:
                continue
            node = self.graph_service.get_node(r[0])
            if node:
                nodes.append(node)
        return nodes

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

    def _get_neighborhood(self, entity_id: str) -> Dict:
        """Get compact neighborhood: edges out/in, filtered."""
        edges_out = self.graph_service.get_edges_from(entity_id)
        edges_in = self.graph_service.get_edges_to(entity_id)

        edges_out = [e for e in edges_out if e["type"] not in ("MENTIONS", "DEALS_WITH")]
        edges_in = [e for e in edges_in if e["type"] not in ("MENTIONS", "DEALS_WITH")]

        return {"edges_out": edges_out, "edges_in": edges_in}

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
                    "context_summary": meta.get("context_summary", ""),
                    "relations_summary": meta.get("relations_summary", ""),
                    "document_keywords": meta.get("document_keywords", []),
                })

        return docs

    # =====================================================================
    # PROMPT BUILDERS
    # =====================================================================

    def _count_tokens(self, text: str) -> int:
        """Count tokens in text. ~4 chars/token heuristic."""
        return len(text) // 4

    def _build_schema_reference(self, entities: List[Dict], edges: List[Dict]) -> str:
        """Build schema reference with full enum values and descriptions."""
        schema = _get_schema_validator().schema
        system_properties = ENRICHMENT_CONFIG.get('system_properties', [])

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

    def _format_entity(self, ent: Dict, neighbor_summaries: Dict) -> str:
        """Format one entity as prompt text."""
        schema = _get_schema_validator().schema
        props = ent.get("properties", {})
        node_type = ent.get("type", "Unknown")
        name = props.get("name", ent.get("id", "?"))

        section = f"### [{node_type}] {name}\n"
        section += f"- ID: {ent['id']}\n"
        section += f"- Aliases: {ent.get('aliases', [])}\n"

        # Context summary (compact identity)
        ctx_summary = props.get("context_summary", "")
        if ctx_summary:
            section += f"- Identitet: {ctx_summary}\n"
        else:
            section += "- Identitet: SAKNAS\n"

        # Relation context (from edges, shown as summary)
        # Actual relation_context lives on edges — _build_neighbor_summaries handles that

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

    def _format_lake_doc(self, doc: Dict) -> str:
        """Format one Lake document as prompt text."""
        section = f"### {doc['filename']}\n"
        section += f"- unit_id: {doc['unit_id']}\n"
        section += f"- Typ: {doc['source_type']}\n"
        section += f"- Sammanfattning: {doc['context_summary']}\n"
        section += f"- Relationer: {doc['relations_summary']}\n"
        section += f"- Nyckelord: {', '.join(doc.get('document_keywords', []))}\n\n"
        return section

    def _build_neighbor_summaries(self, entity_ids: set, budget: int) -> Dict[str, List[str]]:
        """Build compact neighbor summary strings for entities."""
        neighbor_summaries = {}
        remaining_budget = budget

        for eid in entity_ids:
            hood = self._get_neighborhood(eid)
            summaries = []
            for e in hood["edges_out"]:
                other = self.graph_service.get_node(e["target"])
                if other:
                    name = other.get("properties", {}).get("name", e["target"])
                    summaries.append(f"\u2192[{e['type']}]\u2192 {name} ({other.get('type', '?')})")
                    # Show latest relation_context entries
                    rc = e.get("properties", {}).get("relation_context", [])
                    for entry in rc[-3:]:
                        ts = entry.get("timestamp", "?")
                        text = entry.get("text", "")
                        if text:
                            summaries.append(f"    [{ts}] {text}")
            for e in hood["edges_in"]:
                other = self.graph_service.get_node(e["source"])
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
                n_tokens = self._count_tokens(neighbor_text)
                if n_tokens <= remaining_budget:
                    neighbor_summaries[eid] = summaries
                    remaining_budget -= n_tokens

        return neighbor_summaries

    def _build_enrich_prompt(self, all_nodes: List[Dict], token_budget: Optional[int] = None) -> tuple:
        """Build enrich prompt incrementally with token budget.

        Returns: (prompt_text, stats_dict, selected_entities, edges, neighbor_summaries)
        """
        if token_budget is None:
            token_budget = ENRICHMENT_CONFIG.get('token_budget', 22500)

        entity_ceiling_pct = ENRICHMENT_CONFIG.get('entity_ceiling_pct', 0.60)
        neighbor_budget_pct = ENRICHMENT_CONFIG.get('neighbor_budget_pct', 0.40)

        instruction = self.prompts.get("enrichment_prompt", "")
        if not instruction:
            LOGGER.error("Missing enrichment_prompt in config")
            return "", {}, [], [], {}

        stats = {"entities": 0, "entities_skipped": 0, "edges": 0, "docs": 0,
                 "docs_skipped": 0, "neighbors": 0}

        instruction_tokens = self._count_tokens(instruction)
        tokens_used = instruction_tokens

        # Reserve for schema
        schema_reserve = 2000
        tokens_used += schema_reserve

        # --- Priority 1: Entities ---
        entity_header = "## BERÖRDA ENTITETER\n\n"
        tokens_used += self._count_tokens(entity_header)

        selected_entities = []
        entity_ids = set()
        schema = _get_schema_validator().schema

        entity_ceiling = int(token_budget * entity_ceiling_pct)
        for node in all_nodes:
            ent_text = self._format_entity(node, {})
            ent_tokens = self._count_tokens(ent_text)

            if tokens_used + ent_tokens > entity_ceiling and selected_entities:
                stats["entities_skipped"] += 1
                continue

            tokens_used += ent_tokens
            selected_entities.append(node)
            entity_ids.add(node["id"])
            stats["entities"] += 1

        # --- Priority 2: Edges ---
        edges = self._get_edges_between(entity_ids)
        edge_section = "## RELATIONER MELLAN ENTITETER\n\n"
        if edges:
            for e in edges:
                edge_section += f"- {e['source']} \u2014[{e['type']}]\u2192 {e['target']}\n"
                if e.get("properties"):
                    edge_section += f"  Properties: {json.dumps(e['properties'], ensure_ascii=False)}\n"
        else:
            edge_section += "(Inga relationer mellan dessa entiteter)\n"

        tokens_used += self._count_tokens(edge_section)
        stats["edges"] = len(edges)

        # --- Priority 3: Neighbor summaries ---
        neighbor_budget = int((token_budget - tokens_used) * neighbor_budget_pct)
        neighbor_summaries = self._build_neighbor_summaries(entity_ids, neighbor_budget)
        for eid, refs in neighbor_summaries.items():
            stats["neighbors"] += len(refs)
            tokens_used += self._count_tokens("\n".join(refs))

        # Re-build entity texts with neighbor info
        entity_texts_final = []
        for node in selected_entities:
            entity_texts_final.append(self._format_entity(node, neighbor_summaries))

        # --- Build schema reference ---
        schema_section = self._build_schema_reference(selected_entities, edges)
        actual_schema_tokens = self._count_tokens(schema_section)
        tokens_used = tokens_used - schema_reserve + actual_schema_tokens

        # --- Priority 4: Lake documents ---
        all_docs = self._get_lake_docs_for_entities(entity_ids)
        doc_header = "## NYLIGEN INGESTADE DOKUMENT\n\n"
        doc_texts = []
        doc_budget = token_budget - tokens_used - self._count_tokens(doc_header)

        for doc in all_docs:
            doc_text = self._format_lake_doc(doc)
            doc_tokens = self._count_tokens(doc_text)
            if doc_tokens > doc_budget and doc_texts:
                stats["docs_skipped"] += 1
                continue
            doc_texts.append(doc_text)
            doc_budget -= doc_tokens
            tokens_used += doc_tokens
            stats["docs"] += 1

        if not doc_texts:
            doc_header = ""

        # --- Assemble ---
        prompt = ""
        if doc_texts:
            prompt += doc_header + "".join(doc_texts)
        prompt += entity_header + "".join(entity_texts_final)
        prompt += edge_section
        prompt += schema_section
        prompt += instruction

        actual_tokens = self._count_tokens(prompt)
        stats["total_tokens"] = actual_tokens
        stats["budget"] = token_budget
        stats["utilization"] = actual_tokens / token_budget if token_budget > 0 else 0

        return prompt, stats, selected_entities, edges, neighbor_summaries

    # =====================================================================
    # LLM CALL HELPER
    # =====================================================================

    def _call_llm_json(self, prompt: str, model_id: str, step_name: str) -> Optional[Dict]:
        """Call LLM and return parsed JSON result or None."""
        max_output_tokens = ENRICHMENT_CONFIG.get('max_output_tokens', 32768)

        response = self.llm_service.generate(
            prompt=prompt,
            provider='anthropic',
            model=model_id,
            max_tokens=max_output_tokens
        )

        if not response.success:
            LOGGER.error(f"LLM {step_name} failed: {response.error}")
            return None

        try:
            cleaned = response.text.replace("```json", "").replace("```", "").strip()
            result = json.loads(cleaned)
            return result
        except json.JSONDecodeError as e:
            LOGGER.error(f"LLM {step_name} JSON parse failed: {e}")
            # Try to repair truncated JSON
            try:
                repaired = self._repair_truncated_json(cleaned)
                result = json.loads(repaired)
                LOGGER.warning(f"LLM {step_name}: repaired truncated JSON")
                return result
            except (json.JSONDecodeError, ValueError):
                LOGGER.error(f"LLM {step_name}: could not repair JSON")
                return None

    def _repair_truncated_json(self, text: str) -> str:
        """Try to repair truncated JSON by closing open structures."""
        for i in range(len(text) - 1, 0, -1):
            if text[i] == "}":
                attempt = text[:i+1]
                open_brackets = attempt.count("[") - attempt.count("]")
                open_braces = attempt.count("{") - attempt.count("}")
                suffix = "]" * open_brackets + "}" * open_braces
                try:
                    json.loads(attempt + suffix)
                    return attempt + suffix
                except json.JSONDecodeError:
                    continue
        raise ValueError(f"Could not repair truncated JSON (len={len(text)})")

    # =====================================================================
    # WRITE STEP 3: ENRICH WRITES
    # =====================================================================

    def _execute_enrich_writes(self, enrich_result: Dict) -> Dict:
        """Apply enrichment results to graph, Lake, and vector.

        Returns stats dict with counts of operations performed.
        """
        system_properties = set(ENRICHMENT_CONFIG.get('system_properties', []))
        threshold_enrich = ENRICHMENT_CONFIG.get('thresholds', {}).get('enrich', 0.6)

        stats = {"nodes_updated": 0, "edges_updated": 0, "lake_updated": 0,
                 "nodes_skipped": 0, "edges_skipped": 0}
        enriched_node_ids = set()

        # --- Node enrichments ---
        for ne in enrich_result.get("node_enrichments", []):
            node_id = ne.get("node_id", "")
            confidence = ne.get("confidence", 0)

            if confidence < threshold_enrich:
                LOGGER.info(f"Enrich skip (conf {confidence:.2f} < {threshold_enrich}): {_short_id(node_id)}")
                stats["nodes_skipped"] += 1
                continue

            node = self.graph_service.get_node(node_id)
            if not node:
                LOGGER.warning(f"Enrich skip: node {_short_id(node_id)} not found in graph")
                stats["nodes_skipped"] += 1
                continue

            # Clean system properties from LLM output
            updated_props = {k: v for k, v in ne.get("updated_properties", {}).items()
                             if k not in system_properties}
            new_context = ne.get("new_context", [])
            new_aliases = ne.get("new_aliases", [])

            made_changes = False

            # Update properties
            if updated_props:
                self.graph_service.upsert_node(node_id, node["type"], node.get("aliases"), updated_props)
                made_changes = True

            # Update aliases (LLM returns full list)
            if new_aliases:
                self.graph_service.upsert_node(node_id, node["type"], new_aliases, {})
                made_changes = True

            if made_changes:
                # Set last_refined_at
                now_ts = datetime.now().isoformat()
                current_node = self.graph_service.get_node(node_id)
                if current_node:
                    props = current_node.get("properties", {})
                    props["last_refined_at"] = now_ts
                    self.graph_service.update_node_properties(node_id, props)

                # Vector re-index
                updated_node = self.graph_service.get_node(node_id)
                if updated_node:
                    self.vector_service.upsert_node(updated_node)

                enriched_node_ids.add(node_id)
                stats["nodes_updated"] += 1
                reason = ne.get("reason", "")[:60]
                terminal_status("enrichment", f"ENRICH: [{_short_id(node_id)}] {reason}", "done")

        # --- Edge enrichments ---
        for ee in enrich_result.get("edge_enrichments", []):
            confidence = ee.get("confidence", 0)
            if confidence < threshold_enrich:
                stats["edges_skipped"] += 1
                continue

            source = ee.get("source", "")
            target = ee.get("target", "")
            edge_type = ee.get("edge_type", "")

            # Self-loop guard
            if source == target:
                LOGGER.warning(
                    f"Dropped self-loop edge ({edge_type}): "
                    f"source == target ({source[:12]})"
                )
                continue

            # relation_context is append-only from ingestion — Dreamer must not overwrite
            updated_props = {k: v for k, v in ee.get("updated_properties", {}).items()
                             if k not in system_properties and k != "relation_context"}

            if not updated_props:
                continue

            # Read-merge-write pattern (upsert_edge handles list-merge)
            existing_edge = self.graph_service.get_edge(source, target, edge_type)
            if existing_edge:
                merged_props = {**existing_edge.get("properties", {}), **updated_props}
            else:
                merged_props = updated_props

            # Warn if relation_context is getting large
            existing_rc = existing_edge.get("properties", {}).get("relation_context", []) if existing_edge else []
            if len(existing_rc) > 30:
                LOGGER.warning(f"Large relation_context ({len(existing_rc)} entries) on "
                               f"{source} -[{edge_type}]-> {target}")

            try:
                self.graph_service.upsert_edge(source, target, edge_type, merged_props)
                stats["edges_updated"] += 1
            except ValueError as e:
                LOGGER.warning(f"Edge skipped (schema guard): {e}")

        # --- Lake updates ---
        lake_path = self._get_lake_path()
        if lake_path:
            lake_service = LakeService(lake_path)
            for lu in enrich_result.get("lake_updates", []):
                unit_id = lu.get("unit_id", "")
                filepath = self._find_lake_file(lake_path, unit_id)
                if not filepath:
                    continue

                success = lake_service.update_semantics(
                    filepath,
                    context_summary=lu.get("context_summary"),
                    relations_summary=lu.get("relations_summary"),
                    document_keywords=lu.get("document_keywords"),
                    set_timestamp_updated=True
                )
                if success:
                    stats["lake_updated"] += 1

        LOGGER.info(f"Enrich writes: {stats['nodes_updated']} nodes, {stats['edges_updated']} edges, "
                     f"{stats['lake_updated']} lake ({stats['nodes_skipped']} skipped)")
        return stats

    # =====================================================================
    # WRITE: QUALITY FLAGS (OBJEKT-107)
    # =====================================================================

    def _write_quality_flags(self, enrich_result: Dict) -> int:
        """Write quality_flags from enrichment LLM response to graph nodes.

        Flags are written to nodes.properties.quality_flags as a list of strings.
        Graph Sweep reads these flags to decide on SPLIT/RECATEGORIZE actions.

        Returns count of nodes flagged.
        """
        flags_enabled = ENRICHMENT_CONFIG.get('quality_flags', {}).get('enabled', True)
        if not flags_enabled:
            return 0

        flagged_count = 0
        for ne in enrich_result.get("node_enrichments", []):
            quality_flags = ne.get("quality_flags", [])
            flag_reason = ne.get("flag_reason", {})

            if not quality_flags:
                continue

            node_id = ne.get("node_id", "")
            node = self.graph_service.get_node(node_id)
            if not node:
                continue

            props = node.get("properties", {})
            props["quality_flags"] = quality_flags
            if flag_reason:
                props["quality_flag_reason"] = flag_reason
            self.graph_service.update_node_properties(node_id, props)
            flagged_count += 1
            LOGGER.info(f"Quality flags set on {_short_id(node_id)}: {quality_flags}")

        return flagged_count

    # =====================================================================
    # MAIN CYCLE
    # =====================================================================

    def run_enrichment_cycle(self, dry_run: bool = False) -> Dict:
        """
        Main 2-step enrichment cycle (OBJEKT-107).

        Step 1: LLM enrichment (properties, edges, Lake metadata, quality_flags)
        Step 2: Write enrichments + quality_flags to graph/Lake/vector

        Args:
            dry_run: If True, build prompts but don't call LLM or write.

        Returns:
            Dict with stats from all steps.
        """
        model_key = ENRICHMENT_CONFIG.get('model', 'fast')
        model_id = self.llm_service.models.get(model_key, self.llm_service.models['fast'])

        # --- Data gathering ---
        LOGGER.info("Enrichment cycle: gathering candidates...")
        all_nodes = self._get_candidate_nodes()

        if not all_nodes:
            LOGGER.info("No candidates for enrichment cycle")
            return {"status": "no_candidates"}

        LOGGER.info(f"Enrichment cycle: {len(all_nodes)} candidate nodes")

        # --- Step 1: Build enrich prompt ---
        prompt, prompt_stats, selected_entities, edges, neighbor_summaries = self._build_enrich_prompt(all_nodes)

        if not prompt:
            LOGGER.error("Failed to build enrich prompt")
            return {"status": "prompt_build_failed"}

        LOGGER.info(f"Enrich prompt: {prompt_stats['entities']} entities, {prompt_stats['edges']} edges, "
                     f"{prompt_stats['docs']} docs, {prompt_stats['total_tokens']} tokens "
                     f"({prompt_stats['utilization']:.0%} of budget)")

        if dry_run:
            return {"status": "dry_run", "prompt_stats": prompt_stats}

        # --- Step 1: LLM call ENRICH ---
        LOGGER.info(f"LLM call: ENRICH (model: {model_id})...")
        terminal_status("enrichment", "ENRICH LLM call", "processing")
        self.llm_service.reset_token_usage()
        enrich_result = self._call_llm_json(prompt, model_id, "enrich")

        total_usage = self.llm_service.get_token_usage()
        if not enrich_result:
            LOGGER.error("Enrich LLM call failed — no writes")
            return {"status": "enrich_llm_failed", "usage": total_usage}

        n_nodes = len(enrich_result.get("node_enrichments", []))
        n_edges = len(enrich_result.get("edge_enrichments", []))
        n_lake = len(enrich_result.get("lake_updates", []))
        LOGGER.info(f"Enrich result: {n_nodes} nodes, {n_edges} edges, {n_lake} lake")

        # --- Step 2: Write enrich ---
        LOGGER.info("Step 2: Writing enrichments...")
        terminal_status("enrichment", "Writing enrichments", "processing")
        enrich_stats = self._execute_enrich_writes(enrich_result)

        # --- Step 2b: Write quality_flags (OBJEKT-107) ---
        flags_written = self._write_quality_flags(enrich_result)
        enrich_stats["quality_flags"] = flags_written

        # --- Summary ---
        summary = {
            "status": "completed",
            "enrich": enrich_stats,
            "prompt_stats": prompt_stats,
            "usage": total_usage,
        }

        summary_str = (f"Enrichment cycle complete: "
                        f"{enrich_stats.get('nodes_updated', 0)} enriched, "
                        f"{flags_written} flagged")
        LOGGER.info(summary_str)
        terminal_status("enrichment", "Enrichment cycle", "done", detail=summary_str)

        return summary

    # =====================================================================
    # SHARED METHODS (used by Dreamer via import)
    # =====================================================================

    def _validate_edges_for_recategorize(self, node_id: str, new_type: str) -> tuple:
        """Validate edges for hypothetical type change."""
        edges_out = self.graph_service.get_edges_from(node_id)
        edges_in = self.graph_service.get_edges_to(node_id)
        all_edges = edges_out + edges_in

        if not all_edges:
            return ([], [])

        validator = get_schema_validator()
        valid_edges = []
        invalid_edges = []

        for edge in all_edges:
            source_type = new_type if edge["source"] == node_id else self._get_node_type_for_id(edge["source"])
            target_type = new_type if edge["target"] == node_id else self._get_node_type_for_id(edge["target"])

            nodes_map = {edge["source"]: source_type, edge["target"]: target_type}
            ok, msg = validator.validate_edge(edge, nodes_map)

            if ok:
                valid_edges.append(edge)
            else:
                invalid_edges.append(edge)

        return (valid_edges, invalid_edges)

    def _get_node_type_for_id(self, node_id: str) -> str:
        """Get type for a node from the graph."""
        node = self.graph_service.get_node(node_id)
        if node:
            return node.get("type", "Unknown")
        return "Unknown"

    def propagate_changes(self, unit_ids: List[str]) -> int:
        """Regenerate semantic metadata for Lake files affected by graph changes."""
        if not unit_ids:
            return 0

        lake_path = self._get_lake_path()
        if not lake_path:
            LOGGER.error("Could not find Lake path in config")
            return 0

        lake_service = LakeService(lake_path)
        updated_count = 0

        for unit_id in unit_ids:
            filepath = self._find_lake_file(lake_path, unit_id)
            if not filepath:
                LOGGER.warning(f"Could not find Lake file for unit_id: {unit_id}")
                continue

            try:
                current_meta = lake_service.read_metadata(filepath)
                if not current_meta:
                    continue

                file_content = self._read_file_content(filepath)
                if not file_content:
                    continue

                resolved_entities = self._get_resolved_entities_for_unit(unit_id)

                new_semantics = generate_semantic_metadata(
                    text=file_content,
                    resolved_entities=resolved_entities,
                    current_meta={
                        "context_summary": current_meta.get('context_summary', ''),
                        "relations_summary": current_meta.get('relations_summary', ''),
                        "document_keywords": current_meta.get('document_keywords', [])
                    },
                    filename=os.path.basename(filepath),
                    graph_service=self.graph_service
                )

                if new_semantics.get('ai_model') in ['FAILED', 'SKIPPED']:
                    continue

                success = lake_service.update_semantics(
                    filepath,
                    context_summary=new_semantics.get('context_summary'),
                    relations_summary=new_semantics.get('relations_summary'),
                    document_keywords=new_semantics.get('document_keywords'),
                    set_timestamp_updated=True
                )

                if success:
                    updated_count += 1
                    LOGGER.info(f"Semantic update: {os.path.basename(filepath)}")

            except Exception as e:
                LOGGER.error(f"Error during semantic update of {unit_id}: {e}")

        LOGGER.info(f"Semantic update complete: {updated_count}/{len(unit_ids)} files")
        return updated_count

    def _get_lake_path(self) -> str:
        """Get Lake path from config."""
        try:
            config = get_config()
            return os.path.expanduser(config['paths']['lake_store'])
        except Exception as e:
            LOGGER.error(f"Could not read config: {e}")
            return ""

    def _find_lake_file(self, lake_path: str, unit_id: str) -> str:
        """Find Lake file based on unit_id."""
        try:
            for filename in os.listdir(lake_path):
                if unit_id in filename and filename.endswith('.md'):
                    return os.path.join(lake_path, filename)
        except Exception as e:
            LOGGER.error(f"Error searching for Lake file: {e}")
        return ""

    def _read_file_content(self, filepath: str) -> str:
        """Read content from Lake file (excluding frontmatter)."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            if content.startswith('---'):
                parts = content.split('---', 2)
                if len(parts) >= 3:
                    return parts[2].strip()
            return content
        except Exception as e:
            LOGGER.error(f"Could not read file {filepath}: {e}")
            return ""

    def _get_resolved_entities_for_unit(self, unit_id: str) -> List[Dict]:
        """Get entities that the document MENTIONS, formatted as resolved_entities."""
        try:
            mentions = self.graph_service.get_nodes_mentioning_unit(unit_id)

            if not mentions:
                return []

            resolved_entities = []
            for node in mentions:
                resolved_entities.append({
                    "action": "LINK",
                    "target_uuid": node.get('id', ''),
                    "type": node.get('type', 'Unknown'),
                    "canonical_name": node.get('properties', {}).get('name', '')
                })

            return resolved_entities
        except Exception as e:
            LOGGER.warning(f"Could not get entities for {unit_id}: {e}")
            return []
