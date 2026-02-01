#!/usr/bin/env python3
"""
Dreamer Engine (v2.0)

Batch refinement of the knowledge graph.
Phase 3 of the pipeline: Collect & Normalize -> Ingestion -> DREAMING

Responsibilities:
- Scan candidates for refinement (80/20 strategy)
- Structural analysis (SPLIT, RENAME, DELETE, RE-CATEGORIZE)
- Entity resolution (MERGE duplicates)
- Propagate changes back to Lake/Vector
"""

import logging
import json
import os
import re
import yaml
from typing import List, Dict, Any

from services.utils.graph_service import GraphService
from services.utils.vector_service import VectorService
from services.utils.lake_service import LakeService
from services.utils.llm_service import LLMService
from services.utils.schema_validator import SchemaValidator
from services.utils.metadata_service import generate_semantic_metadata

# Load config for logging
def _load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'my_mem_config.yaml')
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

_CONFIG = _load_config()
LOG_FILE = os.path.expanduser(_CONFIG.get('logging', {}).get('system_log', '~/MyMemory/Logs/my_mem_system.log'))

log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

# Configure root logger: file only, no console
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - DREAMER - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

# Tysta tredjepartsloggers
for _name in ['httpx', 'httpcore', 'google', 'google_genai', 'anyio']:
    logging.getLogger(_name).setLevel(logging.WARNING)

LOGGER = logging.getLogger('Dreamer')

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
    """Public accessor for schema validator (used in _prepare_node_for_llm)."""
    return _get_schema_validator()


DREAMER_CONFIG = _CONFIG.get('dreamer', {})


class Dreamer:
    """
    Dreamer Engine: Responsible for identity resolution and graph maintenance.
    Uses VectorService for semantic duplicate detection and LLM for evaluation.
    """

    def __init__(self, graph_service: GraphService, vector_service: VectorService,
                 config_path: str = "config/services_prompts.yaml"):
        self.graph_service = graph_service
        self.vector_service = vector_service
        self.llm_service = LLMService()
        self.prompts = self._load_prompts(config_path)

    def _load_prompts(self, path: str) -> dict:
        try:
            with open(path, "r") as f:
                data = yaml.safe_load(f)
                # Merge dreamer and entity_resolver sections
                prompts = data.get("dreamer", {})
                prompts.update(data.get("entity_resolver", {}))
                return prompts
        except Exception as e:
            LOGGER.error(f"Failed to load prompts from {path}: {e}")
            return {}

    def _get_node_type_description(self, node_type: str) -> str:
        """Get schema description for a node type."""
        schema = get_schema_validator().schema
        node_def = schema.get("nodes", {}).get(node_type, {})
        description = node_def.get("description", "")
        if not description:
            return f"(Ingen beskrivning tillgänglig för '{node_type}')"
        return description

    def _validate_edges_for_recategorize(self, node_id: str, new_type: str) -> tuple:
        """
        Validate edges for hypothetical type change.

        Returns:
            (valid_edges: list of edge dicts, invalid_edges: list of edge dicts)
        """
        edges_out = self.graph_service.get_edges_from(node_id)
        edges_in = self.graph_service.get_edges_to(node_id)
        all_edges = edges_out + edges_in

        if not all_edges:
            return ([], [])

        validator = get_schema_validator()
        valid_edges = []
        invalid_edges = []

        for edge in all_edges:
            # Build nodes_map with the NEW type for this node
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

    def scan_candidates(self) -> List[Dict]:
        """
        Get candidates for refinement using 80/20 strategy.
        Delegates logic to GraphService to capture both 'Heat' (Relevance) and 'Deep Sleep' (Maintenance).
        """
        candidate_limit = DREAMER_CONFIG.get('candidate_limit', 50)
        candidates = self.graph_service.get_refinement_candidates(limit=candidate_limit)

        if candidates:
            LOGGER.info(f"Dreamer selected {len(candidates)} candidates via Relevance/Maintenance strategy.")

        return candidates

    def ensure_node_indexed(self, node: Dict):
        """Ensure node exists in vector index before searching."""
        self.vector_service.upsert_node(node)

    def find_potential_matches(self, node: Dict) -> List[Dict]:
        """Find potential duplicates for a given node using SEMANTIC SEARCH."""
        self.ensure_node_indexed(node)

        name = node.get("properties", {}).get("name", "")
        if not name:
            return []

        # Build search string (Name + Type + Context)
        search_text = f"{name} {node.get('type')}"
        node_context = node.get("properties", {}).get("node_context", [])
        if node_context and isinstance(node_context, list):
            ctx_texts = [c.get('text', '') for c in node_context if isinstance(c, dict)]
            search_text += " " + " ".join(ctx_texts)

        # Semantic search
        vector_limit = DREAMER_CONFIG.get('vector_search_limit', 10)
        results = self.vector_service.search(search_text, limit=vector_limit)

        valid_matches = []
        for res in results:
            match_id = res['id']
            if match_id == node["id"]:
                continue

            match_node = self.graph_service.get_node(match_id)
            if not match_node:
                continue

            if match_node["type"] != node["type"]:
                continue

            valid_matches.append(match_node)

        return valid_matches

    def _prepare_node_for_llm(self, node: Dict) -> Dict:
        """Clean node from technical metadata before sending to LLM."""
        if not node:
            return {}

        clean_node = {
            "type": node.get("type"),
            "aliases": node.get("aliases", []),
            "properties": node.get("properties", {}).copy()
        }

        # Remove system properties
        props = clean_node["properties"]
        schema = get_schema_validator().schema
        base_props = schema.get("base_properties", {}).get("properties", {})
        for key, key_def in base_props.items():
            if not key_def.get("include_in_vector", True):
                props.pop(key, None)

        return clean_node

    def _build_merge_prompt(self, primary: Dict, secondary: Dict) -> str:
        """Build prompt for merge evaluation."""
        prompt_template = self.prompts.get("entity_resolution_prompt", "")
        if not prompt_template:
            LOGGER.error("Missing entity_resolution_prompt")
            return ""

        p_clean = self._prepare_node_for_llm(primary)
        s_clean = self._prepare_node_for_llm(secondary)

        return prompt_template.format(
            node_a_json=json.dumps(p_clean, indent=2, ensure_ascii=False),
            node_b_json=json.dumps(s_clean, indent=2, ensure_ascii=False)
        )

    def _parse_merge_response(self, response_text: str) -> Dict:
        """Parse LLM response for merge evaluation."""
        try:
            cleaned_text = response_text.replace("```json", "").replace("```", "").strip()
            result = json.loads(cleaned_text)

            if isinstance(result, list):
                if result:
                    result = result[0]
                else:
                    return {"decision": "IGNORE", "confidence": 0.0, "reason": "Empty list from LLM"}

            if not isinstance(result, dict):
                return {"decision": "IGNORE", "confidence": 0.0, "reason": "Invalid format from LLM"}

            return result
        except Exception as e:
            LOGGER.error(f"LLM Evaluation parse failed: {e}")
            return {"decision": "IGNORE", "confidence": 0.0, "reason": "LLM Parse Error"}

    def batch_evaluate_merges(self, pairs: List[tuple]) -> List[Dict]:
        """
        Evaluate multiple merge candidates in parallel using batch_generate.

        Args:
            pairs: List of (primary_node, secondary_node) tuples

        Returns:
            List of merge evaluation results in same order as input pairs
        """
        if not pairs:
            return []

        prompts = []
        valid_indices = []

        for i, (primary, secondary) in enumerate(pairs):
            prompt = self._build_merge_prompt(primary, secondary)
            if prompt:
                prompts.append(prompt)
                valid_indices.append(i)

        if not prompts:
            LOGGER.warning("No valid merge prompts could be built")
            return [{"decision": "IGNORE", "confidence": 0.0, "reason": "No prompt"} for _ in pairs]

        LOGGER.info(f"Running batch merge evaluation for {len(prompts)} pairs...")

        # Anthropic Sonnet för entity resolution (OBJEKT-85)
        responses = self.llm_service.batch_generate(prompts, provider='anthropic', model=self.llm_service.models['fast'])

        # Build results list maintaining original order
        results = [{"decision": "IGNORE", "confidence": 0.0, "reason": "No prompt"} for _ in pairs]

        for idx, response in zip(valid_indices, responses):
            if not response.success:
                LOGGER.error(f"Merge evaluation LLM failed: {response.error}")
                results[idx] = {"decision": "IGNORE", "confidence": 0.0, "reason": f"LLM error: {response.error}"}
            else:
                results[idx] = self._parse_merge_response(response.text)

        return results

    def evaluate_merge(self, primary: Dict, secondary: Dict) -> Dict:
        """Ask LLM: Are these the same entity? Single-pair version for backwards compatibility."""
        prompt = self._build_merge_prompt(primary, secondary)

        if not prompt:
            return {"decision": "IGNORE", "confidence": 0.0}

        # Anthropic Sonnet för entity resolution (OBJEKT-85)
        response = self.llm_service.generate(prompt, provider='anthropic', model=self.llm_service.models['fast'])
        if not response.success:
            LOGGER.error(f"LLM Evaluation failed: {response.error}")
            return {"decision": "IGNORE", "confidence": 0.0, "reason": "LLM Error"}

        return self._parse_merge_response(response.text)

    def prune_context(self, node_id: str):
        """Condense node_context for a node if list is too long."""
        node = self.graph_service.get_node(node_id)
        if not node:
            return

        node_context = node.get('properties', {}).get('node_context', [])
        if not isinstance(node_context, list) or len(node_context) < 15:
            return

        LOGGER.info(f"Pruning node_context for {node_id} ({len(node_context)} entries)...")

        ctx_texts = [c.get('text', '') for c in node_context if isinstance(c, dict)]

        prompt_template = self.prompts.get("context_pruning_prompt", "")
        if not prompt_template:
            LOGGER.warning("Missing context_pruning_prompt")
            return

        prompt = prompt_template.format(keywords=json.dumps(ctx_texts, ensure_ascii=False))

        # Anthropic Sonnet för context pruning (OBJEKT-85)
        response = self.llm_service.generate(prompt, provider='anthropic', model=self.llm_service.models['fast'])
        if not response.success:
            LOGGER.error(f"Context pruning LLM failed: {response.error}")
            return

        try:
            cleaned_text = response.text.replace("```json", "").replace("```", "").strip()

            try:
                result = json.loads(cleaned_text)
            except json.JSONDecodeError:
                match = re.search(r'\{.*\}', cleaned_text, re.DOTALL)
                if match:
                    result = json.loads(match.group(0))
                else:
                    raise

            if isinstance(result, dict) and "pruned_keywords" in result:
                pruned_texts = set(result["pruned_keywords"])
                new_context = [c for c in node_context if c.get('text') in pruned_texts]

                props = node.get('properties', {})
                props['node_context'] = new_context
                self.graph_service.upsert_node(node['id'], node['type'], node.get('aliases'), props)
                LOGGER.info(f"Pruned to {len(new_context)} context entries.")

        except Exception as e:
            LOGGER.error(f"Context pruning parse failed: {e}")

    def _is_weak_name(self, name: str) -> bool:
        """Identify UUIDs or generic placeholders."""
        patterns = [
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            r'^Talare \d+$',
            r'^Speaker \d+$',
            r'^Unknown$',
            r'^Unit_.*$'
        ]
        return any(re.match(p, name, re.I) for p in patterns)

    def run_resolution_cycle(self, dry_run: bool = False) -> Dict[str, int]:
        """
        Main loop for cognitive maintenance with causal updates.

        Uses batch LLM calls for parallel processing:
        - Phase 1: Batch structural analysis for all candidates
        - Phase 2: Batch merge evaluation for all candidate-match pairs
        """
        candidates = self.scan_candidates()
        stats = {"merged": 0, "split": 0, "renamed": 0, "recat": 0, "deleted": 0}
        affected_units = set()

        if not candidates:
            LOGGER.info("No candidates for resolution cycle")
            return stats

        thresholds = DREAMER_CONFIG.get('thresholds', {})
        THRESHOLD_DELETE = thresholds.get('delete', 0.95)
        THRESHOLD_SPLIT = thresholds.get('split', 0.90)
        THRESHOLD_RENAME_NORMAL = thresholds.get('rename_normal', 0.95)
        THRESHOLD_RENAME_WEAK = thresholds.get('rename_weak', 0.70)
        THRESHOLD_RECATEGORIZE = thresholds.get('recategorize', 0.90)
        THRESHOLD_MERGE = thresholds.get('merge', 0.90)

        # === PHASE 1: Batch Structural Analysis ===
        LOGGER.info(f"Phase 1: Structural analysis for {len(candidates)} candidates...")
        structural_results = self.batch_structural_analysis(candidates)

        # Track which nodes to skip in merge phase (deleted/split)
        skip_merge_ids = set()
        LOGGER.info(f"Phase 1: Applying structural actions to {len(candidates)} candidates...")

        for i, node in enumerate(candidates):
            node_id = node.get("id")
            node_type = node.get("type", "Unknown")
            analysis = structural_results.get(node_id, {"action": "KEEP", "confidence": 0.0})
            action = analysis.get("action", "KEEP")
            conf = analysis.get("confidence", 0.0)
            reason = analysis.get("reason", "")

            # Utökad logg för RE-CATEGORIZE
            if action == "RE-CATEGORIZE":
                new_type = analysis.get("new_type", "?")
                LOGGER.info(f"  [{i+1}/{len(candidates)}] {node_id}: {node_type} -> {new_type} ({reason})")
            else:
                LOGGER.info(f"  [{i+1}/{len(candidates)}] {node_id}: action={action}")

            # --- HEURISTIC GUARDS ---
            if action == "DELETE":
                if self.graph_service.get_node_degree(node_id) > 0:
                    action = "KEEP"
                elif conf < THRESHOLD_DELETE:
                    action = "KEEP"

            elif action == "RENAME":
                is_weak = self._is_weak_name(node_id)
                target_threshold = THRESHOLD_RENAME_WEAK if is_weak else THRESHOLD_RENAME_NORMAL
                if conf < target_threshold:
                    action = "KEEP"

            # --- EXECUTION ---
            if action == "DELETE" and not dry_run:
                node_name = _node_name(node)
                self.graph_service.delete_node(node_id)
                self.vector_service.delete(node_id)
                stats["deleted"] += 1
                skip_merge_ids.add(node_id)
                terminal_status("dreamer", f"DELETE: [{_short_id(node_id)}] '{node_name}'", "done")

            elif action == "RENAME" and not dry_run:
                old_name = _node_name(node)
                new_name = analysis.get("new_name")
                units = self.graph_service.get_related_unit_ids(node_id)
                self.graph_service.rename_node(node_id, new_name)
                affected_units.update(units)
                stats["renamed"] += 1
                terminal_status("dreamer", f"RENAME: [{_short_id(node_id)}] '{old_name}' → '{new_name}'", "done")
                # Update node reference for merge phase
                node["id"] = new_name
                node.update(self.graph_service.get_node(new_name) or {})

            elif action == "RE-CATEGORIZE" and not dry_run:
                if conf >= THRESHOLD_RECATEGORIZE:
                    new_type = analysis.get("new_type")
                    valid_edges, invalid_edges = self._validate_edges_for_recategorize(node_id, new_type)

                    # Remove invalid edges first
                    for edge in invalid_edges:
                        self.graph_service.delete_edge(edge["source"], edge["target"], edge["type"])

                    self.graph_service.recategorize_node(node_id, new_type)
                    affected_units.update(self.graph_service.get_related_unit_ids(node_id))
                    stats["recat"] += 1
                    terminal_status("dreamer", f"RECAT: [{_short_id(node_id)}] '{_node_name(node)}' → {new_type}", "done")

                    if invalid_edges:
                        LOGGER.info(
                            f"RE-CATEGORIZE {node_id} -> {new_type}: "
                            f"removed {len(invalid_edges)} invalid edges, kept {len(valid_edges)}"
                        )

            elif action == "SPLIT" and not dry_run:
                if conf >= THRESHOLD_SPLIT:
                    units = self.graph_service.get_related_unit_ids(node_id)
                    clusters = analysis.get("split_clusters", [])
                    new_names = [c.get("name", "?") for c in clusters]
                    self.graph_service.split_node(node_id, clusters)
                    affected_units.update(units)
                    stats["split"] += 1
                    skip_merge_ids.add(node_id)
                    terminal_status("dreamer", f"SPLIT: [{_short_id(node_id)}] '{_node_name(node)}' → {new_names}", "done")

        # === PHASE 2: Batch Merge Evaluation ===
        # Collect all (candidate, match) pairs first
        candidates_for_merge = len(candidates) - len(skip_merge_ids)
        LOGGER.info(f"Phase 2: Collecting merge candidates from {candidates_for_merge} nodes...")
        merge_pairs = []
        pair_metadata = []  # Track (node, match) for each pair

        for i, node in enumerate(candidates):
            node_id = node.get("id")
            if node_id in skip_merge_ids:
                continue

            if (i + 1) % 10 == 0:
                LOGGER.info(f"  Finding matches for node {i + 1}/{len(candidates)}...")

            matches = self.find_potential_matches(node)
            for match in matches:
                merge_pairs.append((match, node))
                pair_metadata.append({"node": node, "match": match})

        LOGGER.info(f"Phase 2: Found {len(merge_pairs)} potential merge pairs")
        if merge_pairs:
            LOGGER.info(f"Phase 2: Merge evaluation for {len(merge_pairs)} pairs...")
            merge_results = self.batch_evaluate_merges(merge_pairs)

            # Track already-merged nodes to avoid double merges
            merged_nodes = set()

            for meta, merge_eval in zip(pair_metadata, merge_results):
                node = meta["node"]
                match = meta["match"]
                node_id = node.get("id")

                if node_id in merged_nodes:
                    continue

                if merge_eval.get("decision") == "MERGE" and merge_eval.get("confidence", 0) >= THRESHOLD_MERGE:
                    if not dry_run:
                        source_name = _node_name(node)
                        target_name = _node_name(match)
                        units = self.graph_service.get_related_unit_ids(node_id)
                        self.graph_service.merge_nodes(match["id"], node_id)
                        affected_units.update(units)
                        self.prune_context(match["id"])
                        terminal_status("dreamer", f"MERGE: [{_short_id(node_id)}] '{source_name}' → '{target_name}'", "done")
                    stats["merged"] += 1
                    merged_nodes.add(node_id)

        # === PHASE 3: Causal Semantic Update ===
        if affected_units and not dry_run:
            LOGGER.info(f"Phase 3: Semantic update for {len(affected_units)} files...")
            self.propagate_changes(list(affected_units))

        return stats

    def _build_structural_prompt(self, node: Dict) -> str:
        """Build prompt for structural analysis. Returns empty string if node has no context."""
        context_list = node.get("properties", {}).get("node_context", [])

        if not context_list:
            return ""

        formatted_context = ""
        for i, ctx in enumerate(context_list[:40]):
            text = ctx.get("text", "No content")
            origin = ctx.get("origin", "Unknown source")
            formatted_context += f"[{i}] {text} (Source: {origin})\n"

        prompt_template = self.prompts.get("structural_analysis", "")
        if not prompt_template:
            LOGGER.error("Missing structural_analysis prompt in config")
            return ""

        node_type = node.get("type", "Unknown")
        return prompt_template.format(
            id=node.get("id"),
            type=node_type,
            node_type_description=self._get_node_type_description(node_type),
            context_list=formatted_context,
            taxonomy_nodes="Person, Project, Organization, Group, Event, Roles, Business_relation"
        )

    def _parse_structural_response(self, response_text: str, node_id: str) -> Dict:
        """Parse LLM response for structural analysis."""
        try:
            cleaned_json = response_text.replace("```json", "").replace("```", "").strip()
            result = json.loads(cleaned_json)

            if "action" not in result:
                result["action"] = "KEEP"
            if "confidence" not in result:
                result["confidence"] = 0.0

            return result

        except Exception as e:
            LOGGER.error(f"Structural analysis parse failed for {node_id}: {e}")
            return {"action": "KEEP", "confidence": 0.0, "reason": f"Parse error: {str(e)}"}

    def batch_structural_analysis(self, nodes: List[Dict]) -> Dict[str, Dict]:
        """
        Run structural analysis for multiple nodes in parallel using batch_generate.

        Returns:
            Dict mapping node_id -> analysis result
        """
        # Build prompts for nodes that have context
        prompts = []
        node_ids = []
        skip_results = {}

        for node in nodes:
            node_id = node.get("id", "unknown")
            prompt = self._build_structural_prompt(node)

            if not prompt:
                skip_results[node_id] = {"action": "KEEP", "confidence": 1.0, "reason": "No context available"}
            else:
                prompts.append(prompt)
                node_ids.append(node_id)

        if not prompts:
            LOGGER.info("No nodes with context for structural analysis")
            return skip_results

        LOGGER.info(f"Running batch structural analysis for {len(prompts)} nodes...")

        # Anthropic Sonnet för strukturell analys (OBJEKT-85)
        responses = self.llm_service.batch_generate(prompts, provider='anthropic', model=self.llm_service.models['fast'])

        results = dict(skip_results)
        for node_id, response in zip(node_ids, responses):
            if not response.success:
                LOGGER.error(f"Structural analysis LLM failed for {node_id}: {response.error}")
                results[node_id] = {"action": "KEEP", "confidence": 0.0, "reason": f"LLM error: {response.error}"}
            else:
                results[node_id] = self._parse_structural_response(response.text, node_id)

        return results

    def check_structural_changes(self, node: Dict) -> Dict:
        """
        Call LLM to analyze if node requires structural changes.
        Single-node version for backwards compatibility.

        Actions:
        - KEEP: No change needed
        - DELETE: Node is noise and should be removed
        - RENAME: Node has weak name, change to suggested name
        - SPLIT: Node contains multiple distinct entities
        - RE-CATEGORIZE: Node type doesn't match content
        """
        prompt = self._build_structural_prompt(node)

        if not prompt:
            return {"action": "KEEP", "confidence": 1.0, "reason": "No context available for analysis"}

        # Anthropic Sonnet för strukturell analys (OBJEKT-85)
        response = self.llm_service.generate(prompt, provider='anthropic', model=self.llm_service.models['fast'])
        if not response.success:
            LOGGER.error(f"Structural analysis LLM failed for {node.get('id')}: {response.error}")
            return {"action": "KEEP", "confidence": 0.0, "reason": f"LLM error: {response.error}"}

        return self._parse_structural_response(response.text, node.get("id", "unknown"))

    def propagate_changes(self, unit_ids: List[str]) -> int:
        """
        Regenerate semantic metadata for Lake files affected by graph changes.

        Triggered after MERGE, SPLIT, RENAME, RE-CATEGORIZE operations.
        Updates context_summary, relations_summary and document_keywords
        based on new graph structure.

        Args:
            unit_ids: List of unit_id for files that need updating

        Returns:
            Number of files updated
        """
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

                # Hämta entiteter som dokumentet MENTIONS och konvertera till resolved_entities-format
                resolved_entities = self._get_resolved_entities_for_unit(unit_id)

                # Använd metadata_service för förädling (current_meta != None)
                new_semantics = generate_semantic_metadata(
                    text=file_content,
                    resolved_entities=resolved_entities,
                    current_meta={
                        "context_summary": current_meta.get('context_summary', ''),
                        "relations_summary": current_meta.get('relations_summary', ''),
                        "document_keywords": current_meta.get('document_keywords', [])
                    },
                    filename=os.path.basename(filepath)
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
            config_path = os.path.join(
                os.path.dirname(__file__), '..', '..', 'config', 'my_mem_config.yaml'
            )
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
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
        """
        Get entities that the document MENTIONS, formatted as resolved_entities.

        Returns list in format expected by metadata_service:
        [{"action": "LINK", "target_uuid": "...", "type": "Person", "canonical_name": "..."}]
        """
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

    # DEPRECATED: Ersatt av metadata_service.generate_semantic_metadata()
    def _get_graph_context_for_unit(self, unit_id: str) -> str:
        """
        Get graph context for a specific unit.
        Returns a string with relevant entities and relations.
        NOTE: Kept for backwards compatibility, but _get_resolved_entities_for_unit is preferred.
        """
        try:
            mentions = self.graph_service.get_nodes_mentioning_unit(unit_id)

            if not mentions:
                return "No known entities connected to this document."

            context_lines = ["KNOWN ENTITIES IN DOCUMENT:"]
            for node in mentions:
                node_type = node.get('type', 'Unknown')
                name = node.get('properties', {}).get('name', node.get('id'))
                context_lines.append(f"- [{node_type}] {name}")

            return "\n".join(context_lines)
        except Exception as e:
            LOGGER.warning(f"Could not get graph context for {unit_id}: {e}")
            return ""

    # DEPRECATED: Ersatt av metadata_service.generate_semantic_metadata()
    def _regenerate_semantics_llm(self, file_content: str, current_meta: Dict, graph_context: str) -> Dict:
        """
        Call LLM to regenerate semantic metadata.

        Args:
            file_content: Document content
            current_meta: Current frontmatter
            graph_context: Context from graph (known entities)

        Returns:
            Dict with context_summary, relations_summary, document_keywords
            or None on error
        """
        prompt_template = self.prompts.get("semantic_regeneration", "")
        if not prompt_template:
            LOGGER.warning("Missing semantic_regeneration prompt - using current metadata")
            return None

        truncated_content = file_content[:15000]

        prompt = prompt_template.format(
            file_content=truncated_content,
            current_summary=current_meta.get('context_summary', ''),
            current_relations=current_meta.get('relations_summary', ''),
            current_keywords=json.dumps(current_meta.get('document_keywords', []), ensure_ascii=False),
            graph_context=graph_context
        )

        # Anthropic Sonnet för enrichment (OBJEKT-85)
        response = self.llm_service.generate(prompt, provider='anthropic', model=self.llm_service.models['fast'])
        if not response.success:
            LOGGER.error(f"Semantic regeneration LLM failed: {response.error}")
            return None

        try:
            cleaned_json = response.text.replace("```json", "").replace("```", "").strip()
            result = json.loads(cleaned_json)

            if not isinstance(result, dict):
                return None

            return {
                'context_summary': result.get('context_summary'),
                'relations_summary': result.get('relations_summary'),
                'document_keywords': result.get('document_keywords')
            }

        except Exception as e:
            LOGGER.error(f"Semantic regeneration parse failed: {e}")
            return None
