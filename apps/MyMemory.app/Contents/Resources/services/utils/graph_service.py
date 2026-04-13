"""
GraphService - PostgreSQL-based graph database.

Relational graph model with nodes/edges tables backed by PostgreSQL.
Replaces DuckDB backend (cloud/postgresql-backend branch).

All access BORDE gå via graph_scope() context manager som hanterar
connection lifecycle och tenant-routing.
"""

import json
import logging
import unicodedata
import difflib
from contextlib import contextmanager
from datetime import datetime

import psycopg2
import psycopg2.extras

# --- LOGGING ---
LOGGER = logging.getLogger('GraphService')


def _parse_props(val) -> dict:
    """Parse properties — handles both JSONB (dict) and legacy JSON text."""
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return {}


def _parse_aliases(val) -> list:
    """Parse aliases — handles JSONB (list) and legacy JSON text."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return []


class GraphService:
    """
    Thread-safe grafdatabas med PostgreSQL backend.

    Schema:
        nodes(id UUID, tenant_id UUID, node_type TEXT, aliases JSONB, properties JSONB)
        edges(id UUID, tenant_id UUID, source_id UUID, target_id UUID, edge_type TEXT, properties JSONB)
    """

    def __init__(self, conn, tenant_id: str, read_only: bool = False):
        """
        Skapa en GraphService mot en existerande PostgreSQL-anslutning.

        Args:
            conn: psycopg2 connection
            tenant_id: UUID-sträng för tenant-isolation
            read_only: Om True, blockera skrivoperationer
        """
        self.conn = conn
        self.tenant_id = tenant_id
        self.read_only = read_only
        LOGGER.debug(f"GraphService öppnad (tenant={tenant_id}, read_only={read_only})")

    def close(self):
        """Stäng databasanslutningen."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            LOGGER.debug("GraphService stängd")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # --- NODE OPERATIONS ---

    def get_node(self, node_id: str) -> dict | None:
        """Hämta en nod med givet ID."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, node_type, aliases, properties FROM nodes "
                "WHERE id = %s AND tenant_id = %s",
                [node_id, self.tenant_id]
            )
            row = cur.fetchone()

        if not row:
            return None

        return {
            "id": str(row[0]),
            "type": row[1],
            "aliases": _parse_aliases(row[2]),
            "properties": _parse_props(row[3])
        }

    def find_nodes_by_type(self, node_type: str) -> list[dict]:
        """Hitta alla noder av en viss typ."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, node_type, aliases, properties FROM nodes "
                "WHERE node_type = %s AND tenant_id = %s",
                [node_type, self.tenant_id]
            )
            rows = cur.fetchall()

        return [{
            "id": str(r[0]),
            "type": r[1],
            "aliases": _parse_aliases(r[2]),
            "properties": _parse_props(r[3])
        } for r in rows]

    def find_nodes_by_alias(self, alias: str) -> list[dict]:
        """Hitta noder där alias matchar."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, node_type, aliases, properties FROM nodes "
                "WHERE tenant_id = %s AND aliases @> %s::jsonb",
                [self.tenant_id, json.dumps([alias])]
            )
            rows = cur.fetchall()

        return [{
            "id": str(r[0]),
            "type": r[1],
            "aliases": _parse_aliases(r[2]),
            "properties": _parse_props(r[3])
        } for r in rows]

    def upsert_node(self, id: str, type: str, aliases: list = None, properties: dict = None):
        """
        Skapa eller uppdatera en nod.
        Hanterar merge av properties för att bevara system-metadata.
        """
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        # Schema guard: validate node type
        _validator = None
        try:
            from services.utils.schema_validator import SchemaValidator
            _validator = SchemaValidator()
            valid_types = list(_validator.schema.get("nodes", {}).keys())
            if type not in valid_types:
                raise ValueError(
                    f"Schema guard REJECTED node: unknown type '{type}'. "
                    f"Allowed: {valid_types}"
                )
        except ImportError:
            LOGGER.debug("SchemaValidator not available — skipping node type validation")

        new_props = properties or {}

        with self.conn.cursor() as cur:
            # Hämta existerande properties
            cur.execute(
                "SELECT properties FROM nodes WHERE id = %s AND tenant_id = %s",
                [id, self.tenant_id]
            )
            existing = cur.fetchone()

            final_props = {}

            if existing:
                current_props = _parse_props(existing[0])
                final_props = current_props.copy()

                # Append-merge för list-properties
                for k, v in new_props.items():
                    if isinstance(v, list) and k in final_props and isinstance(final_props[k], list):
                        combined = final_props[k] + v
                        if combined and isinstance(combined[0], dict):
                            seen = set()
                            unique_list = []
                            for item in combined:
                                item_key = tuple(sorted((ik, str(iv)) for ik, iv in item.items()))
                                if item_key not in seen:
                                    seen.add(item_key)
                                    unique_list.append(item)
                            final_props[k] = unique_list
                        else:
                            final_props[k] = list(set(combined))
                    else:
                        final_props[k] = v
            else:
                # Ny nod — initiera defaults enligt schema
                defaults = _validator.get_base_property_defaults() if _validator else {}
                final_props = defaults
                final_props.update(new_props)

            aliases_json = json.dumps(aliases or [], ensure_ascii=False)
            props_json = json.dumps(final_props, ensure_ascii=False)

            cur.execute("""
                INSERT INTO nodes (id, tenant_id, node_type, aliases, properties)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    node_type = EXCLUDED.node_type,
                    aliases = EXCLUDED.aliases,
                    properties = EXCLUDED.properties,
                    updated_at = now()
            """, [id, self.tenant_id, type, aliases_json, props_json])
            self.conn.commit()

    def register_usage(self, node_ids: list):
        """Registrera att noder har använts i ett svar (Relevans)."""
        if not node_ids:
            return

        now_ts = datetime.now().isoformat()

        with self.conn.cursor() as cur:
            for nid in node_ids:
                cur.execute(
                    "SELECT properties FROM nodes WHERE id = %s AND tenant_id = %s",
                    [nid, self.tenant_id]
                )
                row = cur.fetchone()
                if not row:
                    continue

                props = _parse_props(row[0])
                count = props.get('retrieved_times', 0)
                if not isinstance(count, int):
                    count = 0

                props['retrieved_times'] = count + 1
                props['last_retrieved_at'] = now_ts

                cur.execute(
                    "UPDATE nodes SET properties = %s::jsonb WHERE id = %s AND tenant_id = %s",
                    [json.dumps(props, ensure_ascii=False), nid, self.tenant_id]
                )
            self.conn.commit()

        LOGGER.info(f"Registered usage for {len(node_ids)} nodes")

    def get_refinement_candidates(self, limit: int = 50) -> list[dict]:
        """Hämta kandidater för Dreamer-underhåll (80/20 relevans/underhåll)."""
        relevance_limit = int(limit * 0.8)
        maintenance_limit = limit - relevance_limit

        candidates = []

        with self.conn.cursor() as cur:
            # 1. Relevans (heta noder)
            cur.execute("""
                SELECT id, node_type, aliases, properties FROM nodes
                WHERE tenant_id = %s
                ORDER BY properties->>'last_retrieved_at' DESC NULLS LAST
                LIMIT %s
            """, [self.tenant_id, relevance_limit])
            rel_rows = cur.fetchall()

            # 2. Underhåll (glömda noder)
            cur.execute("""
                SELECT id, node_type, aliases, properties FROM nodes
                WHERE tenant_id = %s
                ORDER BY
                    CASE WHEN properties->>'last_refined_at' = 'never' THEN 0 ELSE 1 END,
                    properties->>'last_refined_at' ASC NULLS FIRST
                LIMIT %s
            """, [self.tenant_id, maintenance_limit])
            maint_rows = cur.fetchall()

        seen_ids = set()
        for r in rel_rows + maint_rows:
            rid = str(r[0])
            if rid not in seen_ids:
                candidates.append({
                    "id": rid,
                    "type": r[1],
                    "aliases": _parse_aliases(r[2]),
                    "properties": _parse_props(r[3])
                })
                seen_ids.add(rid)

        return candidates

    def delete_node(self, node_id: str) -> bool:
        """Ta bort en nod och alla dess kanter."""
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM edges WHERE (source_id = %s OR target_id = %s) AND tenant_id = %s",
                [node_id, node_id, self.tenant_id]
            )
            cur.execute(
                "DELETE FROM nodes WHERE id = %s AND tenant_id = %s RETURNING id",
                [node_id, self.tenant_id]
            )
            result = cur.fetchone()
            self.conn.commit()

        return result is not None

    def update_node_properties(self, node_id: str, properties: dict):
        """Direkt överskrivning av properties (inte merge)."""
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        props_json = json.dumps(properties, ensure_ascii=False)
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE nodes SET properties = %s::jsonb, updated_at = now() "
                "WHERE id = %s AND tenant_id = %s",
                [props_json, node_id, self.tenant_id]
            )
            self.conn.commit()

    @staticmethod
    def _fold_diacritics(s: str) -> str:
        """Fold diacritics: Ohlén → ohlen, Björkengren → bjorkengren."""
        return unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode().lower()

    def find_node_by_name(self, node_type: str, name: str, fuzzy: bool = True,
                          matching_config: dict = None) -> str | None:
        """
        Sök efter en nod baserat på namn. Schema-driven matching.

        Matchningsordning:
        1. Exakt match (lowercase + aliases)
        2. Exakt match (diakritik-normaliserad)
        3. Token-subset match
        4. First-token prefix match
        5. Ambiguity guard
        6. Fuzzy match (difflib)
        """
        name_lower = name.strip().lower()
        name_folded = self._fold_diacritics(name)

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, properties FROM nodes WHERE node_type = %s AND tenant_id = %s",
                [node_type, self.tenant_id]
            )
            rows = cur.fetchall()

        # Bygg namn-index
        name_to_uuid: dict[str, list[str]] = {}
        folded_to_uuid: dict[str, list[str]] = {}
        for node_id, props_raw in rows:
            nid = str(node_id)
            props = _parse_props(props_raw)
            node_name = (props.get('name') or 'Unknown').strip().lower()
            if node_name:
                name_to_uuid.setdefault(node_name, []).append(nid)
                node_folded = self._fold_diacritics(node_name)
                folded_to_uuid.setdefault(node_folded, []).append(nid)

            for alias in props.get('aliases', []):
                alias_lower = alias.strip().lower()
                name_to_uuid.setdefault(alias_lower, []).append(nid)
                alias_folded = self._fold_diacritics(alias)
                folded_to_uuid.setdefault(alias_folded, []).append(nid)

        # 1. Exakt matchning (original)
        if name_lower in name_to_uuid:
            hits = name_to_uuid[name_lower]
            if len(hits) > 1:
                LOGGER.warning(f"find_node_by_name: Flera träffar för '{name}' ({node_type}): {hits}")
            return hits[0]

        # 2. Exakt matchning (diakritik-normaliserad)
        if name_folded in folded_to_uuid:
            hits = folded_to_uuid[name_folded]
            if len(hits) == 1:
                LOGGER.info(f"find_node_by_name: Diacritics fold '{name}' -> {hits[0]}")
                return hits[0]
            LOGGER.warning(f"find_node_by_name: Flera träffar (folded) för '{name}' ({node_type}): {hits}")
            return hits[0]

        # Matching config
        mc = matching_config or {}
        do_token_subset = mc.get('token_subset', False)
        do_first_token = mc.get('first_token_prefix', False)
        fuzzy_cutoff = mc.get('fuzzy_cutoff', 0.85)
        ambiguity_action = mc.get('ambiguity_action', 'CREATE')

        # 3-4. Token matching
        if do_token_subset or do_first_token:
            query_tokens = set(name_folded.split())
            token_matches: list[str] = []

            for candidate_folded, uuids in folded_to_uuid.items():
                candidate_tokens = set(candidate_folded.split())

                if do_token_subset:
                    shorter, longer = (query_tokens, candidate_tokens) \
                        if len(query_tokens) <= len(candidate_tokens) \
                        else (candidate_tokens, query_tokens)
                    if shorter and shorter.issubset(longer) and shorter != longer:
                        token_matches.extend(uuids)
                        continue

                if do_first_token and len(query_tokens) == 1:
                    query_token = next(iter(query_tokens))
                    candidate_list = candidate_folded.split()
                    if len(candidate_list) > 1 and candidate_list[0] == query_token:
                        token_matches.extend(uuids)
                        continue

            unique_matches = list(dict.fromkeys(token_matches))

            if len(unique_matches) == 1:
                LOGGER.info(f"find_node_by_name: Token match '{name}' ({node_type}) -> {unique_matches[0]}")
                return unique_matches[0]
            elif len(unique_matches) > 1:
                LOGGER.warning(
                    f"find_node_by_name: Ambiguous token match for '{name}' ({node_type}): "
                    f"{len(unique_matches)} candidates -> {ambiguity_action}"
                )
                if ambiguity_action == "CREATE":
                    return None

        # 6. Fuzzy matchning
        if fuzzy:
            candidates = list(folded_to_uuid.keys())
            matches = difflib.get_close_matches(name_folded, candidates, n=1, cutoff=fuzzy_cutoff)
            if matches:
                matched_name = matches[0]
                hits = folded_to_uuid[matched_name]
                LOGGER.info(
                    f"find_node_by_name: Fuzzy '{name}' ~= '{matched_name}' "
                    f"(cutoff={fuzzy_cutoff}) -> {hits[0]}"
                )
                return hits[0]

        return None

    # --- EDGE OPERATIONS ---

    def get_edges_from(self, node_id: str) -> list[dict]:
        """Hämta alla utgående kanter från en nod."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT source_id, target_id, edge_type, properties FROM edges "
                "WHERE source_id = %s AND tenant_id = %s",
                [node_id, self.tenant_id]
            )
            rows = cur.fetchall()

        return [{
            "source": str(r[0]),
            "target": str(r[1]),
            "type": r[2],
            "properties": _parse_props(r[3])
        } for r in rows]

    def get_edges_to(self, node_id: str) -> list[dict]:
        """Hämta alla inkommande kanter till en nod."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT source_id, target_id, edge_type, properties FROM edges "
                "WHERE target_id = %s AND tenant_id = %s",
                [node_id, self.tenant_id]
            )
            rows = cur.fetchall()

        return [{
            "source": str(r[0]),
            "target": str(r[1]),
            "type": r[2],
            "properties": _parse_props(r[3])
        } for r in rows]

    def get_node_relations_for_vector(self, node_id: str) -> list[dict]:
        """Hämta relationer för vektorindexering (filtrerar bort Source-edges)."""
        try:
            from services.utils.schema_validator import SchemaValidator
            source_edge_types = SchemaValidator().get_source_edge_types()
        except ImportError:
            LOGGER.debug("SchemaValidator not available — skipping source edge filtering")
            source_edge_types = set()

        relations = []

        for edge in self.get_edges_from(node_id):
            if edge['type'] in source_edge_types:
                continue
            other = self.get_node(edge['target'])
            if not other:
                continue
            name = other.get('properties', {}).get('name', other.get('id', ''))
            if name:
                relations.append({
                    'edge_type': edge['type'],
                    'target_name': name,
                    'direction': 'out',
                })

        for edge in self.get_edges_to(node_id):
            if edge['type'] in source_edge_types:
                continue
            other = self.get_node(edge['source'])
            if not other:
                continue
            name = other.get('properties', {}).get('name', other.get('id', ''))
            if name:
                relations.append({
                    'edge_type': edge['type'],
                    'target_name': name,
                    'direction': 'in',
                })

        return relations

    def upsert_edge(self, source: str, target: str, edge_type: str, properties: dict = None):
        """
        Skapa eller uppdatera en kant.
        Listproperties appendas och dedupliceras.
        """
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        # Schema guard
        source_node = self.get_node(source)
        target_node = self.get_node(target)
        if source_node and target_node:
            try:
                from services.utils.schema_validator import SchemaValidator
                validator = SchemaValidator()
                nodes_map = {
                    source: source_node.get("type", "Unknown"),
                    target: target_node.get("type", "Unknown"),
                }
                edge_dict = {"source": source, "target": target, "type": edge_type}
                ok, msg = validator.validate_edge_structure(edge_dict, nodes_map)
                if not ok:
                    LOGGER.warning(
                        f"Schema guard REJECTED edge: {source_node.get('type')}→{target_node.get('type')} "
                        f"via {edge_type} — {msg}"
                    )
                    raise ValueError(
                        f"Schema guard REJECTED edge: {source_node.get('type')}→{target_node.get('type')} "
                        f"via {edge_type} — {msg}"
                    )
            except ImportError:
                LOGGER.debug("SchemaValidator not available — skipping edge validation")

        new_props = properties or {}

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, properties FROM edges "
                "WHERE source_id = %s AND target_id = %s AND edge_type = %s AND tenant_id = %s",
                [source, target, edge_type, self.tenant_id]
            )
            existing = cur.fetchone()

            if existing:
                current_props = _parse_props(existing[1])
                final_props = current_props.copy()
                for k, v in new_props.items():
                    if isinstance(v, list) and k in final_props and isinstance(final_props[k], list):
                        combined = final_props[k] + v
                        if combined and isinstance(combined[0], dict):
                            seen = set()
                            unique_list = []
                            for item in combined:
                                item_key = tuple(sorted((ik, str(iv)) for ik, iv in item.items()))
                                if item_key not in seen:
                                    seen.add(item_key)
                                    unique_list.append(item)
                            if unique_list and "timestamp" in unique_list[0]:
                                unique_list.sort(key=lambda e: e.get("timestamp", ""))
                            final_props[k] = unique_list
                        else:
                            final_props[k] = list(set(combined))
                    else:
                        final_props[k] = v

                cur.execute(
                    "UPDATE edges SET properties = %s::jsonb WHERE id = %s",
                    [json.dumps(final_props, ensure_ascii=False), existing[0]]
                )
            else:
                props_json = json.dumps(new_props, ensure_ascii=False)
                cur.execute("""
                    INSERT INTO edges (tenant_id, source_id, target_id, edge_type, properties)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                """, [self.tenant_id, source, target, edge_type, props_json])

            self.conn.commit()

    def delete_edge(self, source: str, target: str, edge_type: str) -> bool:
        """Ta bort en specifik kant."""
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM edges "
                "WHERE source_id = %s AND target_id = %s AND edge_type = %s AND tenant_id = %s "
                "RETURNING id",
                [source, target, edge_type, self.tenant_id]
            )
            result = cur.fetchone()
            self.conn.commit()

        return result is not None

    def get_edge(self, source: str, target: str, edge_type: str) -> dict | None:
        """Hämta en specifik edge med dess properties."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT source_id, target_id, edge_type, properties FROM edges "
                "WHERE source_id = %s AND target_id = %s AND edge_type = %s AND tenant_id = %s",
                [source, target, edge_type, self.tenant_id]
            )
            row = cur.fetchone()

        if not row:
            return None

        return {
            "source": str(row[0]),
            "target": str(row[1]),
            "type": row[2],
            "properties": _parse_props(row[3])
        }

    # --- STATISTICS ---

    def get_stats(self) -> dict:
        """Hämta statistik om grafen."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT node_type, COUNT(*) FROM nodes WHERE tenant_id = %s GROUP BY node_type",
                [self.tenant_id]
            )
            node_counts = cur.fetchall()

            cur.execute(
                "SELECT edge_type, COUNT(*) FROM edges WHERE tenant_id = %s GROUP BY edge_type",
                [self.tenant_id]
            )
            edge_counts = cur.fetchall()

        nodes_dict = {row[0]: row[1] for row in node_counts}
        edges_dict = {row[0]: row[1] for row in edge_counts}

        return {
            "total_nodes": sum(nodes_dict.values()),
            "total_edges": sum(edges_dict.values()),
            "nodes": nodes_dict,
            "edges": edges_dict
        }

    def get_maintenance_stats(self) -> dict:
        """Returnerar rådata för sömnbehovsberäkning (#135)."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM nodes WHERE node_type != 'Source' AND tenant_id = %s",
                [self.tenant_id]
            )
            total = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM nodes
                WHERE node_type != 'Source' AND tenant_id = %s
                AND properties->>'last_refined_at' = 'never'
            """, [self.tenant_id])
            never_refined = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM nodes
                WHERE node_type != 'Source' AND tenant_id = %s
                AND (
                    properties->>'quality_flags' LIKE '%%possible_split%%'
                    OR properties->>'quality_flags' LIKE '%%possible_recategorize%%'
                )
            """, [self.tenant_id])
            flags = cur.fetchone()[0]

        return {
            "total_nodes": total,
            "nodes_never_refined": never_refined,
            "quality_flags_count": flags,
        }

    # --- SEARCH HELPERS ---

    def find_nodes_fuzzy(self, term: str, limit: int = 10) -> list[dict]:
        """Fuzzy-sök efter noder baserat på ID, namn eller alias."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, node_type, aliases, properties FROM nodes
                WHERE tenant_id = %s
                AND (
                    id::text ILIKE %s
                    OR aliases::text ILIKE %s
                    OR properties->>'name' ILIKE %s
                )
                LIMIT %s
            """, [self.tenant_id, f"%{term}%", f"%{term}%", f"%{term}%", limit])
            rows = cur.fetchall()

        return [{
            "id": str(r[0]),
            "type": r[1],
            "aliases": _parse_aliases(r[2]),
            "properties": _parse_props(r[3])
        } for r in rows]

    def get_related_units(self, entity_id: str, limit: int = 10) -> list[str]:
        """Hitta alla Units (dokument) som nämner en Entity."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT source_id FROM edges
                WHERE target_id = %s AND edge_type = 'MENTIONS' AND tenant_id = %s
                LIMIT %s
            """, [entity_id, self.tenant_id, limit])
            rows = cur.fetchall()

        return [str(r[0]) for r in rows]

    # --- DREAMER SUPPORT ---

    def record_dreamer_decision(self, pass_name, decision, node_id, node_name_str,
                                confidence, reason, metadata=None):
        """Registrera ett Dreamer-beslut i audit trail (#105)."""
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva dreamer_decision i read_only mode")

        metadata_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dreamer_decisions
                (tenant_id, timestamp, pass_name, decision, node_id, node_name, confidence, reason, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """, [self.tenant_id, datetime.now(), pass_name, decision,
                  node_id, node_name_str, confidence, reason, metadata_json])
            self.conn.commit()

    def _merge_edge_relation_context(self, source: str, target: str, edge_type: str, donor_props):
        """Interfoliera relation_context från donator-kant till befintlig kant."""
        donor_props = _parse_props(donor_props)
        donor_rc = donor_props.get('relation_context', [])
        if not donor_rc:
            return

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT properties FROM edges "
                "WHERE source_id = %s AND target_id = %s AND edge_type = %s AND tenant_id = %s",
                [source, target, edge_type, self.tenant_id]
            )
            existing = cur.fetchone()
            if not existing:
                return

            existing_props = _parse_props(existing[0])
            existing_rc = existing_props.get('relation_context', [])

            combined = existing_rc + donor_rc
            seen = set()
            unique_rc = []
            for entry in combined:
                key = (entry.get('text', ''), entry.get('origin', ''))
                if key not in seen:
                    seen.add(key)
                    unique_rc.append(entry)
            unique_rc.sort(key=lambda e: e.get('timestamp', '9999'))

            existing_props['relation_context'] = unique_rc
            cur.execute(
                "UPDATE edges SET properties = %s::jsonb "
                "WHERE source_id = %s AND target_id = %s AND edge_type = %s AND tenant_id = %s",
                [json.dumps(existing_props, ensure_ascii=False), source, target, edge_type, self.tenant_id]
            )

    def merge_nodes(self, target_id: str, source_id: str):
        """Slå ihop source_id in i target_id (atomär med PG-transaktion)."""
        if self.read_only:
            raise RuntimeError("HARDFAIL: Read-only mode")

        with self.conn.cursor() as cur:
            # 1. HÄMTA DATA
            cur.execute("SELECT properties FROM nodes WHERE id = %s AND tenant_id = %s",
                        [target_id, self.tenant_id])
            res_target = cur.fetchone()
            cur.execute("SELECT properties FROM nodes WHERE id = %s AND tenant_id = %s",
                        [source_id, self.tenant_id])
            res_source = cur.fetchone()

            if not res_target or not res_source:
                LOGGER.warning(f"Merge aborted: Node missing ({target_id} or {source_id})")
                return

            props_t = _parse_props(res_target[0])
            props_s = _parse_props(res_source[0])

            # 2. AGGREGERA PROPERTIES
            merged_props = props_t.copy()
            for k, v in props_s.items():
                if isinstance(v, list) and k in merged_props and isinstance(merged_props[k], list):
                    combined = merged_props[k] + v
                    if combined and isinstance(combined[0], dict):
                        seen = set()
                        unique_list = []
                        for item in combined:
                            item_key = tuple(sorted((ik, str(iv)) for ik, iv in item.items()))
                            if item_key not in seen:
                                seen.add(item_key)
                                unique_list.append(item)
                        merged_props[k] = unique_list
                    else:
                        merged_props[k] = list(set(combined))
                elif k not in merged_props:
                    merged_props[k] = v

            cur.execute(
                "UPDATE nodes SET properties = %s::jsonb, updated_at = now() WHERE id = %s AND tenant_id = %s",
                [json.dumps(merged_props, ensure_ascii=False), target_id, self.tenant_id]
            )

            # 3. FLYTTA UTGÅENDE KANTER
            cur.execute("""
                UPDATE edges SET source_id = %s
                WHERE source_id = %s AND tenant_id = %s
                AND NOT EXISTS (
                    SELECT 1 FROM edges e2
                    WHERE e2.source_id = %s AND e2.target_id = edges.target_id
                    AND e2.edge_type = edges.edge_type AND e2.tenant_id = %s
                )
            """, [target_id, source_id, self.tenant_id, target_id, self.tenant_id])

            # 4. FLYTTA INKOMMANDE KANTER
            cur.execute("""
                UPDATE edges SET target_id = %s
                WHERE target_id = %s AND tenant_id = %s
                AND NOT EXISTS (
                    SELECT 1 FROM edges e2
                    WHERE e2.source_id = edges.source_id AND e2.target_id = %s
                    AND e2.edge_type = edges.edge_type AND e2.tenant_id = %s
                )
            """, [target_id, source_id, self.tenant_id, target_id, self.tenant_id])

            # 4b. Interfoliera relation_context för konfliktande kanter
            cur.execute(
                "SELECT target_id, edge_type, properties FROM edges WHERE source_id = %s AND tenant_id = %s",
                [source_id, self.tenant_id]
            )
            remaining_out = cur.fetchall()
            cur.execute(
                "SELECT source_id, edge_type, properties FROM edges WHERE target_id = %s AND tenant_id = %s",
                [source_id, self.tenant_id]
            )
            remaining_in = cur.fetchall()

            for other_id, etype, props_raw in remaining_out:
                self._merge_edge_relation_context(target_id, str(other_id), etype, props_raw)
            for other_id, etype, props_raw in remaining_in:
                self._merge_edge_relation_context(str(other_id), target_id, etype, props_raw)

            # 5. STÄDA KANTER
            cur.execute(
                "DELETE FROM edges WHERE (source_id = %s OR target_id = %s) AND tenant_id = %s",
                [source_id, source_id, self.tenant_id]
            )
            cur.execute(
                "DELETE FROM edges WHERE source_id = %s AND target_id = %s AND tenant_id = %s",
                [target_id, target_id, self.tenant_id]
            )

            # 6. FLYTTA ALIASES
            cur.execute("SELECT aliases FROM nodes WHERE id = %s AND tenant_id = %s", [source_id, self.tenant_id])
            aliases_s = _parse_aliases(cur.fetchone()[0]) if cur.rowcount else []
            cur.execute("SELECT aliases FROM nodes WHERE id = %s AND tenant_id = %s", [target_id, self.tenant_id])
            aliases_t = _parse_aliases(cur.fetchone()[0]) if cur.rowcount else []

            new_aliases = list(set(aliases_t + aliases_s + [source_id]))
            cur.execute(
                "UPDATE nodes SET aliases = %s::jsonb WHERE id = %s AND tenant_id = %s",
                [json.dumps(new_aliases, ensure_ascii=False), target_id, self.tenant_id]
            )

            # 7. RADERA SOURCE
            cur.execute("DELETE FROM nodes WHERE id = %s AND tenant_id = %s", [source_id, self.tenant_id])

            self.conn.commit()

        LOGGER.info(f"Merged {source_id} into {target_id}")

    def rename_node(self, old_id: str, new_name: str):
        """Byt namn på en nod. Uppdaterar properties.name, lägger till gamla som alias."""
        if self.read_only:
            raise RuntimeError("HARDFAIL: Read-only")

        with self.conn.cursor() as cur:
            # Kolla om nod med nya namnet redan finns
            cur.execute(
                "SELECT id FROM nodes WHERE properties->>'name' = %s AND id != %s AND tenant_id = %s",
                [new_name, old_id, self.tenant_id]
            )
            existing = cur.fetchone()

            if existing:
                LOGGER.info(f"Rename: Node with name '{new_name}' exists ({existing[0]}). Merging instead.")
                self.merge_nodes(str(existing[0]), old_id)
                return

            cur.execute(
                "SELECT node_type, aliases, properties FROM nodes WHERE id = %s AND tenant_id = %s",
                [old_id, self.tenant_id]
            )
            res = cur.fetchone()
            if not res:
                LOGGER.warning(f"Rename failed: Source {old_id} not found")
                return

            aliases = _parse_aliases(res[1])
            props = _parse_props(res[2])
            old_name = props.get("name", "")

            if old_name and old_name != new_name and old_name not in aliases:
                aliases.append(old_name)

            props["name"] = new_name

            cur.execute(
                "UPDATE nodes SET aliases = %s::jsonb, properties = %s::jsonb, updated_at = now() "
                "WHERE id = %s AND tenant_id = %s",
                [json.dumps(aliases, ensure_ascii=False),
                 json.dumps(props, ensure_ascii=False),
                 old_id, self.tenant_id]
            )
            self.conn.commit()

        LOGGER.info(f"Renamed node {old_id}: '{old_name}' -> '{new_name}'")

    def split_node(self, original_id: str, split_map: list,
                   edge_assignment: dict = None, source_edge_types: set = None):
        """Dela upp en nod i flera nya noder."""
        import uuid as uuid_module

        if self.read_only:
            raise RuntimeError("HARDFAIL: Read-only")

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT node_type, properties FROM nodes WHERE id = %s AND tenant_id = %s",
                [original_id, self.tenant_id]
            )
            res = cur.fetchone()
            if not res:
                LOGGER.warning(f"Split failed: Node {original_id} not found")
                return []

            orig_type = res[0]
            orig_props = _parse_props(res[1])
            stale_keys = ("context_summary", "quality_flags", "quality_flag_reason")

            created_nodes = []
            for item in split_map:
                new_name = item.get("name")
                if not new_name:
                    continue

                new_node_id = str(uuid_module.uuid4())
                new_props = orig_props.copy()
                new_props["name"] = new_name
                for key in stale_keys:
                    new_props.pop(key, None)

                cur.execute(
                    "INSERT INTO nodes (id, tenant_id, node_type, aliases, properties) "
                    "VALUES (%s, %s, %s, '[]'::jsonb, %s::jsonb)",
                    [new_node_id, self.tenant_id, orig_type,
                     json.dumps(new_props, ensure_ascii=False)]
                )
                LOGGER.info(f"Split: Created node '{new_name}' with ID {new_node_id}")
                created_nodes.append(new_node_id)

            # Kopiera relationer
            if edge_assignment is not None and source_edge_types is not None:
                for ci, new_node in enumerate(created_nodes):
                    for e in edge_assignment.get(ci, []):
                        props_json = json.dumps(e.get("properties", {}), ensure_ascii=False)
                        if e.get("direction") == "out":
                            t = e["target"]
                            if t == new_node:
                                continue
                            cur.execute(
                                "INSERT INTO edges (tenant_id, source_id, target_id, edge_type, properties) "
                                "VALUES (%s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING",
                                [self.tenant_id, new_node, t, e["type"], props_json]
                            )
                        else:
                            s = e["source"]
                            if s == new_node:
                                continue
                            cur.execute(
                                "INSERT INTO edges (tenant_id, source_id, target_id, edge_type, properties) "
                                "VALUES (%s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING",
                                [self.tenant_id, s, new_node, e["type"], props_json]
                            )

                # Source edges till alla nya noder
                cur.execute(
                    "SELECT target_id, edge_type, properties FROM edges "
                    "WHERE source_id = %s AND tenant_id = %s",
                    [original_id, self.tenant_id]
                )
                out_edges = cur.fetchall()
                cur.execute(
                    "SELECT source_id, edge_type, properties FROM edges "
                    "WHERE target_id = %s AND tenant_id = %s",
                    [original_id, self.tenant_id]
                )
                in_edges = cur.fetchall()

                for new_node in created_nodes:
                    for target, etype, props in out_edges:
                        if etype in source_edge_types and str(target) != new_node:
                            cur.execute(
                                "INSERT INTO edges (tenant_id, source_id, target_id, edge_type, properties) "
                                "VALUES (%s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING",
                                [self.tenant_id, new_node, str(target), etype,
                                 json.dumps(_parse_props(props), ensure_ascii=False)]
                            )
                    for source, etype, props in in_edges:
                        if etype in source_edge_types and str(source) != new_node:
                            cur.execute(
                                "INSERT INTO edges (tenant_id, source_id, target_id, edge_type, properties) "
                                "VALUES (%s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING",
                                [self.tenant_id, str(source), new_node, etype,
                                 json.dumps(_parse_props(props), ensure_ascii=False)]
                            )
            else:
                # Legacy: kopiera ALLA kanter till ALLA nya noder
                cur.execute(
                    "SELECT target_id, edge_type, properties FROM edges "
                    "WHERE source_id = %s AND tenant_id = %s",
                    [original_id, self.tenant_id]
                )
                out_edges = cur.fetchall()
                for new_node in created_nodes:
                    for target, etype, props in out_edges:
                        if str(target) == new_node:
                            continue
                        cur.execute(
                            "INSERT INTO edges (tenant_id, source_id, target_id, edge_type, properties) "
                            "VALUES (%s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING",
                            [self.tenant_id, new_node, str(target), etype,
                             json.dumps(_parse_props(props), ensure_ascii=False)]
                        )

                cur.execute(
                    "SELECT source_id, edge_type, properties FROM edges "
                    "WHERE target_id = %s AND tenant_id = %s",
                    [original_id, self.tenant_id]
                )
                in_edges = cur.fetchall()
                for new_node in created_nodes:
                    for source, etype, props in in_edges:
                        if str(source) == new_node:
                            continue
                        cur.execute(
                            "INSERT INTO edges (tenant_id, source_id, target_id, edge_type, properties) "
                            "VALUES (%s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING",
                            [self.tenant_id, str(source), new_node, etype,
                             json.dumps(_parse_props(props), ensure_ascii=False)]
                        )

            # Radera originalnoden
            cur.execute(
                "DELETE FROM edges WHERE (source_id = %s OR target_id = %s) AND tenant_id = %s",
                [original_id, original_id, self.tenant_id]
            )
            cur.execute(
                "DELETE FROM nodes WHERE id = %s AND tenant_id = %s",
                [original_id, self.tenant_id]
            )
            self.conn.commit()

        LOGGER.info(f"Split {original_id} into {len(created_nodes)} nodes: {created_nodes}")
        return created_nodes

    def recategorize_node(self, node_id: str, new_type: str):
        """Byt typ på en nod."""
        if self.read_only:
            raise RuntimeError("HARDFAIL: Read-only")

        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE nodes SET node_type = %s, updated_at = now() WHERE id = %s AND tenant_id = %s",
                [new_type, node_id, self.tenant_id]
            )
            if cur.rowcount == 0:
                LOGGER.warning(f"Recategorize failed: Node {node_id} not found")
                return
            self.conn.commit()

        LOGGER.info(f"Recategorized {node_id} -> {new_type}")

    def get_node_degree(self, node_id: str) -> int:
        """Returnerar antal relationer (exkl. MENTIONS/DEALS_WITH)."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT count(*) FROM edges
                WHERE (source_id = %s OR target_id = %s) AND tenant_id = %s
                AND edge_type NOT IN ('MENTIONS', 'DEALS_WITH')
            """, [node_id, node_id, self.tenant_id])
            res = cur.fetchone()
            return res[0] if res else 0

    def get_related_unit_ids(self, node_id: str) -> list:
        """Hämtar alla Unit-IDs som refererar till denna nod."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT source_id FROM edges
                WHERE target_id = %s AND edge_type IN ('MENTIONS', 'DEALS_WITH') AND tenant_id = %s
            """, [node_id, self.tenant_id])
            return [str(r[0]) for r in cur.fetchall()]

    def get_nodes_mentioning_unit(self, unit_id: str) -> list[dict]:
        """Hämta alla entitetsnoder som ett dokument MENTIONS."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT target_id FROM edges
                WHERE source_id = %s AND edge_type = 'MENTIONS' AND tenant_id = %s
            """, [unit_id, self.tenant_id])
            rows = cur.fetchall()

        nodes = []
        for row in rows:
            node = self.get_node(str(row[0]))
            if node:
                nodes.append(node)
        return nodes


# --- MODULE-LEVEL CONTEXT MANAGER ---

def _get_pg_dsn() -> str:
    """Hämta PostgreSQL DSN från config."""
    from services.utils.config_loader import get_config
    config = get_config()
    pg = config.get('database', {}).get('postgresql', {})
    if not pg:
        raise ValueError("graph_scope: 'database.postgresql' saknas i config")
    return (
        f"host={pg['host']} port={pg.get('port', 5432)} "
        f"dbname={pg['dbname']} user={pg['user']} password={pg['password']}"
    )


def _get_tenant_id() -> str:
    """Hämta tenant_id från config."""
    from services.utils.config_loader import get_config
    config = get_config()
    tid = config.get('database', {}).get('tenant_id')
    if not tid:
        raise ValueError("graph_scope: 'database.tenant_id' saknas i config")
    return tid


@contextmanager
def graph_scope(exclusive=False, timeout=None, db_path=None):
    """
    PostgreSQL graph access: connect, yield GraphService, close.

    Args:
        exclusive: True för skrivoperationer (read_only=False),
                   False för läsning (read_only=True)
        timeout: Connection timeout i sekunder (None = default)
        db_path: Ignorerad (behålls för bakåtkompatibilitet)

    Usage:
        with graph_scope(exclusive=False) as graph:
            results = graph.get_node(node_id)

        with graph_scope(exclusive=True) as graph:
            graph.upsert_node(...)
    """
    dsn = _get_pg_dsn()
    tenant_id = _get_tenant_id()
    read_only = not exclusive

    connect_kwargs = {}
    if timeout is not None:
        connect_kwargs['connect_timeout'] = int(timeout)

    conn = psycopg2.connect(dsn, **connect_kwargs)
    try:
        if read_only:
            conn.set_session(readonly=True, autocommit=False)
        gs = GraphService(conn, tenant_id, read_only=read_only)
        try:
            yield gs
        finally:
            gs.conn = None  # Prevent double-close
    finally:
        if not conn.closed:
            conn.close()


# --- GRAPH HEALTH (#135) ---

def calculate_graph_health(
    maintenance_stats: dict,
    dreamer_counter: int = 0,
    last_sweep_had_decisions: bool = False,
) -> dict:
    """Beräkna grafens sömnbehov (0.0-1.0). Ren funktion."""
    total = maintenance_stats.get("total_nodes", 0)
    unenriched = maintenance_stats.get("nodes_never_refined", 0)
    flags_count = maintenance_stats.get("quality_flags_count", 0)

    if total == 0:
        score = 0.0
    else:
        score = (
            0.35 * (unenriched / total)
            + 0.25 * min(flags_count / 10, 1.0)
            + 0.20 * min(dreamer_counter / 50, 1.0)
            + 0.15 * 0.0
            + 0.05 * (1.0 if last_sweep_had_decisions else 0.0)
        )
        score = max(0.0, min(1.0, score))

    if score >= 0.80:
        level, color = "Kritiskt", "red"
    elif score >= 0.50:
        level, color = "Trött", "orange"
    elif score >= 0.20:
        level, color = "Sömnig", "yellow"
    else:
        level, color = "Utvilad", "green"

    return {
        "score": round(score, 3),
        "level": level,
        "color": color,
        "signals": {
            "unenriched": unenriched,
            "total_nodes": total,
            "quality_flags": flags_count,
            "dreamer_counter": dreamer_counter,
            "last_sweep_had_decisions": last_sweep_had_decisions,
        },
    }
