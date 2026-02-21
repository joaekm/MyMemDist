"""
GraphService - DuckDB-based graph database.

Relational graph model with nodes/edges tables.
Replaces KuzuDB (SOLVED-54).
"""

import os
import json
import logging
import threading
import duckdb
from datetime import datetime

# --- LOGGING ---
LOGGER = logging.getLogger('GraphService')


class GraphService:
    """
    Thread-safe grafdatabas med DuckDB backend.

    Schema:
        nodes(id, type, aliases, properties)
        edges(source, target, edge_type, properties)
    """

    def __init__(self, db_path: str, read_only: bool = False):
        """
        Öppna eller skapa en grafdatabas.

        Args:
            db_path: Sökväg till DuckDB-filen
            read_only: Om True, öppna i read-only läge
        """
        self.db_path = db_path
        self.read_only = read_only
        self._lock = threading.RLock()  # RLock allows reentrant locking (e.g. rename_node -> merge_nodes)

        # Skapa mappen om den inte finns
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Öppna anslutning
        if read_only:
            self.conn = duckdb.connect(db_path, read_only=True)
        else:
            self.conn = duckdb.connect(db_path)
            self._init_schema()

        LOGGER.info(f"GraphService öppnad: {db_path} (read_only={read_only})")

    def _init_schema(self):
        """Skapa tabeller om de inte finns."""
        with self._lock:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    aliases TEXT,
                    properties TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS edges (
                    source TEXT NOT NULL,
                    target TEXT NOT NULL,
                    edge_type TEXT NOT NULL,
                    properties TEXT,
                    PRIMARY KEY (source, target, edge_type)
                )
            """)
            # Index för snabbare sökningar
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(type)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target)")

    def close(self):
        """Stäng databasanslutningen."""
        with self._lock:
            if self.conn:
                self.conn.close()
                self.conn = None
                LOGGER.info(f"GraphService stängd: {self.db_path}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # --- NODE OPERATIONS ---

    def get_node(self, node_id: str) -> dict | None:
        """
        Hämta en nod med givet ID.

        Returns:
            dict med {id, type, aliases, properties} eller None
        """
        with self._lock:
            result = self.conn.execute(
                "SELECT id, type, aliases, properties FROM nodes WHERE id = ?",
                [node_id]
            ).fetchone()

        if not result:
            return None

        return {
            "id": result[0],
            "type": result[1],
            "aliases": json.loads(result[2]) if result[2] else [],
            "properties": json.loads(result[3]) if result[3] else {}
        }

    def find_nodes_by_type(self, node_type: str) -> list[dict]:
        """
        Hitta alla noder av en viss typ.

        Args:
            node_type: Nodtyp att söka efter

        Returns:
            Lista med noder
        """
        with self._lock:
            results = self.conn.execute(
                "SELECT id, type, aliases, properties FROM nodes WHERE type = ?",
                [node_type]
            ).fetchall()

        nodes = []
        for row in results:
            nodes.append({
                "id": row[0],
                "type": row[1],
                "aliases": json.loads(row[2]) if row[2] else [],
                "properties": json.loads(row[3]) if row[3] else {}
            })
        return nodes

    def find_nodes_by_alias(self, alias: str) -> list[dict]:
        """
        Hitta noder där alias matchar.

        Söker i aliases-arrayen (JSON).

        Args:
            alias: Alias att söka efter

        Returns:
            Lista med matchande noder
        """
        # DuckDB stöder JSON-funktioner
        with self._lock:
            results = self.conn.execute("""
                SELECT id, type, aliases, properties
                FROM nodes
                WHERE aliases IS NOT NULL
                  AND list_contains(aliases::TEXT[]::TEXT[], ?)
            """, [alias]).fetchall()

        nodes = []
        for row in results:
            nodes.append({
                "id": row[0],
                "type": row[1],
                "aliases": json.loads(row[2]) if row[2] else [],
                "properties": json.loads(row[3]) if row[3] else {}
            })
        return nodes

    def upsert_node(self, id: str, type: str, aliases: list = None, properties: dict = None):
        """
        Skapa eller uppdatera en nod.
        Hanterar merge av properties för att bevara system-metadata.

        Args:
            id: Unikt nod-ID
            type: Nodtyp (Unit, Entity, Concept, Person)
            aliases: Lista med alternativa namn
            properties: Dict med extra egenskaper
        """
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        new_props = properties or {}

        with self._lock:
            # 1. Hämta existerande egenskaper för att bevara systemfält
            existing = self.conn.execute(
                "SELECT properties FROM nodes WHERE id = ?", [id]
            ).fetchone()

            final_props = {}

            if existing:
                # Noden finns - bevara existerande data, skriv över med nytt
                try:
                    current_props = json.loads(existing[0]) if existing[0] else {}
                except json.JSONDecodeError as e:
                    LOGGER.error(f"Corrupt JSON in node {id}: {e}")
                    raise ValueError(f"Corrupt JSON in existing node {id}") from e

                final_props = current_props.copy()

                # Append-merge för list-properties (node_context, keywords etc.)
                # istället för att skriva över med dict.update()
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
                # Ny nod - Initiera alla required systemfält enligt schema
                now_ts = datetime.now().isoformat()
                defaults = {
                    "created_at": now_ts,
                    "last_synced_at": now_ts,
                    "last_seen_at": now_ts,
                    "last_retrieved_at": now_ts,
                    "retrieved_times": 0,
                    "last_refined_at": "never",
                    "status": "PROVISIONAL",
                    "confidence": 0.5
                }
                final_props = defaults
                final_props.update(new_props)

            aliases_json = json.dumps(aliases or [], ensure_ascii=False)
            properties_json = json.dumps(final_props, ensure_ascii=False)

            # 2. Skriv till DB (UPSERT)
            self.conn.execute("""
                INSERT INTO nodes (id, type, aliases, properties)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    type = EXCLUDED.type,
                    aliases = EXCLUDED.aliases,
                    properties = EXCLUDED.properties
            """, [id, type, aliases_json, properties_json])

    def register_usage(self, node_ids: list):
        """
        Registrera att noder har använts i ett svar (Relevans).
        Ökar retrieved_times och sätter last_retrieved_at till nu.
        """
        if not node_ids: return

        now_ts = datetime.now().isoformat()

        with self._lock:
            # Batch-uppdatering via Read-Modify-Write för säkerhet
            placeholders = ','.join(['?'] * len(node_ids))
            rows = self.conn.execute(
                f"SELECT id, properties FROM nodes WHERE id IN ({placeholders})",
                node_ids
            ).fetchall()

            for r in rows:
                nid = r[0]
                try:
                    props = json.loads(r[1]) if r[1] else {}
                except json.JSONDecodeError as e:
                    LOGGER.error(f"Corrupt JSON in node {nid}: {e}")
                    raise ValueError(f"Corrupt JSON in node {nid}") from e

                # Uppdatera räknare
                count = props.get('retrieved_times', 0)
                if not isinstance(count, int): count = 0

                props['retrieved_times'] = count + 1
                props['last_retrieved_at'] = now_ts

                # Skriv tillbaka
                self.conn.execute(
                    "UPDATE nodes SET properties = ? WHERE id = ?",
                    [json.dumps(props, ensure_ascii=False), nid]
                )

        LOGGER.info(f"Registered usage for {len(node_ids)} nodes")

    def get_refinement_candidates(self, limit: int = 50) -> list[dict]:
        """
        Hämta kandidater för Dreamer-underhåll enligt 80/20-principen.

        - 80% Relevans: Heta noder (nyligen använda).
        - 20% Underhåll: Glömda noder (aldrig städade eller gamla).
        """
        relevance_limit = int(limit * 0.8)
        maintenance_limit = limit - relevance_limit

        candidates = []

        with self._lock:
            # 1. Relevans (Heta noder) - Sortera på last_retrieved_at DESC
            rel_rows = self.conn.execute(f"""
                SELECT id, type, aliases, properties
                FROM nodes
                ORDER BY json_extract_string(properties, '$.last_retrieved_at') DESC
                LIMIT ?
            """, [relevance_limit]).fetchall()

            # 2. Underhåll (Glömda noder)
            # Prioritera 'never' (ostädade) först, sedan äldsta datum
            maint_rows = self.conn.execute(f"""
                SELECT id, type, aliases, properties
                FROM nodes
                ORDER BY
                    CASE WHEN json_extract_string(properties, '$.last_refined_at') = 'never' THEN 0 ELSE 1 END,
                    json_extract_string(properties, '$.last_refined_at') ASC
                LIMIT ?
            """, [maintenance_limit]).fetchall()

            # Slå ihop och deduplicera
            seen_ids = set()
            for r in rel_rows + maint_rows:
                if r[0] not in seen_ids:
                    candidates.append({
                        "id": r[0],
                        "type": r[1],
                        "aliases": json.loads(r[2]) if r[2] else [],
                        "properties": json.loads(r[3]) if r[3] else {}
                    })
                    seen_ids.add(r[0])

        return candidates

    def delete_node(self, node_id: str) -> bool:
        """
        Ta bort en nod och alla dess kanter.

        Args:
            node_id: ID på noden att ta bort

        Returns:
            True om noden fanns och togs bort
        """
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        with self._lock:
            # Ta bort kanter först
            self.conn.execute(
                "DELETE FROM edges WHERE source = ? OR target = ?",
                [node_id, node_id]
            )
            # Ta bort noden
            result = self.conn.execute(
                "DELETE FROM nodes WHERE id = ? RETURNING id",
                [node_id]
            ).fetchone()

            return result is not None

    def update_node_properties(self, node_id: str, properties: dict):
        """
        Direkt överskrivning av properties (inte merge som upsert_node).
        Används vid cleanup, t.ex. borttag av enskilda node_context-entries.

        Args:
            node_id: ID på noden att uppdatera
            properties: Nya properties (ersätter befintliga helt)
        """
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        properties_json = json.dumps(properties, ensure_ascii=False)
        with self._lock:
            self.conn.execute(
                "UPDATE nodes SET properties = ? WHERE id = ?",
                [properties_json, node_id]
            )

    @staticmethod
    def _fold_diacritics(s: str) -> str:
        """Fold diacritics: Ohlén → ohlen, Björkengren → bjorkengren."""
        import unicodedata
        return unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode().lower()

    def find_node_by_name(self, node_type: str, name: str, fuzzy: bool = True) -> str | None:
        """
        Sök efter en nod baserat på namn (exakt eller fuzzy).
        Använder diakritik-normalisering för att matcha Ohlén/Ohlen etc.

        Args:
            node_type: Nodtyp att söka i (Person, Organization, etc.)
            name: Namnet att söka efter
            fuzzy: Om True, använd fuzzy matching (difflib, 85% likhet)

        Returns:
            UUID om matchning hittas, annars None
        """
        import difflib

        name_lower = name.strip().lower()
        name_folded = self._fold_diacritics(name)

        with self._lock:
            # Hämta alla noder av typen
            rows = self.conn.execute("""
                SELECT id, properties FROM nodes WHERE type = ?
            """, [node_type]).fetchall()

        # Bygg namn-index (original lowercase) och folded-index (diakritik-normaliserad)
        name_to_uuid: dict[str, list[str]] = {}
        folded_to_uuid: dict[str, list[str]] = {}
        for node_id, props_raw in rows:
            props = json.loads(props_raw) if props_raw else {}
            node_name = (props.get('name') or 'Unknown').strip().lower()
            if node_name:
                if node_name not in name_to_uuid:
                    name_to_uuid[node_name] = []
                name_to_uuid[node_name].append(node_id)

                node_folded = self._fold_diacritics(node_name)
                if node_folded not in folded_to_uuid:
                    folded_to_uuid[node_folded] = []
                folded_to_uuid[node_folded].append(node_id)

            # Kolla även aliases
            aliases_raw = props.get('aliases', [])
            if aliases_raw:
                for alias in aliases_raw:
                    alias_lower = alias.strip().lower()
                    if alias_lower not in name_to_uuid:
                        name_to_uuid[alias_lower] = []
                    name_to_uuid[alias_lower].append(node_id)

                    alias_folded = self._fold_diacritics(alias)
                    if alias_folded not in folded_to_uuid:
                        folded_to_uuid[alias_folded] = []
                    folded_to_uuid[alias_folded].append(node_id)

        # 1. Exakt matchning (original)
        if name_lower in name_to_uuid:
            hits = name_to_uuid[name_lower]
            if len(hits) == 1:
                return hits[0]
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

        # 3. Fuzzy matchning (diakritik-normaliserad)
        if fuzzy:
            candidates = list(folded_to_uuid.keys())
            matches = difflib.get_close_matches(name_folded, candidates, n=1, cutoff=0.85)
            if matches:
                matched_name = matches[0]
                hits = folded_to_uuid[matched_name]
                LOGGER.info(f"find_node_by_name: Fuzzy '{name}' ~= '{matched_name}' -> {hits[0]}")
                return hits[0]

        return None

    # --- EDGE OPERATIONS ---

    def get_edges_from(self, node_id: str) -> list[dict]:
        """
        Hämta alla utgående kanter från en nod.

        Returns:
            Lista med {source, target, type, properties}
        """
        with self._lock:
            results = self.conn.execute(
                "SELECT source, target, edge_type, properties FROM edges WHERE source = ?",
                [node_id]
            ).fetchall()

        edges = []
        for row in results:
            edges.append({
                "source": row[0],
                "target": row[1],
                "type": row[2],
                "properties": json.loads(row[3]) if row[3] else {}
            })
        return edges

    def get_edges_to(self, node_id: str) -> list[dict]:
        """
        Hämta alla inkommande kanter till en nod.

        Returns:
            Lista med {source, target, type, properties}
        """
        with self._lock:
            results = self.conn.execute(
                "SELECT source, target, edge_type, properties FROM edges WHERE target = ?",
                [node_id]
            ).fetchall()

        edges = []
        for row in results:
            edges.append({
                "source": row[0],
                "target": row[1],
                "type": row[2],
                "properties": json.loads(row[3]) if row[3] else {}
            })
        return edges

    def upsert_edge(self, source: str, target: str, edge_type: str, properties: dict = None):
        """
        Skapa eller uppdatera en kant.

        Args:
            source: Käll-nod ID
            target: Mål-nod ID
            edge_type: Typ av relation (DEALS_WITH, CREATED_BY, etc.)
            properties: Extra egenskaper
        """
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        properties_json = json.dumps(properties or {}, ensure_ascii=False)

        with self._lock:
            self.conn.execute("""
                INSERT INTO edges (source, target, edge_type, properties)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (source, target, edge_type) DO UPDATE SET
                    properties = EXCLUDED.properties
            """, [source, target, edge_type, properties_json])

    def delete_edge(self, source: str, target: str, edge_type: str) -> bool:
        """
        Ta bort en specifik kant.

        Returns:
            True om kanten fanns och togs bort
        """
        if self.read_only:
            raise RuntimeError("HARDFAIL: Försöker skriva i read_only mode")

        with self._lock:
            result = self.conn.execute(
                "DELETE FROM edges WHERE source = ? AND target = ? AND edge_type = ? RETURNING source",
                [source, target, edge_type]
            ).fetchone()

            return result is not None

    # --- STATISTICS ---

    def get_stats(self) -> dict:
        """
        Hämta statistik om grafen.

        Returns:
            dict med total_nodes, total_edges, nodes per typ, edges per typ
        """
        with self._lock:
            # Räkna noder per typ
            node_counts = self.conn.execute(
                "SELECT type, COUNT(*) FROM nodes GROUP BY type"
            ).fetchall()

            # Räkna kanter per typ
            edge_counts = self.conn.execute(
                "SELECT edge_type, COUNT(*) FROM edges GROUP BY edge_type"
            ).fetchall()

        nodes_dict = {row[0]: row[1] for row in node_counts}
        edges_dict = {row[0]: row[1] for row in edge_counts}

        return {
            "total_nodes": sum(nodes_dict.values()),
            "total_edges": sum(edges_dict.values()),
            "nodes": nodes_dict,
            "edges": edges_dict
        }

    # --- SEARCH HELPERS ---

    def find_nodes_fuzzy(self, term: str, limit: int = 10) -> list[dict]:
        """
        Fuzzy-sök efter noder baserat på ID eller alias.

        Args:
            term: Sökterm
            limit: Max antal resultat

        Returns:
            Lista med matchande noder
        """
        # Sök i id och aliases
        with self._lock:
            results = self.conn.execute("""
                SELECT id, type, aliases, properties
                FROM nodes
                WHERE id ILIKE ?
                   OR (aliases IS NOT NULL AND aliases ILIKE ?)
                LIMIT ?
            """, [f"%{term}%", f"%{term}%", limit]).fetchall()

        nodes = []
        for row in results:
            nodes.append({
                "id": row[0],
                "type": row[1],
                "aliases": json.loads(row[2]) if row[2] else [],
                "properties": json.loads(row[3]) if row[3] else {}
            })
        return nodes

    def get_related_units(self, entity_id: str, limit: int = 10) -> list[str]:
        """
        Hitta alla Units som nämner en viss Entity.

        Args:
            entity_id: Entity-nodens ID
            limit: Max antal resultat

        Returns:
            Lista med Unit-IDs
        """
        with self._lock:
            results = self.conn.execute("""
                SELECT DISTINCT source
                FROM edges
                WHERE target = ? AND edge_type = 'UNIT_MENTIONS'
                LIMIT ?
            """, [entity_id, limit]).fetchall()

        return [row[0] for row in results]

    # --- DREAMER SUPPORT ---

    def add_pending_review(self, entity: str, master_node: str, score: float, reason: str, context: dict):
        """
        Lägg till en manuell granskning (för Dreamer).
        """
        import uuid

        review_id = str(uuid.uuid4())
        context_json = json.dumps(context, ensure_ascii=False)

        with self._lock:
            # Skapa tabellen om den saknas
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_reviews (
                    id TEXT PRIMARY KEY,
                    entity TEXT,
                    master_node TEXT,
                    score FLOAT,
                    reason TEXT,
                    context TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            self.conn.execute("""
                INSERT INTO pending_reviews (id, entity, master_node, score, reason, context)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [review_id, entity, master_node, score, reason, context_json])

            LOGGER.info(f"Saved pending review: {entity} vs {master_node} ({score})")

    def merge_nodes(self, target_id: str, source_id: str):
        """
        Slå ihop source_id in i target_id (ROBUST & ATOMÄR).

        Process:
        1. Aggregera properties (hanterar listor och node_context korrekt).
        2. Flytta alla relationer.
        3. Flytta alias.
        4. Radera källnoden.
        """
        if self.read_only:
            raise RuntimeError("HARDFAIL: Read-only mode")

        with self._lock:
            # 1. HÄMTA DATA
            res_target = self.conn.execute("SELECT properties FROM nodes WHERE id = ?", [target_id]).fetchone()
            res_source = self.conn.execute("SELECT properties FROM nodes WHERE id = ?", [source_id]).fetchone()

            if not res_target or not res_source:
                LOGGER.warning(f"Merge aborted: Node missing ({target_id} or {source_id})")
                return

            try:
                props_t = json.loads(res_target[0]) if res_target[0] else {}
                props_s = json.loads(res_source[0]) if res_source[0] else {}
            except json.JSONDecodeError as e:
                LOGGER.error(f"JSON decode error during merge: {e}")
                raise ValueError(f"Corrupt JSON in nodes during merge: {e}") from e

            # 2. AGGREGERA PROPERTIES
            merged_props = props_t.copy()

            for k, v in props_s.items():
                # Om det är en lista (t.ex. keywords, evidence, node_context)
                if isinstance(v, list) and k in merged_props and isinstance(merged_props[k], list):
                    combined = merged_props[k] + v

                    # SPECIALHANTERING: List of Dicts (t.ex. node_context)
                    if combined and isinstance(combined[0], dict):
                        # Deduplicera baserat på innehåll genom serialisering
                        seen = set()
                        unique_list = []
                        for item in combined:
                            # Sortera keys för konsekvent hashning
                            # Skapar en hashbar representation av dictet
                            item_key = tuple(sorted((ik, str(iv)) for ik, iv in item.items()))
                            if item_key not in seen:
                                seen.add(item_key)
                                unique_list.append(item)
                        merged_props[k] = unique_list

                    # STANDARD: List of Strings/Ints
                    else:
                        merged_props[k] = list(set(combined))

                # Om skalärt värde saknas i target, kopiera från source
                elif k not in merged_props:
                    merged_props[k] = v

            # SPARA TARGET (Innan vi flyttar kanter)
            self.conn.execute("UPDATE nodes SET properties = ? WHERE id = ?",
                            [json.dumps(merged_props, ensure_ascii=False), target_id])

            # 3. FLYTTA UTGÅENDE KANTER (source -> X) till (target -> X)
            self.conn.execute("""
                UPDATE edges
                SET source = ?
                WHERE source = ?
                AND NOT EXISTS (
                    SELECT 1 FROM edges e2
                    WHERE e2.source = ? AND e2.target = edges.target AND e2.edge_type = edges.edge_type
                )
            """, [target_id, source_id, target_id])

            # 4. FLYTTA INKOMMANDE KANTER (X -> source) till (X -> target)
            self.conn.execute("""
                UPDATE edges
                SET target = ?
                WHERE target = ?
                AND NOT EXISTS (
                    SELECT 1 FROM edges e2
                    WHERE e2.source = edges.source AND e2.target = ? AND e2.edge_type = edges.edge_type
                )
            """, [target_id, source_id, target_id])

            # 5. STÄDA KANTER (Ta bort dubbletter som uppstod vid flytt eller self-loops)
            self.conn.execute("DELETE FROM edges WHERE source = ? OR target = ?", [source_id, source_id])
            # Ta bort self-loops på target om de skapades
            self.conn.execute("DELETE FROM edges WHERE source = ? AND target = ?", [target_id, target_id])

            # 6. FLYTTA ALIASES
            res_source_a = self.conn.execute("SELECT aliases FROM nodes WHERE id = ?", [source_id]).fetchone()
            aliases_s = json.loads(res_source_a[0]) if res_source_a and res_source_a[0] else []

            res_target_a = self.conn.execute("SELECT aliases FROM nodes WHERE id = ?", [target_id]).fetchone()
            aliases_t = json.loads(res_target_a[0]) if res_target_a and res_target_a[0] else []

            # Gamla IDt blir ett alias
            new_aliases = list(set(aliases_t + aliases_s + [source_id]))

            self.conn.execute("UPDATE nodes SET aliases = ? WHERE id = ?",
                            [json.dumps(new_aliases, ensure_ascii=False), target_id])

            # 7. RADERA SOURCE
            self.conn.execute("DELETE FROM nodes WHERE id = ?", [source_id])

            LOGGER.info(f"Merged {source_id} into {target_id} (Data aggregated)")

    def rename_node(self, old_id: str, new_name: str):
        """
        Byt namn på en nod.
        Uppdaterar properties.name och lägger till gamla namnet som alias.
        Behåller UUID som ID (kritiskt för grafintegritet).
        """
        if self.read_only: raise RuntimeError("HARDFAIL: Read-only")

        with self._lock:
            # Kolla om det finns en annan nod med det nya namnet -> merge istället
            existing = self.conn.execute(
                "SELECT id FROM nodes WHERE json_extract_string(properties, '$.name') = ? AND id != ?",
                [new_name, old_id]
            ).fetchone()

            if existing:
                LOGGER.info(f"Rename: Node with name '{new_name}' exists ({existing[0]}). Merging instead.")
                self.merge_nodes(existing[0], old_id)
                return

            # Hämta nuvarande data
            res = self.conn.execute("SELECT type, aliases, properties FROM nodes WHERE id = ?", [old_id]).fetchone()
            if not res:
                LOGGER.warning(f"Rename failed: Source {old_id} not found")
                return

            try:
                aliases = json.loads(res[1]) if res[1] else []
            except json.JSONDecodeError:
                LOGGER.warning(f"Rename: Invalid aliases JSON for {old_id}")
                aliases = []
            try:
                props = json.loads(res[2]) if res[2] else {}
            except json.JSONDecodeError:
                LOGGER.warning(f"Rename: Invalid properties JSON for {old_id}")
                props = {}

            old_name = props.get("name", "")

            # Lägg till gamla namnet som alias
            if old_name and old_name != new_name and old_name not in aliases:
                aliases.append(old_name)

            # Uppdatera properties.name
            props["name"] = new_name

            # Spara tillbaka (behåll samma UUID som ID!)
            self.conn.execute(
                "UPDATE nodes SET aliases = ?, properties = ? WHERE id = ?",
                [json.dumps(aliases, ensure_ascii=False), json.dumps(props, ensure_ascii=False), old_id]
            )

            LOGGER.info(f"Renamed node {old_id}: '{old_name}' -> '{new_name}'")

    def split_node(self, original_id: str, split_map: list):
        """
        Dela upp en nod i flera nya noder.
        Genererar UUID för varje ny nod och sparar namnet i properties.name.

        Args:
            original_id: ID på noden som ska splittas.
            split_map: Lista av dicts:
                       [{ "name": "Nytt_Namn_1", "context_indices": [0, 2] }, ...]

        Returns:
            Lista med skapade node IDs (UUID:n)
        """
        import uuid as uuid_module

        if self.read_only: raise RuntimeError("HARDFAIL: Read-only")

        with self._lock:
            # 1. Hämta originaldata
            res = self.conn.execute("SELECT type, properties FROM nodes WHERE id = ?", [original_id]).fetchone()
            if not res:
                LOGGER.warning(f"Split failed: Node {original_id} not found")
                return []

            orig_type = res[0]
            try:
                orig_props = json.loads(res[1]) if res[1] else {}
            except json.JSONDecodeError as e:
                LOGGER.error(f"Corrupt JSON in node {original_id}: {e}")
                raise ValueError(f"Corrupt JSON in node {original_id}") from e

            node_context = orig_props.get("node_context", [])

            # 2. Skapa nya noder med UUID
            created_nodes = []
            for item in split_map:
                new_name = item.get("name")
                indices = item.get("context_indices", [])

                if not new_name: continue

                # Generera UUID för den nya noden
                new_node_id = str(uuid_module.uuid4())

                # Bygg properties för den nya noden
                new_props = orig_props.copy()
                new_props["name"] = new_name  # Spara namnet i properties

                # Filtrera node_context baserat på index
                if node_context:
                    specific_context = [ctx for i, ctx in enumerate(node_context) if i in indices]
                    new_props["node_context"] = specific_context

                props_json = json.dumps(new_props, ensure_ascii=False)

                # Skapa noden med UUID som ID
                self.conn.execute("INSERT INTO nodes (id, type, aliases, properties) VALUES (?, ?, '[]', ?)",
                                [new_node_id, orig_type, props_json])

                LOGGER.info(f"Split: Created node '{new_name}' with ID {new_node_id}")
                created_nodes.append(new_node_id)

            # 3. Kopiera relationer (Brute force copy)
            # Eftersom vi inte vet vilken relation som hör till vilket kluster,
            # kopierar vi ALLA relationer till ALLA nya noder.
            # Dreamer får städa detta i framtida cykler (relevans-städning).

            # Utgående
            out_edges = self.conn.execute("SELECT target, edge_type, properties FROM edges WHERE source = ?", [original_id]).fetchall()
            for new_node in created_nodes:
                for target, etype, props in out_edges:
                    # Undvik self-loops om nya noden råkar vara target
                    if target == new_node: continue
                    self.conn.execute("INSERT OR IGNORE INTO edges (source, target, edge_type, properties) VALUES (?, ?, ?, ?)",
                                    [new_node, target, etype, props])

            # Inkommande
            in_edges = self.conn.execute("SELECT source, edge_type, properties FROM edges WHERE target = ?", [original_id]).fetchall()
            for new_node in created_nodes:
                for source, etype, props in in_edges:
                    if source == new_node: continue
                    self.conn.execute("INSERT OR IGNORE INTO edges (source, target, edge_type, properties) VALUES (?, ?, ?, ?)",
                                    [source, new_node, etype, props])

            # 4. Radera originalnoden
            # Detta tar också bort dess kanter via Cascade (om implementerat) eller manuell delete
            self.conn.execute("DELETE FROM edges WHERE source = ? OR target = ?", [original_id, original_id])
            self.conn.execute("DELETE FROM nodes WHERE id = ?", [original_id])

            LOGGER.info(f"Split {original_id} into {len(created_nodes)} nodes: {created_nodes}")
            return created_nodes

    def recategorize_node(self, node_id: str, new_type: str):
        """
        Byt typ på en nod (Re-categorize).
        """
        if self.read_only: raise RuntimeError("HARDFAIL: Read-only")

        with self._lock:
            # Kontrollera att noden finns
            exists = self.conn.execute("SELECT 1 FROM nodes WHERE id = ?", [node_id]).fetchone()
            if not exists:
                LOGGER.warning(f"Recategorize failed: Node {node_id} not found")
                return

            self.conn.execute("UPDATE nodes SET type = ? WHERE id = ?", [new_type, node_id])
            LOGGER.info(f"Recategorized {node_id} -> {new_type}")

    def get_node_degree(self, node_id: str) -> int:
        """Returnerar antal unika relationer (exklusive inkommande från Unit-noder)."""
        with self._lock:
            # Vi räknar kopplingar mot andra entiteter/koncept för att mäta 'viktighet'
            res = self.conn.execute("""
                SELECT count(*) FROM edges
                WHERE (source = ? OR target = ?)
                AND edge_type NOT IN ('UNIT_MENTIONS', 'DEALS_WITH')
            """, [node_id, node_id]).fetchone()
            return res[0] if res else 0

    def get_related_unit_ids(self, node_id: str) -> list:
        """Hämtar alla Unit-IDs (filer) som refererar till denna nod."""
        with self._lock:
            rows = self.conn.execute("""
                SELECT DISTINCT source FROM edges
                WHERE target = ? AND edge_type IN ('UNIT_MENTIONS', 'DEALS_WITH')
            """, [node_id]).fetchall()
            return [r[0] for r in rows]
