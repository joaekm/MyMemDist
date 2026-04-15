"""
VectorService - Central hantering av embeddings och pgvector.

Single Source of Truth för vilken modell som används för att
vektorisera text i systemet.

PostgreSQL + pgvector backend. Ersätter ChromaDB.
All access via vector_scope() context manager.
"""

import json
import logging
import os
import threading
import concurrent.futures
from contextlib import contextmanager
from typing import List, Dict, Any

import psycopg2
import psycopg2.extras

LOGGER = logging.getLogger("VectorService")


def _ensure_hf_cache_writable():
    """Säkerställ skrivbar HF-cache innan sentence_transformers importeras.

    Systemd-kör som www-data har HOME=/var/www (read-only). Sätt HF_HOME
    till en skrivbar path om användaren inte har gjort det. HARDFAIL om
    ingen skrivbar path kan bestämmas.
    """
    if os.environ.get("HF_HOME"):
        return  # Användaren har satt det

    # Läs från config om tillgängligt, annars fallback
    try:
        from services.utils.config_loader import get_config
        config_path = get_config().get('paths', {}).get('hf_cache_dir')
    except (FileNotFoundError, KeyError, TypeError):
        config_path = None

    candidates = [p for p in [
        config_path,
        os.path.join(os.path.expanduser("~"), ".cache", "huggingface"),
    ] if p]
    for path in candidates:
        try:
            os.makedirs(path, exist_ok=True)
            os.environ["HF_HOME"] = path
            LOGGER.info(f"HF_HOME satt till {path}")
            return
        except (OSError, PermissionError):
            continue

    raise RuntimeError(
        "HARDFAIL: Kunde inte hitta skrivbar path för HF_HOME. "
        "Sätt HF_HOME manuellt."
    )


_ensure_hf_cache_writable()

# --- Embedding function cache ---
# SentenceTransformer tar ~1-2s att ladda. Cachas på modulnivå.
_embedding_func_cache = {}
_embedding_func_lock = threading.Lock()


def _get_cached_embedding_func(model_name: str):
    """Cachad SentenceTransformer embedding function."""
    if model_name not in _embedding_func_cache:
        with _embedding_func_lock:
            if model_name not in _embedding_func_cache:
                from sentence_transformers import SentenceTransformer
                _embedding_func_cache[model_name] = SentenceTransformer(model_name)
                LOGGER.info(f"Cached embedding model: {model_name}")
    return _embedding_func_cache[model_name]


def _extract_body(content: str) -> str:
    """Extrahera body från markdown med YAML frontmatter."""
    if content.startswith('---'):
        parts = content.split('---', 2)
        if len(parts) >= 3:
            return parts[2].strip()
    return content


def _extract_chunk_text(meta: dict, doc_content: str) -> str:
    """Extrahera chunk-text från dokumentinnehåll via positioner i metadata.

    Prioritet:
    1. chunk_start/chunk_end — exakta positioner i dokumentets body
    2. Graf-nod — bygg text från metadata (type, name)
    3. Fallback — första 500 tecken av body
    """
    chunk_start = meta.get('chunk_start')
    chunk_end = meta.get('chunk_end')

    if chunk_start is not None and chunk_end is not None and chunk_start >= 0:
        body = _extract_body(doc_content)
        return body[chunk_start:chunk_end]

    if meta.get('source') == 'graph_node':
        name = meta.get('name', '')
        node_type = meta.get('type', '')
        return f"{node_type}: {name}" if name else ''

    if doc_content:
        body = _extract_body(doc_content)
        return body[:500]

    return ''


class VectorService:
    """
    Vektordatabas med PostgreSQL + pgvector backend.

    Schema:
        vectors(id UUID, tenant_id UUID, doc_id UUID, chunk_index INT,
                embedding vector(768), metadata JSONB)
    """

    def __init__(self, conn, tenant_id: str, model_name: str = None):
        """
        Args:
            conn: psycopg2 connection
            tenant_id: UUID-sträng för tenant-isolation
            model_name: Embedding-modell (default: från config)
        """
        self.conn = conn
        self.tenant_id = tenant_id

        if model_name is None:
            from services.utils.config_loader import get_config
            config = get_config()
            model_name = config.get('ai_engine', {}).get('models', {}).get(
                'embedding_model',
                "KBLab/sentence-bert-swedish-cased"
            )

        self.model_name = model_name
        try:
            self.model = _get_cached_embedding_func(model_name)
        except Exception as e:
            LOGGER.error(f"HARDFAIL: Kunde inte ladda embedding-modell {model_name}: {e}")
            raise RuntimeError(f"Kunde inte ladda embedding-modell: {e}") from e

        # Registrera pgvector typer för denna connection
        psycopg2.extras.register_uuid()
        self._register_vector_type()

        LOGGER.debug(f"VectorService opened (tenant={tenant_id}, model={model_name})")

    def _register_vector_type(self):
        """Registrera pgvector vector-typ för psycopg2."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT typname, oid FROM pg_type WHERE typname = 'vector'")
            row = cur.fetchone()
            if row:
                vector_oid = row[1]
                # Adapter: Python list → PostgreSQL vector
                def vector_adapter(v):
                    return psycopg2.extensions.AsIs(f"'[{','.join(str(x) for x in v)}]'::vector")

                # Typecaster: PostgreSQL vector → Python list
                def vector_caster(value, cur):
                    if value is None:
                        return None
                    return [float(x) for x in value.strip('[]').split(',')]

                VECTOR = psycopg2.extensions.new_type((vector_oid,), "VECTOR", vector_caster)
                psycopg2.extensions.register_type(VECTOR, self.conn)
                psycopg2.extensions.register_adapter(list, vector_adapter)

    def _embed(self, text: str) -> list:
        """Generera embedding-vektor för text."""
        return self.model.encode(text).tolist()

    def close(self):
        """Stäng databasanslutningen."""
        try:
            if self.conn and not self.conn.closed:
                self.conn.close()
                LOGGER.debug("VectorService stängd")
        except (OSError, RuntimeError) as e:
            LOGGER.warning(f"VectorService close: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def upsert(self, doc_id: str, chunk_index: int, text: str,
               metadata: Dict[str, Any] = None):
        """Upsert ett chunk för ett dokument.

        Idempotent på (tenant_id, doc_id, chunk_index): befintlig rad raderas
        och ersätts. vectors.id auto-genereras av PG.

        doc_id är UUID till föräldern (documents.id eller nodes.id för
        graf-chunks). chunk_index = sekvens (0 = overview/full-doc, 1..N =
        innehålls-chunks). Tecken-positioner för smart chunking skickas i
        metadata (chunk_start/chunk_end) — id:et bär ingen identitet utöver
        radens PK.
        """
        if not text:
            return

        embedding = self._embed(text)
        meta = metadata or {}
        meta_json = json.dumps(meta, ensure_ascii=False)
        embedding_str = f"[{','.join(str(x) for x in embedding)}]"

        with self.conn.cursor() as cur:
            # Idempotens via DELETE + INSERT (ingen unique constraint på
            # (doc_id, chunk_index) i schemat, så ON CONFLICT är inte
            # tillgängligt).
            cur.execute(
                "DELETE FROM vectors "
                "WHERE tenant_id = %s AND doc_id = %s::uuid AND chunk_index = %s",
                [self.tenant_id, doc_id, chunk_index]
            )
            cur.execute("""
                INSERT INTO vectors (tenant_id, doc_id, chunk_index, embedding, metadata)
                VALUES (%s, %s::uuid, %s, %s::vector, %s::jsonb)
            """, [
                self.tenant_id, doc_id, chunk_index,
                embedding_str, meta_json
            ])
            self.conn.commit()

    def upsert_node(self, node: Dict, edges: list = None):
        """Upsert en graf-nod som vektordokument."""
        if not node:
            return
        node_id = node.get('id')
        name = node.get('properties', {}).get('name', '')
        if not name:
            return

        parts = [f"Name: {name}", f"Type: {node.get('type')}"]
        props = node.get('properties', {})
        if 'role' in props:
            parts.append(f"Role: {props['role']}")
        if node.get('aliases'):
            parts.append(f"Aliases: {', '.join(node['aliases'])}")
        if props.get('context_summary'):
            parts.append(f"Context: {props['context_summary']}")

        if edges:
            rel_parts = []
            for e in edges:
                if e['direction'] == 'out':
                    rel_parts.append(f"{e['edge_type']} → {e['target_name']}")
                else:
                    rel_parts.append(f"{e['edge_type']} ← {e['target_name']}")
            if rel_parts:
                parts.append(f"Relations: {', '.join(rel_parts)}")

        full_text = ". ".join(parts)
        # Graf-noder lagras som en chunk per nod (chunk_index=0) med
        # doc_id = node_id. Detta låter delete_by_parent(node_id) städa.
        self.upsert(
            doc_id=node_id,
            chunk_index=0,
            text=full_text,
            metadata={
                "type": node.get('type'),
                "name": name,
                "source": "graph_node",
            }
        )

    def search(self, query_text: str, limit: int = 5, where: Dict = None,
               timeout: float = None) -> List[Dict]:
        """
        Semantisk sökning med cosine distance. Joinar med documents för chunk-text.

        Args:
            query_text: Sökfras
            limit: Max antal resultat
            where: Metadata-filter (t.ex. {"source": "graph_node"})
            timeout: Timeout i sekunder

        Returns:
            Lista med {id, distance, metadata, document}
        """
        if not query_text:
            return []

        from services.utils.config_loader import get_config
        config = get_config()
        search_timeout = timeout or config.get('search', {}).get('vector_search_timeout', 30)

        embedding = self._embed(query_text)
        embedding_str = f"[{','.join(str(x) for x in embedding)}]"

        # Bygg WHERE-klausul
        conditions = ["v.tenant_id = %s"]
        params = [self.tenant_id]

        if where:
            for key, value in where.items():
                conditions.append(f"v.metadata->>%s = %s")
                params.extend([key, str(value)])

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT v.id, v.embedding <=> %s::vector AS distance,
                   v.metadata, v.doc_id, d.content
            FROM vectors v
            LEFT JOIN documents d ON d.id = v.doc_id AND d.tenant_id = v.tenant_id
            WHERE {where_clause}
            ORDER BY distance
            LIMIT %s
        """
        params = [embedding_str] + params + [limit]

        def _do_query():
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_do_query)
                rows = future.result(timeout=search_timeout)
        except concurrent.futures.TimeoutError:
            LOGGER.error(f"Vektor-sökning timeout efter {search_timeout}s för: {query_text[:50]}")
            return []

        formatted = []
        for row in rows:
            meta = row[2] if isinstance(row[2], dict) else {}
            doc_content = row[4] or ''

            document = _extract_chunk_text(meta, doc_content)

            formatted.append({
                "id": str(row[0]),
                "distance": float(row[1]),
                "metadata": meta,
                "document": document,
            })
        return formatted

    def delete_by_parent(self, parent_id: str) -> int:
        """Radera alla chunks som tillhör ett dokument eller graf-nod.

        Söker på vectors.doc_id-kolumnen. parent_id är antingen
        dokumentets unit_id (för ingesterade dokument) eller graf-nodens
        id (för nod-chunks skrivna av upsert_node).
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM vectors WHERE doc_id = %s::uuid AND tenant_id = %s RETURNING id",
                [parent_id, self.tenant_id]
            )
            count = cur.rowcount
            self.conn.commit()
            return count

    def count(self) -> int:
        """Antal vektorer för denna tenant."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM vectors WHERE tenant_id = %s",
                [self.tenant_id]
            )
            return cur.fetchone()[0]


def _get_pg_dsn() -> str:
    """Hämta PostgreSQL DSN från config."""
    from services.utils.config_loader import get_config
    config = get_config()
    pg = config.get('database', {}).get('postgresql', {})
    if not pg:
        raise ValueError("vector_scope: 'database.postgresql' saknas i config")
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
        raise ValueError("vector_scope: 'database.tenant_id' saknas i config")
    return tid


# Behålls för bakåtkompatibilitet — returnerar VectorService via PG
def get_vector_service(collection_name: str = "knowledge_base"):
    """Skapa en VectorService mot PostgreSQL. OBS: anroparen ansvarar för close()."""
    dsn = _get_pg_dsn()
    tenant_id = _get_tenant_id()
    conn = psycopg2.connect(dsn)
    return VectorService(conn, tenant_id)


@contextmanager
def vector_scope(collection_name: str = "knowledge_base",
                 exclusive: bool = True, timeout: float = None,
                 db_path: str = None):
    """
    PostgreSQL vector access: connect, yield VectorService, close.

    Ingen resource_lock behövs — PostgreSQL hanterar concurrency.

    Args:
        collection_name: Ignorerad (behålls för bakåtkompatibilitet)
        exclusive: True för skrivoperationer (autocommit off),
                   False för läsning (readonly session)
        timeout: Connection timeout i sekunder
        db_path: Ignorerad (behålls för bakåtkompatibilitet)

    Usage:
        with vector_scope(exclusive=True) as vs:
            vs.upsert(doc_id=doc_uuid, chunk_index=0, text="hello")

        with vector_scope(exclusive=False, timeout=10.0) as vs:
            results = vs.search("hello")
    """
    dsn = _get_pg_dsn()
    tenant_id = _get_tenant_id()

    connect_kwargs = {}
    if timeout is not None:
        connect_kwargs['connect_timeout'] = int(timeout)

    conn = psycopg2.connect(dsn, **connect_kwargs)
    try:
        if not exclusive:
            conn.set_session(readonly=True, autocommit=False)
        vs = VectorService(conn, tenant_id)
        try:
            yield vs
        finally:
            vs.conn = None  # Prevent double-close
    finally:
        if not conn.closed:
            conn.close()
