"""
VectorService - Central hantering av embeddings och ChromaDB.

Single Source of Truth för vilken modell som används för att
vektorisera text i systemet.

All access MÅSTE gå via vector_scope() context manager för att
garantera att PersistentClient stängs efter varje operation.
ChromaDB är INTE process-safe — se poc/chromadb/STRATEGI.md.
"""

import os
import logging
import threading
import concurrent.futures
from contextlib import contextmanager

# Patcha tqdm för att tysta progress bars från SentenceTransformer/ChromaDB
import tqdm
import tqdm.auto

_orig_tqdm_init = tqdm.tqdm.__init__
def _silent_tqdm_init(self, *args, **kwargs):
    kwargs['disable'] = True
    return _orig_tqdm_init(self, *args, **kwargs)

tqdm.tqdm.__init__ = _silent_tqdm_init
tqdm.auto.tqdm.__init__ = _silent_tqdm_init

import chromadb
from chromadb.utils import embedding_functions
from typing import List, Dict, Any, Optional

LOGGER = logging.getLogger("VectorService")

# --- Embedding function cache ---
# SentenceTransformer tar ~1-2s att ladda. Cachas på modulnivå
# eftersom den inte har filhandtag till ChromaDB.
_embedding_func_cache = {}
_embedding_func_lock = threading.Lock()


def _get_cached_embedding_func(model_name: str):
    """Cachad SentenceTransformer embedding function. Säker att behålla."""
    if model_name not in _embedding_func_cache:
        with _embedding_func_lock:
            if model_name not in _embedding_func_cache:
                _embedding_func_cache[model_name] = (
                    embedding_functions.SentenceTransformerEmbeddingFunction(
                        model_name=model_name
                    )
                )
                LOGGER.info(f"Cached embedding function: {model_name}")
    return _embedding_func_cache[model_name]


class VectorService:
    _instances = {}
    _lock = threading.Lock()

    def __init__(self, config_path: str = None, collection_name: str = "knowledge_base"):
        self.config = self._load_config(config_path)
        # Robust path lookup: Stödjer både 'chroma_db' och 'vector_db'
        paths = self.config.get('paths', {})
        db_path_raw = paths.get('chroma_db') or paths.get('vector_db')

        if not db_path_raw:
             raise KeyError("Config 'paths' saknar 'chroma_db' eller 'vector_db'")

        self.db_path = os.path.expanduser(db_path_raw)
        self.collection_name = collection_name

        # Init Chroma
        os.makedirs(self.db_path, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.db_path)

        # MODEL SELECTION - Läser från ai_engine.models.embedding_model
        model_name = self.config.get('ai_engine', {}).get('models', {}).get(
            'embedding_model',
            "paraphrase-multilingual-MiniLM-L12-v2"  # Fallback om config saknas
        )
        self.model_name = model_name

        try:
            self.embedding_func = _get_cached_embedding_func(model_name)
        except Exception as e:
            LOGGER.error(f"HARDFAIL: Kunde inte ladda embedding-modell {model_name}: {e}")
            raise RuntimeError(f"Kunde inte ladda embedding-modell: {e}") from e

        # Get/Create Collection
        try:
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                embedding_function=self.embedding_func
            )
            LOGGER.info(f"VectorService opened: {self.db_path} ({self.collection_name})")
        except Exception as e:
            LOGGER.error(f"HARDFAIL: Kunde inte ansluta till ChromaDB collection: {e}")
            raise RuntimeError(f"ChromaDB-fel: {e}") from e

    def _load_config(self, path: str = None) -> dict:
        """Ladda konfiguration via central config loader."""
        from services.utils.config_loader import get_config
        return get_config()

    def close(self):
        """Stäng PersistentClient och rensa singleton-cache.
        Använder workaround från ChromaDB issue #5868.
        Rensar även SharedSystemClient-cachen så nästa PersistentClient
        kan öppnas utan 'Could not connect to tenant' error."""
        try:
            if hasattr(self, 'client') and self.client is not None:
                self.client._system.stop()
                # Rensa ChromaDB:s interna singleton-cache (SharedSystemClient)
                # Utan detta hittar nästa PersistentClient den stoppade instansen
                from chromadb.api.shared_system_client import SharedSystemClient
                SharedSystemClient._identifier_to_system.pop(self.db_path, None)
                LOGGER.debug(f"VectorService closed: {self.collection_name}")
        except (OSError, RuntimeError, AttributeError) as e:
            LOGGER.warning(f"VectorService close warning: {e}")
        finally:
            self.client = None
            self.collection = None
            with VectorService._lock:
                VectorService._instances.pop(self.collection_name, None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def upsert(self, id: str, text: str, metadata: Dict[str, Any] = None):
        if not text: return
        self.collection.upsert(ids=[id], documents=[text], metadatas=[metadata or {}])

    def upsert_node(self, node: Dict):
        if not node: return
        node_id = node.get('id')
        name = node.get('properties', {}).get('name', '')
        if not name: return

        parts = [f"Name: {name}", f"Type: {node.get('type')}"]
        props = node.get('properties', {})
        if 'role' in props: parts.append(f"Role: {props['role']}")
        if node.get('aliases'): parts.append(f"Aliases: {', '.join(node['aliases'])}")
        if props.get('node_context'):
            ctx_list = props['node_context']
            if isinstance(ctx_list, list):
                ctx_texts = [c.get('text', '') for c in ctx_list if isinstance(c, dict)]
                if ctx_texts:
                    parts.append(f"Context: {' | '.join(ctx_texts)}")

        full_text = ". ".join(parts)
        self.upsert(id=node_id, text=full_text, metadata={
            "type": node.get('type'),
            "name": name,
            "source": "graph_node"
        })

    def search(self, query_text: str, limit: int = 5, where: Dict = None, timeout: float = None) -> List[Dict]:
        if not query_text: return []

        search_timeout = timeout or self.config.get('search', {}).get('vector_search_timeout', 30)

        def _do_query():
            return self.collection.query(query_texts=[query_text], n_results=limit, where=where)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_do_query)
                results = future.result(timeout=search_timeout)
        except concurrent.futures.TimeoutError:
            LOGGER.error(f"Vektor-sökning timeout efter {search_timeout}s för: {query_text[:50]}")
            return []

        formatted = []
        if not results['ids']: return []

        ids = results['ids'][0]
        distances = results['distances'][0] if results['distances'] else [0.0]*len(ids)
        metadatas = results['metadatas'][0] if results['metadatas'] else [{}]*len(ids)
        documents = results['documents'][0] if results['documents'] else [""]*len(ids)

        for i in range(len(ids)):
            formatted.append({
                "id": ids[i],
                "distance": distances[i],
                "metadata": metadatas[i] or {},
                "document": documents[i] or ""
            })
        return formatted

    def delete(self, id: str):
        self.collection.delete(ids=[id])

    def delete_by_parent(self, parent_id: str) -> int:
        """Delete all chunks with given parent_id (for transcript parts)."""
        results = self.collection.get(where={"parent_id": parent_id})
        if results and results['ids']:
            self.collection.delete(ids=results['ids'])
            return len(results['ids'])
        return 0

    def count(self) -> int:
        return self.collection.count()


# Singleton Factory (behålls för bakåtkompatibilitet med tools)
def get_vector_service(collection_name: str = "knowledge_base"):
    if collection_name not in VectorService._instances:
        with VectorService._lock:
            if collection_name not in VectorService._instances:
                VectorService._instances[collection_name] = VectorService(collection_name=collection_name)
    return VectorService._instances[collection_name]


@contextmanager
def vector_scope(collection_name: str = "knowledge_base",
                 exclusive: bool = True, timeout: float = None):
    """
    Ephemeral VectorDB access: acquire lock, open client, yield, close, release.

    ChromaDB är INTE process-safe. Denna context manager garanterar att
    PersistentClient öppnas och stängs inom ett resource_lock-scope,
    så att bara en process åt gången har filhandtag till VectorDB.

    Args:
        collection_name: ChromaDB collection name
        exclusive: True för skrivoperationer, False för enbart läsning
        timeout: Lock timeout i sekunder (None = vänta oändligt)

    Usage:
        with vector_scope(exclusive=True) as vs:
            vs.upsert(id="abc", text="hello")

        with vector_scope(exclusive=False, timeout=10.0) as vs:
            results = vs.search("hello")
    """
    from services.utils.shared_lock import resource_lock

    with resource_lock("vector", exclusive=exclusive, timeout=timeout):
        # Rensa eventuell stale singleton innan ny klient skapas
        with VectorService._lock:
            VectorService._instances.pop(collection_name, None)

        vs = VectorService(collection_name=collection_name)
        try:
            yield vs
        finally:
            vs.close()
