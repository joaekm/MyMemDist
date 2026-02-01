"""
VectorService - Central hantering av embeddings och ChromaDB.

Single Source of Truth för vilken modell som används för att
vektorisera text i systemet.
Implementerar Multiton-mönster för att hantera olika collections.
"""

import os
import yaml
import logging
import threading

# --- NY KOD START ---
# Patcha tqdm för att tysta progress bars från SentenceTransformer/ChromaDB
import tqdm
import tqdm.auto

_orig_tqdm_init = tqdm.tqdm.__init__
def _silent_tqdm_init(self, *args, **kwargs):
    kwargs['disable'] = True
    return _orig_tqdm_init(self, *args, **kwargs)

tqdm.tqdm.__init__ = _silent_tqdm_init
tqdm.auto.tqdm.__init__ = _silent_tqdm_init
# --- NY KOD SLUT ---

import chromadb
from chromadb.utils import embedding_functions
from typing import List, Dict, Any, Optional

LOGGER = logging.getLogger("VectorService")

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
        LOGGER.info(f"Using embedding model: {model_name}")
        self.model_name = model_name
        
        try:
            self.embedding_func = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=model_name)
        except Exception as e:
            LOGGER.error(f"HARDFAIL: Kunde inte ladda embedding-modell {model_name}: {e}")
            raise RuntimeError(f"Kunde inte ladda embedding-modell: {e}") from e
        
        # Get/Create Collection
        try:
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name, 
                embedding_function=self.embedding_func
            )
            LOGGER.info(f"VectorService initialized at {self.db_path} (Collection: {self.collection_name})")
        except Exception as e:
            LOGGER.error(f"HARDFAIL: Kunde inte ansluta till ChromaDB collection: {e}")
            raise RuntimeError(f"ChromaDB-fel: {e}") from e

    def _load_config(self, path: str = None) -> dict:
        if not path:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            paths = [
                os.path.join(base_dir, '..', '..', 'config', 'my_mem_config.yaml'),
                os.path.join(base_dir, '..', 'config', 'my_mem_config.yaml'),
                os.path.join(base_dir, 'config', 'my_mem_config.yaml'),
            ]
            for p in paths:
                if os.path.exists(p):
                    path = p
                    break
        
        if not path or not os.path.exists(path):
            raise FileNotFoundError("HARDFAIL: Config not found")

        with open(path, "r") as f:
            return yaml.safe_load(f)

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

    def search(self, query_text: str, limit: int = 5, where: Dict = None) -> List[Dict]:
        if not query_text: return []
        results = self.collection.query(query_texts=[query_text], n_results=limit, where=where)
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
                "metadata": metadatas[i],
                "document": documents[i]
            })
        return formatted

    def delete(self, id: str):
        self.collection.delete(ids=[id])

    def count(self) -> int:
        return self.collection.count()

# Singleton Factory
def get_vector_service(collection_name: str = "knowledge_base"):
    if collection_name not in VectorService._instances:
        with VectorService._lock:
            if collection_name not in VectorService._instances:
                VectorService._instances[collection_name] = VectorService(collection_name=collection_name)
    return VectorService._instances[collection_name]