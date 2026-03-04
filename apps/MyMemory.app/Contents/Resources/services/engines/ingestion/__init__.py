"""
Ingestion Engine package — document processing pipeline.

Re-exports for backward compatibility:
    from services.engines.ingestion import process_document
    from services.engines.ingestion import write_graph, write_lake, write_vector
"""

from services.engines.ingestion.orchestrator import (
    process_document,
    reset_enrichment_counter,
    DocumentHandler,
)
from services.engines.ingestion.writers import (
    write_graph,
    write_lake,
    write_vector,
    clean_before_reingest,
)
from services.engines.ingestion.mcp_extractor import extract_entities_mcp
from services.engines.ingestion.entity_resolver import resolve_entities
from services.engines.ingestion.critic_filter import (
    critic_filter_resolved,
    critic_filter_entities,
)
from services.engines.ingestion.source_profile import (
    get_source_profile,
    apply_source_profile,
)
from services.engines.ingestion.edge_postprocessor import post_process_edges

# Module-level access to mutable shared state (rebuild_graph.py redirects paths here)
from services.engines.ingestion import _shared

# Expose immutable shared state for backward compatibility
from services.engines.ingestion._shared import (
    LAKE_STORE,
    CONFIG,
    PROCESSED_FILES,
    PROCESS_LOCK,
    UUID_SUFFIX_PATTERN,
)

__all__ = [
    'process_document',
    'reset_enrichment_counter',
    'DocumentHandler',
    'write_graph',
    'write_lake',
    'write_vector',
    'clean_before_reingest',
    'extract_entities_mcp',
    'resolve_entities',
    'critic_filter_resolved',
    'critic_filter_entities',
    'get_source_profile',
    'apply_source_profile',
    'post_process_edges',
]
