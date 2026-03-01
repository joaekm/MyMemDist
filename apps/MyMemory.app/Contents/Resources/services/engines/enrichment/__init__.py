"""
Enrichment Engine package — knowledge graph refinement.

Re-exports for backward compatibility:
    from services.engines.enrichment import Enrichment
"""

from services.engines.enrichment.engine import Enrichment, get_schema_validator, ENRICHMENT_CONFIG

__all__ = ['Enrichment', 'get_schema_validator', 'ENRICHMENT_CONFIG']
