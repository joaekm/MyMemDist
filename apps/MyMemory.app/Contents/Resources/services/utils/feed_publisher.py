"""
feed_publisher.py — Genererar smoketrail-feed från en MyMemory-instans.

Producerar k-means centroids av alla embeddings — anonyma tematiska
fingerprints utan klartext. Brokern jämför centroids mellan peers för
att hitta överlapp, och gräver sedan djupare via MCP on-demand.

Använder vector_scope() för all ChromaDB-åtkomst (regel 17).
"""

import logging
import time
from datetime import datetime, timezone

import numpy as np

LOGGER = logging.getLogger("FeedPublisher")


def build_feed(peer_id: str, vector_db_path: str, n_clusters: int = 12) -> dict:
    """Build smoketrail feed for this peer.

    Args:
        peer_id: Identifier for this peer (e.g. owner.id from config)
        vector_db_path: Path to VectorDB directory
        n_clusters: Number of k-means clusters for smoketrail

    Returns:
        Feed dict with smoketrail centroids and metadata
    """
    smoketrail = _build_smoketrail(vector_db_path, n_clusters)

    feed = {
        "peer_id": peer_id,
        "published_at": datetime.now(timezone.utc).isoformat(),
        "smoketrail": smoketrail,
        "total_embeddings": smoketrail[0]["total_embeddings"] if smoketrail else 0,
        "clusters": len(smoketrail),
    }

    LOGGER.info(f"Feed built: {len(smoketrail)} clusters")
    return feed


def _build_smoketrail(vector_db_path: str, n_clusters: int = 12) -> list:
    """Generate smoketrail via k-means clustering of all embeddings.

    Uses vector_scope() for ChromaDB access (rule 17).
    Each cluster centroid becomes an anonymous vector representing
    a thematic gravity zone. No cleartext, no IDs.
    """
    from sklearn.cluster import KMeans
    from services.utils.vector_service import vector_scope

    try:
        with vector_scope(exclusive=False, timeout=30.0,
                          db_path=vector_db_path) as vs:
            total = vs.count()
            if total < 10:
                LOGGER.info(f"Too few embeddings ({total}) for clustering")
                return []

            all_data = vs.collection.get(include=["embeddings"])
    except Exception as e:
        LOGGER.error(f"Failed to extract embeddings: {e}")
        raise

    embeddings = np.array(all_data["embeddings"])
    LOGGER.info(f"Clustering {embeddings.shape[0]} embeddings")

    k = min(n_clusters, len(embeddings) // 5)
    k = max(k, 3)

    t0 = time.monotonic()
    kmeans = KMeans(n_clusters=k, n_init=10, random_state=42)
    kmeans.fit(embeddings)
    elapsed = time.monotonic() - t0
    LOGGER.info(f"K-means completed: k={k}, {elapsed:.1f}s")

    smoketrail = []
    labels = kmeans.labels_
    for i in range(k):
        cluster_mask = labels == i
        cluster_size = int(cluster_mask.sum())

        centroid = kmeans.cluster_centers_[i]
        norm = np.linalg.norm(centroid)
        if norm > 0:
            centroid = centroid / norm

        member_embeddings = embeddings[cluster_mask]
        distances = np.linalg.norm(
            member_embeddings - kmeans.cluster_centers_[i], axis=1
        )
        avg_distance = float(np.mean(distances))

        smoketrail.append({
            "embedding": centroid.tolist(),
            "cluster_size": cluster_size,
            "density": round(1.0 / (1.0 + avg_distance), 3),
            "total_embeddings": len(embeddings),
        })

    smoketrail.sort(key=lambda x: x["cluster_size"], reverse=True)
    return smoketrail
