"""
screener/lib/storage/firestore.py — Async Firestore implementation of StorageDAO.

Uses ``google-cloud-firestore``'s AsyncClient so all I/O is non-blocking and
plays nicely with asyncio-based LangGraph agents.

Vector search uses Firestore's native ``find_nearest`` API (GA since
google-cloud-firestore>=2.16).  The embedding field name and distance measure
follow the same conventions as the private reference project.

Usage::

    from screener.lib.storage.firestore import FirestoreDAO

    dao = FirestoreDAO(project_id="my-gcp-project", database="my-db")
    doc = await dao.get("tickers", "AAPL")
    await dao.set("tickers", "AAPL", {"symbol": "AAPL", "score": 72.4})
    await dao.close()
"""

from __future__ import annotations

import logging
from typing import Any

from google.api_core.exceptions import NotFound
from google.cloud import firestore
from google.cloud.firestore_v1.async_client import AsyncClient
from google.cloud.firestore_v1.base_vector_query import DistanceMeasure
from google.cloud.firestore_v1.vector import Vector

from screener.lib.storage.base import StorageDAO

logger = logging.getLogger(__name__)

# Field name that holds the dense embedding vector inside chunk documents.
_EMBEDDING_FIELD = "embedding"


class FirestoreDAO(StorageDAO):
    """Async Firestore implementation of :class:`StorageDAO`.

    Args:
        project_id: GCP project ID.
        database: Firestore named database ID (e.g. ``"multi-agent-stock-screener"``).
                  Defaults to ``"(default)"`` if not provided.
    """

    def __init__(self, project_id: str, database: str = "(default)") -> None:
        self._project_id = project_id
        self._database = database
        self._client: AsyncClient = firestore.AsyncClient(
            project=project_id,
            database=database,
        )
        self._closed = False
        logger.info(
            "FirestoreDAO initialised",
            extra={"project": project_id, "database": database},
        )

    # ------------------------------------------------------------------
    # StorageDAO interface
    # ------------------------------------------------------------------

    async def get(self, collection: str, doc_id: str) -> dict | None:
        """Fetch a single document by ID.

        Returns ``None`` if the document does not exist.
        """
        doc_ref = self._client.collection(collection).document(doc_id)
        snap = await doc_ref.get()
        if snap.exists:
            logger.debug("get hit", extra={"collection": collection, "doc_id": doc_id})
            return snap.to_dict()
        logger.debug("get miss", extra={"collection": collection, "doc_id": doc_id})
        return None

    async def set(self, collection: str, doc_id: str, data: dict) -> None:
        """Upsert a document.  Overwrites any existing document with the same ID."""
        doc_ref = self._client.collection(collection).document(doc_id)
        await doc_ref.set(data)
        logger.debug("set ok", extra={"collection": collection, "doc_id": doc_id})

    async def delete(self, collection: str, doc_id: str) -> None:
        """Delete a document.  No-op if the document does not exist."""
        doc_ref = self._client.collection(collection).document(doc_id)
        try:
            await doc_ref.delete()
            logger.debug(
                "delete ok", extra={"collection": collection, "doc_id": doc_id}
            )
        except NotFound:
            # Idempotent — a missing document is not an error.
            logger.debug(
                "delete no-op (not found)",
                extra={"collection": collection, "doc_id": doc_id},
            )

    async def query(self, collection: str, filters: dict) -> list[dict]:
        """Return documents matching all key-value equality filters.

        Applies each filter as a ``where(field, "==", value)`` clause chained
        on the collection reference.  An empty ``filters`` dict fetches the
        entire collection.
        """
        col_ref: Any = self._client.collection(collection)
        for field, value in filters.items():
            col_ref = col_ref.where(field, "==", value)

        snaps = await col_ref.get()
        docs = [snap.to_dict() for snap in snaps if snap.exists]
        logger.debug(
            "query complete",
            extra={"collection": collection, "filters": filters, "count": len(docs)},
        )
        return docs

    async def vector_search(
        self,
        collection: str,
        embedding: list[float],
        top_k: int,
        threshold: float,
    ) -> list[dict]:
        """Nearest-neighbour search using Firestore's native ``find_nearest`` API.

        Queries the ``_EMBEDDING_FIELD`` vector field with COSINE distance,
        returns up to ``top_k`` results, and post-filters any result whose
        similarity score is below ``threshold``.

        The similarity score injected into each result as ``_score`` is derived
        from the COSINE distance returned by Firestore:
        ``score = 1.0 - cosine_distance``.

        Args:
            collection: Collection whose vector index to query.
            embedding: Dense query vector.
            top_k: Maximum number of candidates to fetch from Firestore.
            threshold: Minimum similarity (0–1); results below this are dropped.

        Returns:
            List of document dicts (with ``_score`` injected), ordered by
            descending similarity, filtered to those above ``threshold``.
        """
        col_ref = self._client.collection(collection)

        vector_query = col_ref.find_nearest(
            vector_field=_EMBEDDING_FIELD,
            query_vector=Vector(embedding),
            distance_measure=DistanceMeasure.COSINE,
            limit=top_k,
        )

        snaps = await vector_query.get()

        results: list[dict] = []
        for snap in snaps:
            if not snap.exists:
                continue
            doc = snap.to_dict() or {}
            # Firestore returns COSINE *distance* (0 = identical, 2 = opposite).
            # Convert to similarity: score = 1 - distance.
            distance = snap.distance if hasattr(snap, "distance") else None
            score = (1.0 - distance) if distance is not None else 1.0
            if score < threshold:
                continue
            doc["_score"] = round(score, 6)
            results.append(doc)

        # Results arrive pre-ordered by distance (ascending), which means
        # descending similarity — no re-sort needed.
        logger.debug(
            "vector_search complete",
            extra={
                "collection": collection,
                "top_k": top_k,
                "threshold": threshold,
                "returned": len(results),
            },
        )
        return results

    async def close(self) -> None:
        """Close the underlying Firestore client connection.

        Safe to call multiple times.
        """
        if not self._closed:
            self._client.close()
            self._closed = True
            logger.info(
                "FirestoreDAO closed",
                extra={"project": self._project_id, "database": self._database},
            )
