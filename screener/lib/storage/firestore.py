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
import os
from typing import Any

from google.api_core.exceptions import InvalidArgument, NotFound
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

    On Cloud Run the attached service account provides credentials
    automatically via Application Default Credentials (ADC).
    ``GOOGLE_APPLICATION_CREDENTIALS`` must therefore *not* be set in the
    container environment; if it is set to an empty string the variable is
    treated as absent and ADC is used instead.

    Args:
        project_id: GCP project ID.
        database: Firestore named database ID (e.g. ``"multi-agent-stock-screener"``).
                  Defaults to ``"(default)"`` if not provided.
    """

    def __init__(self, project_id: str, database: str = "(default)") -> None:
        self._project_id = project_id
        self._database = database

        # Never pass an explicit credentials path when GOOGLE_APPLICATION_CREDENTIALS
        # is absent or empty — let the google-auth ADC chain resolve credentials
        # automatically (service-account token on Cloud Run, gcloud on local dev).
        # Only log a warning when the variable is set to a non-empty value so
        # operators can spot a misconfigured container immediately.
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        if creds_path:
            logger.warning(
                "GOOGLE_APPLICATION_CREDENTIALS is set to '%s'; "
                "on Cloud Run this should be unset — the attached service account "
                "provides credentials via ADC automatically.",
                creds_path,
            )

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
        filters: dict | None = None,
    ) -> list[dict]:
        """Nearest-neighbour search using Firestore's native ``find_nearest`` API.

        Attempts Firestore's native ANN search first.  When the API rejects the
        query vector because it exceeds the 2048-dimension limit, falls back to
        a brute-force cosine similarity scan over the collection (or a
        pre-filtered subset when ``filters`` is provided).

        The similarity score injected into each result as ``_score`` is derived
        from the COSINE distance returned by Firestore:
        ``score = 1.0 - cosine_distance``.

        Args:
            collection: Collection whose vector index to query.
            embedding: Dense query vector.
            top_k: Maximum number of candidates to fetch from Firestore.
            threshold: Minimum similarity (0–1); results below this are dropped.
            filters: Optional ``{field: value}`` equality constraints applied
                during the brute-force fallback to narrow the candidate set
                before computing cosine similarity.  Ignored on the native ANN
                path.  Defaults to ``None`` (scan whole collection on fallback).

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

        try:
            snaps = await vector_query.get()
        except InvalidArgument as exc:
            if "Vectors must be at most" not in str(exc):
                raise
            logger.warning(
                "Firestore find_nearest rejected %d-dim vector; falling back to "
                "brute-force cosine (collection=%s)",
                len(embedding),
                collection,
            )
            return await self._brute_force_cosine(
                collection, embedding, top_k, threshold, filters
            )

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

    async def _brute_force_cosine(
        self,
        collection: str,
        embedding: list[float],
        top_k: int,
        threshold: float,
        filters: dict | None,
    ) -> list[dict]:
        """Brute-force cosine similarity scan used when find_nearest is unavailable.

        Fetches all candidate documents (optionally pre-filtered by ``filters``),
        computes cosine similarity against ``embedding``, drops candidates below
        ``threshold``, and returns the top ``top_k`` results sorted by descending
        similarity.

        Args:
            collection: Collection to scan.
            embedding: Dense query vector.
            top_k: Maximum results to return.
            threshold: Minimum similarity to include a result.
            filters: Optional ``{field: value}`` equality constraints applied
                as Firestore ``where`` clauses before fetching documents.

        Returns:
            List of document dicts with ``_score`` injected, sorted by
            descending similarity.
        """
        # Use numpy for fast dot-product/norm if available; fall back to math.
        try:
            import numpy as np

            def _cosine(q: list[float], d: list[float]) -> float:
                qv = np.array(q, dtype=np.float64)
                dv = np.array(d, dtype=np.float64)
                norm_q = np.linalg.norm(qv)
                norm_d = np.linalg.norm(dv)
                if norm_q == 0.0 or norm_d == 0.0:
                    return 0.0
                return float(np.dot(qv, dv) / (norm_q * norm_d))

        except ImportError:
            import math

            def _cosine(q: list[float], d: list[float]) -> float:  # type: ignore[misc]
                dot = sum(qi * di for qi, di in zip(q, d))
                norm_q = math.sqrt(sum(qi * qi for qi in q))
                norm_d = math.sqrt(sum(di * di for di in d))
                if norm_q == 0.0 or norm_d == 0.0:
                    return 0.0
                return dot / (norm_q * norm_d)

        col_ref: Any = self._client.collection(collection)
        if filters:
            for field, value in filters.items():
                col_ref = col_ref.where(field, "==", value)

        snaps = await col_ref.get()

        q_dim = len(embedding)
        scored: list[tuple[float, dict]] = []
        for snap in snaps:
            if not snap.exists:
                continue
            doc = snap.to_dict() or {}
            stored = doc.get(_EMBEDDING_FIELD)
            if not stored or len(stored) != q_dim:
                # Dimension mismatch or missing field — skip silently.
                continue
            score = _cosine(embedding, stored)
            if score >= threshold:
                scored.append((score, doc))

        scored.sort(key=lambda t: t[0], reverse=True)

        results: list[dict] = []
        for score, doc in scored[:top_k]:
            doc["_score"] = round(score, 6)
            results.append(doc)

        logger.debug(
            "brute_force_cosine complete",
            extra={
                "collection": collection,
                "top_k": top_k,
                "threshold": threshold,
                "candidates_scanned": len(snaps),
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
