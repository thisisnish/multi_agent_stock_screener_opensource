"""
screener/lib/storage/base.py — Abstract base class for all storage backends.

All implementations must be async-safe.  The DAO interface is intentionally
minimal: CRUD on individual documents, a lightweight filter-based query, a
vector similarity search, and a close() method for connection teardown.

Usage::

    from screener.lib.storage.base import StorageDAO

    class MyDAO(StorageDAO):
        async def get(self, collection, doc_id): ...
        ...
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class StorageDAO(ABC):
    """Abstract storage interface.  All methods are async.

    Implementations must raise their own exceptions on unrecoverable errors;
    callers should not catch broad ``Exception``.  A ``None`` return from
    :meth:`get` is the canonical signal for "document not found".
    """

    @abstractmethod
    async def get(self, collection: str, doc_id: str) -> dict | None:
        """Fetch a single document by ID.

        Args:
            collection: The collection (or table/bucket prefix) name.
            doc_id: The document identifier.

        Returns:
            The document as a plain ``dict``, or ``None`` if it does not exist.
        """

    @abstractmethod
    async def set(self, collection: str, doc_id: str, data: dict) -> None:
        """Write (upsert) a document.

        Creates the document if it does not exist; overwrites it if it does.
        Implementations must treat this as idempotent on the same ``doc_id``.

        Args:
            collection: The collection name.
            doc_id: The document identifier.
            data: The document payload.  Must be JSON-serialisable.
        """

    @abstractmethod
    async def delete(self, collection: str, doc_id: str) -> None:
        """Delete a document.

        Must be a no-op if the document does not exist (idempotent).

        Args:
            collection: The collection name.
            doc_id: The document identifier.
        """

    @abstractmethod
    async def query(self, collection: str, filters: dict) -> list[dict]:
        """Return documents matching all key-value equality filters.

        Implementations apply the filters as logical AND.  An empty ``filters``
        dict returns all documents in the collection (use with care on large
        collections).

        Args:
            collection: The collection name.
            filters: ``{field: value}`` equality constraints.

        Returns:
            A list of matching document dicts (may be empty).
        """

    @abstractmethod
    async def vector_search(
        self,
        collection: str,
        embedding: list[float],
        top_k: int,
        threshold: float,
        filters: dict | None = None,
    ) -> list[dict]:
        """Approximate nearest-neighbour search over embedded documents.

        Args:
            collection: The collection whose vector index to query.
            embedding: The query vector (must match the index dimensionality).
            top_k: Maximum number of results to return.
            threshold: Minimum similarity score; results below this are dropped.
            filters: Optional ``{field: value}`` equality constraints used by
                concrete implementations that fall back to brute-force search
                (e.g. when the native index rejects the query vector).  The
                native ANN path ignores this parameter.  Defaults to ``None``
                (no pre-filtering).

        Returns:
            A list of up to ``top_k`` document dicts, ordered by descending
            similarity.  Each dict may include a ``_score`` key with the raw
            similarity value.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release any held resources (connections, file handles, etc.).

        Safe to call multiple times; must be a no-op after the first call.
        """
