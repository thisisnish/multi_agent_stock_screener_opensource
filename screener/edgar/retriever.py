"""
screener/edgar/retriever.py — EDGAR disclosure indexing and retrieval via vector search.

Provides two public surfaces:

Indexer (TB-07):
    EDGARRetriever(app_config, dao).index_ticker(symbol, dry_run)
        Fetches 10-K/10-Q filings from SEC EDGAR, chunks them, embeds via
        the configured embedder model, and writes chunk vectors to the DAO.
        Respects ``edgar.freshness_days`` — skips if the index is fresh.

Retrieval helpers (existing RAG pipeline):
    get_disclosure_chunks_async(ticker, dao, embedder, top_k, threshold)
    get_disclosure_context(ticker, dao, embedder, top_k, threshold)
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone

from screener.agents.prompts import build_disclosure_block
from screener.lib.config_loader import AppConfig
from screener.lib.storage.base import StorageDAO
from screener.lib.storage.schema import CHUNKS, ticker_to_slug

logger = logging.getLogger(__name__)

# Sentinel doc that records when a ticker's index was last built.
# Stored in the chunks/ collection so it shares the same DAO/namespace.
_INDEX_DOC_SUFFIX = "_index"

# Batch size for embedding API calls — keeps individual requests small
_EMBED_BATCH_SIZE = 100


async def get_disclosure_chunks_async(
    ticker: str,
    dao: StorageDAO,
    embedder,
    top_k: int = 5,
    threshold: float = 0.7,
) -> list[dict]:
    """Embed a generic SEC risk/performance query and retrieve matching chunks.

    Uses ``asyncio.to_thread`` so the synchronous embedder.embed_query call
    does not block the event loop.

    Args:
        ticker: Upper-case ticker symbol, e.g. "AAPL".
        dao: StorageDAO implementation to query.
        embedder: LangChain Embeddings instance with an ``embed_query`` method.
        top_k: Maximum number of chunks to return.
        threshold: Minimum cosine similarity score (0.0–1.0). Chunks below this
            are dropped by the DAO's vector_search implementation.

    Returns:
        List of up to ``top_k`` chunk dicts, ordered by descending similarity.
        Each dict may include a ``_score`` key. Returns empty list on error.
    """
    query = f"SEC filing risk factors financial performance {ticker}"
    try:
        embedding: list[float] = await asyncio.to_thread(embedder.embed_query, query)
        chunks = await dao.vector_search(
            CHUNKS, embedding, top_k=top_k, threshold=threshold
        )
        logger.debug(
            "EDGAR retrieval for %s: %d chunks above threshold %.2f",
            ticker,
            len(chunks),
            threshold,
        )
        return chunks
    except Exception:
        logger.exception("EDGAR retrieval failed for ticker=%s", ticker)
        return []


def get_disclosure_context(
    ticker: str,
    dao: StorageDAO,
    embedder,
    top_k: int = 5,
    threshold: float = 0.7,
) -> str | None:
    """Synchronous wrapper around get_disclosure_chunks_async.

    Intended for use in non-async contexts (e.g. scripts, tests). In the graph,
    build_context_node calls get_disclosure_chunks_async directly.

    Args:
        ticker: Upper-case ticker symbol.
        dao: StorageDAO implementation.
        embedder: LangChain Embeddings instance.
        top_k: Maximum chunks to retrieve.
        threshold: Minimum similarity threshold.

    Returns:
        Formatted disclosure block string or None if no relevant chunks found.
    """
    chunks = asyncio.run(
        get_disclosure_chunks_async(ticker, dao, embedder, top_k, threshold)
    )
    return build_disclosure_block(chunks)


# ---------------------------------------------------------------------------
# EDGARRetriever — indexer (TB-07)
# ---------------------------------------------------------------------------


class EDGARRetriever:
    """Fetch, chunk, embed, and store 10-K/10-Q filings for a ticker.

    Instantiate once per job run, then call :meth:`index_ticker` for each
    symbol.  Freshness is checked per-ticker: if the index sentinel doc is
    younger than ``app_config.edgar.freshness_days``, the ticker is skipped.

    Args:
        app_config: Loaded :class:`~screener.lib.config_loader.AppConfig`.
        dao: :class:`~screener.lib.storage.base.StorageDAO` implementation to
            write chunk vectors and the freshness sentinel into.

    Example::

        retriever = EDGARRetriever(app_config=cfg, dao=dao)
        retriever.index_ticker("AAPL", dry_run=False)
    """

    def __init__(self, app_config: AppConfig, dao: StorageDAO) -> None:
        self._cfg = app_config
        self._dao = dao
        self._embedder = self._build_embedder()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def index_ticker(self, symbol: str, dry_run: bool = False) -> int:
        """Fetch, chunk, embed, and write EDGAR filings for *symbol*.

        Steps:
        1. Check the freshness sentinel — skip if the index is current.
        2. Fetch filings via :mod:`screener.edgar.fetcher`.
        3. Embed all chunks in batches.
        4. Write chunk docs to the ``chunks/`` collection (idempotent doc IDs).
        5. Update the freshness sentinel.

        Args:
            symbol: Upper-case ticker symbol, e.g. ``"AAPL"``.
            dry_run: When ``True``, skip all storage writes and log what would
                have been written.

        Returns:
            Number of chunk documents written (0 on skip or dry-run).
        """
        slug = ticker_to_slug(symbol)

        if self._is_fresh(slug):
            logger.info(
                "EDGAR index is fresh — skipping ticker=%s (freshness_days=%d)",
                symbol,
                self._cfg.edgar.freshness_days,
            )
            return 0

        from screener.edgar.fetcher import get_filing_chunks

        chunks = get_filing_chunks(
            symbol,
            chunk_size=self._cfg.edgar.chunk_size,
            overlap=self._cfg.edgar.chunk_overlap,
        )

        if not chunks:
            logger.warning("EDGAR: no chunks produced for ticker=%s", symbol)
            return 0

        logger.info(
            "EDGAR: embedding %d chunks for ticker=%s", len(chunks), symbol
        )
        enriched = self._embed_chunks(chunks)

        if dry_run:
            logger.info(
                "EDGAR dry_run=True — would write %d chunks for ticker=%s",
                len(enriched),
                symbol,
            )
            return 0

        written = asyncio.run(self._write_chunks(enriched, slug))
        logger.info(
            "EDGAR: wrote %d chunks for ticker=%s", written, symbol
        )
        return written

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_embedder(self):
        """Instantiate the LangChain embedder from config.

        Uses the same ``provider:model_id`` format as the rest of the project.
        The embedder is initialised once per :class:`EDGARRetriever` instance.

        Returns:
            A LangChain ``Embeddings`` instance with an ``embed_documents``
            and ``embed_query`` method.

        Raises:
            ImportError: If the required LangChain provider package is not
                installed.
            ValueError: If the ``embedder_model`` format is unrecognised.
        """
        model_str = self._cfg.llm.embedder_model
        provider, model_id = model_str.split(":", 1)

        if provider == "google_genai":
            from langchain_google_genai import GoogleGenerativeAIEmbeddings

            return GoogleGenerativeAIEmbeddings(model=model_id)

        if provider == "openai":
            from langchain_openai import OpenAIEmbeddings

            return OpenAIEmbeddings(model=model_id)

        raise ValueError(
            f"Unsupported embedder provider '{provider}' in embedder_model='{model_str}'. "
            f"Supported: google_genai, openai."
        )

    def _is_fresh(self, slug: str) -> bool:
        """Return ``True`` if the index sentinel is newer than freshness_days.

        Reads ``chunks/{slug}_index`` from the DAO.  Returns ``False`` (i.e.
        stale) when the doc is missing or when ``indexed_at`` is absent or
        older than the configured TTL.

        Args:
            slug: Canonical ticker slug from :func:`~screener.lib.storage.schema.ticker_to_slug`.

        Returns:
            ``True`` if the index is fresh and should be skipped.
        """
        doc_id = f"{slug}{_INDEX_DOC_SUFFIX}"
        doc: dict | None = asyncio.run(self._dao.get(CHUNKS, doc_id))

        if not doc:
            return False

        indexed_at = doc.get("indexed_at")
        if indexed_at is None:
            return False

        # Normalise to UTC-aware datetime
        if isinstance(indexed_at, str):
            try:
                indexed_at = datetime.fromisoformat(indexed_at)
            except ValueError:
                return False

        if isinstance(indexed_at, datetime):
            if indexed_at.tzinfo is None:
                indexed_at = indexed_at.replace(tzinfo=timezone.utc)
            cutoff = datetime.now(timezone.utc) - timedelta(
                days=self._cfg.edgar.freshness_days
            )
            return indexed_at >= cutoff

        return False

    def _embed_chunks(self, chunks: list[dict]) -> list[dict]:
        """Embed the ``text`` field of each chunk in batches.

        Args:
            chunks: List of chunk dicts from :func:`~screener.edgar.fetcher.get_filing_chunks`.

        Returns:
            New list of chunk dicts with ``embedding`` and ``indexed_at`` keys
            added.  The original dicts are not mutated.

        Raises:
            Exception: On embedding API failure — propagates to the caller
                so :meth:`index_ticker` can log and continue.
        """
        texts = [c["text"] for c in chunks]
        embeddings: list[list[float]] = []
        num_batches = math.ceil(len(texts) / _EMBED_BATCH_SIZE)

        for i in range(num_batches):
            batch = texts[i * _EMBED_BATCH_SIZE : (i + 1) * _EMBED_BATCH_SIZE]
            batch_embeddings = self._embedder.embed_documents(batch)
            embeddings.extend(batch_embeddings)

        now = datetime.now(timezone.utc)
        enriched = []
        for chunk, embedding in zip(chunks, embeddings):
            enriched.append(
                {
                    **chunk,
                    "embedding": embedding,
                    "indexed_at": now.isoformat(),
                }
            )
        return enriched

    async def _write_chunks(self, enriched: list[dict], slug: str) -> int:
        """Write enriched chunks and the freshness sentinel to the DAO.

        Doc IDs are deterministic (``{slug}_{form_type}_{period}_{index:04d}``)
        so re-running the job is idempotent.

        Args:
            enriched: Chunks with ``embedding`` and ``indexed_at`` keys.
            slug: Canonical ticker slug — used for the sentinel doc ID and
                chunk ID prefix.

        Returns:
            Number of chunk documents written (excludes the sentinel doc).
        """
        for chunk in enriched:
            form = chunk["form_type"].replace("-", "").lower()
            period = chunk["period"].replace("-", "")
            idx = chunk["chunk_index"]
            doc_id = f"{slug}_{form}_{period}_{idx:04d}"
            await self._dao.set(CHUNKS, doc_id, chunk)

        # Update freshness sentinel
        sentinel_id = f"{slug}{_INDEX_DOC_SUFFIX}"
        await self._dao.set(
            CHUNKS,
            sentinel_id,
            {"indexed_at": datetime.now(timezone.utc).isoformat()},
        )

        return len(enriched)
