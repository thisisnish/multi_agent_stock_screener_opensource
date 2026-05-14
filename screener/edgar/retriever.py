"""
screener/edgar/retriever.py — EDGAR disclosure indexing and retrieval via vector search.

Provides two public surfaces:

Indexer (TB-07):
    EDGARRetriever(app_config, dao).index_ticker(symbol, dry_run)
        Fetches 10-K/10-Q filings from SEC EDGAR, chunks them, embeds via
        the configured embedder model, and writes chunk vectors to the DAO.
        Respects ``edgar.freshness_days`` — skips if the index is fresh.
        Detects embedder model changes (P2-07) and forces re-index on mismatch.

Retrieval helpers (existing RAG pipeline):
    get_disclosure_chunks_async(ticker, dao, embedder, top_k, threshold)
    get_disclosure_context(ticker, dao, embedder, top_k, threshold)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import re
import time
from datetime import datetime, timedelta, timezone

from screener.agents.prompts import build_disclosure_block
from screener.lib.config_loader import AppConfig
from screener.lib.storage.base import StorageDAO
from screener.lib.storage.schema import CHUNKS, ticker_to_slug

logger = logging.getLogger(__name__)

# Sentinel doc that records when a ticker's index was last built.
# Stored in the chunks/ collection so it shares the same DAO/namespace.
_INDEX_DOC_SUFFIX = "_index"

# Batch size for embedding API calls — kept small to stay within Gemini's 15 RPM free-tier limit.
_EMBED_BATCH_SIZE = 20

# Retry config for embedding API 429 / quota errors.
_EMBED_RETRY_DELAYS = (10, 30, 60)  # seconds between attempts (max 3 retries)

# P2-08: Score boost applied to chunks whose section matches a configured retrieval_section.
_SECTION_BOOST = 0.05

# P2-10: Regex for normalising text before hashing for deduplication.
_DEDUP_NORMALISE_RE = re.compile(r"[^\w\s]")


def _normalise_text(text: str) -> str:
    """Return a normalised form of *text* for deduplication hashing.

    Lowercases, strips punctuation, collapses whitespace.
    """
    lowered = text.lower()
    no_punct = _DEDUP_NORMALISE_RE.sub("", lowered)
    return " ".join(no_punct.split())


def _text_hash(text: str) -> str:
    """MD5 hex digest of the normalised form of *text*."""
    return hashlib.md5(_normalise_text(text).encode()).hexdigest()


async def get_disclosure_chunks_async(
    ticker: str,
    dao: StorageDAO,
    embedder,
    top_k: int = 5,
    threshold: float = 0.7,
    query_templates: list[str] | None = None,
    retrieval_sections: list[str] | None = None,
    embedder_model: str | None = None,
) -> list[dict]:
    """Embed one or more queries and retrieve matching EDGAR chunks for a ticker.

    Each query template is embedded and searched independently. Results are
    merged, deduplicated by chunk_index+period (keeping the highest score per
    chunk), and returned sorted by descending similarity, capped at ``top_k``.

    P2-08: If ``retrieval_sections`` is non-empty, chunks whose ``section``
    field matches an entry receive a +0.05 boost to their similarity score
    before sorting.

    P2-10: After sorting, near-duplicate chunks (same normalised-text MD5 hash)
    are dropped — only the highest-scoring copy is kept.

    Uses ``asyncio.to_thread`` so synchronous embedder calls don't block the
    event loop.

    Args:
        ticker: Upper-case ticker symbol, e.g. "AAPL".
        dao: StorageDAO implementation to query.
        embedder: LangChain Embeddings instance with an ``embed_query`` method.
        top_k: Maximum number of chunks to return across all queries.
        threshold: Minimum cosine similarity score (0.0–1.0). Chunks below this
            are dropped by the DAO's vector_search implementation.
        query_templates: List of query string templates. ``{ticker}`` is replaced
            with the ticker symbol before embedding. Defaults to the built-in
            risk/performance query.
        retrieval_sections: Optional list of section names to boost. Chunks
            matching a listed section have +0.05 added to their score.
        embedder_model: String identifier of the embedder in use — logged for
            observability.  If ``None``, the log entry omits the model label.

    Returns:
        List of up to ``top_k`` chunk dicts, ordered by descending similarity.
        Each dict may include a ``_score`` key. Returns empty list on error.
    """
    if not query_templates:
        query_templates = ["SEC filing risk factors financial performance {ticker}"]

    model_label = embedder_model or "unknown"
    logger.debug("Querying with embedder=%s for ticker=%s", model_label, ticker)

    seen: dict[str, dict] = {}  # dedup key → best chunk
    try:
        for template in query_templates:
            query = template.format(ticker=ticker)
            embedding: list[float] = await asyncio.to_thread(
                embedder.embed_query, query
            )
            results = await dao.vector_search(
                CHUNKS,
                embedding,
                top_k=top_k,
                threshold=threshold,
                filters={"ticker": ticker.upper()},
            )
            for chunk in results:
                # Deduplicate by (period, chunk_index); keep the higher-scoring hit
                key = f"{chunk.get('period', '')}_{chunk.get('chunk_index', '')}"
                if key not in seen or chunk.get("_score", 0) > seen[key].get(
                    "_score", 0
                ):
                    seen[key] = chunk

        # P2-08: apply section score boost before sorting
        boosted_sections = set(retrieval_sections) if retrieval_sections else set()
        if boosted_sections:
            for chunk in seen.values():
                if chunk.get("section", "") in boosted_sections:
                    chunk["_score"] = chunk.get("_score", 0.0) + _SECTION_BOOST

        merged = sorted(seen.values(), key=lambda c: c.get("_score", 0), reverse=True)[
            :top_k
        ]

        # P2-10: text-hash deduplication — drop lower-scoring chunks with identical
        # normalised text.  We iterate in score-descending order so the first
        # occurrence (highest score) always wins.
        seen_hashes: set[str] = set()
        unique: list[dict] = []
        for chunk in merged:
            h = _text_hash(chunk.get("text", ""))
            if h not in seen_hashes:
                seen_hashes.add(h)
                unique.append(chunk)

        dropped = len(merged) - len(unique)
        logger.debug(
            "Deduplication: dropped %d near-duplicate chunks, returning %d unique",
            dropped,
            len(unique),
        )

        # P2-10: persist dedup stats to Firestore for post-hoc analysis.
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        try:
            await dao.set(
                f"analysis/{ticker.upper()}/disclosures",
                run_id,
                {
                    "dedup_dropped_count": dropped,
                    "chunks_returned": len(unique),
                    "run_timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
        except Exception:
            logger.exception(
                "Failed to write dedup stats to Firestore for ticker=%s", ticker
            )

        logger.debug(
            "EDGAR retrieval for %s: %d chunks above threshold %.2f (across %d queries)",
            ticker,
            len(unique),
            threshold,
            len(query_templates),
        )
        return unique
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

    P2-07: The sentinel doc also records the ``embedder_model`` used when the
    index was built.  If the current config value differs from the stored value,
    the existing index is deleted and rebuilt from scratch.

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

    async def index_ticker(self, symbol: str, dry_run: bool = False) -> int:
        """Fetch, chunk, embed, and write EDGAR filings for *symbol*.

        Steps:
        1. Check the freshness sentinel — skip if the index is current AND
           the stored embedder_model matches the configured one.
        2. If the embedder model changed (P2-07), delete all existing chunks
           for the ticker before re-indexing.
        3. Fetch filings via :mod:`screener.edgar.fetcher`.
        4. Embed all chunks in batches (with retry on quota errors).
        5. Write chunk docs to the ``chunks/`` collection (idempotent doc IDs).
        6. Update the freshness sentinel (including embedder_model).

        This method is async so all DAO calls share a single event loop created
        by the caller (``edgar_disclosure/main.py``), which prevents the
        ``RuntimeError: Event loop is closed`` failure that occurs when
        ``asyncio.run()`` is called repeatedly inside a loop — each call closes
        the loop that the grpc.aio-backed FirestoreDAO channel is bound to.

        Args:
            symbol: Upper-case ticker symbol, e.g. ``"AAPL"``.
            dry_run: When ``True``, skip all storage writes and log what would
                have been written.

        Returns:
            Number of chunk documents written (0 on skip or dry-run).
        """
        slug = ticker_to_slug(symbol)
        current_model = self._cfg.llm.embedder_model

        fresh, model_drifted, stored_model = await self._check_freshness_and_drift(
            slug, current_model
        )

        if fresh and not model_drifted:
            logger.info(
                "EDGAR index is fresh — skipping ticker=%s (freshness_days=%d)",
                symbol,
                self._cfg.edgar.freshness_days,
            )
            return 0

        # P2-07: if embedder model changed, purge the stale vector index first.
        if model_drifted and stored_model:
            logger.warning(
                "Embedder model changed from %s to %s; forcing re-index for %s",
                stored_model,
                current_model,
                symbol,
            )
            if not dry_run:
                await self._delete_ticker_chunks(slug)

        from screener.edgar.fetcher import get_filing_chunks

        chunks = get_filing_chunks(
            symbol,
            chunk_size=self._cfg.edgar.chunk_size,
            overlap=self._cfg.edgar.chunk_overlap,
        )

        if not chunks:
            logger.warning("EDGAR: no chunks produced for ticker=%s", symbol)
            return 0

        # Stamp each chunk with the current embedder model (P2-07).
        for chunk in chunks:
            chunk["embedder_model"] = current_model

        logger.info("EDGAR: embedding %d chunks for ticker=%s", len(chunks), symbol)
        # _embed_chunks is synchronous (uses time.sleep for backoff).  Running it
        # in an executor keeps the event loop unblocked, which is good practice
        # even in a batch job — it allows asyncio housekeeping to proceed.
        loop = asyncio.get_event_loop()
        enriched = await loop.run_in_executor(None, self._embed_chunks, chunks)

        if dry_run:
            logger.info(
                "EDGAR dry_run=True — would write %d chunks for ticker=%s",
                len(enriched),
                symbol,
            )
            return 0

        written = await self._write_chunks(enriched, slug, current_model)
        logger.info("EDGAR: wrote %d chunks for ticker=%s", written, symbol)
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

    async def _check_freshness_and_drift(
        self, slug: str, current_model: str
    ) -> tuple[bool, bool, str | None]:
        """Check index freshness and embedder model drift simultaneously.

        Reads the sentinel doc once and returns three values so ``index_ticker``
        can decide whether to skip, re-index from scratch, or do a normal
        incremental run.

        Args:
            slug: Canonical ticker slug.
            current_model: The embedder model string from the current config.

        Returns:
            ``(is_fresh, model_drifted, stored_model)`` where:
            - ``is_fresh``: ``True`` if ``indexed_at`` is within freshness_days.
            - ``model_drifted``: ``True`` if the stored model differs from
              ``current_model``.  Always ``False`` when the sentinel is missing.
            - ``stored_model``: The ``embedder_model`` recorded in the sentinel,
              or ``None`` if absent.
        """
        doc_id = f"{slug}{_INDEX_DOC_SUFFIX}"
        doc: dict | None = await self._dao.get(CHUNKS, doc_id)

        if not doc:
            return False, False, None

        # --- freshness ---
        indexed_at = doc.get("indexed_at")
        is_fresh = False
        if indexed_at is not None:
            if isinstance(indexed_at, str):
                try:
                    indexed_at = datetime.fromisoformat(indexed_at)
                except ValueError:
                    indexed_at = None
            if isinstance(indexed_at, datetime):
                if indexed_at.tzinfo is None:
                    indexed_at = indexed_at.replace(tzinfo=timezone.utc)
                cutoff = datetime.now(timezone.utc) - timedelta(
                    days=self._cfg.edgar.freshness_days
                )
                is_fresh = indexed_at >= cutoff

        # --- P2-07 drift detection ---
        stored_model: str | None = doc.get("embedder_model")
        model_drifted = stored_model is not None and stored_model != current_model

        return is_fresh, model_drifted, stored_model

    async def _is_fresh(self, slug: str) -> bool:
        """Return ``True`` if the index sentinel is newer than freshness_days.

        Reads ``chunks/{slug}_index`` from the DAO.  Returns ``False`` (i.e.
        stale) when the doc is missing or when ``indexed_at`` is absent or
        older than the configured TTL.

        Args:
            slug: Canonical ticker slug from :func:`~screener.lib.storage.schema.ticker_to_slug`.

        Returns:
            ``True`` if the index is fresh and should be skipped.
        """
        is_fresh, _drifted, _model = await self._check_freshness_and_drift(
            slug, self._cfg.llm.embedder_model
        )
        return is_fresh

    async def _delete_ticker_chunks(self, slug: str) -> None:
        """Delete all existing chunk docs for a ticker from the DAO.

        Queries for all docs whose ``ticker`` field matches the slug (after
        de-slugging) and deletes them individually.  The sentinel doc is
        intentionally left in place so the caller can overwrite it with the new
        embedder model after re-indexing.

        Args:
            slug: Canonical ticker slug (e.g. ``"aapl"``).
        """
        ticker_upper = slug.upper()
        existing = await self._dao.query(CHUNKS, {"ticker": ticker_upper})
        logger.debug(
            "EDGAR drift purge: deleting %d stale chunks for ticker=%s",
            len(existing),
            ticker_upper,
        )
        for doc in existing:
            doc_id = doc.get("_id") or doc.get("id")
            if doc_id:
                await self._dao.delete(CHUNKS, doc_id)

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
            last_exc: Exception | None = None
            for attempt, delay in enumerate((None,) + _EMBED_RETRY_DELAYS, start=0):
                if delay is not None:
                    logger.warning(
                        "EDGAR embed batch %d/%d failed (attempt %d) — retrying in %ds",
                        i + 1,
                        num_batches,
                        attempt,
                        delay,
                    )
                    time.sleep(delay)
                try:
                    batch_embeddings = self._embedder.embed_documents(batch)
                    embeddings.extend(batch_embeddings)
                    break
                except Exception as exc:
                    last_exc = exc
            else:
                # All retries exhausted — propagate so index_ticker can log and
                # move on to the next ticker rather than silently producing an
                # incomplete index.
                raise last_exc  # type: ignore[misc]

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

    async def _write_chunks(
        self, enriched: list[dict], slug: str, embedder_model: str
    ) -> int:
        """Write enriched chunks and the freshness sentinel to the DAO.

        Doc IDs are deterministic (``{slug}_{form_type}_{period}_{index:04d}``)
        so re-running the job is idempotent.

        Args:
            enriched: Chunks with ``embedding`` and ``indexed_at`` keys.
            slug: Canonical ticker slug — used for the sentinel doc ID and
                chunk ID prefix.
            embedder_model: The embedder model string to record in the sentinel
                (P2-07).

        Returns:
            Number of chunk documents written (excludes the sentinel doc).
        """
        for chunk in enriched:
            form = chunk["form_type"].replace("-", "").lower()
            period = chunk["period"].replace("-", "")
            idx = chunk["chunk_index"]
            doc_id = f"{slug}_{form}_{period}_{idx:04d}"
            await self._dao.set(CHUNKS, doc_id, chunk)

        # Update freshness sentinel — include embedder_model for drift detection (P2-07).
        sentinel_id = f"{slug}{_INDEX_DOC_SUFFIX}"
        await self._dao.set(
            CHUNKS,
            sentinel_id,
            {
                "indexed_at": datetime.now(timezone.utc).isoformat(),
                "embedder_model": embedder_model,
            },
        )

        return len(enriched)
