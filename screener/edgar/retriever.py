"""
screener/edgar/retriever.py — EDGAR disclosure retrieval via vector search.

Retrieves SEC filing chunks most relevant to a given ticker from the vector store
using cosine similarity. The embedder is a LangChain Embeddings instance (typically
GoogleGenerativeAIEmbeddings), keeping this module provider-agnostic.

Public API
----------
get_disclosure_chunks_async(ticker, dao, embedder, top_k, threshold) -> list[dict]
get_disclosure_context(ticker, dao, embedder, top_k, threshold) -> str | None
"""

from __future__ import annotations

import asyncio
import logging

from screener.agents.prompts import build_disclosure_block
from screener.lib.storage.base import StorageDAO
from screener.lib.storage.schema import CHUNKS

logger = logging.getLogger(__name__)


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
        chunks = await dao.vector_search(CHUNKS, embedding, top_k=top_k, threshold=threshold)
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
