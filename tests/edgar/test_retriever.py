"""
tests/edgar/test_retriever.py — Unit tests for EDGARRetriever in screener/edgar/retriever.py

Covers:
- index_ticker: fresh index skips fetching and writing
- index_ticker: no chunks produced returns 0 and no writes
- index_ticker: dry_run=True skips storage writes, returns 0
- index_ticker: full run writes correct number of chunks and sentinel
- _is_fresh: missing doc → not fresh; recent indexed_at → fresh; old indexed_at → stale
- _embed_chunks: delegates to embedder.embed_documents in batches
- _write_chunks: writes chunk docs and sentinel with expected doc IDs

No real LLM calls, EDGAR HTTP calls, or storage writes are made.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch


from screener.lib.config_loader import (
    AppConfig,
    EdgarConfig,
    EmailConfig,
    FirestoreConfig,
    LLMConfig,
    NotificationsConfig,
    StorageConfig,
)
from screener.lib.storage.schema import CHUNKS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_config(freshness_days: int = 30) -> AppConfig:
    """Minimal valid AppConfig — no real credentials needed."""
    return AppConfig(
        llm=LLMConfig(
            model="anthropic:claude-haiku-4-5-20251001",
            embedder_model="google_genai:models/gemini-embedding-001",
        ),
        storage=StorageConfig(
            provider="firestore",
            firestore=FirestoreConfig(project_id="test-project"),
        ),
        notifications=NotificationsConfig(
            email=EmailConfig(enabled=False),
        ),
        edgar=EdgarConfig(
            freshness_days=freshness_days,
            chunk_size=512,
            chunk_overlap=0.10,
        ),
    )


def _mock_dao(sentinel_doc: dict | None = None) -> MagicMock:
    """Mock DAO.  ``sentinel_doc`` is the value returned by dao.get for the index sentinel."""
    dao = MagicMock()
    dao.get = AsyncMock(return_value=sentinel_doc)
    dao.set = AsyncMock()
    dao.vector_search = AsyncMock(return_value=[])
    dao.query = AsyncMock(return_value=[])
    dao.delete = AsyncMock()
    dao.close = AsyncMock()
    return dao


def _mock_embedder(embedding_dim: int = 4) -> MagicMock:
    """Mock LangChain embedder that returns constant unit vectors."""
    embedder = MagicMock()
    embedder.embed_documents.side_effect = lambda texts: [
        [0.1] * embedding_dim for _ in texts
    ]
    embedder.embed_query.return_value = [0.1] * embedding_dim
    return embedder


def _make_retriever(cfg: AppConfig, dao: MagicMock):
    """Build an EDGARRetriever with a mocked embedder (no real API calls)."""
    from screener.edgar.retriever import EDGARRetriever

    retriever = EDGARRetriever.__new__(EDGARRetriever)
    retriever._cfg = cfg
    retriever._dao = dao
    retriever._embedder = _mock_embedder()
    return retriever


def _fresh_sentinel() -> dict:
    """Sentinel doc stamped 1 minute ago — within any reasonable freshness window."""
    return {
        "indexed_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    }


def _stale_sentinel(freshness_days: int = 30) -> dict:
    """Sentinel doc stamped 2× freshness_days ago — definitely stale."""
    return {
        "indexed_at": (
            datetime.now(timezone.utc) - timedelta(days=freshness_days * 2)
        ).isoformat()
    }


def _sample_chunks(n: int = 3) -> list[dict]:
    """Produce *n* minimal chunk dicts as would be returned by get_filing_chunks."""
    return [
        {
            "ticker": "AAPL",
            "form_type": "10-K",
            "period": "2024-12-31",
            "section": "",
            "chunk_index": i,
            "text": "Revenue grew significantly. " * 50,
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# _is_fresh
# ---------------------------------------------------------------------------


def test_is_fresh_missing_doc():
    """_is_fresh returns False when the sentinel doc does not exist."""
    dao = _mock_dao(sentinel_doc=None)
    retriever = _make_retriever(_stub_config(), dao)
    assert asyncio.run(retriever._is_fresh("aapl")) is False


def test_is_fresh_recent_sentinel():
    """_is_fresh returns True when indexed_at is within the freshness window."""
    dao = _mock_dao(sentinel_doc=_fresh_sentinel())
    retriever = _make_retriever(_stub_config(freshness_days=30), dao)
    assert asyncio.run(retriever._is_fresh("aapl")) is True


def test_is_fresh_stale_sentinel():
    """_is_fresh returns False when indexed_at is outside the freshness window."""
    dao = _mock_dao(sentinel_doc=_stale_sentinel(freshness_days=30))
    retriever = _make_retriever(_stub_config(freshness_days=30), dao)
    assert asyncio.run(retriever._is_fresh("aapl")) is False


def test_is_fresh_missing_indexed_at_field():
    """_is_fresh returns False when the sentinel doc exists but has no indexed_at."""
    dao = _mock_dao(sentinel_doc={"some_other_field": "value"})
    retriever = _make_retriever(_stub_config(), dao)
    assert asyncio.run(retriever._is_fresh("aapl")) is False


# ---------------------------------------------------------------------------
# index_ticker — skip paths
# ---------------------------------------------------------------------------


def test_index_ticker_skips_fresh(monkeypatch):
    """index_ticker returns 0 and makes no fetch call when index is fresh."""
    dao = _mock_dao(sentinel_doc=_fresh_sentinel())
    retriever = _make_retriever(_stub_config(), dao)
    retriever._is_fresh = AsyncMock(return_value=True)

    with patch("screener.edgar.fetcher.get_filing_chunks") as mock_fetch:
        result = asyncio.run(retriever.index_ticker("AAPL", dry_run=False))

    assert result == 0
    mock_fetch.assert_not_called()
    dao.set.assert_not_called()


def test_index_ticker_no_chunks_produced(monkeypatch):
    """index_ticker returns 0 when EDGAR produces no chunks for the ticker."""
    dao = _mock_dao(sentinel_doc=None)  # stale → will proceed
    retriever = _make_retriever(_stub_config(), dao)
    retriever._is_fresh = AsyncMock(return_value=False)

    import screener.edgar.fetcher as fetcher_mod

    original_get = fetcher_mod.get_filing_chunks
    fetcher_mod.get_filing_chunks = lambda *a, **kw: []
    try:
        result = asyncio.run(retriever.index_ticker("ZZZZ", dry_run=False))
    finally:
        fetcher_mod.get_filing_chunks = original_get

    assert result == 0
    dao.set.assert_not_called()


def test_index_ticker_dry_run(monkeypatch):
    """dry_run=True skips all storage writes and returns 0."""
    dao = _mock_dao(sentinel_doc=None)
    retriever = _make_retriever(_stub_config(), dao)
    retriever._is_fresh = AsyncMock(return_value=False)

    chunks = _sample_chunks(3)

    import screener.edgar.fetcher as fetcher_mod

    original_get = fetcher_mod.get_filing_chunks
    fetcher_mod.get_filing_chunks = lambda *a, **kw: chunks
    try:
        result = asyncio.run(retriever.index_ticker("AAPL", dry_run=True))
    finally:
        fetcher_mod.get_filing_chunks = original_get

    assert result == 0
    dao.set.assert_not_called()


# ---------------------------------------------------------------------------
# index_ticker — full write path
# ---------------------------------------------------------------------------


def test_index_ticker_writes_chunks_and_sentinel():
    """Full run writes all chunks + the freshness sentinel to the DAO."""
    dao = _mock_dao(sentinel_doc=None)
    retriever = _make_retriever(_stub_config(), dao)
    retriever._is_fresh = AsyncMock(return_value=False)

    chunks = _sample_chunks(3)

    import screener.edgar.fetcher as fetcher_mod

    original_get = fetcher_mod.get_filing_chunks
    fetcher_mod.get_filing_chunks = lambda *a, **kw: chunks
    try:
        result = asyncio.run(retriever.index_ticker("AAPL", dry_run=False))
    finally:
        fetcher_mod.get_filing_chunks = original_get

    # 3 chunks + 1 sentinel = 4 set calls
    assert result == 3
    assert dao.set.call_count == 4

    # Last call must be the sentinel
    last_call = dao.set.call_args_list[-1]
    coll, doc_id, data = last_call.args
    assert coll == CHUNKS
    assert doc_id == "aapl_index"
    assert "indexed_at" in data


def test_index_ticker_chunk_doc_ids_are_deterministic():
    """Chunk doc IDs follow the expected {slug}_{form}_{period}_{index:04d} pattern."""
    dao = _mock_dao(sentinel_doc=None)
    retriever = _make_retriever(_stub_config(), dao)
    retriever._is_fresh = AsyncMock(return_value=False)

    chunks = _sample_chunks(2)  # chunk_index 0 and 1

    import screener.edgar.fetcher as fetcher_mod

    original_get = fetcher_mod.get_filing_chunks
    fetcher_mod.get_filing_chunks = lambda *a, **kw: chunks
    try:
        asyncio.run(retriever.index_ticker("AAPL", dry_run=False))
    finally:
        fetcher_mod.get_filing_chunks = original_get

    calls = dao.set.call_args_list
    # First two calls are the chunk docs
    chunk_doc_ids = [c.args[1] for c in calls[:2]]
    assert chunk_doc_ids[0] == "aapl_10k_20241231_0000"
    assert chunk_doc_ids[1] == "aapl_10k_20241231_0001"


# ---------------------------------------------------------------------------
# _embed_chunks
# ---------------------------------------------------------------------------


def test_embed_chunks_adds_embedding_and_indexed_at():
    """_embed_chunks enriches each chunk with embedding and indexed_at keys."""
    dao = _mock_dao()
    retriever = _make_retriever(_stub_config(), dao)

    chunks = _sample_chunks(2)
    enriched = retriever._embed_chunks(chunks)

    assert len(enriched) == 2
    for item in enriched:
        assert "embedding" in item
        assert "indexed_at" in item
        assert isinstance(item["embedding"], list)
        assert len(item["embedding"]) > 0


def test_embed_chunks_batches_correctly():
    """embed_documents is called in batches of _EMBED_BATCH_SIZE."""
    from screener.edgar import retriever as ret_mod

    original_batch_size = ret_mod._EMBED_BATCH_SIZE
    ret_mod._EMBED_BATCH_SIZE = 2  # force 2 batches for 3 chunks

    dao = _mock_dao()
    retriever = _make_retriever(_stub_config(), dao)

    chunks = _sample_chunks(3)
    retriever._embed_chunks(chunks)

    # embed_documents called twice: batch [0,1] and batch [2]
    assert retriever._embedder.embed_documents.call_count == 2

    ret_mod._EMBED_BATCH_SIZE = original_batch_size  # restore


def test_embed_chunks_does_not_mutate_input():
    """_embed_chunks returns new dicts; originals are not modified."""
    dao = _mock_dao()
    retriever = _make_retriever(_stub_config(), dao)

    chunks = _sample_chunks(2)
    originals = [dict(c) for c in chunks]  # deep copy for comparison
    retriever._embed_chunks(chunks)

    for original, chunk in zip(originals, chunks):
        assert "embedding" not in chunk
        assert chunk == original


# ---------------------------------------------------------------------------
# get_disclosure_chunks_async — filters propagation
# ---------------------------------------------------------------------------


def test_get_disclosure_chunks_async_passes_ticker_filter():
    """vector_search is called with filters={"ticker": <UPPER>} for brute-force scoping."""
    from screener.edgar.retriever import get_disclosure_chunks_async
    from screener.lib.storage.schema import CHUNKS

    dao = _mock_dao()
    embedder = _mock_embedder(embedding_dim=4)

    asyncio.run(
        get_disclosure_chunks_async("aapl", dao, embedder, top_k=3, threshold=0.5)
    )

    dao.vector_search.assert_awaited_once_with(
        CHUNKS,
        embedder.embed_query.return_value,
        top_k=3,
        threshold=0.5,
        filters={"ticker": "AAPL"},
    )
