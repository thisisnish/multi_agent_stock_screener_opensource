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
- P2-07: embedder model drift triggers re-index and old-model purge
- P2-07: sentinel includes embedder_model; no drift when model matches
- P2-08: section boost applied correctly in get_disclosure_chunks_async
- P2-10: text-hash deduplication drops duplicate chunks

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


def _stub_config(
    freshness_days: int = 30,
    embedder_model: str = "google_genai:models/gemini-embedding-001",
    retrieval_sections: list[str] | None = None,
) -> AppConfig:
    """Minimal valid AppConfig — no real credentials needed."""
    return AppConfig(
        llm=LLMConfig(
            model="anthropic:claude-haiku-4-5-20251001",
            embedder_model=embedder_model,
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
            retrieval_sections=retrieval_sections or [],
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


def _fresh_sentinel(
    embedder_model: str = "google_genai:models/gemini-embedding-001",
) -> dict:
    """Sentinel doc stamped 1 minute ago — within any reasonable freshness window."""
    return {
        "indexed_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        "embedder_model": embedder_model,
    }


def _stale_sentinel(
    freshness_days: int = 30,
    embedder_model: str = "google_genai:models/gemini-embedding-001",
) -> dict:
    """Sentinel doc stamped 2× freshness_days ago — definitely stale."""
    return {
        "indexed_at": (
            datetime.now(timezone.utc) - timedelta(days=freshness_days * 2)
        ).isoformat(),
        "embedder_model": embedder_model,
    }


def _sample_chunks(n: int = 3, section: str = "") -> list[dict]:
    """Produce *n* minimal chunk dicts as would be returned by get_filing_chunks."""
    return [
        {
            "ticker": "AAPL",
            "form_type": "10-K",
            "period": "2024-12-31",
            "section": section,
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
    retriever._check_freshness_and_drift = AsyncMock(
        return_value=(True, False, "google_genai:models/gemini-embedding-001")
    )

    with patch("screener.edgar.fetcher.get_filing_chunks") as mock_fetch:
        result = asyncio.run(retriever.index_ticker("AAPL", dry_run=False))

    assert result == 0
    mock_fetch.assert_not_called()
    dao.set.assert_not_called()


def test_index_ticker_no_chunks_produced(monkeypatch):
    """index_ticker returns 0 when EDGAR produces no chunks for the ticker."""
    dao = _mock_dao(sentinel_doc=None)  # stale → will proceed
    retriever = _make_retriever(_stub_config(), dao)
    retriever._check_freshness_and_drift = AsyncMock(return_value=(False, False, None))

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
    retriever._check_freshness_and_drift = AsyncMock(return_value=(False, False, None))

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
    retriever._check_freshness_and_drift = AsyncMock(return_value=(False, False, None))

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
    # P2-07: sentinel must include embedder_model
    assert "embedder_model" in data
    assert data["embedder_model"] == "google_genai:models/gemini-embedding-001"


def test_index_ticker_chunk_doc_ids_are_deterministic():
    """Chunk doc IDs follow the expected {slug}_{form}_{period}_{index:04d} pattern."""
    dao = _mock_dao(sentinel_doc=None)
    retriever = _make_retriever(_stub_config(), dao)
    retriever._check_freshness_and_drift = AsyncMock(return_value=(False, False, None))

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
# P2-07 — Embedder model drift detection
# ---------------------------------------------------------------------------


def test_check_freshness_and_drift_no_drift():
    """_check_freshness_and_drift returns model_drifted=False when models match."""
    model = "google_genai:models/gemini-embedding-001"
    dao = _mock_dao(sentinel_doc=_fresh_sentinel(embedder_model=model))
    retriever = _make_retriever(_stub_config(embedder_model=model), dao)

    is_fresh, model_drifted, stored = asyncio.run(
        retriever._check_freshness_and_drift("aapl", model)
    )
    assert is_fresh is True
    assert model_drifted is False
    assert stored == model


def test_check_freshness_and_drift_detects_drift():
    """_check_freshness_and_drift returns model_drifted=True when models differ."""
    old_model = "google_genai:models/gemini-embedding-001"
    new_model = "openai:text-embedding-3-small"
    dao = _mock_dao(sentinel_doc=_fresh_sentinel(embedder_model=old_model))
    retriever = _make_retriever(_stub_config(embedder_model=new_model), dao)

    is_fresh, model_drifted, stored = asyncio.run(
        retriever._check_freshness_and_drift("aapl", new_model)
    )
    # Sentinel is fresh by timestamp but model has drifted
    assert is_fresh is True
    assert model_drifted is True
    assert stored == old_model


def test_check_freshness_and_drift_missing_sentinel():
    """_check_freshness_and_drift returns (False, False, None) for missing sentinel."""
    dao = _mock_dao(sentinel_doc=None)
    retriever = _make_retriever(_stub_config(), dao)

    is_fresh, model_drifted, stored = asyncio.run(
        retriever._check_freshness_and_drift(
            "aapl", "google_genai:models/gemini-embedding-001"
        )
    )
    assert is_fresh is False
    assert model_drifted is False
    assert stored is None


def test_index_ticker_drift_triggers_purge_and_reindex():
    """When embedder model changes, old chunks are purged and new ones are written."""
    old_model = "google_genai:models/gemini-embedding-001"
    new_model = "openai:text-embedding-3-small"

    # Sentinel says fresh (by date) but model has drifted
    sentinel = _fresh_sentinel(embedder_model=old_model)
    dao = _mock_dao(sentinel_doc=sentinel)
    # Simulate query returning existing chunk docs with _id fields
    existing_chunks = [
        {"_id": "aapl_10k_20240101_0000", "ticker": "AAPL"},
        {"_id": "aapl_10k_20240101_0001", "ticker": "AAPL"},
    ]
    dao.query = AsyncMock(return_value=existing_chunks)

    retriever = _make_retriever(_stub_config(embedder_model=new_model), dao)
    # Force drift path by overriding _check_freshness_and_drift
    retriever._check_freshness_and_drift = AsyncMock(
        return_value=(True, True, old_model)
    )

    new_chunks = _sample_chunks(2)

    import screener.edgar.fetcher as fetcher_mod

    original_get = fetcher_mod.get_filing_chunks
    fetcher_mod.get_filing_chunks = lambda *a, **kw: new_chunks
    try:
        result = asyncio.run(retriever.index_ticker("AAPL", dry_run=False))
    finally:
        fetcher_mod.get_filing_chunks = original_get

    # Deletion calls should have happened for both stale chunks
    assert dao.delete.call_count == 2
    deleted_ids = {c.args[1] for c in dao.delete.call_args_list}
    assert deleted_ids == {"aapl_10k_20240101_0000", "aapl_10k_20240101_0001"}

    # New chunks written (2 chunks + 1 sentinel)
    assert result == 2
    assert dao.set.call_count == 3

    # Sentinel records new model
    sentinel_call = dao.set.call_args_list[-1]
    assert sentinel_call.args[2]["embedder_model"] == new_model


def test_index_ticker_drift_dry_run_skips_purge():
    """dry_run=True with model drift logs intent but does NOT delete existing chunks."""
    old_model = "google_genai:models/gemini-embedding-001"
    new_model = "openai:text-embedding-3-small"

    sentinel = _fresh_sentinel(embedder_model=old_model)
    dao = _mock_dao(sentinel_doc=sentinel)

    retriever = _make_retriever(_stub_config(embedder_model=new_model), dao)
    retriever._check_freshness_and_drift = AsyncMock(
        return_value=(True, True, old_model)
    )

    new_chunks = _sample_chunks(2)

    import screener.edgar.fetcher as fetcher_mod

    original_get = fetcher_mod.get_filing_chunks
    fetcher_mod.get_filing_chunks = lambda *a, **kw: new_chunks
    try:
        result = asyncio.run(retriever.index_ticker("AAPL", dry_run=True))
    finally:
        fetcher_mod.get_filing_chunks = original_get

    # In dry_run mode: no deletes, no writes, result = 0
    dao.delete.assert_not_called()
    dao.set.assert_not_called()
    assert result == 0


def test_index_ticker_chunk_stamped_with_embedder_model():
    """Every chunk doc written to storage includes embedder_model field."""
    model = "google_genai:models/gemini-embedding-001"
    dao = _mock_dao(sentinel_doc=None)
    retriever = _make_retriever(_stub_config(embedder_model=model), dao)
    retriever._check_freshness_and_drift = AsyncMock(return_value=(False, False, None))

    chunks = _sample_chunks(1)

    import screener.edgar.fetcher as fetcher_mod

    original_get = fetcher_mod.get_filing_chunks
    fetcher_mod.get_filing_chunks = lambda *a, **kw: chunks
    try:
        asyncio.run(retriever.index_ticker("AAPL", dry_run=False))
    finally:
        fetcher_mod.get_filing_chunks = original_get

    # First set call is for the chunk doc (before the sentinel)
    chunk_call = dao.set.call_args_list[0]
    written_doc = chunk_call.args[2]
    assert written_doc["embedder_model"] == model


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


# ---------------------------------------------------------------------------
# P2-08 — Section boost
# ---------------------------------------------------------------------------


def test_get_disclosure_chunks_async_section_boost_applied():
    """Chunks whose section matches retrieval_sections get +0.05 score boost."""
    from screener.edgar.retriever import _SECTION_BOOST, get_disclosure_chunks_async

    dao = _mock_dao()
    embedder = _mock_embedder(embedding_dim=4)

    # Two chunks: one in "Risk Factors", one in "MD&A"
    risk_chunk = {
        "ticker": "AAPL",
        "period": "2024-12-31",
        "chunk_index": 0,
        "section": "Risk Factors",
        "text": "Risk factor content here.",
        "_score": 0.80,
    }
    mda_chunk = {
        "ticker": "AAPL",
        "period": "2024-12-31",
        "chunk_index": 1,
        "section": "MD&A",
        "text": "MD&A content here.",
        "_score": 0.75,
    }
    dao.vector_search = AsyncMock(return_value=[risk_chunk, mda_chunk])

    results = asyncio.run(
        get_disclosure_chunks_async(
            "AAPL",
            dao,
            embedder,
            top_k=5,
            threshold=0.5,
            retrieval_sections=["Risk Factors"],
        )
    )

    # Risk Factors chunk should have score boosted by _SECTION_BOOST
    risk_result = next(r for r in results if r["chunk_index"] == 0)
    mda_result = next(r for r in results if r["chunk_index"] == 1)

    assert abs(risk_result["_score"] - (0.80 + _SECTION_BOOST)) < 1e-9
    assert abs(mda_result["_score"] - 0.75) < 1e-9


def test_get_disclosure_chunks_async_no_section_boost_when_sections_empty():
    """Scores are unmodified when retrieval_sections is empty."""
    from screener.edgar.retriever import get_disclosure_chunks_async

    dao = _mock_dao()
    embedder = _mock_embedder(embedding_dim=4)

    chunk = {
        "ticker": "AAPL",
        "period": "2024-12-31",
        "chunk_index": 0,
        "section": "Risk Factors",
        "text": "Risk factor content.",
        "_score": 0.80,
    }
    dao.vector_search = AsyncMock(return_value=[chunk])

    results = asyncio.run(
        get_disclosure_chunks_async(
            "AAPL",
            dao,
            embedder,
            top_k=5,
            threshold=0.5,
            retrieval_sections=[],
        )
    )

    # Score must be unchanged
    assert abs(results[0]["_score"] - 0.80) < 1e-9


def test_get_disclosure_chunks_async_section_boost_changes_ranking():
    """Section boost can promote a lower-scored chunk above a higher-scored one."""
    from screener.edgar.retriever import get_disclosure_chunks_async

    dao = _mock_dao()
    embedder = _mock_embedder(embedding_dim=4)

    # chunk_a has higher raw score but is NOT in a boosted section
    # chunk_b has lower raw score but IS in a boosted section
    chunk_a = {
        "ticker": "AAPL",
        "period": "2024-12-31",
        "chunk_index": 0,
        "section": "Business",
        "text": "Business content " * 20,
        "_score": 0.82,
    }
    chunk_b = {
        "ticker": "AAPL",
        "period": "2024-12-31",
        "chunk_index": 1,
        "section": "Risk Factors",
        "text": "Risk factor content " * 20,
        "_score": 0.80,
    }
    dao.vector_search = AsyncMock(return_value=[chunk_a, chunk_b])

    results = asyncio.run(
        get_disclosure_chunks_async(
            "AAPL",
            dao,
            embedder,
            top_k=5,
            threshold=0.5,
            retrieval_sections=["Risk Factors"],
        )
    )

    # After boost: chunk_b score = 0.85, chunk_a score = 0.82 → chunk_b ranks first
    assert results[0]["chunk_index"] == 1
    assert results[1]["chunk_index"] == 0


# ---------------------------------------------------------------------------
# P2-10 — Text-hash deduplication
# ---------------------------------------------------------------------------


def test_get_disclosure_chunks_async_deduplicates_identical_text():
    """Chunks with identical normalised text produce only one result (highest score)."""
    from screener.edgar.retriever import get_disclosure_chunks_async

    dao = _mock_dao()
    embedder = _mock_embedder(embedding_dim=4)

    same_text = (
        "The company faces significant competitive risks in its core markets. " * 10
    )

    chunk_q1 = {
        "ticker": "AAPL",
        "period": "2024-03-31",
        "chunk_index": 0,
        "section": "Risk Factors",
        "text": same_text,
        "_score": 0.85,
    }
    chunk_q2 = {
        "ticker": "AAPL",
        "period": "2024-06-30",
        "chunk_index": 0,
        "section": "Risk Factors",
        "text": same_text,
        "_score": 0.80,
    }
    # The DAO dedup key is period+chunk_index so these are different keys;
    # the text-hash dedup in retriever should catch them.
    dao.vector_search = AsyncMock(return_value=[chunk_q1, chunk_q2])

    results = asyncio.run(
        get_disclosure_chunks_async(
            "AAPL",
            dao,
            embedder,
            top_k=5,
            threshold=0.5,
        )
    )

    # Only one chunk should survive (the higher-scored one)
    assert len(results) == 1
    assert results[0]["_score"] == 0.85


def test_get_disclosure_chunks_async_keeps_distinct_texts():
    """Chunks with different content are both returned (no false-positive dedup)."""
    from screener.edgar.retriever import get_disclosure_chunks_async

    dao = _mock_dao()
    embedder = _mock_embedder(embedding_dim=4)

    chunk_a = {
        "ticker": "AAPL",
        "period": "2024-03-31",
        "chunk_index": 0,
        "section": "Risk Factors",
        "text": "Unique risk factor text about competition. " * 10,
        "_score": 0.85,
    }
    chunk_b = {
        "ticker": "AAPL",
        "period": "2024-06-30",
        "chunk_index": 0,
        "section": "MD&A",
        "text": "Revenue grew by 12 percent year over year. " * 10,
        "_score": 0.80,
    }
    dao.vector_search = AsyncMock(return_value=[chunk_a, chunk_b])

    results = asyncio.run(
        get_disclosure_chunks_async(
            "AAPL",
            dao,
            embedder,
            top_k=5,
            threshold=0.5,
        )
    )

    assert len(results) == 2


def test_dedup_normalise_and_hash_are_case_punctuation_insensitive():
    """_normalise_text and _text_hash treat casing/punctuation differences as equal."""
    from screener.edgar.retriever import _normalise_text, _text_hash

    a = "The company faces significant, competitive risks."
    b = "the company faces significant competitive risks"
    assert _normalise_text(a) == _normalise_text(b)
    assert _text_hash(a) == _text_hash(b)


def test_dedup_different_texts_produce_different_hashes():
    """Two clearly different texts produce different MD5 hashes."""
    from screener.edgar.retriever import _text_hash

    h1 = _text_hash("Revenue grew by twelve percent.")
    h2 = _text_hash("Operating expenses increased by eight percent.")
    assert h1 != h2
