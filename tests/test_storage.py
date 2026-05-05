"""
tests/test_storage.py — Unit tests for screener/lib/storage/

Covers:
- FirestoreDAO.get: hit and miss
- FirestoreDAO.set: upsert delegates to Firestore doc.set()
- FirestoreDAO.delete: found and not-found (no-op) cases
- FirestoreDAO.query: filters applied as chained where() clauses
- FirestoreDAO.vector_search: find_nearest called with correct params;
  threshold filtering; _score injected
- FirestoreDAO.close: idempotent (safe to call twice)
- get_storage_dao factory: routes "firestore" to FirestoreDAO
- get_storage_dao factory: unsupported provider raises StorageConfigError
- schema helpers: ticker_to_slug, doc-ID builders, current_week_id, current_quarter_id

No real GCP calls are made — the AsyncClient is fully mocked.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from screener.lib.config_loader import (
    AppConfig,
    EdgarConfig,
    EmailConfig,
    FirestoreConfig,
    LLMConfig,
    NotificationsConfig,
    ScreenerConfig,
    SignalsConfig,
    StorageConfig,
)
from screener.lib.storage import StorageConfigError, get_storage_dao
from screener.lib.storage.firestore import FirestoreDAO
from screener.lib.storage.schema import (
    CHUNKS,
    EVAL,
    EVENTS,
    MEMORY,
    PERFORMANCE,
    PICKS,
    TICKERS,
    chunk_doc_id,
    current_month_id,
    current_quarter_id,
    current_week_id,
    eval_doc_id,
    memory_collection_path,
    memory_doc_id,
    perf_snapshot_doc_id,
    pick_ledger_doc_id,
    picks_doc_id,
    screening_doc_id,
    ticker_to_slug,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app_config(provider: str = "firestore") -> AppConfig:
    llm = LLMConfig(
        model="anthropic:claude-haiku-4-5-20251001",
        embedder_model="google_genai:models/gemini-embedding-001",
    )
    storage = StorageConfig(
        provider=provider,
        firestore=FirestoreConfig(project_id="test-project", database="test-db"),
    )
    return AppConfig(
        llm=llm,
        storage=storage,
        signals=SignalsConfig(),
        screener=ScreenerConfig(),
        notifications=NotificationsConfig(
            email=EmailConfig(enabled=False, recipients=[])
        ),
        edgar=EdgarConfig(),
    )


def _make_snap(exists: bool, data: dict | None = None, distance: float | None = None):
    """Build a mock Firestore DocumentSnapshot."""
    snap = MagicMock()
    snap.exists = exists
    snap.to_dict.return_value = data or {}
    if distance is not None:
        snap.distance = distance
    else:
        # Simulate snap having no .distance attribute (non-vector query result)
        if hasattr(snap, "distance"):
            del snap.distance
    return snap


def _make_dao() -> tuple[FirestoreDAO, MagicMock]:
    """Construct a FirestoreDAO with a fully mocked AsyncClient.

    Returns (dao, mock_client) so tests can configure return values.
    """
    mock_client = MagicMock()

    with patch(
        "screener.lib.storage.firestore.firestore.AsyncClient",
        return_value=mock_client,
    ):
        dao = FirestoreDAO(project_id="test-project", database="test-db")

    return dao, mock_client


# ---------------------------------------------------------------------------
# FirestoreDAO.get
# ---------------------------------------------------------------------------


class TestFirestoreDAOGet:
    def test_get_returns_dict_on_hit(self):
        dao, mock_client = _make_dao()

        snap = _make_snap(exists=True, data={"symbol": "AAPL", "score": 72.4})
        doc_ref = MagicMock()
        doc_ref.get = AsyncMock(return_value=snap)
        mock_client.collection.return_value.document.return_value = doc_ref

        result = asyncio.run(dao.get(TICKERS, "AAPL"))

        assert result == {"symbol": "AAPL", "score": 72.4}
        mock_client.collection.assert_called_with(TICKERS)
        mock_client.collection.return_value.document.assert_called_with("AAPL")

    def test_get_returns_none_on_miss(self):
        dao, mock_client = _make_dao()

        snap = _make_snap(exists=False)
        doc_ref = MagicMock()
        doc_ref.get = AsyncMock(return_value=snap)
        mock_client.collection.return_value.document.return_value = doc_ref

        result = asyncio.run(dao.get(TICKERS, "ZZZZ"))

        assert result is None


# ---------------------------------------------------------------------------
# FirestoreDAO.set
# ---------------------------------------------------------------------------


class TestFirestoreDAOSet:
    def test_set_calls_doc_set_with_data(self):
        dao, mock_client = _make_dao()

        doc_ref = MagicMock()
        doc_ref.set = AsyncMock(return_value=None)
        mock_client.collection.return_value.document.return_value = doc_ref

        payload = {"symbol": "TSLA", "score": 55.0}
        asyncio.run(dao.set(TICKERS, "TSLA", payload))

        doc_ref.set.assert_awaited_once_with(payload)

    def test_set_uses_correct_collection_and_doc_id(self):
        dao, mock_client = _make_dao()

        doc_ref = MagicMock()
        doc_ref.set = AsyncMock(return_value=None)
        mock_client.collection.return_value.document.return_value = doc_ref

        col_path = memory_collection_path("AAPL")
        asyncio.run(
            dao.set(col_path, "2026-04", {"ticker": "AAPL", "month_id": "2026-04"})
        )

        mock_client.collection.assert_called_with(col_path)
        mock_client.collection.return_value.document.assert_called_with("2026-04")


# ---------------------------------------------------------------------------
# FirestoreDAO.delete
# ---------------------------------------------------------------------------


class TestFirestoreDAODelete:
    def test_delete_calls_doc_delete(self):
        dao, mock_client = _make_dao()

        doc_ref = MagicMock()
        doc_ref.delete = AsyncMock(return_value=None)
        mock_client.collection.return_value.document.return_value = doc_ref

        asyncio.run(dao.delete(PICKS, "picks_202618"))

        doc_ref.delete.assert_awaited_once()

    def test_delete_is_noop_on_not_found(self):
        from google.api_core.exceptions import NotFound

        dao, mock_client = _make_dao()

        doc_ref = MagicMock()
        doc_ref.delete = AsyncMock(side_effect=NotFound("not found"))
        mock_client.collection.return_value.document.return_value = doc_ref

        # Must not raise
        asyncio.run(dao.delete(PICKS, "nonexistent_doc"))


# ---------------------------------------------------------------------------
# FirestoreDAO.query
# ---------------------------------------------------------------------------


class TestFirestoreDAOQuery:
    def test_query_chains_where_clauses(self):
        dao, mock_client = _make_dao()

        # Build a chain of .where() returning mock objects that themselves
        # have .where() and .get()
        final_query = MagicMock()
        final_query.get = AsyncMock(
            return_value=[
                _make_snap(exists=True, data={"status": "active", "ticker": "AAPL"})
            ]
        )

        where_chain = MagicMock()
        where_chain.where.return_value = final_query
        mock_client.collection.return_value.where.return_value = where_chain

        result = asyncio.run(
            dao.query(PERFORMANCE, {"status": "active", "ticker": "AAPL"})
        )

        # Both where clauses were applied (order-agnostic — just check the chain fired)
        assert len(result) == 1
        assert result[0]["status"] == "active"

    def test_query_empty_filters_calls_get_on_collection(self):
        dao, mock_client = _make_dao()

        snap1 = _make_snap(exists=True, data={"symbol": "AAPL"})
        snap2 = _make_snap(exists=True, data={"symbol": "MSFT"})
        mock_client.collection.return_value.get = AsyncMock(return_value=[snap1, snap2])

        result = asyncio.run(dao.query(TICKERS, {}))

        assert len(result) == 2

    def test_query_skips_non_existing_snaps(self):
        dao, mock_client = _make_dao()

        snap_ok = _make_snap(exists=True, data={"symbol": "AAPL"})
        snap_bad = _make_snap(exists=False)
        mock_client.collection.return_value.get = AsyncMock(
            return_value=[snap_ok, snap_bad]
        )

        result = asyncio.run(dao.query(TICKERS, {}))

        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"


# ---------------------------------------------------------------------------
# FirestoreDAO.vector_search
# ---------------------------------------------------------------------------


class TestFirestoreDAOVectorSearch:
    def _setup_vector_query(self, mock_client, snaps: list):
        vector_query = MagicMock()
        vector_query.get = AsyncMock(return_value=snaps)
        mock_client.collection.return_value.find_nearest.return_value = vector_query
        return vector_query

    def test_vector_search_calls_find_nearest_with_correct_params(self):
        from google.cloud.firestore_v1.base_vector_query import DistanceMeasure
        from google.cloud.firestore_v1.vector import Vector

        dao, mock_client = _make_dao()
        self._setup_vector_query(mock_client, [])

        embedding = [0.1, 0.2, 0.3]
        asyncio.run(dao.vector_search(CHUNKS, embedding, top_k=5, threshold=0.7))

        mock_client.collection.return_value.find_nearest.assert_called_once_with(
            vector_field="embedding",
            query_vector=Vector(embedding),
            distance_measure=DistanceMeasure.COSINE,
            limit=5,
        )

    def test_vector_search_filters_below_threshold(self):
        dao, mock_client = _make_dao()

        # distance=0.1 → score=0.9 (above threshold 0.7) — should be kept
        snap_keep = _make_snap(exists=True, data={"text": "keep me"}, distance=0.1)
        # distance=0.5 → score=0.5 (below threshold 0.7) — should be dropped
        snap_drop = _make_snap(exists=True, data={"text": "drop me"}, distance=0.5)

        self._setup_vector_query(mock_client, [snap_keep, snap_drop])

        results = asyncio.run(
            dao.vector_search(CHUNKS, [0.1, 0.2], top_k=5, threshold=0.7)
        )

        assert len(results) == 1
        assert results[0]["text"] == "keep me"

    def test_vector_search_injects_score(self):
        dao, mock_client = _make_dao()

        snap = _make_snap(exists=True, data={"text": "hello"}, distance=0.2)
        self._setup_vector_query(mock_client, [snap])

        results = asyncio.run(dao.vector_search(CHUNKS, [0.1], top_k=3, threshold=0.5))

        assert "_score" in results[0]
        assert abs(results[0]["_score"] - 0.8) < 1e-5

    def test_vector_search_returns_empty_on_no_results(self):
        dao, mock_client = _make_dao()
        self._setup_vector_query(mock_client, [])

        results = asyncio.run(
            dao.vector_search(CHUNKS, [0.0, 1.0], top_k=5, threshold=0.7)
        )

        assert results == []

    def test_vector_search_skips_non_existing_snaps(self):
        dao, mock_client = _make_dao()

        snap_ok = _make_snap(exists=True, data={"text": "ok"}, distance=0.05)
        snap_bad = _make_snap(exists=False)
        self._setup_vector_query(mock_client, [snap_ok, snap_bad])

        results = asyncio.run(dao.vector_search(CHUNKS, [0.1], top_k=5, threshold=0.5))

        assert len(results) == 1

    def test_vector_search_fallback_on_dimension_error(self):
        """InvalidArgument('Vectors must be at most …') triggers brute-force path."""
        from google.api_core.exceptions import InvalidArgument

        dao, mock_client = _make_dao()

        # Native query raises the dimension error.
        vector_query = MagicMock()
        vector_query.get = AsyncMock(
            side_effect=InvalidArgument("Vectors must be at most 2048 dimensions.")
        )
        mock_client.collection.return_value.find_nearest.return_value = vector_query

        # Brute-force scan returns one matching doc.
        # embedding must be same dimension as query (2) and have cosine ~ 1.0
        q = [1.0, 0.0]
        stored_doc = {"text": "match", "ticker": "AAPL", "embedding": [1.0, 0.0]}
        snap = _make_snap(exists=True, data=stored_doc)
        mock_client.collection.return_value.get = AsyncMock(return_value=[snap])

        results = asyncio.run(dao.vector_search(CHUNKS, q, top_k=5, threshold=0.5))

        assert len(results) == 1
        assert results[0]["text"] == "match"
        assert "_score" in results[0]

    def test_vector_search_reraises_other_invalid_argument(self):
        """InvalidArgument without the dimension message is not swallowed."""
        from google.api_core.exceptions import InvalidArgument

        dao, mock_client = _make_dao()

        vector_query = MagicMock()
        vector_query.get = AsyncMock(
            side_effect=InvalidArgument("Some other argument error.")
        )
        mock_client.collection.return_value.find_nearest.return_value = vector_query

        with pytest.raises(InvalidArgument, match="Some other argument error"):
            asyncio.run(dao.vector_search(CHUNKS, [0.1, 0.2], top_k=5, threshold=0.5))

    def test_vector_search_fallback_applies_filters(self):
        """filters dict is passed as where() clauses in the brute-force scan."""
        from google.api_core.exceptions import InvalidArgument

        dao, mock_client = _make_dao()

        vector_query = MagicMock()
        vector_query.get = AsyncMock(
            side_effect=InvalidArgument("Vectors must be at most 2048 dimensions.")
        )
        mock_client.collection.return_value.find_nearest.return_value = vector_query

        q = [1.0, 0.0]
        stored_doc = {"text": "filtered", "ticker": "AAPL", "embedding": [1.0, 0.0]}
        snap = _make_snap(exists=True, data=stored_doc)

        # Simulate chained .where().get() returning one doc.
        where_result = MagicMock()
        where_result.get = AsyncMock(return_value=[snap])
        mock_client.collection.return_value.where.return_value = where_result

        results = asyncio.run(
            dao.vector_search(
                CHUNKS, q, top_k=5, threshold=0.5, filters={"ticker": "AAPL"}
            )
        )

        # Confirm where() was called with the filter field and value.
        mock_client.collection.return_value.where.assert_called_once_with(
            "ticker", "==", "AAPL"
        )
        assert len(results) == 1

    def test_vector_search_fallback_skips_dim_mismatch(self):
        """Stored embeddings with wrong dimensionality are silently skipped."""
        from google.api_core.exceptions import InvalidArgument

        dao, mock_client = _make_dao()

        vector_query = MagicMock()
        vector_query.get = AsyncMock(
            side_effect=InvalidArgument("Vectors must be at most 2048 dimensions.")
        )
        mock_client.collection.return_value.find_nearest.return_value = vector_query

        q = [1.0, 0.0]
        good_doc = {"text": "good", "embedding": [1.0, 0.0]}
        bad_doc = {
            "text": "bad_dim",
            "embedding": [1.0, 0.0, 0.0],
        }  # 3-dim vs 2-dim query
        snaps = [
            _make_snap(exists=True, data=good_doc),
            _make_snap(exists=True, data=bad_doc),
        ]
        mock_client.collection.return_value.get = AsyncMock(return_value=snaps)

        results = asyncio.run(dao.vector_search(CHUNKS, q, top_k=5, threshold=0.0))

        assert len(results) == 1
        assert results[0]["text"] == "good"

    def test_vector_search_fallback_sorts_and_caps_top_k(self):
        """Brute-force results are sorted by descending similarity and capped at top_k."""
        from google.api_core.exceptions import InvalidArgument

        dao, mock_client = _make_dao()

        vector_query = MagicMock()
        vector_query.get = AsyncMock(
            side_effect=InvalidArgument("Vectors must be at most 2048 dimensions.")
        )
        mock_client.collection.return_value.find_nearest.return_value = vector_query

        # Query along [1,0]; cosine against each doc is proportional to first component.
        q = [1.0, 0.0]
        docs = [
            {"text": "medium", "embedding": [0.7, 0.714]},  # cosine ≈ 0.70
            {"text": "high", "embedding": [0.99, 0.141]},  # cosine ≈ 0.99
            {"text": "low", "embedding": [0.5, 0.866]},  # cosine ≈ 0.50
        ]
        snaps = [_make_snap(exists=True, data=d) for d in docs]
        mock_client.collection.return_value.get = AsyncMock(return_value=snaps)

        results = asyncio.run(dao.vector_search(CHUNKS, q, top_k=2, threshold=0.0))

        assert len(results) == 2
        assert results[0]["text"] == "high"
        assert results[1]["text"] == "medium"


# ---------------------------------------------------------------------------
# FirestoreDAO.close
# ---------------------------------------------------------------------------


class TestFirestoreDAOClose:
    def test_close_calls_client_close(self):
        dao, mock_client = _make_dao()
        mock_client.close = MagicMock()

        asyncio.run(dao.close())

        mock_client.close.assert_called_once()

    def test_close_is_idempotent(self):
        dao, mock_client = _make_dao()
        mock_client.close = MagicMock()

        asyncio.run(dao.close())
        asyncio.run(dao.close())

        # close() on the underlying client should only be called once
        mock_client.close.assert_called_once()


# ---------------------------------------------------------------------------
# get_storage_dao factory
# ---------------------------------------------------------------------------


class TestGetStorageDAOFactory:
    def test_firestore_provider_returns_firestore_dao(self):
        cfg = _make_app_config(provider="firestore")

        with patch(
            "screener.lib.storage.firestore.firestore.AsyncClient",
            return_value=MagicMock(),
        ):
            dao = get_storage_dao(cfg)

        assert isinstance(dao, FirestoreDAO)

    def test_firestore_dao_receives_correct_project_and_database(self):
        cfg = _make_app_config(provider="firestore")

        with patch(
            "screener.lib.storage.firestore.firestore.AsyncClient",
            return_value=MagicMock(),
        ) as mock_async_client:
            get_storage_dao(cfg)

        mock_async_client.assert_called_once_with(
            project="test-project",
            database="test-db",
        )

    def test_unsupported_provider_raises_storage_config_error(self):
        # Manually construct an AppConfig with an unusual provider.
        # We bypass the validator by patching the StorageConfig provider field
        # after construction — simpler to just test the factory guard directly.
        cfg = _make_app_config(provider="firestore")

        # Monkeypatch the provider on the already-validated config to simulate
        # a future provider that hasn't been implemented yet.
        object.__setattr__(cfg.storage, "provider", "opensearch")

        with pytest.raises(StorageConfigError, match="opensearch"):
            get_storage_dao(cfg)

    def test_unsupported_provider_error_message_mentions_firestore(self):
        cfg = _make_app_config(provider="firestore")
        object.__setattr__(cfg.storage, "provider", "s3")

        with pytest.raises(StorageConfigError, match="firestore"):
            get_storage_dao(cfg)


# ---------------------------------------------------------------------------
# schema helpers
# ---------------------------------------------------------------------------


class TestSchemaCollectionConstants:
    def test_collection_constants_have_expected_values(self):
        assert TICKERS == "tickers"
        assert MEMORY == "memory"
        assert PICKS == "picks"
        assert PERFORMANCE == "performance"
        assert CHUNKS == "chunks"
        assert EVAL == "eval"
        assert EVENTS == "events"

    def test_memory_collection_path_returns_subcollection_path(self):
        assert memory_collection_path("AAPL") == "tickers/AAPL/memory"

    def test_memory_collection_path_uppercases_ticker(self):
        assert memory_collection_path("aapl") == "tickers/AAPL/memory"

    def test_memory_collection_path_handles_special_chars(self):
        # Ticker slugging is NOT applied here — raw upper() only, matching spec
        assert memory_collection_path("BRK.B") == "tickers/BRK.B/memory"


class TestTickerToSlug:
    def test_lowercase(self):
        assert ticker_to_slug("AAPL") == "aapl"

    def test_dot_replaced_with_underscore(self):
        assert ticker_to_slug("BRK.B") == "brk_b"

    def test_hyphen_replaced_with_underscore(self):
        assert ticker_to_slug("BF-B") == "bf_b"

    def test_already_lowercase_unchanged(self):
        assert ticker_to_slug("msft") == "msft"


class TestDocIdHelpers:
    def test_screening_doc_id_strips_dashes(self):
        assert screening_doc_id("2026-04-29") == "screening_20260429"

    def test_picks_doc_id(self):
        assert picks_doc_id("202618") == "picks_202618"

    def test_perf_snapshot_doc_id(self):
        assert perf_snapshot_doc_id("202618") == "perf_202618"

    def test_pick_ledger_doc_id_uppercases_ticker(self):
        assert pick_ledger_doc_id("aapl", "202618") == "AAPL_202618"

    def test_memory_doc_id_returns_month_id_unchanged(self):
        assert memory_doc_id("2026-04") == "2026-04"

    def test_memory_doc_id_december(self):
        assert memory_doc_id("2026-12") == "2026-12"

    def test_chunk_doc_id(self):
        assert chunk_doc_id("AAPL", 42) == "aapl_42"

    def test_eval_doc_id_zero_pads_month(self):
        assert eval_doc_id(2026, 4) == "eval_202604"

    def test_eval_doc_id_december(self):
        assert eval_doc_id(2026, 12) == "eval_202612"


class TestCurrentWeekId:
    def test_returns_six_digit_string(self):
        wid = current_week_id()
        assert len(wid) == 6
        assert wid.isdigit()

    def test_uses_provided_datetime(self):
        dt = datetime(2026, 4, 29, tzinfo=timezone.utc)  # ISO week 18 of 2026
        assert current_week_id(dt) == "202618"


class TestCurrentMonthId:
    def test_returns_yyyy_mm_format(self):
        mid = current_month_id()
        assert len(mid) == 7
        assert mid[4] == "-"
        assert mid[:4].isdigit()
        assert mid[5:].isdigit()

    def test_uses_provided_datetime(self):
        dt = datetime(2026, 4, 29, tzinfo=timezone.utc)
        assert current_month_id(dt) == "2026-04"

    def test_zero_pads_month(self):
        dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert current_month_id(dt) == "2026-01"

    def test_december(self):
        dt = datetime(2026, 12, 31, tzinfo=timezone.utc)
        assert current_month_id(dt) == "2026-12"


class TestCurrentQuarterId:
    def test_q1(self):
        dt = datetime(2026, 2, 15, tzinfo=timezone.utc)
        assert current_quarter_id(dt) == "2026Q1"

    def test_q2(self):
        dt = datetime(2026, 4, 29, tzinfo=timezone.utc)
        assert current_quarter_id(dt) == "2026Q2"

    def test_q3(self):
        dt = datetime(2026, 8, 1, tzinfo=timezone.utc)
        assert current_quarter_id(dt) == "2026Q3"

    def test_q4(self):
        dt = datetime(2026, 11, 30, tzinfo=timezone.utc)
        assert current_quarter_id(dt) == "2026Q4"
