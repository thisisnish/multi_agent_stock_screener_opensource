"""
tests/eval/test_loader.py — Unit tests for screener/eval/loader.py.

Covers:
- prior_month_id() basic month decrement
- prior_month_id() wraps December when month is January
- prior_month_id() raises ValueError on malformed input
- fetch_eval_context_async() returns None when eval doc does not exist
- fetch_eval_context_async() returns None when eval_context key is absent
- fetch_eval_context_async() returns eval_context dict when doc exists
- fetch_eval_context_async() returns None (graceful degrade) on storage error
- fetch_eval_context_async() queries the correct EVAL collection doc ID

No real GCP calls — StorageDAO.get is fully mocked.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

# ---------------------------------------------------------------------------
# Ensure project root is importable (mirrors conftest.py behaviour)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# ---------------------------------------------------------------------------
# prior_month_id
# ---------------------------------------------------------------------------


class TestPriorMonthId:
    def test_basic_decrement(self):
        from screener.eval.loader import prior_month_id

        assert prior_month_id("2026-04") == "2026-03"

    def test_january_wraps_to_prior_december(self):
        from screener.eval.loader import prior_month_id

        assert prior_month_id("2026-01") == "2025-12"

    def test_december_decrements_within_year(self):
        from screener.eval.loader import prior_month_id

        assert prior_month_id("2026-12") == "2026-11"

    def test_february_to_january(self):
        from screener.eval.loader import prior_month_id

        assert prior_month_id("2026-02") == "2026-01"

    def test_zero_padded_output(self):
        """Output must always be zero-padded to "YYYY-MM" format."""
        from screener.eval.loader import prior_month_id

        result = prior_month_id("2026-01")
        assert result == "2025-12"
        # Verify the month portion is always 2 digits
        month_part = result.split("-")[1]
        assert len(month_part) == 2

    def test_invalid_format_raises_value_error(self):
        from screener.eval.loader import prior_month_id

        with pytest.raises(ValueError, match="YYYY-MM"):
            prior_month_id("2026/04")

    def test_missing_month_raises_value_error(self):
        from screener.eval.loader import prior_month_id

        with pytest.raises(ValueError, match="YYYY-MM"):
            prior_month_id("2026")

    def test_plain_date_raises_value_error(self):
        from screener.eval.loader import prior_month_id

        with pytest.raises(ValueError, match="YYYY-MM"):
            prior_month_id("2026-04-01")


# ---------------------------------------------------------------------------
# fetch_eval_context_async — DAO stub helpers
# ---------------------------------------------------------------------------


def _make_dao(get_return_value) -> MagicMock:
    """Return a mock StorageDAO whose get() coroutine returns get_return_value."""
    dao = MagicMock()
    dao.get = AsyncMock(return_value=get_return_value)
    return dao


def _make_error_dao(exc: Exception) -> MagicMock:
    """Return a mock StorageDAO whose get() raises exc."""
    dao = MagicMock()
    dao.get = AsyncMock(side_effect=exc)
    return dao


# ---------------------------------------------------------------------------
# fetch_eval_context_async
# ---------------------------------------------------------------------------


class TestFetchEvalContextAsync:
    def _run(self, coro):
        import asyncio

        return asyncio.run(coro)

    def test_returns_none_when_doc_does_not_exist(self):
        from screener.eval.loader import fetch_eval_context_async

        dao = _make_dao(None)
        result = self._run(fetch_eval_context_async(dao, "2026-04"))
        assert result is None

    def test_returns_none_when_eval_context_key_absent(self):
        from screener.eval.loader import fetch_eval_context_async

        # Doc exists but no eval_context key
        dao = _make_dao({"month_id": "2026-03", "metrics": {}})
        result = self._run(fetch_eval_context_async(dao, "2026-04"))
        assert result is None

    def test_returns_eval_context_when_present(self):
        from screener.eval.loader import fetch_eval_context_async

        eval_ctx = {
            "total_picks_scored": 5,
            "overall_accuracy": 60.0,
            "bull_accuracy": 70.0,
            "bear_accuracy": 50.0,
            "directional_bias": "bullish",
            "confidence_calibration": 10.0,
            "systematic_issues": [],
            "acid_test": {},
        }
        doc = {"month_id": "2026-03", "eval_context": eval_ctx}
        dao = _make_dao(doc)
        result = self._run(fetch_eval_context_async(dao, "2026-04"))
        assert result == eval_ctx

    def test_returns_none_on_storage_error(self):
        """Graceful degrade: storage failure must not propagate."""
        from screener.eval.loader import fetch_eval_context_async

        dao = _make_error_dao(RuntimeError("connection refused"))
        result = self._run(fetch_eval_context_async(dao, "2026-04"))
        assert result is None

    def test_queries_prior_month_doc_id(self):
        """Ensure the correct (prior month) doc ID is passed to dao.get."""
        from screener.eval.loader import fetch_eval_context_async
        from screener.lib.storage.schema import EVAL, eval_doc_id

        dao = _make_dao(None)
        self._run(fetch_eval_context_async(dao, "2026-04"))

        # Called once, with EVAL collection and the prior-month doc ID
        dao.get.assert_awaited_once()
        call_args = dao.get.await_args[0]
        assert call_args[0] == EVAL
        assert call_args[1] == eval_doc_id(2026, 3)  # prior month of 2026-04

    def test_queries_december_of_prior_year_for_january(self):
        """January 2026 → prior month is December 2025."""
        from screener.eval.loader import fetch_eval_context_async
        from screener.lib.storage.schema import eval_doc_id

        dao = _make_dao(None)
        self._run(fetch_eval_context_async(dao, "2026-01"))

        call_args = dao.get.await_args[0]
        assert call_args[1] == eval_doc_id(2025, 12)

    def test_empty_eval_context_dict_returns_none(self):
        """An eval_context that is empty dict (falsy) should return None."""
        from screener.eval.loader import fetch_eval_context_async

        doc = {"month_id": "2026-03", "eval_context": {}}
        dao = _make_dao(doc)
        result = self._run(fetch_eval_context_async(dao, "2026-04"))
        assert result is None
