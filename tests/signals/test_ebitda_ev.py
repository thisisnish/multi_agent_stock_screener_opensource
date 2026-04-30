"""
tests/signals/test_ebitda_ev.py — Unit tests for screener/metrics/ebitda_ev.py.

All yf.Ticker calls are patched — no network I/O.
write_quarterly_signals uses AsyncMock DAO — no Firestore I/O.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from screener.metrics.ebitda_ev import (
    EBITDA_EV_CAP,
    EBITDA_SIGNAL_KEY,
    QUARTERLY_COLLECTION,
    _fetch_one,
    fetch_ebitda_ev,
    write_quarterly_signals,
)

MODULE = "screener.metrics.ebitda_ev"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ticker_mock(info: dict) -> MagicMock:
    mock = MagicMock()
    mock.info = info
    return mock


def _patch_ticker(info: dict):
    return patch(f"{MODULE}.yf.Ticker", return_value=_ticker_mock(info))


# ---------------------------------------------------------------------------
# _fetch_one unit tests
# ---------------------------------------------------------------------------

class TestFetchOne:
    def test_normal_case(self):
        with _patch_ticker({"ebitda": 20_000_000, "enterpriseValue": 200_000_000, "mostRecentQuarter": 1700000000}):
            result = _fetch_one("AAPL")
        assert result["skipped"] is False
        assert result["ebitda_ev"] == pytest.approx(0.10)
        assert result["ebitda"] == 20_000_000.0
        assert result["enterprise_value"] == 200_000_000.0
        assert result["skip_reason"] is None

    def test_negative_ebitda_clips_to_zero_yield_but_preserves_raw(self):
        with _patch_ticker({"ebitda": -5_000_000, "enterpriseValue": 100_000_000}):
            result = _fetch_one("LOSS")
        assert result["skipped"] is False
        assert result["ebitda_ev"] == pytest.approx(0.0)
        assert result["ebitda"] == -5_000_000.0

    def test_zero_ebitda_gives_zero_ratio(self):
        with _patch_ticker({"ebitda": 0, "enterpriseValue": 100_000_000}):
            result = _fetch_one("ZERO")
        assert result["skipped"] is False
        assert result["ebitda_ev"] == pytest.approx(0.0)

    def test_ebitda_ev_cap_applied(self):
        # ebitda=80M, ev=100M → raw=0.80 > EBITDA_EV_CAP(0.50) → capped
        with _patch_ticker({"ebitda": 80_000_000, "enterpriseValue": 100_000_000}):
            result = _fetch_one("CAP")
        assert result["skipped"] is False
        assert result["ebitda_ev"] == pytest.approx(EBITDA_EV_CAP)

    def test_none_ebitda_skipped(self):
        with _patch_ticker({"ebitda": None, "enterpriseValue": 100_000_000}):
            result = _fetch_one("AAPL")
        assert result["skipped"] is True
        assert "ebitda" in result["skip_reason"].lower()

    def test_none_ev_skipped(self):
        with _patch_ticker({"ebitda": 10_000_000, "enterpriseValue": None}):
            result = _fetch_one("AAPL")
        assert result["skipped"] is True
        assert "enterpriseValue" in result["skip_reason"]

    def test_ev_zero_skipped_with_reason(self):
        with _patch_ticker({"ebitda": 10_000_000, "enterpriseValue": 0}):
            result = _fetch_one("AAPL")
        assert result["skipped"] is True
        assert "<= 0" in result["skip_reason"]

    def test_negative_ev_skipped(self):
        with _patch_ticker({"ebitda": 10_000_000, "enterpriseValue": -1_000_000}):
            result = _fetch_one("AAPL")
        assert result["skipped"] is True
        assert "<= 0" in result["skip_reason"]

    def test_most_recent_quarter_stored_correctly(self):
        mrq = 1700000000
        with _patch_ticker({"ebitda": 10_000_000, "enterpriseValue": 100_000_000, "mostRecentQuarter": mrq}):
            result = _fetch_one("AAPL")
        assert result["most_recent_quarter"] == int(float(mrq))

    def test_most_recent_quarter_none_when_missing(self):
        with _patch_ticker({"ebitda": 10_000_000, "enterpriseValue": 100_000_000}):
            result = _fetch_one("AAPL")
        assert result["most_recent_quarter"] is None

    def test_exception_returns_skipped(self):
        with patch(f"{MODULE}.yf.Ticker", side_effect=RuntimeError("network error")):
            result = _fetch_one("AAPL")
        assert result["skipped"] is True
        assert "fetch error" in result["skip_reason"]

    def test_all_result_keys_present(self):
        with _patch_ticker({"ebitda": 10_000_000, "enterpriseValue": 100_000_000}):
            result = _fetch_one("AAPL")
        expected = {"ebitda_ev", "ebitda", "enterprise_value", "most_recent_quarter", "skipped", "skip_reason"}
        assert set(result.keys()) == expected

    def test_all_result_keys_present_on_skip(self):
        with _patch_ticker({"ebitda": None, "enterpriseValue": 100_000_000}):
            result = _fetch_one("AAPL")
        expected = {"ebitda_ev", "ebitda", "enterprise_value", "most_recent_quarter", "skipped", "skip_reason"}
        assert set(result.keys()) == expected


# ---------------------------------------------------------------------------
# fetch_ebitda_ev integration-level unit tests
# ---------------------------------------------------------------------------

class TestFetchEbitdaEv:
    def test_returns_all_tickers(self):
        tickers = ["AAPL", "MSFT", "GOOG"]
        info = {"ebitda": 10_000_000, "enterpriseValue": 100_000_000}
        with _patch_ticker(info):
            results = fetch_ebitda_ev(tickers)
        assert set(results.keys()) == set(tickers)

    def test_empty_list_returns_empty_dict(self):
        results = fetch_ebitda_ev([])
        assert results == {}

    def test_no_sleep_for_single_batch(self):
        tickers = [f"T{i}" for i in range(10)]
        info = {"ebitda": 10_000_000, "enterpriseValue": 100_000_000}
        with _patch_ticker(info), patch(f"{MODULE}.time.sleep") as mock_sleep:
            fetch_ebitda_ev(tickers)
        mock_sleep.assert_not_called()

    def test_sleep_between_batches_for_51_tickers(self):
        tickers = [f"T{i}" for i in range(51)]
        info = {"ebitda": 10_000_000, "enterpriseValue": 100_000_000}
        with _patch_ticker(info), patch(f"{MODULE}.time.sleep") as mock_sleep:
            fetch_ebitda_ev(tickers)
        mock_sleep.assert_called_once()


# ---------------------------------------------------------------------------
# write_quarterly_signals async tests
# ---------------------------------------------------------------------------

class TestWriteQuarterlySignals:
    def test_dao_set_called_with_merge_true(self):
        dao = AsyncMock()
        signals = {"AAPL": {"ebitda_ev": 0.10, "skipped": False}}
        quarter_id = "2024-Q1"

        import asyncio
        asyncio.get_event_loop().run_until_complete(write_quarterly_signals(signals, quarter_id, dao))

        dao.set.assert_called_once()
        call_args = dao.set.call_args
        assert call_args.args[0] == QUARTERLY_COLLECTION
        assert call_args.args[1] == quarter_id
        assert call_args.kwargs.get("merge") is True

    def test_payload_contains_ebitda_signal_key(self):
        dao = AsyncMock()
        signals = {"AAPL": {"ebitda_ev": 0.10, "skipped": False}}
        quarter_id = "2024-Q2"

        import asyncio
        asyncio.get_event_loop().run_until_complete(write_quarterly_signals(signals, quarter_id, dao))

        payload = dao.set.call_args.args[2]
        assert EBITDA_SIGNAL_KEY in payload
        assert payload[EBITDA_SIGNAL_KEY] == signals

    def test_payload_contains_quarter_id(self):
        dao = AsyncMock()
        signals = {}
        quarter_id = "2024-Q3"

        import asyncio
        asyncio.get_event_loop().run_until_complete(write_quarterly_signals(signals, quarter_id, dao))

        payload = dao.set.call_args.args[2]
        assert payload["quarter_id"] == quarter_id

    def test_payload_contains_timestamp(self):
        import asyncio
        dao = AsyncMock()
        asyncio.get_event_loop().run_until_complete(write_quarterly_signals({}, "2024-Q4", dao))
        payload = dao.set.call_args.args[2]
        assert "ebitda_written_ts" in payload
