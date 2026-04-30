"""
tests/signals/test_earnings_yield.py — Unit tests for screener/metrics/earnings_yield.py.

All yf.Ticker calls are patched — no network I/O.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from screener.metrics.earnings_yield import (
    AbortSignal,
    _fetch_one,
    fetch_earnings_yield,
)

MODULE = "screener.metrics.earnings_yield"


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
        with _patch_ticker({"trailingEps": 2.50, "currentPrice": 100.0}):
            result = _fetch_one("AAPL")
        assert result["skipped"] is False
        assert result["earnings_yield"] == pytest.approx(0.025)
        assert result["trailing_eps"] == pytest.approx(2.5)
        assert result["price"] == pytest.approx(100.0)
        assert result["skip_reason"] is None

    def test_negative_eps_clips_to_zero_yield_but_preserves_raw(self):
        with _patch_ticker({"trailingEps": -1.0, "currentPrice": 50.0}):
            result = _fetch_one("LOSS")
        assert result["skipped"] is False
        # clipped to 0.0 for yield computation
        assert result["earnings_yield"] == pytest.approx(0.0)
        # raw EPS preserved in trailing_eps
        assert result["trailing_eps"] == pytest.approx(-1.0)

    def test_zero_eps_gives_zero_yield(self):
        with _patch_ticker({"trailingEps": 0.0, "currentPrice": 50.0}):
            result = _fetch_one("ZERO")
        assert result["skipped"] is False
        assert result["earnings_yield"] == pytest.approx(0.0)

    def test_none_eps_skipped(self):
        with _patch_ticker({"trailingEps": None, "currentPrice": 100.0}):
            result = _fetch_one("AAPL")
        assert result["skipped"] is True
        assert "trailingEps" in result["skip_reason"]

    def test_none_price_skipped(self):
        with _patch_ticker(
            {"trailingEps": 2.0, "currentPrice": None, "regularMarketPrice": None}
        ):
            result = _fetch_one("AAPL")
        assert result["skipped"] is True
        assert "currentPrice" in result["skip_reason"]

    def test_zero_price_skipped(self):
        with _patch_ticker({"trailingEps": 2.0, "currentPrice": 0.0}):
            result = _fetch_one("AAPL")
        assert result["skipped"] is True

    def test_fallback_to_regular_market_price(self):
        with _patch_ticker(
            {"trailingEps": 5.0, "currentPrice": None, "regularMarketPrice": 200.0}
        ):
            result = _fetch_one("AAPL")
        assert result["skipped"] is False
        assert result["earnings_yield"] == pytest.approx(5.0 / 200.0)
        assert result["price"] == pytest.approx(200.0)

    def test_exception_from_yf_ticker_returns_skipped(self):
        with patch(f"{MODULE}.yf.Ticker", side_effect=RuntimeError("network error")):
            result = _fetch_one("AAPL")
        assert result["skipped"] is True
        assert "fetch error" in result["skip_reason"]

    def test_all_result_keys_present(self):
        with _patch_ticker({"trailingEps": 1.0, "currentPrice": 50.0}):
            result = _fetch_one("AAPL")
        expected_keys = {
            "earnings_yield",
            "trailing_eps",
            "price",
            "skipped",
            "skip_reason",
        }
        assert set(result.keys()) == expected_keys

    def test_all_result_keys_present_on_skip(self):
        with _patch_ticker({"trailingEps": None, "currentPrice": 50.0}):
            result = _fetch_one("AAPL")
        expected_keys = {
            "earnings_yield",
            "trailing_eps",
            "price",
            "skipped",
            "skip_reason",
        }
        assert set(result.keys()) == expected_keys


# ---------------------------------------------------------------------------
# fetch_earnings_yield integration-level unit tests (no network)
# ---------------------------------------------------------------------------


class TestFetchEarningsYield:
    def test_returns_all_tickers(self):
        tickers = ["AAPL", "MSFT", "GOOG"]
        info = {"trailingEps": 1.0, "currentPrice": 50.0}
        with patch(f"{MODULE}.yf.Ticker", return_value=_ticker_mock(info)):
            results = fetch_earnings_yield(tickers)
        assert set(results.keys()) == set(tickers)

    def test_empty_list_returns_empty_dict(self):
        results = fetch_earnings_yield([])
        assert results == {}

    def test_no_sleep_for_single_batch(self):
        tickers = [f"T{i}" for i in range(10)]
        info = {"trailingEps": 1.0, "currentPrice": 50.0}
        with (
            patch(f"{MODULE}.yf.Ticker", return_value=_ticker_mock(info)),
            patch(f"{MODULE}.time.sleep") as mock_sleep,
        ):
            fetch_earnings_yield(tickers)
        mock_sleep.assert_not_called()

    def test_sleep_between_batches_for_51_tickers(self):
        tickers = [f"T{i}" for i in range(51)]
        info = {"trailingEps": 1.0, "currentPrice": 50.0}
        with (
            patch(f"{MODULE}.yf.Ticker", return_value=_ticker_mock(info)),
            patch(f"{MODULE}.time.sleep") as mock_sleep,
        ):
            fetch_earnings_yield(tickers)
        mock_sleep.assert_called_once()

    def test_abort_signal_raised_when_skip_rate_exceeds_threshold(self):
        # 20 tickers, all skip → 100% skip rate > 15%
        tickers = [f"T{i}" for i in range(20)]
        with (
            patch(
                f"{MODULE}.yf.Ticker", return_value=_ticker_mock({"trailingEps": None})
            ),
            patch(f"{MODULE}.time.sleep"),
        ):
            with pytest.raises(AbortSignal):
                fetch_earnings_yield(tickers)

    def test_no_abort_when_skip_rate_at_threshold(self):
        # 20 tickers, 3 skip → 15% = MAX_SKIP_RATE, should NOT raise
        tickers = [f"T{i}" for i in range(20)]
        good_info = {"trailingEps": 1.0, "currentPrice": 50.0}
        bad_info = {"trailingEps": None, "currentPrice": 50.0}

        def side_effect(symbol):
            # First 3 are bad, rest good
            idx = int(symbol[1:])
            return _ticker_mock(bad_info if idx < 3 else good_info)

        with (
            patch(f"{MODULE}.yf.Ticker", side_effect=side_effect),
            patch(f"{MODULE}.time.sleep"),
        ):
            results = fetch_earnings_yield(tickers)
        # 3/20 = 15% = MAX_SKIP_RATE — no raise
        assert len(results) == 20

    def test_no_abort_signal_when_skip_rate_below_threshold(self):
        # 100 tickers, 10 skip → 10% < 15%
        tickers = [f"T{i}" for i in range(100)]
        good_info = {"trailingEps": 2.0, "currentPrice": 100.0}
        bad_info = {"trailingEps": None, "currentPrice": 100.0}

        def side_effect(symbol):
            idx = int(symbol[1:])
            return _ticker_mock(bad_info if idx < 10 else good_info)

        with (
            patch(f"{MODULE}.yf.Ticker", side_effect=side_effect),
            patch(f"{MODULE}.time.sleep"),
        ):
            results = fetch_earnings_yield(tickers)
        assert len(results) == 100
