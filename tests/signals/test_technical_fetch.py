"""
tests/signals/test_technical_fetch.py — Tests for BUG-05 fix.

Covers:
- fetch_technical_signal() calls yfinance and delegates to compute_score
- fetch_technical_signal() returns skipped dict when yfinance returns empty data
- fetch_technical_signal() returns skipped dict when compute_score detects
  insufficient rows
- screener_job main() aborts (sys.exit(1)) when any ticker has a skipped
  technical signal
- screener_job main() proceeds normally when all technical signals succeed

No real yfinance calls are made — yfinance.Ticker and all I/O are mocked.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# Ensure project root is importable (mirrors conftest.py behaviour)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from screener.metrics.technical import MIN_ROWS, fetch_technical_signal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_df(
    n: int,
    start_price: float = 100.0,
    slope: float = 0.5,
    volume: float = 1_000_000.0,
) -> pd.DataFrame:
    """Synthetic OHLCV data — mirrors the helper in test_technical.py."""
    rng = np.random.default_rng(seed=42)
    noise = rng.normal(0, 1.0, n)
    closes = [max(start_price + i * slope + noise[i], 1.0) for i in range(n)]
    return pd.DataFrame({"Close": closes, "Volume": [volume] * n})


def _mock_ticker(df: pd.DataFrame | None) -> MagicMock:
    """Return a mock yfinance.Ticker whose .history() returns *df*."""
    ticker = MagicMock()
    ticker.history.return_value = df if df is not None else pd.DataFrame()
    return ticker


# ---------------------------------------------------------------------------
# fetch_technical_signal — yfinance delegation
# ---------------------------------------------------------------------------


class TestFetchTechnicalSignalDelegation:
    """fetch_technical_signal downloads history and calls compute_score."""

    def test_calls_ticker_history_with_period(self):
        df = _make_df(MIN_ROWS + 20)
        mock_ticker = _mock_ticker(df)

        with patch("screener.metrics.technical.yf.Ticker", return_value=mock_ticker):
            fetch_technical_signal("AAPL")

        mock_ticker.history.assert_called_once()
        period_arg = (
            mock_ticker.history.call_args[1].get("period")
            or mock_ticker.history.call_args[0][0]
        )
        assert "d" in str(period_arg)

    def test_returns_score_dict_on_sufficient_data(self):
        df = _make_df(MIN_ROWS + 20)

        with patch(
            "screener.metrics.technical.yf.Ticker", return_value=_mock_ticker(df)
        ):
            result = fetch_technical_signal("AAPL")

        assert result["skipped"] is False
        assert "score" in result
        assert "rsi" in result
        assert "price" in result
        assert result["price"] > 0.0
        assert result["rsi"] > 0.0

    def test_score_in_valid_range(self):
        df = _make_df(MIN_ROWS + 50)

        with patch(
            "screener.metrics.technical.yf.Ticker", return_value=_mock_ticker(df)
        ):
            result = fetch_technical_signal("AAPL")

        assert 0.0 <= result["score"] <= 100.0


# ---------------------------------------------------------------------------
# fetch_technical_signal — insufficient data handling
# ---------------------------------------------------------------------------


class TestFetchTechnicalSignalInsufficientData:
    """fetch_technical_signal must return skipped=True, never impute RSI=0."""

    def test_empty_dataframe_returns_skipped(self):
        with patch(
            "screener.metrics.technical.yf.Ticker",
            return_value=_mock_ticker(pd.DataFrame()),
        ):
            result = fetch_technical_signal("AAPL")

        assert result["skipped"] is True
        assert "reason" in result

    def test_none_history_returns_skipped(self):
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()

        with patch("screener.metrics.technical.yf.Ticker", return_value=mock_ticker):
            result = fetch_technical_signal("AAPL")

        assert result["skipped"] is True

    def test_too_few_rows_returns_skipped(self):
        df = _make_df(MIN_ROWS - 1)

        with patch(
            "screener.metrics.technical.yf.Ticker", return_value=_mock_ticker(df)
        ):
            result = fetch_technical_signal("AAPL")

        assert result["skipped"] is True
        assert "reason" in result

    def test_skipped_result_has_no_rsi_key(self):
        """Skipped result must NOT carry rsi/price keys — no silent imputation."""
        df = _make_df(MIN_ROWS - 1)

        with patch(
            "screener.metrics.technical.yf.Ticker", return_value=_mock_ticker(df)
        ):
            result = fetch_technical_signal("AAPL")

        assert result["skipped"] is True
        # These keys must not exist in a skipped result to prevent callers
        # from reading stale / default zeros.
        assert "rsi" not in result or result.get("rsi") is None
        assert "price" not in result or result.get("price") is None

    def test_symbol_passed_to_ticker(self):
        df = _make_df(MIN_ROWS + 5)
        with patch(
            "screener.metrics.technical.yf.Ticker", return_value=_mock_ticker(df)
        ) as mock_yf_ticker:
            fetch_technical_signal("NVDA")

        mock_yf_ticker.assert_called_once_with("NVDA")


# ---------------------------------------------------------------------------
# screener_job abort behaviour — technical signal hard constraint
# ---------------------------------------------------------------------------

_TICKERS_YAML = b"""
tickers:
  - symbol: AAPL
    sector: Technology
  - symbol: MSFT
    sector: Technology
"""

_GOOD_TECHNICAL = {
    "skipped": False,
    "score": 65.0,
    "rsi": 48.5,
    "ma50": 185.0,
    "ma200": 175.0,
    "price": 190.0,
    "signals": {
        "rsi": {"score": 70.0, "weight": 0.30},
        "ma50": {"score": 55.0, "weight": 0.25},
        "ma200": {"score": 58.0, "weight": 0.20},
        "volume": {"score": 52.0, "weight": 0.15},
        "momentum": {"score": 60.0, "weight": 0.10},
    },
}

_SKIPPED_TECHNICAL = {
    "skipped": True,
    "reason": "insufficient data: 0 rows, need 205",
}

_EARNINGS_OK = {"earnings_yield": 0.03, "skipped": False}
_FCF_OK = {"fcf_yield": 0.05, "skipped": False}
_EBITDA_OK = {"ebitda_ev": 0.08, "skipped": False}


def _fake_app_config():
    """Minimal AppConfig stub for screener_job tests."""
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

    return AppConfig(
        llm=LLMConfig(
            model="anthropic:claude-haiku-4-5-20251001",
            embedder_model="google_genai:models/gemini-embedding-001",
        ),
        storage=StorageConfig(
            provider="firestore",
            firestore=FirestoreConfig(project_id="test-project", database="test-db"),
        ),
        signals=SignalsConfig(),
        screener=ScreenerConfig(),
        notifications=NotificationsConfig(
            email=EmailConfig(enabled=False, recipients=[])
        ),
        edgar=EdgarConfig(),
    )


def _run_screener_main(
    env: dict,
    technical_side_effect,
    tickers_content: bytes = _TICKERS_YAML,
) -> None:
    """Run screener_job main() with all external I/O mocked.

    *technical_side_effect* is a callable passed as side_effect to
    fetch_technical_signal — use it to return good or skipped results
    per symbol.

    Raises SystemExit when main() calls sys.exit().
    """
    import os
    import tempfile
    from unittest.mock import AsyncMock, MagicMock

    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp_tickers:
        tmp_tickers.write(tickers_content)
        tmp_tickers.flush()

        mock_dao_instance = MagicMock()
        mock_dao_instance.set = AsyncMock(return_value=None)

        mock_graph = MagicMock()
        mock_graph.ainvoke = AsyncMock(return_value={})

        with (
            patch.dict(os.environ, {**env, "GCS_CONFIG_BUCKET": ""}),
            patch(
                "screener.lib.storage.firestore.FirestoreDAO",
                return_value=mock_dao_instance,
            ),
            patch(
                "screener.lib.config_loader.load_config",
                return_value=_fake_app_config(),
            ),
            patch(
                "screener.metrics.technical.fetch_technical_signal",
                side_effect=technical_side_effect,
            ),
            patch(
                "screener.metrics.earnings_yield.fetch_earnings_yield",
                side_effect=lambda syms: {s: _EARNINGS_OK for s in syms},
            ),
            patch(
                "screener.metrics.fcf_yield.fetch_fcf_yield",
                side_effect=lambda syms: {s: _FCF_OK for s in syms},
            ),
            patch(
                "screener.metrics.ebitda_ev.fetch_ebitda_ev",
                side_effect=lambda syms: {s: _EBITDA_OK for s in syms},
            ),
            patch("screener.agents.graph.build_debate_graph", return_value=mock_graph),
        ):
            import builtins

            original_open = builtins.open

            def patched_open(path, *args, **kwargs):
                if str(path).endswith("tickers.yaml"):
                    return original_open(tmp_tickers.name, *args, **kwargs)
                return original_open(path, *args, **kwargs)

            with patch("builtins.open", side_effect=patched_open):
                import importlib

                import jobs.screener.main as screener_mod

                importlib.reload(screener_mod)
                screener_mod.main()


class TestScreenerJobAbortOnSkippedTechnical:
    """screener_job must abort (sys.exit 1) when technical signal is skipped."""

    def test_exits_nonzero_when_one_ticker_skipped(self):
        """If any ticker returns skipped technical, main() must sys.exit(1)."""

        def _technical(sym: str) -> dict:
            # AAPL succeeds; MSFT returns skipped
            if sym == "MSFT":
                return _SKIPPED_TECHNICAL
            return _GOOD_TECHNICAL

        with pytest.raises(SystemExit) as exc_info:
            _run_screener_main(
                {"MONTH_ID": "2026-04", "DRY_RUN": "false"},
                technical_side_effect=_technical,
            )

        assert exc_info.value.code == 1

    def test_exits_nonzero_when_all_tickers_skipped(self):
        """All tickers skipped — must still abort non-zero."""

        def _technical(sym: str) -> dict:
            return _SKIPPED_TECHNICAL

        with pytest.raises(SystemExit) as exc_info:
            _run_screener_main(
                {"MONTH_ID": "2026-04", "DRY_RUN": "false"},
                technical_side_effect=_technical,
            )

        assert exc_info.value.code == 1

    def test_exits_nonzero_in_dry_run_when_skipped(self):
        """Abort must happen even in dry_run mode — dry_run does not bypass the guard."""

        def _technical(sym: str) -> dict:
            return _SKIPPED_TECHNICAL

        with pytest.raises(SystemExit) as exc_info:
            _run_screener_main(
                {"MONTH_ID": "2026-04", "DRY_RUN": "true"},
                technical_side_effect=_technical,
            )

        assert exc_info.value.code == 1

    def test_no_exit_when_all_technical_succeed(self):
        """When all tickers have valid technical signals, main() should not exit(1)."""

        def _technical(sym: str) -> dict:
            return _GOOD_TECHNICAL

        # Should complete without raising SystemExit
        # (debate graph and email are fully mocked so no real I/O occurs).
        _run_screener_main(
            {"MONTH_ID": "2026-04", "DRY_RUN": "true"},
            technical_side_effect=_technical,
        )
