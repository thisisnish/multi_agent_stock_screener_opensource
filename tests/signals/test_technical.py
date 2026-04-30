"""
tests/signals/test_technical.py — Unit tests for screener/metrics/technical.py.

All tests are pure pandas — no I/O, no yfinance calls.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from screener.metrics.technical import (
    MIN_ROWS,
    WEIGHTS,
    _ma_score,
    _momentum_score,
    _rsi_score,
    _volume_score,
    compute_score,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_df(
    n: int, start_price: float = 100.0, slope: float = 0.5, volume: float = 1_000_000.0
) -> pd.DataFrame:
    """
    Synthetic OHLCV data with a slight upward trend but alternating noise so that
    RSI is well-defined (both gains and losses exist across the series).
    """
    rng = np.random.default_rng(seed=42)
    # Small alternating noise around a linear trend
    noise = rng.normal(0, 1.0, n)
    closes = [max(start_price + i * slope + noise[i], 1.0) for i in range(n)]
    return pd.DataFrame(
        {
            "Close": closes,
            "Volume": [volume] * n,
        }
    )


# ---------------------------------------------------------------------------
# compute_score — data-level guards
# ---------------------------------------------------------------------------


class TestComputeScoreGuards:
    def test_none_df_returns_skipped(self):
        result = compute_score("AAPL", None)
        assert result["skipped"] is True
        assert "reason" in result
        assert "0 rows" in result["reason"]

    def test_too_few_rows_returns_skipped(self):
        df = _make_df(MIN_ROWS - 1)
        result = compute_score("AAPL", df)
        assert result["skipped"] is True
        assert str(MIN_ROWS - 1) in result["reason"]

    def test_exactly_min_rows_not_skipped(self):
        df = _make_df(MIN_ROWS)
        result = compute_score("AAPL", df)
        assert result["skipped"] is False

    def test_sufficient_data_returns_correct_keys(self):
        df = _make_df(MIN_ROWS + 20)
        result = compute_score("AAPL", df)
        assert result["skipped"] is False
        for key in ("score", "rsi", "ma50", "ma200", "price", "signals"):
            assert key in result

    def test_score_in_0_100_range(self):
        df = _make_df(MIN_ROWS + 50)
        result = compute_score("AAPL", df)
        assert 0.0 <= result["score"] <= 100.0

    def test_signals_contain_all_weight_keys(self):
        df = _make_df(MIN_ROWS + 50)
        result = compute_score("AAPL", df)
        for k in WEIGHTS:
            assert k in result["signals"]
            assert "score" in result["signals"][k]
            assert "weight" in result["signals"][k]

    def test_nan_close_prices_after_dropna_too_few_returns_skipped(self):
        # Most closes are NaN; after dropna fewer than MIN_ROWS remain
        n = MIN_ROWS * 2
        closes = [np.nan] * (n - 5) + [100.0 + i for i in range(5)]
        df = pd.DataFrame({"Close": closes, "Volume": [1_000_000.0] * n})
        result = compute_score("AAPL", df)
        assert result["skipped"] is True

    def test_composite_is_weighted_sum_of_sub_scores(self):
        df = _make_df(MIN_ROWS + 50)
        result = compute_score("AAPL", df)
        signals = result["signals"]
        expected = sum(signals[k]["score"] * signals[k]["weight"] for k in signals)
        assert abs(result["score"] - round(expected, 2)) < 0.01


# ---------------------------------------------------------------------------
# _rsi_score
# ---------------------------------------------------------------------------


class TestRsiScore:
    def test_rsi_below_30_returns_100(self):
        assert _rsi_score(20.0) == 100.0
        assert _rsi_score(0.0) == 100.0
        assert _rsi_score(29.9) == 100.0

    def test_rsi_30_returns_100(self):
        assert _rsi_score(30.0) == 100.0

    def test_rsi_35_returns_90(self):
        # 100 - (35-30)*2 = 90
        assert _rsi_score(35.0) == pytest.approx(90.0)

    def test_rsi_40_returns_80(self):
        # 100 - (40-30)*2 = 80
        assert _rsi_score(40.0) == pytest.approx(80.0)

    def test_rsi_50_returns_70(self):
        # 80 - (50-40)*1 = 70
        assert _rsi_score(50.0) == pytest.approx(70.0)

    def test_rsi_60_returns_60(self):
        # 80 - (60-40)*1 = 60
        assert _rsi_score(60.0) == pytest.approx(60.0)

    def test_rsi_65_returns_45(self):
        # 60 - (65-60)*3 = 45
        assert _rsi_score(65.0) == pytest.approx(45.0)

    def test_rsi_just_below_70_returns_nonzero(self):
        # 60 - (69.99-60)*3 = 60 - 29.97 = 30.03 (> 0)
        assert _rsi_score(69.99) == pytest.approx(30.03, abs=0.1)

    def test_rsi_at_70_returns_0(self):
        # 70.0 is NOT < 70, so it hits the else branch → 0.0
        assert _rsi_score(70.0) == 0.0

    def test_rsi_above_70_returns_0(self):
        assert _rsi_score(70.001) == 0.0
        assert _rsi_score(100.0) == 0.0
        assert _rsi_score(80.0) == 0.0

    def test_rsi_score_declines_from_30_to_40(self):
        scores = [_rsi_score(float(r)) for r in range(30, 41)]
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], f"Not declining at RSI={30 + i}"

    def test_rsi_score_declines_from_60_to_70(self):
        scores = [_rsi_score(float(r)) for r in range(60, 71)]
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], f"Not declining steeply at RSI={60 + i}"


# ---------------------------------------------------------------------------
# _ma_score
# ---------------------------------------------------------------------------


class TestMaScore:
    def test_price_equals_ma_gives_50(self):
        assert _ma_score(100.0, 100.0) == pytest.approx(50.0)

    def test_price_above_ma_gives_above_50(self):
        assert _ma_score(110.0, 100.0) > 50.0

    def test_price_below_ma_gives_below_50(self):
        assert _ma_score(90.0, 100.0) < 50.0

    def test_ma_zero_gives_50(self):
        assert _ma_score(100.0, 0.0) == 50.0

    def test_score_clamped_to_100(self):
        # Extremely high price vs MA
        assert _ma_score(1_000_000.0, 1.0) == 100.0

    def test_score_clamped_to_0(self):
        # Price very far below MA
        assert _ma_score(1.0, 1_000_000.0) == 0.0


# ---------------------------------------------------------------------------
# _volume_score
# ---------------------------------------------------------------------------


class TestVolumeScore:
    def test_vol_short_greater_than_long_above_50(self):
        assert _volume_score(2_000_000.0, 1_000_000.0) > 50.0

    def test_vol_short_less_than_long_below_50(self):
        assert _volume_score(500_000.0, 1_000_000.0) < 50.0

    def test_vol_long_zero_gives_50(self):
        assert _volume_score(1_000_000.0, 0.0) == 50.0

    def test_score_clamped_to_100(self):
        assert _volume_score(1_000_000_000.0, 1.0) == 100.0

    def test_score_clamped_to_0(self):
        assert _volume_score(1.0, 1_000_000_000.0) == 0.0


# ---------------------------------------------------------------------------
# _momentum_score
# ---------------------------------------------------------------------------


class TestMomentumScore:
    def test_positive_pct_change_above_50(self):
        assert _momentum_score(0.10) > 50.0

    def test_negative_pct_change_below_50(self):
        assert _momentum_score(-0.10) < 50.0

    def test_zero_pct_change_gives_50(self):
        assert _momentum_score(0.0) == pytest.approx(50.0)

    def test_score_clamped_to_100(self):
        assert _momentum_score(100.0) == 100.0

    def test_score_clamped_to_0(self):
        assert _momentum_score(-100.0) == 0.0
