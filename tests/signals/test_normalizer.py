"""
tests/signals/test_normalizer.py — Unit tests for screener/lib/normalizer.py.

No I/O — pure in-memory dictionary manipulation.
"""

from __future__ import annotations

import pytest

from screener.lib.normalizer import MIN_SECTOR_SIZE, sector_z_scores

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SECTOR_A = "Technology"
SECTOR_B = "Healthcare"

VALUE_KEY = "earnings_yield"


def _signal(value: float | None, skipped: bool = False) -> dict:
    return {VALUE_KEY: value, "skipped": skipped}


def _skipped() -> dict:
    return {VALUE_KEY: None, "skipped": True}


def _make_sector(
    symbols: list[str], values: list[float], sector: str
) -> tuple[dict, dict]:
    """Return (signals, sector_map) for a set of symbols with given values."""
    signals = {sym: _signal(val) for sym, val in zip(symbols, values)}
    sector_map = {sym: sector for sym in symbols}
    return signals, sector_map


def _min_sector(base_value: float = 0.05, sector: str = SECTOR_A) -> tuple[dict, dict]:
    """Exactly MIN_SECTOR_SIZE tickers in one sector."""
    symbols = [f"S{i}" for i in range(MIN_SECTOR_SIZE)]
    values = [base_value + i * 0.01 for i in range(MIN_SECTOR_SIZE)]
    return _make_sector(symbols, values, sector)


# ---------------------------------------------------------------------------
# Basic scoring
# ---------------------------------------------------------------------------


class TestBasicScoring:
    def test_median_ticker_scores_near_50(self):
        signals, sector_map = _make_sector(
            [f"S{i}" for i in range(5)],
            [0.05 + i * 0.01 for i in range(5)],
            SECTOR_A,
        )
        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        median_sym = "S2"  # middle of 5
        assert scores[median_sym] == pytest.approx(50.0, abs=5.0)

    def test_high_value_above_50(self):
        signals, sector_map = _make_sector(
            [f"S{i}" for i in range(5)],
            [0.05 + i * 0.01 for i in range(5)],
            SECTOR_A,
        )
        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        # S4 has the highest value
        assert scores["S4"] > 50.0

    def test_low_value_below_50(self):
        signals, sector_map = _min_sector()
        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        # S0 has the lowest value
        assert scores["S0"] < 50.0

    def test_scores_clamped_to_0_100(self):
        # Use extreme spread to force z-score clamping
        symbols = [f"S{i}" for i in range(MIN_SECTOR_SIZE)]
        values = [0.001, 0.001, 0.001, 0.001, 1.0]  # S4 is an outlier
        signals = {sym: _signal(val) for sym, val in zip(symbols, values)}
        sector_map = {sym: SECTOR_A for sym in symbols}

        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        for sym, score in scores.items():
            if score is not None:
                assert 0.0 <= score <= 100.0

    def test_scores_monotone_within_sector(self):
        """Higher raw value → higher score."""
        signals, sector_map = _min_sector()
        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        ordered = [scores[f"S{i}"] for i in range(MIN_SECTOR_SIZE)]
        for i in range(len(ordered) - 1):
            assert ordered[i] <= ordered[i + 1], f"Not monotone at index {i}"


# ---------------------------------------------------------------------------
# Degenerate sector cases
# ---------------------------------------------------------------------------


class TestDegenerateSectors:
    def test_std_zero_all_none(self):
        """All tickers have identical values → std=0 → all return neutral 50.0."""
        symbols = [f"S{i}" for i in range(MIN_SECTOR_SIZE)]
        signals = {sym: _signal(0.05) for sym in symbols}
        sector_map = {sym: SECTOR_A for sym in symbols}

        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        for sym in symbols:
            assert scores[sym] == pytest.approx(50.0)

    def test_sector_below_min_size_returns_neutral(self):
        """Fewer than MIN_SECTOR_SIZE valid tickers → all 50.0 (neutral, no peer comparison)."""
        symbols = [f"S{i}" for i in range(MIN_SECTOR_SIZE - 1)]
        signals = {sym: _signal(0.05) for sym in symbols}
        sector_map = {sym: SECTOR_A for sym in symbols}

        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        for sym in symbols:
            assert scores[sym] == pytest.approx(50.0)

    def test_exactly_min_sector_size_scored(self):
        """Exactly MIN_SECTOR_SIZE → scoring is applied (not all None)."""
        signals, sector_map = _min_sector()
        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        non_null = [v for v in scores.values() if v is not None]
        assert len(non_null) > 0

    def test_skipped_ticker_returns_none_excluded_from_stats(self):
        """Skipped ticker gets None and does NOT influence sector mean/std."""
        symbols = [f"S{i}" for i in range(MIN_SECTOR_SIZE + 1)]
        # Last ticker is skipped
        signals = {sym: _signal(0.05 + i * 0.01) for i, sym in enumerate(symbols[:-1])}
        signals[symbols[-1]] = _skipped()
        sector_map = {sym: SECTOR_A for sym in symbols}

        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        assert scores[symbols[-1]] is None
        # The other MIN_SECTOR_SIZE tickers should still be scored
        non_null = [scores[sym] for sym in symbols[:-1] if scores[sym] is not None]
        assert len(non_null) == MIN_SECTOR_SIZE


# ---------------------------------------------------------------------------
# Cross-sector isolation
# ---------------------------------------------------------------------------


class TestCrossSectorIsolation:
    def test_sectors_scored_independently(self):
        """Two sectors with same absolute values → same relative scores within each sector."""
        sym_a = [f"A{i}" for i in range(MIN_SECTOR_SIZE)]
        sym_b = [f"B{i}" for i in range(MIN_SECTOR_SIZE)]
        values = [0.01 * i for i in range(1, MIN_SECTOR_SIZE + 1)]

        signals = {}
        sector_map = {}
        for sym, val in zip(sym_a, values):
            signals[sym] = _signal(val)
            sector_map[sym] = SECTOR_A
        for sym, val in zip(sym_b, values):
            signals[sym] = _signal(val)
            sector_map[sym] = SECTOR_B

        scores = sector_z_scores(signals, VALUE_KEY, sector_map)

        # Corresponding symbols should have equal scores since data is identical within each sector
        for i in range(MIN_SECTOR_SIZE):
            assert scores[sym_a[i]] == pytest.approx(scores[sym_b[i]], abs=0.01)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_missing_sector_map_entry_returns_none(self):
        """Symbol not in sector_map → score is None and excluded from stats."""
        signals, sector_map = _min_sector()
        # Add an extra symbol with no sector mapping
        signals["ORPHAN"] = _signal(0.10)
        # sector_map does not include ORPHAN

        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        assert scores["ORPHAN"] is None

    def test_empty_signals_returns_empty_dict(self):
        scores = sector_z_scores({}, VALUE_KEY, {})
        assert scores == {}

    def test_all_skipped_returns_all_none(self):
        symbols = [f"S{i}" for i in range(MIN_SECTOR_SIZE)]
        signals = {sym: _skipped() for sym in symbols}
        sector_map = {sym: SECTOR_A for sym in symbols}

        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        for sym in symbols:
            assert scores[sym] is None

    def test_none_value_in_signal_returns_none(self):
        """Signal present but value_key is None → score is None."""
        symbols = [f"S{i}" for i in range(MIN_SECTOR_SIZE)]
        signals = {sym: _signal(None) for sym in symbols}
        sector_map = {sym: SECTOR_A for sym in symbols}

        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        for sym in symbols:
            assert scores[sym] is None

    def test_returns_all_input_symbols(self):
        """Output dict has same keys as input signals."""
        signals, sector_map = _min_sector()
        signals["EXTRA"] = _skipped()
        sector_map["EXTRA"] = SECTOR_A

        scores = sector_z_scores(signals, VALUE_KEY, sector_map)
        assert set(scores.keys()) == set(signals.keys())
