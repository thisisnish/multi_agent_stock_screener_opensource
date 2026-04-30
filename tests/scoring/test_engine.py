"""
tests/scoring/test_engine.py — Unit tests for screener.scoring.engine.

All tests use pure-function inputs (no yfinance, no I/O).
"""

from __future__ import annotations

import pytest

from screener.lib.config_loader import SignalWeightsConfig
from screener.scoring.engine import compute_composite_scores, select_top_n

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DEFAULT_WEIGHTS = SignalWeightsConfig(
    technical=0.20,
    earnings=0.30,
    fcf=0.30,
    ebitda=0.20,
)


def _entry(
    symbol: str,
    sector: str,
    score: float,
    price: float,
    ma200: float,
) -> dict:
    """Minimal scored entry dict (mirrors compute_score() output shape)."""
    return {
        "symbol": symbol,
        "sector": sector,
        "score": score,
        "price": price,
        "ma200": ma200,
    }


# ---------------------------------------------------------------------------
# 1. Basic composite — all signals present, verify pre-gate and post-gate
# ---------------------------------------------------------------------------


def test_basic_composite_all_signals_present():
    entries = [
        _entry("AAPL", "Technology", 80.0, 200.0, 150.0),
        _entry("MSFT", "Technology", 60.0, 300.0, 280.0),
    ]
    ey = {"AAPL": 70.0, "MSFT": 50.0}
    fcf = {"AAPL": 90.0, "MSFT": 40.0}
    ebitda = {"AAPL": 75.0, "MSFT": 55.0}

    result = compute_composite_scores(entries, ey, fcf, ebitda, DEFAULT_WEIGHTS)

    # AAPL: 80*0.20 + 70*0.30 + 90*0.30 + 75*0.20 = 16+21+27+15 = 79.0
    # price(200) >= ma200(150) → multiplier 1.0 → post_gate = 79.0
    assert result[0]["composite_pre_gate"] == pytest.approx(79.0)
    assert result[0]["composite_post_gate"] == pytest.approx(79.0)
    assert result[0]["score"] == pytest.approx(79.0)

    # MSFT: 60*0.20 + 50*0.30 + 40*0.30 + 55*0.20 = 12+15+12+11 = 50.0
    # price(300) >= ma200(280) → multiplier 1.0 → post_gate = 50.0
    assert result[1]["composite_pre_gate"] == pytest.approx(50.0)
    assert result[1]["composite_post_gate"] == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# 2. MA200 gate halves score when price < ma200
# ---------------------------------------------------------------------------


def test_ma200_gate_halves_score_when_below():
    entries = [_entry("XYZ", "Financials", 60.0, 80.0, 120.0)]
    ey = {"XYZ": 60.0}
    fcf = {"XYZ": 60.0}
    ebitda = {"XYZ": 60.0}

    compute_composite_scores(entries, ey, fcf, ebitda, DEFAULT_WEIGHTS)

    pre = entries[0]["composite_pre_gate"]
    post = entries[0]["composite_post_gate"]
    assert entries[0]["ma200_gate"]["above_ma200"] is False
    assert entries[0]["ma200_gate"]["multiplier"] == pytest.approx(0.5)
    assert post == pytest.approx(pre * 0.5)


# ---------------------------------------------------------------------------
# 3. Missing signal imputed as 50.0 (ey_scores={} with ey_available=True)
# ---------------------------------------------------------------------------


def test_missing_signal_imputed_as_50():
    entries = [_entry("ABC", "Healthcare", 70.0, 100.0, 90.0)]
    # ey_scores is empty — symbol not present
    compute_composite_scores(
        entries,
        ey_scores={},
        fcf_scores={"ABC": 60.0},
        ebitda_scores={"ABC": 55.0},
        weights=DEFAULT_WEIGHTS,
        ey_available=True,
    )

    # Raw stored value should be None (symbol absent from ey_scores)
    assert entries[0]["earnings_yield_score"] is None

    # The factor_scores entry should show imputed=True and score=50.0
    ey_factor = entries[0]["factor_scores"]["earnings_yield"]
    assert ey_factor["imputed"] is True
    assert ey_factor["score"] == pytest.approx(50.0)

    # Composite pre-gate should use 50.0 for earnings contribution
    # 70*0.20 + 50*0.30 + 60*0.30 + 55*0.20 = 14 + 15 + 18 + 11 = 58.0
    assert entries[0]["composite_pre_gate"] == pytest.approx(58.0)


# ---------------------------------------------------------------------------
# 4. Signal unavailable (ey_available=False) → imputed as 50.0 regardless
# ---------------------------------------------------------------------------


def test_signal_unavailable_imputed_as_50():
    entries = [_entry("DEF", "Energy", 80.0, 100.0, 90.0)]
    # Provide a score for the symbol — it must be ignored when unavailable
    compute_composite_scores(
        entries,
        ey_scores={"DEF": 99.0},
        fcf_scores={"DEF": 60.0},
        ebitda_scores={"DEF": 60.0},
        weights=DEFAULT_WEIGHTS,
        ey_available=False,
    )

    # Raw stored value is None (stream marked unavailable)
    assert entries[0]["earnings_yield_score"] is None

    ey_factor = entries[0]["factor_scores"]["earnings_yield"]
    assert ey_factor["imputed"] is True
    assert ey_factor["score"] == pytest.approx(50.0)

    # Composite must NOT use 99.0
    # 80*0.20 + 50*0.30 + 60*0.30 + 60*0.20 = 16 + 15 + 18 + 12 = 61.0
    assert entries[0]["composite_pre_gate"] == pytest.approx(61.0)


# ---------------------------------------------------------------------------
# 5. Sector cap — 5 entries same sector, top_n=3, sector_cap=2 → only 2 returned
# ---------------------------------------------------------------------------


def test_sector_cap_limits_picks_per_sector():
    entries = [
        _entry(f"T{i}", "Technology", float(100 - i), 100.0, 90.0)
        for i in range(5)
    ]
    compute_composite_scores(
        entries,
        ey_scores={f"T{i}": 50.0 for i in range(5)},
        fcf_scores={f"T{i}": 50.0 for i in range(5)},
        ebitda_scores={f"T{i}": 50.0 for i in range(5)},
        weights=DEFAULT_WEIGHTS,
    )

    result = select_top_n(entries, top_n=3, sector_cap=2)

    tech_picks = [e for e in result if e["sector"] == "Technology"]
    assert len(tech_picks) == 2
    assert len(result) == 2  # Only 2 sectors available, cap hit before top_n


# ---------------------------------------------------------------------------
# 6. Top-N stops at N — 20 entries, top_n=5 → exactly 5 returned
# ---------------------------------------------------------------------------


def test_top_n_stops_at_n():
    entries = [
        _entry(f"S{i}", f"Sector{i % 4}", float(i), 100.0, 90.0)
        for i in range(20)
    ]
    compute_composite_scores(
        entries,
        ey_scores={f"S{i}": 50.0 for i in range(20)},
        fcf_scores={f"S{i}": 50.0 for i in range(20)},
        ebitda_scores={f"S{i}": 50.0 for i in range(20)},
        weights=DEFAULT_WEIGHTS,
    )

    result = select_top_n(entries, top_n=5, sector_cap=10)

    assert len(result) == 5


# ---------------------------------------------------------------------------
# 7. Rank field assigned correctly
# ---------------------------------------------------------------------------


def test_rank_field_assigned():
    entries = [
        _entry(f"R{i}", "Industrials", float(50 + i), 100.0, 90.0)
        for i in range(5)
    ]
    compute_composite_scores(
        entries,
        ey_scores={f"R{i}": 50.0 for i in range(5)},
        fcf_scores={f"R{i}": 50.0 for i in range(5)},
        ebitda_scores={f"R{i}": 50.0 for i in range(5)},
        weights=DEFAULT_WEIGHTS,
    )

    result = select_top_n(entries, top_n=5, sector_cap=10)

    assert result[0]["rank"] == 1
    assert result[-1]["rank"] == len(result)
    # Ranks are contiguous 1-indexed
    assert [e["rank"] for e in result] == list(range(1, len(result) + 1))


# ---------------------------------------------------------------------------
# 8. Weights sum check — verify composite math with canonical weights
# ---------------------------------------------------------------------------


def test_weights_sum_composite_math():
    weights = SignalWeightsConfig(
        technical=0.20, earnings=0.30, fcf=0.30, ebitda=0.20
    )
    entries = [_entry("GOOG", "Communication Services", 40.0, 150.0, 100.0)]
    ey = {"GOOG": 80.0}
    fcf = {"GOOG": 60.0}
    ebitda = {"GOOG": 70.0}

    compute_composite_scores(entries, ey, fcf, ebitda, weights)

    # 40*0.20 + 80*0.30 + 60*0.30 + 70*0.20
    # = 8.0 + 24.0 + 18.0 + 14.0 = 64.0
    assert entries[0]["composite_pre_gate"] == pytest.approx(64.0)

    # price(150) >= ma200(100) → multiplier 1.0
    assert entries[0]["composite_post_gate"] == pytest.approx(64.0)

    # Technical is never imputed
    assert entries[0]["factor_scores"]["technical"]["imputed"] is False
    assert entries[0]["factor_scores"]["technical"]["weight"] == pytest.approx(0.20)
    assert entries[0]["factor_scores"]["earnings_yield"]["weight"] == pytest.approx(0.30)
    assert entries[0]["factor_scores"]["fcf_yield"]["weight"] == pytest.approx(0.30)
    assert entries[0]["factor_scores"]["ebitda_ev"]["weight"] == pytest.approx(0.20)
