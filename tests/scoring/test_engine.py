"""
tests/scoring/test_engine.py — Unit tests for P1-11b: scoring engine.

Covers:
- compute_composite_scores: weighted average, partial factors, MA200 gate,
  symbol exclusion when all factors None, full vs partial weight normalisation
- apply_sector_cap: top-N selection, sector concentration cap, ordering,
  exact count enforcement, cross-sector interleaving
"""

from __future__ import annotations

from screener.scoring.engine import apply_sector_cap, compute_composite_scores


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GATE_ABOVE = {"above_ma200": True, "multiplier": 1.0}
_GATE_BELOW = {"above_ma200": False, "multiplier": 0.5}


def _gate_above(price: float, ma200: float) -> dict:
    return _GATE_ABOVE


def _gate_below(price: float, ma200: float) -> dict:
    return _GATE_BELOW


def _real_gate(price: float, ma200: float) -> dict:
    above = price >= ma200
    return {"above_ma200": above, "multiplier": 1.0 if above else 0.5}


def _sig(
    symbol: str, sector: str = "Tech", price: float = 100.0, ma200: float = 90.0
) -> dict:
    return {
        "symbol": symbol,
        "sector": sector,
        "technical": {"price": price, "ma200": ma200},
    }


def _factor_scores(syms: list[str], scores: dict[str, dict[str, float | None]]) -> dict:
    """Build a factor_scores dict with all four standard factors."""
    default = {s: None for s in syms}
    return {
        "technical": {**default, **scores.get("technical", {})},
        "earnings": {**default, **scores.get("earnings", {})},
        "fcf": {**default, **scores.get("fcf", {})},
        "ebitda": {**default, **scores.get("ebitda", {})},
    }


_STANDARD_WEIGHTS = {
    "technical": 0.2,
    "earnings": 0.3,
    "fcf": 0.3,
    "ebitda": 0.2,
}


# ---------------------------------------------------------------------------
# compute_composite_scores
# ---------------------------------------------------------------------------


class TestComputeCompositeScores:
    def test_single_symbol_all_factors_above_gate(self):
        sigs = {"AAPL": _sig("AAPL")}
        fs = _factor_scores(
            ["AAPL"],
            {
                "technical": {"AAPL": 60.0},
                "earnings": {"AAPL": 70.0},
                "fcf": {"AAPL": 80.0},
                "ebitda": {"AAPL": 50.0},
            },
        )
        result = compute_composite_scores(sigs, fs, _STANDARD_WEIGHTS, _gate_above)
        assert len(result) == 1
        entry = result[0]
        # 60*0.2 + 70*0.3 + 80*0.3 + 50*0.2 = 12+21+24+10 = 67.0; gate=1.0
        assert abs(entry["composite_score"] - 67.0) < 1e-6

    def test_ma200_gate_below_halves_score(self):
        sigs = {"AAPL": _sig("AAPL")}
        fs = _factor_scores(
            ["AAPL"],
            {
                "technical": {"AAPL": 60.0},
                "earnings": {"AAPL": 70.0},
                "fcf": {"AAPL": 80.0},
                "ebitda": {"AAPL": 50.0},
            },
        )
        result = compute_composite_scores(sigs, fs, _STANDARD_WEIGHTS, _gate_below)
        assert abs(result[0]["composite_score"] - 33.5) < 1e-6

    def test_real_gate_above_ma200(self):
        sigs = {"AAPL": _sig("AAPL", price=150.0, ma200=100.0)}
        fs = _factor_scores(["AAPL"], {"technical": {"AAPL": 80.0}})
        weights = {"technical": 1.0, "earnings": 0.0, "fcf": 0.0, "ebitda": 0.0}
        result = compute_composite_scores(sigs, fs, weights, _real_gate)
        assert result[0]["ma200_gate"]["above_ma200"] is True
        assert result[0]["ma200_gate"]["multiplier"] == 1.0

    def test_real_gate_below_ma200(self):
        sigs = {"AAPL": _sig("AAPL", price=80.0, ma200=100.0)}
        fs = _factor_scores(["AAPL"], {"technical": {"AAPL": 80.0}})
        weights = {"technical": 1.0, "earnings": 0.0, "fcf": 0.0, "ebitda": 0.0}
        result = compute_composite_scores(sigs, fs, weights, _real_gate)
        assert result[0]["ma200_gate"]["above_ma200"] is False
        assert result[0]["ma200_gate"]["multiplier"] == 0.5

    def test_symbol_excluded_when_all_factors_none(self):
        sigs = {"AAPL": _sig("AAPL"), "MSFT": _sig("MSFT")}
        fs = _factor_scores(
            ["AAPL", "MSFT"],
            {
                "technical": {"MSFT": 60.0},
                "earnings": {"MSFT": 55.0},
                "fcf": {"MSFT": 65.0},
                "ebitda": {"MSFT": 50.0},
            },
        )
        # AAPL has all None — should be excluded
        result = compute_composite_scores(sigs, fs, _STANDARD_WEIGHTS, _gate_above)
        symbols = [r["symbol"] for r in result]
        assert "AAPL" not in symbols
        assert "MSFT" in symbols

    def test_partial_factors_normalised_by_available_weight(self):
        # Only technical (0.2) and earnings (0.3) available → total_weight=0.5 < 1.0
        # raw_composite = (60*0.2 + 70*0.3) / 0.5 = (12+21)/0.5 = 66.0
        sigs = {"AAPL": _sig("AAPL")}
        fs = _factor_scores(
            ["AAPL"],
            {"technical": {"AAPL": 60.0}, "earnings": {"AAPL": 70.0}},
        )
        result = compute_composite_scores(sigs, fs, _STANDARD_WEIGHTS, _gate_above)
        assert abs(result[0]["composite_score"] - 66.0) < 1e-6

    def test_full_weights_sum_to_one_uses_weighted_sum_directly(self):
        # All four factors, weights sum to 1.0 → no normalisation
        sigs = {"AAPL": _sig("AAPL")}
        fs = _factor_scores(
            ["AAPL"],
            {
                "technical": {"AAPL": 50.0},
                "earnings": {"AAPL": 50.0},
                "fcf": {"AAPL": 50.0},
                "ebitda": {"AAPL": 50.0},
            },
        )
        result = compute_composite_scores(sigs, fs, _STANDARD_WEIGHTS, _gate_above)
        assert abs(result[0]["composite_score"] - 50.0) < 1e-6

    def test_original_signal_fields_preserved(self):
        sigs = {"AAPL": {**_sig("AAPL"), "extra_field": "preserved"}}
        fs = _factor_scores(["AAPL"], {"technical": {"AAPL": 60.0}})
        weights = {"technical": 1.0, "earnings": 0.0, "fcf": 0.0, "ebitda": 0.0}
        result = compute_composite_scores(sigs, fs, weights, _gate_above)
        assert result[0]["extra_field"] == "preserved"
        assert result[0]["symbol"] == "AAPL"

    def test_ma200_gate_dict_attached_to_entry(self):
        sigs = {"AAPL": _sig("AAPL")}
        fs = _factor_scores(["AAPL"], {"technical": {"AAPL": 60.0}})
        weights = {"technical": 1.0, "earnings": 0.0, "fcf": 0.0, "ebitda": 0.0}
        result = compute_composite_scores(sigs, fs, weights, _gate_above)
        assert "ma200_gate" in result[0]
        assert result[0]["ma200_gate"] == _GATE_ABOVE

    def test_multiple_symbols_all_scored(self):
        sigs = {
            "AAPL": _sig("AAPL"),
            "MSFT": _sig("MSFT"),
            "NVDA": _sig("NVDA"),
        }
        scores_val = {"AAPL": 70.0, "MSFT": 60.0, "NVDA": 50.0}
        fs = _factor_scores(
            ["AAPL", "MSFT", "NVDA"],
            {
                "technical": scores_val,
                "earnings": scores_val,
                "fcf": scores_val,
                "ebitda": scores_val,
            },
        )
        result = compute_composite_scores(sigs, fs, _STANDARD_WEIGHTS, _gate_above)
        assert len(result) == 3

    def test_price_and_ma200_passed_to_gate_fn(self):
        calls: list[tuple[float, float]] = []

        def recording_gate(price: float, ma200: float) -> dict:
            calls.append((price, ma200))
            return _GATE_ABOVE

        sigs = {"AAPL": _sig("AAPL", price=155.0, ma200=140.0)}
        fs = _factor_scores(["AAPL"], {"technical": {"AAPL": 60.0}})
        weights = {"technical": 1.0, "earnings": 0.0, "fcf": 0.0, "ebitda": 0.0}
        compute_composite_scores(sigs, fs, weights, recording_gate)
        assert calls == [(155.0, 140.0)]

    def test_missing_price_ma200_defaults_to_zero(self):
        calls: list[tuple[float, float]] = []

        def recording_gate(price: float, ma200: float) -> dict:
            calls.append((price, ma200))
            return _GATE_ABOVE

        sigs = {"AAPL": {"symbol": "AAPL", "sector": "Tech", "technical": {}}}
        fs = _factor_scores(["AAPL"], {"technical": {"AAPL": 60.0}})
        weights = {"technical": 1.0, "earnings": 0.0, "fcf": 0.0, "ebitda": 0.0}
        compute_composite_scores(sigs, fs, weights, recording_gate)
        assert calls == [(0, 0)]

    def test_empty_signals_returns_empty_list(self):
        result = compute_composite_scores({}, {}, _STANDARD_WEIGHTS, _gate_above)
        assert result == []


# ---------------------------------------------------------------------------
# apply_sector_cap
# ---------------------------------------------------------------------------


def _entry(symbol: str, sector: str, score: float) -> dict:
    return {"symbol": symbol, "sector": sector, "composite_score": score}


class TestApplySectorCap:
    def test_returns_top_n(self):
        gated = [
            _entry("A", "Tech", 90.0),
            _entry("B", "Tech", 80.0),
            _entry("C", "Finance", 70.0),
            _entry("D", "Finance", 60.0),
            _entry("E", "Health", 50.0),
        ]
        picks = apply_sector_cap(gated, top_n=3, max_per_sector=2)
        assert len(picks) == 3

    def test_ordered_by_composite_score_descending(self):
        gated = [
            _entry("C", "Finance", 70.0),
            _entry("A", "Tech", 90.0),
            _entry("B", "Tech", 80.0),
        ]
        picks = apply_sector_cap(gated, top_n=3, max_per_sector=3)
        scores = [p["composite_score"] for p in picks]
        assert scores == sorted(scores, reverse=True)

    def test_sector_cap_respected(self):
        gated = [
            _entry("A", "Tech", 95.0),
            _entry("B", "Tech", 90.0),
            _entry("C", "Tech", 85.0),  # 3rd tech — should be blocked
            _entry("D", "Finance", 80.0),
        ]
        picks = apply_sector_cap(gated, top_n=4, max_per_sector=2)
        tech_picks = [p for p in picks if p["sector"] == "Tech"]
        assert len(tech_picks) == 2

    def test_sector_cap_allows_lower_ranked_from_other_sector(self):
        gated = [
            _entry("A", "Tech", 95.0),
            _entry("B", "Tech", 90.0),
            _entry("C", "Tech", 85.0),  # capped
            _entry("D", "Finance", 60.0),
        ]
        picks = apply_sector_cap(gated, top_n=3, max_per_sector=2)
        symbols = [p["symbol"] for p in picks]
        assert (
            "D" in symbols
        )  # D included despite lower score due to sector cap on Tech
        assert "C" not in symbols

    def test_top_n_hard_limit(self):
        gated = [_entry(str(i), "Finance", float(100 - i)) for i in range(10)]
        picks = apply_sector_cap(gated, top_n=5, max_per_sector=10)
        assert len(picks) == 5

    def test_fewer_than_top_n_available(self):
        gated = [_entry("A", "Tech", 70.0), _entry("B", "Health", 60.0)]
        picks = apply_sector_cap(gated, top_n=10, max_per_sector=5)
        assert len(picks) == 2

    def test_all_same_sector_capped(self):
        gated = [_entry(str(i), "Tech", float(100 - i)) for i in range(6)]
        picks = apply_sector_cap(gated, top_n=6, max_per_sector=3)
        assert len(picks) == 3
        tech_picks = [p for p in picks if p["sector"] == "Tech"]
        assert len(tech_picks) == 3

    def test_empty_gated_returns_empty(self):
        assert apply_sector_cap([], top_n=5, max_per_sector=2) == []

    def test_missing_sector_defaults_to_unknown(self):
        gated = [
            {"symbol": "A", "composite_score": 80.0},  # no sector key
            {"symbol": "B", "composite_score": 70.0},
            _entry("C", "Tech", 60.0),
        ]
        picks = apply_sector_cap(gated, top_n=3, max_per_sector=1)
        # A and B both fall into "Unknown" sector — only one should be selected
        unknown_picks = [p for p in picks if p.get("sector", "Unknown") == "Unknown"]
        assert len(unknown_picks) == 1

    def test_correct_symbols_selected(self):
        gated = [
            _entry("A", "Tech", 95.0),
            _entry("B", "Finance", 85.0),
            _entry("C", "Tech", 75.0),  # capped if max_per_sector=1
            _entry("D", "Health", 65.0),
        ]
        picks = apply_sector_cap(gated, top_n=3, max_per_sector=1)
        symbols = {p["symbol"] for p in picks}
        assert symbols == {"A", "B", "D"}

    def test_cross_sector_interleaving(self):
        # 3 sectors, 2 per sector max, top-4
        gated = [
            _entry("A", "Tech", 100.0),
            _entry("B", "Finance", 90.0),
            _entry("C", "Tech", 80.0),
            _entry("D", "Finance", 70.0),
            _entry("E", "Health", 60.0),
        ]
        picks = apply_sector_cap(gated, top_n=4, max_per_sector=2)
        assert len(picks) == 4
        assert {p["symbol"] for p in picks} == {"A", "B", "C", "D"}
