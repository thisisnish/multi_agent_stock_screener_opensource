"""
tests/eval/test_metrics.py — Unit tests for screener/eval/metrics.py.

Covers: compute_metrics(), detect_systematic_issues(), compute_acid_test(),
compute_disclosure_citation_rate().
"""

from __future__ import annotations

import pytest

from screener.eval.metrics import (
    compute_acid_test,
    compute_disclosure_citation_rate,
    compute_metrics,
    detect_systematic_issues,
)
from screener.lib.models import ScoreResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _score(
    accuracy: bool | None,
    bull_accuracy: bool | None = None,
    bear_accuracy: bool | None = None,
    confidence_alignment: int = 60,
    score: int = 70,
) -> ScoreResult:
    """Build a minimal ScoreResult for testing."""
    return ScoreResult(
        score=score,
        accuracy=accuracy,
        confidence_alignment=confidence_alignment,
        timing_quality=70,
        risk_management=70,
        error_flags=[],
        rationale="",
        bull_accuracy=bull_accuracy,
        bear_accuracy=bear_accuracy,
    )


def _pick(
    action: str,
    beat_spy: bool | None = None,
    confidence_score: float | None = None,
    pick_return_pct: float | None = None,
    bull_signal_citations: list | None = None,
    bear_signal_citations: list | None = None,
) -> dict:
    return {
        "action": action,
        "beat_spy": beat_spy,
        "confidence_score": confidence_score,
        "pick_return_pct": pick_return_pct,
        "bull_signal_citations": bull_signal_citations or [],
        "bear_signal_citations": bear_signal_citations or [],
    }


# ---------------------------------------------------------------------------
# compute_metrics
# ---------------------------------------------------------------------------


def test_compute_metrics_basic_accuracy():
    # 4 closed picks, 2 correct → 50.0%
    results = [
        _score(accuracy=True),
        _score(accuracy=True),
        _score(accuracy=False),
        _score(accuracy=False),
    ]
    metrics = compute_metrics("2026-04", results)
    assert metrics.overall_accuracy == 50.0
    assert metrics.total_picks == 4
    assert metrics.closed_picks == 4
    assert metrics.open_picks == 0


def test_compute_metrics_empty_raises():
    with pytest.raises(ValueError):
        compute_metrics("2026-04", [])


def test_bull_accuracy_only_counts_buy_picks():
    # 1 BUY correct, 1 SELL correct → bull=100%, bear=100%
    results = [
        _score(accuracy=True, bull_accuracy=True),
        _score(accuracy=True, bear_accuracy=True),
    ]
    metrics = compute_metrics("2026-04", results)
    assert metrics.bull_accuracy == 100.0
    assert metrics.bear_accuracy == 100.0


def test_bear_accuracy_only_counts_sell_picks():
    # 1 SELL incorrect → bear=0%
    results = [
        _score(accuracy=False, bear_accuracy=False),
    ]
    metrics = compute_metrics("2026-04", results)
    assert metrics.bear_accuracy == 0.0
    assert metrics.bull_accuracy is None  # no BUY picks


def test_directional_bias_bullish():
    # bull_accuracy=80, bear_accuracy=30 → gap=50 > 20 → "bullish"
    results = [
        # 4 BUY: 3 correct, 1 wrong → 75%
        _score(accuracy=True, bull_accuracy=True),
        _score(accuracy=True, bull_accuracy=True),
        _score(accuracy=True, bull_accuracy=True),
        _score(accuracy=False, bull_accuracy=False),
        # 2 SELL: 0 correct → 0%
        _score(accuracy=False, bear_accuracy=False),
        _score(accuracy=False, bear_accuracy=False),
    ]
    metrics = compute_metrics("2026-04", results)
    assert metrics.directional_bias == "bullish"


def test_directional_bias_balanced():
    # bull=2/4=50%, bear=2/4=50% → gap=0 < 20 → "balanced"
    results = [
        _score(accuracy=True, bull_accuracy=True),
        _score(accuracy=True, bull_accuracy=True),
        _score(accuracy=False, bull_accuracy=False),
        _score(accuracy=False, bull_accuracy=False),
        _score(accuracy=True, bear_accuracy=True),
        _score(accuracy=True, bear_accuracy=True),
        _score(accuracy=False, bear_accuracy=False),
        _score(accuracy=False, bear_accuracy=False),
    ]
    metrics = compute_metrics("2026-04", results)
    assert metrics.directional_bias == "balanced"


# ---------------------------------------------------------------------------
# detect_systematic_issues
# ---------------------------------------------------------------------------


def test_detect_overconfidence():
    # avg_confidence_alignment = 90, overall_accuracy = 50 → gap = 40 > 20 → issue
    results = [
        _score(accuracy=True, confidence_alignment=90),
        _score(accuracy=True, confidence_alignment=90),
        _score(accuracy=False, confidence_alignment=90),
        _score(accuracy=False, confidence_alignment=90),
    ]
    metrics = compute_metrics("2026-04", results)
    issues = detect_systematic_issues(metrics)
    assert any("Overconfidence" in i for i in issues)


# ---------------------------------------------------------------------------
# Confidence bins
# ---------------------------------------------------------------------------


def test_confidence_bins_high_accuracy():
    # 2 picks with confidence 75 and 80, both beat SPY → high_accuracy = 100.0
    scores = [
        _score(accuracy=True),
        _score(accuracy=True),
    ]
    picks_raw = [
        _pick("BUY", beat_spy=True, confidence_score=75),
        _pick("BUY", beat_spy=True, confidence_score=80),
    ]
    metrics = compute_metrics("2026-04", scores, picks_raw=picks_raw)
    assert metrics.high_confidence_accuracy == 100.0
    assert metrics.medium_confidence_accuracy is None
    assert metrics.low_confidence_accuracy is None


def test_confidence_bins_low_accuracy():
    # 2 picks with confidence 25 and 30, both lose → low_accuracy = 0.0
    scores = [
        _score(accuracy=False),
        _score(accuracy=False),
    ]
    picks_raw = [
        _pick("BUY", beat_spy=False, confidence_score=25),
        _pick("BUY", beat_spy=False, confidence_score=30),
    ]
    metrics = compute_metrics("2026-04", scores, picks_raw=picks_raw)
    assert metrics.low_confidence_accuracy == 0.0
    assert metrics.high_confidence_accuracy is None


# ---------------------------------------------------------------------------
# compute_acid_test
# ---------------------------------------------------------------------------


def test_acid_test_max_drawdown():
    # pick with pick_return_pct=-8 in High tier → max_drawdown=8
    picks = [
        _pick("BUY", beat_spy=False, confidence_score=75, pick_return_pct=-8.0),
        _pick("BUY", beat_spy=True, confidence_score=75, pick_return_pct=5.0),
    ]
    result = compute_acid_test(picks)
    assert result["High"]["max_drawdown"] == 8.0
    assert result["High"]["count"] == 2
    # Medium and Low tiers had no picks
    assert result["Medium"]["count"] == 0
    assert result["Low"]["count"] == 0


# ---------------------------------------------------------------------------
# compute_disclosure_citation_rate
# ---------------------------------------------------------------------------


def test_disclosure_citation_rate():
    # 2 of 4 picks have "Disclosures" in bull_signal_citations → rate = 0.5
    picks = [
        _pick("BUY", bull_signal_citations=["Disclosures", "Technical"]),
        _pick("SELL", bull_signal_citations=["Technical"]),
        _pick("BUY", bear_signal_citations=["Disclosures"]),
        _pick("SELL"),
    ]
    rate = compute_disclosure_citation_rate(picks)
    assert rate == 0.5
