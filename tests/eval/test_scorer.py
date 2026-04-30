"""
tests/eval/test_scorer.py — Unit tests for screener/eval/scorer.py.

Covers the pure-math scoring path only. LLM path is not tested here as it
requires a live LLM.
"""

from __future__ import annotations

from screener.eval.scorer import score_pick_pure_math, score_picks_pure_math


def _pick(action: str, beat_spy=None, **kwargs) -> dict:
    """Build a minimal pick dict for testing."""
    return {"action": action, "beat_spy": beat_spy, **kwargs}


# ---------------------------------------------------------------------------
# score_pick_pure_math
# ---------------------------------------------------------------------------


def test_score_pick_buy_beats_spy():
    result = score_pick_pure_math(_pick("BUY", beat_spy=True))
    assert result is not None
    assert result.accuracy is True
    assert result.bull_accuracy is True
    assert result.bear_accuracy is None


def test_score_pick_sell_beats_spy():
    result = score_pick_pure_math(_pick("SELL", beat_spy=True))
    assert result is not None
    assert result.accuracy is True
    assert result.bull_accuracy is None
    assert result.bear_accuracy is True


def test_score_pick_hold_returns_none():
    result = score_pick_pure_math(_pick("HOLD", beat_spy=True))
    assert result is None


def test_score_pick_missing_beat_spy_returns_none():
    result = score_pick_pure_math(_pick("BUY", beat_spy=None))
    assert result is None


# ---------------------------------------------------------------------------
# score_picks_pure_math
# ---------------------------------------------------------------------------


def test_score_picks_pure_math_filters_hold():
    picks = [
        _pick("BUY", beat_spy=True),
        _pick("HOLD", beat_spy=True),
        _pick("SELL", beat_spy=False),
    ]
    results = score_picks_pure_math(picks)
    assert len(results) == 2
    actions_present = {(r.bull_accuracy, r.bear_accuracy) for r in results}
    # BUY pick: bull_accuracy=True, bear_accuracy=None
    assert (True, None) in actions_present
    # SELL pick: bull_accuracy=None, bear_accuracy=False
    assert (None, False) in actions_present


def test_score_picks_pure_math_empty():
    assert score_picks_pure_math([]) == []
