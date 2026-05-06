"""
tests/agents/test_adaptive_weights.py — Unit tests for BUG-12: adaptive weights.

Covers:
- compute_adaptive_weights returns None when fewer than SCORING_MIN_SAMPLE scored verdicts
- compute_adaptive_weights returns correct weights with enough scored verdicts
- compute_adaptive_weights ignores verdicts where direction_correct is None (open picks)
- compute_adaptive_weights handles all-bull or all-bear winning_side splits
- compute_adaptive_weights clamps to 50/50 when both sides have zero accuracy
- default_weights returns expected structure
"""

from __future__ import annotations

from screener.agents.adaptive_weights import compute_adaptive_weights, default_weights
from screener.agents.prompts import SCORING_MIN_SAMPLE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _verdict(winning_side: str, direction_correct: bool | None) -> dict:
    return {"winning_side": winning_side, "direction_correct": direction_correct}


def _prior(verdicts: list[dict]) -> dict:
    return {f"2026-{i + 1:02d}": v for i, v in enumerate(verdicts)}


# ---------------------------------------------------------------------------
# default_weights
# ---------------------------------------------------------------------------


class TestDefaultWeights:
    def test_returns_50_50(self):
        w = default_weights()
        assert w["bull_weight"] == 0.5
        assert w["bear_weight"] == 0.5

    def test_sample_size_zero(self):
        w = default_weights()
        assert w["sample_size"] == 0

    def test_returns_new_dict_each_call(self):
        w1 = default_weights()
        w2 = default_weights()
        w1["bull_weight"] = 0.9
        assert w2["bull_weight"] == 0.5


# ---------------------------------------------------------------------------
# compute_adaptive_weights — below threshold
# ---------------------------------------------------------------------------


class TestComputeAdaptiveWeightsInsufficientData:
    def test_empty_prior_months_returns_none(self):
        assert compute_adaptive_weights({}) is None

    def test_fewer_than_min_sample_returns_none(self):
        verdicts = [_verdict("bull", True)] * (SCORING_MIN_SAMPLE - 1)
        assert compute_adaptive_weights(_prior(verdicts)) is None

    def test_open_picks_not_counted(self):
        # Exactly SCORING_MIN_SAMPLE verdicts but all open (direction_correct=None)
        verdicts = [_verdict("bull", None)] * SCORING_MIN_SAMPLE
        assert compute_adaptive_weights(_prior(verdicts)) is None

    def test_mixed_open_and_scored_below_threshold(self):
        # 2 scored + (SCORING_MIN_SAMPLE - 3) open → still below threshold
        scored = [_verdict("bull", True)] * 2
        open_picks = [_verdict("bull", None)] * (SCORING_MIN_SAMPLE - 3)
        verdicts = scored + open_picks
        if SCORING_MIN_SAMPLE > 3:
            assert compute_adaptive_weights(_prior(verdicts)) is None


# ---------------------------------------------------------------------------
# compute_adaptive_weights — at or above threshold
# ---------------------------------------------------------------------------


class TestComputeAdaptiveWeightsWithData:
    def _make_prior(
        self,
        bull_correct: int,
        bull_wrong: int,
        bear_correct: int,
        bear_wrong: int,
    ) -> dict:
        verdicts: list[dict] = []
        verdicts += [_verdict("bull", True)] * bull_correct
        verdicts += [_verdict("bull", False)] * bull_wrong
        verdicts += [_verdict("bear", True)] * bear_correct
        verdicts += [_verdict("bear", False)] * bear_wrong
        return _prior(verdicts)

    def test_returns_dict_at_min_sample(self):
        # Exactly SCORING_MIN_SAMPLE scored verdicts — should activate
        verdicts = [_verdict("bull", True)] * SCORING_MIN_SAMPLE
        result = compute_adaptive_weights(_prior(verdicts))
        assert result is not None

    def test_weights_sum_to_one(self):
        prior = self._make_prior(
            bull_correct=3, bull_wrong=1, bear_correct=2, bear_wrong=2
        )
        result = compute_adaptive_weights(prior)
        assert result is not None
        assert abs(result["bull_weight"] + result["bear_weight"] - 1.0) < 1e-6

    def test_bull_more_accurate_gets_higher_weight(self):
        # Bull: 4/4 correct, Bear: 2/4 correct
        prior = self._make_prior(
            bull_correct=4, bull_wrong=0, bear_correct=2, bear_wrong=2
        )
        result = compute_adaptive_weights(prior)
        assert result is not None
        assert result["bull_weight"] > result["bear_weight"]

    def test_bear_more_accurate_gets_higher_weight(self):
        # Bull: 1/4 correct, Bear: 4/4 correct
        prior = self._make_prior(
            bull_correct=1, bull_wrong=3, bear_correct=4, bear_wrong=0
        )
        result = compute_adaptive_weights(prior)
        assert result is not None
        assert result["bear_weight"] > result["bull_weight"]

    def test_equal_accuracy_gives_50_50(self):
        # Bull: 2/4 correct, Bear: 2/4 correct
        prior = self._make_prior(
            bull_correct=2, bull_wrong=2, bear_correct=2, bear_wrong=2
        )
        result = compute_adaptive_weights(prior)
        assert result is not None
        assert abs(result["bull_weight"] - 0.5) < 1e-4
        assert abs(result["bear_weight"] - 0.5) < 1e-4

    def test_sample_size_matches_scored_count(self):
        prior = self._make_prior(
            bull_correct=3, bull_wrong=1, bear_correct=2, bear_wrong=2
        )
        result = compute_adaptive_weights(prior)
        assert result is not None
        assert result["sample_size"] == 8

    def test_open_picks_excluded_from_sample_size(self):
        verdicts = (
            [_verdict("bull", True)] * 3
            + [_verdict("bear", True)] * 3
            + [_verdict("bull", None)] * 5  # open, should not count
        )
        result = compute_adaptive_weights(_prior(verdicts))
        assert result is not None
        assert result["sample_size"] == 6

    def test_all_bull_winning_side(self):
        # No bear verdicts at all — bear_acc should default to 0.5
        verdicts = [_verdict("bull", True)] * SCORING_MIN_SAMPLE
        result = compute_adaptive_weights(_prior(verdicts))
        assert result is not None
        # bull_acc=1.0, bear_acc=0.5 → bull_weight = 1.0/1.5
        expected_bull = round(1.0 / 1.5, 4)
        assert result["bull_weight"] == expected_bull

    def test_all_bear_winning_side(self):
        # No bull verdicts at all — bull_acc should default to 0.5
        verdicts = [_verdict("bear", True)] * SCORING_MIN_SAMPLE
        result = compute_adaptive_weights(_prior(verdicts))
        assert result is not None
        expected_bear = round(1.0 / 1.5, 4)
        assert result["bear_weight"] == expected_bear

    def test_weights_rounded_to_4_decimal_places(self):
        prior = self._make_prior(
            bull_correct=3, bull_wrong=1, bear_correct=2, bear_wrong=2
        )
        result = compute_adaptive_weights(prior)
        assert result is not None
        assert result["bull_weight"] == round(result["bull_weight"], 4)
        assert result["bear_weight"] == round(result["bear_weight"], 4)

    def test_zero_total_accuracy_gives_50_50(self):
        # Bull: 0/2 correct, Bear: 0/2 correct → total=0, should default to 50/50
        prior = self._make_prior(
            bull_correct=0, bull_wrong=2, bear_correct=0, bear_wrong=2
        )
        result = compute_adaptive_weights(prior)
        assert result is not None
        assert result["bull_weight"] == 0.5
        assert result["bear_weight"] == 0.5
