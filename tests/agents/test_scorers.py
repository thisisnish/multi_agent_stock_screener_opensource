"""
tests/agents/test_scorers.py — Unit tests for white-box scorer modules.

Tests cover:
- parse_source_categories: keyword detection in rationale strings
- compute_hedge_penalty: tier-based penalty accumulation
- score_judge_confidence: full scoring pipeline for DECISIVE and CONTESTED margins
- conviction_scorer: bull/bear scoring with rich vs sparse outputs
- hard_rules node: confidence threshold overrides
- contested_truth: conviction gap detection
"""

from screener.agents.nodes import hard_rules
from screener.agents.state import DebateState
from screener.lib.models import BearCaseOutput, BullCaseOutput, JudgeOutput
from screener.metrics.confidence_scorer import (
    compute_hedge_penalty,
    parse_source_categories,
    score_judge_confidence,
)
from screener.metrics.conviction_scorer import score_conviction


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_judge_output(
    action="BUY",
    margin="DECISIVE",
    winning_side="BULL",
    rationale="",
    horizon="60d",
) -> JudgeOutput:
    return JudgeOutput(
        action=action,
        judge_self_confidence=70,
        horizon=horizon,
        winning_side=winning_side,
        margin_of_victory=margin,
        decisive_factor="strong earnings beat",
        rationale=rationale,
    )


def _make_debate_state(judge_output, confidence=75.0) -> DebateState:
    return DebateState(
        ticker="AAPL",
        ticker_name="Apple Inc.",
        signals={"price": 200.0},
        month_id="2026-04",
        judge_output=judge_output,
        confidence_score=confidence,
        final_action=judge_output.action,
        horizon="60d",
        bull_output=BullCaseOutput(),
        bear_output=BearCaseOutput(),
        prior_months={},
    )


# ---------------------------------------------------------------------------
# parse_source_categories
# ---------------------------------------------------------------------------


def test_parse_source_categories_technical():
    """RSI keyword maps to Technical Indicators category."""
    categories = parse_source_categories("The RSI is at 65, showing bullish momentum.")
    assert "Technical Indicators" in categories


def test_parse_source_categories_multiple():
    """Multiple signal keywords return multiple categories."""
    rationale = "Strong EPS beat and RSI momentum with FCF yield expansion."
    categories = parse_source_categories(rationale)
    assert "Technical Indicators" in categories
    assert "Earnings" in categories
    assert "FCF" in categories


def test_parse_source_categories_empty():
    """Empty string returns empty list."""
    assert parse_source_categories("") == []


def test_parse_source_categories_disclosures():
    """SEC filing keywords map to Disclosures."""
    categories = parse_source_categories("The 10-K discloses significant risk factors.")
    assert "Disclosures" in categories


def test_parse_source_categories_returns_sorted():
    """Result is always sorted for determinism."""
    cats = parse_source_categories("RSI EBITDA EPS FCF")
    assert cats == sorted(cats)


# ---------------------------------------------------------------------------
# compute_hedge_penalty
# ---------------------------------------------------------------------------


def test_compute_hedge_penalty_tier1():
    """Tier-1 hedge word 'might' produces -1.0 penalty."""
    penalty = compute_hedge_penalty("This might be a good entry point.")
    assert penalty == -1.0


def test_compute_hedge_penalty_no_hedges():
    """Clean rationale produces 0.0 penalty."""
    penalty = compute_hedge_penalty("Strong buy signal with decisive earnings beat.")
    assert penalty == 0.0


def test_compute_hedge_penalty_multiple_tiers():
    """Multiple hedge words accumulate penalty, capped at -20."""
    # tier_2 "uncertain" = -2, tier_1 "might" = -1, tier_1 "could" = -1 → -4
    penalty = compute_hedge_penalty(
        "The outcome is uncertain and might could be difficult."
    )
    assert penalty <= -3.0


def test_compute_hedge_penalty_cap():
    """Very hedged text is capped at -20, not lower."""
    heavily_hedged = (
        "might could may possibly perhaps potentially "
        "uncertain unclear mixed signals hard to say difficult to predict "
        "impossible to know completely uncertain no way to tell"
    )
    penalty = compute_hedge_penalty(heavily_hedged)
    assert penalty >= -20.0


# ---------------------------------------------------------------------------
# score_judge_confidence
# ---------------------------------------------------------------------------


def test_score_judge_confidence_decisive():
    """DECISIVE margin produces a high confidence score."""
    judge = _make_judge_output(
        action="BUY",
        margin="DECISIVE",
        rationale="Strong RSI momentum and EPS beat drove EBITDA expansion.",
    )
    score, contested, categories = score_judge_confidence(judge)
    assert score > 60.0
    assert contested is False
    assert "Technical Indicators" in categories


def test_score_judge_confidence_contested():
    """CONTESTED margin produces a low score."""
    judge = _make_judge_output(
        action="HOLD",
        margin="CONTESTED",
        winning_side="NEUTRAL",
        rationale="Mixed signals with uncertain outlook.",
    )
    score, contested, _ = score_judge_confidence(judge)
    assert score < 60.0


def test_score_judge_confidence_bounds():
    """Score is always in [0, 100]."""
    for margin in ("DECISIVE", "NARROW", "CONTESTED"):
        judge = _make_judge_output(margin=margin)
        score, _, _ = score_judge_confidence(judge)
        assert 0.0 <= score <= 100.0


# ---------------------------------------------------------------------------
# score_conviction (conviction_scorer)
# ---------------------------------------------------------------------------


def test_conviction_scorer_bull_rich():
    """BullCaseOutput with rich arguments scores > 0."""
    bull = BullCaseOutput(
        bull_arguments=["Strong FCF yield", "RSI momentum", "Earnings beat"],
        key_catalysts=["Product launch", "Buyback program"],
        bull_counter_argument="Macro headwinds are a valid concern.",
        signal_citations=["Technical", "Earnings", "FCF"],
    )
    score = score_conviction(bull, "bull")
    assert score > 0.0


def test_conviction_scorer_bear_empty_lower():
    """BearCaseOutput with no arguments scores lower than a rich bull case."""
    rich_bull = BullCaseOutput(
        bull_arguments=["Strong FCF", "RSI momentum", "Beat estimates", "Buyback"],
        key_catalysts=["New product", "Margin expansion"],
        bull_counter_argument="Bear has a point on valuation.",
        signal_citations=["Technical", "Earnings", "FCF", "EBITDA"],
    )
    empty_bear = BearCaseOutput()

    rich_score = score_conviction(rich_bull, "bull")
    empty_score = score_conviction(empty_bear, "bear")
    assert rich_score > empty_score


def test_conviction_scorer_concession_bonus():
    """Non-empty counter_argument earns the concession bonus."""
    with_concession = BullCaseOutput(
        bull_arguments=["Arg1"],
        bull_counter_argument="Valid bear point.",
    )
    without_concession = BullCaseOutput(
        bull_arguments=["Arg1"],
        bull_counter_argument="",
    )
    assert score_conviction(with_concession, "bull") > score_conviction(
        without_concession, "bull"
    )


def test_conviction_scorer_score_bounded():
    """Score is always in [0, 100]."""
    bear = BearCaseOutput(
        bear_arguments=["A"] * 10,  # more than max
        counter_arguments=["C"] * 10,
        bear_counter_argument="Concedes.",
        signal_citations=[
            "Technical",
            "Earnings",
            "FCF",
            "EBITDA",
            "Sentiment",
            "Disclosures",
        ],
    )
    score = score_conviction(bear, "bear")
    assert 0.0 <= score <= 100.0


# ---------------------------------------------------------------------------
# hard_rules node
# ---------------------------------------------------------------------------


def test_hard_rules_forces_hold_below_40():
    """confidence < 40 forces HOLD with 30d horizon regardless of judge action."""
    judge = _make_judge_output(action="BUY", margin="DECISIVE")
    state = _make_debate_state(judge, confidence=30.0)
    result = hard_rules(state)
    assert result["final_action"] == "HOLD"
    assert result["horizon"] == "30d"


def test_hard_rules_passes_buy_high_confidence():
    """confidence=80 with BUY action → BUY with 90d horizon."""
    judge = _make_judge_output(action="BUY", margin="DECISIVE")
    state = _make_debate_state(judge, confidence=80.0)
    result = hard_rules(state)
    assert result["final_action"] == "BUY"
    assert result["horizon"] == "90d"


def test_hard_rules_horizon_60d():
    """confidence=60 → 60d horizon."""
    judge = _make_judge_output(action="SELL", margin="NARROW")
    state = _make_debate_state(judge, confidence=60.0)
    result = hard_rules(state)
    assert result["horizon"] == "60d"


def test_hard_rules_boundary_at_40():
    """Exactly confidence=40 passes through (not forced to HOLD)."""
    judge = _make_judge_output(action="BUY", margin="NARROW")
    state = _make_debate_state(judge, confidence=40.0)
    result = hard_rules(state)
    # 40 >= 40, so it should pass through
    assert result["final_action"] == "BUY"


def test_hard_rules_forces_hold_at_39():
    """confidence=39.9 is still forced to HOLD."""
    judge = _make_judge_output(action="SELL", margin="CONTESTED")
    state = _make_debate_state(judge, confidence=39.9)
    result = hard_rules(state)
    assert result["final_action"] == "HOLD"


# ---------------------------------------------------------------------------
# Contested truth via conviction gap
# ---------------------------------------------------------------------------


def test_conviction_gap_triggers_contested_truth_narrow():
    """Conviction gap > 30 + NARROW margin → contested_truth=True."""
    judge = _make_judge_output(action="BUY", margin="NARROW")
    _, contested, _ = score_judge_confidence(
        judge,
        bull_conviction=90.0,
        bear_conviction=20.0,
    )
    # Gap = 70 > 30 threshold and NARROW margin
    assert contested is True


def test_conviction_gap_no_contested_decisive():
    """Even with a large gap, DECISIVE margin does not trigger contested_truth."""
    judge = _make_judge_output(action="BUY", margin="DECISIVE")
    _, contested, _ = score_judge_confidence(
        judge,
        bull_conviction=90.0,
        bear_conviction=20.0,
    )
    assert contested is False


def test_conviction_gap_small_no_contested():
    """Small conviction gap does not trigger contested_truth even with NARROW margin."""
    judge = _make_judge_output(action="BUY", margin="NARROW")
    _, contested, _ = score_judge_confidence(
        judge,
        bull_conviction=55.0,
        bear_conviction=50.0,
    )
    # Gap = 5 < 30 threshold
    assert contested is False


def test_conviction_gap_contested_margin():
    """Conviction gap > 30 + CONTESTED margin → contested_truth=True."""
    judge = _make_judge_output(
        action="HOLD", margin="CONTESTED", winning_side="NEUTRAL"
    )
    _, contested, _ = score_judge_confidence(
        judge,
        bull_conviction=85.0,
        bear_conviction=30.0,
    )
    assert contested is True
