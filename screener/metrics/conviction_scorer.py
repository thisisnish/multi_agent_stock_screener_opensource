"""
screener/metrics/conviction_scorer.py — White-box conviction scoring for Bull/Bear outputs.

Conviction is a 0–100 score that measures the *strength* and *diversity* of the
arguments produced by one side of the debate. It is entirely white-box (no LLM
tokens consumed) and feeds into the Judge prompt and the contested_truth detector.

Scoring components:
  - Argument count: up to 30 points (5 pts per argument, max 6)
  - Key catalysts / counter_arguments count: up to 20 points (5 pts each, max 4)
  - Signal citation diversity: up to 30 points (5 pts per unique category)
  - Conceded counter-argument present: 10 points (rewards intellectual honesty)
  - Hedge penalty: up to -10 points (drawn from confidence_scorer)

Public API
----------
score_conviction(agent_output, side: "bull" | "bear") -> float
"""

from __future__ import annotations

import logging

from screener.metrics.confidence_scorer import compute_hedge_penalty

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------

_POINTS_PER_ARGUMENT = 5.0
_MAX_ARGUMENT_POINTS = 30.0  # capped at 6 arguments

_POINTS_PER_CATALYST = 5.0
_MAX_CATALYST_POINTS = 20.0  # capped at 4 items

_POINTS_PER_CITATION = 5.0
_MAX_CITATION_POINTS = 30.0  # capped at 6 unique categories

_CONCESSION_BONUS = 10.0  # reward for acknowledging the opposing case

_MAX_HEDGE_DEDUCTION = 10.0  # hedge penalty capped at -10 for conviction


def score_conviction(agent_output, side: str) -> float:
    """Compute a white-box conviction score for one side's debate output.

    The score rewards:
      - Having more (and more diverse) arguments
      - Citing multiple distinct signal categories
      - Acknowledging the strongest counter-argument (intellectual honesty signal)

    And penalises:
      - Hedge language in arguments or the counter_argument text

    Args:
        agent_output: A BullCaseOutput or BearCaseOutput instance.
        side: ``"bull"`` or ``"bear"`` — used to extract the correct field names.

    Returns:
        Float in [0.0, 100.0].
    """
    side_lower = side.lower()

    # --- Extract fields by side ---
    if side_lower == "bull":
        primary_args: list[str] = getattr(agent_output, "bull_arguments", []) or []
        secondary_items: list[str] = getattr(agent_output, "key_catalysts", []) or []
        counter_text: str = getattr(agent_output, "bull_counter_argument", "") or ""
    else:
        primary_args = getattr(agent_output, "bear_arguments", []) or []
        secondary_items = getattr(agent_output, "counter_arguments", []) or []
        counter_text = getattr(agent_output, "bear_counter_argument", "") or ""

    signal_citations: list[str] = getattr(agent_output, "signal_citations", []) or []

    # --- Component scores ---

    # 1. Primary argument count (capped)
    arg_points = min(len(primary_args) * _POINTS_PER_ARGUMENT, _MAX_ARGUMENT_POINTS)

    # 2. Secondary item count (catalysts for bull, counter_arguments for bear)
    catalyst_points = min(
        len(secondary_items) * _POINTS_PER_CATALYST, _MAX_CATALYST_POINTS
    )

    # 3. Signal citation diversity
    unique_citations = len(set(signal_citations))
    citation_points = min(unique_citations * _POINTS_PER_CITATION, _MAX_CITATION_POINTS)

    # 4. Concession bonus — non-empty counter_argument text earns the full bonus
    concession_points = _CONCESSION_BONUS if counter_text.strip() else 0.0

    # 5. Hedge penalty — computed over all argument text combined
    all_text = " ".join(primary_args + secondary_items + [counter_text])
    raw_hedge_penalty = compute_hedge_penalty(all_text)
    # Clamp to the conviction-specific max deduction (confidence_scorer can go to -20)
    hedge_deduction = max(raw_hedge_penalty, -_MAX_HEDGE_DEDUCTION)

    raw_score = (
        arg_points
        + catalyst_points
        + citation_points
        + concession_points
        + hedge_deduction  # already non-positive
    )

    conviction = max(0.0, min(100.0, raw_score))

    logger.debug(
        "conviction score side=%s: args=%.0f catalysts=%.0f citations=%.0f "
        "concession=%.0f hedge=%.0f → %.1f",
        side_lower,
        arg_points,
        catalyst_points,
        citation_points,
        concession_points,
        hedge_deduction,
        conviction,
    )

    return conviction
