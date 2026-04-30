"""
screener/eval/scorer.py — Pick scoring utilities.

Two scoring paths:

1. **Pure math** (primary, no tokens): derives a ScoreResult from the
   ``beat_spy`` flag already stored in the pick ledger at close time.
   Used by ``run_eval_main()`` in production.

2. **LLM rubric** (optional, P1-08a): invokes the Judge LLM with a scoring
   prompt to produce richer sub-scores. Not called in the production eval path
   but available for offline deep-scoring experiments.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from screener.lib.models import RubricDefinition, ScoreResult

if TYPE_CHECKING:
    from screener.lib.config_loader import AppConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pure math path (production)
# ---------------------------------------------------------------------------


def score_pick_pure_math(pick: dict) -> ScoreResult | None:
    """Build a ScoreResult from a closed pick's stored beat_spy flag.

    Returns None if pick action is HOLD or beat_spy is missing — these cannot
    produce a directional score.

    Args:
        pick: Dict with keys: action, beat_spy, pick_return_pct,
              spy_return_pct, bull_signal_citations, bear_signal_citations.

    Returns:
        ScoreResult with flat scores (70) and directional accuracy fields
        populated from beat_spy, or None.
    """
    action = pick.get("action", "HOLD")
    beat_spy = pick.get("beat_spy")

    if beat_spy is None or action == "HOLD":
        return None

    bull_accuracy = beat_spy if action == "BUY" else None
    bear_accuracy = beat_spy if action == "SELL" else None

    return ScoreResult(
        score=70,
        accuracy=beat_spy,
        confidence_alignment=70,
        timing_quality=70,
        risk_management=70,
        error_flags=[],
        rationale="",
        bull_accuracy=bull_accuracy,
        bear_accuracy=bear_accuracy,
    )


def score_picks_pure_math(picks: list[dict]) -> list[ScoreResult]:
    """Score a list of closed picks using pure math.

    Skips HOLD actions and picks missing beat_spy.

    Args:
        picks: List of pick dicts, each with at minimum ``action`` and
               ``beat_spy`` keys.

    Returns:
        List of ScoreResult instances (may be shorter than input list).
    """
    results = []
    for pick in picks:
        result = score_pick_pure_math(pick)
        if result is not None:
            results.append(result)
    return results


# ---------------------------------------------------------------------------
# LLM rubric path (P1-08a — optional deep scoring)
# ---------------------------------------------------------------------------


def score_judge_pick(
    ticker: str,
    decision: str,
    entry_date: str,
    entry_price: float,
    exit_date: str | None,
    exit_price: float | None,
    confidence: float,
    rationale: str,
    app_config: "AppConfig",
    current_price: float | None = None,
    spy_return: float | None = None,
    rubric: RubricDefinition | None = None,
) -> ScoreResult:
    """LLM rubric scoring for a single pick.

    Builds a structured prompt from the pick metadata and rubric definition,
    then invokes the judge LLM with structured output binding to ScoreResult.
    This is the richer eval path — it costs tokens but produces sub-scores
    and error flags that the pure math path cannot.

    Args:
        ticker: Upper-case ticker symbol (e.g. "AAPL").
        decision: "BUY", "SELL", or "HOLD".
        entry_date: ISO date string of entry.
        entry_price: Price at entry.
        exit_date: ISO date string of exit, or None if still open.
        exit_price: Price at exit, or None if still open.
        confidence: White-box confidence score (0–100).
        rationale: Judge rationale text from the original debate.
        app_config: AppConfig instance for LLM factory routing.
        current_price: Most recent price if exit not yet available.
        spy_return: SPY return over the same period (%) for comparison.
        rubric: Rubric to score against; defaults to default_v1.

    Returns:
        ScoreResult with LLM-populated sub-scores and error flags.
    """
    from langchain_core.messages import HumanMessage, SystemMessage

    from screener.eval.rubric import get_default_rubric
    from screener.lib.agent_creator import get_structured_llm

    if rubric is None:
        rubric = get_default_rubric()

    # Compute return metrics if exit data is available
    pick_return: float | None = None
    beat_spy: bool | None = None
    if entry_price and exit_price:
        pick_return = round((exit_price - entry_price) / entry_price * 100, 2)
        if spy_return is not None:
            beat_spy = pick_return > spy_return

    # Detect sentiment bias words in rationale
    bias_hits = [
        w for w in rubric.sentiment_bias_words if w.lower() in rationale.lower()
    ]

    system_prompt = f"""\
You are an investment decision quality evaluator. Your task is to score a stock
pick made by an AI Judge on a structured rubric.

Rubric weights (must sum to 100):
  accuracy_weight:              {rubric.accuracy_weight}
  confidence_alignment_weight:  {rubric.confidence_alignment_weight}
  timing_quality_weight:        {rubric.timing_quality_weight}
  risk_management_weight:       {rubric.risk_management_weight}

Overconfidence threshold: {rubric.overconfidence_threshold} pts above actual accuracy.
Poor timing threshold: entry/exit within {rubric.poor_timing_threshold}% of optimal.

Error flags to detect (report any that apply):
{chr(10).join(f"  {k}: {v}" for k, v in rubric.error_flags_schema.items())}

Return a ScoreResult with integer scores 0–100 and a list of triggered error_flags.
"""

    outcome_block = ""
    if pick_return is not None:
        outcome_block = f"  pick_return_pct:  {pick_return:.2f}%\n"
        if spy_return is not None:
            outcome_block += f"  spy_return_pct:   {spy_return:.2f}%\n"
            outcome_block += f"  beat_spy:         {beat_spy}\n"
    elif current_price is not None:
        unrealised = (
            round((current_price - entry_price) / entry_price * 100, 2)
            if entry_price
            else None
        )
        if unrealised is not None:
            outcome_block = (
                f"  unrealised_return_pct: {unrealised:.2f}% (position still open)\n"
            )

    eval_prompt = f"""\
Pick to evaluate:
  ticker:       {ticker}
  decision:     {decision}
  entry_date:   {entry_date}
  entry_price:  {entry_price}
  exit_date:    {exit_date or "still open"}
  exit_price:   {exit_price or "N/A"}
  confidence:   {confidence:.1f} / 100
{outcome_block}
Sentiment bias words detected in rationale: {bias_hits or "none"}

Judge rationale:
{rationale}

Score this pick according to the rubric. Populate all fields of ScoreResult.
"""

    llm = get_structured_llm("judge", ScoreResult, app_config)
    result: ScoreResult = llm.invoke(
        [SystemMessage(content=system_prompt), HumanMessage(content=eval_prompt)]
    )

    # Backfill directional accuracy fields that the LLM cannot know from prompt alone
    if beat_spy is not None:
        result.accuracy = beat_spy
        result.bull_accuracy = beat_spy if decision == "BUY" else None
        result.bear_accuracy = beat_spy if decision == "SELL" else None

    return result
