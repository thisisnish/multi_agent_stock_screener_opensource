"""
screener/agents/state.py — DebateState TypedDict.

This is the single mutable object threaded through the LangGraph debate graph.
Each node returns a partial dict; LangGraph merges it into the shared state.
All keys are optional (total=False) since different nodes populate different slices.
"""

from __future__ import annotations

from typing import Optional, TypedDict

from screener.lib.models import BearCaseOutput, BullCaseOutput, JudgeOutput


class DebateState(TypedDict, total=False):
    """State object threaded through all 8 debate graph nodes.

    Inputs (caller provides before invoking the graph):
        ticker: Upper-case ticker symbol, e.g. "AAPL".
        ticker_name: Human-readable company name, e.g. "Apple Inc.".
        signals: Composite-scored entry from the scoring engine — a dict
            with keys like technical, earnings, fcf, ebitda, composite_score, price, sector.
        month_id: Month identifier in "YYYY-MM" format, e.g. "2026-04".
        eval_context: Optional dict from the eval pipeline injected into the
            Judge prompt as prior-month feedback.

    memory_read outputs:
        memory_doc: Raw dict from StorageDAO.get(MEMORY, ...) or None.
        scoring_weights: Adaptive bull/bear weights from episodic memory, or None.
        prior_months: Dict of {month_id: WeekVerdict-like dict} from memory.

    build_context outputs:
        disclosure_block: Formatted EDGAR disclosure text or None.

    debate_node outputs:
        bull_output: Structured BullCaseOutput from the Bull agent.
        bear_output: Structured BearCaseOutput from the Bear agent.

    conviction_node outputs:
        bull_conviction: White-box conviction score 0–100 for the Bull case.
        bear_conviction: White-box conviction score 0–100 for the Bear case.

    judge_node outputs:
        judge_output: Structured JudgeOutput enriched with conviction scores
            and citation lists by the judge_node.

    confidence_node outputs:
        confidence_score: White-box confidence score 0–100.
        contested_truth: True when conviction gap > 30pts and margin is
            NARROW or CONTESTED.

    hard_rules outputs:
        final_action: "BUY", "SELL", or "HOLD" (may override judge if confidence < 40).
        horizon: Holding horizon — "30d", "60d", or "90d".
    """

    # --- Inputs (caller provides) ---
    ticker: str
    ticker_name: str
    signals: dict  # composite-scored entry from scoring engine
    month_id: str  # "2026-04"
    eval_context: Optional[dict]

    # --- memory_read outputs ---
    memory_doc: Optional[dict]
    scoring_weights: Optional[dict]
    prior_months: dict  # {month_id: WeekVerdict-like dict}

    # --- build_context outputs ---
    disclosure_block: Optional[str]

    # --- debate_node outputs ---
    bull_output: BullCaseOutput
    bear_output: BearCaseOutput

    # --- conviction_node outputs ---
    bull_conviction: Optional[float]
    bear_conviction: Optional[float]

    # --- judge_node outputs ---
    judge_output: JudgeOutput

    # --- confidence_node outputs ---
    confidence_score: float
    contested_truth: bool

    # --- hard_rules outputs ---
    final_action: str
    horizon: str
