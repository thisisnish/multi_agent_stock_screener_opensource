"""
screener/agents/adaptive_weights.py — Adaptive bull/bear weight computation.

After ≥ SCORING_MIN_SAMPLE scored verdict months, the debate adapts: Bull/Bear
weights shift toward whichever side has historically been more accurate for the
ticker.  Weights are stored in the memory doc and injected into both the Judge
prompt and the conviction scorer.

Public API
----------
compute_adaptive_weights(prior_months) -> dict | None
"""

from __future__ import annotations

import logging

from screener.agents.prompts import SCORING_MIN_SAMPLE

logger = logging.getLogger(__name__)

_DEFAULT_WEIGHTS = {"bull_weight": 0.5, "bear_weight": 0.5, "sample_size": 0}


def compute_adaptive_weights(prior_months: dict) -> dict | None:
    """Compute bull/bear accuracy weights from prior scored verdicts.

    Only verdicts where ``direction_correct`` has been set (by the eval
    pipeline after the pick closes) count toward accuracy.  Verdicts that
    are still open (``direction_correct is None``) are ignored so that
    incomplete data does not bias the weights.

    Returns ``None`` when fewer than ``SCORING_MIN_SAMPLE`` scored verdicts
    exist — callers should fall back to the stored (or default) weights.

    Args:
        prior_months: ``{month_id: verdict_dict}`` from episodic memory.

    Returns:
        Dict with ``bull_weight``, ``bear_weight``, ``sample_size`` or None.
    """
    scored = [
        v for v in prior_months.values() if v.get("direction_correct") is not None
    ]

    if len(scored) < SCORING_MIN_SAMPLE:
        logger.debug(
            "adaptive_weights: only %d scored verdicts (need %d) — skipping",
            len(scored),
            SCORING_MIN_SAMPLE,
        )
        return None

    bull_verdicts = [v for v in scored if v.get("winning_side") == "bull"]
    bear_verdicts = [v for v in scored if v.get("winning_side") == "bear"]

    bull_acc = (
        sum(1 for v in bull_verdicts if v.get("direction_correct")) / len(bull_verdicts)
        if bull_verdicts
        else 0.5
    )
    bear_acc = (
        sum(1 for v in bear_verdicts if v.get("direction_correct")) / len(bear_verdicts)
        if bear_verdicts
        else 0.5
    )

    total = bull_acc + bear_acc
    if total == 0.0:
        bull_weight = bear_weight = 0.5
    else:
        bull_weight = bull_acc / total
        bear_weight = bear_acc / total

    weights = {
        "bull_weight": round(bull_weight, 4),
        "bear_weight": round(bear_weight, 4),
        "sample_size": len(scored),
    }

    logger.debug(
        "adaptive_weights: bull_acc=%.2f bear_acc=%.2f → bull_w=%.2f bear_w=%.2f (n=%d)",
        bull_acc,
        bear_acc,
        bull_weight,
        bear_weight,
        len(scored),
    )

    return weights


def default_weights() -> dict:
    """Return the static 50/50 default weights used before enough history exists."""
    return dict(_DEFAULT_WEIGHTS)
