"""
screener/metrics/confidence_scorer.py — White-box confidence scoring for Judge output.

Computes a 0–100 confidence score based on:
  - W1 (40%): Margin of victory from the Judge (DECISIVE/NARROW/CONTESTED)
  - W2 (35%): Unique signal source diversity in the rationale
  - W3 (25%): Hedge language penalty (how much weasel language appears)

All configuration is embedded as module-level constants — no external YAML files.
The caller can pass a custom ``weights`` dict to override W1/W2/W3 defaults.

Also detects contested_truth: True when the conviction gap exceeds the threshold
AND the margin is NARROW or CONTESTED — a signal that the debate is genuinely
unresolved and the verdict should be treated with extra skepticism.

Public API
----------
score_judge_confidence(judge_output, weights, bull_conviction, bear_conviction,
                       conviction_gap_threshold) -> tuple[float, bool, list[str]]
parse_source_categories(rationale) -> list[str]
compute_hedge_penalty(rationale) -> float
"""

from __future__ import annotations

import logging
import math

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hedge keyword defaults (embedded; no file I/O)
# ---------------------------------------------------------------------------

_DEFAULT_HEDGE_KEYWORDS: dict[str, list[str]] = {
    "tier_1": ["might", "could", "may", "possibly", "perhaps", "potentially"],
    "tier_2": [
        "uncertain",
        "unclear",
        "hard to say",
        "difficult to predict",
        "mixed signals",
    ],
    "tier_3": [
        "impossible to know",
        "completely uncertain",
        "no way to tell",
    ],
}

# Penalty applied per hedge hit: tier_1 = -1, tier_2 = -2, tier_3 = -4
_HEDGE_TIER_PENALTIES: dict[str, float] = {
    "tier_1": -1.0,
    "tier_2": -2.0,
    "tier_3": -4.0,
}

# ---------------------------------------------------------------------------
# Scoring weight defaults
# ---------------------------------------------------------------------------

_DEFAULT_WEIGHTS: dict[str, float] = {
    "W1_margin": 0.40,
    "W2_unique_sources": 0.35,
    "W3_hedge": 0.25,
}

# ---------------------------------------------------------------------------
# Source category keywords for diversity scoring
# ---------------------------------------------------------------------------

_SOURCE_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "Technical Indicators": [
        "RSI",
        "MACD",
        "moving average",
        "MA50",
        "MA200",
        "momentum",
        "volume",
        "technical",
        "chart",
        "price action",
        "breakout",
        "support",
        "resistance",
    ],
    "Earnings": [
        "EPS",
        "earnings",
        "P/E",
        "revenue",
        "guidance",
        "beat",
        "miss",
        "estimate",
        "profit",
        "income",
    ],
    "FCF": [
        "free cash flow",
        "FCF",
        "cash generation",
        "operating cash",
        "capital expenditure",
        "capex",
    ],
    "EBITDA": [
        "EBITDA",
        "enterprise value",
        "EV/EBITDA",
        "operating income",
        "margin expansion",
    ],
    "Sentiment": [
        "sentiment",
        "analyst",
        "upgrade",
        "downgrade",
        "rating",
        "outlook",
        "consensus",
        "news",
    ],
    "Disclosures": [
        "10-K",
        "10-Q",
        "SEC",
        "filing",
        "risk factor",
        "disclosure",
        "annual report",
        "quarterly report",
        "EDGAR",
    ],
}

# ---------------------------------------------------------------------------
# Margin of victory score mapping
# ---------------------------------------------------------------------------

_MARGIN_BASE_SCORES: dict[str, float] = {
    "DECISIVE": 75.0,
    "NARROW": 50.0,
    "CONTESTED": 30.0,
}


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def parse_source_categories(rationale: str) -> list[str]:
    """Identify which signal source categories are mentioned in a rationale string.

    Scans the rationale for known keywords and returns a deduplicated list of
    matching category names. Case-insensitive.

    Args:
        rationale: Free-text rationale from a Bull, Bear, or Judge output.

    Returns:
        Sorted list of category names found (subset of the six canonical categories).

    Example::

        >>> parse_source_categories("The RSI is overbought and EPS beat estimates")
        ['Earnings', 'Technical Indicators']
    """
    rationale_lower = rationale.lower()
    found: list[str] = []
    for category, keywords in _SOURCE_CATEGORY_KEYWORDS.items():
        if any(kw.lower() in rationale_lower for kw in keywords):
            found.append(category)
    return sorted(found)


def compute_hedge_penalty(rationale: str) -> float:
    """Compute the total hedge penalty for a rationale string.

    Scans for hedge phrases across three tiers and accumulates penalties.
    Tier 1 (mild hedges): -1.0 each.
    Tier 2 (moderate hedges): -2.0 each.
    Tier 3 (strong uncertainty): -4.0 each.

    The cumulative penalty is capped at -20.0 to prevent extreme outliers from
    dominating the final score.

    Args:
        rationale: Free-text rationale string.

    Returns:
        A non-positive float (e.g. -3.0 means 3 points of hedging detected).
    """
    rationale_lower = rationale.lower()
    total_penalty = 0.0
    for tier, keywords in _DEFAULT_HEDGE_KEYWORDS.items():
        tier_penalty = _HEDGE_TIER_PENALTIES[tier]
        for kw in keywords:
            if kw.lower() in rationale_lower:
                total_penalty += tier_penalty
    # Cap the total penalty so one extremely hedged sentence can't zero the score
    return max(total_penalty, -20.0)


def score_judge_confidence(
    judge_output,
    weights: dict[str, float] | None = None,
    bull_conviction: float | None = None,
    bear_conviction: float | None = None,
    conviction_gap_threshold: float = 30.0,
) -> tuple[float, bool, list[str]]:
    """Compute the white-box confidence score and contested_truth flag.

    Formula::

        raw = W1 * margin_score
            + W2 * (50 + ln(unique_sources + 1) * 10)
            + W3 * (50 + hedge_penalty)
        score = clamp(raw, 0, 100)

    Where:
      - margin_score is 75 for DECISIVE, 50 for NARROW, 30 for CONTESTED.
      - unique_sources is the count of distinct signal categories in the rationale.
      - hedge_penalty is the output of compute_hedge_penalty().

    contested_truth is True when:
      - bull_conviction and bear_conviction are both provided, AND
      - abs(bull_conviction - bear_conviction) > conviction_gap_threshold, AND
      - margin_of_victory is NARROW or CONTESTED.

    Args:
        judge_output: A JudgeOutput instance.
        weights: Optional dict overriding W1_margin, W2_unique_sources, W3_hedge.
            Missing keys fall back to _DEFAULT_WEIGHTS.
        bull_conviction: White-box bull conviction score (0–100), from conviction_node.
        bear_conviction: White-box bear conviction score (0–100), from conviction_node.
        conviction_gap_threshold: Minimum gap between conviction scores to trigger
            contested_truth (default 30.0 points).

    Returns:
        A tuple of (confidence_score, contested_truth, source_categories) where:
          - confidence_score: float in [0.0, 100.0]
          - contested_truth: bool
          - source_categories: list[str] of signal categories found in the rationale
    """
    resolved_weights = {**_DEFAULT_WEIGHTS, **(weights or {})}
    w1 = resolved_weights["W1_margin"]
    w2 = resolved_weights["W2_unique_sources"]
    w3 = resolved_weights["W3_hedge"]

    # W1: margin of victory
    margin = getattr(judge_output, "margin_of_victory", "CONTESTED")
    margin_score = _MARGIN_BASE_SCORES.get(margin, 30.0)

    # W2: source diversity from rationale
    rationale = getattr(judge_output, "rationale", "") or ""
    source_categories = parse_source_categories(rationale)
    unique_sources = len(source_categories)
    # ln(0+1)=0, ln(1+1)≈6.9, ln(2+1)≈11, ln(6+1)≈19.5 — scaled to 0-20 range on top of 50
    source_score = 50.0 + math.log(unique_sources + 1) * 10.0

    # W3: hedge penalty applied to base score of 50
    hedge_penalty = compute_hedge_penalty(rationale)
    hedge_score = 50.0 + hedge_penalty

    raw = w1 * margin_score + w2 * source_score + w3 * hedge_score
    confidence_score = max(0.0, min(100.0, raw))

    # Contested truth detection
    contested_truth = False
    if bull_conviction is not None and bear_conviction is not None:
        gap = abs(bull_conviction - bear_conviction)
        if gap > conviction_gap_threshold and margin in ("NARROW", "CONTESTED"):
            contested_truth = True
            logger.debug(
                "contested_truth=True for ticker: conviction gap=%.1f, margin=%s",
                gap,
                margin,
            )

    return confidence_score, contested_truth, source_categories
