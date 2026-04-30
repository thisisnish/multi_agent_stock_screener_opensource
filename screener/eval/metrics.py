"""
screener/eval/metrics.py — Aggregation and reporting for eval score results.

Public API:
    compute_metrics(period, score_results, picks_metadata, picks_raw) -> EvalMetrics
    detect_systematic_issues(metrics) -> list[str]
    format_metrics_report(metrics) -> str
    compute_acid_test(picks) -> dict
    compute_disclosure_citation_rate(picks) -> float | None
"""

from __future__ import annotations

import logging

from screener.lib.models import EvalMetrics, ScoreResult

logger = logging.getLogger(__name__)

# Bias thresholds: if one directional accuracy is >= this many points above the
# other, we label that direction as the dominant bias.
_BIAS_THRESHOLD: float = 20.0

# Overconfidence: if avg_confidence exceeds overall_accuracy by this amount, flag it.
_OVERCONFIDENCE_THRESHOLD: float = 20.0

# Poor discrimination: if high-confidence accuracy is not better than
# low-confidence accuracy by at least this many points, flag calibration drift.
_CALIBRATION_DRIFT_THRESHOLD: float = 10.0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pct(outcomes: list[bool]) -> float | None:
    """Return percentage of True values, or None for empty list."""
    return round(sum(outcomes) / len(outcomes) * 100, 1) if outcomes else None


def _compute_confidence_bins(
    picks: list[dict],
) -> tuple[float | None, float | None, float | None]:
    """Compute accuracy per confidence tier from raw pick dicts.

    Returns (high_accuracy, medium_accuracy, low_accuracy).
    Each is None if the tier has no closed picks.

    Tier boundaries:
        high:   confidence_score >= 70
        medium: 40 <= confidence_score < 70
        low:    confidence_score < 40

    HOLD picks and picks without beat_spy are skipped.
    """
    tiers: dict[str, list[bool]] = {"high": [], "medium": [], "low": []}
    for pick in picks:
        confidence = pick.get("confidence_score")
        beat_spy = pick.get("beat_spy")
        if confidence is None or beat_spy is None:
            continue
        if pick.get("action", "HOLD") == "HOLD":
            continue
        if confidence >= 70:
            tiers["high"].append(beat_spy)
        elif confidence >= 40:
            tiers["medium"].append(beat_spy)
        else:
            tiers["low"].append(beat_spy)

    return _pct(tiers["high"]), _pct(tiers["medium"]), _pct(tiers["low"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_metrics(
    period: str,
    score_results: list[ScoreResult],
    picks_metadata: list[dict] | None = None,
    picks_raw: list[dict] | None = None,
) -> EvalMetrics:
    """Aggregate ScoreResult objects into an EvalMetrics summary.

    Args:
        period: "YYYY-MM" month identifier.
        score_results: Non-empty list of ScoreResult objects from the scorer.
        picks_metadata: Optional list of dicts with ``return_pct`` and
            ``sector`` keys — used for sector concentration and return curves.
        picks_raw: Optional list of raw pick dicts — used for confidence bin
            computation. Must include ``confidence_score``, ``beat_spy``, and
            ``action`` keys.

    Returns:
        EvalMetrics with all fields populated.

    Raises:
        ValueError: If score_results is empty.
    """
    if not score_results:
        raise ValueError("score_results must be non-empty to compute metrics")

    total = len(score_results)

    # Closed vs open — open picks have accuracy=None
    closed = [r for r in score_results if r.accuracy is not None]
    open_picks = [r for r in score_results if r.accuracy is None]

    # Overall accuracy
    overall_accuracy: float | None = None
    if closed:
        overall_accuracy = round(sum(1 for r in closed if r.accuracy) / len(closed) * 100, 1)

    # Bull accuracy (BUY picks that beat SPY)
    bull_results = [r for r in closed if r.bull_accuracy is not None]
    bull_accuracy: float | None = _pct([r.bull_accuracy for r in bull_results])  # type: ignore[arg-type]

    # Bear accuracy (SELL picks that beat SPY)
    bear_results = [r for r in closed if r.bear_accuracy is not None]
    bear_accuracy: float | None = _pct([r.bear_accuracy for r in bear_results])  # type: ignore[arg-type]

    # Average scores
    avg_score = round(sum(r.score for r in score_results) / total, 1)
    avg_confidence = round(
        sum(r.confidence_alignment for r in score_results) / total, 1
    )

    # Confidence calibration: |avg_confidence - overall_accuracy|
    confidence_calibration = (
        round(abs(avg_confidence - overall_accuracy), 1)
        if overall_accuracy is not None
        else 0.0
    )

    # Error flag frequency
    error_flag_frequency: dict[str, int] = {}
    for r in score_results:
        for flag in r.error_flags:
            error_flag_frequency[flag] = error_flag_frequency.get(flag, 0) + 1

    # Directional bias
    directional_bias = _compute_directional_bias(bull_accuracy, bear_accuracy)

    # Sector concentration from metadata
    sector_concentration: dict[str, int] = {}
    avg_return_correct: float | None = None
    avg_return_wrong: float | None = None

    if picks_metadata:
        for meta in picks_metadata:
            sector = meta.get("sector", "Unknown") or "Unknown"
            sector_concentration[sector] = sector_concentration.get(sector, 0) + 1

        # Return curves need to cross-reference with accuracy — use index alignment
        if len(picks_metadata) == len(score_results):
            correct_returns = []
            wrong_returns = []
            for meta, result in zip(picks_metadata, score_results):
                ret = meta.get("return_pct")
                if ret is not None and result.accuracy is not None:
                    if result.accuracy:
                        correct_returns.append(ret)
                    else:
                        wrong_returns.append(ret)
            avg_return_correct = (
                round(sum(correct_returns) / len(correct_returns), 2)
                if correct_returns else None
            )
            avg_return_wrong = (
                round(sum(wrong_returns) / len(wrong_returns), 2)
                if wrong_returns else None
            )

    # Confidence bins from raw picks
    high_acc: float | None = None
    med_acc: float | None = None
    low_acc: float | None = None
    if picks_raw:
        high_acc, med_acc, low_acc = _compute_confidence_bins(picks_raw)

    return EvalMetrics(
        period=period,
        total_picks=total,
        closed_picks=len(closed),
        open_picks=len(open_picks),
        overall_accuracy=overall_accuracy,
        bull_accuracy=bull_accuracy,
        bear_accuracy=bear_accuracy,
        avg_confidence=avg_confidence,
        avg_score=avg_score,
        confidence_calibration=confidence_calibration,
        error_flag_frequency=error_flag_frequency,
        directional_bias=directional_bias,
        sector_concentration=sector_concentration,
        average_return_when_correct=avg_return_correct,
        average_return_when_wrong=avg_return_wrong,
        high_confidence_accuracy=high_acc,
        medium_confidence_accuracy=med_acc,
        low_confidence_accuracy=low_acc,
    )


def _compute_directional_bias(
    bull_accuracy: float | None,
    bear_accuracy: float | None,
) -> str:
    """Return "bullish", "bearish", or "balanced" based on directional accuracy gap."""
    if bull_accuracy is None and bear_accuracy is None:
        return "balanced"
    if bull_accuracy is None:
        return "bearish"
    if bear_accuracy is None:
        return "bullish"
    diff = bull_accuracy - bear_accuracy
    if diff >= _BIAS_THRESHOLD:
        return "bullish"
    if diff <= -_BIAS_THRESHOLD:
        return "bearish"
    return "balanced"


def detect_systematic_issues(metrics: EvalMetrics) -> list[str]:
    """Identify systematic quality issues from EvalMetrics.

    Checks performed:
    - Overconfidence: avg_confidence exceeds overall_accuracy by threshold
    - Poor calibration: high-confidence tier not outperforming low tier
    - Directional bias: strong bull or bear skew in accuracy
    - Low disclosure citation: < 20% analyses cite SEC filings
    - High error flag concentration: any single flag in > 30% of picks

    Args:
        metrics: Computed EvalMetrics from compute_metrics().

    Returns:
        List of human-readable issue strings (empty if no issues detected).
    """
    issues: list[str] = []

    # Overconfidence
    if metrics.overall_accuracy is not None:
        if metrics.avg_confidence - metrics.overall_accuracy >= _OVERCONFIDENCE_THRESHOLD:
            issues.append(
                f"Overconfidence: avg confidence {metrics.avg_confidence:.1f} "
                f"vs accuracy {metrics.overall_accuracy:.1f}% "
                f"(gap: {metrics.avg_confidence - metrics.overall_accuracy:.1f}pts)"
            )

    # Calibration drift — high tier should beat low tier by threshold
    if (
        metrics.high_confidence_accuracy is not None
        and metrics.low_confidence_accuracy is not None
    ):
        gap = metrics.high_confidence_accuracy - metrics.low_confidence_accuracy
        if gap < _CALIBRATION_DRIFT_THRESHOLD:
            issues.append(
                f"Calibration drift: high-confidence accuracy "
                f"({metrics.high_confidence_accuracy:.1f}%) is not meaningfully "
                f"better than low-confidence ({metrics.low_confidence_accuracy:.1f}%)"
            )

    # Directional bias
    if metrics.directional_bias in ("bullish", "bearish"):
        issues.append(
            f"Directional bias detected: {metrics.directional_bias} "
            f"(bull={metrics.bull_accuracy}, bear={metrics.bear_accuracy})"
        )

    # Low disclosure citation rate
    if (
        metrics.disclosure_citation_rate is not None
        and metrics.disclosure_citation_rate < 0.20
    ):
        pct = round(metrics.disclosure_citation_rate * 100, 1)
        issues.append(
            f"Low disclosure citation rate: {pct}% of picks cited SEC filings"
        )

    # High error flag concentration
    if metrics.total_picks > 0:
        for flag, count in metrics.error_flag_frequency.items():
            rate = count / metrics.total_picks
            if rate >= 0.30:
                issues.append(
                    f"Error flag '{flag}' appears in {count}/{metrics.total_picks} picks "
                    f"({round(rate * 100, 1)}%)"
                )

    return issues


def format_metrics_report(metrics: EvalMetrics) -> str:
    """Format an EvalMetrics object as a human-readable text report.

    Args:
        metrics: Computed EvalMetrics.

    Returns:
        Multi-line string suitable for logging or email inclusion.
    """
    lines = [
        f"=== Eval Report: {metrics.period} ===",
        f"Total picks:          {metrics.total_picks}",
        f"  Closed:             {metrics.closed_picks}",
        f"  Open:               {metrics.open_picks}",
        "",
        f"Overall accuracy:     {metrics.overall_accuracy:.1f}%"
        if metrics.overall_accuracy is not None else "Overall accuracy:     N/A (no closed picks)",
        f"Bull accuracy:        {metrics.bull_accuracy:.1f}%"
        if metrics.bull_accuracy is not None else "Bull accuracy:        N/A",
        f"Bear accuracy:        {metrics.bear_accuracy:.1f}%"
        if metrics.bear_accuracy is not None else "Bear accuracy:        N/A",
        "",
        f"Avg score:            {metrics.avg_score:.1f} / 100",
        f"Avg confidence:       {metrics.avg_confidence:.1f} / 100",
        f"Confidence calib:     {metrics.confidence_calibration:.1f} pts gap",
        f"Directional bias:     {metrics.directional_bias or 'balanced'}",
    ]

    if metrics.high_confidence_accuracy is not None or metrics.medium_confidence_accuracy is not None:
        lines += [
            "",
            "Confidence bins:",
            f"  High (>=70):        {metrics.high_confidence_accuracy:.1f}%"
            if metrics.high_confidence_accuracy is not None else "  High (>=70):        N/A",
            f"  Medium (40-69):     {metrics.medium_confidence_accuracy:.1f}%"
            if metrics.medium_confidence_accuracy is not None else "  Medium (40-69):     N/A",
            f"  Low (<40):          {metrics.low_confidence_accuracy:.1f}%"
            if metrics.low_confidence_accuracy is not None else "  Low (<40):          N/A",
        ]

    if metrics.disclosure_citation_rate is not None:
        pct = round(metrics.disclosure_citation_rate * 100, 1)
        lines += ["", f"Disclosure citation:  {pct}%"]

    if metrics.error_flag_frequency:
        lines += ["", "Error flags:"]
        for flag, count in sorted(
            metrics.error_flag_frequency.items(), key=lambda x: -x[1]
        ):
            lines.append(f"  {flag}: {count}")

    if metrics.sector_concentration:
        lines += ["", "Sector concentration:"]
        for sector, count in sorted(
            metrics.sector_concentration.items(), key=lambda x: -x[1]
        ):
            lines.append(f"  {sector}: {count}")

    lines.append("")
    return "\n".join(lines)


def compute_acid_test(picks: list[dict]) -> dict:
    """Group closed picks by confidence tier and compute max drawdown per tier.

    Max drawdown here is the worst (most negative) pick_return_pct within each
    tier, expressed as a positive number (e.g. -8% return → drawdown of 8).

    Args:
        picks: List of raw pick dicts with keys: confidence_score, beat_spy,
               action, pick_return_pct.

    Returns:
        Dict keyed by tier label ("High", "Medium", "Low"), each value a dict:
        {
            "count": int,
            "accuracy_pct": float | None,
            "max_drawdown": float | None,   # worst loss as positive number
            "avg_return": float | None,
        }
    """
    tiers: dict[str, dict] = {
        "High": {"picks": [], "returns": []},
        "Medium": {"picks": [], "returns": []},
        "Low": {"picks": [], "returns": []},
    }

    for pick in picks:
        confidence = pick.get("confidence_score")
        beat_spy = pick.get("beat_spy")
        action = pick.get("action", "HOLD")
        ret = pick.get("pick_return_pct")

        if action == "HOLD" or confidence is None or beat_spy is None:
            continue

        if confidence >= 70:
            tier = "High"
        elif confidence >= 40:
            tier = "Medium"
        else:
            tier = "Low"

        tiers[tier]["picks"].append(beat_spy)
        if ret is not None:
            tiers[tier]["returns"].append(ret)

    result: dict = {}
    for label, data in tiers.items():
        outcomes: list[bool] = data["picks"]
        returns: list[float] = data["returns"]
        accuracy = (
            round(sum(outcomes) / len(outcomes) * 100, 1) if outcomes else None
        )
        max_drawdown: float | None = None
        avg_return: float | None = None
        if returns:
            worst = min(returns)
            max_drawdown = round(abs(worst), 2) if worst < 0 else 0.0
            avg_return = round(sum(returns) / len(returns), 2)

        result[label] = {
            "count": len(outcomes),
            "accuracy_pct": accuracy,
            "max_drawdown": max_drawdown,
            "avg_return": avg_return,
        }

    return result


def compute_disclosure_citation_rate(picks: list[dict]) -> float | None:
    """Compute the fraction of picks where Bull OR Bear cited SEC disclosures.

    A pick is counted as having a disclosure citation if the string
    "Disclosures" appears in either ``bull_signal_citations`` or
    ``bear_signal_citations``.

    Args:
        picks: List of raw pick dicts.

    Returns:
        Float in [0.0, 1.0], or None if picks list is empty.
    """
    if not picks:
        return None

    cited = 0
    for pick in picks:
        bull_cites = pick.get("bull_signal_citations") or []
        bear_cites = pick.get("bear_signal_citations") or []
        all_cites = list(bull_cites) + list(bear_cites)
        if any("Disclosures" in c for c in all_cites):
            cited += 1

    return round(cited / len(picks), 4)
