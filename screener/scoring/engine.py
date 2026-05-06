"""
screener/scoring/engine.py — Pure scoring functions extracted from screener_job.

Both functions are stateless and dependency-free (no I/O, no LLM calls).

Public API
----------
compute_composite_scores(signals_by_symbol, factor_scores, factor_weights, apply_gate)
    -> list[dict]   # each entry is {**sig, composite_score, ma200_gate}

apply_sector_cap(gated, top_n, max_per_sector)
    -> list[dict]   # top-N picks respecting per-sector concentration cap
"""

from __future__ import annotations

from typing import Callable


def compute_composite_scores(
    signals_by_symbol: dict[str, dict],
    factor_scores: dict[str, dict[str, float | None]],
    factor_weights: dict[str, float],
    apply_gate: Callable[[float, float], dict],
) -> list[dict]:
    """Compute composite scores for all symbols and apply the MA200 gate.

    Symbols where every factor score is None (total_weight == 0) are dropped
    from the output — they cannot be ranked.

    When the sum of weights for available factors is less than 1.0 (partial
    data), the weighted sum is divided by that partial total so the score
    remains in the 0–100 range.  When all factors are present
    (total_weight >= 1.0), the weighted sum is used directly.

    Args:
        signals_by_symbol: Raw signal dicts keyed by symbol.  Each dict must
            contain a ``"technical"`` sub-dict with ``"price"`` and ``"ma200"``.
        factor_scores: Normalised 0–100 scores per factor per symbol.  Keyed
            as ``{factor_name: {symbol: score | None}}``.
        factor_weights: Contribution weight per factor (e.g. 0.2 for technical).
        apply_gate: Callable matching ``screener.metrics.ma200_gate.apply_gate``
            — returns ``{"above_ma200": bool, "multiplier": float}``.

    Returns:
        List of signal dicts enriched with ``composite_score`` and
        ``ma200_gate`` fields, one entry per scoreable symbol.
    """
    gated: list[dict] = []

    for sym, sig in signals_by_symbol.items():
        weighted_sum = 0.0
        total_weight = 0.0

        for factor, weight in factor_weights.items():
            score = factor_scores[factor].get(sym)
            if score is not None:
                weighted_sum += score * weight
                total_weight += weight

        if total_weight == 0.0:
            continue

        raw_composite = (
            weighted_sum / total_weight if total_weight < 1.0 else weighted_sum
        )

        gate = apply_gate(
            sig.get("technical", {}).get("price", 0) or 0,
            sig.get("technical", {}).get("ma200", 0) or 0,
        )
        composite_score = raw_composite * gate["multiplier"]
        gated.append({**sig, "composite_score": composite_score, "ma200_gate": gate})

    return gated


def apply_sector_cap(
    gated: list[dict],
    top_n: int,
    max_per_sector: int,
) -> list[dict]:
    """Select the top-N picks while enforcing a per-sector concentration cap.

    Args:
        gated: Scored entries from ``compute_composite_scores``.  Each dict
            must have ``composite_score`` and ``sector`` fields.
        top_n: Maximum total picks to return.
        max_per_sector: Maximum picks from any single GICS sector.

    Returns:
        Ranked list of up to ``top_n`` picks, each sector capped at
        ``max_per_sector``, ordered by ``composite_score`` descending.
    """
    sorted_entries = sorted(gated, key=lambda x: x["composite_score"], reverse=True)
    sector_counts: dict[str, int] = {}
    picks: list[dict] = []

    for entry in sorted_entries:
        sector = entry.get("sector", "Unknown")
        if sector_counts.get(sector, 0) >= max_per_sector:
            continue
        picks.append(entry)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(picks) >= top_n:
            break

    return picks
