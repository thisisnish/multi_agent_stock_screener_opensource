"""
screener/scoring/engine.py — Composite scoring engine.

Two pure functions (no I/O, no config loading):

    compute_composite_scores() — merges 4 weighted signals, applies MA200 gate.
    select_top_n()             — sorts by composite_post_gate, enforces sector cap,
                                 returns top-N entries with rank field.
"""

from __future__ import annotations

from screener.lib.config_loader import SignalWeightsConfig
from screener.metrics.ma200_gate import apply_gate

# Score imputed for any missing or unavailable signal — sector-neutral midpoint.
_IMPUTED_SCORE = 50.0


def compute_composite_scores(
    scored: list[dict],
    ey_scores: dict[str, float | None],
    fcf_scores: dict[str, float | None],
    ebitda_scores: dict[str, float | None],
    weights: SignalWeightsConfig,
    *,
    ey_available: bool = True,
    fcf_available: bool = True,
    ebitda_available: bool = True,
) -> list[dict]:
    """Merge four signals into composite_pre_gate, apply MA200 gate → composite_post_gate.

    Mutates each entry in ``scored`` in place (adds keys) and returns the list.

    Missing signal score (key absent or value is None) is imputed as 50.0 —
    sector-neutral, not penalised.  When an entire signal stream is marked
    unavailable (``ey_available=False`` etc.) every ticker is imputed regardless
    of what ``ey_scores`` contains.

    Args:
        scored:         List of dicts from ``compute_score()``; must contain
                        ``symbol``, ``score``, ``price``, ``ma200`` keys.
        ey_scores:      Per-symbol earnings-yield z-scores (0–100).
        fcf_scores:     Per-symbol FCF-yield z-scores (0–100).
        ebitda_scores:  Per-symbol EBITDA/EV z-scores (0–100).
        weights:        ``SignalWeightsConfig`` holding the four factor weights.
        ey_available:   False → impute all earnings-yield scores as 50.0.
        fcf_available:  False → impute all FCF-yield scores as 50.0.
        ebitda_available: False → impute all EBITDA/EV scores as 50.0.

    Returns:
        The mutated ``scored`` list (same object).
    """
    for entry in scored:
        sym: str = entry["symbol"]
        tech_score: float = entry["score"]

        # Resolve each value-signal score, imputing if unavailable or missing.
        raw_ey = ey_scores.get(sym) if ey_available else None
        ey_imputed = raw_ey is None
        ey_used = _IMPUTED_SCORE if ey_imputed else raw_ey  # type: ignore[assignment]

        raw_fcf = fcf_scores.get(sym) if fcf_available else None
        fcf_imputed = raw_fcf is None
        fcf_used = _IMPUTED_SCORE if fcf_imputed else raw_fcf  # type: ignore[assignment]

        raw_ebitda = ebitda_scores.get(sym) if ebitda_available else None
        ebitda_imputed = raw_ebitda is None
        ebitda_used = _IMPUTED_SCORE if ebitda_imputed else raw_ebitda  # type: ignore[assignment]

        composite_pre_gate = round(
            tech_score * weights.technical
            + ey_used * weights.earnings
            + fcf_used * weights.fcf
            + ebitda_used * weights.ebitda,
            2,
        )

        gate = apply_gate(entry["price"], entry["ma200"])
        composite_post_gate = round(composite_pre_gate * gate["multiplier"], 2)

        # Persist raw per-signal scores (None means signal was not available).
        entry["technical_score"] = tech_score
        entry["earnings_yield_score"] = ey_scores.get(sym) if ey_available else None
        entry["fcf_yield_score"] = fcf_scores.get(sym) if fcf_available else None
        entry["ebitda_ev_score"] = ebitda_scores.get(sym) if ebitda_available else None

        entry["composite_pre_gate"] = composite_pre_gate
        entry["composite_post_gate"] = composite_post_gate
        entry["score"] = composite_post_gate  # overwrite with final score
        entry["ma200_gate"] = gate

        entry["factor_scores"] = {
            "technical": {
                "score": tech_score,
                "weight": weights.technical,
                "imputed": False,
            },
            "earnings_yield": {
                "score": ey_used,
                "weight": weights.earnings,
                "imputed": ey_imputed,
            },
            "fcf_yield": {
                "score": fcf_used,
                "weight": weights.fcf,
                "imputed": fcf_imputed,
            },
            "ebitda_ev": {
                "score": ebitda_used,
                "weight": weights.ebitda,
                "imputed": ebitda_imputed,
            },
        }

    return scored


def select_top_n(
    scored: list[dict],
    top_n: int,
    sector_cap: int,
) -> list[dict]:
    """Sort by composite_post_gate descending, enforce sector cap, return top-N.

    Iterates through entries in descending score order.  Entries are skipped
    once their sector has reached ``sector_cap`` picks.  Iteration stops as
    soon as the result list reaches ``top_n``.  Each entry in the result
    receives a ``rank`` key (1-indexed).

    Args:
        scored:     List of dicts; must contain ``composite_post_gate`` and
                    ``sector`` keys.
        top_n:      Maximum number of entries to return.
        sector_cap: Maximum picks allowed per GICS sector.

    Returns:
        New list of up to ``top_n`` entries with ``rank`` assigned.
    """
    sorted_entries = sorted(
        scored, key=lambda e: e["composite_post_gate"], reverse=True
    )

    sector_counts: dict[str, int] = {}
    result: list[dict] = []

    for entry in sorted_entries:
        if len(result) >= top_n:
            break

        sector: str = entry.get("sector", "Unknown")
        count = sector_counts.get(sector, 0)

        if count >= sector_cap:
            continue

        sector_counts[sector] = count + 1
        result.append(entry)

    for rank, entry in enumerate(result, start=1):
        entry["rank"] = rank

    return result
