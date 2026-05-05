"""
screener/lib/normalizer.py — Sector Z-score normalizer (0–100).

Formula:
    z     = (value - sector_mean) / sector_std
    score = clamp(z * 15 + 50, 0, 100)

Guards:
    - Sector std = 0 → return 50.0 (neutral) for all tickers in that sector
    - Sector < MIN_SECTOR_SIZE valid tickers → return 50.0 (neutral) for all
    - Missing / None value → return None (sector-neutral imputation handled by caller)
"""

from __future__ import annotations

import logging
import statistics

logger = logging.getLogger(__name__)

MIN_SECTOR_SIZE = 2


def sector_z_scores(
    signals: dict[str, dict],
    value_key: str,
    sector_map: dict[str, str],
) -> dict[str, float | None]:
    """
    Compute sector-normalised 0–100 scores for a single factor.

    Parameters
    ----------
    signals : dict[str, dict]
        Output from a factor fetcher, keyed by symbol.
        Each value must have `value_key` and `"skipped"` fields.
    value_key : str
        Key in each signal dict holding the raw float (e.g. "earnings_yield").
    sector_map : dict[str, str]
        Maps symbol → GICS sector string.

    Returns
    -------
    dict[str, float | None]
        Maps symbol → normalised score 0–100, or None if skipped / no data.
    """
    sector_values: dict[str, list[tuple[str, float]]] = {}

    for symbol, data in signals.items():
        if data.get("skipped") or data.get(value_key) is None:
            continue
        sector = sector_map.get(symbol)
        if not sector:
            continue
        val = float(data[value_key])
        sector_values.setdefault(sector, []).append((symbol, val))

    sector_stats: dict[str, tuple[float, float]] = {}
    small_sectors: set[str] = set()

    for sector, entries in sector_values.items():
        if len(entries) < MIN_SECTOR_SIZE:
            logger.warning(
                "sector below min size — returning neutral",
                extra={"sector": sector, "size": len(entries), "min": MIN_SECTOR_SIZE},
            )
            small_sectors.add(sector)
            continue
        values = [v for _, v in entries]
        mean = statistics.mean(values)
        std = statistics.pstdev(values)
        sector_stats[sector] = (mean, std)

    scores: dict[str, float] = {}

    for symbol, data in signals.items():
        sector = sector_map.get(symbol)

        if data.get("skipped") or data.get(value_key) is None:
            scores[symbol] = None
            continue

        if sector in small_sectors:
            scores[symbol] = 50.0
            continue

        if sector not in sector_stats:
            scores[symbol] = None
            continue

        mean, std = sector_stats[sector]

        if std == 0.0:
            logger.info(
                "sector std=0 — all values identical, returning neutral",
                extra={"sector": sector, "factor": value_key},
            )
            scores[symbol] = 50.0
            continue

        z = (float(data[value_key]) - mean) / std
        scores[symbol] = float(_clamp(z * 15.0 + 50.0, 0.0, 100.0))

    return scores


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))
