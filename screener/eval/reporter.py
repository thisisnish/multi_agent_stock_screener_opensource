"""
screener/eval/reporter.py — Longitudinal eval trend reporting utilities.

Public API
----------
run_eval_trend_report(dao, n_months) -> dict
    Fetch the last N months of EvalTrendDoc records and return a summary dict
    describing confidence-gap trends over time.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from screener.lib.storage.schema import EVAL_TREND, eval_trend_doc_id

logger = logging.getLogger(__name__)


def _last_n_month_ids(n: int, now: datetime | None = None) -> list[str]:
    """Return a list of the last ``n`` month identifiers, newest last.

    Includes the current month and counts backwards.

    Args:
        n: Number of months to generate (>= 1).
        now: Datetime to use as "now"; defaults to ``datetime.now(UTC)``.

    Returns:
        List of ``"YYYY-MM"`` strings in ascending (chronological) order,
        e.g. ``["2025-06", "2025-07", ...]``.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    months: list[str] = []
    year, month = now.year, now.month
    for _ in range(n):
        months.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    # We appended newest-first; reverse to chronological order
    months.reverse()
    return months


async def run_eval_trend_report(dao, n_months: int = 12) -> dict:
    """Fetch the last N months of eval-trend docs and return a trend summary.

    Reads up to ``n_months`` documents from the ``eval_trend`` Firestore
    collection.  Documents for months where no eval has run yet will simply
    be absent (``None``) and are counted but excluded from averages.

    Args:
        dao: StorageDAO instance.
        n_months: Number of trailing months to query (including current).
                  Defaults to 12.

    Returns:
        Dict with the following keys:

        ``months_queried``
            Total number of month IDs that were attempted.

        ``months_with_data``
            Number of months where an EvalTrendDoc was found.

        ``avg_confidence_gap``
            Mean ``confidence_gap`` across all docs that have a non-None
            value, rounded to 1 decimal place; or ``None`` if no docs have
            the field populated.

        ``confidence_gap_trend``
            List of dicts sorted chronologically, one per queried month:
            ``{"period": str, "confidence_gap": float | None,
            "confidence_calibration": float | None}``.
    """
    month_ids = _last_n_month_ids(n_months)
    logger.info(
        "run_eval_trend_report: querying %d months (%s … %s)",
        len(month_ids),
        month_ids[0],
        month_ids[-1],
    )

    trend_entries: list[dict] = []
    months_with_data = 0

    for period in month_ids:
        doc = await dao.get(EVAL_TREND, eval_trend_doc_id(period))
        if doc:
            months_with_data += 1
        trend_entries.append(
            {
                "period": period,
                "confidence_gap": doc.get("confidence_gap") if doc else None,
                "confidence_calibration": (
                    doc.get("confidence_calibration") if doc else None
                ),
            }
        )

    gaps = [
        e["confidence_gap"] for e in trend_entries if e["confidence_gap"] is not None
    ]
    avg_confidence_gap: float | None = round(sum(gaps) / len(gaps), 1) if gaps else None

    return {
        "months_queried": len(month_ids),
        "months_with_data": months_with_data,
        "avg_confidence_gap": avg_confidence_gap,
        "confidence_gap_trend": trend_entries,
    }
