"""
screener/calibration/tracker.py — Rolling confidence calibration tracker.

Reads the last N months of PerformanceSnapshotDoc from Firestore, aggregates
per-tier metrics (High/Med/Low), checks if High > Med > Low order holds on
avg_alpha_pct, flags drift, and writes recommended weight adjustments when drift
is detected.

Public API
----------
run_calibration_tracking(dao, month_id, window_months=12, source="judge", dry_run=False) -> dict
run_calibration_trend_report(dao, n_months=12, source="judge") -> dict
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from screener.lib.storage.schema import (
    CALIBRATION,
    CALIBRATION_HISTORY,
    PERFORMANCE,
    CalibrationHistoryDoc,
    CalibrationReportDoc,
    WeightOverrideDoc,
    calibration_history_doc_id,
    calibration_report_doc_id,
    performance_doc_id,
    weight_override_doc_id,
)
from screener.metrics.confidence_scorer import _DEFAULT_WEIGHTS

logger = logging.getLogger(__name__)

# Minimum alpha gap (in percentage points) required between tiers to count as
# properly ordered.  Mirrors the spirit of _CALIBRATION_DRIFT_THRESHOLD in
# screener/eval/metrics.py but applied to the rolling cross-tier comparison.
_REQUIRED_ALPHA_GAP: float = 2.0

# Minimum months of closed-pick data needed before we emit any drift signal.
_MIN_MONTHS_FOR_CALIBRATION: int = 3


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _prior_month_ids(month_id: str, n: int) -> list[str]:
    """Return the last ``n`` month IDs ending just before ``month_id``.

    Args:
        month_id: Reference month in ``"YYYY-MM"`` format (excluded from result).
        n: Number of prior months to return.

    Returns:
        List of ``"YYYY-MM"`` strings in ascending chronological order.

    Example::

        _prior_month_ids("2026-05", 3) -> ["2026-02", "2026-03", "2026-04"]
        _prior_month_ids("2026-01", 3) -> ["2025-10", "2025-11", "2025-12"]
    """
    year, month = int(month_id[:4]), int(month_id[5:7])
    months: list[str] = []
    for i in range(n, 0, -1):
        total_months = year * 12 + month - 1 - i
        y = total_months // 12
        m = total_months % 12 + 1
        months.append(f"{y:04d}-{m:02d}")
    return months


async def _fetch_snapshots(dao, month_ids: list[str], source: str) -> list[dict]:
    """Fetch PerformanceSnapshotDocs for the given month IDs.

    Skips months where the document does not exist or where all tier alpha
    metrics are None (no closed picks for that month).

    Args:
        dao: StorageDAO instance.
        month_ids: List of ``"YYYY-MM"`` strings to fetch.
        source: Agent source label (e.g. ``"judge"``).

    Returns:
        List of raw snapshot dicts that have at least one non-None tier metric.
    """
    results: list[dict] = []
    for mid in month_ids:
        doc = await dao.get(PERFORMANCE, performance_doc_id(mid, source))
        if doc is None:
            logger.debug("no performance snapshot for month=%s source=%s", mid, source)
            continue
        tier_fields = (
            doc.get("high_avg_alpha_pct"),
            doc.get("med_avg_alpha_pct"),
            doc.get("low_avg_alpha_pct"),
            doc.get("high_avg_return_pct"),
            doc.get("med_avg_return_pct"),
            doc.get("low_avg_return_pct"),
        )
        if all(v is None for v in tier_fields):
            logger.debug("skipping snapshot month=%s — no closed pick tier data", mid)
            continue
        results.append(doc)
    return results


def _aggregate_tiers(snapshots: list[dict]) -> dict:
    """Average per-tier metrics across a list of PerformanceSnapshotDoc dicts.

    Args:
        snapshots: List of raw snapshot dicts (output of ``_fetch_snapshots``).

    Returns:
        Dict with averaged tier fields and a ``months_included`` key listing
        the month IDs of snapshots that contributed data.  Fields with no
        non-None values across snapshots are omitted from the result.
    """
    if not snapshots:
        return {}

    fields = [
        "high_avg_alpha_pct",
        "med_avg_alpha_pct",
        "low_avg_alpha_pct",
        "high_avg_return_pct",
        "med_avg_return_pct",
        "low_avg_return_pct",
        "high_win_rate",
        "med_win_rate",
        "low_win_rate",
    ]

    accumulators: dict[str, list[float]] = {f: [] for f in fields}
    for snap in snapshots:
        for field in fields:
            val = snap.get(field)
            if val is not None:
                accumulators[field].append(val)

    result: dict = {}
    for field, values in accumulators.items():
        if values:
            result[field] = sum(values) / len(values)

    months_included = [snap["month_id"] for snap in snapshots if snap.get("month_id")]
    result["months_included"] = months_included
    return result


def _check_calibration(agg: dict) -> tuple[bool, list[str]]:
    """Check whether High > Med > Low ordering holds on avg_alpha_pct.

    Requires a gap of at least ``_REQUIRED_ALPHA_GAP`` percentage points between
    adjacent tiers to count as properly ordered.  The return_pct ordering is
    checked as a secondary signal and included in flags when it also violates
    the expected order.

    If any tier's alpha data is missing, returns ``(True, [])`` — insufficient
    data is not treated as drift to avoid false positives.

    Args:
        agg: Output of ``_aggregate_tiers``.

    Returns:
        ``(calibration_ok, drift_flags)`` tuple.
    """
    high_alpha = agg.get("high_avg_alpha_pct")
    med_alpha = agg.get("med_avg_alpha_pct")
    low_alpha = agg.get("low_avg_alpha_pct")

    if high_alpha is None or med_alpha is None or low_alpha is None:
        return True, []

    flags: list[str] = []

    if high_alpha - med_alpha < _REQUIRED_ALPHA_GAP:
        flags.append(
            f"High avg_alpha ({high_alpha:.2f}%) not sufficiently above "
            f"Med avg_alpha ({med_alpha:.2f}%) — gap={high_alpha - med_alpha:.2f}pp "
            f"(required {_REQUIRED_ALPHA_GAP}pp)"
        )

    if med_alpha - low_alpha < _REQUIRED_ALPHA_GAP:
        flags.append(
            f"Med avg_alpha ({med_alpha:.2f}%) not sufficiently above "
            f"Low avg_alpha ({low_alpha:.2f}%) — gap={med_alpha - low_alpha:.2f}pp "
            f"(required {_REQUIRED_ALPHA_GAP}pp)"
        )

    high_ret = agg.get("high_avg_return_pct")
    med_ret = agg.get("med_avg_return_pct")
    low_ret = agg.get("low_avg_return_pct")

    if high_ret is not None and med_ret is not None:
        if high_ret - med_ret < _REQUIRED_ALPHA_GAP:
            flags.append(
                f"High avg_return ({high_ret:.2f}%) not sufficiently above "
                f"Med avg_return ({med_ret:.2f}%) — secondary signal"
            )

    if med_ret is not None and low_ret is not None:
        if med_ret - low_ret < _REQUIRED_ALPHA_GAP:
            flags.append(
                f"Med avg_return ({med_ret:.2f}%) not sufficiently above "
                f"Low avg_return ({low_ret:.2f}%) — secondary signal"
            )

    calibration_ok = len(flags) == 0
    return calibration_ok, flags


def _compute_weight_adjustments(drift_flags: list[str]) -> dict | None:
    """Compute nudged confidence weights in response to calibration drift.

    Each detected drift flag (High <= Med or Med <= Low on alpha) shifts
    W1_margin down by 0.05 and W2_unique_sources up by 0.05, reflecting that
    the margin-of-victory signal is overweighted relative to source diversity.

    All weights are clamped to [0.10, 0.70] after nudging, then re-normalised
    to sum to 1.0.

    Args:
        drift_flags: Non-empty list of flag strings from ``_check_calibration``.

    Returns:
        Dict with ``W1_margin``, ``W2_unique_sources``, ``W3_hedge``, and
        ``reason`` keys, or ``None`` if ``drift_flags`` is empty.
    """
    if not drift_flags:
        return None

    alpha_flags = [f for f in drift_flags if "avg_alpha" in f and "secondary" not in f]
    nudge_count = len(alpha_flags)

    w1_raw = _DEFAULT_WEIGHTS["W1_margin"] - nudge_count * 0.05
    w2_raw = _DEFAULT_WEIGHTS["W2_unique_sources"] + nudge_count * 0.05
    w3_raw = _DEFAULT_WEIGHTS["W3_hedge"]

    # Clamp each weight to [0.10, 0.70] first.
    w1 = max(0.10, min(0.70, w1_raw))
    w2 = max(0.10, min(0.70, w2_raw))
    w3 = max(0.10, min(0.70, w3_raw))

    # Re-normalise so weights sum to 1.0 without violating the clamp bounds.
    # Strategy: any weight that was pushed to its clamp limit is treated as
    # fixed; remaining slack is distributed to the unclamped weights.  This
    # guarantees the clamp invariant survives normalisation.
    total = w1 + w2 + w3
    if abs(total - 1.0) > 1e-9:
        # Identify clamped weights (those whose raw value was modified).
        clamped = {
            "w1": w1 != w1_raw,
            "w2": w2 != w2_raw,
            "w3": w3 != w3_raw,
        }
        fixed_sum = sum(
            v
            for v, is_clamped in [
                (w1, clamped["w1"]),
                (w2, clamped["w2"]),
                (w3, clamped["w3"]),
            ]
            if is_clamped
        )
        remaining = 1.0 - fixed_sum
        free_raw = [
            (k, raw)
            for k, raw, is_clamped in [
                ("w1", w1_raw, clamped["w1"]),
                ("w2", w2_raw, clamped["w2"]),
                ("w3", w3_raw, clamped["w3"]),
            ]
            if not is_clamped
        ]
        free_sum = sum(r for _, r in free_raw)
        if free_sum > 0 and free_raw:
            for key, raw in free_raw:
                scaled = max(0.10, min(0.70, remaining * raw / free_sum))
                if key == "w1":
                    w1 = scaled
                elif key == "w2":
                    w2 = scaled
                else:
                    w3 = scaled
        else:
            # All weights are clamped; distribute residual to W3 (least sensitive).
            w3 = round(1.0 - w1 - w2, 9)

    w1 = round(w1, 6)
    w2 = round(w2, 6)
    w3 = round(1.0 - w1 - w2, 6)

    reason = (
        f"Calibration drift detected ({nudge_count} alpha flag(s)): "
        f"W1_margin nudged down {nudge_count * 0.05:.2f}, "
        f"W2_unique_sources nudged up {nudge_count * 0.05:.2f}"
    )

    return {
        "W1_margin": w1,
        "W2_unique_sources": w2,
        "W3_hedge": w3,
        "reason": reason,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def run_calibration_tracking(
    dao,
    month_id: str,
    window_months: int = 12,
    source: str = "judge",
    dry_run: bool = False,
) -> dict:
    """Run rolling calibration tracking for the given month and window.

    Steps:
      1. Compute prior month IDs for the rolling window.
      2. Fetch PerformanceSnapshotDocs for each month.
      3. Return early if fewer than ``_MIN_MONTHS_FOR_CALIBRATION`` months have
         closed-pick tier data.
      4. Aggregate per-tier metrics across fetched snapshots.
      5. Check High > Med > Low calibration ordering.
      6. Write CalibrationReportDoc to Firestore (unless dry_run).
      7. If calibration is not OK: compute and write WeightOverrideDoc.

    Args:
        dao: StorageDAO instance.
        month_id: Current month in ``"YYYY-MM"`` format (excluded from window).
        window_months: Number of prior months to include in the rolling window.
        source: Agent source label (e.g. ``"judge"``).
        dry_run: If True, skip all Firestore writes.

    Returns:
        Result dict with keys: ``status``, ``calibration_ok``, ``drift_flags``,
        ``months_with_data``.  On insufficient data: ``status="insufficient_data"``.
    """
    prior_ids = _prior_month_ids(month_id, window_months)
    snapshots = await _fetch_snapshots(dao, prior_ids, source)

    months_with_data = len(snapshots)
    if months_with_data < _MIN_MONTHS_FOR_CALIBRATION:
        logger.info(
            "calibration tracking: insufficient data (%d months, need %d)",
            months_with_data,
            _MIN_MONTHS_FOR_CALIBRATION,
        )
        return {
            "status": "insufficient_data",
            "months_with_data": months_with_data,
        }

    agg = _aggregate_tiers(snapshots)
    calibration_ok, drift_flags = _check_calibration(agg)

    report_doc = CalibrationReportDoc(
        window_months=window_months,
        source=source,
        months_included=agg.get("months_included", []),
        high_avg_alpha_pct=agg.get("high_avg_alpha_pct"),
        med_avg_alpha_pct=agg.get("med_avg_alpha_pct"),
        low_avg_alpha_pct=agg.get("low_avg_alpha_pct"),
        high_avg_return_pct=agg.get("high_avg_return_pct"),
        med_avg_return_pct=agg.get("med_avg_return_pct"),
        low_avg_return_pct=agg.get("low_avg_return_pct"),
        high_win_rate=agg.get("high_win_rate"),
        med_win_rate=agg.get("med_win_rate"),
        low_win_rate=agg.get("low_win_rate"),
        calibration_ok=calibration_ok,
        drift_flags=drift_flags,
    )

    if not dry_run:
        await dao.set(
            CALIBRATION,
            calibration_report_doc_id(window_months, source),
            report_doc.model_dump(mode="json"),
        )
        logger.info(
            "wrote calibration report doc — window=%dm source=%s ok=%s flags=%d",
            window_months,
            source,
            calibration_ok,
            len(drift_flags),
        )

    # Read existing override doc to get before-weights for history tracking.
    existing_override = await dao.get(CALIBRATION, weight_override_doc_id(source))
    if existing_override is not None:
        w1_before = existing_override["W1_margin"]
        w2_before = existing_override["W2_unique_sources"]
        w3_before = existing_override["W3_hedge"]
    else:
        w1_before = _DEFAULT_WEIGHTS["W1_margin"]
        w2_before = _DEFAULT_WEIGHTS["W2_unique_sources"]
        w3_before = _DEFAULT_WEIGHTS["W3_hedge"]

    weight_result: dict = {}
    w1_after = w1_before
    w2_after = w2_before
    w3_after = w3_before

    if not calibration_ok:
        adjustments = _compute_weight_adjustments(drift_flags)
        if adjustments:
            w1_after = adjustments["W1_margin"]
            w2_after = adjustments["W2_unique_sources"]
            w3_after = adjustments["W3_hedge"]
            override_doc = WeightOverrideDoc(
                source=source,
                W1_margin=w1_after,
                W2_unique_sources=w2_after,
                W3_hedge=w3_after,
                reason=adjustments["reason"],
            )
            if not dry_run:
                await dao.set(
                    CALIBRATION,
                    weight_override_doc_id(source),
                    override_doc.model_dump(mode="json"),
                )
                logger.info(
                    "wrote weight override doc — W1=%.4f W2=%.4f W3=%.4f",
                    w1_after,
                    w2_after,
                    w3_after,
                )
            weight_result = {
                "weight_override_written": not dry_run,
                "W1_margin": w1_after,
                "W2_unique_sources": w2_after,
                "W3_hedge": w3_after,
            }

    delta_magnitude = (
        abs(w1_after - w1_before)
        + abs(w2_after - w2_before)
        + abs(w3_after - w3_before)
    )
    history_doc = CalibrationHistoryDoc(
        month_id=month_id,
        source=source,
        W1_before=w1_before,
        W1_after=w1_after,
        W2_before=w2_before,
        W2_after=w2_after,
        W3_before=w3_before,
        W3_after=w3_after,
        delta_magnitude=delta_magnitude,
        drift_flags_count=len(drift_flags),
        calibration_ok=calibration_ok,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
    if not dry_run:
        await dao.set(
            CALIBRATION_HISTORY,
            calibration_history_doc_id(month_id, source),
            history_doc.model_dump(mode="json"),
        )
        logger.info(
            "wrote calibration history doc — month=%s delta=%.4f ok=%s",
            month_id,
            delta_magnitude,
            calibration_ok,
        )

    return {
        "status": "success",
        "calibration_ok": calibration_ok,
        "drift_flags": drift_flags,
        "months_with_data": months_with_data,
        "history_doc_written": not dry_run,
        **weight_result,
    }


async def run_calibration_trend_report(
    dao,
    n_months: int = 12,
    source: str = "judge",
) -> dict:
    """Generate a trend report from the last ``n_months`` of calibration history docs.

    Queries ``calibration_history`` for the last ``n_months`` including the
    current month and summarises convergence / oscillation signals.

    Args:
        dao: StorageDAO instance.
        n_months: Number of months to include, counting back from the current month.
        source: Agent source label (e.g. ``"judge"``).

    Returns:
        Dict with keys: ``months_queried``, ``months_with_data``,
        ``calibration_ok_count``, ``calibration_ok_rate``, ``avg_drift_flags``,
        ``weight_delta_trend``.
    """
    now = datetime.now(timezone.utc)
    current_year = now.year
    current_month = now.month

    # Build month IDs for the last n_months INCLUDING the current month.
    month_ids: list[str] = []
    for i in range(n_months - 1, -1, -1):
        total = current_year * 12 + current_month - 1 - i
        y = total // 12
        m = total % 12 + 1
        month_ids.append(f"{y:04d}-{m:02d}")

    docs: list[dict] = []
    for mid in month_ids:
        doc = await dao.get(
            CALIBRATION_HISTORY, calibration_history_doc_id(mid, source)
        )
        if doc is not None:
            docs.append(doc)

    months_with_data = len(docs)
    calibration_ok_count = sum(1 for d in docs if d.get("calibration_ok") is True)

    calibration_ok_rate: float | None = None
    if months_with_data > 0:
        calibration_ok_rate = calibration_ok_count / months_with_data

    avg_drift_flags: float | None = None
    if months_with_data > 0:
        avg_drift_flags = (
            sum(d.get("drift_flags_count", 0) for d in docs) / months_with_data
        )

    weight_delta_trend = sorted(
        [
            {
                "month_id": d["month_id"],
                "delta_magnitude": d.get("delta_magnitude", 0.0),
                "calibration_ok": d.get("calibration_ok", True),
            }
            for d in docs
        ],
        key=lambda x: x["month_id"],
    )

    return {
        "months_queried": len(month_ids),
        "months_with_data": months_with_data,
        "calibration_ok_count": calibration_ok_count,
        "calibration_ok_rate": calibration_ok_rate,
        "avg_drift_flags": avg_drift_flags,
        "weight_delta_trend": weight_delta_trend,
    }
