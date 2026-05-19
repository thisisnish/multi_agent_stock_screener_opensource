"""
tests/calibration/test_tracker.py — Unit tests for screener/calibration/tracker.py.

Covers:
- _prior_month_ids: basic case and year-boundary wrap
- _aggregate_tiers: empty input, single snapshot, multi-snapshot averaging
- _check_calibration: calibrated (gap >= 2pp), uncalibrated (gap < 2pp), missing data
- _compute_weight_adjustments: weight nudge, normalisation, no flags returns None
- run_calibration_tracking: insufficient data path, success path with mock dao

No real Firestore calls are made — dao is fully mocked with AsyncMock.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# ---------------------------------------------------------------------------
# _prior_month_ids
# ---------------------------------------------------------------------------


class TestPriorMonthIds:
    def test_basic_three_months(self):
        from screener.calibration.tracker import _prior_month_ids

        result = _prior_month_ids("2026-05", 3)
        assert result == ["2026-02", "2026-03", "2026-04"]

    def test_year_boundary_wrap(self):
        from screener.calibration.tracker import _prior_month_ids

        result = _prior_month_ids("2026-01", 3)
        assert result == ["2025-10", "2025-11", "2025-12"]

    def test_single_month(self):
        from screener.calibration.tracker import _prior_month_ids

        result = _prior_month_ids("2026-03", 1)
        assert result == ["2026-02"]

    def test_twelve_months_count(self):
        from screener.calibration.tracker import _prior_month_ids

        result = _prior_month_ids("2026-05", 12)
        assert len(result) == 12

    def test_twelve_months_last_entry(self):
        from screener.calibration.tracker import _prior_month_ids

        result = _prior_month_ids("2026-05", 12)
        assert result[-1] == "2026-04"

    def test_twelve_months_first_entry(self):
        from screener.calibration.tracker import _prior_month_ids

        result = _prior_month_ids("2026-05", 12)
        assert result[0] == "2025-05"

    def test_excludes_current_month(self):
        from screener.calibration.tracker import _prior_month_ids

        result = _prior_month_ids("2026-05", 3)
        assert "2026-05" not in result

    def test_order_is_ascending(self):
        from screener.calibration.tracker import _prior_month_ids

        result = _prior_month_ids("2026-06", 4)
        assert result == sorted(result)


# ---------------------------------------------------------------------------
# _aggregate_tiers
# ---------------------------------------------------------------------------


class TestAggregateTiers:
    def test_empty_input_returns_empty_dict(self):
        from screener.calibration.tracker import _aggregate_tiers

        result = _aggregate_tiers([])
        assert result == {}

    def test_single_snapshot_passes_through(self):
        from screener.calibration.tracker import _aggregate_tiers

        snap = {
            "month_id": "2026-04",
            "high_avg_alpha_pct": 5.0,
            "med_avg_alpha_pct": 2.0,
            "low_avg_alpha_pct": -1.0,
            "high_avg_return_pct": 8.0,
            "med_avg_return_pct": 4.0,
            "low_avg_return_pct": 1.0,
        }
        result = _aggregate_tiers([snap])
        assert result["high_avg_alpha_pct"] == pytest.approx(5.0)
        assert result["med_avg_alpha_pct"] == pytest.approx(2.0)
        assert result["low_avg_alpha_pct"] == pytest.approx(-1.0)

    def test_two_snapshots_averaged(self):
        from screener.calibration.tracker import _aggregate_tiers

        snaps = [
            {"month_id": "2026-03", "high_avg_alpha_pct": 4.0},
            {"month_id": "2026-04", "high_avg_alpha_pct": 8.0},
        ]
        result = _aggregate_tiers(snaps)
        assert result["high_avg_alpha_pct"] == pytest.approx(6.0)

    def test_none_values_excluded_from_average(self):
        from screener.calibration.tracker import _aggregate_tiers

        snaps = [
            {
                "month_id": "2026-03",
                "high_avg_alpha_pct": 6.0,
                "med_avg_alpha_pct": None,
            },
            {
                "month_id": "2026-04",
                "high_avg_alpha_pct": 4.0,
                "med_avg_alpha_pct": 2.0,
            },
        ]
        result = _aggregate_tiers(snaps)
        assert result["high_avg_alpha_pct"] == pytest.approx(5.0)
        assert result["med_avg_alpha_pct"] == pytest.approx(2.0)

    def test_months_included_populated(self):
        from screener.calibration.tracker import _aggregate_tiers

        snaps = [
            {"month_id": "2026-03", "high_avg_alpha_pct": 5.0},
            {"month_id": "2026-04", "high_avg_alpha_pct": 3.0},
        ]
        result = _aggregate_tiers(snaps)
        assert "2026-03" in result["months_included"]
        assert "2026-04" in result["months_included"]

    def test_field_absent_when_all_none(self):
        from screener.calibration.tracker import _aggregate_tiers

        snaps = [
            {
                "month_id": "2026-04",
                "high_avg_alpha_pct": None,
                "med_avg_alpha_pct": None,
            },
        ]
        result = _aggregate_tiers(snaps)
        assert "high_avg_alpha_pct" not in result
        assert "med_avg_alpha_pct" not in result


# ---------------------------------------------------------------------------
# _check_calibration
# ---------------------------------------------------------------------------


class TestCheckCalibration:
    def test_calibrated_high_above_med_above_low(self):
        from screener.calibration.tracker import _check_calibration

        agg = {
            "high_avg_alpha_pct": 8.0,
            "med_avg_alpha_pct": 5.0,
            "low_avg_alpha_pct": 2.0,
        }
        ok, flags = _check_calibration(agg)
        assert ok is True
        assert flags == []

    def test_flagged_when_high_not_above_med_by_gap(self):
        from screener.calibration.tracker import _check_calibration

        agg = {
            "high_avg_alpha_pct": 5.0,
            "med_avg_alpha_pct": 4.5,
            "low_avg_alpha_pct": 1.0,
        }
        ok, flags = _check_calibration(agg)
        assert ok is False
        assert len(flags) >= 1
        assert any("High" in f and "Med" in f for f in flags)

    def test_flagged_when_med_not_above_low_by_gap(self):
        from screener.calibration.tracker import _check_calibration

        agg = {
            "high_avg_alpha_pct": 8.0,
            "med_avg_alpha_pct": 3.0,
            "low_avg_alpha_pct": 2.5,
        }
        ok, flags = _check_calibration(agg)
        assert ok is False
        assert any("Med" in f and "Low" in f for f in flags)

    def test_missing_high_alpha_returns_ok_no_flags(self):
        from screener.calibration.tracker import _check_calibration

        agg = {
            "med_avg_alpha_pct": 3.0,
            "low_avg_alpha_pct": 1.0,
        }
        ok, flags = _check_calibration(agg)
        assert ok is True
        assert flags == []

    def test_missing_med_alpha_returns_ok_no_flags(self):
        from screener.calibration.tracker import _check_calibration

        agg = {
            "high_avg_alpha_pct": 8.0,
            "low_avg_alpha_pct": 1.0,
        }
        ok, flags = _check_calibration(agg)
        assert ok is True
        assert flags == []

    def test_missing_low_alpha_returns_ok_no_flags(self):
        from screener.calibration.tracker import _check_calibration

        agg = {
            "high_avg_alpha_pct": 8.0,
            "med_avg_alpha_pct": 4.0,
        }
        ok, flags = _check_calibration(agg)
        assert ok is True
        assert flags == []

    def test_exact_two_pp_gap_is_calibrated(self):
        from screener.calibration.tracker import _check_calibration

        agg = {
            "high_avg_alpha_pct": 7.0,
            "med_avg_alpha_pct": 5.0,
            "low_avg_alpha_pct": 3.0,
        }
        ok, flags = _check_calibration(agg)
        assert ok is True
        assert flags == []

    def test_gap_just_below_threshold_flagged(self):
        from screener.calibration.tracker import _check_calibration

        agg = {
            "high_avg_alpha_pct": 6.9,
            "med_avg_alpha_pct": 5.0,
            "low_avg_alpha_pct": 3.0,
        }
        ok, flags = _check_calibration(agg)
        assert ok is False

    def test_empty_agg_returns_ok_no_flags(self):
        from screener.calibration.tracker import _check_calibration

        ok, flags = _check_calibration({})
        assert ok is True
        assert flags == []


# ---------------------------------------------------------------------------
# _compute_weight_adjustments
# ---------------------------------------------------------------------------


class TestComputeWeightAdjustments:
    def test_no_flags_returns_none(self):
        from screener.calibration.tracker import _compute_weight_adjustments

        assert _compute_weight_adjustments([]) is None

    def test_weights_sum_to_one(self):
        from screener.calibration.tracker import _compute_weight_adjustments

        result = _compute_weight_adjustments(
            ["High avg_alpha (5.00%) not sufficiently above Med avg_alpha"]
        )
        assert result is not None
        total = result["W1_margin"] + result["W2_unique_sources"] + result["W3_hedge"]
        assert total == pytest.approx(1.0, abs=1e-6)

    def test_w1_nudged_down(self):
        from screener.calibration.tracker import _compute_weight_adjustments
        from screener.metrics.confidence_scorer import _DEFAULT_WEIGHTS

        result = _compute_weight_adjustments(
            ["High avg_alpha (5.00%) not sufficiently above Med avg_alpha"]
        )
        assert result is not None
        assert result["W1_margin"] < _DEFAULT_WEIGHTS["W1_margin"]

    def test_w2_nudged_up(self):
        from screener.calibration.tracker import _compute_weight_adjustments
        from screener.metrics.confidence_scorer import _DEFAULT_WEIGHTS

        result = _compute_weight_adjustments(
            ["High avg_alpha (5.00%) not sufficiently above Med avg_alpha"]
        )
        assert result is not None
        assert result["W2_unique_sources"] > _DEFAULT_WEIGHTS["W2_unique_sources"]

    def test_reason_field_present(self):
        from screener.calibration.tracker import _compute_weight_adjustments

        result = _compute_weight_adjustments(["High avg_alpha drift"])
        assert result is not None
        assert "reason" in result
        assert isinstance(result["reason"], str)
        assert len(result["reason"]) > 0

    def test_two_alpha_flags_double_nudge(self):
        from screener.calibration.tracker import _compute_weight_adjustments
        from screener.metrics.confidence_scorer import _DEFAULT_WEIGHTS

        flags = [
            "High avg_alpha (5.00%) not sufficiently above Med avg_alpha (4.50%)",
            "Med avg_alpha (4.50%) not sufficiently above Low avg_alpha (4.00%)",
        ]
        result = _compute_weight_adjustments(flags)
        assert result is not None
        assert result["W1_margin"] < _DEFAULT_WEIGHTS["W1_margin"]

    def test_weights_clamped_to_min(self):
        from screener.calibration.tracker import _compute_weight_adjustments

        many_flags = [f"High avg_alpha drift flag {i}" for i in range(10)]
        result = _compute_weight_adjustments(many_flags)
        assert result is not None
        assert result["W1_margin"] >= 0.10
        assert result["W2_unique_sources"] <= 0.70


# ---------------------------------------------------------------------------
# run_calibration_tracking
# ---------------------------------------------------------------------------


def _make_mock_dao(get_return_values: dict | None = None) -> MagicMock:
    """Build a mock DAO where get() returns values from a lookup dict keyed by doc_id."""
    dao = MagicMock()
    dao.set = AsyncMock(return_value=None)

    async def _get(collection, doc_id):
        if get_return_values is None:
            return None
        return get_return_values.get(doc_id)

    dao.get = _get
    return dao


def _make_snapshot(
    month_id: str, high_alpha: float, med_alpha: float, low_alpha: float
) -> dict:
    return {
        "month_id": month_id,
        "high_avg_alpha_pct": high_alpha,
        "med_avg_alpha_pct": med_alpha,
        "low_avg_alpha_pct": low_alpha,
        "high_avg_return_pct": high_alpha + 1.0,
        "med_avg_return_pct": med_alpha + 1.0,
        "low_avg_return_pct": low_alpha + 1.0,
        "high_win_rate": 0.7,
        "med_win_rate": 0.6,
        "low_win_rate": 0.5,
    }


class TestRunCalibrationTracking:
    def test_insufficient_data_two_snapshots(self):
        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import performance_doc_id

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 6.0, 3.0, 0.5
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 5.5, 2.5, 0.2
            ),
        }
        dao = _make_mock_dao(get_values)

        import asyncio

        result = asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=True)
        )
        assert result["status"] == "insufficient_data"
        assert result["months_with_data"] == 2

    def test_sufficient_data_calibrated_returns_success(self):
        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import performance_doc_id

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 8.0, 5.0, 2.0
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 7.0, 4.0, 1.0
            ),
            performance_doc_id("2026-04", "judge"): _make_snapshot(
                "2026-04", 9.0, 6.0, 3.0
            ),
        }
        dao = _make_mock_dao(get_values)

        import asyncio

        result = asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=True)
        )
        assert result["status"] == "success"
        assert result["calibration_ok"] is True
        assert result["drift_flags"] == []

    def test_drift_detected_returns_flags(self):
        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import performance_doc_id

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 5.0, 4.8, 4.6
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 5.0, 4.8, 4.6
            ),
            performance_doc_id("2026-04", "judge"): _make_snapshot(
                "2026-04", 5.0, 4.8, 4.6
            ),
        }
        dao = _make_mock_dao(get_values)

        import asyncio

        result = asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=True)
        )
        assert result["status"] == "success"
        assert result["calibration_ok"] is False
        assert len(result["drift_flags"]) > 0

    def test_dry_run_skips_firestore_write(self):
        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import performance_doc_id

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 8.0, 5.0, 2.0
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 7.0, 4.0, 1.0
            ),
            performance_doc_id("2026-04", "judge"): _make_snapshot(
                "2026-04", 9.0, 6.0, 3.0
            ),
        }
        dao = _make_mock_dao(get_values)

        import asyncio

        asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=True)
        )
        dao.set.assert_not_called()

    def test_non_dry_run_writes_calibration_doc(self):
        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import (
            CALIBRATION,
            calibration_report_doc_id,
            performance_doc_id,
        )

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 8.0, 5.0, 2.0
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 7.0, 4.0, 1.0
            ),
            performance_doc_id("2026-04", "judge"): _make_snapshot(
                "2026-04", 9.0, 6.0, 3.0
            ),
        }
        dao = _make_mock_dao(get_values)

        import asyncio

        asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=False)
        )
        set_calls = dao.set.call_args_list
        collections = [c[0][0] for c in set_calls]
        assert CALIBRATION in collections

        doc_ids = [c[0][1] for c in set_calls]
        assert calibration_report_doc_id(3, "judge") in doc_ids

    def test_drift_triggers_weight_override_doc(self):
        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import (
            performance_doc_id,
            weight_override_doc_id,
        )

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 5.0, 4.8, 4.6
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 5.0, 4.8, 4.6
            ),
            performance_doc_id("2026-04", "judge"): _make_snapshot(
                "2026-04", 5.0, 4.8, 4.6
            ),
        }
        dao = _make_mock_dao(get_values)

        import asyncio

        asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=False)
        )
        doc_ids = [c[0][1] for c in dao.set.call_args_list]
        assert weight_override_doc_id("judge") in doc_ids

    def test_no_snapshots_returns_insufficient_data(self):
        from screener.calibration.tracker import run_calibration_tracking

        dao = _make_mock_dao({})

        import asyncio

        result = asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=True)
        )
        assert result["status"] == "insufficient_data"
        assert result["months_with_data"] == 0

    def test_result_includes_months_with_data_count(self):
        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import performance_doc_id

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 8.0, 5.0, 2.0
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 7.0, 4.0, 1.0
            ),
            performance_doc_id("2026-04", "judge"): _make_snapshot(
                "2026-04", 9.0, 6.0, 3.0
            ),
        }
        dao = _make_mock_dao(get_values)

        import asyncio

        result = asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=True)
        )
        assert result["months_with_data"] == 3


# ---------------------------------------------------------------------------
# CalibrationHistoryDoc writing (P3-06)
# ---------------------------------------------------------------------------


class TestCalibrationHistoryDoc:
    """Tests for the history doc written by run_calibration_tracking (P3-06)."""

    def test_history_doc_written_when_calibration_ok(self):
        """When calibration is OK, history doc has delta_magnitude=0 and after==before."""
        import asyncio

        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import (
            CALIBRATION_HISTORY,
            calibration_history_doc_id,
            performance_doc_id,
        )

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 8.0, 5.0, 2.0
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 7.0, 4.0, 1.0
            ),
            performance_doc_id("2026-04", "judge"): _make_snapshot(
                "2026-04", 9.0, 6.0, 3.0
            ),
        }
        dao = _make_mock_dao(get_values)

        result = asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=False)
        )

        assert result["calibration_ok"] is True
        assert result["history_doc_written"] is True

        # Find the set() call for CALIBRATION_HISTORY.
        history_calls = [
            c for c in dao.set.call_args_list if c[0][0] == CALIBRATION_HISTORY
        ]
        assert len(history_calls) == 1
        _, doc_id, payload = history_calls[0][0]
        assert doc_id == calibration_history_doc_id("2026-05", "judge")
        assert payload["calibration_ok"] is True
        assert payload["delta_magnitude"] == pytest.approx(0.0)
        assert payload["W1_after"] == pytest.approx(payload["W1_before"])

    def test_history_doc_written_when_drift_detected(self):
        """When drift is detected, history doc has delta_magnitude > 0 and after != before."""
        import asyncio

        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import (
            CALIBRATION_HISTORY,
            performance_doc_id,
        )

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 5.0, 4.8, 4.6
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 5.0, 4.8, 4.6
            ),
            performance_doc_id("2026-04", "judge"): _make_snapshot(
                "2026-04", 5.0, 4.8, 4.6
            ),
        }
        dao = _make_mock_dao(get_values)

        result = asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=False)
        )

        assert result["calibration_ok"] is False
        assert result["history_doc_written"] is True

        history_calls = [
            c for c in dao.set.call_args_list if c[0][0] == CALIBRATION_HISTORY
        ]
        assert len(history_calls) == 1
        _, _, payload = history_calls[0][0]
        assert payload["calibration_ok"] is False
        assert payload["delta_magnitude"] > 0.0
        assert payload["W1_after"] != pytest.approx(payload["W1_before"])

    def test_dry_run_skips_history_write(self):
        """With dry_run=True, the history doc is NOT written (dao.set not called at all)."""
        import asyncio

        from screener.calibration.tracker import run_calibration_tracking
        from screener.lib.storage.schema import (
            CALIBRATION_HISTORY,
            performance_doc_id,
        )

        get_values = {
            performance_doc_id("2026-02", "judge"): _make_snapshot(
                "2026-02", 8.0, 5.0, 2.0
            ),
            performance_doc_id("2026-03", "judge"): _make_snapshot(
                "2026-03", 7.0, 4.0, 1.0
            ),
            performance_doc_id("2026-04", "judge"): _make_snapshot(
                "2026-04", 9.0, 6.0, 3.0
            ),
        }
        dao = _make_mock_dao(get_values)

        result = asyncio.run(
            run_calibration_tracking(dao, "2026-05", window_months=3, dry_run=True)
        )

        assert result["history_doc_written"] is False

        history_calls = [
            c for c in dao.set.call_args_list if c[0][0] == CALIBRATION_HISTORY
        ]
        assert len(history_calls) == 0


# ---------------------------------------------------------------------------
# run_calibration_trend_report (P3-07)
# ---------------------------------------------------------------------------


def _make_history_doc(
    month_id: str,
    delta_magnitude: float,
    calibration_ok: bool,
    drift_flags_count: int = 0,
) -> dict:
    """Build a minimal CalibrationHistoryDoc dict for use in trend-report tests."""
    return {
        "month_id": month_id,
        "source": "judge",
        "W1_before": 0.40,
        "W1_after": 0.40 - delta_magnitude / 3,
        "W2_before": 0.35,
        "W2_after": 0.35 + delta_magnitude / 3,
        "W3_before": 0.25,
        "W3_after": 0.25,
        "delta_magnitude": delta_magnitude,
        "drift_flags_count": drift_flags_count,
        "calibration_ok": calibration_ok,
        "timestamp": "2026-05-01T00:00:00+00:00",
    }


class TestRunCalibrationTrendReport:
    """Tests for run_calibration_trend_report (P3-07)."""

    def test_trend_report_empty(self):
        """When no history docs exist, the report has zeroed-out / None fields."""
        import asyncio

        from screener.calibration.tracker import run_calibration_trend_report

        dao = _make_mock_dao({})
        result = asyncio.run(
            run_calibration_trend_report(dao, n_months=3, source="judge")
        )

        assert result["months_queried"] == 3
        assert result["months_with_data"] == 0
        assert result["calibration_ok_count"] == 0
        assert result["calibration_ok_rate"] is None
        assert result["avg_drift_flags"] is None
        assert result["weight_delta_trend"] == []

    def test_trend_report_with_data(self):
        """With mock history docs, correct calibration_ok_rate and sorted trend returned."""
        import asyncio

        from screener.calibration.tracker import run_calibration_trend_report
        from screener.lib.storage.schema import calibration_history_doc_id

        # Provide two months of history: one ok, one with drift.
        get_values = {
            calibration_history_doc_id("2026-03", "judge"): _make_history_doc(
                "2026-03", delta_magnitude=0.0, calibration_ok=True, drift_flags_count=0
            ),
            calibration_history_doc_id("2026-04", "judge"): _make_history_doc(
                "2026-04",
                delta_magnitude=0.1,
                calibration_ok=False,
                drift_flags_count=2,
            ),
        }
        dao = _make_mock_dao(get_values)

        # Query 3 months — one will be missing (2026-05 has no doc in this window
        # because n_months=3 starting from the current month 2026-05 covers
        # 2026-03, 2026-04, 2026-05).
        result = asyncio.run(
            run_calibration_trend_report(dao, n_months=3, source="judge")
        )

        assert result["months_queried"] == 3
        assert result["months_with_data"] == 2
        assert result["calibration_ok_count"] == 1
        assert result["calibration_ok_rate"] == pytest.approx(0.5)
        assert result["avg_drift_flags"] == pytest.approx(1.0)  # (0 + 2) / 2

        # Trend is sorted chronologically.
        trend = result["weight_delta_trend"]
        assert len(trend) == 2
        assert trend[0]["month_id"] == "2026-03"
        assert trend[1]["month_id"] == "2026-04"
        assert trend[0]["delta_magnitude"] == pytest.approx(0.0)
        assert trend[1]["delta_magnitude"] == pytest.approx(0.1)
        assert trend[0]["calibration_ok"] is True
        assert trend[1]["calibration_ok"] is False
