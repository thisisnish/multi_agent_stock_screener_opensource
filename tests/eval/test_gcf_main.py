"""
tests/eval/test_gcf_main.py — Unit tests for gcf/eval/main.py.

Covers:
- _parse_month_id() parses valid "YYYY-MM" strings correctly
- _parse_month_id() raises ValueError on malformed input
- _build_eval_context() returns correct keys from EvalMetrics and acid_test
- _build_eval_context() maps closed_picks to total_picks_scored
- run_eval_main() returns "no_picks" status when no scoreable picks found
- run_eval_main() calls dao.set on EVAL collection when dry_run=False
- run_eval_main() skips dao.set when dry_run=True
- run_eval_main() returns "success" status with accuracy and bias on valid picks
- run_eval_main() writes eval_context as a key in the eval doc
- eval_handler() returns 400 when month_id is missing from body
- eval_handler() returns 200 on success
- eval_handler() returns 400 on ValueError (bad month_id)
- eval_handler() returns 500 on unexpected error

No real GCP or LangChain calls are made — StorageDAO is fully mocked.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Ensure project root is importable (mirrors conftest.py behaviour)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_closed_pick(
    action: str = "BUY",
    beat_spy: bool = True,
    entry_month: str = "2026-03",
    confidence_score: float = 75.0,
    pick_return_pct: float = 5.0,
    sector: str = "Technology",
) -> dict:
    return {
        "action": action,
        "beat_spy": beat_spy,
        "entry_month": entry_month,
        "status": "closed",
        "confidence_score": confidence_score,
        "pick_return_pct": pick_return_pct,
        "sector": sector,
        "bull_signal_citations": [],
        "bear_signal_citations": [],
    }


def _make_app_config():
    """Minimal AppConfig stub that satisfies run_eval_main() interface."""
    from screener.lib.config_loader import (
        AppConfig,
        EdgarConfig,
        EmailConfig,
        FirestoreConfig,
        LLMConfig,
        NotificationsConfig,
        ScreenerConfig,
        SignalsConfig,
        StorageConfig,
    )

    return AppConfig(
        llm=LLMConfig(
            model="anthropic:claude-haiku-4-5-20251001",
            embedder_model="google_genai:models/gemini-embedding-001",
        ),
        storage=StorageConfig(
            provider="firestore",
            firestore=FirestoreConfig(project_id="test-project", database="test-db"),
        ),
        signals=SignalsConfig(),
        screener=ScreenerConfig(),
        notifications=NotificationsConfig(
            email=EmailConfig(enabled=False, recipients=[])
        ),
        edgar=EdgarConfig(),
    )


# ---------------------------------------------------------------------------
# _parse_month_id
# ---------------------------------------------------------------------------


class TestParseMonthId:
    def test_valid_month_id(self):
        from gcf.eval.main import _parse_month_id

        year, month = _parse_month_id("2026-04")
        assert year == 2026
        assert month == 4

    def test_january(self):
        from gcf.eval.main import _parse_month_id

        year, month = _parse_month_id("2026-01")
        assert year == 2026
        assert month == 1

    def test_december(self):
        from gcf.eval.main import _parse_month_id

        year, month = _parse_month_id("2025-12")
        assert year == 2025
        assert month == 12

    def test_invalid_format_raises(self):
        from gcf.eval.main import _parse_month_id

        with pytest.raises(ValueError, match="YYYY-MM"):
            _parse_month_id("2026/04")

    def test_missing_month_raises(self):
        from gcf.eval.main import _parse_month_id

        with pytest.raises(ValueError, match="YYYY-MM"):
            _parse_month_id("2026")

    def test_invalid_month_number_raises(self):
        from gcf.eval.main import _parse_month_id

        with pytest.raises(ValueError):
            _parse_month_id("2026-13")


# ---------------------------------------------------------------------------
# _build_eval_context
# ---------------------------------------------------------------------------


class TestBuildEvalContext:
    def _make_metrics(
        self,
        closed_picks: int = 5,
        overall_accuracy: float | None = 60.0,
        bull_accuracy: float | None = 70.0,
        bear_accuracy: float | None = 50.0,
        directional_bias: str = "bullish",
        confidence_calibration: float = 10.0,
    ):
        from screener.lib.models import EvalMetrics

        return EvalMetrics(
            period="2026-03",
            total_picks=5,
            closed_picks=closed_picks,
            open_picks=0,
            overall_accuracy=overall_accuracy,
            bull_accuracy=bull_accuracy,
            bear_accuracy=bear_accuracy,
            avg_confidence=70.0,
            avg_score=70.0,
            confidence_calibration=confidence_calibration,
            directional_bias=directional_bias,
        )

    def test_total_picks_scored_maps_to_closed_picks(self):
        from gcf.eval.main import _build_eval_context

        metrics = self._make_metrics(closed_picks=5)
        context = _build_eval_context(metrics, [], {})
        assert context["total_picks_scored"] == 5

    def test_accuracy_values_pass_through(self):
        from gcf.eval.main import _build_eval_context

        metrics = self._make_metrics(
            overall_accuracy=62.5,
            bull_accuracy=70.0,
            bear_accuracy=55.0,
        )
        context = _build_eval_context(metrics, [], {})
        assert context["overall_accuracy"] == 62.5
        assert context["bull_accuracy"] == 70.0
        assert context["bear_accuracy"] == 55.0

    def test_systematic_issues_included(self):
        from gcf.eval.main import _build_eval_context

        metrics = self._make_metrics()
        issues = [
            "Overconfidence: avg confidence 90.0 vs accuracy 60.0% (gap: 30.0pts)"
        ]
        context = _build_eval_context(metrics, issues, {})
        assert context["systematic_issues"] == issues

    def test_acid_test_included(self):
        from gcf.eval.main import _build_eval_context

        metrics = self._make_metrics()
        acid = {
            "High": {
                "count": 3,
                "accuracy_pct": 66.7,
                "max_drawdown": 5.0,
                "avg_return": 2.0,
            }
        }
        context = _build_eval_context(metrics, [], acid)
        assert context["acid_test"] == acid

    def test_directional_bias_and_calibration_included(self):
        from gcf.eval.main import _build_eval_context

        metrics = self._make_metrics(
            directional_bias="bearish",
            confidence_calibration=15.5,
        )
        context = _build_eval_context(metrics, [], {})
        assert context["directional_bias"] == "bearish"
        assert context["confidence_calibration"] == 15.5

    def test_all_required_keys_present(self):
        from gcf.eval.main import _build_eval_context

        metrics = self._make_metrics()
        context = _build_eval_context(metrics, [], {})
        required_keys = {
            "total_picks_scored",
            "overall_accuracy",
            "bull_accuracy",
            "bear_accuracy",
            "directional_bias",
            "confidence_calibration",
            "systematic_issues",
            "acid_test",
        }
        assert required_keys.issubset(context.keys())


# ---------------------------------------------------------------------------
# run_eval_main — no picks
# ---------------------------------------------------------------------------


class TestRunEvalMainNoPicks:
    def _make_mock_dao(self, picks: list[dict]) -> AsyncMock:
        dao = MagicMock()
        dao.query = AsyncMock(return_value=picks)
        dao.set = AsyncMock()
        return dao

    def test_returns_no_picks_status_when_all_picks_are_hold(self):
        from gcf.eval.main import run_eval_main

        dao = self._make_mock_dao(
            [
                {
                    "action": "HOLD",
                    "beat_spy": None,
                    "entry_month": "2026-03",
                    "status": "closed",
                }
            ]
        )
        result = run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        assert result["status"] == "no_picks"
        assert result["month_id"] == "2026-03"

    def test_returns_no_picks_status_when_closed_picks_empty(self):
        from gcf.eval.main import run_eval_main

        dao = self._make_mock_dao([])
        result = run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        assert result["status"] == "no_picks"
        assert result["total_picks"] == 0

    def test_does_not_call_dao_set_on_no_picks(self):
        from gcf.eval.main import run_eval_main

        dao = self._make_mock_dao([])
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        dao.set.assert_not_called()


# ---------------------------------------------------------------------------
# run_eval_main — valid picks, dry_run=True
# ---------------------------------------------------------------------------


class TestRunEvalMainDryRun:
    def _make_mock_dao(self, picks: list[dict]) -> AsyncMock:
        dao = MagicMock()
        dao.query = AsyncMock(return_value=picks)
        dao.set = AsyncMock()
        return dao

    def test_returns_success_status(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True), _make_closed_pick(beat_spy=False)]
        dao = self._make_mock_dao(picks)
        result = run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        assert result["status"] == "success"
        assert result["month_id"] == "2026-03"

    def test_scored_picks_count_matches_non_hold_picks(self):
        from gcf.eval.main import run_eval_main

        picks = [
            _make_closed_pick(action="BUY", beat_spy=True),
            _make_closed_pick(action="SELL", beat_spy=False),
            {
                "action": "HOLD",
                "beat_spy": None,
                "entry_month": "2026-03",
                "status": "closed",
            },
        ]
        dao = self._make_mock_dao(picks)
        result = run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        # HOLD is filtered by score_picks_pure_math
        assert result["scored_picks"] == 2

    def test_skips_dao_set_when_dry_run_true(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True)]
        dao = self._make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        dao.set.assert_not_called()

    def test_overall_accuracy_100_when_all_beat_spy(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True) for _ in range(4)]
        dao = self._make_mock_dao(picks)
        result = run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        assert result["overall_accuracy"] == 100.0

    def test_overall_accuracy_0_when_none_beat_spy(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=False) for _ in range(3)]
        dao = self._make_mock_dao(picks)
        result = run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        assert result["overall_accuracy"] == 0.0


# ---------------------------------------------------------------------------
# run_eval_main — valid picks, dry_run=False
# ---------------------------------------------------------------------------


class TestRunEvalMainWritePath:
    def _make_mock_dao(self, picks: list[dict]) -> AsyncMock:
        dao = MagicMock()
        dao.query = AsyncMock(return_value=picks)
        dao.set = AsyncMock()
        return dao

    def test_calls_dao_set_once_for_eval_doc(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True), _make_closed_pick(beat_spy=False)]
        dao = self._make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        dao.set.assert_called_once()

    def test_dao_set_writes_to_eval_collection(self):
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL

        picks = [_make_closed_pick(beat_spy=True)]
        dao = self._make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        call_args = dao.set.call_args
        collection = call_args[0][0]
        assert collection == EVAL

    def test_eval_doc_id_matches_eval_doc_id_helper(self):
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import eval_doc_id

        picks = [_make_closed_pick(beat_spy=True)]
        dao = self._make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        call_args = dao.set.call_args
        doc_id = call_args[0][1]
        assert doc_id == eval_doc_id(2026, 3)

    def test_eval_doc_contains_eval_context_key(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True)]
        dao = self._make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        call_args = dao.set.call_args
        doc = call_args[0][2]
        assert "eval_context" in doc

    def test_eval_doc_contains_month_id(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True)]
        dao = self._make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        call_args = dao.set.call_args
        doc = call_args[0][2]
        assert doc["month_id"] == "2026-03"

    def test_eval_doc_contains_metrics(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True), _make_closed_pick(beat_spy=False)]
        dao = self._make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        call_args = dao.set.call_args
        doc = call_args[0][2]
        assert "metrics" in doc

    def test_eval_doc_contains_acid_test(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True)]
        dao = self._make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        call_args = dao.set.call_args
        doc = call_args[0][2]
        assert "acid_test" in doc

    def test_eval_context_total_picks_scored_is_non_zero(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True), _make_closed_pick(beat_spy=True)]
        dao = self._make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        call_args = dao.set.call_args
        doc = call_args[0][2]
        assert doc["eval_context"]["total_picks_scored"] > 0

    def test_filters_picks_by_entry_month(self):
        """Picks from a different entry_month must not be scored."""
        from gcf.eval.main import run_eval_main

        picks = [
            _make_closed_pick(beat_spy=True, entry_month="2026-03"),
            _make_closed_pick(beat_spy=False, entry_month="2026-02"),  # wrong month
        ]
        dao = self._make_mock_dao(picks)
        result = run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        # Only the 2026-03 pick should be scored
        assert result["scored_picks"] == 1

    def test_systematic_issues_in_result(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True)]
        dao = self._make_mock_dao(picks)
        result = run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        assert "systematic_issues" in result
        assert isinstance(result["systematic_issues"], list)


# ---------------------------------------------------------------------------
# eval_handler
# ---------------------------------------------------------------------------


class TestEvalHandler:
    def _make_request(self, body: dict | None = None) -> MagicMock:
        """Build a mock Flask Request object."""
        req = MagicMock()
        req.get_json = MagicMock(return_value=body)
        return req

    def test_missing_month_id_returns_400(self):
        from gcf.eval.main import eval_handler

        req = self._make_request(body={})
        body, status = eval_handler(req)
        assert status == 400
        assert "month_id" in body

    def test_none_body_returns_400(self):
        from gcf.eval.main import eval_handler

        req = MagicMock()
        req.get_json = MagicMock(return_value=None)
        # No MONTH_ID env var set
        with patch("os.environ.get", return_value=None):
            body, status = eval_handler(req)
        assert status == 400

    def test_invalid_month_id_returns_400(self):
        from gcf.eval.main import eval_handler

        req = self._make_request(body={"month_id": "not-a-date"})

        mock_dao = MagicMock()
        mock_dao.query = AsyncMock(return_value=[])
        mock_dao.set = AsyncMock()

        with (
            patch("gcf.eval.main.load_config", return_value=_make_app_config()),
            patch("gcf.eval.main.FirestoreDAO", return_value=mock_dao),
        ):
            body, status = eval_handler(req)
        assert status == 400

    def test_success_returns_200(self):
        from gcf.eval.main import eval_handler

        req = self._make_request(body={"month_id": "2026-03", "dry_run": True})

        mock_dao = MagicMock()
        mock_dao.query = AsyncMock(return_value=[])
        mock_dao.set = AsyncMock()

        with (
            patch("gcf.eval.main.load_config", return_value=_make_app_config()),
            patch("gcf.eval.main.FirestoreDAO", return_value=mock_dao),
        ):
            body, status = eval_handler(req)
        assert status == 200

    def test_success_body_is_json(self):
        import json

        from gcf.eval.main import eval_handler

        req = self._make_request(body={"month_id": "2026-03", "dry_run": True})

        mock_dao = MagicMock()
        mock_dao.query = AsyncMock(return_value=[])
        mock_dao.set = AsyncMock()

        with (
            patch("gcf.eval.main.load_config", return_value=_make_app_config()),
            patch("gcf.eval.main.FirestoreDAO", return_value=mock_dao),
        ):
            body, _ = eval_handler(req)
        # Must be parseable JSON
        parsed = json.loads(body)
        assert isinstance(parsed, dict)

    def test_unexpected_error_returns_500(self):
        from gcf.eval.main import eval_handler

        req = self._make_request(body={"month_id": "2026-03"})

        with (
            patch("gcf.eval.main.load_config", side_effect=RuntimeError("boom")),
        ):
            body, status = eval_handler(req)
        assert status == 500

    def test_dry_run_flag_passed_through(self):
        """dry_run=True in request body must result in no dao.set call."""
        from gcf.eval.main import eval_handler

        picks = [_make_closed_pick(beat_spy=True)]
        req = self._make_request(body={"month_id": "2026-03", "dry_run": True})

        mock_dao = MagicMock()
        mock_dao.query = AsyncMock(return_value=picks)
        mock_dao.set = AsyncMock()

        with (
            patch("gcf.eval.main.load_config", return_value=_make_app_config()),
            patch("gcf.eval.main.FirestoreDAO", return_value=mock_dao),
        ):
            eval_handler(req)

        mock_dao.set.assert_not_called()
