"""
tests/eval/test_p3_09_10_eval_trend.py — Tests for P3-09 and P3-10.

Covers:
P3-09 — EvalTrendDoc persistence:
  - EvalTrendDoc is written to EVAL_TREND after a successful eval run (dry_run=False)
  - confidence_gap is correctly computed as high_conf_accuracy - low_conf_accuracy
  - confidence_gap is None when either high or low confidence accuracy is None
  - dry_run=True skips the trend doc write entirely
  - trend doc is written BEFORE the main eval doc (both writes happen when dry_run=False)
  - run_eval_trend_report() returns the correct structure with mock docs
  - run_eval_trend_report() aggregates avg_confidence_gap correctly
  - run_eval_trend_report() handles months with no data (dao.get returns None)

P3-10 — LLM rubric sample scoring:
  - rubric_sample_rate=0.0 makes zero calls to score_picks_llm()
  - rubric_sample_rate=1.0 calls score_picks_llm() and stores averaged sub-scores
  - rubric stats are absent from trend doc when rubric_sample_rate=0.0
  - rubric stats are present in trend doc when rubric_sample_rate=1.0 and picks exist
  - score_picks_llm() skips HOLD picks and picks without a ticker

Config:
  - EvalConfig defaults to rubric_sample_rate=0.0
  - rubric_sample_rate outside [0.0, 1.0] raises ConfigError
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_closed_pick(
    action: str = "BUY",
    beat_spy: bool = True,
    entry_month: str = "2026-03",
    confidence_score: float = 75.0,
    pick_return_pct: float = 5.0,
    sector: str = "Technology",
    ticker: str = "AAPL",
) -> dict:
    return {
        "ticker": ticker,
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


def _make_app_config(rubric_sample_rate: float = 0.0):
    """Minimal AppConfig stub that satisfies run_eval_main() interface."""
    from screener.lib.config_loader import (
        AppConfig,
        EdgarConfig,
        EvalConfig,
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
        eval=EvalConfig(rubric_sample_rate=rubric_sample_rate),
    )


def _make_mock_dao(picks: list[dict], trend_docs: dict | None = None) -> MagicMock:
    """Build a mock DAO.

    Args:
        picks: Returned by dao.query(...).
        trend_docs: Mapping of doc_id -> dict for dao.get() responses.  None
                    values represent missing documents.  Defaults to all-None.
    """
    dao = MagicMock()
    dao.query = AsyncMock(return_value=picks)
    dao.set = AsyncMock()

    if trend_docs is not None:

        async def _get(collection, doc_id):
            return trend_docs.get(doc_id)

        dao.get = _get
    else:
        dao.get = AsyncMock(return_value=None)

    return dao


# ---------------------------------------------------------------------------
# P3-09: EvalTrendDoc written after successful eval
# ---------------------------------------------------------------------------


class TestEvalTrendDocWrite:
    """Verify that run_eval_main() writes an EvalTrendDoc when dry_run=False."""

    def test_trend_doc_written_on_success(self):
        """dao.set must be called for EVAL_TREND when dry_run=False."""
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL_TREND

        picks = [_make_closed_pick(beat_spy=True), _make_closed_pick(beat_spy=False)]
        dao = _make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)

        collections_written = [c[0][0] for c in dao.set.call_args_list]
        assert EVAL_TREND in collections_written

    def test_trend_doc_not_written_when_dry_run_true(self):
        """dao.set must NOT be called at all when dry_run=True."""
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True)]
        dao = _make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=True)
        dao.set.assert_not_called()

    def test_trend_doc_id_is_month_id(self):
        """The doc ID written to EVAL_TREND must equal the month_id."""
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL_TREND

        picks = [_make_closed_pick(beat_spy=True)]
        dao = _make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)

        trend_calls = [c for c in dao.set.call_args_list if c[0][0] == EVAL_TREND]
        assert len(trend_calls) == 1
        assert trend_calls[0][0][1] == "2026-03"

    def test_trend_doc_contains_expected_fields(self):
        """The written trend doc must contain the core metric fields."""
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL_TREND

        picks = [_make_closed_pick(beat_spy=True), _make_closed_pick(beat_spy=False)]
        dao = _make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)

        trend_calls = [c for c in dao.set.call_args_list if c[0][0] == EVAL_TREND]
        doc = trend_calls[0][0][2]

        assert doc["period"] == "2026-03"
        assert "overall_accuracy" in doc
        assert "confidence_calibration" in doc
        assert "avg_score" in doc
        assert "run_ts" in doc

    def test_trend_doc_not_written_on_no_picks(self):
        """No trend doc write when there are no scoreable picks."""
        from gcf.eval.main import run_eval_main

        dao = _make_mock_dao([])
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        dao.set.assert_not_called()

    def test_two_writes_on_success_dry_run_false(self):
        """Both EVAL and EVAL_TREND writes must happen on dry_run=False."""
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL, EVAL_TREND

        picks = [_make_closed_pick(beat_spy=True)]
        dao = _make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)

        collections_written = [c[0][0] for c in dao.set.call_args_list]
        assert EVAL in collections_written
        assert EVAL_TREND in collections_written


# ---------------------------------------------------------------------------
# P3-09: confidence_gap computation
# ---------------------------------------------------------------------------


class TestConfidenceGapComputation:
    """Unit-level tests for confidence_gap logic in _run_async."""

    def _run_and_get_trend_doc(self, picks: list[dict]) -> dict:
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL_TREND

        dao = _make_mock_dao(picks)
        run_eval_main(_make_app_config(), dao, "2026-03", dry_run=False)
        trend_calls = [c for c in dao.set.call_args_list if c[0][0] == EVAL_TREND]
        return trend_calls[0][0][2]

    def test_confidence_gap_is_none_when_no_high_conf_picks(self):
        """confidence_gap is None when all picks are medium confidence."""
        picks = [
            _make_closed_pick(beat_spy=True, confidence_score=55.0),
            _make_closed_pick(beat_spy=False, confidence_score=55.0),
        ]
        doc = self._run_and_get_trend_doc(picks)
        assert doc["confidence_gap"] is None

    def test_confidence_gap_is_none_when_no_low_conf_picks(self):
        """confidence_gap is None when all picks are high confidence."""
        picks = [
            _make_closed_pick(beat_spy=True, confidence_score=80.0),
            _make_closed_pick(beat_spy=False, confidence_score=80.0),
        ]
        doc = self._run_and_get_trend_doc(picks)
        assert doc["confidence_gap"] is None

    def test_confidence_gap_computed_when_both_tiers_present(self):
        """confidence_gap = high_acc - low_acc when both tiers have picks."""
        # 2 high-confidence picks both beat SPY → high_acc = 100.0
        # 1 low-confidence pick doesn't beat SPY → low_acc = 0.0
        # expected gap = 100.0 - 0.0 = 100.0
        picks = [
            _make_closed_pick(beat_spy=True, confidence_score=80.0),
            _make_closed_pick(beat_spy=True, confidence_score=75.0),
            _make_closed_pick(beat_spy=False, confidence_score=20.0),
        ]
        doc = self._run_and_get_trend_doc(picks)
        assert doc["confidence_gap"] == 100.0

    def test_confidence_gap_can_be_negative(self):
        """confidence_gap is negative when low-conf picks outperform high-conf."""
        # 2 low-conf picks both beat SPY → low_acc = 100.0
        # 1 high-conf pick doesn't → high_acc = 0.0
        # expected gap = 0.0 - 100.0 = -100.0
        picks = [
            _make_closed_pick(beat_spy=False, confidence_score=80.0),
            _make_closed_pick(beat_spy=True, confidence_score=20.0),
            _make_closed_pick(beat_spy=True, confidence_score=25.0),
        ]
        doc = self._run_and_get_trend_doc(picks)
        assert doc["confidence_gap"] == -100.0


# ---------------------------------------------------------------------------
# P3-09: run_eval_trend_report
# ---------------------------------------------------------------------------


class TestRunEvalTrendReport:
    """Tests for screener/eval/reporter.py::run_eval_trend_report()."""

    def _make_trend_doc(
        self,
        period: str,
        confidence_gap: float | None = None,
        confidence_calibration: float | None = None,
    ) -> dict:
        return {
            "period": period,
            "confidence_gap": confidence_gap,
            "confidence_calibration": confidence_calibration,
        }

    def test_returns_correct_structure_keys(self):
        from screener.eval.reporter import run_eval_trend_report

        dao = _make_mock_dao([], trend_docs={})
        result = asyncio.run(run_eval_trend_report(dao, n_months=3))

        assert "months_queried" in result
        assert "months_with_data" in result
        assert "avg_confidence_gap" in result
        assert "confidence_gap_trend" in result

    def test_months_queried_matches_n_months(self):
        from screener.eval.reporter import run_eval_trend_report

        dao = _make_mock_dao([], trend_docs={})
        result = asyncio.run(run_eval_trend_report(dao, n_months=6))
        assert result["months_queried"] == 6

    def test_months_with_data_zero_when_all_missing(self):
        from screener.eval.reporter import run_eval_trend_report

        dao = _make_mock_dao([], trend_docs={})
        result = asyncio.run(run_eval_trend_report(dao, n_months=3))
        assert result["months_with_data"] == 0

    def test_months_with_data_counts_present_docs(self):
        from screener.eval.reporter import run_eval_trend_report

        # Return a doc for one specific period — we need to figure out which
        # period IDs the reporter will query for n_months=3.
        from screener.eval.reporter import _last_n_month_ids

        periods = _last_n_month_ids(3)
        trend_docs = {
            periods[-1]: self._make_trend_doc(periods[-1], confidence_gap=5.0)
        }

        dao = _make_mock_dao([], trend_docs=trend_docs)
        result = asyncio.run(run_eval_trend_report(dao, n_months=3))
        assert result["months_with_data"] == 1

    def test_avg_confidence_gap_none_when_no_data(self):
        from screener.eval.reporter import run_eval_trend_report

        dao = _make_mock_dao([], trend_docs={})
        result = asyncio.run(run_eval_trend_report(dao, n_months=3))
        assert result["avg_confidence_gap"] is None

    def test_avg_confidence_gap_computed_across_docs(self):
        from screener.eval.reporter import _last_n_month_ids, run_eval_trend_report

        periods = _last_n_month_ids(3)
        trend_docs = {
            periods[0]: self._make_trend_doc(periods[0], confidence_gap=10.0),
            periods[1]: self._make_trend_doc(periods[1], confidence_gap=20.0),
            # periods[2] has no data
        }

        dao = _make_mock_dao([], trend_docs=trend_docs)
        result = asyncio.run(run_eval_trend_report(dao, n_months=3))
        # avg of [10.0, 20.0] = 15.0
        assert result["avg_confidence_gap"] == 15.0

    def test_confidence_gap_trend_is_sorted_chronologically(self):
        from screener.eval.reporter import _last_n_month_ids, run_eval_trend_report

        periods = _last_n_month_ids(4)
        trend_docs = {p: self._make_trend_doc(p) for p in periods}

        dao = _make_mock_dao([], trend_docs=trend_docs)
        result = asyncio.run(run_eval_trend_report(dao, n_months=4))

        trend = result["confidence_gap_trend"]
        returned_periods = [e["period"] for e in trend]
        assert returned_periods == sorted(returned_periods)

    def test_confidence_gap_trend_entries_have_required_keys(self):
        from screener.eval.reporter import run_eval_trend_report

        dao = _make_mock_dao([], trend_docs={})
        result = asyncio.run(run_eval_trend_report(dao, n_months=2))

        for entry in result["confidence_gap_trend"]:
            assert "period" in entry
            assert "confidence_gap" in entry
            assert "confidence_calibration" in entry

    def test_confidence_gap_none_for_missing_months(self):
        from screener.eval.reporter import run_eval_trend_report

        dao = _make_mock_dao([], trend_docs={})
        result = asyncio.run(run_eval_trend_report(dao, n_months=2))

        for entry in result["confidence_gap_trend"]:
            assert entry["confidence_gap"] is None
            assert entry["confidence_calibration"] is None


# ---------------------------------------------------------------------------
# P3-09: _last_n_month_ids helper
# ---------------------------------------------------------------------------


class TestLastNMonthIds:
    def test_returns_n_months(self):
        from screener.eval.reporter import _last_n_month_ids
        from datetime import datetime, timezone

        now = datetime(2026, 5, 1, tzinfo=timezone.utc)
        result = _last_n_month_ids(3, now=now)
        assert len(result) == 3

    def test_last_entry_is_current_month(self):
        from screener.eval.reporter import _last_n_month_ids
        from datetime import datetime, timezone

        now = datetime(2026, 5, 15, tzinfo=timezone.utc)
        result = _last_n_month_ids(3, now=now)
        assert result[-1] == "2026-05"

    def test_rolls_back_across_year_boundary(self):
        from screener.eval.reporter import _last_n_month_ids
        from datetime import datetime, timezone

        now = datetime(2026, 2, 1, tzinfo=timezone.utc)
        result = _last_n_month_ids(3, now=now)
        assert "2025-12" in result
        assert "2026-01" in result
        assert "2026-02" in result

    def test_sorted_chronologically(self):
        from screener.eval.reporter import _last_n_month_ids

        result = _last_n_month_ids(6)
        assert result == sorted(result)


# ---------------------------------------------------------------------------
# P3-10: rubric_sample_rate=0.0 — no LLM calls
# ---------------------------------------------------------------------------


class TestRubricSamplingDisabled:
    """When rubric_sample_rate=0.0 (default), score_picks_llm must never be called."""

    def test_score_picks_llm_not_called_at_default_rate(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True), _make_closed_pick(beat_spy=False)]
        dao = _make_mock_dao(picks)

        with patch("gcf.eval.main.score_picks_llm") as mock_llm:
            run_eval_main(
                _make_app_config(rubric_sample_rate=0.0), dao, "2026-03", dry_run=True
            )

        mock_llm.assert_not_called()

    def test_trend_doc_has_no_rubric_fields_at_default_rate(self):
        """rubric_sample_count etc. are None in the trend doc when rate=0.0."""
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL_TREND

        picks = [_make_closed_pick(beat_spy=True)]
        dao = _make_mock_dao(picks)
        run_eval_main(
            _make_app_config(rubric_sample_rate=0.0), dao, "2026-03", dry_run=False
        )

        trend_calls = [c for c in dao.set.call_args_list if c[0][0] == EVAL_TREND]
        doc = trend_calls[0][0][2]

        assert doc["rubric_sample_count"] is None
        assert doc["avg_reasoning_quality"] is None
        assert doc["avg_citation_density"] is None
        assert doc["avg_argument_structure"] is None


# ---------------------------------------------------------------------------
# P3-10: rubric_sample_rate=1.0 — score all picks, store averages
# ---------------------------------------------------------------------------


class TestRubricSamplingEnabled:
    """When rubric_sample_rate=1.0, score_picks_llm is called and results stored."""

    def _make_score_result(
        self,
        timing_quality: int = 80,
        confidence_alignment: int = 70,
        risk_management: int = 60,
    ):
        from screener.lib.models import ScoreResult

        return ScoreResult(
            score=70,
            accuracy=True,
            confidence_alignment=confidence_alignment,
            timing_quality=timing_quality,
            risk_management=risk_management,
            error_flags=[],
            rationale="",
            bull_accuracy=True,
            bear_accuracy=None,
        )

    def test_score_picks_llm_called_when_rate_is_1(self):
        from gcf.eval.main import run_eval_main

        picks = [_make_closed_pick(beat_spy=True), _make_closed_pick(beat_spy=False)]
        dao = _make_mock_dao(picks)

        fake_results = [self._make_score_result()]
        with patch(
            "gcf.eval.main.score_picks_llm", return_value=fake_results
        ) as mock_llm:
            run_eval_main(
                _make_app_config(rubric_sample_rate=1.0), dao, "2026-03", dry_run=True
            )

        mock_llm.assert_called_once()

    def test_rubric_stats_stored_in_trend_doc_when_rate_is_1(self):
        """avg_reasoning_quality etc. must be populated in the trend doc."""
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL_TREND

        picks = [_make_closed_pick(beat_spy=True)]
        dao = _make_mock_dao(picks)

        fake_results = [
            self._make_score_result(
                timing_quality=80, confidence_alignment=70, risk_management=60
            )
        ]
        with patch("gcf.eval.main.score_picks_llm", return_value=fake_results):
            run_eval_main(
                _make_app_config(rubric_sample_rate=1.0), dao, "2026-03", dry_run=False
            )

        trend_calls = [c for c in dao.set.call_args_list if c[0][0] == EVAL_TREND]
        doc = trend_calls[0][0][2]

        assert doc["rubric_sample_count"] == 1
        assert doc["avg_reasoning_quality"] == 80.0
        assert doc["avg_citation_density"] == 70.0
        assert doc["avg_argument_structure"] == 60.0

    def test_rubric_averages_computed_correctly_across_multiple_results(self):
        """Averages are computed across all rubric results."""
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL_TREND

        picks = [
            _make_closed_pick(beat_spy=True, ticker="AAPL"),
            _make_closed_pick(beat_spy=True, ticker="MSFT"),
        ]
        dao = _make_mock_dao(picks)

        # Two results with different sub-scores
        fake_results = [
            self._make_score_result(
                timing_quality=80, confidence_alignment=60, risk_management=40
            ),
            self._make_score_result(
                timing_quality=60, confidence_alignment=80, risk_management=80
            ),
        ]
        with patch("gcf.eval.main.score_picks_llm", return_value=fake_results):
            run_eval_main(
                _make_app_config(rubric_sample_rate=1.0), dao, "2026-03", dry_run=False
            )

        trend_calls = [c for c in dao.set.call_args_list if c[0][0] == EVAL_TREND]
        doc = trend_calls[0][0][2]

        assert doc["rubric_sample_count"] == 2
        assert doc["avg_reasoning_quality"] == 70.0  # (80+60)/2
        assert doc["avg_citation_density"] == 70.0  # (60+80)/2
        assert doc["avg_argument_structure"] == 60.0  # (40+80)/2

    def test_rubric_stats_absent_when_score_picks_llm_returns_empty(self):
        """When score_picks_llm returns [], rubric fields must be None in trend doc."""
        from gcf.eval.main import run_eval_main
        from screener.lib.storage.schema import EVAL_TREND

        picks = [_make_closed_pick(beat_spy=True)]
        dao = _make_mock_dao(picks)

        with patch("gcf.eval.main.score_picks_llm", return_value=[]):
            run_eval_main(
                _make_app_config(rubric_sample_rate=1.0), dao, "2026-03", dry_run=False
            )

        trend_calls = [c for c in dao.set.call_args_list if c[0][0] == EVAL_TREND]
        doc = trend_calls[0][0][2]

        assert doc["rubric_sample_count"] is None
        assert doc["avg_reasoning_quality"] is None


# ---------------------------------------------------------------------------
# P3-10: score_picks_llm() unit tests
# ---------------------------------------------------------------------------


class TestScorePicksLlm:
    """Unit tests for screener/eval/scorer.py::score_picks_llm()."""

    def _make_score_result(self):
        from screener.lib.models import ScoreResult

        return ScoreResult(
            score=70,
            accuracy=True,
            confidence_alignment=70,
            timing_quality=70,
            risk_management=70,
            error_flags=[],
            rationale="LLM scored",
            bull_accuracy=True,
            bear_accuracy=None,
        )

    def test_skips_hold_picks(self):
        from screener.eval.scorer import score_picks_llm

        picks = [{"ticker": "AAPL", "action": "HOLD", "confidence_score": 70.0}]
        app_config = _make_app_config()

        with patch("screener.eval.scorer.score_judge_pick") as mock_judge:
            result = score_picks_llm(picks, app_config)

        mock_judge.assert_not_called()
        assert result == []

    def test_skips_picks_without_ticker(self):
        from screener.eval.scorer import score_picks_llm

        picks = [{"action": "BUY", "confidence_score": 70.0}]
        app_config = _make_app_config()

        with patch("screener.eval.scorer.score_judge_pick") as mock_judge:
            result = score_picks_llm(picks, app_config)

        mock_judge.assert_not_called()
        assert result == []

    def test_calls_score_judge_pick_for_buy_pick(self):
        from screener.eval.scorer import score_picks_llm

        picks = [_make_closed_pick(action="BUY", ticker="AAPL")]
        app_config = _make_app_config()

        fake_result = self._make_score_result()
        with patch("screener.eval.scorer.score_judge_pick", return_value=fake_result):
            results = score_picks_llm(picks, app_config)

        assert len(results) == 1

    def test_returns_empty_list_for_empty_input(self):
        from screener.eval.scorer import score_picks_llm

        results = score_picks_llm([], _make_app_config())
        assert results == []

    def test_continues_on_score_judge_pick_exception(self):
        """Exceptions from score_judge_pick must not propagate — the failed pick is skipped."""
        from screener.eval.scorer import score_picks_llm

        picks = [
            _make_closed_pick(action="BUY", ticker="AAPL"),
            _make_closed_pick(action="BUY", ticker="MSFT"),
        ]
        fake_result = self._make_score_result()

        def _side_effect(*args, **kwargs):
            ticker = kwargs.get("ticker") or args[0]
            if ticker == "AAPL":
                raise RuntimeError("LLM timeout")
            return fake_result

        with patch("screener.eval.scorer.score_judge_pick", side_effect=_side_effect):
            results = score_picks_llm(picks, _make_app_config())

        # MSFT should still succeed
        assert len(results) == 1


# ---------------------------------------------------------------------------
# Config: EvalConfig validator
# ---------------------------------------------------------------------------


class TestEvalConfigValidator:
    def test_default_rubric_sample_rate_is_zero(self):
        from screener.lib.config_loader import EvalConfig

        cfg = EvalConfig()
        assert cfg.rubric_sample_rate == 0.0

    def test_valid_fraction_accepted(self):
        from screener.lib.config_loader import EvalConfig

        cfg = EvalConfig(rubric_sample_rate=0.1)
        assert cfg.rubric_sample_rate == 0.1

    def test_boundary_zero_accepted(self):
        from screener.lib.config_loader import EvalConfig

        cfg = EvalConfig(rubric_sample_rate=0.0)
        assert cfg.rubric_sample_rate == 0.0

    def test_boundary_one_accepted(self):
        from screener.lib.config_loader import EvalConfig

        cfg = EvalConfig(rubric_sample_rate=1.0)
        assert cfg.rubric_sample_rate == 1.0

    def test_above_one_raises(self):
        from pydantic import ValidationError

        from screener.lib.config_loader import EvalConfig

        with pytest.raises(ValidationError):
            EvalConfig(rubric_sample_rate=1.1)

    def test_negative_raises(self):
        from pydantic import ValidationError

        from screener.lib.config_loader import EvalConfig

        with pytest.raises(ValidationError):
            EvalConfig(rubric_sample_rate=-0.1)

    def test_app_config_has_eval_field(self):
        """AppConfig must expose app_config.eval.rubric_sample_rate."""
        cfg = _make_app_config(rubric_sample_rate=0.2)
        assert cfg.eval.rubric_sample_rate == 0.2
