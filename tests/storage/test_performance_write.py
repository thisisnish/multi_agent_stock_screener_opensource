"""
tests/storage/test_performance_write.py — Tests for P1-BUG-07 fix: performance/
collection write path.

Covers:
- schema.PERFORMANCE constant value
- schema.performance_doc_id helper (month_id + source formatting)
- schema.PerformanceSnapshotDoc fields (required and optional defaults)
- schema.PickLedgerDoc gains alpha_pct field
- tracker.fetch_spy_price returns float on success, None on yfinance failure
- tracker.build_pick_ledger_entries produces one entry per verdict with correct fields
- tracker.build_pick_ledger_entries populates entry_price from picks list
- tracker.build_pick_ledger_entries sets status="active" for new entries
- tracker.build_performance_snapshot aggregates total/active/closed counts correctly
- tracker.build_performance_snapshot sets return metrics to None at entry time
- tracker.write_performance_docs writes N+1 docs (N ledger entries + 1 snapshot)
- tracker.write_performance_docs uses PERFORMANCE collection for all writes
- tracker.write_performance_docs snapshot doc ID matches performance_doc_id()
- tracker.write_performance_docs ledger doc IDs match pick_ledger_doc_id() pattern
- screener_job main() writes performance/ docs when dry_run=False
- screener_job main() skips performance/ writes when dry_run=True
- performance/ writes occur after picks/ writes

No real GCP, yfinance, or LangGraph calls are made — FirestoreDAO.set,
build_debate_graph, all signal fetchers, and fetch_spy_price are fully mocked.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# Ensure project root is importable (mirrors conftest.py behaviour)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


class TestPerformanceConstant:
    def test_performance_constant_value(self):
        from screener.lib.storage.schema import PERFORMANCE

        assert PERFORMANCE == "performance"


class TestPerformanceDocId:
    def test_basic_judge_source(self):
        from screener.lib.storage.schema import performance_doc_id

        assert performance_doc_id("2026-04") == "2026-04_judge"

    def test_default_source_is_judge(self):
        from screener.lib.storage.schema import performance_doc_id

        assert performance_doc_id("2026-04", "judge") == "2026-04_judge"

    def test_custom_source(self):
        from screener.lib.storage.schema import performance_doc_id

        assert performance_doc_id("2026-04", "bull") == "2026-04_bull"

    def test_different_month(self):
        from screener.lib.storage.schema import performance_doc_id

        assert performance_doc_id("2026-12", "judge") == "2026-12_judge"


class TestPerformanceSnapshotDoc:
    def test_required_fields_set(self):
        from screener.lib.storage.schema import PerformanceSnapshotDoc

        doc = PerformanceSnapshotDoc(
            month_id="2026-04",
            total_picks=10,
            active_picks=10,
            closed_picks=0,
        )
        assert doc.month_id == "2026-04"
        assert doc.total_picks == 10
        assert doc.active_picks == 10
        assert doc.closed_picks == 0

    def test_source_defaults_to_judge(self):
        from screener.lib.storage.schema import PerformanceSnapshotDoc

        doc = PerformanceSnapshotDoc(
            month_id="2026-04",
            total_picks=5,
            active_picks=5,
            closed_picks=0,
        )
        assert doc.source == "judge"

    def test_return_metrics_default_to_none(self):
        from screener.lib.storage.schema import PerformanceSnapshotDoc

        doc = PerformanceSnapshotDoc(
            month_id="2026-04",
            total_picks=3,
            active_picks=3,
            closed_picks=0,
        )
        assert doc.win_rate is None
        assert doc.avg_return_pct is None
        assert doc.avg_spy_return_pct is None
        assert doc.avg_alpha_pct is None
        assert doc.beats_spy_rate is None
        assert doc.entry_spy_price is None

    def test_entry_spy_price_can_be_set(self):
        from screener.lib.storage.schema import PerformanceSnapshotDoc

        doc = PerformanceSnapshotDoc(
            month_id="2026-04",
            total_picks=2,
            active_picks=2,
            closed_picks=0,
            entry_spy_price=523.45,
        )
        assert doc.entry_spy_price == 523.45

    def test_created_at_is_datetime(self):
        from screener.lib.storage.schema import PerformanceSnapshotDoc

        doc = PerformanceSnapshotDoc(
            month_id="2026-04",
            total_picks=1,
            active_picks=1,
            closed_picks=0,
        )
        assert isinstance(doc.created_at, datetime)

    def test_model_dump_json_is_firestore_safe(self):
        """model_dump(mode='json') must produce a dict with no datetime objects."""
        from screener.lib.storage.schema import PerformanceSnapshotDoc

        doc = PerformanceSnapshotDoc(
            month_id="2026-04",
            total_picks=2,
            active_picks=2,
            closed_picks=0,
            entry_spy_price=510.0,
        )
        payload = doc.model_dump(mode="json")
        assert isinstance(payload["created_at"], str)
        assert payload["month_id"] == "2026-04"
        assert payload["total_picks"] == 2


class TestPickLedgerDocAlphaPct:
    def test_alpha_pct_defaults_to_none(self):
        from screener.lib.storage.schema import PickLedgerDoc

        doc = PickLedgerDoc(
            ticker="AAPL",
            entry_month="2026-04",
        )
        assert doc.alpha_pct is None

    def test_alpha_pct_can_be_set(self):
        from screener.lib.storage.schema import PickLedgerDoc

        doc = PickLedgerDoc(
            ticker="AAPL",
            entry_month="2026-04",
            pick_return_pct=8.5,
            spy_return_pct=3.2,
            alpha_pct=5.3,
        )
        assert doc.alpha_pct == 5.3


# ---------------------------------------------------------------------------
# tracker unit tests
# ---------------------------------------------------------------------------


class TestFetchSpyPrice:
    def test_returns_float_on_success(self):
        from screener.performance.tracker import fetch_spy_price

        mock_hist = MagicMock()
        mock_hist.empty = False
        mock_hist.__getitem__ = lambda self, key: MagicMock(
            dropna=lambda: MagicMock(iloc=[-1], __getitem__=lambda s, i: 523.45)
        )

        # Build a proper pandas-like mock for hist["Close"].dropna().iloc[-1]
        import pandas as pd

        close_series = pd.Series([520.0, 521.5, 523.45])
        mock_hist2 = MagicMock()
        mock_hist2.empty = False
        mock_hist2.__getitem__ = MagicMock(return_value=close_series)

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = mock_hist2

        with patch("screener.performance.tracker.yf") as mock_yf:
            mock_yf.Ticker.return_value = mock_ticker
            result = fetch_spy_price()

        assert isinstance(result, float)
        assert result == 523.45

    def test_returns_none_on_empty_data(self):
        from screener.performance.tracker import fetch_spy_price

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = MagicMock(empty=True)

        with patch("screener.performance.tracker.yf") as mock_yf:
            mock_yf.Ticker.return_value = mock_ticker
            result = fetch_spy_price()

        assert result is None

    def test_returns_none_on_exception(self):
        from screener.performance.tracker import fetch_spy_price

        with patch("screener.performance.tracker.yf") as mock_yf:
            mock_yf.Ticker.side_effect = RuntimeError("network error")
            result = fetch_spy_price()

        assert result is None


class TestBuildPickLedgerEntries:
    _VERDICTS = [
        {"ticker": "AAPL", "final_action": "BUY", "confidence_score": 72.0},
        {"ticker": "MSFT", "final_action": "BUY", "confidence_score": 65.0},
    ]
    _PICKS = [
        {"symbol": "AAPL", "price": 175.0},
        {"symbol": "MSFT", "price": 420.0},
    ]

    def test_returns_one_entry_per_verdict(self):
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=self._VERDICTS,
            picks=self._PICKS,
            month_id="2026-04",
            entry_spy_price=523.0,
        )
        assert len(entries) == 2

    def test_entry_prices_match_picks(self):
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=self._VERDICTS,
            picks=self._PICKS,
            month_id="2026-04",
            entry_spy_price=523.0,
        )
        by_ticker = {e["ticker"]: e for e in entries}
        assert by_ticker["AAPL"]["entry_price"] == 175.0
        assert by_ticker["MSFT"]["entry_price"] == 420.0

    def test_entry_spy_price_propagated(self):
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=self._VERDICTS,
            picks=self._PICKS,
            month_id="2026-04",
            entry_spy_price=523.0,
        )
        for entry in entries:
            assert entry["entry_spy_price"] == 523.0

    def test_entry_spy_price_none_propagated(self):
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=self._VERDICTS,
            picks=self._PICKS,
            month_id="2026-04",
            entry_spy_price=None,
        )
        for entry in entries:
            assert entry["entry_spy_price"] is None

    def test_status_is_active(self):
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=self._VERDICTS,
            picks=self._PICKS,
            month_id="2026-04",
            entry_spy_price=520.0,
        )
        for entry in entries:
            assert entry["status"] == "active"

    def test_returns_not_yet_computed(self):
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=self._VERDICTS,
            picks=self._PICKS,
            month_id="2026-04",
            entry_spy_price=520.0,
        )
        for entry in entries:
            assert entry["pick_return_pct"] is None
            assert entry["spy_return_pct"] is None
            assert entry["alpha_pct"] is None
            assert entry["beat_spy"] is None

    def test_doc_id_key_present(self):
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=self._VERDICTS,
            picks=self._PICKS,
            month_id="2026-04",
            entry_spy_price=520.0,
        )
        for entry in entries:
            assert "_doc_id" in entry

    def test_doc_id_format_matches_pick_ledger_doc_id(self):
        from screener.lib.storage.schema import pick_ledger_doc_id
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=self._VERDICTS,
            picks=self._PICKS,
            month_id="2026-04",
            entry_spy_price=520.0,
        )
        by_ticker = {e["ticker"]: e for e in entries}
        assert by_ticker["AAPL"]["_doc_id"] == pick_ledger_doc_id(
            "AAPL", "2026-04", "judge"
        )
        assert by_ticker["MSFT"]["_doc_id"] == pick_ledger_doc_id(
            "MSFT", "2026-04", "judge"
        )

    def test_entry_month_field_set(self):
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=self._VERDICTS,
            picks=self._PICKS,
            month_id="2026-04",
            entry_spy_price=520.0,
        )
        for entry in entries:
            assert entry["entry_month"] == "2026-04"

    def test_empty_verdicts_returns_empty_list(self):
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=[],
            picks=[],
            month_id="2026-04",
            entry_spy_price=520.0,
        )
        assert entries == []

    def test_missing_price_in_picks_is_none(self):
        """Verdicts for tickers not in picks list get entry_price=None (graceful degrade)."""
        from screener.performance.tracker import build_pick_ledger_entries

        entries = build_pick_ledger_entries(
            verdicts=[
                {"ticker": "NVDA", "final_action": "BUY", "confidence_score": 80.0}
            ],
            picks=[],  # no picks — NVDA not in price lookup
            month_id="2026-04",
            entry_spy_price=520.0,
        )
        assert len(entries) == 1
        assert entries[0]["entry_price"] is None


class TestBuildPerformanceSnapshot:
    def _make_ledger_entries(self, n: int, status: str = "active") -> list[dict]:
        return [{"ticker": f"T{i}", "status": status} for i in range(n)]

    def test_total_picks_count(self):
        from screener.performance.tracker import build_performance_snapshot

        entries = self._make_ledger_entries(5)
        snap = build_performance_snapshot("2026-04", entries, entry_spy_price=520.0)
        assert snap["total_picks"] == 5

    def test_active_picks_count(self):
        from screener.performance.tracker import build_performance_snapshot

        entries = self._make_ledger_entries(5, status="active")
        snap = build_performance_snapshot("2026-04", entries, entry_spy_price=520.0)
        assert snap["active_picks"] == 5

    def test_closed_picks_count(self):
        from screener.performance.tracker import build_performance_snapshot

        entries = self._make_ledger_entries(3, status="closed")
        snap = build_performance_snapshot("2026-04", entries, entry_spy_price=None)
        assert snap["closed_picks"] == 3

    def test_mixed_status_counts(self):
        from screener.performance.tracker import build_performance_snapshot

        entries = self._make_ledger_entries(4, "active") + self._make_ledger_entries(
            2, "closed"
        )
        snap = build_performance_snapshot("2026-04", entries, entry_spy_price=510.0)
        assert snap["total_picks"] == 6
        assert snap["active_picks"] == 4
        assert snap["closed_picks"] == 2

    def test_return_metrics_none_at_entry(self):
        from screener.performance.tracker import build_performance_snapshot

        entries = self._make_ledger_entries(3)
        snap = build_performance_snapshot("2026-04", entries, entry_spy_price=520.0)
        assert snap["win_rate"] is None
        assert snap["avg_return_pct"] is None
        assert snap["avg_spy_return_pct"] is None
        assert snap["avg_alpha_pct"] is None
        assert snap["beats_spy_rate"] is None

    def test_entry_spy_price_included(self):
        from screener.performance.tracker import build_performance_snapshot

        entries = self._make_ledger_entries(2)
        snap = build_performance_snapshot("2026-04", entries, entry_spy_price=523.45)
        assert snap["entry_spy_price"] == 523.45

    def test_month_id_in_snapshot(self):
        from screener.performance.tracker import build_performance_snapshot

        entries = self._make_ledger_entries(2)
        snap = build_performance_snapshot("2026-04", entries, entry_spy_price=None)
        assert snap["month_id"] == "2026-04"

    def test_source_defaults_to_judge(self):
        from screener.performance.tracker import build_performance_snapshot

        entries = self._make_ledger_entries(2)
        snap = build_performance_snapshot("2026-04", entries, entry_spy_price=None)
        assert snap["source"] == "judge"

    def test_empty_verdicts_zero_counts(self):
        from screener.performance.tracker import build_performance_snapshot

        snap = build_performance_snapshot("2026-04", [], entry_spy_price=None)
        assert snap["total_picks"] == 0
        assert snap["active_picks"] == 0
        assert snap["closed_picks"] == 0


# ---------------------------------------------------------------------------
# Shared fixtures for screener_job integration tests
# ---------------------------------------------------------------------------

_TICKERS_YAML = b"""
tickers:
  - symbol: AAPL
    sector: Technology
  - symbol: MSFT
    sector: Technology
"""

_TECHNICAL = {
    "AAPL": {
        "score": 65.0,
        "rsi": 55.0,
        "price": 175.0,
        "ma50": 170.0,
        "ma200": 160.0,
        "skipped": False,
        "skip_reason": None,
    },
    "MSFT": {
        "score": 70.0,
        "rsi": 58.0,
        "price": 420.0,
        "ma50": 415.0,
        "ma200": 400.0,
        "skipped": False,
        "skip_reason": None,
    },
}

_EARNINGS = {
    "AAPL": {
        "earnings_yield": 0.03,
        "trailing_eps": 6.5,
        "price": 175.0,
        "skipped": False,
        "skip_reason": None,
    },
    "MSFT": {
        "earnings_yield": 0.035,
        "trailing_eps": 12.5,
        "price": 420.0,
        "skipped": False,
        "skip_reason": None,
    },
}

_FCF = {
    "AAPL": {
        "fcf_yield": 0.05,
        "free_cashflow": 9e10,
        "market_cap": 2.7e12,
        "most_recent_quarter": 0,
        "skipped": False,
        "skip_reason": None,
    },
    "MSFT": {
        "fcf_yield": 0.04,
        "free_cashflow": 2.5e10,
        "market_cap": 3.1e12,
        "most_recent_quarter": 0,
        "skipped": False,
        "skip_reason": None,
    },
}

_EBITDA = {
    "AAPL": {
        "ebitda_ev": 0.08,
        "ebitda": 1.3e11,
        "enterprise_value": 2.9e12,
        "most_recent_quarter": 0,
        "skipped": False,
        "skip_reason": None,
    },
    "MSFT": {
        "ebitda_ev": 0.12,
        "ebitda": 1.3e11,
        "enterprise_value": 3.2e12,
        "most_recent_quarter": 0,
        "skipped": False,
        "skip_reason": None,
    },
}


def _mock_dao() -> MagicMock:
    """Return a mock FirestoreDAO whose set() is an awaitable no-op."""
    dao = MagicMock()
    dao.set = AsyncMock(return_value=None)
    # Return None so the idempotency gate in _run_pipeline does not skip the debate.
    dao.get = AsyncMock(return_value=None)
    return dao


def _fake_app_config():
    """Minimal AppConfig stub that satisfies load_config callers in main()."""
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


def _fake_graph():
    """Return a mock debate graph whose ainvoke returns a minimal DebateState dict."""

    async def _ainvoke(state: dict):
        ticker = state["ticker"]
        return {
            "ticker": ticker,
            "month_id": state["month_id"],
            "final_action": "BUY",
            "confidence_score": 72.0,
            "judge_output": {
                "margin_of_victory": "DECISIVE",
                "decisive_factor": "FCF yield",
            },
        }

    graph = MagicMock()
    graph.ainvoke = _ainvoke
    return graph


def _run_main(env: dict, tickers_content: bytes = _TICKERS_YAML) -> MagicMock:
    """Patch all external dependencies and invoke screener_job main().

    Returns the mock FirestoreDAO instance so callers can assert on .set().
    """
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp_tickers:
        tmp_tickers.write(tickers_content)
        tmp_tickers.flush()

        mock_dao_instance = _mock_dao()

        with (
            patch.dict(os.environ, {**env, "GCS_CONFIG_BUCKET": ""}),
            patch(
                "screener.lib.storage.firestore.FirestoreDAO",
                return_value=mock_dao_instance,
            ),
            patch(
                "screener.lib.config_loader.load_config",
                return_value=_fake_app_config(),
            ),
            patch(
                "screener.metrics.technical.fetch_technical_signal",
                side_effect=lambda sym: _TECHNICAL[sym],
            ),
            patch(
                "screener.metrics.earnings_yield.fetch_earnings_yield",
                side_effect=lambda syms: {s: _EARNINGS[s] for s in syms},
            ),
            patch(
                "screener.metrics.fcf_yield.fetch_fcf_yield",
                side_effect=lambda syms: {s: _FCF[s] for s in syms},
            ),
            patch(
                "screener.metrics.ebitda_ev.fetch_ebitda_ev",
                side_effect=lambda syms: {s: _EBITDA[s] for s in syms},
            ),
            patch(
                "screener.agents.graph.build_debate_graph",
                return_value=_fake_graph(),
            ),
            patch("screener.lib.email_sender.send_email"),
            # Prevent real yfinance call for SPY price fetch.
            patch(
                "screener.performance.tracker.fetch_spy_price",
                return_value=523.45,
            ),
        ):
            import builtins

            original_open = builtins.open

            def patched_open(path, *args, **kwargs):
                if str(path).endswith("tickers.yaml"):
                    return original_open(tmp_tickers.name, *args, **kwargs)
                return original_open(path, *args, **kwargs)

            with patch("builtins.open", side_effect=patched_open):
                import importlib

                import jobs.screener.main as screener_mod

                importlib.reload(screener_mod)
                screener_mod.main()

    return mock_dao_instance


# ---------------------------------------------------------------------------
# screener_job integration tests
# ---------------------------------------------------------------------------


class TestPerformanceWriteNonDryRun:
    """Verify performance/ docs are written when dry_run=False."""

    def _perf_calls(self, mock_dao: MagicMock) -> list:
        from screener.lib.storage.schema import PERFORMANCE

        return [
            call for call in mock_dao.set.call_args_list if call[0][0] == PERFORMANCE
        ]

    def test_performance_collection_receives_writes(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        perf_calls = self._perf_calls(mock_dao)
        # 2 tickers => 2 pick ledger docs + 1 snapshot = 3 performance writes
        assert len(perf_calls) == 3

    def test_snapshot_doc_id_matches_performance_doc_id(self):
        from screener.lib.storage.schema import performance_doc_id

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        perf_calls = self._perf_calls(mock_dao)
        doc_ids = {call[0][1] for call in perf_calls}
        expected_snapshot_id = performance_doc_id("2026-05", "judge")
        assert expected_snapshot_id in doc_ids

    def test_ledger_doc_ids_contain_ticker_and_month(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        perf_calls = self._perf_calls(mock_dao)
        doc_ids = {call[0][1] for call in perf_calls}
        assert "AAPL_2026-05_judge" in doc_ids
        assert "MSFT_2026-05_judge" in doc_ids

    def test_snapshot_payload_has_total_picks(self):
        from screener.lib.storage.schema import performance_doc_id

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        perf_calls = self._perf_calls(mock_dao)
        snapshot_id = performance_doc_id("2026-05", "judge")
        snapshot_call = next(c for c in perf_calls if c[0][1] == snapshot_id)
        payload = snapshot_call[0][2]
        assert payload["total_picks"] == 2

    def test_snapshot_payload_has_active_picks(self):
        from screener.lib.storage.schema import performance_doc_id

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        perf_calls = self._perf_calls(mock_dao)
        snapshot_id = performance_doc_id("2026-05", "judge")
        snapshot_call = next(c for c in perf_calls if c[0][1] == snapshot_id)
        payload = snapshot_call[0][2]
        assert payload["active_picks"] == 2
        assert payload["closed_picks"] == 0

    def test_snapshot_payload_has_entry_spy_price(self):
        from screener.lib.storage.schema import performance_doc_id

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        perf_calls = self._perf_calls(mock_dao)
        snapshot_id = performance_doc_id("2026-05", "judge")
        snapshot_call = next(c for c in perf_calls if c[0][1] == snapshot_id)
        payload = snapshot_call[0][2]
        assert payload["entry_spy_price"] == 523.45

    def test_ledger_entry_has_entry_price(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        perf_calls = self._perf_calls(mock_dao)
        aapl_call = next(c for c in perf_calls if c[0][1] == "AAPL_2026-05_judge")
        payload = aapl_call[0][2]
        assert payload["entry_price"] == 175.0

    def test_ledger_entry_status_is_active(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        perf_calls = self._perf_calls(mock_dao)
        for call in perf_calls:
            doc_id = call[0][1]
            if doc_id != "2026-05_judge":  # skip snapshot
                assert call[0][2]["status"] == "active"


class TestPerformanceWriteDryRun:
    """Verify performance/ writes are skipped when dry_run=True."""

    def _perf_calls(self, mock_dao: MagicMock) -> list:
        from screener.lib.storage.schema import PERFORMANCE

        return [
            call for call in mock_dao.set.call_args_list if call[0][0] == PERFORMANCE
        ]

    def test_no_performance_writes_in_dry_run(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "true"})
        assert self._perf_calls(mock_dao) == []

    def test_no_performance_writes_with_1_flag(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "1"})
        assert self._perf_calls(mock_dao) == []


class TestPerformanceWriteOrdering:
    """Verify performance/ writes occur after picks/ writes."""

    def test_picks_written_before_performance(self):
        from screener.lib.storage.schema import PERFORMANCE, PICKS

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        all_calls = mock_dao.set.call_args_list
        collections = [call[0][0] for call in all_calls]

        first_picks = next(i for i, c in enumerate(collections) if c == PICKS)
        first_perf = next(i for i, c in enumerate(collections) if c == PERFORMANCE)

        assert first_picks < first_perf, (
            f"picks/ write (index {first_picks}) must precede "
            f"performance/ write (index {first_perf})"
        )

    def test_total_set_calls_includes_tickers_picks_performance(self):
        """2 tickers + 1 screenings + 2 analysis + 2 picks + 2 perf ledgers + 1 perf snapshot = 10 total."""
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        assert mock_dao.set.call_count == 10
