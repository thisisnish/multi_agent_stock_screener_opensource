"""
tests/storage/test_analysis_write.py — Tests for P1-BUG-10 fix: analysis/
collection write path (debate cache).

Covers:
- schema.ANALYSIS constant value
- schema.analysis_doc_id helper (ticker + month_id format)
- schema.analysis_doc_id upper-cases the ticker
- schema.AnalysisDoc fields (required + optional defaults)
- schema.AnalysisDoc created_at is a datetime
- schema.AnalysisDoc model_dump(mode='json') is Firestore-safe (no datetime objects)
- schema.AnalysisDoc full round-trip
- writer.build_analysis_doc returns a dict
- writer.build_analysis_doc extracts all Bull/Bear/Judge fields (Pydantic + dict inputs)
- writer.build_analysis_doc handles missing optional fields gracefully
- writer.build_analysis_doc output is Firestore-safe
- writer.write_analysis_doc calls dao.set exactly once
- writer.write_analysis_doc uses ANALYSIS collection and correct doc ID
- writer.write_analysis_doc passes correct payload
- writer.write_analysis_doc is idempotent (two calls do not raise)
- screener_job main() writes analysis/ doc per ticker when dry_run=False
- screener_job main() skips analysis/ write when dry_run=True
- screener_job main() skips debate when analysis doc already exists
- screener_job main() still populates results when analysis doc exists
- analysis/ write happens before picks/ write

No real GCP, yfinance, or LangGraph calls are made — FirestoreDAO.set/get,
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
# Schema: ANALYSIS constant
# ---------------------------------------------------------------------------


class TestAnalysisConstant:
    def test_analysis_constant_value(self):
        from screener.lib.storage.schema import ANALYSIS

        assert ANALYSIS == "analysis"


# ---------------------------------------------------------------------------
# Schema: analysis_doc_id helper
# ---------------------------------------------------------------------------


class TestAnalysisDocId:
    def test_format_ticker_and_month_id(self):
        from screener.lib.storage.schema import analysis_doc_id

        assert analysis_doc_id("AAPL", "2026-04") == "AAPL_2026-04"

    def test_upper_cases_ticker(self):
        from screener.lib.storage.schema import analysis_doc_id

        assert analysis_doc_id("aapl", "2026-04") == "AAPL_2026-04"

    def test_lower_ticker_round_trip(self):
        from screener.lib.storage.schema import analysis_doc_id

        assert analysis_doc_id("msft", "2026-12") == "MSFT_2026-12"

    def test_different_month(self):
        from screener.lib.storage.schema import analysis_doc_id

        assert analysis_doc_id("NVDA", "2025-01") == "NVDA_2025-01"

    def test_mixed_case_ticker(self):
        from screener.lib.storage.schema import analysis_doc_id

        assert analysis_doc_id("Goog", "2026-04") == "GOOG_2026-04"


# ---------------------------------------------------------------------------
# Schema: AnalysisDoc
# ---------------------------------------------------------------------------


class TestAnalysisDoc:
    def test_required_fields_set(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(ticker="AAPL", month_id="2026-04")
        assert doc.ticker == "AAPL"
        assert doc.month_id == "2026-04"

    def test_list_fields_default_empty(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(ticker="AAPL", month_id="2026-04")
        assert doc.bull_thesis == []
        assert doc.bull_catalysts == []
        assert doc.bear_thesis == []
        assert doc.bull_sources == []
        assert doc.bear_sources == []

    def test_judge_verdict_defaults_to_hold(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(ticker="AAPL", month_id="2026-04")
        assert doc.judge_verdict == "HOLD"

    def test_contested_truth_defaults_false(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(ticker="AAPL", month_id="2026-04")
        assert doc.contested_truth is False

    def test_optional_fields_default_none(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(ticker="AAPL", month_id="2026-04")
        assert doc.judge_confidence is None
        assert doc.bull_conviction is None
        assert doc.bear_conviction is None
        assert doc.decisive_factor is None
        assert doc.margin_of_victory is None
        assert doc.horizon is None

    def test_judge_reasoning_defaults_empty_string(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(ticker="AAPL", month_id="2026-04")
        assert doc.judge_reasoning == ""

    def test_created_at_is_datetime(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(ticker="AAPL", month_id="2026-04")
        assert isinstance(doc.created_at, datetime)

    def test_model_dump_json_no_datetime_objects(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(ticker="AAPL", month_id="2026-04")
        payload = doc.model_dump(mode="json")
        for k, v in payload.items():
            assert not hasattr(v, "isoformat"), f"non-serialisable value at key {k!r}"

    def test_model_dump_json_created_at_is_string(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(ticker="AAPL", month_id="2026-04")
        payload = doc.model_dump(mode="json")
        assert isinstance(payload["created_at"], str)

    def test_full_fields_round_trip(self):
        from screener.lib.storage.schema import AnalysisDoc

        doc = AnalysisDoc(
            ticker="MSFT",
            month_id="2026-04",
            bull_thesis=["Strong FCF", "Cloud growth"],
            bull_catalysts=["Azure expansion"],
            bear_thesis=["Macro risk"],
            bull_sources=["FCF", "Technical"],
            bear_sources=["Earnings"],
            judge_reasoning="Bull case prevails on FCF.",
            judge_verdict="BUY",
            judge_confidence=72.5,
            bull_conviction=80.0,
            bear_conviction=45.0,
            decisive_factor="FCF yield",
            margin_of_victory="DECISIVE",
            contested_truth=False,
            horizon="60d",
        )
        payload = doc.model_dump(mode="json")
        assert payload["ticker"] == "MSFT"
        assert payload["judge_verdict"] == "BUY"
        assert payload["bull_thesis"] == ["Strong FCF", "Cloud growth"]
        assert payload["judge_confidence"] == 72.5
        assert payload["margin_of_victory"] == "DECISIVE"
        assert payload["contested_truth"] is False


# ---------------------------------------------------------------------------
# Shared DebateState fixtures for writer tests
# ---------------------------------------------------------------------------

from screener.lib.models import BearCaseOutput, BullCaseOutput, JudgeOutput  # noqa: E402

_BULL_MODEL = BullCaseOutput(
    bull_arguments=["Strong FCF", "Cloud momentum"],
    key_catalysts=["Product launch Q3", "Buyback program"],
    signal_citations=["FCF", "Technical"],
)

_BEAR_MODEL = BearCaseOutput(
    bear_arguments=["Macro headwinds", "High valuation"],
    signal_citations=["Earnings"],
)

_JUDGE_MODEL = JudgeOutput(
    action="BUY",
    judge_self_confidence=75,
    horizon="60d",
    winning_side="BULL",
    margin_of_victory="DECISIVE",
    decisive_factor="FCF yield",
    rationale="Bull case wins on strong FCF generation.",
)

_STATE_WITH_MODELS = {
    "ticker": "AAPL",
    "month_id": "2026-04",
    "bull_output": _BULL_MODEL,
    "bear_output": _BEAR_MODEL,
    "judge_output": _JUDGE_MODEL,
    "final_action": "BUY",
    "confidence_score": 72.0,
    "bull_conviction": 80.0,
    "bear_conviction": 45.0,
    "contested_truth": False,
    "horizon": "60d",
}

_STATE_WITH_DICTS = {
    "ticker": "AAPL",
    "month_id": "2026-04",
    "bull_output": {
        "bull_arguments": ["Strong FCF", "Cloud momentum"],
        "key_catalysts": ["Product launch Q3", "Buyback program"],
        "signal_citations": ["FCF", "Technical"],
    },
    "bear_output": {
        "bear_arguments": ["Macro headwinds", "High valuation"],
        "signal_citations": ["Earnings"],
    },
    "judge_output": {
        "rationale": "Bull case wins on strong FCF generation.",
        "decisive_factor": "FCF yield",
        "margin_of_victory": "DECISIVE",
    },
    "final_action": "BUY",
    "confidence_score": 72.0,
    "bull_conviction": 80.0,
    "bear_conviction": 45.0,
    "contested_truth": False,
    "horizon": "60d",
}


# ---------------------------------------------------------------------------
# writer.build_analysis_doc
# ---------------------------------------------------------------------------


class TestBuildAnalysisDoc:
    def test_returns_dict(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert isinstance(result, dict)

    def test_extracts_bull_thesis_from_pydantic_model(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["bull_thesis"] == ["Strong FCF", "Cloud momentum"]

    def test_extracts_bull_thesis_from_dict(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_DICTS)
        assert result["bull_thesis"] == ["Strong FCF", "Cloud momentum"]

    def test_extracts_bull_catalysts(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["bull_catalysts"] == ["Product launch Q3", "Buyback program"]

    def test_extracts_bear_thesis_from_pydantic_model(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["bear_thesis"] == ["Macro headwinds", "High valuation"]

    def test_extracts_bull_sources_from_pydantic_model(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["bull_sources"] == ["FCF", "Technical"]

    def test_extracts_bear_sources_from_pydantic_model(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["bear_sources"] == ["Earnings"]

    def test_extracts_judge_reasoning_from_pydantic_model(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["judge_reasoning"] == "Bull case wins on strong FCF generation."

    def test_extracts_judge_reasoning_from_dict(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_DICTS)
        assert result["judge_reasoning"] == "Bull case wins on strong FCF generation."

    def test_extracts_judge_verdict_from_final_action(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["judge_verdict"] == "BUY"

    def test_extracts_judge_confidence(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["judge_confidence"] == 72.0

    def test_extracts_bull_conviction(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["bull_conviction"] == 80.0

    def test_extracts_bear_conviction(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["bear_conviction"] == 45.0

    def test_extracts_decisive_factor_from_pydantic_model(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["decisive_factor"] == "FCF yield"

    def test_extracts_margin_of_victory_from_pydantic_model(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["margin_of_victory"] == "DECISIVE"

    def test_extracts_contested_truth(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["contested_truth"] is False

    def test_contested_truth_defaults_false_when_missing(self):
        from screener.analysis.writer import build_analysis_doc

        state = {**_STATE_WITH_DICTS}
        state.pop("contested_truth", None)
        result = build_analysis_doc("AAPL", "2026-04", state)
        assert result["contested_truth"] is False

    def test_extracts_horizon(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        assert result["horizon"] == "60d"

    def test_sets_ticker_and_month_id(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("MSFT", "2026-05", _STATE_WITH_MODELS)
        assert result["ticker"] == "MSFT"
        assert result["month_id"] == "2026-05"

    def test_handles_missing_optional_fields_gracefully(self):
        from screener.analysis.writer import build_analysis_doc

        minimal_state: dict = {
            "ticker": "JPM",
            "month_id": "2026-04",
        }
        result = build_analysis_doc("JPM", "2026-04", minimal_state)
        assert result["bull_thesis"] == []
        assert result["bear_thesis"] == []
        assert result["judge_reasoning"] == ""
        assert result["judge_verdict"] == "HOLD"
        assert result["judge_confidence"] is None
        assert result["bull_conviction"] is None
        assert result["bear_conviction"] is None

    def test_output_is_firestore_safe(self):
        from screener.analysis.writer import build_analysis_doc

        result = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        # created_at must be serialised as ISO string, not a datetime object
        assert isinstance(result["created_at"], str)
        for k, v in result.items():
            assert not hasattr(v, "isoformat"), f"non-serialisable value at key {k!r}"


# ---------------------------------------------------------------------------
# writer.write_analysis_doc (async unit tests)
# ---------------------------------------------------------------------------


class TestWriteAnalysisDoc:
    def _mock_dao(self) -> MagicMock:
        dao = MagicMock()
        dao.set = AsyncMock(return_value=None)
        return dao

    def test_calls_dao_set_exactly_once(self):
        import asyncio

        from screener.analysis.writer import write_analysis_doc

        dao = self._mock_dao()
        asyncio.run(
            write_analysis_doc(
                dao=dao, ticker="AAPL", month_id="2026-04", state=_STATE_WITH_MODELS
            )
        )
        assert dao.set.call_count == 1

    def test_uses_analysis_collection(self):
        import asyncio

        from screener.analysis.writer import write_analysis_doc
        from screener.lib.storage.schema import ANALYSIS

        dao = self._mock_dao()
        asyncio.run(
            write_analysis_doc(
                dao=dao, ticker="AAPL", month_id="2026-04", state=_STATE_WITH_MODELS
            )
        )
        collection_used = dao.set.call_args[0][0]
        assert collection_used == ANALYSIS

    def test_uses_correct_doc_id(self):
        import asyncio

        from screener.analysis.writer import write_analysis_doc
        from screener.lib.storage.schema import analysis_doc_id

        dao = self._mock_dao()
        asyncio.run(
            write_analysis_doc(
                dao=dao, ticker="AAPL", month_id="2026-04", state=_STATE_WITH_MODELS
            )
        )
        doc_id_used = dao.set.call_args[0][1]
        assert doc_id_used == analysis_doc_id("AAPL", "2026-04")

    def test_passes_correct_payload(self):
        import asyncio

        from screener.analysis.writer import build_analysis_doc, write_analysis_doc

        dao = self._mock_dao()
        asyncio.run(
            write_analysis_doc(
                dao=dao, ticker="AAPL", month_id="2026-04", state=_STATE_WITH_MODELS
            )
        )
        payload = dao.set.call_args[0][2]
        expected = build_analysis_doc("AAPL", "2026-04", _STATE_WITH_MODELS)
        # Compare all keys except created_at (timestamp varies)
        for k in expected:
            if k == "created_at":
                continue
            assert payload[k] == expected[k], f"mismatch at key {k!r}"

    def test_is_idempotent(self):
        """Calling write_analysis_doc twice must not raise."""
        import asyncio

        from screener.analysis.writer import write_analysis_doc

        dao = self._mock_dao()
        asyncio.run(
            write_analysis_doc(
                dao=dao, ticker="AAPL", month_id="2026-04", state=_STATE_WITH_MODELS
            )
        )
        asyncio.run(
            write_analysis_doc(
                dao=dao, ticker="AAPL", month_id="2026-04", state=_STATE_WITH_MODELS
            )
        )
        assert dao.set.call_count == 2


# ---------------------------------------------------------------------------
# Integration: screener_job main() tests
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


def _mock_dao(get_return_value=None) -> MagicMock:
    dao = MagicMock()
    dao.set = AsyncMock(return_value=None)
    dao.get = AsyncMock(return_value=get_return_value)
    return dao


def _fake_app_config():
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


def _run_main(
    env: dict,
    tickers_content: bytes = _TICKERS_YAML,
    get_return_value=None,
) -> MagicMock:
    """Patch all external dependencies and invoke screener_job main().

    Returns the mock FirestoreDAO instance so callers can assert on .set/.get.
    ``get_return_value`` controls what dao.get returns (None = no cached doc,
    a dict = idempotency-skip path).
    """
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp_tickers:
        tmp_tickers.write(tickers_content)
        tmp_tickers.flush()

        mock_dao_instance = _mock_dao(get_return_value=get_return_value)

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


def _analysis_set_calls(mock_dao: MagicMock) -> list:
    from screener.lib.storage.schema import ANALYSIS

    return [call for call in mock_dao.set.call_args_list if call[0][0] == ANALYSIS]


class TestAnalysisWriteNonDryRun:
    """Verify analysis/ docs are written after debate when dry_run=False."""

    def test_analysis_collection_receives_one_doc_per_ticker(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        analysis_calls = _analysis_set_calls(mock_dao)
        # 2 tickers (AAPL, MSFT) → 2 analysis docs
        assert len(analysis_calls) == 2

    def test_analysis_doc_ids_use_ticker_month_format(self):
        from screener.lib.storage.schema import analysis_doc_id

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        analysis_calls = _analysis_set_calls(mock_dao)
        doc_ids = {c[0][1] for c in analysis_calls}
        assert analysis_doc_id("AAPL", "2026-05") in doc_ids
        assert analysis_doc_id("MSFT", "2026-05") in doc_ids

    def test_analysis_payload_contains_judge_verdict(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        analysis_calls = _analysis_set_calls(mock_dao)
        for c in analysis_calls:
            payload = c[0][2]
            assert payload["judge_verdict"] == "BUY"

    def test_analysis_payload_contains_month_id(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        analysis_calls = _analysis_set_calls(mock_dao)
        for c in analysis_calls:
            payload = c[0][2]
            assert payload["month_id"] == "2026-05"

    def test_analysis_written_before_picks(self):
        """analysis/ write must come before picks/ write."""
        from screener.lib.storage.schema import ANALYSIS, PICKS

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        all_calls = mock_dao.set.call_args_list
        collections_in_order = [c[0][0] for c in all_calls]

        try:
            first_analysis = next(
                i for i, c in enumerate(collections_in_order) if c == ANALYSIS
            )
            first_picks = next(
                i for i, c in enumerate(collections_in_order) if c == PICKS
            )
        except StopIteration:
            raise AssertionError(
                f"Expected both '{ANALYSIS}' and '{PICKS}' writes; "
                f"got collections: {collections_in_order}"
            )

        assert first_analysis < first_picks, (
            f"analysis/ write (index {first_analysis}) must precede "
            f"picks/ write (index {first_picks})"
        )


class TestAnalysisWriteDryRun:
    """Verify analysis/ write is skipped when dry_run=True."""

    def test_no_analysis_writes_in_dry_run(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "true"})
        analysis_calls = _analysis_set_calls(mock_dao)
        assert len(analysis_calls) == 0

    def test_no_set_calls_at_all_in_dry_run(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "true"})
        mock_dao.set.assert_not_called()

    def test_dry_run_via_1_flag_no_analysis_writes(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "1"})
        analysis_calls = _analysis_set_calls(mock_dao)
        assert len(analysis_calls) == 0


class TestAnalysisIdempotencyGate:
    """Verify debate is skipped and results still populated when doc already exists."""

    _CACHED_DOC = {
        "ticker": "AAPL",
        "month_id": "2026-05",
        "judge_verdict": "SELL",
        "judge_confidence": 65.0,
        "margin_of_victory": "NARROW",
        "decisive_factor": "Valuation risk",
        "bull_thesis": ["Strong FCF"],
        "bear_thesis": ["High PE"],
    }

    def test_debate_skipped_when_analysis_doc_exists(self):
        """When dao.get returns a cached doc, build_debate_graph.ainvoke is not called."""
        mock_dao = _run_main(
            {"MONTH_ID": "2026-05", "DRY_RUN": "false"},
            get_return_value=self._CACHED_DOC,
        )
        # No analysis writes because debate was skipped and we already have the doc
        analysis_calls = _analysis_set_calls(mock_dao)
        assert len(analysis_calls) == 0

    def test_results_still_populated_from_cached_doc(self):
        """When analysis doc exists, results list must still be populated so
        picks/ and email steps have data."""
        from screener.lib.storage.schema import PICKS

        mock_dao = _run_main(
            {"MONTH_ID": "2026-05", "DRY_RUN": "false"},
            get_return_value=self._CACHED_DOC,
        )
        # Picks must still be written (results reconstructed from cached doc)
        picks_calls = [c for c in mock_dao.set.call_args_list if c[0][0] == PICKS]
        assert len(picks_calls) == 2  # AAPL + MSFT

    def test_reconstructed_verdict_uses_cached_judge_verdict(self):
        """Picks/ payload must use judge_verdict from cached doc, not a hardcoded default."""
        from screener.lib.storage.schema import PICKS

        mock_dao = _run_main(
            {"MONTH_ID": "2026-05", "DRY_RUN": "false"},
            get_return_value=self._CACHED_DOC,
        )
        picks_calls = [c for c in mock_dao.set.call_args_list if c[0][0] == PICKS]
        # Both AAPL and MSFT get the same cached verdict since get always returns _CACHED_DOC
        for c in picks_calls:
            payload = c[0][2]
            # The picks payload uses final_action (from reconstructed DebateState dict)
            assert payload.get("final_action") == "SELL"
