"""
tests/storage/test_ticker_write.py — Tests for P1-BUG-06 fix: tickers/ master
collection write path.

Covers:
- schema.TICKERS constant value
- schema.ticker_doc_id helper (uppercasing, passthrough)
- schema.TickerSignalDoc fields (full and optional-defaults variant)
- schema.TickerSignalDoc.active defaults to True
- schema.TickerSignalDoc.latest_screening_date (not screening_date)
- screener_job main() writes tickers/ for every scored ticker when dry_run=False
- screener_job main() skips tickers/ writes when dry_run=True
- ticker docs written before picks docs (write ordering)
- doc payload contains composite_score and sector fields

No real GCP, yfinance, or LangGraph calls are made — FirestoreDAO.set,
build_debate_graph, and all signal fetchers are fully mocked.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# Ensure project root is importable (mirrors conftest.py behaviour)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


class TestTickersConstant:
    def test_tickers_constant_value(self):
        from screener.lib.storage.schema import TICKERS

        assert TICKERS == "tickers"


class TestTickerDocId:
    def test_uppercase_passthrough(self):
        from screener.lib.storage.schema import ticker_doc_id

        assert ticker_doc_id("AAPL") == "AAPL"

    def test_lowercase_upcased(self):
        from screener.lib.storage.schema import ticker_doc_id

        assert ticker_doc_id("aapl") == "AAPL"

    def test_mixed_case(self):
        from screener.lib.storage.schema import ticker_doc_id

        assert ticker_doc_id("Msft") == "MSFT"

    def test_dot_in_symbol_preserved(self):
        """Symbols like BRK.B are valid Firestore doc IDs and must not be slugged."""
        from screener.lib.storage.schema import ticker_doc_id

        assert ticker_doc_id("brk.b") == "BRK.B"


class TestTickerSignalDoc:
    def test_required_fields_set(self):
        from screener.lib.storage.schema import TickerSignalDoc

        doc = TickerSignalDoc(
            symbol="AAPL",
            latest_screening_date="2026-05-05",
            technical=62.3,
            earnings=55.1,
            fcf=70.4,
            ebitda=48.9,
            composite_score=60.0,
            sector="Technology",
        )
        assert doc.symbol == "AAPL"
        assert doc.latest_screening_date == "2026-05-05"
        assert doc.composite_score == 60.0
        assert doc.sector == "Technology"

    def test_active_defaults_true(self):
        from screener.lib.storage.schema import TickerSignalDoc

        doc = TickerSignalDoc(
            symbol="MSFT",
            latest_screening_date="2026-05-05",
            technical=55.0,
            earnings=60.0,
            fcf=65.0,
            ebitda=50.0,
            composite_score=58.0,
            sector="Technology",
        )
        assert doc.active is True

    def test_optional_price_and_above_ma200_default_none(self):
        from screener.lib.storage.schema import TickerSignalDoc

        doc = TickerSignalDoc(
            symbol="NVDA",
            latest_screening_date="2026-05-05",
            technical=80.0,
            earnings=40.0,
            fcf=50.0,
            ebitda=55.0,
            composite_score=61.0,
            sector="Technology",
        )
        assert doc.price is None
        assert doc.above_ma200 is None

    def test_full_payload_round_trips(self):
        from screener.lib.storage.schema import TickerSignalDoc

        doc = TickerSignalDoc(
            symbol="GOOGL",
            latest_screening_date="2026-05-05",
            technical=71.5,
            earnings=58.2,
            fcf=66.3,
            ebitda=52.8,
            composite_score=63.4,
            sector="Communication Services",
            price=175.20,
            above_ma200=True,
            active=True,
        )
        payload = doc.model_dump(mode="json")
        assert payload["symbol"] == "GOOGL"
        assert payload["composite_score"] == 63.4
        assert payload["above_ma200"] is True
        assert payload["active"] is True
        assert payload["price"] == 175.20

    def test_model_dump_no_datetime_objects(self):
        """model_dump(mode='json') must return a Firestore-safe dict."""
        from screener.lib.storage.schema import TickerSignalDoc

        doc = TickerSignalDoc(
            symbol="META",
            latest_screening_date="2026-05-05",
            technical=68.0,
            earnings=52.0,
            fcf=60.0,
            ebitda=48.0,
            composite_score=57.5,
            sector="Communication Services",
        )
        payload = doc.model_dump(mode="json")
        # All values must be JSON-native types
        for v in payload.values():
            assert not hasattr(v, "isoformat"), f"non-serialisable value found: {v!r}"

    def test_latest_screening_date_not_screening_date(self):
        """Ensure old field name 'screening_date' was replaced by 'latest_screening_date'."""
        from screener.lib.storage.schema import TickerSignalDoc

        doc = TickerSignalDoc(
            symbol="AAPL",
            latest_screening_date="2026-05-05",
            technical=60.0,
            earnings=55.0,
            fcf=65.0,
            ebitda=50.0,
            composite_score=59.0,
            sector="Technology",
        )
        payload = doc.model_dump()
        assert "latest_screening_date" in payload
        assert "screening_date" not in payload


# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

_TICKERS_YAML = b"""
tickers:
  - symbol: AAPL
    sector: Technology
  - symbol: MSFT
    sector: Technology
"""

# Minimal technical signal dicts (one per ticker)
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


class TestTickerWriteNonDryRun:
    """Verify tickers/ docs are written for every scored ticker when dry_run=False."""

    def _ticker_set_calls(self, mock_dao: MagicMock) -> list:
        """Return only the dao.set() calls targeting the tickers/ collection."""
        from screener.lib.storage.schema import TICKERS

        return [call for call in mock_dao.set.call_args_list if call[0][0] == TICKERS]

    def test_tickers_collection_receives_two_docs(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        ticker_calls = self._ticker_set_calls(mock_dao)
        assert len(ticker_calls) == 2

    def test_doc_ids_are_uppercase_symbols(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        ticker_calls = self._ticker_set_calls(mock_dao)
        doc_ids = {call[0][1] for call in ticker_calls}
        assert doc_ids == {"AAPL", "MSFT"}

    def test_payload_contains_composite_score(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        ticker_calls = self._ticker_set_calls(mock_dao)
        payloads = {call[0][1]: call[0][2] for call in ticker_calls}
        for sym, payload in payloads.items():
            assert "composite_score" in payload, f"composite_score missing for {sym}"
            assert isinstance(payload["composite_score"], float)

    def test_payload_contains_sector(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        ticker_calls = self._ticker_set_calls(mock_dao)
        payloads = {call[0][1]: call[0][2] for call in ticker_calls}
        for sym, payload in payloads.items():
            assert payload["sector"] == "Technology", f"sector wrong for {sym}"

    def test_payload_contains_active_flag(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        ticker_calls = self._ticker_set_calls(mock_dao)
        payloads = {call[0][1]: call[0][2] for call in ticker_calls}
        for sym, payload in payloads.items():
            assert payload.get("active") is True, f"active flag missing/wrong for {sym}"

    def test_payload_contains_latest_screening_date(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        ticker_calls = self._ticker_set_calls(mock_dao)
        payloads = {call[0][1]: call[0][2] for call in ticker_calls}
        for sym, payload in payloads.items():
            assert "latest_screening_date" in payload, (
                f"latest_screening_date missing for {sym}"
            )

    def test_tickers_written_before_picks(self):
        """tickers/ writes must precede picks/ writes in call order."""
        from screener.lib.storage.schema import PICKS, TICKERS

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        all_calls = mock_dao.set.call_args_list
        collections_in_order = [call[0][0] for call in all_calls]

        # Find first occurrence of each collection
        try:
            first_tickers = next(
                i for i, c in enumerate(collections_in_order) if c == TICKERS
            )
            first_picks = next(
                i for i, c in enumerate(collections_in_order) if c == PICKS
            )
        except StopIteration:
            raise AssertionError(
                f"Expected both '{TICKERS}' and '{PICKS}' writes; "
                f"got collections: {collections_in_order}"
            )

        assert first_tickers < first_picks, (
            f"tickers/ write (index {first_tickers}) must precede "
            f"picks/ write (index {first_picks})"
        )

    def test_total_set_calls_includes_tickers_and_picks(self):
        """Two tickers + two pick ledger entries = 4 total dao.set() calls."""
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        assert mock_dao.set.call_count == 4


class TestTickerWriteDryRun:
    """Verify tickers/ writes are skipped when dry_run=True."""

    def test_no_set_calls_in_dry_run(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "true"})
        mock_dao.set.assert_not_called()

    def test_dry_run_via_1_flag(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "1"})
        mock_dao.set.assert_not_called()
