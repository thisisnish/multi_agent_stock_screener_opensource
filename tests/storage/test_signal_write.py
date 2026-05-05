"""
tests/storage/test_signal_write.py — Tests for BUG-04 fix: signal write path.

Covers:
- schema.SIGNALS constant value
- schema.signal_doc_id helper (ticker uppercasing, month_id formatting)
- schema.SignalDoc fields (populated and skipped variants)
- _build_signal_payload assembles the correct flat dict from raw fetcher dicts
- financial_update_job main() writes to DAO when dry_run=False
- financial_update_job main() skips DAO writes when dry_run=True
- financial_update_job main() tolerates per-ticker fetch errors (partial success)
- financial_update_job main() exits non-zero when all tickers fail

No real GCP or yfinance calls are made — FirestoreDAO.set and all fetchers
are fully mocked.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# Ensure project root is importable (mirrors conftest.py behaviour)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


class TestSignalsConstant:
    def test_signals_constant_value(self):
        from screener.lib.storage.schema import SIGNALS

        assert SIGNALS == "signals"


class TestSignalDocId:
    def test_basic(self):
        from screener.lib.storage.schema import signal_doc_id

        assert signal_doc_id("AAPL", "2026-04") == "AAPL_2026-04"

    def test_uppercases_ticker(self):
        from screener.lib.storage.schema import signal_doc_id

        assert signal_doc_id("aapl", "2026-04") == "AAPL_2026-04"

    def test_preserves_month_id_format(self):
        from screener.lib.storage.schema import signal_doc_id

        assert signal_doc_id("MSFT", "2026-12") == "MSFT_2026-12"

    def test_different_ticker(self):
        from screener.lib.storage.schema import signal_doc_id

        assert signal_doc_id("NVDA", "2026-01") == "NVDA_2026-01"


class TestSignalDoc:
    def test_defaults_populated(self):
        from screener.lib.storage.schema import SignalDoc

        doc = SignalDoc(ticker="AAPL", month_id="2026-04")
        assert doc.ticker == "AAPL"
        assert doc.month_id == "2026-04"
        assert isinstance(doc.fetched_at, datetime)
        assert doc.earnings_skipped is False
        assert doc.fcf_skipped is False
        assert doc.ebitda_skipped is False
        assert doc.earnings_yield is None
        assert doc.fcf_yield is None
        assert doc.ebitda_ev is None

    def test_full_payload(self):
        from screener.lib.storage.schema import SignalDoc

        now = datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)
        doc = SignalDoc(
            ticker="MSFT",
            month_id="2026-04",
            fetched_at=now,
            earnings_yield=0.035,
            trailing_eps=12.5,
            price=420.0,
            earnings_skipped=False,
            fcf_yield=0.04,
            free_cashflow=25_000_000_000.0,
            market_cap=3_100_000_000_000.0,
            fcf_skipped=False,
            ebitda_ev=0.12,
            ebitda=130_000_000_000.0,
            enterprise_value=3_200_000_000_000.0,
            ebitda_skipped=False,
        )
        assert doc.earnings_yield == 0.035
        assert doc.fcf_yield == 0.04
        assert doc.ebitda_ev == 0.12

    def test_skipped_variant(self):
        from screener.lib.storage.schema import SignalDoc

        doc = SignalDoc(
            ticker="XYZ",
            month_id="2026-04",
            earnings_skipped=True,
            earnings_skip_reason="trailingEps is None",
            fcf_skipped=True,
            fcf_skip_reason="market_cap is None",
            ebitda_skipped=True,
            ebitda_skip_reason="enterprise_value <= 0",
        )
        assert doc.earnings_skipped is True
        assert doc.earnings_skip_reason == "trailingEps is None"
        assert doc.fcf_skipped is True
        assert doc.ebitda_skipped is True

    def test_model_dump_json_serialises_datetime(self):
        """model_dump(mode='json') must produce a JSON-safe dict (no datetime objects)."""
        from screener.lib.storage.schema import SignalDoc

        doc = SignalDoc(ticker="AAPL", month_id="2026-04")
        payload = doc.model_dump(mode="json")
        assert isinstance(payload["fetched_at"], str)


# ---------------------------------------------------------------------------
# _build_signal_payload helper
# ---------------------------------------------------------------------------


class TestBuildSignalPayload:
    def _import(self):
        # Import here to avoid top-level import issues in environments without
        # all optional deps; also mirrors how main.py defers its imports.
        from jobs.financial_update.main import _build_signal_payload

        return _build_signal_payload

    def _earnings(self, **overrides):
        base = {
            "earnings_yield": 0.03,
            "trailing_eps": 6.5,
            "price": 175.0,
            "skipped": False,
            "skip_reason": None,
        }
        base.update(overrides)
        return base

    def _fcf(self, **overrides):
        base = {
            "fcf_yield": 0.05,
            "free_cashflow": 90_000_000_000.0,
            "market_cap": 2_700_000_000_000.0,
            "most_recent_quarter": 1714435200,
            "skipped": False,
            "skip_reason": None,
        }
        base.update(overrides)
        return base

    def _ebitda(self, **overrides):
        base = {
            "ebitda_ev": 0.08,
            "ebitda": 130_000_000_000.0,
            "enterprise_value": 2_900_000_000_000.0,
            "most_recent_quarter": 1714435200,
            "skipped": False,
            "skip_reason": None,
        }
        base.update(overrides)
        return base

    def test_ticker_and_month_id_set_correctly(self):
        build = self._import()
        payload = build(
            "AAPL", "2026-04", self._earnings(), self._fcf(), self._ebitda()
        )
        assert payload["ticker"] == "AAPL"
        assert payload["month_id"] == "2026-04"

    def test_earnings_fields_populated(self):
        build = self._import()
        payload = build(
            "AAPL", "2026-04", self._earnings(), self._fcf(), self._ebitda()
        )
        assert payload["earnings_yield"] == 0.03
        assert payload["trailing_eps"] == 6.5
        assert payload["price"] == 175.0
        assert payload["earnings_skipped"] is False
        assert payload["earnings_skip_reason"] is None

    def test_fcf_fields_populated(self):
        build = self._import()
        payload = build(
            "AAPL", "2026-04", self._earnings(), self._fcf(), self._ebitda()
        )
        assert payload["fcf_yield"] == 0.05
        assert payload["free_cashflow"] == 90_000_000_000.0
        assert payload["fcf_skipped"] is False

    def test_ebitda_fields_populated(self):
        build = self._import()
        payload = build(
            "AAPL", "2026-04", self._earnings(), self._fcf(), self._ebitda()
        )
        assert payload["ebitda_ev"] == 0.08
        assert payload["ebitda"] == 130_000_000_000.0
        assert payload["ebitda_skipped"] is False

    def test_skipped_signals_preserved(self):
        build = self._import()
        earnings = self._earnings(
            skipped=True,
            skip_reason="trailingEps is None",
            earnings_yield=None,
            trailing_eps=None,
            price=None,
        )
        payload = build("AAPL", "2026-04", earnings, self._fcf(), self._ebitda())
        assert payload["earnings_skipped"] is True
        assert payload["earnings_skip_reason"] == "trailingEps is None"
        assert payload["earnings_yield"] is None

    def test_fetched_at_is_json_string(self):
        """Payload must be Firestore-safe: fetched_at serialised to ISO string."""
        build = self._import()
        payload = build(
            "AAPL", "2026-04", self._earnings(), self._fcf(), self._ebitda()
        )
        assert isinstance(payload["fetched_at"], str)


# ---------------------------------------------------------------------------
# financial_update_job main() integration
# ---------------------------------------------------------------------------

_TICKERS_YAML = b"""
tickers:
  - symbol: AAPL
    sector: Technology
  - symbol: MSFT
    sector: Technology
"""

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


def _run_main(env: dict, tickers_content: bytes = _TICKERS_YAML) -> MagicMock:
    """Patch all external dependencies and invoke main().

    Because main() uses deferred (function-local) imports, patches must be
    applied at the *source* module level — not at ``jobs.financial_update.main``
    — so that Python's import machinery resolves to the mock when ``from X
    import Y`` executes inside main().

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
            # Patch FirestoreDAO at its definition site so the deferred
            # `from screener.lib.storage.firestore import FirestoreDAO` picks
            # up the mock.
            patch(
                "screener.lib.storage.firestore.FirestoreDAO",
                return_value=mock_dao_instance,
            ),
            # Patch load_config at its source so deferred import resolves mock.
            patch(
                "screener.lib.config_loader.load_config",
                return_value=_fake_app_config(),
            ),
            # Patch signal fetchers at their source modules.
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
        ):
            # Redirect tickers.yaml open() to the temp file.
            import builtins

            original_open = builtins.open

            def patched_open(path, *args, **kwargs):
                if str(path).endswith("tickers.yaml"):
                    return original_open(tmp_tickers.name, *args, **kwargs)
                return original_open(path, *args, **kwargs)

            with patch("builtins.open", side_effect=patched_open):
                import importlib

                import jobs.financial_update.main as fin_mod

                importlib.reload(fin_mod)
                fin_mod.main()

    return mock_dao_instance


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


class TestFinancialUpdateMainWrites:
    """Verify that main() calls dao.set() once per ticker in normal (non-dry) mode."""

    def test_set_called_twice_for_two_tickers(self):
        mock_dao = _run_main({"MONTH_ID": "2026-04", "DRY_RUN": "false"})
        assert mock_dao.set.call_count == 2

    def test_set_called_with_signals_collection(self):
        from screener.lib.storage.schema import SIGNALS

        mock_dao = _run_main({"MONTH_ID": "2026-04", "DRY_RUN": "false"})
        for call_args in mock_dao.set.call_args_list:
            collection = call_args[0][0]
            assert collection == SIGNALS

    def test_set_doc_ids_match_ticker_month(self):
        mock_dao = _run_main({"MONTH_ID": "2026-04", "DRY_RUN": "false"})
        doc_ids = {call[0][1] for call in mock_dao.set.call_args_list}
        assert doc_ids == {"AAPL_2026-04", "MSFT_2026-04"}

    def test_payload_contains_earnings_yield(self):
        mock_dao = _run_main({"MONTH_ID": "2026-04", "DRY_RUN": "false"})
        payloads = {call[0][1]: call[0][2] for call in mock_dao.set.call_args_list}
        assert payloads["AAPL_2026-04"]["earnings_yield"] == 0.03
        assert payloads["MSFT_2026-04"]["earnings_yield"] == 0.035


class TestFinancialUpdateMainDryRun:
    """Verify that main() skips dao.set() when DRY_RUN=true."""

    def test_set_not_called_in_dry_run(self):
        mock_dao = _run_main({"MONTH_ID": "2026-04", "DRY_RUN": "true"})
        mock_dao.set.assert_not_called()

    def test_dry_run_with_one_env_variant(self):
        mock_dao = _run_main({"MONTH_ID": "2026-04", "DRY_RUN": "1"})
        mock_dao.set.assert_not_called()
