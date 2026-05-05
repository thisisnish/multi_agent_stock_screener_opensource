"""
tests/storage/test_screening_write.py — Tests for P1-BUG-09 fix: screenings/
collection write path.

Covers:
- schema.SCREENINGS constant value
- schema.screening_run_doc_id helper (month_id passthrough)
- schema.TickerScreeningEntry fields (required + optional defaults)
- schema.TickerScreeningEntry cap_filtered flag logic
- schema.ScreeningDoc fields (required + optional defaults)
- schema.ScreeningDoc timestamp is a datetime
- schema.ScreeningDoc model_dump(mode='json') is Firestore-safe (no datetime objects)
- writer.build_ticker_entries produces one entry per gated ticker
- writer.build_ticker_entries sets in_top_n_before_cap correctly
- writer.build_ticker_entries sets in_top_n_after_cap correctly
- writer.build_ticker_entries sets cap_filtered for tickers excluded by sector cap
- writer.build_ticker_entries propagates factor scores correctly
- writer.build_ticker_entries propagates ma200_multiplier from ma200_gate
- writer.build_screening_doc assembles top_n_before_cap and top_n_after_cap lists
- writer.build_screening_doc computes sector_distribution from final picks
- writer.build_screening_doc defaults signal_vintage_dates to today if not supplied
- writer.build_screening_doc accepts explicit signal_vintage_dates
- writer.write_screening_doc writes exactly one doc to SCREENINGS collection
- writer.write_screening_doc uses screening_run_doc_id as doc ID
- writer.write_screening_doc skips write when gated is empty
- screener_job main() writes screenings/ doc when dry_run=False
- screener_job main() skips screenings/ write when dry_run=True
- screenings/ write occurs after tickers/ write and before picks/ write

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
# Schema: SCREENINGS constant
# ---------------------------------------------------------------------------


class TestScreeningsConstant:
    def test_screenings_constant_value(self):
        from screener.lib.storage.schema import SCREENINGS

        assert SCREENINGS == "screenings"


# ---------------------------------------------------------------------------
# Schema: screening_run_doc_id helper
# ---------------------------------------------------------------------------


class TestScreeningRunDocId:
    def test_returns_month_id_unchanged(self):
        from screener.lib.storage.schema import screening_run_doc_id

        assert screening_run_doc_id("2026-04") == "2026-04"

    def test_different_month(self):
        from screener.lib.storage.schema import screening_run_doc_id

        assert screening_run_doc_id("2026-12") == "2026-12"

    def test_earliest_valid_month(self):
        from screener.lib.storage.schema import screening_run_doc_id

        assert screening_run_doc_id("2020-01") == "2020-01"


# ---------------------------------------------------------------------------
# Schema: TickerScreeningEntry
# ---------------------------------------------------------------------------


class TestTickerScreeningEntry:
    def test_required_fields_set(self):
        from screener.lib.storage.schema import TickerScreeningEntry

        entry = TickerScreeningEntry(
            symbol="AAPL",
            sector="Technology",
            composite_score=65.5,
        )
        assert entry.symbol == "AAPL"
        assert entry.sector == "Technology"
        assert entry.composite_score == 65.5

    def test_optional_factor_scores_default_none(self):
        from screener.lib.storage.schema import TickerScreeningEntry

        entry = TickerScreeningEntry(
            symbol="AAPL",
            sector="Technology",
            composite_score=65.5,
        )
        assert entry.technical is None
        assert entry.earnings is None
        assert entry.fcf is None
        assert entry.ebitda is None

    def test_flags_default_false(self):
        from screener.lib.storage.schema import TickerScreeningEntry

        entry = TickerScreeningEntry(
            symbol="AAPL",
            sector="Technology",
            composite_score=65.5,
        )
        assert entry.in_top_n_before_cap is False
        assert entry.in_top_n_after_cap is False
        assert entry.cap_filtered is False

    def test_ma200_multiplier_defaults_to_one(self):
        from screener.lib.storage.schema import TickerScreeningEntry

        entry = TickerScreeningEntry(
            symbol="AAPL",
            sector="Technology",
            composite_score=65.5,
        )
        assert entry.ma200_multiplier == 1.0

    def test_cap_filtered_can_be_true(self):
        from screener.lib.storage.schema import TickerScreeningEntry

        entry = TickerScreeningEntry(
            symbol="AAPL",
            sector="Technology",
            composite_score=65.5,
            in_top_n_before_cap=True,
            in_top_n_after_cap=False,
            cap_filtered=True,
        )
        assert entry.cap_filtered is True

    def test_full_entry_round_trips(self):
        from screener.lib.storage.schema import TickerScreeningEntry

        entry = TickerScreeningEntry(
            symbol="MSFT",
            sector="Technology",
            technical=70.1,
            earnings=55.3,
            fcf=62.8,
            ebitda=48.4,
            composite_score=61.7,
            ma200_multiplier=1.0,
            in_top_n_before_cap=True,
            in_top_n_after_cap=True,
            cap_filtered=False,
        )
        payload = entry.model_dump(mode="json")
        assert payload["symbol"] == "MSFT"
        assert payload["composite_score"] == 61.7
        assert payload["in_top_n_before_cap"] is True
        assert payload["in_top_n_after_cap"] is True
        assert payload["cap_filtered"] is False

    def test_model_dump_no_datetime_objects(self):
        from screener.lib.storage.schema import TickerScreeningEntry

        entry = TickerScreeningEntry(
            symbol="NVDA",
            sector="Technology",
            composite_score=80.0,
        )
        payload = entry.model_dump(mode="json")
        for v in payload.values():
            assert not hasattr(v, "isoformat"), f"non-serialisable value: {v!r}"


# ---------------------------------------------------------------------------
# Schema: ScreeningDoc
# ---------------------------------------------------------------------------


class TestScreeningDoc:
    def test_required_fields_set(self):
        from screener.lib.storage.schema import ScreeningDoc

        doc = ScreeningDoc(month_id="2026-04")
        assert doc.month_id == "2026-04"

    def test_timestamp_is_datetime(self):
        from screener.lib.storage.schema import ScreeningDoc

        doc = ScreeningDoc(month_id="2026-04")
        assert isinstance(doc.timestamp, datetime)

    def test_lists_default_empty(self):
        from screener.lib.storage.schema import ScreeningDoc

        doc = ScreeningDoc(month_id="2026-04")
        assert doc.all_signals == []
        assert doc.top_n_before_cap == []
        assert doc.top_n_after_cap == []

    def test_dicts_default_empty(self):
        from screener.lib.storage.schema import ScreeningDoc

        doc = ScreeningDoc(month_id="2026-04")
        assert doc.sector_distribution == {}
        assert doc.signal_vintage_dates == {}

    def test_model_dump_json_timestamp_is_string(self):
        from screener.lib.storage.schema import ScreeningDoc

        doc = ScreeningDoc(month_id="2026-04")
        payload = doc.model_dump(mode="json")
        assert isinstance(payload["timestamp"], str)

    def test_model_dump_json_no_datetime_objects(self):
        from screener.lib.storage.schema import ScreeningDoc

        doc = ScreeningDoc(
            month_id="2026-04",
            top_n_before_cap=["AAPL", "MSFT"],
            top_n_after_cap=["AAPL", "MSFT"],
            sector_distribution={"Technology": 2},
            signal_vintage_dates={"technical": "2026-04-30"},
        )
        payload = doc.model_dump(mode="json")
        assert payload["month_id"] == "2026-04"
        assert payload["top_n_before_cap"] == ["AAPL", "MSFT"]
        assert payload["sector_distribution"] == {"Technology": 2}
        # No datetime objects in top-level values
        for k, v in payload.items():
            assert not hasattr(v, "isoformat"), f"non-serialisable value at key {k!r}"

    def test_all_signals_accepts_entries(self):
        from screener.lib.storage.schema import ScreeningDoc, TickerScreeningEntry

        entries = [
            TickerScreeningEntry(
                symbol="AAPL", sector="Technology", composite_score=65.0
            ),
            TickerScreeningEntry(
                symbol="MSFT", sector="Technology", composite_score=62.0
            ),
        ]
        doc = ScreeningDoc(month_id="2026-04", all_signals=entries)
        assert len(doc.all_signals) == 2


# ---------------------------------------------------------------------------
# Shared test fixtures for writer tests
# ---------------------------------------------------------------------------

_GATED = [
    {
        "symbol": "AAPL",
        "sector": "Technology",
        "composite_score": 70.0,
        "ma200_gate": {"multiplier": 1.0},
    },
    {
        "symbol": "MSFT",
        "sector": "Technology",
        "composite_score": 65.0,
        "ma200_gate": {"multiplier": 1.0},
    },
    {
        "symbol": "JPM",
        "sector": "Financials",
        "composite_score": 60.0,
        "ma200_gate": {"multiplier": 1.0},
    },
    {
        "symbol": "GS",
        "sector": "Financials",
        "composite_score": 55.0,
        "ma200_gate": {"multiplier": 0.5},
    },
]

_PICKS = [
    {"symbol": "AAPL", "sector": "Technology", "composite_score": 70.0},
    {"symbol": "JPM", "sector": "Financials", "composite_score": 60.0},
    {"symbol": "GS", "sector": "Financials", "composite_score": 55.0},
]

_FACTOR_SCORES: dict = {
    "technical": {"AAPL": 72.0, "MSFT": 68.0, "JPM": 58.0, "GS": 50.0},
    "earnings": {"AAPL": 60.0, "MSFT": 62.0, "JPM": 55.0, "GS": 48.0},
    "fcf": {"AAPL": 75.0, "MSFT": 70.0, "JPM": None, "GS": 45.0},
    "ebitda": {"AAPL": 65.0, "MSFT": 60.0, "JPM": 52.0, "GS": None},
}

# top_n=3: AAPL, MSFT, JPM are "before cap"; MSFT is filtered by sector cap
# (Technology already has AAPL; max_per_sector=1 in this test scenario via picks)


# ---------------------------------------------------------------------------
# writer.build_ticker_entries
# ---------------------------------------------------------------------------


class TestBuildTickerEntries:
    def test_returns_one_entry_per_gated_ticker(self):
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        assert len(entries) == 4

    def test_entries_ordered_by_composite_score_descending(self):
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        scores = [e["composite_score"] for e in entries]
        assert scores == sorted(scores, reverse=True)

    def test_in_top_n_before_cap_set_for_top_3(self):
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        by_sym = {e["symbol"]: e for e in entries}
        # top-3 by score: AAPL(70), MSFT(65), JPM(60)
        assert by_sym["AAPL"]["in_top_n_before_cap"] is True
        assert by_sym["MSFT"]["in_top_n_before_cap"] is True
        assert by_sym["JPM"]["in_top_n_before_cap"] is True
        # GS(55) is rank 4 — not in before_cap
        assert by_sym["GS"]["in_top_n_before_cap"] is False

    def test_in_top_n_after_cap_matches_picks(self):
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        by_sym = {e["symbol"]: e for e in entries}
        # picks are AAPL, JPM, GS
        assert by_sym["AAPL"]["in_top_n_after_cap"] is True
        assert by_sym["JPM"]["in_top_n_after_cap"] is True
        assert by_sym["GS"]["in_top_n_after_cap"] is True
        assert by_sym["MSFT"]["in_top_n_after_cap"] is False

    def test_cap_filtered_for_msft(self):
        """MSFT was in top-N before cap but excluded by sector cap — cap_filtered=True."""
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        by_sym = {e["symbol"]: e for e in entries}
        assert by_sym["MSFT"]["cap_filtered"] is True

    def test_cap_filtered_false_for_picks(self):
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        by_sym = {e["symbol"]: e for e in entries}
        for sym in ["AAPL", "JPM", "GS"]:
            assert by_sym[sym]["cap_filtered"] is False, f"{sym} should not be filtered"

    def test_cap_filtered_false_for_below_top_n(self):
        """GS(rank 4) is not in before_cap, so cap_filtered must be False."""
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        by_sym = {e["symbol"]: e for e in entries}
        assert by_sym["GS"]["cap_filtered"] is False

    def test_factor_scores_propagated(self):
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        by_sym = {e["symbol"]: e for e in entries}
        assert by_sym["AAPL"]["technical"] == 72.0
        assert by_sym["AAPL"]["earnings"] == 60.0
        assert by_sym["AAPL"]["fcf"] == 75.0
        assert by_sym["AAPL"]["ebitda"] == 65.0

    def test_none_factor_scores_propagated(self):
        """None factor scores (skipped signals) must remain None in the entry."""
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        by_sym = {e["symbol"]: e for e in entries}
        assert by_sym["JPM"]["fcf"] is None
        assert by_sym["GS"]["ebitda"] is None

    def test_ma200_multiplier_propagated(self):
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        by_sym = {e["symbol"]: e for e in entries}
        # GS has multiplier=0.5 (below MA200)
        assert by_sym["GS"]["ma200_multiplier"] == 0.5
        # AAPL is above MA200
        assert by_sym["AAPL"]["ma200_multiplier"] == 1.0

    def test_missing_ma200_gate_defaults_to_1(self):
        """Entries without ma200_gate key should default to multiplier 1.0."""
        from screener.screening.writer import build_ticker_entries

        gated_no_gate = [
            {"symbol": "AAPL", "sector": "Technology", "composite_score": 70.0},
            {"symbol": "MSFT", "sector": "Technology", "composite_score": 65.0},
        ]
        picks = [{"symbol": "AAPL", "sector": "Technology", "composite_score": 70.0}]
        entries = build_ticker_entries(
            gated=gated_no_gate,
            picks=picks,
            factor_scores=_FACTOR_SCORES,
            top_n=1,
        )
        by_sym = {e["symbol"]: e for e in entries}
        assert by_sym["AAPL"]["ma200_multiplier"] == 1.0
        assert by_sym["MSFT"]["ma200_multiplier"] == 1.0

    def test_empty_gated_returns_empty_list(self):
        from screener.screening.writer import build_ticker_entries

        entries = build_ticker_entries(
            gated=[],
            picks=[],
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )
        assert entries == []


# ---------------------------------------------------------------------------
# writer.build_screening_doc
# ---------------------------------------------------------------------------


class TestBuildScreeningDoc:
    def _make_entries(self) -> list[dict]:
        from screener.screening.writer import build_ticker_entries

        return build_ticker_entries(
            gated=_GATED,
            picks=_PICKS,
            factor_scores=_FACTOR_SCORES,
            top_n=3,
        )

    def test_month_id_stored(self):
        from screener.screening.writer import build_screening_doc

        entries = self._make_entries()
        payload = build_screening_doc(
            month_id="2026-04",
            ticker_entries=entries,
            picks=_PICKS,
        )
        assert payload["month_id"] == "2026-04"

    def test_top_n_before_cap_contains_correct_symbols(self):
        from screener.screening.writer import build_screening_doc

        entries = self._make_entries()
        payload = build_screening_doc(
            month_id="2026-04",
            ticker_entries=entries,
            picks=_PICKS,
        )
        # top-3 by score: AAPL, MSFT, JPM
        assert set(payload["top_n_before_cap"]) == {"AAPL", "MSFT", "JPM"}

    def test_top_n_after_cap_contains_picks(self):
        from screener.screening.writer import build_screening_doc

        entries = self._make_entries()
        payload = build_screening_doc(
            month_id="2026-04",
            ticker_entries=entries,
            picks=_PICKS,
        )
        assert set(payload["top_n_after_cap"]) == {"AAPL", "JPM", "GS"}

    def test_sector_distribution_counts_final_picks(self):
        from screener.screening.writer import build_screening_doc

        entries = self._make_entries()
        payload = build_screening_doc(
            month_id="2026-04",
            ticker_entries=entries,
            picks=_PICKS,
        )
        # AAPL=Technology(1), JPM+GS=Financials(2)
        assert payload["sector_distribution"]["Technology"] == 1
        assert payload["sector_distribution"]["Financials"] == 2

    def test_signal_vintage_dates_defaults_to_today(self):
        from screener.screening.writer import build_screening_doc

        entries = self._make_entries()
        payload = build_screening_doc(
            month_id="2026-04",
            ticker_entries=entries,
            picks=_PICKS,
        )
        dates = payload["signal_vintage_dates"]
        assert set(dates.keys()) == {"technical", "earnings", "fcf", "ebitda"}
        for v in dates.values():
            assert isinstance(v, str)
            # ISO date format YYYY-MM-DD
            assert len(v) == 10

    def test_explicit_signal_vintage_dates_stored(self):
        from screener.screening.writer import build_screening_doc

        entries = self._make_entries()
        vintage = {"technical": "2026-04-25", "earnings": "2026-04-20"}
        payload = build_screening_doc(
            month_id="2026-04",
            ticker_entries=entries,
            picks=_PICKS,
            signal_vintage_dates=vintage,
        )
        assert payload["signal_vintage_dates"] == vintage

    def test_all_signals_length_matches_gated(self):
        from screener.screening.writer import build_screening_doc

        entries = self._make_entries()
        payload = build_screening_doc(
            month_id="2026-04",
            ticker_entries=entries,
            picks=_PICKS,
        )
        assert len(payload["all_signals"]) == len(_GATED)

    def test_payload_has_no_datetime_objects(self):
        from screener.screening.writer import build_screening_doc

        entries = self._make_entries()
        payload = build_screening_doc(
            month_id="2026-04",
            ticker_entries=entries,
            picks=_PICKS,
        )
        # timestamp must be serialised as ISO string
        assert isinstance(payload["timestamp"], str)

    def test_empty_picks_results_in_empty_sector_distribution(self):
        from screener.screening.writer import build_screening_doc

        payload = build_screening_doc(
            month_id="2026-04",
            ticker_entries=[],
            picks=[],
        )
        assert payload["sector_distribution"] == {}
        assert payload["top_n_after_cap"] == []


# ---------------------------------------------------------------------------
# writer.write_screening_doc (async unit tests)
# ---------------------------------------------------------------------------


class TestWriteScreeningDoc:
    def _mock_dao(self) -> MagicMock:
        dao = MagicMock()
        dao.set = AsyncMock(return_value=None)
        return dao

    def test_writes_exactly_one_doc(self):
        import asyncio

        from screener.screening.writer import write_screening_doc

        dao = self._mock_dao()
        asyncio.run(
            write_screening_doc(
                dao=dao,
                month_id="2026-04",
                gated=_GATED,
                picks=_PICKS,
                factor_scores=_FACTOR_SCORES,
                top_n=3,
            )
        )
        assert dao.set.call_count == 1

    def test_writes_to_screenings_collection(self):
        import asyncio

        from screener.lib.storage.schema import SCREENINGS
        from screener.screening.writer import write_screening_doc

        dao = self._mock_dao()
        asyncio.run(
            write_screening_doc(
                dao=dao,
                month_id="2026-04",
                gated=_GATED,
                picks=_PICKS,
                factor_scores=_FACTOR_SCORES,
                top_n=3,
            )
        )
        collection_used = dao.set.call_args[0][0]
        assert collection_used == SCREENINGS

    def test_doc_id_matches_screening_run_doc_id(self):
        import asyncio

        from screener.lib.storage.schema import screening_run_doc_id
        from screener.screening.writer import write_screening_doc

        dao = self._mock_dao()
        asyncio.run(
            write_screening_doc(
                dao=dao,
                month_id="2026-04",
                gated=_GATED,
                picks=_PICKS,
                factor_scores=_FACTOR_SCORES,
                top_n=3,
            )
        )
        doc_id_used = dao.set.call_args[0][1]
        assert doc_id_used == screening_run_doc_id("2026-04")

    def test_payload_contains_month_id(self):
        import asyncio

        from screener.screening.writer import write_screening_doc

        dao = self._mock_dao()
        asyncio.run(
            write_screening_doc(
                dao=dao,
                month_id="2026-04",
                gated=_GATED,
                picks=_PICKS,
                factor_scores=_FACTOR_SCORES,
                top_n=3,
            )
        )
        payload = dao.set.call_args[0][2]
        assert payload["month_id"] == "2026-04"

    def test_payload_all_signals_has_all_tickers(self):
        import asyncio

        from screener.screening.writer import write_screening_doc

        dao = self._mock_dao()
        asyncio.run(
            write_screening_doc(
                dao=dao,
                month_id="2026-04",
                gated=_GATED,
                picks=_PICKS,
                factor_scores=_FACTOR_SCORES,
                top_n=3,
            )
        )
        payload = dao.set.call_args[0][2]
        assert len(payload["all_signals"]) == len(_GATED)

    def test_payload_sector_distribution_correct(self):
        import asyncio

        from screener.screening.writer import write_screening_doc

        dao = self._mock_dao()
        asyncio.run(
            write_screening_doc(
                dao=dao,
                month_id="2026-04",
                gated=_GATED,
                picks=_PICKS,
                factor_scores=_FACTOR_SCORES,
                top_n=3,
            )
        )
        payload = dao.set.call_args[0][2]
        assert payload["sector_distribution"]["Technology"] == 1
        assert payload["sector_distribution"]["Financials"] == 2

    def test_skips_write_when_gated_empty(self):
        import asyncio

        from screener.screening.writer import write_screening_doc

        dao = self._mock_dao()
        asyncio.run(
            write_screening_doc(
                dao=dao,
                month_id="2026-04",
                gated=[],
                picks=[],
                factor_scores=_FACTOR_SCORES,
                top_n=3,
            )
        )
        dao.set.assert_not_called()

    def test_signal_vintage_dates_passed_through(self):
        import asyncio

        from screener.screening.writer import write_screening_doc

        dao = self._mock_dao()
        vintage = {
            "technical": "2026-04-28",
            "earnings": "2026-04-25",
            "fcf": "2026-04-20",
            "ebitda": "2026-04-20",
        }
        asyncio.run(
            write_screening_doc(
                dao=dao,
                month_id="2026-04",
                gated=_GATED,
                picks=_PICKS,
                factor_scores=_FACTOR_SCORES,
                top_n=3,
                signal_vintage_dates=vintage,
            )
        )
        payload = dao.set.call_args[0][2]
        assert payload["signal_vintage_dates"] == vintage


# ---------------------------------------------------------------------------
# screener_job integration tests
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
    dao = MagicMock()
    dao.set = AsyncMock(return_value=None)
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


class TestScreeningWriteNonDryRun:
    """Verify screenings/ doc is written when dry_run=False."""

    def _screening_set_calls(self, mock_dao: MagicMock) -> list:
        from screener.lib.storage.schema import SCREENINGS

        return [
            call for call in mock_dao.set.call_args_list if call[0][0] == SCREENINGS
        ]

    def test_screenings_collection_receives_one_doc(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        screening_calls = self._screening_set_calls(mock_dao)
        assert len(screening_calls) == 1

    def test_doc_id_is_month_id(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        screening_calls = self._screening_set_calls(mock_dao)
        doc_id = screening_calls[0][0][1]
        assert doc_id == "2026-05"

    def test_payload_contains_month_id(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        screening_calls = self._screening_set_calls(mock_dao)
        payload = screening_calls[0][0][2]
        assert payload["month_id"] == "2026-05"

    def test_payload_contains_all_signals(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        screening_calls = self._screening_set_calls(mock_dao)
        payload = screening_calls[0][0][2]
        assert "all_signals" in payload
        assert len(payload["all_signals"]) == 2  # AAPL + MSFT

    def test_payload_contains_sector_distribution(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        screening_calls = self._screening_set_calls(mock_dao)
        payload = screening_calls[0][0][2]
        assert "sector_distribution" in payload
        assert isinstance(payload["sector_distribution"], dict)

    def test_payload_contains_top_n_before_cap(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        screening_calls = self._screening_set_calls(mock_dao)
        payload = screening_calls[0][0][2]
        assert "top_n_before_cap" in payload
        assert isinstance(payload["top_n_before_cap"], list)

    def test_payload_contains_top_n_after_cap(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        screening_calls = self._screening_set_calls(mock_dao)
        payload = screening_calls[0][0][2]
        assert "top_n_after_cap" in payload
        assert isinstance(payload["top_n_after_cap"], list)

    def test_payload_contains_signal_vintage_dates(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        screening_calls = self._screening_set_calls(mock_dao)
        payload = screening_calls[0][0][2]
        assert "signal_vintage_dates" in payload
        assert isinstance(payload["signal_vintage_dates"], dict)

    def test_screenings_written_after_tickers(self):
        """screenings/ write must come after tickers/ write."""
        from screener.lib.storage.schema import SCREENINGS, TICKERS

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        all_calls = mock_dao.set.call_args_list
        collections_in_order = [call[0][0] for call in all_calls]

        try:
            first_tickers = next(
                i for i, c in enumerate(collections_in_order) if c == TICKERS
            )
            first_screenings = next(
                i for i, c in enumerate(collections_in_order) if c == SCREENINGS
            )
        except StopIteration:
            raise AssertionError(
                f"Expected both '{TICKERS}' and '{SCREENINGS}' writes; "
                f"got collections: {collections_in_order}"
            )

        assert first_tickers < first_screenings, (
            f"tickers/ write (index {first_tickers}) must precede "
            f"screenings/ write (index {first_screenings})"
        )

    def test_screenings_written_before_picks(self):
        """screenings/ write must come before picks/ write."""
        from screener.lib.storage.schema import PICKS, SCREENINGS

        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        all_calls = mock_dao.set.call_args_list
        collections_in_order = [call[0][0] for call in all_calls]

        try:
            first_screenings = next(
                i for i, c in enumerate(collections_in_order) if c == SCREENINGS
            )
            first_picks = next(
                i for i, c in enumerate(collections_in_order) if c == PICKS
            )
        except StopIteration:
            raise AssertionError(
                f"Expected both '{SCREENINGS}' and '{PICKS}' writes; "
                f"got collections: {collections_in_order}"
            )

        assert first_screenings < first_picks, (
            f"screenings/ write (index {first_screenings}) must precede "
            f"picks/ write (index {first_picks})"
        )

    def test_total_set_calls_includes_screenings(self):
        """
        Expected writes: 2 tickers + 1 screenings + 2 picks + 2 perf ledger + 1 perf snapshot = 8.
        """
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "false"})
        assert mock_dao.set.call_count == 8


class TestScreeningWriteDryRun:
    """Verify screenings/ write is skipped when dry_run=True."""

    def test_no_set_calls_in_dry_run(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "true"})
        mock_dao.set.assert_not_called()

    def test_dry_run_via_1_flag(self):
        mock_dao = _run_main({"MONTH_ID": "2026-05", "DRY_RUN": "1"})
        mock_dao.set.assert_not_called()
