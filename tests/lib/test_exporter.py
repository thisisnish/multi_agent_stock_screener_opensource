"""
tests/lib/test_exporter.py — Unit tests for screener/lib/exporter.py.

fetch_pick_history and export_pick_history are async; they are driven with
asyncio.run() so that the tests remain plain pytest functions (no pytest-asyncio
dependency required).
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from screener.lib.exporter import (
    _is_ledger_doc,
    export_pick_history,
    fetch_pick_history,
    records_to_csv,
    records_to_json,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_LEDGER_DOC_APRIL = {
    "ticker": "AAPL",
    "source": "judge",
    "entry_month": "2026-04",
    "entry_price": 170.0,
    "entry_spy_price": 510.0,
    "status": "closed",
    "exit_week": "202617",
    "exit_price": 185.0,
    "exit_spy_price": 520.0,
    "pick_return_pct": 8.82,
    "spy_return_pct": 1.96,
    "alpha_pct": 6.86,
    "beat_spy": True,
    "price_timestamp": "2026-04-30",
    "confidence_score": 0.78,
    "confidence_tier": "high",
}

_LEDGER_DOC_MAY = {
    "ticker": "MSFT",
    "source": "judge",
    "entry_month": "2026-05",
    "entry_price": 390.0,
    "entry_spy_price": 520.0,
    "status": "active",
    "exit_week": None,
    "exit_price": None,
    "exit_spy_price": None,
    "pick_return_pct": None,
    "spy_return_pct": None,
    "alpha_pct": None,
    "beat_spy": None,
    "price_timestamp": None,
    "confidence_score": 0.62,
    "confidence_tier": "med",
}

_SNAPSHOT_DOC = {
    "month_id": "2026-04",
    "source": "judge",
    "total_picks": 5,
    "active_picks": 2,
    "closed_picks": 3,
}


def _make_dao(*docs: dict) -> AsyncMock:
    dao = AsyncMock()
    dao.query = AsyncMock(return_value=list(docs))
    return dao


# ---------------------------------------------------------------------------
# _is_ledger_doc
# ---------------------------------------------------------------------------


def test_is_ledger_doc_returns_true_for_ledger():
    assert _is_ledger_doc(_LEDGER_DOC_APRIL) is True


def test_is_ledger_doc_returns_false_for_snapshot():
    assert _is_ledger_doc(_SNAPSHOT_DOC) is False


def test_is_ledger_doc_returns_false_when_ticker_missing():
    assert _is_ledger_doc({"source": "judge", "total_picks": 5}) is False


def test_is_ledger_doc_returns_false_when_both_keys_present():
    assert _is_ledger_doc({"ticker": "AAPL", "total_picks": 5}) is False


# ---------------------------------------------------------------------------
# fetch_pick_history — filtering snapshot docs
# ---------------------------------------------------------------------------


def test_fetch_pick_history_filters_out_snapshot_docs():
    dao = _make_dao(_LEDGER_DOC_APRIL, _SNAPSHOT_DOC, _LEDGER_DOC_MAY)
    result = asyncio.run(fetch_pick_history(dao))
    tickers = [r["ticker"] for r in result]
    assert "AAPL" in tickers
    assert "MSFT" in tickers
    # snapshot has no "ticker" key, so its month_id must not appear as ticker
    assert len(result) == 2


def test_fetch_pick_history_passes_source_filter_to_query():
    dao = _make_dao(_LEDGER_DOC_APRIL)
    asyncio.run(fetch_pick_history(dao, source="bull"))
    dao.query.assert_called_once_with("performance", {"source": "bull"})


# ---------------------------------------------------------------------------
# fetch_pick_history — month filtering
# ---------------------------------------------------------------------------


def test_fetch_pick_history_with_months_filters_correctly():
    dao = _make_dao(_LEDGER_DOC_APRIL, _LEDGER_DOC_MAY, _SNAPSHOT_DOC)
    result = asyncio.run(fetch_pick_history(dao, months=["2026-04"]))
    assert len(result) == 1
    assert result[0]["ticker"] == "AAPL"


def test_fetch_pick_history_with_empty_months_returns_nothing():
    dao = _make_dao(_LEDGER_DOC_APRIL, _LEDGER_DOC_MAY)
    result = asyncio.run(fetch_pick_history(dao, months=[]))
    assert result == []


def test_fetch_pick_history_with_none_months_returns_all_ledger_docs():
    dao = _make_dao(_LEDGER_DOC_APRIL, _LEDGER_DOC_MAY, _SNAPSHOT_DOC)
    result = asyncio.run(fetch_pick_history(dao, months=None))
    assert len(result) == 2


def test_fetch_pick_history_multiple_months():
    dao = _make_dao(_LEDGER_DOC_APRIL, _LEDGER_DOC_MAY, _SNAPSHOT_DOC)
    result = asyncio.run(fetch_pick_history(dao, months=["2026-04", "2026-05"]))
    assert len(result) == 2


# ---------------------------------------------------------------------------
# records_to_csv
# ---------------------------------------------------------------------------


def test_records_to_csv_header_row_present():
    csv_str = records_to_csv([_LEDGER_DOC_APRIL])
    header = csv_str.splitlines()[0]
    assert header.startswith("ticker,")
    assert "entry_month" in header
    assert "confidence_tier" in header


def test_records_to_csv_data_row_contains_ticker():
    csv_str = records_to_csv([_LEDGER_DOC_APRIL])
    assert "AAPL" in csv_str


def test_records_to_csv_correct_column_order():
    csv_str = records_to_csv([])
    header = csv_str.splitlines()[0]
    columns = header.split(",")
    assert columns[0] == "ticker"
    assert columns[1] == "entry_month"
    assert columns[-1] == "source"


def test_records_to_csv_handles_missing_fields_without_error():
    sparse = {"ticker": "NVDA", "entry_month": "2026-04", "source": "judge"}
    csv_str = records_to_csv([sparse])
    lines = csv_str.splitlines()
    assert len(lines) == 2
    data_row = lines[1]
    # exit_price column should be present but empty
    assert "NVDA" in data_row


def test_records_to_csv_none_values_written_as_empty():
    record = dict(_LEDGER_DOC_MAY)
    csv_str = records_to_csv([record])
    lines = csv_str.splitlines()
    assert len(lines) == 2


def test_records_to_csv_ignores_extra_fields():
    extra = dict(_LEDGER_DOC_APRIL)
    extra["unexpected_field"] = "should_be_dropped"
    csv_str = records_to_csv([extra])
    assert "unexpected_field" not in csv_str
    assert "should_be_dropped" not in csv_str


def test_records_to_csv_empty_list_returns_header_only():
    csv_str = records_to_csv([])
    lines = [ln for ln in csv_str.splitlines() if ln]
    assert len(lines) == 1


def test_records_to_csv_multiple_records():
    csv_str = records_to_csv([_LEDGER_DOC_APRIL, _LEDGER_DOC_MAY])
    lines = csv_str.splitlines()
    assert len(lines) == 3  # header + 2 data rows


# ---------------------------------------------------------------------------
# records_to_json
# ---------------------------------------------------------------------------


def test_records_to_json_is_valid_json():
    json_str = records_to_json([_LEDGER_DOC_APRIL])
    parsed = json.loads(json_str)
    assert isinstance(parsed, list)


def test_records_to_json_round_trips_correctly():
    json_str = records_to_json([_LEDGER_DOC_APRIL, _LEDGER_DOC_MAY])
    parsed = json.loads(json_str)
    assert len(parsed) == 2
    assert parsed[0]["ticker"] == "AAPL"
    assert parsed[1]["ticker"] == "MSFT"


def test_records_to_json_handles_datetime_objects():
    from datetime import datetime, timezone

    record = {"ticker": "AAPL", "ts": datetime(2026, 4, 30, tzinfo=timezone.utc)}
    json_str = records_to_json([record])
    parsed = json.loads(json_str)
    assert "2026-04-30" in parsed[0]["ts"]


def test_records_to_json_empty_list_returns_array():
    json_str = records_to_json([])
    assert json.loads(json_str) == []


def test_records_to_json_is_indented():
    json_str = records_to_json([_LEDGER_DOC_APRIL])
    assert "\n" in json_str


# ---------------------------------------------------------------------------
# export_pick_history
# ---------------------------------------------------------------------------


def test_export_pick_history_csv_returns_string_starting_with_header():
    dao = _make_dao(_LEDGER_DOC_APRIL)
    result = asyncio.run(export_pick_history(dao, format="csv"))
    assert isinstance(result, str)
    assert result.startswith("ticker,")


def test_export_pick_history_json_returns_valid_json():
    dao = _make_dao(_LEDGER_DOC_APRIL)
    result = asyncio.run(export_pick_history(dao, format="json"))
    parsed = json.loads(result)
    assert isinstance(parsed, list)


def test_export_pick_history_bad_format_raises_value_error():
    dao = _make_dao(_LEDGER_DOC_APRIL)
    with pytest.raises(ValueError, match="format must be"):
        asyncio.run(export_pick_history(dao, format="bad"))


def test_export_pick_history_with_output_path_writes_file(tmp_path):
    dao = _make_dao(_LEDGER_DOC_APRIL)
    out = tmp_path / "picks.csv"
    result = asyncio.run(export_pick_history(dao, format="csv", output_path=str(out)))
    assert out.exists()
    assert out.read_text(encoding="utf-8") == result


def test_export_pick_history_json_output_path_writes_file(tmp_path):
    dao = _make_dao(_LEDGER_DOC_APRIL)
    out = tmp_path / "picks.json"
    result = asyncio.run(export_pick_history(dao, format="json", output_path=str(out)))
    assert out.exists()
    written = out.read_text(encoding="utf-8")
    assert json.loads(written) == json.loads(result)


def test_export_pick_history_no_output_path_does_not_create_file(tmp_path):
    dao = _make_dao(_LEDGER_DOC_APRIL)
    asyncio.run(export_pick_history(dao, format="csv"))
    assert list(tmp_path.iterdir()) == []


def test_export_pick_history_passes_source_and_months():
    dao = _make_dao(_LEDGER_DOC_APRIL)
    asyncio.run(
        export_pick_history(dao, format="csv", source="bull", months=["2026-04"])
    )
    dao.query.assert_called_once_with("performance", {"source": "bull"})
