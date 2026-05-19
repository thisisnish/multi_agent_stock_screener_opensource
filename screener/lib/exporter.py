"""
screener/lib/exporter.py — Pick history export utilities.

Public API
----------
fetch_pick_history(dao, source, months) -> list[dict]
    Fetch all PickLedgerDoc records from the PERFORMANCE collection,
    optionally filtered by source and entry_month.

records_to_csv(records) -> str
    Serialise a list of pick dicts to a CSV string with a fixed column order.

records_to_json(records) -> str
    Serialise a list of pick dicts to an indented JSON string.

export_pick_history(dao, format, output_path, source, months) -> str
    Convenience wrapper: fetch + serialise + optionally write to disk.

CLI modes (via subcommand)
--------------------------
  picks        Export pick history to CSV or JSON (original mode).
  eval-trend   Print a JSON summary of the last N months of eval trend data.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import logging
from pathlib import Path

from screener.lib.storage.schema import PERFORMANCE

logger = logging.getLogger(__name__)

_CSV_COLUMNS: list[str] = [
    "ticker",
    "entry_month",
    "status",
    "confidence_tier",
    "confidence_score",
    "entry_price",
    "exit_price",
    "pick_return_pct",
    "spy_return_pct",
    "alpha_pct",
    "beat_spy",
    "entry_spy_price",
    "exit_spy_price",
    "exit_week",
    "price_timestamp",
    "source",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _is_ledger_doc(doc: dict) -> bool:
    return "ticker" in doc and "total_picks" not in doc


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


async def fetch_pick_history(
    dao,
    source: str = "judge",
    months: list[str] | None = None,
) -> list[dict]:
    """Fetch PickLedgerDoc records from the PERFORMANCE collection.

    Args:
        dao: StorageDAO instance.
        source: The agent source to filter by (e.g. "judge").
        months: Optional list of "YYYY-MM" strings; when provided, only records
            whose entry_month is in this list are returned.

    Returns:
        List of raw pick dicts (exactly what is stored — no transformation).
    """
    raw = await dao.query(PERFORMANCE, {"source": source})
    records = [doc for doc in raw if _is_ledger_doc(doc)]

    if months is not None:
        month_set = set(months)
        records = [r for r in records if r.get("entry_month") in month_set]

    logger.info(
        "fetch_pick_history: source=%s months=%s returned %d records",
        source,
        months,
        len(records),
    )
    return records


def records_to_csv(records: list[dict]) -> str:
    """Serialise pick dicts to a CSV string with a fixed column order.

    Unknown fields in the dicts are silently ignored.  Missing fields are
    written as empty strings.

    Args:
        records: List of raw pick dicts as returned by fetch_pick_history.

    Returns:
        Full CSV string including the header row.
    """
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=_CSV_COLUMNS,
        extrasaction="ignore",
        restval="",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(records)
    return buf.getvalue()


def records_to_json(records: list[dict]) -> str:
    """Serialise pick dicts to an indented JSON string.

    Args:
        records: List of raw pick dicts as returned by fetch_pick_history.

    Returns:
        JSON string; datetime objects are coerced to str via default=str.
    """
    return json.dumps(records, indent=2, default=str)


async def export_pick_history(
    dao,
    format: str = "csv",
    output_path: str | None = None,
    source: str = "judge",
    months: list[str] | None = None,
) -> str:
    """Fetch and serialise pick history, optionally writing to a file.

    Args:
        dao: StorageDAO instance.
        format: Output format — "csv" or "json".
        output_path: If given, the serialised string is written to this path.
        source: Agent source passed through to fetch_pick_history.
        months: Optional month filter passed through to fetch_pick_history.

    Returns:
        The serialised string (regardless of whether output_path was given).

    Raises:
        ValueError: If format is not "csv" or "json".
    """
    if format not in ("csv", "json"):
        raise ValueError(f"format must be 'csv' or 'json', got: {format!r}")

    records = await fetch_pick_history(dao, source=source, months=months)

    if format == "csv":
        content = records_to_csv(records)
    else:
        content = records_to_json(records)

    if output_path is not None:
        Path(output_path).write_text(content, encoding="utf-8")
        logger.info("wrote %d bytes to %s", len(content), output_path)

    return content


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    parser = argparse.ArgumentParser(
        description="Export pick ledger history or eval trend data."
    )
    subparsers = parser.add_subparsers(dest="report", metavar="REPORT")
    subparsers.required = False  # default to "picks" mode when omitted

    # ---- picks subcommand (default) ----------------------------------------
    picks_parser = subparsers.add_parser(
        "picks",
        help="Export pick ledger history to CSV or JSON (default mode).",
    )
    picks_parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Output format (default: csv)",
    )
    picks_parser.add_argument(
        "--output",
        default=None,
        metavar="FILE",
        help="File path to write; if omitted, output is printed to stdout",
    )
    picks_parser.add_argument(
        "--source",
        default="judge",
        help="Agent source to filter by (default: judge)",
    )
    picks_parser.add_argument(
        "--months",
        nargs="*",
        metavar="YYYY-MM",
        default=None,
        help="Zero or more YYYY-MM month identifiers; omit to export all months",
    )

    # ---- eval-trend subcommand (P3-09) -------------------------------------
    trend_parser = subparsers.add_parser(
        "eval-trend",
        help="Print a JSON summary of the last N months of eval trend data.",
    )
    trend_parser.add_argument(
        "--months",
        type=int,
        default=12,
        metavar="N",
        help="Number of trailing months to query (default: 12)",
    )

    args = parser.parse_args()

    from screener.lib.config_loader import load_config
    from screener.lib.storage.firestore import FirestoreDAO

    _app_config = load_config()
    _dao = FirestoreDAO(
        project_id=_app_config.storage.firestore.project_id,
        database=_app_config.storage.firestore.database,
    )

    # Default to "picks" when no subcommand is given
    _report_mode = args.report or "picks"

    if _report_mode == "picks":
        _months_list = getattr(args, "months", None) or None

        _content = asyncio.run(
            export_pick_history(
                _dao,
                format=getattr(args, "format", "csv"),
                output_path=getattr(args, "output", None),
                source=getattr(args, "source", "judge"),
                months=_months_list,
            )
        )

        if getattr(args, "output", None) is None:
            print(_content, end="")

    elif _report_mode == "eval-trend":
        from screener.eval.reporter import run_eval_trend_report

        _n_months = getattr(args, "months", 12)
        _result = asyncio.run(run_eval_trend_report(_dao, n_months=_n_months))
        print(json.dumps(_result, indent=2))

    sys.exit(0)
