"""
screener/metrics/ebitda_ev.py — EBITDA/EV signal fetcher.

EV <= 0 is skipped. Negative EBITDA clipped to 0.0. Capped at EBITDA_EV_CAP (50%).

fetch_ebitda_ev() is synchronous.
write_quarterly_signals() is async — accepts a StorageDAO.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import yfinance as yf

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
BATCH_SLEEP = 5.0
EBITDA_EV_CAP = 0.50

EBITDA_SIGNAL_KEY = "ebitda_ev_signals"
QUARTERLY_COLLECTION = "quarterly_signals"


def fetch_ebitda_ev(tickers: list[str]) -> dict[str, dict]:
    """
    Fetch EBITDA/EV for a list of tickers.

    Returns dict keyed by symbol:
        {
            "ebitda_ev":          float | None,
            "ebitda":             float | None,
            "enterprise_value":   float | None,
            "most_recent_quarter": int | None,
            "skipped":            bool,
            "skip_reason":        str | None,
        }
    """
    results: dict[str, dict] = {}
    skipped = 0

    for batch_start in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[batch_start : batch_start + BATCH_SIZE]
        for symbol in batch:
            result = _fetch_one(symbol)
            results[symbol] = result
            if result["skipped"]:
                skipped += 1
        if batch_start + BATCH_SIZE < len(tickers):
            time.sleep(BATCH_SLEEP)

    logger.info(
        "ebitda_ev fetch complete", extra={"total": len(tickers), "skipped": skipped}
    )
    return results


async def write_quarterly_signals(
    signals: dict[str, dict],
    quarter_id: str,
    dao,  # StorageDAO — typed loosely to avoid circular import at module level
) -> None:
    """
    Persist EBITDA/EV signals to the quarterly_signals collection via StorageDAO.

    Uses merge=True so FCF and EBITDA/EV agents share the same quarterly doc.
    """
    payload = {
        EBITDA_SIGNAL_KEY: signals,
        "quarter_id": quarter_id,
        "ebitda_written_ts": datetime.now(timezone.utc).isoformat(),
    }
    await dao.set(QUARTERLY_COLLECTION, quarter_id, payload, merge=True)
    logger.info("quarterly ebitda_ev write ok", extra={"doc_id": quarter_id})


def _fetch_one(symbol: str) -> dict:
    try:
        info = yf.Ticker(symbol).info
        ebitda = info.get("ebitda")
        ev = info.get("enterpriseValue")

        if ebitda is None:
            return _skip(symbol, "ebitda is None")
        if ev is None:
            return _skip(symbol, "enterpriseValue is None")
        if ev <= 0:
            return _skip(symbol, f"enterpriseValue <= 0: {ev}")

        ebitda_clipped = max(float(ebitda), 0.0)
        ebitda_ev = min(ebitda_clipped / float(ev), EBITDA_EV_CAP)

        mrq = info.get("mostRecentQuarter")
        try:
            mrq_int = int(float(mrq)) if mrq is not None else None
        except (TypeError, ValueError):
            mrq_int = None

        return {
            "ebitda_ev": round(ebitda_ev, 6),
            "ebitda": round(float(ebitda), 0),
            "enterprise_value": round(float(ev), 0),
            "most_recent_quarter": mrq_int,
            "skipped": False,
            "skip_reason": None,
        }
    except Exception as exc:
        return _skip(symbol, f"fetch error: {exc}")


def _skip(symbol: str, reason: str) -> dict:
    logger.debug("ebitda_ev skip", extra={"symbol": symbol, "reason": reason})
    return {
        "ebitda_ev": None,
        "ebitda": None,
        "enterprise_value": None,
        "most_recent_quarter": None,
        "skipped": True,
        "skip_reason": reason,
    }
