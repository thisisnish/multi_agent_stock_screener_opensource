"""
screener/metrics/fcf_yield.py — FCF yield (freeCashflow / marketCap) signal fetcher.

Negative FCF is clipped to 0.0. Result is capped at FCF_YIELD_CAP (30%).

fetch_fcf_yield() is synchronous (yfinance I/O with rate-limit sleep).
write_quarterly_signals() is async — accepts a StorageDAO from screener.lib.storage.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import yfinance as yf

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
BATCH_SLEEP = 5.0
FCF_YIELD_CAP = 0.30

FCF_SIGNAL_KEY = "fcf_yield_signals"
QUARTERLY_COLLECTION = "quarterly_signals"


def fetch_fcf_yield(tickers: list[str]) -> dict[str, dict]:
    """
    Fetch FCF yield for a list of tickers.

    Returns dict keyed by symbol:
        {
            "fcf_yield":          float | None,
            "free_cashflow":      float | None,
            "market_cap":         float | None,
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
        "fcf_yield fetch complete", extra={"total": len(tickers), "skipped": skipped}
    )
    return results


async def write_quarterly_signals(
    signals: dict[str, dict],
    quarter_id: str,
    dao,  # StorageDAO — typed loosely to avoid circular import at module level
) -> None:
    """
    Persist FCF yield signals to the quarterly_signals collection via StorageDAO.

    Uses merge=True so FCF and EBITDA/EV agents share the same quarterly doc.
    """
    payload = {
        FCF_SIGNAL_KEY: signals,
        "quarter_id": quarter_id,
        "fcf_written_ts": datetime.now(timezone.utc).isoformat(),
    }
    await dao.set(QUARTERLY_COLLECTION, quarter_id, payload, merge=True)
    logger.info("quarterly fcf_yield write ok", extra={"doc_id": quarter_id})


def _fetch_one(symbol: str) -> dict:
    try:
        info = yf.Ticker(symbol).info
        fcf = info.get("freeCashflow")
        mktcap = info.get("marketCap")

        if fcf is None:
            return _skip(symbol, "freeCashflow is None")
        if mktcap is None or mktcap <= 0:
            return _skip(symbol, f"marketCap invalid: {mktcap}")

        fcf_clipped = max(float(fcf), 0.0)
        fcf_yield = min(fcf_clipped / float(mktcap), FCF_YIELD_CAP)

        mrq = info.get("mostRecentQuarter")
        try:
            mrq_int = int(float(mrq)) if mrq is not None else None
        except (TypeError, ValueError):
            mrq_int = None

        return {
            "fcf_yield": round(fcf_yield, 6),
            "free_cashflow": round(float(fcf), 0),
            "market_cap": round(float(mktcap), 0),
            "most_recent_quarter": mrq_int,
            "skipped": False,
            "skip_reason": None,
        }
    except Exception as exc:
        return _skip(symbol, f"fetch error: {exc}")


def _skip(symbol: str, reason: str) -> dict:
    logger.debug("fcf_yield skip", extra={"symbol": symbol, "reason": reason})
    return {
        "fcf_yield": None,
        "free_cashflow": None,
        "market_cap": None,
        "most_recent_quarter": None,
        "skipped": True,
        "skip_reason": reason,
    }
