"""
screener/metrics/earnings_yield.py — Earnings yield (E/P) signal fetcher.

earnings_yield = trailingEps / currentPrice

Fetched via yfinance Ticker.info per-ticker, batched with rate-limit sleep.
Raises AbortSignal if skip rate exceeds MAX_SKIP_RATE — a partial Z-score
is worse than no Z-score.
"""

from __future__ import annotations

import logging
import time

import yfinance as yf

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
BATCH_SLEEP = 1.0
TICKER_TIMEOUT = 3
MAX_SKIP_RATE = 0.15


class AbortSignal(Exception):
    """Raised when skip rate exceeds MAX_SKIP_RATE."""


def fetch_earnings_yield(tickers: list[str]) -> dict[str, dict]:
    """
    Fetch earnings yield (E/P) for a list of tickers.

    Returns dict keyed by symbol:
        {
            "earnings_yield": float,
            "trailing_eps":   float,
            "price":          float,
            "skipped":        bool,
            "skip_reason":    str | None,
        }

    Raises AbortSignal if skipped fraction exceeds MAX_SKIP_RATE.
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

    total = len(tickers)
    skip_rate = skipped / total if total > 0 else 0.0

    logger.info(
        "earnings_yield fetch complete",
        extra={"total": total, "skipped": skipped, "skip_rate": f"{skip_rate:.1%}"},
    )

    if skip_rate > MAX_SKIP_RATE:
        raise AbortSignal(
            f"earnings yield skip rate {skip_rate:.1%} exceeds threshold "
            f"{MAX_SKIP_RATE:.0%} ({skipped}/{total} tickers skipped)"
        )

    return results


def _fetch_one(symbol: str) -> dict:
    try:
        info = yf.Ticker(symbol).info
        eps = info.get("trailingEps")
        price = info.get("currentPrice") or info.get("regularMarketPrice")

        if eps is None:
            return _skip(symbol, "trailingEps is None")
        if price is None or price <= 0:
            return _skip(symbol, f"currentPrice invalid: {price}")

        eps_clipped = max(float(eps), 0.0)
        earnings_yield = eps_clipped / float(price)

        return {
            "earnings_yield": round(earnings_yield, 6),
            "trailing_eps": round(float(eps), 4),
            "price": round(float(price), 2),
            "skipped": False,
            "skip_reason": None,
        }
    except Exception as exc:
        return _skip(symbol, f"fetch error: {exc}")


def _skip(symbol: str, reason: str) -> dict:
    logger.debug("earnings_yield skip", extra={"symbol": symbol, "reason": reason})
    return {
        "earnings_yield": None,
        "trailing_eps": None,
        "price": None,
        "skipped": True,
        "skip_reason": reason,
    }
