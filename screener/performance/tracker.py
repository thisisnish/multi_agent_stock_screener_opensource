"""
screener/performance/tracker.py — Performance tracking for monthly pick ledgers.

Responsibilities:
    1. Fetch the current SPY price to use as the benchmark entry price.
    2. Build ``PickLedgerDoc`` entries from debate verdicts + entry prices.
    3. Compute ``PerformanceSnapshotDoc`` aggregates for the monthly run.
    4. Write both to the ``performance/`` Firestore collection.

All price fetches use yfinance (same dependency as the signal fetchers).
SPY is used as the market benchmark following AGENT.md conventions.

This module is pure computation + I/O — no LLM calls, no LangGraph.
All public functions are async to match the DAO interface.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

try:
    import yfinance as yf
except ImportError:  # pragma: no cover — yfinance always present in .venv
    yf = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# Benchmark ticker for alpha calculations.
_SPY = "SPY"


def _confidence_tier(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    if score >= 70:
        return "High"
    if score >= 40:
        return "Med"
    return "Low"


def fetch_spy_price() -> Optional[float]:
    """Fetch the latest SPY closing price from yfinance.

    Returns:
        Most-recent closing price as a float, or ``None`` if yfinance
        returns no data (graceful-degrade — the caller handles None).
    """
    try:
        if yf is None:
            logger.warning("yfinance not available — cannot fetch SPY price")
            return None

        ticker = yf.Ticker(_SPY)
        hist = ticker.history(period="5d")
        if hist is None or hist.empty:
            logger.warning("yfinance returned no data for SPY")
            return None
        price = float(hist["Close"].dropna().iloc[-1])
        logger.info("SPY entry price fetched: %.2f", price)
        return price
    except Exception:
        logger.exception(
            "failed to fetch SPY price — performance doc will omit entry_spy_price"
        )
        return None


def build_pick_ledger_entries(
    verdicts: list[dict],
    picks: list[dict],
    month_id: str,
    entry_spy_price: Optional[float],
    source: str = "judge",
) -> list[dict]:
    """Build serialised ``PickLedgerDoc`` payloads for each debate verdict.

    One ledger entry per verdict.  Entry prices come from the technical signal
    data already embedded in ``picks`` (the ``price`` field set during the
    scoring step).  SPY entry price is shared across all entries — it's the
    benchmark at the moment the picks are published.

    Args:
        verdicts:        List of debate state dicts from ``graph.ainvoke()``.
        picks:           Scored pick dicts (carry technical signal ``price``).
        month_id:        Current month identifier, e.g. ``"2026-04"``.
        entry_spy_price: SPY closing price at pick entry date (may be None).
        source:          Agent source label; defaults to ``"judge"``.

    Returns:
        List of ``dict`` payloads ready for ``dao.set(PERFORMANCE, doc_id, payload)``.
        Each dict is keyed by its Firestore doc ID under ``_doc_id`` for the
        caller to route correctly.
    """
    from screener.lib.storage.schema import PickLedgerDoc, pick_ledger_doc_id

    # Build a fast lookup: symbol → entry price from the picks list.
    price_by_symbol: dict[str, Optional[float]] = {
        p["symbol"]: p.get("price") for p in picks
    }

    now_iso = datetime.now(timezone.utc).isoformat()
    entries: list[dict] = []

    for verdict in verdicts:
        symbol = verdict.get("ticker", "UNKNOWN")
        entry_price = price_by_symbol.get(symbol)
        conf_score: Optional[float] = verdict.get("confidence_score")
        adaptive_active: bool = verdict.get("adaptive_weights_active", False)

        doc = PickLedgerDoc(
            ticker=symbol,
            source=source,
            entry_month=month_id,
            entry_price=entry_price,
            entry_spy_price=entry_spy_price,
            status="active",
            price_timestamp=now_iso,
            confidence_score=conf_score,
            confidence_tier=_confidence_tier(conf_score),
            adaptive_weights_active=adaptive_active,
        )

        doc_id = pick_ledger_doc_id(symbol, month_id, source)
        payload = doc.model_dump(mode="json")
        payload["_doc_id"] = doc_id
        entries.append(payload)
        logger.debug(
            "pick ledger entry built — %s entry_price=%.2f",
            symbol,
            entry_price or 0.0,
        )

    return entries


def build_performance_snapshot(
    month_id: str,
    ledger_entries: list[dict],
    entry_spy_price: Optional[float],
    source: str = "judge",
) -> dict:
    """Compute aggregate ``PerformanceSnapshotDoc`` from the ledger entries.

    At pick-entry time all picks are ``active`` and no returns are available,
    so ``win_rate``, ``avg_return_pct``, ``avg_spy_return_pct``,
    ``avg_alpha_pct``, and ``beats_spy_rate`` are all ``None``.  These fields
    are populated by a future update step (e.g. the eval Cloud Function) once
    prices have moved.

    Args:
        month_id:        Current month identifier, e.g. ``"2026-04"``.
        ledger_entries:  List of pick ledger payload dicts (output of
                         :func:`build_pick_ledger_entries`).
        entry_spy_price: SPY price at entry (may be None).
        source:          Agent source label; defaults to ``"judge"``.

    Returns:
        Serialised ``PerformanceSnapshotDoc`` payload dict (no ``_doc_id``
        key — caller supplies the doc ID via :func:`performance_doc_id`).
    """
    from screener.lib.storage.schema import PerformanceSnapshotDoc

    total = len(ledger_entries)
    active = sum(1 for e in ledger_entries if e.get("status") == "active")
    closed = total - active

    # Group entries by confidence tier.
    tier_entries: dict[str, list[dict]] = {"High": [], "Med": [], "Low": []}
    for e in ledger_entries:
        tier = e.get("confidence_tier")
        if tier in tier_entries:
            tier_entries[tier].append(e)

    def _tier_stats(
        entries: list[dict],
    ) -> tuple[int, Optional[float], Optional[float], Optional[float]]:
        count = len(entries)
        if count == 0:
            return 0, None, None, None
        closed_entries = [e for e in entries if e.get("pick_return_pct") is not None]
        if not closed_entries:
            return count, None, None, None
        win_rate = sum(1 for e in closed_entries if e.get("beat_spy")) / len(
            closed_entries
        )
        avg_return = sum(e["pick_return_pct"] for e in closed_entries) / len(
            closed_entries
        )
        alphas = [
            e.get("alpha_pct") for e in closed_entries if e.get("alpha_pct") is not None
        ]
        avg_alpha = sum(alphas) / len(alphas) if alphas else None
        return count, win_rate, avg_return, avg_alpha

    h_count, h_win, h_ret, h_alpha = _tier_stats(tier_entries["High"])
    m_count, m_win, m_ret, m_alpha = _tier_stats(tier_entries["Med"])
    l_count, l_win, l_ret, l_alpha = _tier_stats(tier_entries["Low"])

    # Cohort split: adaptive vs default picks.
    adaptive_entries = [e for e in ledger_entries if e.get("adaptive_weights_active")]
    default_entries = [e for e in ledger_entries if not e.get("adaptive_weights_active")]

    def _cohort_win_rate(entries: list[dict]) -> Optional[float]:
        """Win rate over closed entries (beat_spy not None); None if no closed entries."""
        closed = [e for e in entries if e.get("beat_spy") is not None]
        if not closed:
            return None
        return sum(1 for e in closed if e.get("beat_spy")) / len(closed)

    adaptive_count: Optional[int] = len(adaptive_entries) if adaptive_entries else None
    default_count: Optional[int] = len(default_entries) if default_entries else None
    adaptive_win = _cohort_win_rate(adaptive_entries)
    default_win = _cohort_win_rate(default_entries)

    snapshot = PerformanceSnapshotDoc(
        month_id=month_id,
        source=source,
        total_picks=total,
        active_picks=active,
        closed_picks=closed,
        entry_spy_price=entry_spy_price,
        # Returns computed once picks close; None at entry time.
        win_rate=None,
        avg_return_pct=None,
        avg_spy_return_pct=None,
        avg_alpha_pct=None,
        beats_spy_rate=None,
        high_tier_count=h_count if h_count > 0 else None,
        med_tier_count=m_count if m_count > 0 else None,
        low_tier_count=l_count if l_count > 0 else None,
        high_win_rate=h_win,
        med_win_rate=m_win,
        low_win_rate=l_win,
        high_avg_return_pct=h_ret,
        med_avg_return_pct=m_ret,
        low_avg_return_pct=l_ret,
        high_avg_alpha_pct=h_alpha,
        med_avg_alpha_pct=m_alpha,
        low_avg_alpha_pct=l_alpha,
        adaptive_picks_count=adaptive_count,
        default_picks_count=default_count,
        adaptive_win_rate=adaptive_win,
        default_win_rate=default_win,
    )
    return snapshot.model_dump(mode="json")


async def write_performance_docs(
    dao,
    month_id: str,
    verdicts: list[dict],
    picks: list[dict],
    source: str = "judge",
) -> None:
    """Fetch SPY price, build ledger entries + snapshot, and write to Firestore.

    Writes to two Firestore paths:
        - ``performance/{MONTH_ID}_{source}``         — monthly snapshot aggregate
        - ``performance/{TICKER}_{MONTH_ID}_{source}`` — individual pick ledger entries

    All writes use ``dao.set()`` (upsert) so the function is idempotent: re-running
    the screener for the same month safely overwrites existing docs.

    Args:
        dao:      StorageDAO instance.
        month_id: Current month identifier, e.g. ``"2026-04"``.
        verdicts: Debate state dicts from ``graph.ainvoke()`` calls.
        picks:    Scored pick dicts (carry ``symbol`` and ``price``).
        source:   Agent label; defaults to ``"judge"``.
    """
    from screener.lib.storage.schema import PERFORMANCE, performance_doc_id

    if not verdicts:
        logger.info("no verdicts to track — skipping performance write")
        return

    # Fetch SPY benchmark price (gracefully skip if yfinance is unavailable).
    entry_spy_price = fetch_spy_price()

    # Build per-pick ledger entries.
    ledger_entries = build_pick_ledger_entries(
        verdicts=verdicts,
        picks=picks,
        month_id=month_id,
        entry_spy_price=entry_spy_price,
        source=source,
    )

    # Write individual pick ledger docs into performance/ collection.
    for entry in ledger_entries:
        doc_id = entry.pop("_doc_id")
        await dao.set(PERFORMANCE, doc_id, entry)
        logger.debug("performance ledger doc written — %s", doc_id)

    # Build and write the monthly snapshot aggregate.
    snapshot_payload = build_performance_snapshot(
        month_id=month_id,
        ledger_entries=ledger_entries,
        entry_spy_price=entry_spy_price,
        source=source,
    )
    snapshot_doc_id = performance_doc_id(month_id, source)
    await dao.set(PERFORMANCE, snapshot_doc_id, snapshot_payload)
    logger.info(
        "performance snapshot written — %s (total_picks=%d, active=%d)",
        snapshot_doc_id,
        snapshot_payload["total_picks"],
        snapshot_payload["active_picks"],
    )
