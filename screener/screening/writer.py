"""
screener/screening/writer.py — ScreeningDoc builder and Firestore writer.

Responsibilities:
    1. Build per-ticker ``TickerScreeningEntry`` rows from the scored signal dicts
       produced by the normalisation and sector-cap steps in ``screener_job``.
    2. Assemble the ``ScreeningDoc`` with full scoring run metadata:
       - all_signals       : every ticker that was scored
       - top_n_before_cap  : tickers that ranked in top-N before sector-cap
       - top_n_after_cap   : tickers in the final post-cap picks
       - sector_distribution: sector → count of final picks
       - signal_vintage_dates: factor → ISO date of last data retrieval
    3. Write the document to ``screenings/{MONTH_ID}`` via the DAO.

The module is pure computation + I/O — no LLM calls, no LangGraph.
``write_screening_doc`` is async to match the DAO interface.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def build_ticker_entries(
    gated: list[dict],
    picks: list[dict],
    factor_scores: dict[str, dict[str, Optional[float]]],
    top_n: int,
) -> list[dict]:
    """Build serialised ``TickerScreeningEntry`` payloads for every scored ticker.

    Determines ``in_top_n_before_cap``, ``in_top_n_after_cap``, and
    ``cap_filtered`` flags from the sorted order of ``gated`` and the final
    ``picks`` list.

    Args:
        gated:         All scored ticker dicts (each has ``symbol``, ``sector``,
                       ``composite_score``, ``ma200_gate``).  Sorted descending
                       by composite_score within this function.
        picks:         The post-cap top-N ticker dicts (subset of ``gated``).
        factor_scores: ``{factor: {symbol: score | None}}`` from
                       ``sector_z_scores()`` for each of the four factors.
        top_n:         The configured top-N size; used to determine which tickers
                       would have ranked in top-N before the sector cap was applied.

    Returns:
        List of ``dict`` payloads (``TickerScreeningEntry.model_dump(mode="json")``).
    """
    from screener.lib.storage.schema import TickerScreeningEntry

    sorted_by_score = sorted(gated, key=lambda x: x["composite_score"], reverse=True)
    # Symbols in descending score order — the first top_n are "before cap"
    before_cap_symbols: set[str] = {e["symbol"] for e in sorted_by_score[:top_n]}
    after_cap_symbols: set[str] = {p["symbol"] for p in picks}

    entries: list[dict] = []
    for entry in sorted_by_score:
        sym = entry["symbol"]
        ma200_gate = entry.get("ma200_gate") or {}

        in_before = sym in before_cap_symbols
        in_after = sym in after_cap_symbols
        # cap_filtered: was in top-N before cap but excluded by the cap
        cap_filtered = in_before and not in_after

        row = TickerScreeningEntry(
            symbol=sym,
            sector=entry.get("sector", "Unknown"),
            technical=factor_scores.get("technical", {}).get(sym),
            earnings=factor_scores.get("earnings", {}).get(sym),
            fcf=factor_scores.get("fcf", {}).get(sym),
            ebitda=factor_scores.get("ebitda", {}).get(sym),
            composite_score=entry["composite_score"],
            ma200_multiplier=float(ma200_gate.get("multiplier", 1.0)),
            in_top_n_before_cap=in_before,
            in_top_n_after_cap=in_after,
            cap_filtered=cap_filtered,
        )
        entries.append(row.model_dump(mode="json"))
        logger.debug(
            "screening entry built — %s composite=%.2f before_cap=%s after_cap=%s",
            sym,
            entry["composite_score"],
            in_before,
            in_after,
        )

    return entries


def build_screening_doc(
    month_id: str,
    ticker_entries: list[dict],
    picks: list[dict],
    signal_vintage_dates: Optional[dict[str, str]] = None,
) -> dict:
    """Assemble the ``ScreeningDoc`` payload from scored ticker entries.

    Args:
        month_id:             Current month identifier, e.g. ``"2026-04"``.
        ticker_entries:       Serialised ``TickerScreeningEntry`` dicts (output of
                              :func:`build_ticker_entries`).
        picks:                Post-cap final picks list (ordered by rank).
        signal_vintage_dates: Optional mapping from factor name to ISO date string
                              indicating when signals were last fetched.  If None,
                              defaults to today's UTC date for all four factors.

    Returns:
        Serialised ``ScreeningDoc`` payload dict ready for
        ``dao.set(SCREENINGS, doc_id, payload)``.
    """
    from screener.lib.storage.schema import ScreeningDoc

    if signal_vintage_dates is None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        signal_vintage_dates = {
            "technical": today,
            "earnings": today,
            "fcf": today,
            "ebitda": today,
        }

    top_n_before_cap = [
        e["symbol"] for e in ticker_entries if e.get("in_top_n_before_cap")
    ]
    top_n_after_cap = [
        e["symbol"] for e in ticker_entries if e.get("in_top_n_after_cap")
    ]

    # Sector distribution of final picks
    sector_distribution: dict[str, int] = {}
    for pick in picks:
        sector = pick.get("sector", "Unknown")
        sector_distribution[sector] = sector_distribution.get(sector, 0) + 1

    doc = ScreeningDoc(
        month_id=month_id,
        all_signals=ticker_entries,  # type: ignore[arg-type]
        top_n_before_cap=top_n_before_cap,
        top_n_after_cap=top_n_after_cap,
        sector_distribution=sector_distribution,
        signal_vintage_dates=signal_vintage_dates,
    )
    return doc.model_dump(mode="json")


async def write_screening_doc(
    dao,
    month_id: str,
    gated: list[dict],
    picks: list[dict],
    factor_scores: dict[str, dict[str, Optional[float]]],
    top_n: int,
    signal_vintage_dates: Optional[dict[str, str]] = None,
) -> None:
    """Build and write the ``ScreeningDoc`` to ``screenings/{MONTH_ID}``.

    Uses ``dao.set()`` (upsert) so the function is idempotent: re-running the
    screener for the same month safely overwrites the existing document.

    Args:
        dao:                  StorageDAO instance.
        month_id:             Current month identifier, e.g. ``"2026-04"``.
        gated:                All scored ticker dicts (output of the composite
                              scoring + MA200 gate step).
        picks:                Post-cap top-N ticker dicts (final picks).
        factor_scores:        ``{factor: {symbol: score | None}}`` from
                              ``sector_z_scores()`` for each factor.
        top_n:                Configured top-N size.
        signal_vintage_dates: Optional factor → date mapping; defaults to today.
    """
    from screener.lib.storage.schema import SCREENINGS, screening_run_doc_id

    if not gated:
        logger.info("no scored tickers — skipping screening doc write")
        return

    ticker_entries = build_ticker_entries(
        gated=gated,
        picks=picks,
        factor_scores=factor_scores,
        top_n=top_n,
    )

    payload = build_screening_doc(
        month_id=month_id,
        ticker_entries=ticker_entries,
        picks=picks,
        signal_vintage_dates=signal_vintage_dates,
    )

    doc_id = screening_run_doc_id(month_id)
    await dao.set(SCREENINGS, doc_id, payload)
    logger.info(
        "screenings doc written — %s (total=%d before_cap=%d after_cap=%d)",
        doc_id,
        len(ticker_entries),
        len(payload["top_n_before_cap"]),
        len(payload["top_n_after_cap"]),
    )
