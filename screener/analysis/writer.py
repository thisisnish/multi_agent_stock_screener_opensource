"""
screener/analysis/writer.py — AnalysisDoc builder and Firestore writer.

Responsibilities:
    1. Extract Bull/Bear/Judge output from a completed DebateState dict.
    2. Assemble an ``AnalysisDoc`` capturing the full debate cache.
    3. Write the document to ``analysis/{TICKER}_{MONTH_ID}`` via the DAO.

The module is pure computation + I/O — no LLM calls, no LangGraph.
``write_analysis_doc`` is async to match the DAO interface.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _get_attr_or_key(obj, key, default=None):
    """Return ``obj.key`` if obj has the attribute, else ``obj[key]``, else default."""
    if hasattr(obj, key):
        return getattr(obj, key)
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default


def build_analysis_doc(ticker: str, month_id: str, state: dict) -> dict:
    """Extract Bull/Bear/Judge output from a completed DebateState and assemble
    an ``AnalysisDoc`` payload.

    Handles both Pydantic model and plain-dict forms for ``bull_output``,
    ``bear_output``, and ``judge_output`` so the function is safe to call
    regardless of whether LangGraph has already serialised the state.

    Args:
        ticker:   Upper-case ticker symbol, e.g. ``"AAPL"``.
        month_id: Month identifier in ``"YYYY-MM"`` format, e.g. ``"2026-04"``.
        state:    Completed ``DebateState`` dict (or any dict with the same keys).

    Returns:
        Serialised ``AnalysisDoc`` payload dict (``model_dump(mode="json")``),
        ready for ``dao.set(ANALYSIS, doc_id, payload)``.
    """
    from screener.lib.storage.schema import AnalysisDoc

    bull = state.get("bull_output") or {}
    bear = state.get("bear_output") or {}
    judge = state.get("judge_output") or {}

    bull_thesis = _get_attr_or_key(bull, "bull_arguments", default=[]) or []
    bull_catalysts = _get_attr_or_key(bull, "key_catalysts", default=[]) or []
    bull_sources = _get_attr_or_key(bull, "signal_citations", default=[]) or []

    bear_thesis = _get_attr_or_key(bear, "bear_arguments", default=[]) or []
    bear_sources = _get_attr_or_key(bear, "signal_citations", default=[]) or []

    judge_reasoning = _get_attr_or_key(judge, "rationale", default="") or ""
    decisive_factor = _get_attr_or_key(judge, "decisive_factor", default=None)
    margin_of_victory = _get_attr_or_key(judge, "margin_of_victory", default=None)

    doc = AnalysisDoc(
        ticker=ticker,
        month_id=month_id,
        bull_thesis=bull_thesis,
        bull_catalysts=bull_catalysts,
        bear_thesis=bear_thesis,
        bull_sources=bull_sources,
        bear_sources=bear_sources,
        judge_reasoning=judge_reasoning,
        judge_verdict=state.get("final_action", "HOLD") or "HOLD",
        judge_confidence=state.get("confidence_score"),
        bull_conviction=state.get("bull_conviction"),
        bear_conviction=state.get("bear_conviction"),
        decisive_factor=decisive_factor,
        margin_of_victory=margin_of_victory,
        contested_truth=state.get("contested_truth", False) or False,
        horizon=state.get("horizon"),
    )
    return doc.model_dump(mode="json")


async def write_analysis_doc(
    dao,
    ticker: str,
    month_id: str,
    state: dict,
) -> None:
    """Build and write the ``AnalysisDoc`` to ``analysis/{TICKER}_{MONTH_ID}``.

    Uses ``dao.set()`` (upsert) so the function is idempotent: re-running the
    screener for the same ticker + month safely overwrites the existing document.

    Args:
        dao:      StorageDAO instance.
        ticker:   Upper-case ticker symbol, e.g. ``"AAPL"``.
        month_id: Current month identifier, e.g. ``"2026-04"``.
        state:    Completed ``DebateState`` dict from ``graph.ainvoke()``.
    """
    from screener.lib.storage.schema import ANALYSIS, analysis_doc_id

    payload = build_analysis_doc(ticker=ticker, month_id=month_id, state=state)
    doc_id = analysis_doc_id(ticker, month_id)
    await dao.set(ANALYSIS, doc_id, payload)
    logger.info("analysis doc written — %s", doc_id)
