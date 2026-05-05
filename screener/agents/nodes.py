"""
screener/agents/nodes.py — LangGraph node functions for the debate graph.

All I/O nodes (memory_read, build_context, debate_node, judge_node, memory_write)
are built as async closures that capture app_config and/or dao at graph-build time.
Pure computation nodes (conviction_node, confidence_node, hard_rules) are plain
synchronous functions that accept only DebateState.

Node factory functions (make_*) follow the pattern:
    make_<node_name>(dao, app_config) -> async callable

LangGraph node signature:
    async def node(state: DebateState) -> dict   # returns partial state update
    def node(state: DebateState) -> dict          # sync variant for pure nodes

Public API
----------
make_memory_read_node(dao) -> Callable
make_build_context_node(dao, app_config) -> Callable
make_debate_node(app_config) -> Callable
conviction_node(state) -> dict
make_judge_node(app_config) -> Callable
confidence_node(state) -> dict
hard_rules(state) -> dict
make_memory_write_node(dao) -> Callable
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from langchain_core.messages import HumanMessage, SystemMessage

from screener.agents.prompts import (
    BEAR_SYSTEM_PROMPT,
    BULL_SYSTEM_PROMPT,
    JUDGE_SYSTEM_PROMPT,
    build_disclosure_block,
    build_judge_context,
    build_ticker_context,
)
from screener.agents.state import DebateState
from screener.edgar.retriever import get_disclosure_chunks_async
from screener.lib.agent_creator import get_structured_llm
from screener.lib.models import BearCaseOutput, BullCaseOutput, JudgeOutput
from screener.lib.storage.schema import memory_collection_path, memory_doc_id
from screener.metrics.confidence_scorer import score_judge_confidence
from screener.metrics.conviction_scorer import score_conviction

if TYPE_CHECKING:
    from screener.lib.config_loader import AppConfig
    from screener.lib.storage.base import StorageDAO

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# memory_read
# ---------------------------------------------------------------------------


def make_memory_read_node(dao: "StorageDAO"):
    """Build the memory_read node, capturing the DAO.

    Fetches the per-ticker episodic memory document and unpacks scoring_weights
    and prior_months for downstream nodes.

    Args:
        dao: StorageDAO implementation.

    Returns:
        Async node function compatible with LangGraph.
    """

    async def memory_read(state: DebateState) -> dict:
        ticker = state["ticker"]
        month_id = state["month_id"]
        col_path = memory_collection_path(ticker)

        # Fetch all month docs from tickers/{SYMBOL}/memory subcollection
        all_month_docs: list[dict] = await dao.query(col_path, {})

        if not all_month_docs:
            logger.debug("No memory docs found for ticker=%s", ticker)
            return {
                "memory_doc": None,
                "scoring_weights": None,
                "prior_months": {},
            }

        # Build prior_months map: {month_id: verdict_dict}
        # Exclude the current month (we're writing it this run)
        prior_months: dict = {}
        scoring_weights = None
        current_month_doc = None

        for doc in all_month_docs:
            doc_month = doc.get("month_id")
            if doc_month == month_id:
                current_month_doc = doc
                scoring_weights = doc.get("scoring_weights")
            elif doc_month and doc.get("verdict"):
                prior_months[doc_month] = doc["verdict"]

        # If no current month doc yet, pull scoring_weights from most recent past month
        if scoring_weights is None and all_month_docs:
            sorted_docs = sorted(
                (d for d in all_month_docs if d.get("month_id")),
                key=lambda d: d["month_id"],
                reverse=True,
            )
            if sorted_docs:
                scoring_weights = sorted_docs[0].get("scoring_weights")

        logger.debug(
            "Loaded memory for ticker=%s: %d prior months", ticker, len(prior_months)
        )
        return {
            "memory_doc": current_month_doc,
            "scoring_weights": scoring_weights,
            "prior_months": prior_months,
        }

    return memory_read


# ---------------------------------------------------------------------------
# build_context
# ---------------------------------------------------------------------------


def make_build_context_node(dao: "StorageDAO", app_config: "AppConfig"):
    """Build the build_context node, capturing DAO and config.

    Retrieves EDGAR disclosure chunks via vector search and formats them
    into a disclosure_block string for injection into the debate context.

    Args:
        dao: StorageDAO implementation.
        app_config: Validated AppConfig (used to get the embedder model).

    Returns:
        Async node function compatible with LangGraph.
    """

    async def build_context(state: DebateState) -> dict:
        ticker = state["ticker"]
        edgar_cfg = app_config.edgar

        # Lazy import — route on provider prefix to select the correct embedder package
        raw_model = app_config.llm.embedder_model
        provider, model_id = (
            raw_model.split(":", 1) if ":" in raw_model else ("google_genai", raw_model)
        )

        try:
            if provider == "openai":
                from langchain_openai import OpenAIEmbeddings  # type: ignore[import]

                embedder = OpenAIEmbeddings(model=model_id)
            elif provider == "google_genai":
                from langchain_google_genai import GoogleGenerativeAIEmbeddings  # type: ignore[import]

                embedder = GoogleGenerativeAIEmbeddings(model=model_id)
            else:
                logger.warning(
                    "Unknown embedder provider '%s'; skipping EDGAR retrieval for %s",
                    provider,
                    ticker,
                )
                return {"disclosure_block": None}
        except ImportError:
            logger.warning(
                "Embedder package for provider '%s' not installed; skipping EDGAR retrieval for %s",
                provider,
                ticker,
            )
            return {"disclosure_block": None}

        chunks = await get_disclosure_chunks_async(
            ticker,
            dao,
            embedder,
            top_k=edgar_cfg.top_k,
            threshold=edgar_cfg.similarity_threshold,
        )
        disclosure_block = build_disclosure_block(chunks)

        if disclosure_block:
            logger.debug(
                "Built disclosure block for %s: %d chunk(s)", ticker, len(chunks)
            )
        else:
            logger.debug("No EDGAR chunks above threshold for ticker=%s", ticker)

        return {"disclosure_block": disclosure_block}

    return build_context


# ---------------------------------------------------------------------------
# debate_node
# ---------------------------------------------------------------------------


def make_debate_node(app_config: "AppConfig"):
    """Build the debate_node, capturing app_config.

    Runs Bull and Bear agents in parallel using asyncio.gather,
    injecting signal data and EDGAR disclosures into both contexts.

    Args:
        app_config: Validated AppConfig.

    Returns:
        Async node function compatible with LangGraph.
    """

    async def debate_node(state: DebateState) -> dict:
        ticker = state["ticker"]
        ticker_name = state.get("ticker_name", ticker)
        signals = state.get("signals", {})
        disclosure_block = state.get("disclosure_block")

        context = build_ticker_context(
            ticker,
            ticker_name,
            signals,
            news=None,  # news agent is separate; inject NEUTRAL placeholder
            disclosure_block=disclosure_block,
        )

        bull_llm = get_structured_llm("bull", BullCaseOutput, app_config)
        bear_llm = get_structured_llm("bear", BearCaseOutput, app_config)

        bull_messages = [
            SystemMessage(content=BULL_SYSTEM_PROMPT),
            HumanMessage(content=context),
        ]
        bear_messages = [
            SystemMessage(content=BEAR_SYSTEM_PROMPT),
            HumanMessage(content=context),
        ]

        bull_output, bear_output = await asyncio.gather(
            bull_llm.ainvoke(bull_messages),
            bear_llm.ainvoke(bear_messages),
        )
        outputs = {"bull": bull_output, "bear": bear_output}

        logger.debug(
            "Debate complete for ticker=%s: bull_citations=%s bear_citations=%s",
            ticker,
            outputs["bull"].signal_citations,
            outputs["bear"].signal_citations,
        )

        return {
            "bull_output": outputs["bull"],
            "bear_output": outputs["bear"],
        }

    return debate_node


# ---------------------------------------------------------------------------
# conviction_node (pure sync)
# ---------------------------------------------------------------------------


def conviction_node(state: DebateState) -> dict:
    """Compute white-box conviction scores for both sides.

    Pure function — no I/O, no LLM calls. Scores are derived entirely from
    the structure and content of Bull/Bear outputs.

    Args:
        state: Current DebateState.

    Returns:
        Partial state update with bull_conviction and bear_conviction.
    """
    bull_conv = score_conviction(state["bull_output"], "bull")
    bear_conv = score_conviction(state["bear_output"], "bear")

    logger.debug("Conviction scores: bull=%.1f bear=%.1f", bull_conv, bear_conv)

    return {"bull_conviction": bull_conv, "bear_conviction": bear_conv}


# ---------------------------------------------------------------------------
# judge_node
# ---------------------------------------------------------------------------


def make_judge_node(app_config: "AppConfig"):
    """Build the judge_node, capturing app_config.

    Invokes the Judge LLM with the full debate context and enriches the
    JudgeOutput with conviction scores and citation lists.

    Args:
        app_config: Validated AppConfig.

    Returns:
        Async node function compatible with LangGraph.
    """

    async def judge_node(state: DebateState) -> dict:
        ticker = state["ticker"]
        ticker_name = state.get("ticker_name", ticker)
        bull_output: BullCaseOutput = state["bull_output"]
        bear_output: BearCaseOutput = state["bear_output"]

        judge_context = build_judge_context(
            ticker,
            ticker_name,
            bull_output,
            bear_output,
            scoring_weights=state.get("scoring_weights"),
            eval_context=state.get("eval_context"),
            bull_conviction=state.get("bull_conviction"),
            bear_conviction=state.get("bear_conviction"),
            prior_months=state.get("prior_months") or None,
        )

        judge_llm = get_structured_llm("judge", JudgeOutput, app_config)
        messages = [
            SystemMessage(content=JUDGE_SYSTEM_PROMPT),
            HumanMessage(content=judge_context),
        ]
        judge_output: JudgeOutput = await judge_llm.ainvoke(messages)

        # Enrich with conviction scores and citation lists (white-box data)
        judge_output.bull_conviction_score = state.get("bull_conviction")
        judge_output.bear_conviction_score = state.get("bear_conviction")
        judge_output.bull_signal_citations = bull_output.signal_citations
        judge_output.bear_signal_citations = bear_output.signal_citations

        logger.debug(
            "Judge verdict for %s: action=%s margin=%s conviction_b=%.1f/%.1f",
            ticker,
            judge_output.action,
            judge_output.margin_of_victory,
            state.get("bull_conviction", 0.0),
            state.get("bear_conviction", 0.0),
        )

        return {"judge_output": judge_output}

    return judge_node


# ---------------------------------------------------------------------------
# confidence_node (pure sync)
# ---------------------------------------------------------------------------


def confidence_node(state: DebateState) -> dict:
    """Compute the white-box confidence score and contested_truth flag.

    Pure function — no I/O. Delegates entirely to score_judge_confidence().

    Args:
        state: Current DebateState.

    Returns:
        Partial state update with confidence_score and contested_truth.
    """
    confidence_score, contested_truth, _ = score_judge_confidence(
        state["judge_output"],
        bull_conviction=state.get("bull_conviction"),
        bear_conviction=state.get("bear_conviction"),
    )

    logger.debug(
        "Confidence score=%.1f contested_truth=%s",
        confidence_score,
        contested_truth,
    )

    return {"confidence_score": confidence_score, "contested_truth": contested_truth}


# ---------------------------------------------------------------------------
# hard_rules (pure sync)
# ---------------------------------------------------------------------------


def _assign_horizon(confidence: float) -> str:
    """Assign a holding horizon based on the confidence score.

    Args:
        confidence: Float in [0.0, 100.0].

    Returns:
        "90d" for confidence >= 75, "60d" for >= 55, "30d" otherwise.
    """
    if confidence >= 75.0:
        return "90d"
    if confidence >= 55.0:
        return "60d"
    return "30d"


def hard_rules(state: DebateState) -> dict:
    """Apply hard override rules to the Judge's verdict.

    Rules:
      - If confidence_score < 40: force action to HOLD with 30d horizon,
        regardless of what the Judge decided.
      - Otherwise: pass through Judge's action, compute horizon from confidence.

    Args:
        state: Current DebateState.

    Returns:
        Partial state update with final_action and horizon.
    """
    action = state["judge_output"].action
    confidence = state.get("confidence_score", 50.0)

    if confidence < 40.0:
        logger.debug("hard_rules: forcing HOLD (confidence=%.1f < 40)", confidence)
        return {"final_action": "HOLD", "horizon": "30d"}

    horizon = _assign_horizon(confidence)
    logger.debug(
        "hard_rules: passing through action=%s horizon=%s (confidence=%.1f)",
        action,
        horizon,
        confidence,
    )
    return {"final_action": action, "horizon": horizon}


# ---------------------------------------------------------------------------
# memory_write
# ---------------------------------------------------------------------------


def make_memory_write_node(dao: "StorageDAO"):
    """Build the memory_write node, capturing the DAO.

    Persists the current month's verdict into the ticker's episodic memory
    document. Uses set() which is a full upsert per the StorageDAO contract.

    Args:
        dao: StorageDAO implementation.

    Returns:
        Async node function compatible with LangGraph.
    """

    async def memory_write(state: DebateState) -> dict:
        ticker = state["ticker"]
        month_id = state["month_id"]
        existing_doc: dict = dict(state.get("memory_doc") or {})

        verdict = {
            "action": state["final_action"],
            "confidence": (state.get("confidence_score") or 50.0) / 100.0,
            "horizon": state["horizon"],
            "winning_side": state["judge_output"].winning_side,
            "entry_price": state["signals"].get("price"),
            "judge_signal_citations": state["judge_output"].judge_signal_citations,
        }

        new_doc = {
            "ticker": ticker,
            "month_id": month_id,
            "scoring_weights": existing_doc.get(
                "scoring_weights",
                {"bull_weight": 0.5, "bear_weight": 0.5, "sample_size": 0},
            ),
            "verdict": verdict,
        }

        col_path = memory_collection_path(ticker)
        await dao.set(col_path, memory_doc_id(month_id), new_doc)

        logger.debug(
            "Memory written for ticker=%s month_id=%s action=%s",
            ticker,
            month_id,
            state["final_action"],
        )

        return {}

    return memory_write
