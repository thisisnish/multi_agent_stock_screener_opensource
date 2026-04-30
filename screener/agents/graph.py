"""
screener/agents/graph.py — LangGraph debate StateGraph builder and run helper.

Assembles the 8-node linear debate graph:
    memory_read → build_context → debate_node → conviction_node →
    judge_node → confidence_node → hard_rules → memory_write → END

Public API
----------
build_debate_graph(app_config, dao) -> CompiledGraph
run_debate(ticker, ticker_name, signals, month_id, app_config, dao,
           eval_context) -> DebateState
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from langgraph.graph import END, StateGraph  # type: ignore[import]

from screener.agents.nodes import (
    confidence_node,
    conviction_node,
    hard_rules,
    make_build_context_node,
    make_debate_node,
    make_judge_node,
    make_memory_read_node,
    make_memory_write_node,
)
from screener.agents.state import DebateState

if TYPE_CHECKING:
    from screener.lib.config_loader import AppConfig
    from screener.lib.storage.base import StorageDAO


def build_debate_graph(app_config: "AppConfig", dao: "StorageDAO"):
    """Build and compile the 8-node debate StateGraph.

    Args:
        app_config: Validated AppConfig — passed through to LLM node factories.
        dao: StorageDAO implementation — passed through to I/O node factories.

    Returns:
        A compiled LangGraph graph ready for invocation.
    """
    graph = StateGraph(DebateState)

    graph.add_node("memory_read", make_memory_read_node(dao))
    graph.add_node("build_context", make_build_context_node(dao, app_config))
    graph.add_node("debate_node", make_debate_node(app_config))
    graph.add_node("conviction_node", conviction_node)
    graph.add_node("judge_node", make_judge_node(app_config))
    graph.add_node("confidence_node", confidence_node)
    graph.add_node("hard_rules", hard_rules)
    graph.add_node("memory_write", make_memory_write_node(dao))

    graph.set_entry_point("memory_read")
    graph.add_edge("memory_read", "build_context")
    graph.add_edge("build_context", "debate_node")
    graph.add_edge("debate_node", "conviction_node")
    graph.add_edge("conviction_node", "judge_node")
    graph.add_edge("judge_node", "confidence_node")
    graph.add_edge("confidence_node", "hard_rules")
    graph.add_edge("hard_rules", "memory_write")
    graph.add_edge("memory_write", END)

    return graph.compile()


def run_debate(
    ticker: str,
    ticker_name: str,
    signals: dict,
    month_id: str,
    app_config: "AppConfig",
    dao: "StorageDAO",
    eval_context: dict | None = None,
) -> DebateState:
    """Run the full debate for a single ticker and return the final DebateState.

    This is a synchronous wrapper intended for use in the main screener job.
    The underlying graph contains async nodes; LangGraph's compiled .invoke()
    handles the event loop internally.

    Args:
        ticker: Upper-case ticker symbol, e.g. "AAPL".
        ticker_name: Human-readable company name.
        signals: Composite-scored dict from the scoring engine.
        month_id: Month identifier in "YYYY-MM" format, e.g. "2026-04".
        app_config: Validated AppConfig.
        dao: StorageDAO implementation.
        eval_context: Optional eval feedback from the prior month.

    Returns:
        The final DebateState after all 8 nodes have run.
    """
    compiled = build_debate_graph(app_config, dao)
    initial_state: DebateState = {
        "ticker": ticker,
        "ticker_name": ticker_name,
        "signals": signals,
        "month_id": month_id,
        "eval_context": eval_context,
    }
    return compiled.invoke(initial_state)
