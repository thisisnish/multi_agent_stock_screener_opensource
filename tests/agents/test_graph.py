"""
tests/agents/test_graph.py — Structural tests for the debate LangGraph.

Tests verify that the graph compiles correctly and contains the expected nodes
without invoking any real LLM calls or making any I/O to Firestore.
"""

from unittest.mock import AsyncMock, MagicMock

from screener.agents.graph import build_debate_graph
from screener.lib.config_loader import (
    AppConfig,
    EmailConfig,
    LLMConfig,
    NotificationsConfig,
    StorageConfig,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _stub_config() -> AppConfig:
    """Minimal valid AppConfig for graph construction (no real credentials)."""
    return AppConfig(
        llm=LLMConfig(
            model="anthropic:claude-haiku-4-5-20251001",
        ),
        storage=StorageConfig(
            provider="firestore",
            firestore={"project_id": "test-project"},
        ),
        notifications=NotificationsConfig(
            email=EmailConfig(enabled=False),
        ),
    )


def _mock_dao() -> MagicMock:
    """Mock DAO with async stubs for all required methods."""
    dao = MagicMock()
    dao.get = AsyncMock(return_value=None)
    dao.set = AsyncMock()
    dao.vector_search = AsyncMock(return_value=[])
    dao.delete = AsyncMock()
    dao.query = AsyncMock(return_value=[])
    dao.close = AsyncMock()
    return dao


# ---------------------------------------------------------------------------
# Graph compilation tests
# ---------------------------------------------------------------------------


def test_graph_compiles():
    """build_debate_graph returns a compiled LangGraph without errors."""
    compiled = build_debate_graph(_stub_config(), _mock_dao())
    # If we get here without exception, the graph compiled successfully.
    assert compiled is not None


def test_graph_node_names():
    """Compiled graph contains all 8 expected node names."""
    compiled = build_debate_graph(_stub_config(), _mock_dao())
    graph_repr = compiled.get_graph()
    node_names = set(graph_repr.nodes.keys())

    expected_nodes = {
        "memory_read",
        "build_context",
        "debate_node",
        "conviction_node",
        "judge_node",
        "confidence_node",
        "hard_rules",
        "memory_write",
    }
    # LangGraph also adds __start__ and __end__ nodes — check our nodes are present
    assert expected_nodes.issubset(node_names), (
        f"Missing nodes: {expected_nodes - node_names}"
    )


def test_graph_different_dao_instances():
    """Graph can be built with different DAO instances without sharing state."""
    dao1 = _mock_dao()
    dao2 = _mock_dao()
    cfg = _stub_config()

    compiled1 = build_debate_graph(cfg, dao1)
    compiled2 = build_debate_graph(cfg, dao2)

    # Both should compile independently
    assert compiled1 is not None
    assert compiled2 is not None
    # They should be different objects (no shared state)
    assert compiled1 is not compiled2


def test_graph_edge_count():
    """The debate graph should have exactly 9 edges (8 user edges + __start__ injected by LangGraph)."""
    compiled = build_debate_graph(_stub_config(), _mock_dao())
    graph_repr = compiled.get_graph()
    edges = list(graph_repr.edges)
    # LangGraph injects a __start__ → memory_read edge, giving 9 total:
    # __start__→memory_read, memory_read→build_context, ..., memory_write→__end__
    assert len(edges) == 9, f"Expected 9 edges, got {len(edges)}: {edges}"
