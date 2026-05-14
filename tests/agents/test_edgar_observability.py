"""
tests/agents/test_edgar_observability.py — Tests for EDGAR RAG observability features.

Covers P2-04, P2-05, P2-06, P2-09:
- P2-04: Chunk score logging — score stats logged, disclosure doc written to Firestore
- P2-05: Empty retrieval alerting — WARN emitted, Firestore marker doc written
- P2-06: Disclosure block score annotation — [relevance: X.XX] in formatted output
- P2-09: Token budget enforcement — low-scoring chunks dropped when budget exceeded

No real LLM, EDGAR, or Firestore calls are made.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from screener.agents.prompts import build_disclosure_block
from screener.lib.config_loader import (
    AppConfig,
    EdgarConfig,
    EmailConfig,
    FirestoreConfig,
    LLMConfig,
    NotificationsConfig,
    StorageConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_config(max_disclosure_tokens: int = 2048) -> AppConfig:
    """Minimal valid AppConfig for build_context tests."""
    return AppConfig(
        llm=LLMConfig(
            model="anthropic:claude-haiku-4-5-20251001",
            embedder_model="openai:text-embedding-3-large",
        ),
        storage=StorageConfig(
            provider="firestore",
            firestore=FirestoreConfig(project_id="test-project"),
        ),
        notifications=NotificationsConfig(
            email=EmailConfig(enabled=False),
        ),
        edgar=EdgarConfig(
            freshness_days=30,
            chunk_size=512,
            chunk_overlap=0.10,
            similarity_threshold=0.7,
            top_k=5,
            max_disclosure_tokens=max_disclosure_tokens,
        ),
    )


def _mock_dao() -> MagicMock:
    """Mock DAO with async stubs."""
    dao = MagicMock()
    dao.get = AsyncMock(return_value=None)
    dao.set = AsyncMock()
    dao.vector_search = AsyncMock(return_value=[])
    dao.query = AsyncMock(return_value=[])
    dao.delete = AsyncMock()
    dao.close = AsyncMock()
    return dao


def _make_chunks(scores: list[float], text_size: int = 40) -> list[dict]:
    """Produce chunk dicts with given scores and controlled text sizes."""
    return [
        {
            "text": ("word " * text_size).strip(),
            "filing_type": "10-K",
            "filing_date": "2024-01-15",
            "_score": score,
            "filing_id": f"chunk_{i}",
        }
        for i, score in enumerate(scores)
    ]


# ---------------------------------------------------------------------------
# P2-06: Disclosure block score annotation
# ---------------------------------------------------------------------------


class TestDisclosureBlockScoreAnnotation:
    def test_relevance_annotation_present_when_score_available(self):
        """Filing header includes [relevance: X.XX] when _score is in chunk."""
        chunks = _make_chunks([0.91, 0.74])
        block = build_disclosure_block(chunks)
        assert block is not None
        assert "[relevance: 0.91]" in block
        assert "[relevance: 0.74]" in block

    def test_relevance_annotation_format_two_decimal_places(self):
        """Score annotation uses exactly two decimal places."""
        chunks = _make_chunks([0.8765])
        block = build_disclosure_block(chunks)
        assert "[relevance: 0.88]" in block

    def test_no_annotation_when_score_absent(self):
        """Header line has no relevance annotation when _score key is missing."""
        chunk = {
            "text": "Some filing text here.",
            "filing_type": "10-K",
            "filing_date": "2024-01-15",
        }
        block = build_disclosure_block([chunk])
        assert block is not None
        assert "[relevance:" not in block
        assert "10-K (2024-01-15):" in block

    def test_scores_match_chunk_order_in_output(self):
        """Annotated scores appear in passage order (highest score is passage 1)."""
        # build_disclosure_block respects input order when no max_tokens cap.
        chunks = _make_chunks([0.85, 0.72])
        block = build_disclosure_block(chunks)
        assert block is not None
        lines = block.split("\n")
        # First block header should have the first chunk's score
        header_lines = [l for l in lines if "10-K" in l and "[relevance:" in l]
        assert len(header_lines) == 2
        assert "0.85" in header_lines[0]
        assert "0.72" in header_lines[1]

    def test_annotation_at_end_of_filing_header_line(self):
        """[relevance: X.XX] appears at the end of the filing header (before the colon)."""
        chunks = _make_chunks([0.91])
        block = build_disclosure_block(chunks)
        assert block is not None
        # The format is: "[1] Filing (date) [relevance: X.XX]:\ntext"
        # After splitting by \n the first line is the full header ending with ":"
        for line in block.split("\n"):
            if line.startswith("[1]"):
                assert "[relevance: 0.91]" in line
                # Colon follows the relevance annotation
                assert line.endswith(":")
                break
        else:
            pytest.fail("Did not find [1] header line in block")

    def test_empty_chunks_returns_none(self):
        """build_disclosure_block returns None for empty or None chunks."""
        assert build_disclosure_block([]) is None
        assert build_disclosure_block(None) is None


# ---------------------------------------------------------------------------
# P2-09: Token budget enforcement
# ---------------------------------------------------------------------------


class TestTokenBudgetEnforcement:
    def test_chunks_within_budget_all_injected(self):
        """All chunks are included when cumulative tokens fit within budget."""
        # 2 chunks × 40 words × 5 chars/word ≈ 400 chars ≈ 100 tokens each → 200 total
        chunks = _make_chunks([0.9, 0.8], text_size=40)
        block = build_disclosure_block(chunks, ticker="AAPL", max_tokens=2048)
        assert block is not None
        assert "[1]" in block
        assert "[2]" in block

    def test_low_scoring_chunks_dropped_when_budget_exceeded(self):
        """Chunks exceeding the token budget are dropped, lowest score first."""
        # text_size=100 words × 5 chars = ~500 chars ≈ 125 tokens each.
        # Budget 200 → first (highest-score) chunk fits (~125 tokens),
        # second would push to ~250 tokens → dropped.
        chunks = _make_chunks([0.9, 0.7, 0.6], text_size=100)
        block = build_disclosure_block(chunks, ticker="AAPL", max_tokens=200)
        assert block is not None
        # Only one passage should appear
        assert "[1]" in block
        assert "[2]" not in block

    def test_chunks_sorted_by_score_before_budget_applied(self):
        """When budget truncates, the highest-scoring chunks are kept."""
        # Provide chunks in ascending score order — budget should keep the highest.
        # text_size=100 → ~125 tokens each; budget 200 keeps only 1.
        chunks = _make_chunks([0.6, 0.7, 0.9], text_size=100)
        block = build_disclosure_block(chunks, ticker="AAPL", max_tokens=200)
        assert block is not None
        # The retained chunk should be score 0.9 (shown as passage [1])
        assert "[relevance: 0.90]" in block

    def test_zero_max_tokens_disables_budget_cap(self):
        """max_tokens=0 means no budget is enforced — all chunks injected."""
        chunks = _make_chunks([0.9, 0.8, 0.7], text_size=500)
        block = build_disclosure_block(chunks, ticker="AAPL", max_tokens=0)
        assert block is not None
        assert "[1]" in block
        assert "[2]" in block
        assert "[3]" in block

    def test_budget_log_message_emitted(self, caplog):
        """build_disclosure_block logs injected chunk count and dropped count."""
        chunks = _make_chunks([0.9, 0.8], text_size=40)
        with caplog.at_level(logging.INFO, logger="screener.agents.prompts"):
            build_disclosure_block(chunks, ticker="AAPL", max_tokens=2048)
        # Should have logged an INFO message about injecting chunks
        assert any("Injecting" in r.message for r in caplog.records)

    def test_config_default_max_disclosure_tokens(self):
        """EdgarConfig.max_disclosure_tokens defaults to 2048."""
        cfg = EdgarConfig()
        assert cfg.max_disclosure_tokens == 2048

    def test_config_max_disclosure_tokens_validator_rejects_zero(self):
        """EdgarConfig rejects max_disclosure_tokens < 1."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            EdgarConfig(max_disclosure_tokens=0)


# ---------------------------------------------------------------------------
# P2-04 + P2-05: build_context node — Firestore writes and logging
# ---------------------------------------------------------------------------


class TestBuildContextNode:
    """Integration tests for make_build_context_node with mocked DAO and embedder."""

    def _build_node(self, dao, cfg, mock_chunks):
        """Return a build_context coroutine patching the embedder and retriever."""
        from screener.agents.nodes import make_build_context_node

        node = make_build_context_node(dao, cfg)

        # Patch embedder construction and get_disclosure_chunks_async
        with (
            patch(
                "screener.agents.nodes.get_disclosure_chunks_async",
                AsyncMock(return_value=mock_chunks),
            ),
            patch("langchain_openai.OpenAIEmbeddings", return_value=MagicMock()),
        ):
            state = {"ticker": "AAPL", "month_id": "2026-05"}
            return asyncio.run(node(state))

    def test_p2_04_firestore_doc_written_with_scores(self):
        """P2-04: Firestore doc is written with chunk scores and metadata."""
        dao = _mock_dao()
        cfg = _stub_config()
        chunks = _make_chunks([0.91, 0.74])

        from screener.agents.nodes import make_build_context_node

        node = make_build_context_node(dao, cfg)
        with (
            patch(
                "screener.agents.nodes.get_disclosure_chunks_async",
                AsyncMock(return_value=chunks),
            ),
            patch("langchain_openai.OpenAIEmbeddings", return_value=MagicMock()),
        ):
            state = {"ticker": "AAPL", "month_id": "2026-05"}
            asyncio.run(node(state))

        # dao.set should have been called for the observability doc
        assert dao.set.call_count >= 1
        # Find the call to the disclosures subcollection
        disclosure_calls = [
            c for c in dao.set.call_args_list if "disclosures" in str(c)
        ]
        assert len(disclosure_calls) == 1
        _, doc_id, payload = disclosure_calls[0].args
        assert doc_id == "2026-05"
        assert payload["chunk_count_passing_threshold"] == 2
        assert payload["status"] == "ok"
        assert "min_score" in payload
        assert "max_score" in payload
        assert "mean_score" in payload
        assert "chunks" in payload
        # Each chunk record should have _score
        for rec in payload["chunks"]:
            assert "_score" in rec

    def test_p2_04_score_stats_logged(self, caplog):
        """P2-04: Score stats are logged to stdout at INFO level."""
        dao = _mock_dao()
        cfg = _stub_config()
        chunks = _make_chunks([0.91, 0.74])

        from screener.agents.nodes import make_build_context_node

        node = make_build_context_node(dao, cfg)
        with (
            patch(
                "screener.agents.nodes.get_disclosure_chunks_async",
                AsyncMock(return_value=chunks),
            ),
            patch("langchain_openai.OpenAIEmbeddings", return_value=MagicMock()),
            caplog.at_level(logging.INFO, logger="screener.agents.nodes"),
        ):
            state = {"ticker": "AAPL", "month_id": "2026-05"}
            asyncio.run(node(state))

        score_log = [r for r in caplog.records if "chunks for AAPL" in r.message]
        assert len(score_log) >= 1
        assert "0.91" in score_log[0].message or "0.74" in score_log[0].message

    def test_p2_05_warn_logged_on_empty_retrieval(self, caplog):
        """P2-05: WARN is emitted when retrieval returns 0 chunks."""
        dao = _mock_dao()
        cfg = _stub_config()

        from screener.agents.nodes import make_build_context_node

        node = make_build_context_node(dao, cfg)
        with (
            patch(
                "screener.agents.nodes.get_disclosure_chunks_async",
                AsyncMock(return_value=[]),
            ),
            patch("langchain_openai.OpenAIEmbeddings", return_value=MagicMock()),
            caplog.at_level(logging.WARNING, logger="screener.agents.nodes"),
        ):
            state = {"ticker": "AAPL", "month_id": "2026-05"}
            result = asyncio.run(node(state))

        assert result == {"disclosure_block": None}
        warn_records = [
            r for r in caplog.records if r.levelno == logging.WARNING and "AAPL" in r.message
        ]
        assert len(warn_records) >= 1
        assert "0 chunks" in warn_records[0].message

    def test_p2_05_marker_doc_written_on_empty_retrieval(self):
        """P2-05: Firestore marker doc is written with status=empty_retrieval."""
        dao = _mock_dao()
        cfg = _stub_config()

        from screener.agents.nodes import make_build_context_node

        node = make_build_context_node(dao, cfg)
        with (
            patch(
                "screener.agents.nodes.get_disclosure_chunks_async",
                AsyncMock(return_value=[]),
            ),
            patch("langchain_openai.OpenAIEmbeddings", return_value=MagicMock()),
        ):
            state = {"ticker": "AAPL", "month_id": "2026-05"}
            asyncio.run(node(state))

        # Exactly one set call — the empty_retrieval marker
        assert dao.set.call_count == 1
        coll, doc_id, payload = dao.set.call_args.args
        assert "disclosures" in coll
        assert "AAPL" in coll
        assert doc_id == "2026-05"
        assert payload["status"] == "empty_retrieval"
        assert "run_timestamp" in payload
        assert "attempted_query" in payload

    def test_p2_05_no_chunks_no_disclosure_block_returned(self):
        """P2-05: When no chunks, disclosure_block state key is None."""
        dao = _mock_dao()
        cfg = _stub_config()

        from screener.agents.nodes import make_build_context_node

        node = make_build_context_node(dao, cfg)
        with (
            patch(
                "screener.agents.nodes.get_disclosure_chunks_async",
                AsyncMock(return_value=[]),
            ),
            patch("langchain_openai.OpenAIEmbeddings", return_value=MagicMock()),
        ):
            state = {"ticker": "AAPL", "month_id": "2026-05"}
            result = asyncio.run(node(state))

        assert result["disclosure_block"] is None

    def test_p2_09_max_disclosure_tokens_in_firestore_doc(self):
        """P2-09: max_disclosure_tokens value is recorded in Firestore observability doc."""
        dao = _mock_dao()
        cfg = _stub_config(max_disclosure_tokens=1024)
        chunks = _make_chunks([0.85])

        from screener.agents.nodes import make_build_context_node

        node = make_build_context_node(dao, cfg)
        with (
            patch(
                "screener.agents.nodes.get_disclosure_chunks_async",
                AsyncMock(return_value=chunks),
            ),
            patch("langchain_openai.OpenAIEmbeddings", return_value=MagicMock()),
        ):
            state = {"ticker": "AAPL", "month_id": "2026-05"}
            asyncio.run(node(state))

        disclosure_calls = [
            c for c in dao.set.call_args_list if "disclosures" in str(c)
        ]
        assert len(disclosure_calls) == 1
        _, _, payload = disclosure_calls[0].args
        assert payload["max_disclosure_tokens"] == 1024

    def test_disclosure_block_has_relevance_annotations(self):
        """P2-06: Disclosure block returned from build_context contains [relevance:] tags."""
        dao = _mock_dao()
        cfg = _stub_config()
        chunks = _make_chunks([0.91, 0.74])

        from screener.agents.nodes import make_build_context_node

        node = make_build_context_node(dao, cfg)
        with (
            patch(
                "screener.agents.nodes.get_disclosure_chunks_async",
                AsyncMock(return_value=chunks),
            ),
            patch("langchain_openai.OpenAIEmbeddings", return_value=MagicMock()),
        ):
            state = {"ticker": "AAPL", "month_id": "2026-05"}
            result = asyncio.run(node(state))

        block = result.get("disclosure_block")
        assert block is not None
        assert "[relevance:" in block

    def test_firestore_write_failure_does_not_crash_node(self):
        """Firestore write errors are caught — node still returns disclosure_block."""
        dao = _mock_dao()
        dao.set = AsyncMock(side_effect=Exception("Firestore unavailable"))
        cfg = _stub_config()
        chunks = _make_chunks([0.85])

        from screener.agents.nodes import make_build_context_node

        node = make_build_context_node(dao, cfg)
        with (
            patch(
                "screener.agents.nodes.get_disclosure_chunks_async",
                AsyncMock(return_value=chunks),
            ),
            patch("langchain_openai.OpenAIEmbeddings", return_value=MagicMock()),
        ):
            state = {"ticker": "AAPL", "month_id": "2026-05"}
            # Should not raise even if Firestore is unavailable
            result = asyncio.run(node(state))

        # disclosure_block should still be returned
        assert "disclosure_block" in result


# ---------------------------------------------------------------------------
# P2-09: EdgarConfig schema validation
# ---------------------------------------------------------------------------


class TestEdgarConfigSchema:
    def test_max_disclosure_tokens_default(self):
        """max_disclosure_tokens defaults to 2048."""
        cfg = EdgarConfig()
        assert cfg.max_disclosure_tokens == 2048

    def test_max_disclosure_tokens_custom_value(self):
        """max_disclosure_tokens accepts valid positive integer."""
        cfg = EdgarConfig(max_disclosure_tokens=512)
        assert cfg.max_disclosure_tokens == 512

    def test_max_disclosure_tokens_rejects_zero(self):
        """max_disclosure_tokens=0 raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            EdgarConfig(max_disclosure_tokens=0)

    def test_max_disclosure_tokens_rejects_negative(self):
        """Negative max_disclosure_tokens raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            EdgarConfig(max_disclosure_tokens=-100)

    def test_empty_retrieval_alert_threshold_default(self):
        """empty_retrieval_alert_threshold defaults to 0.20."""
        cfg = EdgarConfig()
        assert cfg.empty_retrieval_alert_threshold == 0.20

    def test_empty_retrieval_alert_threshold_accepts_one(self):
        """empty_retrieval_alert_threshold=1.0 is valid (disables alerting)."""
        cfg = EdgarConfig(empty_retrieval_alert_threshold=1.0)
        assert cfg.empty_retrieval_alert_threshold == 1.0

    def test_empty_retrieval_alert_threshold_rejects_above_one(self):
        """empty_retrieval_alert_threshold > 1.0 raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            EdgarConfig(empty_retrieval_alert_threshold=1.1)
