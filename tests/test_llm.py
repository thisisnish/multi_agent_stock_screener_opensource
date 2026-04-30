"""
tests/test_llm.py — Unit tests for screener/lib/agent_creator.py

Covers:
- ModelConfig.from_string: happy path and malformed string
- init_chat_model: each of the 5 providers routes to the correct class
- init_chat_model: unknown provider raises LLMConfigError
- get_agent_llm: per-agent override is used when set
- get_agent_llm: falls back to default model when no override is set
- get_agent_llm: unknown agent name raises LLMConfigError
- get_structured_llm: calls .with_structured_output with the given schema

All LangChain provider constructors are mocked — no real API calls are made.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from screener.lib.config_loader import AppConfig, LLMConfig
from screener.lib.agent_creator import (
    LLMConfigError,
    ModelConfig,
    get_agent_llm,
    get_structured_llm,
    init_chat_model,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app_config(**llm_kwargs) -> AppConfig:
    """Build a minimal AppConfig wired to a firestore backend with email off."""
    from screener.lib.config_loader import (
        EdgarConfig,
        EmailConfig,
        FirestoreConfig,
        NotificationsConfig,
        ScreenerConfig,
        SignalsConfig,
        StorageConfig,
    )

    llm = LLMConfig(**llm_kwargs)
    storage = StorageConfig(
        provider="firestore",
        firestore=FirestoreConfig(project_id="test-project"),
    )
    notifications = NotificationsConfig(
        email=EmailConfig(enabled=False, recipients=[])
    )
    return AppConfig(
        llm=llm,
        storage=storage,
        signals=SignalsConfig(),
        screener=ScreenerConfig(),
        notifications=notifications,
        edgar=EdgarConfig(),
    )


# ---------------------------------------------------------------------------
# ModelConfig.from_string
# ---------------------------------------------------------------------------


class TestModelConfigFromString:
    def test_happy_path_anthropic(self):
        cfg = ModelConfig.from_string("anthropic:claude-haiku-4-5-20251001")
        assert cfg.provider == "anthropic"
        assert cfg.model_id == "claude-haiku-4-5-20251001"
        assert cfg.raw == "anthropic:claude-haiku-4-5-20251001"

    def test_happy_path_google_genai_model_id_with_path(self):
        """model_id may itself contain slashes (e.g. Gemini embedding)."""
        cfg = ModelConfig.from_string("google_genai:models/gemini-embedding-001")
        assert cfg.provider == "google_genai"
        assert cfg.model_id == "models/gemini-embedding-001"

    def test_provider_is_lowercased(self):
        cfg = ModelConfig.from_string("Anthropic:claude-haiku-4-5-20251001")
        assert cfg.provider == "anthropic"

    def test_missing_colon_raises(self):
        with pytest.raises(LLMConfigError, match="provider:model_id"):
            ModelConfig.from_string("anthropic-claude-haiku")

    def test_empty_string_raises(self):
        with pytest.raises(LLMConfigError):
            ModelConfig.from_string("")


# ---------------------------------------------------------------------------
# init_chat_model — provider routing
# ---------------------------------------------------------------------------


class TestInitChatModelProviderRouting:
    """Each provider must route to the correct LangChain class constructor."""

    def _make_cfg(self, provider: str, model_id: str = "some-model") -> ModelConfig:
        return ModelConfig(provider=provider, model_id=model_id, raw=f"{provider}:{model_id}")

    def test_anthropic_routes_to_chat_anthropic(self):
        mock_cls = MagicMock()
        with patch("langchain_anthropic.ChatAnthropic", mock_cls):
            init_chat_model(self._make_cfg("anthropic", "claude-haiku-4-5-20251001"))
        mock_cls.assert_called_once_with(model="claude-haiku-4-5-20251001")

    def test_openai_routes_to_chat_openai(self):
        mock_cls = MagicMock()
        with patch("langchain_openai.ChatOpenAI", mock_cls):
            init_chat_model(self._make_cfg("openai", "gpt-4o-mini"))
        mock_cls.assert_called_once_with(model="gpt-4o-mini")

    def test_google_genai_routes_to_chat_google_generative_ai(self):
        mock_cls = MagicMock()
        with patch("langchain_google_genai.ChatGoogleGenerativeAI", mock_cls):
            init_chat_model(self._make_cfg("google_genai", "gemini-2.0-flash"))
        mock_cls.assert_called_once_with(model="gemini-2.0-flash")

    def test_groq_routes_to_chat_groq(self):
        mock_cls = MagicMock()
        with patch("langchain_groq.ChatGroq", mock_cls):
            init_chat_model(self._make_cfg("groq", "llama-3.1-8b-instant"))
        mock_cls.assert_called_once_with(model="llama-3.1-8b-instant")

    def test_ollama_routes_to_chat_ollama(self):
        mock_cls = MagicMock()
        with patch("langchain_ollama.ChatOllama", mock_cls):
            init_chat_model(self._make_cfg("ollama", "llama3"))
        mock_cls.assert_called_once_with(model="llama3")

    def test_unknown_provider_raises_llm_config_error(self):
        cfg = ModelConfig(provider="bedrock", model_id="titan", raw="bedrock:titan")
        with pytest.raises(LLMConfigError, match="bedrock"):
            init_chat_model(cfg)

    def test_unknown_provider_error_message_lists_supported_providers(self):
        cfg = ModelConfig(provider="bedrock", model_id="titan", raw="bedrock:titan")
        with pytest.raises(LLMConfigError, match="anthropic"):
            init_chat_model(cfg)

    def test_returns_value_from_constructor(self):
        """init_chat_model must return whatever the constructor returns."""
        fake_llm = MagicMock(name="fake_llm_instance")
        mock_cls = MagicMock(return_value=fake_llm)
        with patch("langchain_anthropic.ChatAnthropic", mock_cls):
            result = init_chat_model(self._make_cfg("anthropic"))
        assert result is fake_llm


# ---------------------------------------------------------------------------
# get_agent_llm — per-agent overrides and fallback
# ---------------------------------------------------------------------------


class TestGetAgentLlm:
    """Per-agent override should be used when set; default used when not."""

    def _patch_init(self):
        """Return a patcher for init_chat_model at its import location."""
        return patch("screener.lib.agent_creator.init_chat_model")

    def test_bull_override_used_when_set(self):
        app_cfg = _make_app_config(
            model="anthropic:claude-haiku-4-5-20251001",
            bull_model="openai:gpt-4o-mini",
        )
        with self._patch_init() as mock_init:
            get_agent_llm("bull", app_cfg)
        called_cfg: ModelConfig = mock_init.call_args[0][0]
        assert called_cfg.provider == "openai"
        assert called_cfg.model_id == "gpt-4o-mini"

    def test_bear_override_used_when_set(self):
        app_cfg = _make_app_config(
            model="anthropic:claude-haiku-4-5-20251001",
            bear_model="groq:llama-3.1-8b-instant",
        )
        with self._patch_init() as mock_init:
            get_agent_llm("bear", app_cfg)
        called_cfg: ModelConfig = mock_init.call_args[0][0]
        assert called_cfg.provider == "groq"

    def test_judge_override_used_when_set(self):
        app_cfg = _make_app_config(
            model="anthropic:claude-haiku-4-5-20251001",
            judge_model="anthropic:claude-sonnet-4-5-20251015",
        )
        with self._patch_init() as mock_init:
            get_agent_llm("judge", app_cfg)
        called_cfg: ModelConfig = mock_init.call_args[0][0]
        assert called_cfg.model_id == "claude-sonnet-4-5-20251015"

    def test_narrator_override_used_when_set(self):
        app_cfg = _make_app_config(
            model="anthropic:claude-haiku-4-5-20251001",
            narrator_model="google_genai:gemini-2.0-flash",
        )
        with self._patch_init() as mock_init:
            get_agent_llm("narrator", app_cfg)
        called_cfg: ModelConfig = mock_init.call_args[0][0]
        assert called_cfg.provider == "google_genai"

    def test_embedder_uses_embedder_model_field(self):
        app_cfg = _make_app_config(
            model="anthropic:claude-haiku-4-5-20251001",
            embedder_model="google_genai:models/gemini-embedding-001",
        )
        with self._patch_init() as mock_init:
            get_agent_llm("embedder", app_cfg)
        called_cfg: ModelConfig = mock_init.call_args[0][0]
        assert called_cfg.provider == "google_genai"
        assert called_cfg.model_id == "models/gemini-embedding-001"

    def test_falls_back_to_default_when_bull_override_is_none(self):
        app_cfg = _make_app_config(
            model="anthropic:claude-haiku-4-5-20251001",
            bull_model=None,
        )
        with self._patch_init() as mock_init:
            get_agent_llm("bull", app_cfg)
        called_cfg: ModelConfig = mock_init.call_args[0][0]
        assert called_cfg.provider == "anthropic"
        assert called_cfg.model_id == "claude-haiku-4-5-20251001"

    def test_falls_back_to_default_when_bear_override_is_none(self):
        app_cfg = _make_app_config(
            model="openai:gpt-4o-mini",
            bear_model=None,
        )
        with self._patch_init() as mock_init:
            get_agent_llm("bear", app_cfg)
        called_cfg: ModelConfig = mock_init.call_args[0][0]
        assert called_cfg.provider == "openai"
        assert called_cfg.model_id == "gpt-4o-mini"

    def test_unknown_agent_raises_llm_config_error(self):
        app_cfg = _make_app_config(model="anthropic:claude-haiku-4-5-20251001")
        with pytest.raises(LLMConfigError, match="foobar"):
            get_agent_llm("foobar", app_cfg)

    def test_unknown_agent_error_lists_known_agents(self):
        app_cfg = _make_app_config(model="anthropic:claude-haiku-4-5-20251001")
        with pytest.raises(LLMConfigError, match="bull"):
            get_agent_llm("unknown_agent", app_cfg)


# ---------------------------------------------------------------------------
# get_structured_llm — structured output chaining
# ---------------------------------------------------------------------------


class SampleSchema(BaseModel):
    verdict: str
    confidence: int


class TestGetStructuredLlm:
    def test_calls_with_structured_output_with_schema(self):
        """get_structured_llm must chain .with_structured_output(schema)."""
        fake_llm = MagicMock(name="fake_llm")
        fake_runnable = MagicMock(name="fake_runnable")
        fake_llm.with_structured_output.return_value = fake_runnable

        app_cfg = _make_app_config(model="anthropic:claude-haiku-4-5-20251001")

        with patch("screener.lib.agent_creator.get_agent_llm", return_value=fake_llm) as mock_get:
            result = get_structured_llm("bull", SampleSchema, app_cfg)

        # get_agent_llm was called with the right arguments
        mock_get.assert_called_once_with("bull", app_cfg)

        # .with_structured_output was called with the schema class
        fake_llm.with_structured_output.assert_called_once_with(SampleSchema)

        # The return value is the chained runnable
        assert result is fake_runnable

    def test_returns_runnable_from_with_structured_output(self):
        """The return value must be exactly what .with_structured_output() returns."""
        fake_llm = MagicMock(name="fake_llm")
        sentinel = object()
        fake_llm.with_structured_output.return_value = sentinel

        app_cfg = _make_app_config(model="anthropic:claude-haiku-4-5-20251001")

        with patch("screener.lib.agent_creator.get_agent_llm", return_value=fake_llm):
            result = get_structured_llm("judge", SampleSchema, app_cfg)

        assert result is sentinel

    def test_llm_config_error_propagates(self):
        """LLMConfigError from get_agent_llm must propagate out of get_structured_llm."""
        app_cfg = _make_app_config(model="anthropic:claude-haiku-4-5-20251001")
        with pytest.raises(LLMConfigError):
            get_structured_llm("not_a_real_agent", SampleSchema, app_cfg)
