"""
screener/lib/llm.py — LLM factory: provider routing, per-agent overrides,
and structured-output helpers.

Public API
----------
ModelConfig
    Parsed representation of a ``provider:model_id`` config string.

init_chat_model(model_cfg: ModelConfig) -> BaseChatModel
    Instantiate the correct LangChain chat model for the given provider.

get_agent_llm(agent: str, app_config: AppConfig) -> BaseChatModel
    Resolve the per-agent model override (or fall back to default) and
    call init_chat_model().

get_structured_llm(agent: str, schema: type[BaseModel], app_config: AppConfig) -> Runnable
    Like get_agent_llm() but chains .with_structured_output(schema) on the
    result so callers can drive structured extraction directly.

Raises
------
LLMConfigError
    Raised when an unknown provider is requested or the model string is
    malformed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    # Avoid hard import at module load — LangChain is optional until first use.
    from langchain_core.language_models import BaseChatModel
    from langchain_core.runnables import Runnable

from screener.lib.config import AppConfig

# ---------------------------------------------------------------------------
# Public exception
# ---------------------------------------------------------------------------


class LLMConfigError(ValueError):
    """Raised when an unknown provider is requested or model string is malformed."""


# ---------------------------------------------------------------------------
# ModelConfig — parsed representation of "provider:model_id"
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ModelConfig:
    """Parsed form of a ``provider:model_id`` config string.

    Attributes:
        provider: Lower-cased provider name (e.g. ``"anthropic"``).
        model_id: The model identifier passed to the provider
                  (e.g. ``"claude-haiku-4-5-20251001"``).
        raw: The original unparsed string, preserved for error messages.
    """

    provider: str
    model_id: str
    raw: str

    @classmethod
    def from_string(cls, s: str) -> "ModelConfig":
        """Parse ``provider:model_id`` into a :class:`ModelConfig`.

        Args:
            s: A string in the form ``"provider:model_id"``.

        Returns:
            A :class:`ModelConfig` instance.

        Raises:
            LLMConfigError: If the string does not contain exactly one ``:``.
        """
        if ":" not in s:
            raise LLMConfigError(
                f"LLM model string must use 'provider:model_id' format, got: '{s}'"
            )
        provider, _, model_id = s.partition(":")
        return cls(provider=provider.strip().lower(), model_id=model_id.strip(), raw=s)


# ---------------------------------------------------------------------------
# Supported providers
# ---------------------------------------------------------------------------

_KNOWN_PROVIDERS = frozenset(
    {"anthropic", "openai", "google_genai", "groq", "ollama"}
)


# ---------------------------------------------------------------------------
# P1-03a — Provider routing
# ---------------------------------------------------------------------------


def init_chat_model(model_cfg: ModelConfig) -> "BaseChatModel":
    """Instantiate the correct LangChain chat model for *model_cfg*.

    Routes to:
    - ``anthropic``    → :class:`langchain_anthropic.ChatAnthropic`
    - ``openai``       → :class:`langchain_openai.ChatOpenAI`
    - ``google_genai`` → :class:`langchain_google_genai.ChatGoogleGenerativeAI`
    - ``groq``         → :class:`langchain_groq.ChatGroq`
    - ``ollama``       → :class:`langchain_ollama.ChatOllama` (no API key)

    Args:
        model_cfg: A :class:`ModelConfig` produced by
                   :meth:`ModelConfig.from_string`.

    Returns:
        An instantiated :class:`~langchain_core.language_models.BaseChatModel`.

    Raises:
        LLMConfigError: If ``model_cfg.provider`` is not one of the supported
                        providers listed above.
    """
    provider = model_cfg.provider

    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic  # type: ignore[import]

        return ChatAnthropic(model=model_cfg.model_id)  # type: ignore[call-arg]

    if provider == "openai":
        from langchain_openai import ChatOpenAI  # type: ignore[import]

        return ChatOpenAI(model=model_cfg.model_id)

    if provider == "google_genai":
        from langchain_google_genai import ChatGoogleGenerativeAI  # type: ignore[import]

        return ChatGoogleGenerativeAI(model=model_cfg.model_id)

    if provider == "groq":
        from langchain_groq import ChatGroq  # type: ignore[import]

        return ChatGroq(model=model_cfg.model_id)

    if provider == "ollama":
        from langchain_ollama import ChatOllama  # type: ignore[import]

        return ChatOllama(model=model_cfg.model_id)

    raise LLMConfigError(
        f"Unknown LLM provider '{provider}' (from model string '{model_cfg.raw}'). "
        f"Supported providers: {sorted(_KNOWN_PROVIDERS)}."
    )


# ---------------------------------------------------------------------------
# P1-03b — Per-agent model override
# ---------------------------------------------------------------------------

#: Maps agent name to the attribute name on :class:`~screener.lib.config.LLMConfig`
#: that holds its optional override string.
_AGENT_OVERRIDE_ATTR: dict[str, str] = {
    "bull": "bull_model",
    "bear": "bear_model",
    "judge": "judge_model",
    "narrator": "narrator_model",
    "embedder": "embedder_model",
}


def get_agent_llm(agent: str, app_config: AppConfig) -> "BaseChatModel":
    """Return the instantiated LLM for *agent*, respecting per-agent overrides.

    Resolution order:
    1. If ``app_config.llm.<agent>_model`` is set, use that.
    2. Otherwise fall back to ``app_config.llm.model`` (the default).

    Args:
        agent: One of ``"bull"``, ``"bear"``, ``"judge"``, ``"narrator"``,
               ``"embedder"``.
        app_config: The validated application config from :func:`load_config`.

    Returns:
        An instantiated :class:`~langchain_core.language_models.BaseChatModel`.

    Raises:
        LLMConfigError: If *agent* is not a recognised agent name, or if the
                        resolved model string is malformed / uses an unknown
                        provider.
    """
    if agent not in _AGENT_OVERRIDE_ATTR:
        raise LLMConfigError(
            f"Unknown agent '{agent}'. "
            f"Recognised agents: {sorted(_AGENT_OVERRIDE_ATTR)}."
        )

    override_attr = _AGENT_OVERRIDE_ATTR[agent]
    override_str: str | None = getattr(app_config.llm, override_attr)

    model_str = override_str if override_str is not None else app_config.llm.model
    model_cfg = ModelConfig.from_string(model_str)
    return init_chat_model(model_cfg)


# ---------------------------------------------------------------------------
# P1-03c — Structured output
# ---------------------------------------------------------------------------


def get_structured_llm(
    agent: str,
    schema: type[BaseModel],
    app_config: AppConfig,
) -> "Runnable":
    """Return a runnable that forces structured output conforming to *schema*.

    Calls :func:`get_agent_llm` then chains
    ``.with_structured_output(schema)`` so callers get typed Pydantic objects
    back from every invocation.

    Args:
        agent: Agent name passed through to :func:`get_agent_llm`.
        schema: A Pydantic :class:`~pydantic.BaseModel` subclass that defines
                the desired output structure.
        app_config: The validated application config from :func:`load_config`.

    Returns:
        A LangChain :class:`~langchain_core.runnables.Runnable` that yields
        instances of *schema*.

    Raises:
        LLMConfigError: Propagated from :func:`get_agent_llm`.
    """
    llm = get_agent_llm(agent, app_config)
    return llm.with_structured_output(schema)
