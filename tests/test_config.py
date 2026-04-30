"""
tests/test_config.py — Unit tests for screener/lib/config_loader.py

Covers:
- Happy path: valid YAML parses into AppConfig with correct values
- Env-var interpolation: ${VAR} resolved from os.environ
- Missing env var: raises ConfigError with clear message
- Missing config file: raises ConfigError
- Invalid YAML: raises ConfigError
- Pydantic validation failures: weights != 1.0, unknown provider, bad overlap
- Defaults: missing sections produce sensible defaults
- Active backend validation: firestore without project_id fails
- Email recipients required when enabled
"""

import os
import textwrap
from pathlib import Path

import pytest

from screener.lib.config_loader import AppConfig, ConfigError, load_config, _interpolate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# _interpolate unit tests
# ---------------------------------------------------------------------------


def test_interpolate_resolves_env_var(monkeypatch):
    monkeypatch.setenv("MY_KEY", "secret123")
    assert _interpolate("${MY_KEY}") == "secret123"


def test_interpolate_leaves_non_placeholder_unchanged():
    assert _interpolate("plain string") == "plain string"


def test_interpolate_nested_dict(monkeypatch):
    monkeypatch.setenv("PROJECT", "my-proj")
    result = _interpolate({"project_id": "${PROJECT}", "database": "db"})
    assert result == {"project_id": "my-proj", "database": "db"}


def test_interpolate_list(monkeypatch):
    monkeypatch.setenv("ADDR", "a@b.com")
    assert _interpolate(["${ADDR}", "static@x.com"]) == ["a@b.com", "static@x.com"]


def test_interpolate_missing_env_var_raises():
    os.environ.pop("DEFINITELY_NOT_SET", None)
    with pytest.raises(ConfigError, match="DEFINITELY_NOT_SET"):
        _interpolate("${DEFINITELY_NOT_SET}")


def test_interpolate_non_string_passthrough():
    assert _interpolate(42) == 42
    assert _interpolate(True) is True
    assert _interpolate(None) is None


# ---------------------------------------------------------------------------
# load_config — happy path
# ---------------------------------------------------------------------------

_MINIMAL_VALID_YAML = """\
    llm:
      model: "anthropic:claude-haiku-4-5-20251001"
      embedder_model: "google_genai:models/gemini-embedding-001"
    storage:
      provider: "firestore"
      firestore:
        project_id: "test-project"
    signals:
      weights:
        technical: 0.20
        earnings: 0.30
        fcf: 0.30
        ebitda: 0.20
    screener:
      top_n: 10
      max_picks_per_sector: 3
    notifications:
      email:
        enabled: false
        from_address: "noreply@example.com"
        recipients: []
    edgar:
      freshness_days: 30
      chunk_size: 512
      chunk_overlap: 0.10
      similarity_threshold: 0.7
      top_k: 5
"""


def test_load_config_happy_path(tmp_path):
    p = write_yaml(tmp_path, _MINIMAL_VALID_YAML)
    cfg = load_config(p)

    assert isinstance(cfg, AppConfig)
    assert cfg.llm.model == "anthropic:claude-haiku-4-5-20251001"
    assert cfg.storage.provider == "firestore"
    assert cfg.storage.firestore.project_id == "test-project"
    assert cfg.signals.weights.technical == pytest.approx(0.20)
    assert cfg.signals.weights.earnings == pytest.approx(0.30)
    assert cfg.screener.top_n == 10
    assert cfg.screener.max_picks_per_sector == 3
    assert cfg.notifications.email.enabled is False
    assert cfg.edgar.chunk_size == 512
    assert cfg.edgar.similarity_threshold == pytest.approx(0.7)


def test_load_config_env_var_interpolation(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_GCP_PROJECT", "env-resolved-project")
    yaml_content = """\
        storage:
          provider: "firestore"
          firestore:
            project_id: "${TEST_GCP_PROJECT}"
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    cfg = load_config(p)
    assert cfg.storage.firestore.project_id == "env-resolved-project"


# ---------------------------------------------------------------------------
# load_config — file errors
# ---------------------------------------------------------------------------


def test_load_config_missing_file_raises():
    with pytest.raises(ConfigError, match="not found"):
        load_config("/nonexistent/path/config.yaml")


def test_load_config_invalid_yaml_raises(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text("key: [unclosed bracket", encoding="utf-8")
    with pytest.raises(ConfigError, match="parse config YAML"):
        load_config(p)


def test_load_config_empty_yaml_uses_defaults(tmp_path):
    """An empty YAML file should produce an AppConfig with all defaults."""
    p = tmp_path / "config.yaml"
    p.write_text("", encoding="utf-8")
    # This will fail firestore validation (no project_id) — confirm it raises ConfigError
    with pytest.raises(ConfigError):
        load_config(p)


# ---------------------------------------------------------------------------
# load_config — unresolvable env var
# ---------------------------------------------------------------------------


def test_load_config_unresolved_env_var_raises(tmp_path):
    os.environ.pop("MISSING_VAR_XYZ", None)
    p = write_yaml(tmp_path, 'storage:\n  provider: "${MISSING_VAR_XYZ}"\n')
    with pytest.raises(ConfigError, match="MISSING_VAR_XYZ"):
        load_config(p)


# ---------------------------------------------------------------------------
# Pydantic validation — LLM
# ---------------------------------------------------------------------------


def test_llm_model_without_provider_prefix_raises(tmp_path):
    yaml_content = """\
        llm:
          model: "claude-haiku-4-5-20251001"
          embedder_model: "google_genai:models/gemini-embedding-001"
        storage:
          provider: "firestore"
          firestore:
            project_id: "p"
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="provider:model_id"):
        load_config(p)


def test_embedder_model_without_provider_prefix_raises(tmp_path):
    yaml_content = """\
        llm:
          model: "anthropic:claude-haiku-4-5-20251001"
          embedder_model: "gemini-embedding-001"
        storage:
          provider: "firestore"
          firestore:
            project_id: "p"
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="provider:model_id"):
        load_config(p)


# ---------------------------------------------------------------------------
# Pydantic validation — storage
# ---------------------------------------------------------------------------


def test_unknown_storage_provider_raises(tmp_path):
    yaml_content = """\
        storage:
          provider: "mongodb"
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="storage.provider"):
        load_config(p)


def test_firestore_without_project_id_raises(tmp_path):
    yaml_content = """\
        storage:
          provider: "firestore"
          firestore:
            project_id: ""
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="project_id"):
        load_config(p)


def test_s3_without_bucket_raises(tmp_path):
    yaml_content = """\
        storage:
          provider: "s3"
          s3:
            bucket: ""
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="bucket"):
        load_config(p)


# ---------------------------------------------------------------------------
# Pydantic validation — signals
# ---------------------------------------------------------------------------


def test_signal_weights_not_summing_to_one_raises(tmp_path):
    yaml_content = """\
        storage:
          provider: "firestore"
          firestore:
            project_id: "p"
        signals:
          weights:
            technical: 0.30
            earnings:  0.30
            fcf:       0.30
            ebitda:    0.30
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="sum to 1.0"):
        load_config(p)


# ---------------------------------------------------------------------------
# Pydantic validation — screener
# ---------------------------------------------------------------------------


def test_top_n_zero_raises(tmp_path):
    yaml_content = """\
        storage:
          provider: "firestore"
          firestore:
            project_id: "p"
        screener:
          top_n: 0
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="top_n"):
        load_config(p)


# ---------------------------------------------------------------------------
# Pydantic validation — notifications
# ---------------------------------------------------------------------------


def test_email_enabled_without_recipients_raises(tmp_path):
    yaml_content = """\
        storage:
          provider: "firestore"
          firestore:
            project_id: "p"
        notifications:
          email:
            enabled: true
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="recipients"):
        load_config(p)


def test_email_disabled_with_empty_recipients_ok(tmp_path):
    yaml_content = """\
        storage:
          provider: "firestore"
          firestore:
            project_id: "p"
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    cfg = load_config(p)
    assert cfg.notifications.email.enabled is False
    assert cfg.notifications.email.recipients == []


# ---------------------------------------------------------------------------
# Pydantic validation — EDGAR
# ---------------------------------------------------------------------------


def test_edgar_chunk_overlap_out_of_range_raises(tmp_path):
    yaml_content = """\
        storage:
          provider: "firestore"
          firestore:
            project_id: "p"
        edgar:
          chunk_overlap: 1.5
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="chunk_overlap"):
        load_config(p)


def test_edgar_similarity_threshold_out_of_range_raises(tmp_path):
    yaml_content = """\
        storage:
          provider: "firestore"
          firestore:
            project_id: "p"
        edgar:
          similarity_threshold: 1.5
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    with pytest.raises(ConfigError, match="similarity_threshold"):
        load_config(p)


# ---------------------------------------------------------------------------
# Per-agent model overrides
# ---------------------------------------------------------------------------


def test_per_agent_overrides_parsed(tmp_path):
    yaml_content = """\
        llm:
          model: "anthropic:claude-haiku-4-5-20251001"
          embedder_model: "google_genai:models/gemini-embedding-001"
          bull_model: "openai:gpt-4o-mini"
          bear_model: "groq:llama-3.1-8b-instant"
          judge_model: "anthropic:claude-sonnet-4-5-20251015"
          narrator_model: "google_genai:gemini-2.0-flash"
        storage:
          provider: "firestore"
          firestore:
            project_id: "p"
        notifications:
          email:
            enabled: false
            recipients: []
    """
    p = write_yaml(tmp_path, yaml_content)
    cfg = load_config(p)
    assert cfg.llm.bull_model == "openai:gpt-4o-mini"
    assert cfg.llm.bear_model == "groq:llama-3.1-8b-instant"
    assert cfg.llm.judge_model == "anthropic:claude-sonnet-4-5-20251015"
    assert cfg.llm.narrator_model == "google_genai:gemini-2.0-flash"
