"""
screener/lib/config_loader.py — Config loader with env-var interpolation and Pydantic validation.

Usage:
    from screener.lib.config_loader import load_config

    cfg = load_config("config/config.yaml")
    print(cfg.llm.model)
    print(cfg.storage.provider)

`load_config` reads config.yaml, interpolates ${VAR_NAME} placeholders from the
process environment (with .env auto-loaded via python-dotenv when present), then
validates the result through the Pydantic model tree.  Any missing required value
or type error raises a clear ConfigError at startup — not at the call site.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic import ValidationError

# ---------------------------------------------------------------------------
# Optional: load .env automatically when python-dotenv is installed.
# Never a hard dependency — CI sets env vars directly.
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv

    load_dotenv(override=False)  # env vars already set in the process take precedence
except ModuleNotFoundError:
    pass  # python-dotenv is optional; skip silently


# ---------------------------------------------------------------------------
# Public exception
# ---------------------------------------------------------------------------


class ConfigError(RuntimeError):
    """Raised on any config validation or interpolation failure."""


# ---------------------------------------------------------------------------
# Env-var interpolation
# ---------------------------------------------------------------------------

_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _interpolate(value: object) -> object:
    """Recursively resolve ${VAR_NAME} placeholders in strings, lists, dicts."""
    if isinstance(value, str):

        def _replace(match: re.Match) -> str:
            var = match.group(1)
            resolved = os.environ.get(var)
            if resolved is None:
                raise ConfigError(
                    f"Config references env var '${{{var}}}' but it is not set. "
                    f"Check your .env file or environment."
                )
            return resolved

        return _ENV_VAR_RE.sub(_replace, value)

    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}

    if isinstance(value, list):
        return [_interpolate(item) for item in value]

    return value


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class LLMConfig(BaseModel):
    model: str = "anthropic:claude-haiku-4-5-20251001"
    bull_model: Optional[str] = None
    bear_model: Optional[str] = None
    judge_model: Optional[str] = None
    news_model: Optional[str] = None
    narrator_model: Optional[str] = "google_genai:gemini-2.0-flash"
    embedder_model: str = "google_genai:models/gemini-embedding-001"

    @field_validator("model", "embedder_model")
    @classmethod
    def must_have_provider_prefix(cls, v: str) -> str:
        if ":" not in v:
            raise ValueError(
                f"LLM model string must use 'provider:model_id' format, got: '{v}'"
            )
        return v


class FirestoreConfig(BaseModel):
    project_id: str
    database: str = "multi-agent-stock-screener"


class S3Config(BaseModel):
    bucket: str
    region: str = "us-east-1"


class OpenSearchConfig(BaseModel):
    host: str
    port: int = 9200
    index: str = "stock-screener-chunks"


class StorageConfig(BaseModel):
    provider: str = "firestore"
    firestore: FirestoreConfig = Field(
        default_factory=lambda: FirestoreConfig(project_id="")
    )
    s3: S3Config = Field(default_factory=lambda: S3Config(bucket=""))
    opensearch: OpenSearchConfig = Field(
        default_factory=lambda: OpenSearchConfig(host="localhost")
    )

    @field_validator("provider")
    @classmethod
    def provider_must_be_known(cls, v: str) -> str:
        allowed = {"firestore", "s3", "opensearch"}
        if v not in allowed:
            raise ValueError(
                f"storage.provider must be one of {sorted(allowed)}, got: '{v}'"
            )
        return v

    @model_validator(mode="after")
    def active_backend_must_be_configured(self) -> "StorageConfig":
        if self.provider == "firestore" and not self.firestore.project_id:
            raise ValueError(
                "storage.firestore.project_id is required when provider = 'firestore'. "
                "Set GCP_PROJECT_ID in your environment."
            )
        if self.provider == "s3" and not self.s3.bucket:
            raise ValueError(
                "storage.s3.bucket is required when provider = 's3'. "
                "Set S3_BUCKET_NAME in your environment."
            )
        if self.provider == "opensearch" and not self.opensearch.host:
            raise ValueError(
                "storage.opensearch.host is required when provider = 'opensearch'. "
                "Set OPENSEARCH_HOST in your environment."
            )
        return self


class SignalWeightsConfig(BaseModel):
    technical: float = 0.20
    earnings: float = 0.30
    fcf: float = 0.30
    ebitda: float = 0.20

    @model_validator(mode="after")
    def weights_must_sum_to_one(self) -> "SignalWeightsConfig":
        total = self.technical + self.earnings + self.fcf + self.ebitda
        if not abs(total - 1.0) < 1e-6:
            raise ValueError(
                f"signals.weights must sum to 1.0, but got {total:.4f}. "
                f"Adjust technical/earnings/fcf/ebitda in config.yaml."
            )
        return self


class SignalsConfig(BaseModel):
    weights: SignalWeightsConfig = Field(default_factory=SignalWeightsConfig)


class ScreenerConfig(BaseModel):
    top_n: int = 10
    max_picks_per_sector: int = 3

    @field_validator("top_n")
    @classmethod
    def top_n_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"screener.top_n must be >= 1, got {v}")
        return v

    @field_validator("max_picks_per_sector")
    @classmethod
    def sector_cap_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"screener.max_picks_per_sector must be >= 1, got {v}")
        return v


class EmailConfig(BaseModel):
    enabled: bool = True
    from_address: str = ""
    recipients: list[str] = Field(default_factory=list)
    subject_prefix: str = "[Stock Screener]"

    @model_validator(mode="after")
    def recipients_required_when_enabled(self) -> "EmailConfig":
        if self.enabled and not self.recipients:
            raise ValueError(
                "notifications.email.recipients must not be empty when email is enabled. "
                "Add at least one address or set enabled: false."
            )
        return self


class NotificationsConfig(BaseModel):
    email: EmailConfig = Field(default_factory=EmailConfig)


class EdgarConfig(BaseModel):
    freshness_days: int = 30
    chunk_size: int = 512
    chunk_overlap: float = 0.10
    similarity_threshold: float = 0.7
    top_k: int = 5

    @field_validator("chunk_overlap")
    @classmethod
    def overlap_must_be_fraction(cls, v: float) -> float:
        if not 0.0 <= v < 1.0:
            raise ValueError(f"edgar.chunk_overlap must be in [0.0, 1.0), got {v}")
        return v

    @field_validator("similarity_threshold")
    @classmethod
    def threshold_must_be_fraction(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError(
                f"edgar.similarity_threshold must be in [0.0, 1.0], got {v}"
            )
        return v


class AppConfig(BaseModel):
    llm: LLMConfig = Field(default_factory=LLMConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    signals: SignalsConfig = Field(default_factory=SignalsConfig)
    screener: ScreenerConfig = Field(default_factory=ScreenerConfig)
    notifications: NotificationsConfig = Field(default_factory=NotificationsConfig)
    edgar: EdgarConfig = Field(default_factory=EdgarConfig)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def load_config(path: str | Path = "config/config.yaml") -> AppConfig:
    """Load, interpolate, and validate config.yaml.

    Args:
        path: Path to the YAML config file.  Defaults to ``config/config.yaml``
              relative to the current working directory.

    Returns:
        A fully validated :class:`AppConfig` instance.

    Raises:
        ConfigError: If the file is missing, any ${VAR_NAME} is unset,
                     or validation fails.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(
            f"Config file not found: '{config_path.resolve()}'. "
            f"Make sure config/config.yaml exists and the process is started "
            f"from the project root."
        )

    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"Failed to parse config YAML: {exc}") from exc

    if raw is None:
        raw = {}

    # Interpolate ${ENV_VAR} placeholders before Pydantic sees the data.
    try:
        interpolated = _interpolate(raw)
    except ConfigError:
        raise  # already has a clear message

    try:
        return AppConfig.model_validate(interpolated)
    except ValidationError as exc:
        # Re-raise as ConfigError with a clean, human-readable message.
        errors = "\n".join(
            f"  [{' -> '.join(str(loc) for loc in e['loc'])}] {e['msg']}"
            for e in exc.errors()
        )
        raise ConfigError(
            f"Config validation failed in '{config_path}':\n{errors}"
        ) from exc
