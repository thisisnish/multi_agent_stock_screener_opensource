"""
screener/lib/llm_metrics.py — Best-effort LLM token usage tracking.

Public API
----------
TokenAccumulator
    LangChain callback handler that sums token counts across LLM calls.

emit_token_metric(project_id, model_id, token_count)
    Write a single data point to custom.googleapis.com/screener/llm_tokens_used.
    Never raises — monitoring failures are logged as warnings only.
"""

from __future__ import annotations

import logging
import time

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult

logger = logging.getLogger(__name__)


class TokenAccumulator(BaseCallbackHandler):
    """Accumulates token usage across one or more LLM calls.

    Attach via ``runnable.with_config({"callbacks": [accumulator]})``.
    After the call completes, read ``accumulator.total_tokens``.
    """

    def __init__(self) -> None:
        super().__init__()
        self.total_tokens: int = 0

    def on_llm_end(self, response: LLMResult, **kwargs) -> None:
        try:
            usage = response.llm_output or {}
            # OpenAI format: {"token_usage": {"total_tokens": N}}
            total = usage.get("token_usage", {}).get("total_tokens", 0)
            if not total:
                # Anthropic format: {"usage": {"input_tokens": N, "output_tokens": M}}
                inner = usage.get("usage", {})
                total = inner.get("input_tokens", 0) + inner.get("output_tokens", 0)
            self.total_tokens += total
        except Exception:
            logger.warning(
                "TokenAccumulator: failed to extract token count", exc_info=True
            )


def emit_token_metric(project_id: str, model_id: str, token_count: int) -> None:
    """Write a GAUGE INT64 data point to custom.googleapis.com/screener/llm_tokens_used.

    No-ops silently when:
    - token_count <= 0
    - project_id is empty
    - google-cloud-monitoring is not installed (local dev without the package)

    Never raises — any write failure is logged as a warning.

    Args:
        project_id: GCP project ID (e.g. ``"my-project"``).
        model_id: Model identifier used as the ``model`` metric label.
        token_count: Number of tokens to record.
    """
    if token_count <= 0 or not project_id:
        return

    try:
        from google.cloud import monitoring_v3  # type: ignore[import]
    except ImportError:
        return

    try:
        client = monitoring_v3.MetricServiceClient()
        project_name = f"projects/{project_id}"

        series = monitoring_v3.TimeSeries()
        series.metric.type = "custom.googleapis.com/screener/llm_tokens_used"
        series.metric.labels["model"] = model_id
        series.resource.type = "global"

        now = time.time()
        interval = monitoring_v3.TimeInterval(
            {"end_time": {"seconds": int(now), "nanos": int((now % 1) * 1e9)}}
        )
        point = monitoring_v3.Point(
            {
                "interval": interval,
                "value": {"int64_value": token_count},
            }
        )
        series.points = [point]

        client.create_time_series(
            request={"name": project_name, "time_series": [series]}
        )
        logger.debug(
            "emitted llm_tokens_used: model=%s tokens=%d project=%s",
            model_id,
            token_count,
            project_id,
        )
    except Exception:
        logger.warning(
            "emit_token_metric: failed to write metric (model=%s tokens=%d)",
            model_id,
            token_count,
            exc_info=True,
        )
