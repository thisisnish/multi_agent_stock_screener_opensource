"""tests/lib/test_llm_metrics.py — Unit tests for screener.lib.llm_metrics."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from langchain_core.outputs import LLMResult

from screener.lib.llm_metrics import TokenAccumulator, emit_token_metric


# ---------------------------------------------------------------------------
# TokenAccumulator.on_llm_end
# ---------------------------------------------------------------------------


def _make_result(llm_output: dict | None) -> LLMResult:
    return LLMResult(generations=[], llm_output=llm_output)


def test_accumulator_openai_format():
    acc = TokenAccumulator()
    result = _make_result({"token_usage": {"total_tokens": 150}})
    acc.on_llm_end(result)
    assert acc.total_tokens == 150


def test_accumulator_anthropic_format():
    acc = TokenAccumulator()
    result = _make_result({"usage": {"input_tokens": 80, "output_tokens": 40}})
    acc.on_llm_end(result)
    assert acc.total_tokens == 120


def test_accumulator_accumulates_across_calls():
    acc = TokenAccumulator()
    acc.on_llm_end(_make_result({"token_usage": {"total_tokens": 50}}))
    acc.on_llm_end(_make_result({"usage": {"input_tokens": 30, "output_tokens": 20}}))
    assert acc.total_tokens == 100


def test_accumulator_missing_llm_output_does_not_raise():
    acc = TokenAccumulator()
    acc.on_llm_end(_make_result(None))
    assert acc.total_tokens == 0


def test_accumulator_malformed_llm_output_does_not_raise():
    acc = TokenAccumulator()
    # llm_output is a non-dict type — should be swallowed
    result = LLMResult(generations=[], llm_output=None)
    result.llm_output = "not-a-dict"  # type: ignore[assignment]
    acc.on_llm_end(result)
    assert acc.total_tokens == 0


# ---------------------------------------------------------------------------
# emit_token_metric
# ---------------------------------------------------------------------------


def _mock_monitoring():
    """Return a (mock_monitoring_v3, mock_google_cloud) pair for sys.modules injection."""
    mock_monitoring = MagicMock()
    mock_google_cloud = MagicMock()
    mock_google_cloud.monitoring_v3 = mock_monitoring
    return mock_monitoring, mock_google_cloud


def test_emit_noop_when_token_count_zero():
    mock_monitoring, mock_google_cloud = _mock_monitoring()
    modules = {
        "google": mock_google_cloud,
        "google.cloud": mock_google_cloud,
        "google.cloud.monitoring_v3": mock_monitoring,
    }
    with patch.dict("sys.modules", modules):
        emit_token_metric("my-project", "claude-haiku", 0)
    mock_monitoring.MetricServiceClient.assert_not_called()


def test_emit_noop_when_token_count_negative():
    mock_monitoring, mock_google_cloud = _mock_monitoring()
    modules = {
        "google": mock_google_cloud,
        "google.cloud": mock_google_cloud,
        "google.cloud.monitoring_v3": mock_monitoring,
    }
    with patch.dict("sys.modules", modules):
        emit_token_metric("my-project", "claude-haiku", -5)
    mock_monitoring.MetricServiceClient.assert_not_called()


def test_emit_noop_when_project_id_empty():
    mock_monitoring, mock_google_cloud = _mock_monitoring()
    modules = {
        "google": mock_google_cloud,
        "google.cloud": mock_google_cloud,
        "google.cloud.monitoring_v3": mock_monitoring,
    }
    with patch.dict("sys.modules", modules):
        emit_token_metric("", "claude-haiku", 100)
    mock_monitoring.MetricServiceClient.assert_not_called()


def test_emit_calls_create_time_series_with_correct_args():
    mock_monitoring = MagicMock()
    mock_client = MagicMock()
    mock_monitoring.MetricServiceClient.return_value = mock_client

    # Use a real dict for labels so __setitem__ / __getitem__ round-trip correctly.
    labels_dict: dict = {}
    metric_mock = MagicMock()
    metric_mock.labels = labels_dict
    resource_mock = MagicMock()

    mock_series = MagicMock()
    mock_series.metric = metric_mock
    mock_series.resource = resource_mock
    mock_monitoring.TimeSeries.return_value = mock_series
    mock_monitoring.TimeInterval.return_value = MagicMock()
    mock_monitoring.Point.return_value = MagicMock()

    mock_google_cloud = MagicMock()
    mock_google_cloud.monitoring_v3 = mock_monitoring

    modules = {
        "google": mock_google_cloud,
        "google.cloud": mock_google_cloud,
        "google.cloud.monitoring_v3": mock_monitoring,
    }
    with patch.dict("sys.modules", modules):
        emit_token_metric("test-project", "claude-haiku-4-5-20251001", 250)

    mock_client.create_time_series.assert_called_once()
    call_kwargs = mock_client.create_time_series.call_args
    request = call_kwargs.kwargs.get("request") or call_kwargs.args[0]
    assert request["name"] == "projects/test-project"
    assert request["time_series"] == [mock_series]

    assert metric_mock.type == "custom.googleapis.com/screener/llm_tokens_used"
    assert labels_dict["model"] == "claude-haiku-4-5-20251001"
    assert resource_mock.type == "global"


def test_emit_swallows_create_time_series_exception():
    mock_monitoring = MagicMock()
    mock_client = MagicMock()
    mock_client.create_time_series.side_effect = RuntimeError("network failure")
    mock_monitoring.MetricServiceClient.return_value = mock_client
    mock_monitoring.TimeSeries.return_value = MagicMock()
    mock_monitoring.TimeInterval.return_value = MagicMock()
    mock_monitoring.Point.return_value = MagicMock()

    mock_google_cloud = MagicMock()
    mock_google_cloud.monitoring_v3 = mock_monitoring

    modules = {
        "google": mock_google_cloud,
        "google.cloud": mock_google_cloud,
        "google.cloud.monitoring_v3": mock_monitoring,
    }
    with patch.dict("sys.modules", modules):
        # Must not raise
        emit_token_metric("test-project", "some-model", 100)
