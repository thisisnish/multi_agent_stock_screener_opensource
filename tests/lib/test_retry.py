"""tests/lib/test_retry.py — Unit tests for screener.lib.retry."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests.exceptions

from screener.lib.retry import retry_transient


def test_success_on_first_attempt():
    fn = MagicMock(return_value=42)
    with patch("time.sleep") as mock_sleep:
        result = retry_transient(fn, max_attempts=3, backoff_base=2.0)
    assert result == 42
    fn.assert_called_once()
    mock_sleep.assert_not_called()


def test_retries_on_transient_then_succeeds():
    timeout_exc = requests.exceptions.Timeout("timed out")
    fn = MagicMock(side_effect=[timeout_exc, timeout_exc, "ok"])
    with patch("time.sleep") as mock_sleep:
        result = retry_transient(fn, max_attempts=3, backoff_base=2.0)
    assert result == "ok"
    assert fn.call_count == 3
    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(1.0)  # backoff_base ** 0
    mock_sleep.assert_any_call(2.0)  # backoff_base ** 1


def test_fails_fast_on_validation_error():
    fn = MagicMock(side_effect=ValueError("bad input"))
    with patch("time.sleep") as mock_sleep:
        with pytest.raises(ValueError, match="bad input"):
            retry_transient(fn, max_attempts=3, backoff_base=2.0)
    fn.assert_called_once()
    mock_sleep.assert_not_called()


def test_exhausts_retries_and_reraises():
    exc = ConnectionResetError("reset")
    fn = MagicMock(side_effect=exc)
    with patch("time.sleep") as mock_sleep:
        with pytest.raises(ConnectionResetError):
            retry_transient(fn, max_attempts=3, backoff_base=2.0)
    assert fn.call_count == 3
    assert mock_sleep.call_count == 2


def test_http_429_is_transient():
    mock_response = MagicMock()
    mock_response.status_code = 429
    exc = requests.exceptions.HTTPError(response=mock_response)
    fn = MagicMock(side_effect=[exc, "ok"])
    with patch("time.sleep"):
        result = retry_transient(fn, max_attempts=3, backoff_base=2.0)
    assert result == "ok"
    assert fn.call_count == 2


def test_http_400_is_not_retried():
    mock_response = MagicMock()
    mock_response.status_code = 400
    exc = requests.exceptions.HTTPError(response=mock_response)
    fn = MagicMock(side_effect=exc)
    with patch("time.sleep") as mock_sleep:
        with pytest.raises(requests.exceptions.HTTPError):
            retry_transient(fn, max_attempts=3, backoff_base=2.0)
    fn.assert_called_once()
    mock_sleep.assert_not_called()
