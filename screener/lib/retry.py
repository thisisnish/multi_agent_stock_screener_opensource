"""
screener/lib/retry.py — Transient-error retry helper for external API calls.
"""

from __future__ import annotations

import errno
import logging
import time
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

_VALIDATION_EXCEPTIONS = (ValueError, TypeError, KeyError, AttributeError)


def _is_transient(exc: BaseException) -> bool:
    """Return True if *exc* should be retried, False if it should propagate immediately."""
    try:
        import requests.exceptions as req_exc

        if isinstance(exc, req_exc.Timeout):
            return True
        if isinstance(exc, req_exc.ConnectionError):
            return True
        if isinstance(exc, req_exc.HTTPError):
            resp = getattr(exc, "response", None)
            if resp is None:
                return True
            return resp.status_code == 429 or resp.status_code >= 500
    except ImportError:
        pass

    try:
        import httpx

        if isinstance(exc, httpx.TimeoutException):
            return True
        if isinstance(exc, httpx.ConnectError):
            return True
    except ImportError:
        pass

    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, ConnectionResetError):
        return True

    # OSError: only retry on network-level errno values (ETIMEDOUT / ECONNRESET).
    if isinstance(exc, OSError):
        err = getattr(exc, "errno", None)
        return err in (errno.ETIMEDOUT, errno.ECONNRESET)

    return False


def retry_transient(
    fn: Callable[..., T],
    *args: Any,
    max_attempts: int = 3,
    backoff_base: float = 2.0,
    **kwargs: Any,
) -> T:
    """Call *fn* with retry on transient network errors.

    Retries up to *max_attempts* times with exponential backoff
    (``backoff_base ** attempt`` seconds between tries).  Validation errors
    (ValueError, TypeError, KeyError, AttributeError) are re-raised immediately
    without retry.  Unknown errors also propagate immediately.

    Args:
        fn: Callable to invoke.
        *args: Positional arguments forwarded to *fn*.
        max_attempts: Maximum number of calls (including the first attempt).
        backoff_base: Base for the exponential sleep: ``backoff_base ** attempt``.
        **kwargs: Keyword arguments forwarded to *fn*.

    Returns:
        Whatever *fn* returns on success.

    Raises:
        The last transient exception if all attempts are exhausted.
        Any validation or unknown exception immediately on first occurrence.
    """
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return fn(*args, **kwargs)
        except _VALIDATION_EXCEPTIONS:
            raise
        except Exception as exc:
            if not _is_transient(exc):
                raise
            last_exc = exc
            if attempt < max_attempts - 1:
                delay = backoff_base ** attempt
                logger.warning(
                    "retrying %s attempt %d/%d after %.1fs (error: %s)",
                    getattr(fn, "__name__", str(fn)),
                    attempt + 1,
                    max_attempts,
                    delay,
                    exc,
                )
                time.sleep(delay)

    raise last_exc  # type: ignore[misc]
