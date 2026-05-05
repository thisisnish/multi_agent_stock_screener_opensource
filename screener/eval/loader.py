"""
screener/eval/loader.py — Load prior-month eval context for Judge prompt injection.

Public API:
    fetch_eval_context_async(dao, month_id) -> dict | None
    prior_month_id(month_id) -> str

Called by the screener job before running debates so that eval feedback from
the previous month is available to the Judge prompt for that month's run.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from screener.lib.storage.schema import EVAL, eval_doc_id

if TYPE_CHECKING:
    from screener.lib.storage.base import StorageDAO

logger = logging.getLogger(__name__)


def prior_month_id(month_id: str) -> str:
    """Return the YYYY-MM identifier for the month prior to month_id.

    Args:
        month_id: Month identifier in "YYYY-MM" format, e.g. "2026-04".

    Returns:
        Prior month identifier, e.g. "2026-03" for "2026-04".

    Raises:
        ValueError: If month_id does not match expected format.
    """
    from datetime import datetime

    try:
        dt = datetime.strptime(month_id, "%Y-%m")
    except ValueError:
        raise ValueError(f"month_id must be in YYYY-MM format, got: {month_id!r}")

    if dt.month == 1:
        prior_year = dt.year - 1
        prior_month = 12
    else:
        prior_year = dt.year
        prior_month = dt.month - 1

    return f"{prior_year:04d}-{prior_month:02d}"


async def fetch_eval_context_async(
    dao: "StorageDAO",
    month_id: str,
) -> dict | None:
    """Fetch eval_context for the month prior to month_id from the EVAL collection.

    Looks up the eval doc written by the eval Cloud Function for the prior month
    and extracts its ``eval_context`` dict for injection into the Judge prompt.

    Returns None (with a debug log) in these cases:
      - The prior month's eval doc does not exist yet.
      - The doc exists but has no ``eval_context`` key.
      - Any storage error (graceful degrade — debate must still run).

    Args:
        dao: StorageDAO instance.
        month_id: Current month identifier in "YYYY-MM" format (e.g. "2026-04").
            The function will look up the prior month's doc (e.g. "2026-03").

    Returns:
        The eval_context dict from the prior month's eval doc, or None if
        unavailable.
    """
    target_month = prior_month_id(month_id)

    # eval_doc_id expects (year, month) integers
    year, month = int(target_month[:4]), int(target_month[5:])
    doc_id = eval_doc_id(year, month)

    try:
        doc = await dao.get(EVAL, doc_id)
    except Exception:
        logger.warning(
            "failed to fetch eval doc %s from EVAL collection — skipping eval_context injection",
            doc_id,
        )
        return None

    if doc is None:
        logger.debug(
            "no eval doc found for prior month %s (doc_id=%s) — eval_context will be empty",
            target_month,
            doc_id,
        )
        return None

    eval_context = doc.get("eval_context")
    if not eval_context:
        logger.debug(
            "eval doc %s exists but has no eval_context key — skipping injection",
            doc_id,
        )
        return None

    logger.info(
        "loaded eval_context from %s for Judge injection (month_id=%s)",
        doc_id,
        month_id,
    )
    return eval_context
