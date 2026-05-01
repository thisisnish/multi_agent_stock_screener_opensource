"""
gcf/eval/main.py — Monthly eval orchestrator + Cloud Function HTTP entry point.

Called by Cloud Workflows after screener_job completes. Receives month_id via
the HTTP request body (JSON ``{"month_id": "YYYY-MM"}``) when running as a
Cloud Function, or directly via ``run_eval_main()`` when called from Python.

Cloud Function entry point: ``eval_handler(request)``
Direct Python entry point: ``run_eval_main(app_config, dao, month_id, dry_run)``

Flow:
    1. Fetch closed picks for month_id from PICKS collection
    2. Score with pure math (beat_spy already stored at ledger close)
    3. Compute EvalMetrics + confidence bins + acid test + disclosure rate
    4. Detect systematic issues
    5. Build eval_context for Judge injection
    6. Write eval doc to EVAL/{eval_doc_id} (unless dry_run)
    7. Return result dict
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import flask

from screener.eval.metrics import (
    compute_acid_test,
    compute_disclosure_citation_rate,
    compute_metrics,
    detect_systematic_issues,
    format_metrics_report,
)
from screener.eval.scorer import score_picks_pure_math
from screener.lib.config_loader import AppConfig, load_config
from screener.lib.storage.base import StorageDAO
from screener.lib.storage.firestore import FirestoreDAO
from screener.lib.storage.schema import EVAL, PICKS, eval_doc_id

logger = logging.getLogger(__name__)


def _parse_month_id(month_id: str) -> tuple[int, int]:
    """Parse "YYYY-MM" into (year, month).

    Args:
        month_id: Month identifier in "YYYY-MM" format, e.g. "2026-04".

    Returns:
        Tuple of (year, month) as integers.

    Raises:
        ValueError: If month_id does not match expected format or values are
            out of range.
    """
    try:
        dt = datetime.strptime(month_id, "%Y-%m")
    except ValueError:
        raise ValueError(f"month_id must be in YYYY-MM format, got: {month_id!r}")
    return dt.year, dt.month


async def _fetch_closed_picks_async(dao: StorageDAO, month_id: str) -> list[dict]:
    """Query PICKS collection for closed picks from the given month.

    Filters to picks whose ``entry_month`` field equals month_id and whose
    ``status`` is "closed" (i.e. beat_spy has been resolved).

    Args:
        dao: StorageDAO instance.
        month_id: "YYYY-MM" month identifier.

    Returns:
        List of pick dicts with at minimum: action, beat_spy, entry_month,
        confidence_score, pick_return_pct, bull_signal_citations,
        bear_signal_citations.
    """
    all_picks = await dao.query(PICKS, {"status": "closed"})
    return [p for p in all_picks if p.get("entry_month") == month_id]


def _build_eval_context(
    metrics: object,
    issues: list[str],
    acid_test: dict,
) -> dict:
    """Build the eval_context dict in the shape expected by build_judge_context().

    ``build_judge_context()`` in screener/agents/prompts.py guards injection
    on ``eval_context.get("total_picks_scored", 0) >= 4``.

    Args:
        metrics: EvalMetrics from compute_metrics().
        issues: List of systematic issue strings from detect_systematic_issues().
        acid_test: Dict from compute_acid_test().

    Returns:
        Dict matching the keys checked by build_judge_context():
        total_picks_scored, overall_accuracy, bull_accuracy, bear_accuracy,
        directional_bias, confidence_calibration, systematic_issues, acid_test.
    """
    return {
        "total_picks_scored": metrics.closed_picks,  # type: ignore[attr-defined]
        "overall_accuracy": metrics.overall_accuracy,  # type: ignore[attr-defined]
        "bull_accuracy": metrics.bull_accuracy,  # type: ignore[attr-defined]
        "bear_accuracy": metrics.bear_accuracy,  # type: ignore[attr-defined]
        "directional_bias": metrics.directional_bias,  # type: ignore[attr-defined]
        "confidence_calibration": metrics.confidence_calibration,  # type: ignore[attr-defined]
        "systematic_issues": issues,
        "acid_test": acid_test,
    }


def run_eval_main(
    app_config: AppConfig,
    dao: StorageDAO,
    month_id: str,
    dry_run: bool = False,
) -> dict:
    """Core eval orchestrator — sync wrapper over async internals.

    Args:
        app_config: Application config (used for LLM factory; not consumed
            by the pure-math eval path but kept for interface consistency).
        dao: StorageDAO instance — Firestore, S3, or OpenSearch backend.
        month_id: "YYYY-MM" month to evaluate, e.g. "2026-04".
        dry_run: If True, skip writing the eval doc to storage.

    Returns:
        Result dict with keys: status, month_id, total_picks, scored_picks,
        overall_accuracy, directional_bias, systematic_issues.
        On no-picks case: {status: "no_picks", month_id, total_picks}.
    """
    return asyncio.run(_run_async(app_config, dao, month_id, dry_run))


async def _run_async(
    app_config: AppConfig,
    dao: StorageDAO,
    month_id: str,
    dry_run: bool,
) -> dict:
    year, month = _parse_month_id(month_id)
    doc_id = eval_doc_id(year, month)

    picks = await _fetch_closed_picks_async(dao, month_id)
    logger.info("fetched %d closed picks for %s", len(picks), month_id)

    score_results = score_picks_pure_math(picks)

    if not score_results:
        logger.warning("no scoreable picks for %s", month_id)
        return {
            "status": "no_picks",
            "month_id": month_id,
            "total_picks": len(picks),
        }

    picks_metadata = [
        {
            "return_pct": p.get("pick_return_pct"),
            "sector": p.get("sector") or "Unknown",
        }
        for p in picks
    ]

    metrics = compute_metrics(
        period=month_id,
        score_results=score_results,
        picks_metadata=picks_metadata,
        picks_raw=picks,
    )

    issues = detect_systematic_issues(metrics)
    acid_test = compute_acid_test(picks)
    disclosure_rate = compute_disclosure_citation_rate(picks)
    metrics.disclosure_citation_rate = disclosure_rate

    eval_context = _build_eval_context(metrics, issues, acid_test)

    eval_doc = {
        "month_id": month_id,
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "total_picks": len(picks),
        "metrics": metrics.model_dump(),
        "acid_test": acid_test,
        "eval_context": eval_context,
        "disclosure_citation_rate": disclosure_rate,
    }

    if not dry_run:
        await dao.set(EVAL, doc_id, eval_doc)
        logger.info("wrote eval doc %s", doc_id)

    logger.info(
        "eval complete — %s: accuracy=%.1f%% bias=%s issues=%d",
        month_id,
        metrics.overall_accuracy or 0,
        metrics.directional_bias or "balanced",
        len(issues),
    )

    report = format_metrics_report(metrics)
    logger.debug("metrics report:\n%s", report)

    return {
        "status": "success",
        "month_id": month_id,
        "total_picks": len(picks),
        "scored_picks": len(score_results),
        "overall_accuracy": metrics.overall_accuracy,
        "directional_bias": metrics.directional_bias,
        "systematic_issues": issues,
    }


# ---------------------------------------------------------------------------
# Cloud Function HTTP entry point
# ---------------------------------------------------------------------------


def eval_handler(request: "flask.Request") -> tuple[str, int]:
    """Cloud Function HTTP entry point.

    Invoked by Cloud Workflows via an authenticated OIDC HTTP POST.

    Expected request body (JSON):
        {"month_id": "YYYY-MM"}

    Returns a JSON body and an HTTP status code.

    Args:
        request: The incoming Flask request object provided by the Cloud
            Functions runtime.

    Returns:
        A (body, status_code) tuple.  Body is a JSON string.
    """
    logging.basicConfig(level=logging.INFO)

    try:
        payload = request.get_json(silent=True) or {}
        month_id: str | None = payload.get("month_id") or os.environ.get("MONTH_ID")
        if not month_id:
            return (
                json.dumps({"error": "month_id is required in request body or MONTH_ID env var"}),
                400,
            )

        dry_run = bool(payload.get("dry_run", False))

        app_config = load_config()
        dao: StorageDAO = FirestoreDAO(
            project_id=app_config.storage.firestore.project_id,
            database=app_config.storage.firestore.database,
        )

        result = run_eval_main(app_config, dao, month_id, dry_run=dry_run)
        return json.dumps(result), 200

    except ValueError as exc:
        logger.error("bad request: %s", exc)
        return json.dumps({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("eval_handler unhandled error")
        return json.dumps({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Standalone entry point — for local testing and Cloud Run fallback
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    _month_id = os.environ.get("MONTH_ID")
    if not _month_id:
        print("ERROR: MONTH_ID environment variable is required", file=sys.stderr)
        sys.exit(1)

    _dry_run = os.environ.get("DRY_RUN", "false").lower() in ("1", "true", "yes")
    _app_config = load_config()
    _dao: StorageDAO = FirestoreDAO(
        project_id=_app_config.storage.firestore.project_id,
        database=_app_config.storage.firestore.database,
    )

    _result = run_eval_main(_app_config, _dao, _month_id, dry_run=_dry_run)
    print(json.dumps(_result, indent=2))
    sys.exit(0 if _result.get("status") in ("success", "no_picks") else 1)
