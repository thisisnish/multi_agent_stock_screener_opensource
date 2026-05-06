"""
screener/events/writer.py — Pipeline lifecycle event emitter.

Writes EventDoc documents to the ``events/`` Firestore collection at key
pipeline checkpoints.  All failures are swallowed with a warning so a storage
blip never aborts the pipeline.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def emit_event(
    dao: Any,
    *,
    event_type: str,
    job_name: str,
    step: str,
    status: str,
    month_id: str | None = None,
    duration_ms: int | None = None,
    error: str | None = None,
    payload: dict | None = None,
) -> None:
    """Write a single lifecycle event to the ``events/`` collection.

    Gracefully degrades on any storage error — the pipeline continues
    regardless of whether the event write succeeds.

    Args:
        dao:         StorageDAO instance.
        event_type:  Human-readable checkpoint name, e.g. ``"job_started"``.
        job_name:    Cloud Run Job or GCF name, e.g. ``"screener_job"``.
        step:        Sub-step label, e.g. ``"debate"``, ``"picks_write"``.
        status:      ``"started"``, ``"success"``, or ``"error"``.
        month_id:    Current pipeline month, e.g. ``"2026-05"``.
        duration_ms: Elapsed milliseconds since the step began (optional).
        error:       Exception message if ``status == "error"`` (optional).
        payload:     Arbitrary extra context (optional).
    """
    from screener.lib.storage.schema import EVENTS, EventDoc, event_doc_id

    doc = EventDoc(
        event_type=event_type,
        job_name=job_name,
        step=step,
        status=status,
        month_id=month_id,
        duration_ms=duration_ms,
        error=error,
        payload=payload or {},
    )
    doc_id = event_doc_id()
    try:
        await dao.set(EVENTS, doc_id, doc.model_dump(mode="json"))
        logger.debug(
            "event emitted — %s %s %s",
            job_name,
            step,
            status,
            extra={"doc_id": doc_id},
        )
    except Exception:
        logger.warning(
            "failed to emit event — %s %s %s (non-fatal)",
            job_name,
            step,
            status,
        )
