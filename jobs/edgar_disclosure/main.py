"""
jobs/edgar_disclosure/main.py — Cloud Run Job entry point.

Fetches 10-K and 10-Q filings from SEC EDGAR for all configured tickers,
chunks them, embeds them via the configured embedder model, and writes
the chunk vectors to the storage backend for later RAG retrieval.

Respects the ``edgar.freshness_days`` setting: skips a ticker if its
existing index is younger than that threshold.

Environment variables:
    MONTH_ID        — optional YYYY-MM override; defaults to the current month.
    DRY_RUN         — set to "1" or "true" to skip storage writes.
    GCP_PROJECT_ID  — required when storage.provider = "firestore".

All API keys are injected from Secret Manager (see deploy/deploy_all.sh).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def _download_configs_from_gcs(bucket_name: str) -> tuple[str, str]:
    """Download config.yaml and tickers.yaml from GCS to a temp directory.

    Returns (config_path, tickers_path) as absolute paths to the downloaded files.
    """
    from google.cloud import storage as gcs

    client = gcs.Client()
    bucket = client.bucket(bucket_name)
    tmp_dir = tempfile.mkdtemp(prefix="screener_config_")

    config_path = os.path.join(tmp_dir, "config.yaml")
    tickers_path = os.path.join(tmp_dir, "tickers.yaml")

    bucket.blob("config.yaml").download_to_filename(config_path)
    logger.info("downloaded gs://%s/config.yaml → %s", bucket_name, config_path)

    bucket.blob("tickers.yaml").download_to_filename(tickers_path)
    logger.info("downloaded gs://%s/tickers.yaml → %s", bucket_name, tickers_path)

    return config_path, tickers_path


async def _run_indexing(
    retriever, tickers: list[str], dry_run: bool, dao, month_id: str
) -> tuple[int, int]:
    """Await ``retriever.index_ticker`` for each ticker inside a single event loop.

    Using a single ``asyncio.run()`` call (in ``main``) and awaiting each ticker
    here avoids the ``RuntimeError: Event loop is closed`` bug that occurs when
    ``asyncio.run()`` is called multiple times: every call closes the loop that
    the grpc.aio-backed FirestoreDAO channel is bound to, causing all subsequent
    tickers to fail.

    Per-ticker errors are caught and logged so that one bad ticker never aborts
    the rest of the batch.

    Args:
        retriever: Initialised :class:`~screener.edgar.retriever.EDGARRetriever`.
        tickers: Ordered list of upper-case ticker symbols.
        dry_run: Forwarded verbatim to :meth:`~EDGARRetriever.index_ticker`.
        dao: StorageDAO instance for event emission.
        month_id: Current pipeline month string, e.g. ``"2026-05"``.

    Returns:
        ``(success_count, error_count)`` tuple.
    """
    from screener.events.writer import emit_event

    t0 = time.monotonic()

    await emit_event(
        dao,
        event_type="job_started",
        job_name="edgar_disclosure_job",
        step="edgar_indexing",
        status="started",
        month_id=month_id,
        payload={"ticker_count": len(tickers)},
    )

    success = 0
    errors = 0
    for symbol in tickers:
        try:
            await retriever.index_ticker(symbol, dry_run=dry_run)
            success += 1
        except Exception:
            logger.exception("failed to index EDGAR for %s", symbol)
            errors += 1

    await emit_event(
        dao,
        event_type="job_complete",
        job_name="edgar_disclosure_job",
        step="edgar_indexing",
        status="success" if errors == 0 else "error",
        month_id=month_id,
        duration_ms=int((time.monotonic() - t0) * 1000),
        payload={"success": success, "errors": errors},
    )

    return success, errors


def main() -> None:
    from screener.lib.config_loader import load_config
    from screener.lib.storage.firestore import FirestoreDAO

    month_id = os.environ.get("MONTH_ID") or datetime.now(timezone.utc).strftime(
        "%Y-%m"
    )
    dry_run = os.environ.get("DRY_RUN", "false").lower() in ("1", "true", "yes")

    logger.info(
        "edgar_disclosure_job starting — month_id=%s dry_run=%s", month_id, dry_run
    )

    gcs_bucket = os.environ.get("GCS_CONFIG_BUCKET")
    if gcs_bucket:
        config_path, tickers_path = _download_configs_from_gcs(gcs_bucket)
    else:
        logger.info("GCS_CONFIG_BUCKET not set — using local config/ files")
        config_path = "config/config.yaml"
        tickers_path = "config/tickers.yaml"

    app_config = load_config(path=config_path)
    dao = FirestoreDAO(
        project_id=app_config.storage.firestore.project_id,
        database=app_config.storage.firestore.database,
    )

    # Load ticker list from config
    tickers: list[str] = []
    try:
        import yaml

        with open(tickers_path) as f:
            ticker_data = yaml.safe_load(f)
        tickers = [entry["symbol"] for entry in ticker_data.get("tickers", [])]
    except FileNotFoundError:
        logger.warning("tickers.yaml not found — using empty ticker list")

    logger.info("indexing EDGAR filings for %d tickers", len(tickers))

    try:
        from screener.edgar.retriever import EDGARRetriever

        retriever = EDGARRetriever(app_config=app_config, dao=dao)
    except (ImportError, AttributeError):
        logger.warning(
            "EDGARRetriever not implemented — skipping EDGAR indexing (see TB-07)"
        )
        return

    success, errors = asyncio.run(
        _run_indexing(retriever, tickers, dry_run, dao, month_id)
    )

    logger.info(
        "edgar_disclosure_job complete — success=%d errors=%d month_id=%s",
        success,
        errors,
        month_id,
    )

    if errors > 0 and success == 0:
        logger.error("all tickers failed — exiting non-zero")
        sys.exit(1)


if __name__ == "__main__":
    main()
