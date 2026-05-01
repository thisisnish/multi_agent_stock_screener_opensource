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

import logging
import os
import sys
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def main() -> None:
    from screener.edgar.retriever import EDGARRetriever
    from screener.lib.config_loader import load_config
    from screener.lib.storage.firestore import FirestoreDAO

    month_id = os.environ.get("MONTH_ID") or datetime.now(timezone.utc).strftime("%Y-%m")
    dry_run = os.environ.get("DRY_RUN", "false").lower() in ("1", "true", "yes")

    logger.info("edgar_disclosure_job starting — month_id=%s dry_run=%s", month_id, dry_run)

    app_config = load_config()
    dao = FirestoreDAO(
        project_id=app_config.storage.firestore.project_id,
        database=app_config.storage.firestore.database,
    )

    # Load ticker list from config
    tickers: list[str] = []
    try:
        import yaml

        with open("config/tickers.yaml") as f:
            ticker_data = yaml.safe_load(f)
        tickers = [entry["symbol"] for entry in ticker_data.get("tickers", [])]
    except FileNotFoundError:
        logger.warning("config/tickers.yaml not found — using empty ticker list")

    logger.info("indexing EDGAR filings for %d tickers", len(tickers))

    retriever = EDGARRetriever(app_config=app_config, dao=dao)

    success = 0
    errors = 0
    for symbol in tickers:
        try:
            retriever.index_ticker(symbol, dry_run=dry_run)
            success += 1
        except Exception:
            logger.exception("failed to index EDGAR for %s", symbol)
            errors += 1

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
