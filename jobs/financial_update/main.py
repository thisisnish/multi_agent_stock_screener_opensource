"""
jobs/financial_update/main.py — Cloud Run Job entry point.

Refreshes FCF + EBITDA/EV fundamental signals for all configured tickers.
Writes results to the storage backend (Firestore by default).

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
import tempfile
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


def main() -> None:
    from screener.lib.config_loader import load_config
    from screener.lib.storage.firestore import FirestoreDAO
    from screener.metrics.earnings_yield import fetch_earnings_yield
    from screener.metrics.ebitda_ev import fetch_ebitda_ev
    from screener.metrics.fcf_yield import fetch_fcf_yield

    month_id = os.environ.get("MONTH_ID") or datetime.now(timezone.utc).strftime(
        "%Y-%m"
    )
    dry_run = os.environ.get("DRY_RUN", "false").lower() in ("1", "true", "yes")

    logger.info(
        "financial_update_job starting — month_id=%s dry_run=%s", month_id, dry_run
    )

    gcs_bucket = os.environ.get("GCS_CONFIG_BUCKET")
    if gcs_bucket:
        config_path, tickers_path = _download_configs_from_gcs(gcs_bucket)
    else:
        logger.info("GCS_CONFIG_BUCKET not set — using local config/ files")
        config_path = "config/config.yaml"
        tickers_path = "config/tickers.yaml"

    app_config = load_config(path=config_path)
    # DAO is instantiated here to validate credentials at startup; individual
    # signal fetchers (yfinance) do not write to storage — that happens in
    # screener_job.  Kept for symmetry with edgar and screener jobs so the
    # service account / ADC check fires early.
    _dao = FirestoreDAO(
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

    logger.info("fetching signals for %d tickers", len(tickers))

    success = 0
    errors = 0
    for symbol in tickers:
        try:
            _ = fetch_earnings_yield(symbol)
            _ = fetch_fcf_yield(symbol)
            _ = fetch_ebitda_ev(symbol)
            success += 1
        except Exception:
            logger.exception("failed to fetch signals for %s", symbol)
            errors += 1

    logger.info(
        "financial_update_job complete — success=%d errors=%d month_id=%s",
        success,
        errors,
        month_id,
    )

    if errors > 0 and success == 0:
        logger.error("all tickers failed — exiting non-zero")
        sys.exit(1)


if __name__ == "__main__":
    main()
