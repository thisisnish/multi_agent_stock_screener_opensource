"""
jobs/financial_update/main.py — Cloud Run Job entry point.

Refreshes FCF + EBITDA/EV fundamental signals for all configured tickers and
writes the results to the ``signals/{TICKER}_{MONTH_ID}`` Firestore collection.

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


def _build_signal_payload(
    symbol: str,
    month_id: str,
    earnings: dict,
    fcf: dict,
    ebitda: dict,
) -> dict:
    """Assemble the Firestore document payload for a single ticker's signals.

    Merges earnings, FCF, and EBITDA dicts into the flat ``SignalDoc`` schema.
    The ``fetched_at`` timestamp is set to UTC now so the document is always
    stamped with the actual fetch time regardless of any MONTH_ID override.
    """
    from screener.lib.storage.schema import SignalDoc

    doc = SignalDoc(
        ticker=symbol,
        month_id=month_id,
        fetched_at=datetime.now(timezone.utc),
        earnings_yield=earnings.get("earnings_yield"),
        trailing_eps=earnings.get("trailing_eps"),
        price=earnings.get("price"),
        earnings_skipped=bool(earnings.get("skipped", False)),
        earnings_skip_reason=earnings.get("skip_reason"),
        fcf_yield=fcf.get("fcf_yield"),
        free_cashflow=fcf.get("free_cashflow"),
        market_cap=fcf.get("market_cap"),
        fcf_skipped=bool(fcf.get("skipped", False)),
        fcf_skip_reason=fcf.get("skip_reason"),
        ebitda_ev=ebitda.get("ebitda_ev"),
        ebitda=ebitda.get("ebitda"),
        enterprise_value=ebitda.get("enterprise_value"),
        ebitda_skipped=bool(ebitda.get("skipped", False)),
        ebitda_skip_reason=ebitda.get("skip_reason"),
    )
    # model_dump with mode="json" serialises datetime → ISO string for Firestore.
    return doc.model_dump(mode="json")


def main() -> None:
    from screener.lib.config_loader import load_config
    from screener.lib.storage.firestore import FirestoreDAO
    from screener.lib.storage.schema import SIGNALS, signal_doc_id
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

    logger.info("fetching signals for %d tickers", len(tickers))

    # Fetch signals synchronously, then flush all writes in one asyncio.run()
    # call.  Calling asyncio.run() per-ticker destroyed the event loop after
    # each write; the Firestore AsyncClient's gRPC channel is bound to the
    # first loop and raises "Event loop is closed" for every subsequent write,
    # leaving only one document in the signals collection.
    writes: list[tuple[str, dict]] = []
    success = 0
    errors = 0
    for symbol in tickers:
        try:
            earnings = fetch_earnings_yield([symbol]).get(symbol, {})
            fcf = fetch_fcf_yield([symbol]).get(symbol, {})
            ebitda = fetch_ebitda_ev([symbol]).get(symbol, {})

            payload = _build_signal_payload(symbol, month_id, earnings, fcf, ebitda)
            doc_id = signal_doc_id(symbol, month_id)

            if dry_run:
                logger.info(
                    "dry_run — skipping write for %s (doc_id=%s)", symbol, doc_id
                )
            else:
                writes.append((doc_id, payload))

            success += 1
        except Exception:
            logger.exception("failed to fetch signals for %s", symbol)
            errors += 1

    if writes:
        async def _write_all() -> None:
            await asyncio.gather(
                *[dao.set(SIGNALS, doc_id, payload) for doc_id, payload in writes]
            )
            for doc_id, _ in writes:
                logger.info("wrote signals/%s month_id=%s", doc_id, month_id)

        asyncio.run(_write_all())

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
