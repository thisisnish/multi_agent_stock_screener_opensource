"""
jobs/screener/main.py — Cloud Run Job entry point.

Runs the full screener pipeline for the current month:
    1. Fetch signals (technical, earnings, FCF, EBITDA) for all tickers
    2. Compute composite scores; enforce sector cap
    3. Run multi-agent debate on top-N tickers
    4. Write picks to storage
    5. Email the monthly report

Environment variables:
    MONTH_ID        — optional YYYY-MM override; defaults to the current month.
    DRY_RUN         — set to "1" or "true" to skip storage writes and email.
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

    month_id = os.environ.get("MONTH_ID") or datetime.now(timezone.utc).strftime(
        "%Y-%m"
    )
    dry_run = os.environ.get("DRY_RUN", "false").lower() in ("1", "true", "yes")

    logger.info("screener_job starting — month_id=%s dry_run=%s", month_id, dry_run)

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

    # Load ticker + sector list from config
    tickers: list[dict] = []
    try:
        import yaml

        with open(tickers_path) as f:
            ticker_data = yaml.safe_load(f)
        tickers = ticker_data.get("tickers", [])
    except FileNotFoundError:
        logger.warning("tickers.yaml not found — using empty ticker list")

    if not tickers:
        logger.error("no tickers configured — exiting")
        sys.exit(1)

    logger.info("running screener on %d tickers for %s", len(tickers), month_id)

    # Import here to avoid paying startup cost if config/ticker load fails
    from screener.agents.graph import build_debate_graph
    from screener.lib.email_sender import send_report
    from screener.lib.normalizer import normalize_signals
    from screener.metrics.earnings_yield import fetch_earnings_yield
    from screener.metrics.ebitda_ev import fetch_ebitda_ev
    from screener.metrics.fcf_yield import fetch_fcf_yield
    from screener.metrics.ma200_gate import apply_ma200_gate
    from screener.metrics.technical import fetch_technical_signal

    # Step 1 — fetch signals
    raw_signals: list[dict] = []
    for entry in tickers:
        symbol = entry["symbol"]
        sector = entry.get("sector", "Unknown")
        try:
            technical = fetch_technical_signal(symbol)
            earnings = fetch_earnings_yield(symbol)
            fcf = fetch_fcf_yield(symbol)
            ebitda = fetch_ebitda_ev(symbol)
            raw_signals.append(
                {
                    "symbol": symbol,
                    "sector": sector,
                    "technical": technical,
                    "earnings": earnings,
                    "fcf": fcf,
                    "ebitda": ebitda,
                }
            )
        except Exception:
            logger.exception("signal fetch failed for %s — skipping", symbol)

    logger.info("fetched signals for %d/%d tickers", len(raw_signals), len(tickers))

    # Step 2 — normalize + score
    weights = app_config.signals.weights
    scored = normalize_signals(raw_signals, weights)
    gated = [apply_ma200_gate(entry) for entry in scored]

    # Step 3 — top-N with sector cap
    top_n: int = app_config.screener.top_n
    max_per_sector: int = app_config.screener.max_picks_per_sector

    sorted_by_score = sorted(gated, key=lambda x: x["composite_score"], reverse=True)
    sector_counts: dict[str, int] = {}
    picks: list[dict] = []
    for entry in sorted_by_score:
        sector = entry.get("sector", "Unknown")
        if sector_counts.get(sector, 0) >= max_per_sector:
            continue
        picks.append(entry)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(picks) >= top_n:
            break

    logger.info("selected top %d tickers for debate", len(picks))

    # Step 4 — multi-agent debate
    graph = build_debate_graph(app_config=app_config, dao=dao)
    verdicts: list[dict] = []
    for pick in picks:
        symbol = pick["symbol"]
        try:
            result = graph.invoke(
                {"ticker": symbol, "month_id": month_id, "signals": pick}
            )
            verdicts.append(result)
        except Exception:
            logger.exception("debate failed for %s — skipping", symbol)

    logger.info("debate complete — %d verdicts", len(verdicts))

    # Step 5 — write picks + email
    if not dry_run:
        import asyncio

        async def _write_picks() -> None:
            from screener.lib.storage.schema import (
                PICKS,
                current_week_id,
                pick_ledger_doc_id,
            )

            week_id = current_week_id()
            for verdict in verdicts:
                symbol = verdict.get("ticker", "UNKNOWN")
                doc_id = pick_ledger_doc_id(symbol, week_id)
                await dao.set(PICKS, doc_id, {**verdict, "entry_month": month_id})

        asyncio.run(_write_picks())
        logger.info("picks written to storage")

        if app_config.notifications.email.enabled:
            send_report(app_config=app_config, verdicts=verdicts, month_id=month_id)
            logger.info("email report sent")
        else:
            logger.info("email disabled in config — skipping")
    else:
        logger.info("dry_run=True — skipping storage writes and email")

    logger.info(
        "screener_job complete — month_id=%s verdicts=%d", month_id, len(verdicts)
    )


if __name__ == "__main__":
    main()
