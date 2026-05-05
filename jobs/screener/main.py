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
    from screener.lib.email_sender import send_email
    from screener.lib.normalizer import sector_z_scores
    from screener.metrics.earnings_yield import fetch_earnings_yield
    from screener.metrics.ebitda_ev import fetch_ebitda_ev
    from screener.metrics.fcf_yield import fetch_fcf_yield
    from screener.metrics.ma200_gate import apply_gate
    from screener.metrics.technical import compute_score

    # Step 1 — fetch signals
    raw_signals: list[dict] = []
    for entry in tickers:
        symbol = entry["symbol"]
        sector = entry.get("sector", "Unknown")
        try:
            technical = compute_score(symbol, None)
            earnings = fetch_earnings_yield([symbol]).get(symbol, {})
            fcf = fetch_fcf_yield([symbol]).get(symbol, {})
            ebitda = fetch_ebitda_ev([symbol]).get(symbol, {})
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
    sector_map = {
        entry["symbol"]: entry.get("sector", "Unknown") for entry in raw_signals
    }
    signals_by_symbol = {entry["symbol"]: entry for entry in raw_signals}

    # Normalize each factor independently via sector z-scores, then combine.
    # sector_z_scores expects {symbol: sub_dict} where each sub_dict has the
    # value_key field and a "skipped" field.
    factor_scores: dict[str, dict[str, float | None]] = {
        "technical": sector_z_scores(
            {sym: sig["technical"] for sym, sig in signals_by_symbol.items()},
            "score",
            sector_map,
        ),
        "earnings": sector_z_scores(
            {sym: sig["earnings"] for sym, sig in signals_by_symbol.items()},
            "earnings_yield",
            sector_map,
        ),
        "fcf": sector_z_scores(
            {sym: sig["fcf"] for sym, sig in signals_by_symbol.items()},
            "fcf_yield",
            sector_map,
        ),
        "ebitda": sector_z_scores(
            {sym: sig["ebitda"] for sym, sig in signals_by_symbol.items()},
            "ebitda_ev",
            sector_map,
        ),
    }
    factor_weights = {
        "technical": weights.technical,
        "earnings": weights.earnings,
        "fcf": weights.fcf,
        "ebitda": weights.ebitda,
    }

    gated: list[dict] = []
    for sym, sig in signals_by_symbol.items():
        weighted_sum = 0.0
        total_weight = 0.0
        for factor, factor_weight in factor_weights.items():
            score = factor_scores[factor].get(sym)
            if score is not None:
                weighted_sum += score * factor_weight
                total_weight += factor_weight

        if total_weight == 0.0:
            # All factors skipped — cannot score this symbol
            continue

        raw_composite = (
            weighted_sum / total_weight if total_weight < 1.0 else weighted_sum
        )

        gate = apply_gate(
            sig.get("technical", {}).get("price", 0) or 0,
            sig.get("technical", {}).get("ma200", 0) or 0,
        )
        composite_score = raw_composite * gate["multiplier"]

        gated.append({**sig, "composite_score": composite_score, "ma200_gate": gate})

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
    # Enrich picks with fields required by build_picks_table_html before debate
    # so the same dicts flow cleanly into the email step.
    for i, pick in enumerate(picks, start=1):
        pick["rank"] = i
        pick["score"] = pick.get("composite_score")
        technical = pick.get("technical") or {}
        pick.setdefault("rsi", technical.get("rsi") or 0.0)
        pick.setdefault("price", technical.get("price") or 0.0)

    import asyncio

    graph = build_debate_graph(app_config=app_config, dao=dao)

    async def _run_pipeline() -> list[dict]:
        results = []
        for pick in picks:
            symbol = pick["symbol"]
            try:
                result = await graph.ainvoke(
                    {"ticker": symbol, "month_id": month_id, "signals": pick}
                )
                results.append(result)
            except Exception:
                logger.exception("debate failed for %s — skipping", symbol)

        logger.info("debate complete — %d verdicts", len(results))

        if not dry_run:
            from screener.lib.storage.schema import (
                PICKS,
                pick_ledger_doc_id,
            )

            def _to_serializable(obj):
                from pydantic import BaseModel

                if isinstance(obj, BaseModel):
                    return obj.model_dump()
                if isinstance(obj, dict):
                    return {k: _to_serializable(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [_to_serializable(v) for v in obj]
                return obj

            for verdict in results:
                symbol = verdict.get("ticker", "UNKNOWN")
                doc_id = pick_ledger_doc_id(symbol, month_id, source="judge")
                await dao.set(
                    PICKS,
                    doc_id,
                    _to_serializable(
                        {**verdict, "entry_month": month_id, "source": "judge"}
                    ),
                )
            logger.info("picks written to storage")

        return results

    verdicts = asyncio.run(_run_pipeline())

    # Reshape raw DebateState dicts into the flat shape expected by
    # build_verdicts_table_html: {symbol, verdict, margin, confidence, decisive_factor}.
    # The graph state uses different key names:
    #   ticker        -> symbol
    #   final_action  -> verdict
    #   judge_output.margin_of_victory -> margin  (Pydantic model or already-serialized dict)
    #   confidence_score               -> confidence
    #   judge_output.decisive_factor   -> decisive_factor
    def _reshape_verdict(state: dict) -> dict:
        judge = state.get("judge_output") or {}
        # judge_output may be a Pydantic model (JudgeOutput) or already a dict
        if hasattr(judge, "margin_of_victory"):
            margin_raw = judge.margin_of_victory
            decisive = judge.decisive_factor
        else:
            margin_raw = judge.get("margin_of_victory", "—")
            decisive = judge.get("decisive_factor", "—")

        # Convert DECISIVE/NARROW/CONTESTED labels to numeric margin score (0–100)
        margin_map = {"DECISIVE": 75.0, "NARROW": 55.0, "CONTESTED": 45.0}
        margin = margin_map.get(str(margin_raw), None)

        return {
            "symbol": state.get("ticker", "?"),
            "verdict": state.get("final_action", "—"),
            "margin": margin,
            "confidence": state.get("confidence_score"),
            "decisive_factor": decisive or "—",
        }

    email_verdicts = [_reshape_verdict(v) for v in verdicts]

    if not dry_run:
        if app_config.notifications.email.enabled:
            send_email(
                cfg=app_config, picks=picks, date=month_id, verdicts=email_verdicts
            )
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
