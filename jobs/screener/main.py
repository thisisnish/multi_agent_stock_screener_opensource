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
    from screener.metrics.technical import fetch_technical_signal

    # Step 1 — fetch signals
    raw_signals: list[dict] = []
    for entry in tickers:
        symbol = entry["symbol"]
        sector = entry.get("sector", "Unknown")
        try:
            technical = fetch_technical_signal(symbol)
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

    # Hard-abort: technical signal is a hard constraint per AGENT.md.
    # Any ticker whose technical data was skipped (insufficient history) must
    # cause the entire run to fail — silent RSI=0 imputation is not allowed.
    skipped_technical = [
        sig["symbol"]
        for sig in raw_signals
        if sig.get("technical", {}).get("skipped", False)
    ]
    if skipped_technical:
        technical_by_sym = {sig["symbol"]: sig["technical"] for sig in raw_signals}
        for sym in skipped_technical:
            reason = technical_by_sym[sym].get("reason", "unknown")
            logger.error("technical signal failed for %s — %s", sym, reason)
        logger.error(
            "aborting screener_job: technical signal missing for %d ticker(s): %s",
            len(skipped_technical),
            ", ".join(skipped_technical),
        )
        sys.exit(1)

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

    # Build today's date string for latest_screening_date field
    screening_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    graph = build_debate_graph(app_config=app_config, dao=dao)

    async def _write_ticker_docs(scored_entries: list[dict]) -> None:
        """Upsert one master record per ticker into tickers/ collection.

        Written for every scored ticker (not just top-N picks) so the
        collection provides a complete sector inventory queryable for
        sector-cap enforcement verification and general reference.
        """
        from screener.lib.storage.schema import TICKERS, TickerSignalDoc, ticker_doc_id

        for entry in scored_entries:
            sym = entry["symbol"]
            technical_raw = entry.get("technical") or {}
            ma200_gate = entry.get("ma200_gate") or {}
            above = ma200_gate.get("multiplier", 1.0) == 1.0

            doc = TickerSignalDoc(
                symbol=sym,
                latest_screening_date=screening_date,
                technical=factor_scores["technical"].get(sym) or 0.0,
                earnings=factor_scores["earnings"].get(sym) or 0.0,
                fcf=factor_scores["fcf"].get(sym) or 0.0,
                ebitda=factor_scores["ebitda"].get(sym) or 0.0,
                composite_score=entry["composite_score"],
                sector=entry.get("sector", "Unknown"),
                price=technical_raw.get("price"),
                above_ma200=above,
                active=True,
            )
            await dao.set(TICKERS, ticker_doc_id(sym), doc.model_dump(mode="json"))
            logger.debug("ticker doc written — %s", sym)

        logger.info("tickers/ collection updated — %d docs", len(scored_entries))

    async def _run_pipeline() -> list[dict]:
        from screener.events.writer import emit_event

        t0 = time.monotonic()

        await emit_event(
            dao,
            event_type="job_started",
            job_name="screener_job",
            step="start",
            status="started",
            month_id=month_id,
            payload={"ticker_count": len(gated), "pick_count": len(picks)},
        )

        # Write tickers/ master collection for all scored tickers before debate.
        # Done unconditionally of dry_run so even dry runs populate the
        # collection snapshot — callers can inspect without triggering debate.
        if not dry_run:
            await _write_ticker_docs(gated)

            # Write screenings/ collection: full scoring run snapshot including
            # sector-cap enforcement audit trail.  Written once per month after
            # the sector-cap step and before the debate loop.
            from screener.screening.writer import write_screening_doc

            await write_screening_doc(
                dao=dao,
                month_id=month_id,
                gated=gated,
                picks=picks,
                factor_scores=factor_scores,
                top_n=top_n,
            )

        await emit_event(
            dao,
            event_type="scoring_complete",
            job_name="screener_job",
            step="scoring",
            status="success",
            month_id=month_id,
            duration_ms=int((time.monotonic() - t0) * 1000),
            payload={"ticker_count": len(gated), "pick_count": len(picks)},
        )

        # Fetch prior-month eval_context once before the debate loop.
        # Graceful degrade: if the eval doc is missing or storage fails,
        # eval_context is None and the Judge runs without eval feedback.
        from screener.eval.loader import fetch_eval_context_async

        eval_context = await fetch_eval_context_async(dao, month_id)
        if eval_context:
            logger.info(
                "eval_context loaded for month_id=%s — injecting into Judge prompts",
                month_id,
            )
        else:
            logger.debug(
                "no eval_context available for month_id=%s — Judge runs without eval feedback",
                month_id,
            )

        results = []
        for pick in picks:
            symbol = pick["symbol"]
            try:
                from screener.lib.storage.schema import ANALYSIS, analysis_doc_id

                existing_analysis = await dao.get(
                    ANALYSIS, analysis_doc_id(symbol, month_id)
                )
                if existing_analysis:
                    logger.info(
                        "analysis doc exists for %s %s — skipping debate",
                        symbol,
                        month_id,
                    )
                    # Reconstruct minimal DebateState-compatible dict so downstream
                    # reshape works correctly without re-running the debate.
                    results.append(
                        {
                            "ticker": symbol,
                            "final_action": existing_analysis.get(
                                "judge_verdict", "HOLD"
                            ),
                            "confidence_score": existing_analysis.get(
                                "judge_confidence"
                            ),
                            "judge_output": {
                                "margin_of_victory": existing_analysis.get(
                                    "margin_of_victory", "CONTESTED"
                                ),
                                "decisive_factor": existing_analysis.get(
                                    "decisive_factor", "—"
                                ),
                            },
                        }
                    )
                    continue

                result = await graph.ainvoke(
                    {
                        "ticker": symbol,
                        "month_id": month_id,
                        "signals": pick,
                        "eval_context": eval_context,
                    }
                )
                if not dry_run:
                    from screener.analysis.writer import write_analysis_doc

                    await write_analysis_doc(
                        dao=dao, ticker=symbol, month_id=month_id, state=result
                    )
                results.append(result)
            except Exception:
                logger.exception("debate failed for %s — skipping", symbol)

        logger.info("debate complete — %d verdicts", len(results))

        await emit_event(
            dao,
            event_type="debate_complete",
            job_name="screener_job",
            step="debate",
            status="success",
            month_id=month_id,
            duration_ms=int((time.monotonic() - t0) * 1000),
            payload={"verdict_count": len(results)},
        )

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

            await emit_event(
                dao,
                event_type="picks_written",
                job_name="screener_job",
                step="picks_write",
                status="success",
                month_id=month_id,
                duration_ms=int((time.monotonic() - t0) * 1000),
                payload={"pick_count": len(results)},
            )

            # Write performance/ collection: per-pick ledger entries + monthly
            # snapshot aggregate.  Done after picks/ so performance docs are
            # always a strict superset of pick data.
            from screener.performance.tracker import write_performance_docs

            await write_performance_docs(
                dao=dao,
                month_id=month_id,
                verdicts=results,
                picks=picks,
                source="judge",
            )

        await emit_event(
            dao,
            event_type="job_complete",
            job_name="screener_job",
            step="end",
            status="success",
            month_id=month_id,
            duration_ms=int((time.monotonic() - t0) * 1000),
            payload={"verdict_count": len(results)},
        )

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
