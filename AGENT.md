# Multi-Agent Stock Screener — Engineering Overview

## What It Does

A monthly stock analysis pipeline that scores a configurable universe of equities, runs a structured multi-agent debate on top picks, tracks picks against SPY, and emails a report. Core loop: **score → debate → pick → track → evaluate**.

No human-in-the-loop. Fully automated via Cloud Workflows. All LLM providers and storage backends are swappable via config.

---

## System Architecture

```
Cloud Scheduler (monthly)
  └─► stock-screener-monthly-pipeline (Cloud Workflows)
        Step 1: financial_update_job   — refresh FCF + EBITDA/EV for all tickers
        Step 2: edgar_disclosure_job   — index 10-K/10-Q filings into vector store
        Step 3: screener_job           — score + debate + email + write picks
        Step 4: eval GCF               — score prior month picks, write quality metrics
```

All three Cloud Run Jobs share a single Docker image per job (`docker/`). No Flask server. No HTTP API. Entry points are plain `python main.py` scripts.

---

## Core Execution Loop (`jobs/screener/main.py:main()`)

```
1. Fetch signals for all tickers (technical, earnings, FCF, EBITDA)
2. Normalise to 0–100, apply sector-neutral imputation for missing signals
3. Composite score = Technical(20%) + Earnings(30%) + FCF(30%) + EBITDA(20%) × MA200 gate
4. Take top N (default 10), enforce max 3 picks per GICS sector
5. For each top ticker → run_debate()
6. Write picks + performance → storage
7. Email report
```

---

## Multi-Agent Debate (`screener/agents/`)

**LangGraph StateGraph** — linear graph with one parallel step:

```
memory_read → build_context → debate_node → conviction_node
            → judge_node → confidence_node → hard_rules → memory_write
```

| Node | Type | Description |
|------|------|-------------|
| `memory_read` | async I/O | Read per-ticker verdict history; compute adaptive bull/bear weights from scored prior months; set `adaptive_weights_active=true` if ≥4 scored months exist, `false` otherwise |
| `build_context` | async I/O | Vector-search EDGAR chunks (section-aware boosting, dedup); write per-run observability to Firestore (chunk scores, empty-retrieval markers); build disclosure block with token budget enforcement |
| `debate_node` | LLM × 2 (parallel) | Bull + Bear run simultaneously via `asyncio.gather` |
| `conviction_node` | white-box | Source diversity + hedge penalty score for Bull and Bear; scales by adaptive weights when ≥4 scored months |
| `judge_node` | LLM × 1 | Adjudicates debate; declares BUY/SELL/HOLD + margin + decisive factor |
| `confidence_node` | white-box | `W1·margin + W2·ln(sources) − W3·hedge` — no LLM tokens; weights loaded from `calibration/weights_judge` at startup if a calibration override exists |
| `hard_rules` | sync | Force HOLD if confidence < 40; set `contested_truth` if conviction gap > 30pts + NARROW/CONTESTED |
| `memory_write` | async I/O | Persist verdict + scoring_weights to `tickers/{SYMBOL}/memory/{MONTH_ID}` |

**Episodic memory**: Each ticker accumulates a scored verdict history. After ≥4 scored months, the debate adapts — Bull/Bear weights shift toward whichever side has been more accurate for this ticker.

**Systemic memory (RAG)**: 10-K and 10-Q filings are chunked (512 tokens, 10% overlap), section-tagged (Item 1A/7/8 etc.), and embedded via the configured embedder (default: OpenAI text-embedding-3-large, dim 3072). If `llm.embedder_model` changes in config, `index_ticker()` detects the drift against the stored sentinel and forces a full re-index automatically. Retrieval uses cosine similarity with ticker pre-filter (threshold 0.7), optional section-aware score boosting (+0.05 for sections in `edgar.retrieval_sections`), and text-hash deduplication of near-identical chunks. The disclosure block is token-budget capped (`edgar.max_disclosure_tokens`, default 2048; lowest-scoring chunks dropped first). Per-run observability — chunk scores, dedup counts, empty-retrieval markers — is written to `analysis/{TICKER}/disclosures/{run_id}` and logged to stdout.

**Eval feedback loop**: Monthly eval scores are written to `eval/{MONTH_ID}` and injected as `eval_context` into the Judge prompt the following month. The Judge receives its own historical accuracy, directional bias, and systematic issues.

---

## LLM Configuration

Any provider via `init_chat_model("provider:model")` — no custom provider code:

```yaml
llm:
  model: "anthropic:claude-haiku-4-5-20251001"  # default for all agents
  bull_model: null     # override per-agent if needed
  bear_model: null
  judge_model: null
  news_model: null
  narrator_model: "google_genai:gemini-2.0-flash"
  embedder_model: "openai:text-embedding-3-large"
```

Supported providers: Anthropic, OpenAI, Gemini, Ollama (local), Groq, any LangChain-integrated provider. Structured outputs via `.with_structured_output(PydanticModel)`.

---

## Storage

Abstract DAO interface (`screener/lib/storage/base.py`). Provider set in `config.yaml`:

| Provider | Best for | Vector search |
|----------|----------|---------------|
| `firestore` | GCP (default) | Native vector index (cosine, dim 3072) |
| `s3` | AWS / local | Brute-force cosine in-memory (~10k chunks, viable at S&P 500 scale) |
| `opensearch` | Self-hosted | k-NN plugin |

All collections live in one logical database: `multi-agent-stock-screener`.

| Collection | Doc ID pattern | Purpose |
|-----------|----------------|---------|
| `tickers/{SYMBOL}` | `AAPL` | Master record |
| `tickers/{SYMBOL}/memory/{MONTH_ID}` | `AAPL/memory/2026-04` | Episodic verdict history + adaptive scoring_weights |
| `screenings/{MONTH_ID}` | `2026-04` | Full scoring run output |
| `analysis/{TICKER}_{MONTH_ID}` | `AAPL_2026-04` | Cached debate output |
| `signals/{TICKER}_{MONTH_ID}` | `AAPL_2026-04` | Quarterly fundamentals |
| `picks/{TICKER}_{MONTH_ID}_{source}` | `AAPL_2026-04_judge` | Unified pick ledger |
| `performance/{MONTH_ID}_{source}` | `2026-04_judge` | Win rate, alpha, bull/bear/adaptive accuracy; `adaptive_picks_count`, `default_picks_count`, `adaptive_win_rate`, `default_win_rate` |
| `chunks/{DOC_ID}` | sha256 hash | EDGAR vector chunks (includes `section`, `embedder_model` fields) |
| `analysis/{TICKER}/disclosures/{run_id}` | ISO timestamp | Per-run EDGAR retrieval observability: chunk scores (min/max/mean), dedup dropped count, empty-retrieval marker |
| `eval/{MONTH_ID}` | `2026-04` | Monthly quality metrics + acid test |
| `calibration_history/{month_id}_{source}` | `history_2026-04_judge` | Per-run weight adjustment history: `W1_before/after`, `W2_before/after`, `W3_before/after`, `delta_magnitude`, `drift_flags_count`, `calibration_ok` |
| `eval_trend/{MONTH_ID}` | `2026-04` | Monthly eval metrics time series: confidence tier accuracy, `confidence_gap`, `confidence_calibration`, rubric sub-scores |
| `calibration/{Nm_source}` | `12m_judge` | Rolling N-month calibration report (High>Med>Low check) |
| `calibration/weights_{source}` | `weights_judge` | Recommended confidence weight overrides; written when calibration drifts |
| `events/{ID}` | uuid | Event log |

---

## Signal Model

4 signals normalised to 0–100 Z-score, merged by configurable weights:

| Signal | Weight | Source | Failure behaviour |
|--------|--------|--------|-------------------|
| Technical (RSI, MA50/200, volume, momentum) | 20% | yfinance | **Abort run** if fails |
| Earnings yield (E/P) | 30% | yfinance | Impute 50.0 (sector-neutral) |
| FCF yield | 30% | yfinance (quarterly cache) | Impute 50.0 |
| EBITDA/EV | 20% | yfinance (quarterly cache) | Impute 50.0 |

MA200 gate: score × 1.0 if price > MA200, × 0.5 if below.

---

## Eval Pipeline

Runs monthly (Step 4). Scores prior month's picks on decision quality (0–100) using mathematical confidence metrics. Outputs to `eval/{MONTH_ID}`:
- Overall accuracy, bull accuracy, bear accuracy, directional bias
- Confidence calibration (do high-confidence picks beat low-confidence?)
- Acid test: max drawdown by confidence tier (High ≥70 / Med 40–69 / Low <40)
- Disclosure citation rate (% of analyses that cited SEC filings)

Optional: Set `eval.rubric_sample_rate: 0.0–1.0` in config (default 0). When > 0, randomly samples `ceil(len(picks) × rubric_sample_rate)` picks and scores them with the LLM rubric. Averaged sub-scores (`rubric_sample_count`, `avg_reasoning_quality`, `avg_citation_density`, `avg_argument_structure`) are written to `EvalTrendDoc`.

After eval completes, an `EvalTrendDoc` is written to `eval_trend/{MONTH_ID}` with the full monthly metrics snapshot: `overall_accuracy`, `bull_accuracy`, `bear_accuracy`, confidence tier accuracies, `confidence_gap` (high-confidence minus low-confidence accuracy), `confidence_calibration` (gap between avg stated confidence and overall accuracy), `directional_bias`, `disclosure_citation_rate`, `avg_score`, and rubric sub-scores (if sampled).

Eval output feeds back into the Judge prompt next month as `eval_context`.

**Rolling calibration tracker** — also runs after each eval. On every run, writes a `CalibrationHistoryDoc` to `calibration_history/{month_id}_{source}` (gap-free time series). Reads the last 12 months of `PerformanceSnapshotDoc` records, aggregates per-tier alpha and win-rate, and checks that High > Med > Low holds by at least a 2pp gap. Results written to `calibration/12m_judge`. If the ordering is violated, recommended weight adjustments are written to `calibration/weights_judge` and picked up by `screener_job` on its next run. History doc captures before/after weight values (`W1_before/after`, `W2_before/after`, `W3_before/after`), delta magnitude, drift flag count, and calibration status.

---

## Loop Effectiveness Telemetry

The eval and calibration loops produce structured telemetry enabling direct measurement of system improvement over time.

**Three data sources:**

1. **Calibration History** (`calibration_history/{month_id}_{source}`) — written on every eval run (gap-free time series). Fields: `W1_before/after`, `W2_before/after`, `W3_before/after`, `delta_magnitude` (sum of absolute weight deltas), `drift_flags_count`, `calibration_ok` (bool), `timestamp`. When `calibration_ok=true`, `delta_magnitude=0`; when drift is detected and corrected, `delta_magnitude>0`.

2. **Eval Trend** (`eval_trend/{MONTH_ID}`) — written after each eval run. Includes `overall_accuracy`, per-tier accuracy (`high_confidence_accuracy`, `medium_confidence_accuracy`, `low_confidence_accuracy`), `confidence_gap` (high minus low tier accuracy), `confidence_calibration` (stated confidence vs actual accuracy), rubric sub-scores if sampling is enabled, and `avg_score`.

3. **Adaptive Weights Cohort** (per-pick `adaptive_weights_active` on `PickLedgerDoc`, aggregated in `PerformanceSnapshotDoc` as `adaptive_win_rate` vs `default_win_rate`) — tracks whether picks made with per-ticker adaptive weights beat default 50/50 splits.

**Interpreting the signals:**

- **Calibration convergence**: Query `calibration_history/` for the last 12 months. If `delta_magnitude` is shrinking over time, the confidence weight loop is settling. If deltas remain large and constant, the system is unstable (likely misaligned confidence model).

- **Confidence discrimination**: Query `eval_trend/` for the last 12 months. A healthy `confidence_gap` (high-confidence picks beating low-confidence by >10pp) indicates strong tier discrimination. If the gap is shrinking, the confidence model is losing predictive power.

- **Confidence calibration**: Check `confidence_calibration` (gap between average stated confidence and overall accuracy). This should shrink over time. A large positive gap means the system is overconfident; negative means underconfident.

- **Adaptive weight effectiveness**: Compare `adaptive_win_rate` to `default_win_rate` in `PerformanceSnapshotDoc`. If adaptive > default consistently, the per-ticker learning loop is working. If they converge, the loop may not have enough history yet (< 4 scored months per ticker).

**CLI queries** (see README for full syntax):
```
python -m screener.lib.exporter calibration-trend --months 12
python -m screener.lib.exporter eval-trend --months 12
```

---

## Key Invariants

- **Idempotent writes**: All storage writes use `set()` (upsert). Re-analysis is prevented by an explicit existence check in screener_job before invoking the debate graph.
- **Graceful degrade**: Signal failures → impute 50.0. EDGAR failure → empty disclosure block. News failure → skip analysis.
- **Hard rule**: HOLD forced if Judge confidence < 40.
- **Sector cap**: Max 3 picks per GICS sector in top N.
- **No HTTP surface**: All entry points are `python main.py`. No Flask, no inbound webhooks.
- **Zero hardcoded secrets**: All API keys resolved from env vars at runtime.
- **EDGAR observability**: Per-run chunk score stats (min/max/mean) and dedup counts are written to `analysis/{TICKER}/disclosures/{run_id}`. Empty retrieval (0 chunks above threshold) emits a WARN log and writes an `empty_retrieval` marker doc to the same path.
- **Embedder drift detection**: `index_ticker()` stamps `embedder_model` on every chunk doc and the freshness sentinel. If `llm.embedder_model` changes in config, the next run detects the mismatch and forces a full re-index for that ticker.
- **Section-aware retrieval**: Chunks are tagged with their 10-K/10-Q section heading at index time. Sections listed in `edgar.retrieval_sections` receive a +0.05 cosine score boost during retrieval. Empty list (default) disables boosting.
- **Disclosure token budget**: Chunks are dropped lowest-score-first when the cumulative token count would exceed `edgar.max_disclosure_tokens` (default 2048). Set to 0 to disable. Injection counts are logged at INFO level per run.
- **Calibration history append-only**: `calibration_history/{month_id}_{source}` is written on every eval run without gaps, even when `calibration_ok=true` (delta_magnitude=0 in those cases). This preserves a continuous time series for trend analysis.
- **Adaptive weights tagging**: Every `PickLedgerDoc` includes `adaptive_weights_active: bool`, set by `memory_read` node based on whether ≥4 scored months exist for the ticker. Enables downstream cohort analysis of adaptive vs default weight effectiveness.
