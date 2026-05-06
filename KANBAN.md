# KANBAN — Multi-Agent Stock Screener (Open Source)

_Last updated: 2026-05-06 (BUG-01 to BUG-11 in progress)_

---

## Phase 1 — Core Engine

### Backlog

- **[P1-01]** ~~Project scaffold — directory structure, pyproject.toml, requirements.txt, Docker setup~~ ✅ done
  - [P1-01a] ~~Repo init + core file layout (jobs/, screener/, deploy/, config/, tests/, docker/)~~
  - [P1-01b] ~~Docker image per job (financial_update, edgar_disclosure, screener, eval GCF)~~

- **[P1-02]** ~~Config system — YAML config loader, environment variable injection, defaults~~ ✅ done
  - [P1-02a] ~~config.yaml schema (LLM, storage, signals, screener, notifications, EDGAR)~~
  - [P1-02b] ~~.env.example with secrets template (API keys, GCP creds, email)~~
  - [P1-02c] ~~Env var interpolation and validation at startup~~

- **[P1-03]** ~~LLM factory — unified `init_chat_model()` supporting 5+ providers via LangChain~~ ✅ done
  - [P1-03a] ~~Provider routing: Anthropic, OpenAI, Gemini, Groq, Ollama (local)~~
  - [P1-03b] ~~Per-agent model override (bull_model, bear_model, judge_model, narrator_model, embedder_model)~~
  - [P1-03c] ~~Structured output via `.with_structured_output(PydanticModel)`~~

- **[P1-04]** ~~Storage abstraction — base DAO interface (Firestore, S3, OpenSearch)~~ ✅ done
  - [P1-04a] ~~Abstract base class (StorageDAO) with CRUD methods~~
  - [P1-04b] ~~Firestore implementation (native vector search via index)~~
  - [P1-04c] ~~S3 implementation (brute-force cosine similarity for ~10k chunks)~~
  - [P1-04d] ~~OpenSearch implementation (k-NN vector search)~~
  - [P1-04e] ~~Collection schema: tickers/, memory/, picks/, performance/, chunks/, eval/, events/~~

- **[P1-05]** ~~Signal fetcher — Technical, Earnings, FCF, EBITDA signals from yfinance~~ ✅ done
  - [P1-05a] ~~Technical signal: RSI, MA50/200 cross, volume, momentum (Z-score normalised 0–100)~~
  - [P1-05b] ~~Earnings yield (E/P) with sector-neutral imputation (50.0 fallback)~~
  - [P1-05c] ~~FCF yield from quarterly cache with imputation fallback~~
  - [P1-05d] ~~EBITDA/EV with imputation fallback~~
  - [P1-05e] ~~MA200 gate: score × 1.0 if price > MA200, × 0.5 if below~~

- **[P1-06]** ~~Scoring engine — composite score (weighted signals + MA200 gate), sector-cap enforcement~~ ✅ done
  - [P1-06a] ~~Weighted composite: Technical(20%) + Earnings(30%) + FCF(30%) + EBITDA(20%)~~
  - [P1-06b] ~~MA200 gating logic~~
  - [P1-06c] ~~Sector concentration cap (max 3 picks per GICS sector in top N)~~
  - [P1-06d] ~~Top-N selection + sector-balanced ranking~~

- **[P1-07]** ~~Multi-agent debate — Bull/Bear/Judge LangGraph state machine with episodic memory + EDGAR RAG~~ ✅ done
  - [P1-07a] ~~LangGraph StateGraph: memory_read → build_context → [debate_node, conviction_node] → judge_node → confidence_node → hard_rules → memory_write~~
  - [P1-07b] ~~Bull & Bear nodes (parallel) with signal + EDGAR context injection~~
  - [P1-07c] ~~Judge node: adjudicates, declares BUY/SELL/HOLD + margin + decisive factor~~
  - [P1-07d] ~~Episodic memory: per-ticker verdict history, 4+ month threshold for adaptive weighting~~
  - [P1-07e] ~~EDGAR RAG: 10-K/10-Q chunking (512 tokens, 10% overlap), embedding (Gemini text-embedding-001), cosine retrieval (threshold 0.7)~~
  - [P1-07f] ~~Conviction scoring: source diversity + hedge penalty (white-box, no tokens)~~
  - [P1-07g] ~~Confidence scoring: margin + ln(sources) − hedge penalty, HOLD forced if <40~~
  - [P1-07h] ~~Hard rules: contested truth detection (conviction gap > 30pts + NARROW/CONTESTED)~~

- **[P1-08]** ~~Eval pipeline — monthly pick quality scoring, feedback loop injection into Judge prompt~~ ✅ done
  - [P1-08a] ~~Eval node: LLM rubric scores prior month's picks (0–100 decision quality)~~
  - [P1-08b] ~~Accuracy metrics: overall, bull-only, bear-only, directional bias~~
  - [P1-08c] ~~Confidence calibration: high-confidence (≥70) vs medium (40–69) vs low (<40) bins~~
  - [P1-08d] ~~Acid test: max drawdown by confidence tier~~
  - [P1-08e] ~~Disclosure citation rate (% analyses citing SEC filings)~~
  - [P1-08f] ~~Eval context injection into Judge prompt the following month~~ (completed in BUG-08 fix)

- **[P1-09]** ~~Email reporter — Resend integration, narrative generation, HTML templating~~ ✅ done
  - [P1-09a] ~~Resend API integration + sender verification~~
  - [P1-09b] ~~HTML email template for monthly report (picks, verdicts, performance curves)~~
  - [P1-09c] Narrative generation (narrator_model for summary copy) - SKIP
  - [P1-09d] ~~Recipient config from config.yaml (notifications.email.recipients)~~

- **[P1-10]** ~~GCP deployment — Cloud Run Jobs, Cloud Workflows, Cloud Scheduler, Secret Manager setup~~ ✅ done
  - [P1-10a] ~~Cloud Run Jobs: financial_update_job, edgar_disclosure_job, screener_job~~
  - [P1-10b] ~~Eval Cloud Function (eval GCF)~~
  - [P1-10c] ~~Cloud Workflows: sp500-monthly-pipeline (4-step DAG)~~
  - [P1-10d] ~~Cloud Scheduler: monthly trigger (1st Friday, 9AM ET)~~
  - [P1-10e] ~~Secret Manager: API keys, GCP creds, email config~~
  - [P1-10f] ~~setup_gcp.sh (one-time resource creation)~~
  - [P1-10g] ~~deploy_all.sh (Docker build, Artifact Registry push, Cloud Run redeploy)~~

- **[P1-10h]** Consolidate all GCP resources to us-west1 — update deploy_all.sh default region, redeploy jobs, verify Scheduler trigger

- **[P1-11]** Unit tests — signal fetcher, scoring, debate nodes, storage DAOs
  - [P1-11a] tests/signals/ — technical, earnings, FCF, EBITDA with edge cases
  - [P1-11b] tests/scoring/ — composite score, MA200 gate, sector cap logic
  - [P1-11c] tests/agents/ — debate nodes, conviction, confidence, hard rules
  - [P1-11d] tests/storage/ — DAO CRUD, vector search, idempotency

- **[P1-12]** Integration tests — full local dry-run pipeline (--dry-run flag)
  - [P1-12a] screener/main.py --dry-run (no storage writes, no email)
  - [P1-12b] End-to-end test: fetch signals → score → debate → eval feedback → email render

- **[P1-13]** README & quick-start guide
  - [P1-13a] How It Works section (6 steps: score, debate, memory, SEC filings, eval, report)
  - [P1-13b] Prerequisites (Python 3.11+, GCP or AWS, LLM key, Resend)
  - [P1-13c] Quick Start (clone, venv, config, --dry-run)
  - [P1-13d] Configuration section (LLM models, stock universe, signal weights, screener settings, notifications, storage, EDGAR)
  - [P1-13e] Secrets (.env template)
  - [P1-13f] Running locally (dry-run, full run, financial_update, edgar_disclosure, tests)
  - [P1-13g] GCP deployment (setup_gcp.sh, deploy_all.sh, manual trigger)
  - [P1-13h] Updating tickers (config/tickers.yaml, no auto-fetch)
  - [P1-13i] Switching storage backends (Firestore → S3 example)
  - [P1-13j] Architecture overview (Cloud Workflows DAG)
  - [P1-13k] Cost estimates (LLM, EDGAR, Cloud Run, Firestore, email)
  - [P1-13l] License (MIT)

- **[P1-14]** AGENT.md — technical deep-dive (architecture, data model, invariants)
  - [P1-14a] System architecture (Cloud Scheduler → Cloud Workflows → 4 jobs)
  - [P1-14b] Execution loop (fetch → normalize → score → debate → write → email)
  - [P1-14c] LangGraph debate architecture (7-node linear graph)
  - [P1-14d] LLM configuration (provider routing, per-agent overrides)
  - [P1-14e] Storage collections + doc ID patterns
  - [P1-14f] Signal model (4 signals, weights, failure modes, MA200 gate)
  - [P1-14g] Eval pipeline (quality scoring, acid test, confidence bins)
  - [P1-14h] Key invariants (idempotency, graceful degrade, hard rules, sector cap, no HTTP, no hardcoded secrets)

- **[P1-15]** Deployment guide (setup_gcp.sh, deploy_all.sh, manual trigger docs)
  - [P1-15a] One-time GCP resource setup
  - [P1-15b] Docker image build & Artifact Registry push
  - [P1-15c] Cloud Run Job redeploy
  - [P1-15d] Manual trigger instructions (Cloud Workflows console, individual job trigger)

---

## Phase 1.5 — Critical Bug Fixes (Post-First-Run)

_Generated 2026-05-04 after first end-to-end pipeline run. These tickets block data integrity and feature completeness._

### Backlog

- **[P1-BUG-01]** ~~Judge verdict missing from email table~~ ✅ done
- **[P1-BUG-02]** ~~Firestore `memory/` flat collection → `tickers/{SYMBOL}/memory/{MONTH_ID}` subcollection~~ ✅ done
- **[P1-BUG-03]** ~~Picks doc ID scheme: `{TICKER}_{WEEK_ID}` → `{TICKER}_{MONTH_ID}_{source}`~~ ✅ done
- **[P1-BUG-04]** ~~Financial signals not written to Firestore~~ ✅ done
- **[P1-BUG-05]** ~~Technical signal: RSI=0.0 / Price=$0.00 (yfinance data never fetched)~~ ✅ done

- **[P1-BUG-06]** ~~Missing `tickers/` master collection~~ ✅ done
  - `ticker_doc_id()` helper added to schema; `TickerSignalDoc` updated with `latest_screening_date` and `active` fields
  - `screener_job` upserts one doc per scored ticker into `tickers/` after scoring, before debate
  - 21 unit tests in `tests/storage/test_ticker_write.py`

- **[P1-BUG-07]** ~~Missing `performance/` collection — picks not tracked against SPY~~ ✅ done
  - `performance_doc_id()` helper added to schema; `PerformanceSnapshotDoc` updated with `month_id`, `source`, `created_at`, `entry_spy_price`, `avg_alpha_pct` fields
  - `PickLedgerDoc` gains `alpha_pct` field
  - `screener/performance/tracker.py` added: `fetch_spy_price()`, `build_pick_ledger_entries()`, `build_performance_snapshot()`, `write_performance_docs()`
  - `screener_job` calls `write_performance_docs()` after picks are written; SPY price fetched via yfinance (graceful-degrade on failure)
  - Per-pick ledger entries written to `performance/{TICKER}_{MONTH_ID}_{source}`; monthly snapshot to `performance/{MONTH_ID}_{source}`
  - 48 unit tests in `tests/storage/test_performance_write.py`

- **[P1-BUG-08]** ~~Eval pipeline not implemented (Step 4)~~ ✅ done
  - `screener/eval/` fully implemented: `metrics.py` (compute_metrics, acid_test, disclosure_citation_rate), `rubric.py` (default/aggressive/conservative rubrics), `scorer.py` (pure-math + LLM-rubric scoring paths)
  - `gcf/eval/main.py` Cloud Function: reads closed picks → scores → computes EvalMetrics + acid_test + disclosure_rate → writes `eval/{eval_doc_id}` doc → surfaces `eval_context`
  - `screener/eval/loader.py` added: `prior_month_id()` + `fetch_eval_context_async()` — loads prior-month eval doc from Firestore (graceful degrade on missing doc or storage error)
  - `jobs/screener/main.py` updated: fetches `eval_context` once before debate loop; injects into every `graph.ainvoke()` call so Judge receives prior-month feedback
  - `build_judge_context()` in `screener/agents/prompts.py` already handles `eval_context` injection into Judge prompt (pre-existing)
  - Cloud Workflows Step 4 already wired to `eval-handler` GCF (pre-existing)
  - 52 unit tests: `tests/eval/test_gcf_main.py` (37 tests) + `tests/eval/test_loader.py` (15 tests)

- **[P1-BUG-09]** ~~Missing `screenings/` collection~~ ✅ done
  - `SCREENINGS` constant + `screening_run_doc_id()` helper added to schema; `TickerScreeningEntry` + `ScreeningDoc` Pydantic models added
  - `screener/screening/writer.py` added: `build_ticker_entries()`, `build_screening_doc()`, `write_screening_doc()`
  - `screener_job` calls `write_screening_doc()` after `_write_ticker_docs()` and before the debate loop
  - ScreeningDoc captures: all scored tickers, top_n_before_cap, top_n_after_cap (sector-cap audit trail), sector_distribution, signal_vintage_dates
  - 60 unit tests in `tests/storage/test_screening_write.py`

- **[P1-BUG-10]** ~~Missing `analysis/` collection (debate cache)~~ ✅ done
  - `ANALYSIS` constant + `analysis_doc_id()` helper added to schema; `AnalysisDoc` Pydantic model added
  - `screener/analysis/writer.py` added: `write_analysis_doc()` extracts bull/bear/judge fields from LangGraph state
  - `screener_job` calls `write_analysis_doc()` after each debate; checks for existing doc to skip re-analysis (idempotency)

- **[P1-BUG-13]** ~~`signals/` collection: only 1 doc written per run~~ ✅ done
  - **Root cause**: `asyncio.run(dao.set(...))` was called once per ticker inside the fetch loop; the Firestore `AsyncClient` gRPC channel is bound to the first event loop, which is destroyed after the first `asyncio.run()` returns — all subsequent writes raised `RuntimeError: Event loop is closed` and were swallowed by `try/except`
  - **Fix**: Collect `(doc_id, payload)` pairs during the synchronous fetch loop; flush all writes in one `asyncio.run(_write_all())` via `asyncio.gather()` — one event loop, all N writes
  - Confirmed from Cloud Run logs: after fix `success=10 errors=0`

- **[P1-BUG-11]** Missing `events/` collection — pipeline event logging not implemented — _priority: P2 (observability)_
  - **Issue**: AGENT.md specifies `events/{ID}` for pipeline lifecycle events. No event logging code exists.
  - **Fix**: Create EventDoc schema if not exists. Emit events at key pipeline checkpoints: job_started, signals_fetched, scoring_complete, debate_start, debate_complete, picks_written, email_sent, eval_complete. Write to `events/` collection.
  - **Acceptance criteria**:
    - EventDoc schema defined and wired to DAO
    - Events emitted at minimum: job_started, scoring_complete, debate_complete, picks_written
    - Each event has timestamp, job_name, step, status, duration, error details
    - Queryable for troubleshooting

- **[P1-BUG-12]** Adaptive Bull/Bear weights not implemented — _priority: P1 (feature incomplete)_
  - **Issue**: AGENT.md specifies after ≥4 scored months, debate adapts — Bull/Bear weights shift toward whichever side has been more accurate. Current memory stores static `{bull: 0.5, bear: 0.5, sample_size: 0}` weights that never change.
  - **Fix**: After memory_read, compute bull/bear accuracy from episodic memory (≥4 verdicts). Update `scoring_weights/current` subcollection or weights field. Inject into Judge prompt (e.g., "Bull has been 65% accurate on AAPL; Bear 35%"). Apply weights to conviction scoring.
  - **Acceptance criteria**:
    - Weights updated monthly after 4+ months of verdict history
    - Weights stored in `tickers/{SYMBOL}/scoring_weights/current` or memory doc
    - Judge prompt includes per-ticker accuracy context
    - Conviction scoring applies adaptive weights to Bull + Bear sides

---

## Phase 2 — Observability & Hardening

### Backlog

- **[P2-01]** Structured logging — JSON event logs to Firestore `events/` collection, Stackdriver integration — _priority: high_
  - [P2-01a] Event schema (timestamp, job name, step, status, duration, error details, source)
  - [P2-01b] Emit events from all three job entry points (financial_update_job, edgar_disclosure_job, screener_job, eval GCF)
  - [P2-01c] Firestore `events/` collection writer
  - [P2-01d] Stackdriver logging integration (JSON sink)

- **[P2-02]** Alerting on pipeline failures — Cloud Monitoring alerts for Cloud Run job failures, retries logic — _priority: high_
  - [P2-02a] Cloud Monitoring uptime checks for Cloud Workflows execution
  - [P2-02b] Alert policy: notify if any Cloud Run job exits non-zero
  - [P2-02c] Retry logic: exponential backoff for transient failures (yfinance timeout, API rate limit, etc.)
  - [P2-02d] Max retries threshold (e.g., 3 attempts before alert)

- **[P2-03]** Monitoring dashboard — Cloud Monitoring dashboard showing SLA metrics (run time, accuracy by tier, cost) — _priority: medium_
  - [P2-03a] Execution latency (financial_update, edgar_disclosure, screener, eval)
  - [P2-03b] Accuracy by confidence tier (High ≥70 / Med 40–69 / Low <40)
  - [P2-03c] LLM token usage per run
  - [P2-03d] Storage I/O metrics (reads, writes, vector search calls)
  - [P2-03e] Cost breakdown (LLM, Cloud Run, Firestore, EDGAR embedding)

- **[P2-04]** Error recovery — idempotency keys, retry backoff, partial run recovery (skip cached tickers if n-1 fail) — _priority: high_
  - [P2-04a] Idempotency key generation (hash of month_id + ticker + step)
  - [P2-04b] Skip logic: if analysis doc exists, skip that ticker (already cached)
  - [P2-04c] Partial run recovery: if N−1 tickers fail, continue with others, report partial results
  - [P2-04d] Transient failure detection: retry network timeouts, rate limits; fail fast on validation errors

- **[P2-05]** Performance profiling — log LLM latency, token usage per run, storage I/O, identify bottlenecks — _priority: medium_
  - [P2-05a] LLM latency per debate (bull_latency, bear_latency, judge_latency)
  - [P2-05b] Token counters (input, output per agent, per run)
  - [P2-05c] Storage I/O latency (read, write, vector search)
  - [P2-05d] Bottleneck report (which step is slowest)

---

## Phase 3 — Advanced Evaluation & Feedback

### Backlog

- **[P3-01]** Adaptive Bull/Bear weighting — increase weight on side with higher historical accuracy per ticker — _priority: medium_
  - [P3-01a] Compute per-ticker bull/bear accuracy from episodic memory (≥4 months)
  - [P3-01b] Store adaptive weights in `tickers/{SYMBOL}/scoring_weights/current`
  - [P3-01c] Inject weights into Judge prompt (e.g., "Bull has been 65% accurate on this ticker historically")
  - [P3-01d] Decay old verdicts (e.g., weight recent 12 months higher)

- **[P3-02]** Confidence calibration tracking — measure if high-confidence picks outperform low-confidence ones — _priority: medium_
  - [P3-02a] Bin verdicts by confidence (High ≥70 / Med 40–69 / Low <40)
  - [P3-02b] Eval each bin separately; compute win rate by bin
  - [P3-02c] Calibration metric: High should beat Med, Med should beat Low
  - [P3-02d] Adjust confidence formula if calibration drifts

- **[P3-03]** Sector-specific Judge variants — separate Judge prompts per GICS sector, trained on sector history — _priority: low_
  - [P3-03a] Partition episodic memory by sector
  - [P3-03b] Create sector-specific Judge prompt templates (e.g., financials Judge mentions rate sensitivity)
  - [P3-03c] Route Judge based on ticker sector
  - [P3-03d] A/B test sector-specific vs. generic Judge

- **[P3-04]** News injection — optional real-time market news context into debate (NewsAPI or similar) — _priority: low_
  - [P3-04a] News fetcher (NewsAPI or similar, configurable)
  - [P3-04b] Inject top 3 recent headlines into Bull + Bear context
  - [P3-04c] Toggle via config (news.enabled: true/false)
  - [P3-04d] Rate-limit news API calls (e.g., 5 per run)

---

## Phase 4 — Web Portal & Customization

### Backlog

- **[P4-01]** Web dashboard — authenticated portal to view past picks, verdicts, performance, accuracy curves — _priority: medium_
  - [P4-01a] Frontend framework (React or Svelte)
  - [P4-01b] Auth layer (Google Sign-In or similar)
  - [P4-01c] Picks table: ticker, verdict, margin, confidence, outcome, performance
  - [P4-01d] Performance curves: accuracy over time, alpha vs. SPY, drawdown
  - [P4-01e] Backend API (FastAPI or similar, read-only Firestore queries)

- **[P4-02]** Pick history export — CSV/JSON export of all picks with verdicts, sources, performance — _priority: medium_
  - [P4-02a] Export endpoint (GET /api/picks/export?format=csv|json)
  - [P4-02b] Schema: ticker, month, verdict, margin, confidence, bull_reasoning, bear_reasoning, judge_decisive_factor, outcome
  - [P4-02c] Email export link (monthly report includes download)

- **[P4-03]** Universe customization UI — allow users to upload custom tickers, adjust weights, preview scores — _priority: medium_
  - [P4-03a] Upload tickers (CSV or paste list)
  - [P4-03b] Sector assignment UI or auto-detect
  - [P4-03c] Signal weight slider (adjust Technical, Earnings, FCF, EBITDA percentages)
  - [P4-03d] Preview score calculation (show how top 10 would rank with new config)
  - [P4-03e] Save custom universes to Firestore

- **[P4-04]** Email customization — user-defined recipient lists, format preferences (HTML/plain text), frequency — _priority: low_
  - [P4-04a] Recipient management UI
  - [P4-04b] Format choice (HTML, plain text)
  - [P4-04c] Frequency option (monthly only for now)

---

## Phase 5 — Scale & Governance

### Backlog

- **[P5-01]** Multi-universe support — allow separate runs for S&P 500, Russell 2000, NASDAQ 100, crypto, bonds — _priority: low_
  - [P5-01a] Generalize config to support multiple universes per run
  - [P5-01b] Cloud Workflows DAG: separate pipeline per universe
  - [P5-01c] Storage: namespace picks/eval/performance by universe
  - [P5-01d] Email: separate report per universe or combined

- **[P5-02]** Comparative mode — show performance of alt-strategies (buy-and-hold SPY, equal-weight, market-cap) — _priority: low_
  - [P5-02a] Fetch SPY daily returns
  - [P5-02b] Compute equal-weight and market-cap-weight benchmarks
  - [P5-02c] Overlay on performance curves (screener vs. SPY vs. benchmarks)
  - [P5-02d] Statistical significance tests (alpha, Sharpe, max drawdown)

- **[P5-03]** OSS governance — contribution guide, CLA, issue triage, release process — _priority: medium_
  - [P5-03a] CONTRIBUTING.md (fork, branch, test, PR, code review, CI)
  - [P5-03b] CODEOWNERS file + branch protection rules
  - [P5-03c] Issue template (bug, feature request, discussion)
  - [P5-03d] Release checklist (changelog, version bump, tag, GitHub Release)
  - [P5-03e] CLA (Contributor License Agreement, if needed)

- **[P5-04]** Commercial license path — option for closed-source proprietary use or SaaS wrapper — _priority: low_
  - [P5-04a] Dual-license strategy (MIT for OSS, commercial for proprietary)
  - [P5-04b] SaaS wrapper: hosted API endpoint, managed Firestore + Cloud Run
  - [P5-04c] Feature gates (free tier: monthly only, limited universe; paid tier: weekly runs, custom universes, portfolio tracking)

---

## Tech Debt

### Backlog

- **[TB-08]** Consolidate all GCP resources to us-west1 region — _priority: medium_
  - [TB-08a] Update `deploy_all.sh` default region to us-west1 (currently us-central1)
  - [TB-08b] Redeploy all Cloud Run Jobs (financial_update_job, edgar_disclosure_job, screener_job) to us-west1
  - [TB-08c] Redeploy Cloud Scheduler and Cloud Workflows to us-west1
  - [TB-08d] Verify Cloud Workflows monthly trigger fires and executes in us-west1
  - [TB-08e] Update setup_gcp.sh with us-west1 as default region for new deployments
  - [TB-08f] Document region choice in README.md (us-west1 chosen for consistency with workflow region)

- **[TB-07]** ~~Implement `EDGARRetriever` indexer — SEC EDGAR fetch, chunking, embedding, vector store write~~ ✅ done
  - [TB-07a] ~~Fetch 10-K/10-Q filings from SEC EDGAR for a given ticker~~
  - [TB-07b] ~~Chunk filings (512 tokens, 10% overlap)~~
  - [TB-07c] ~~Embed chunks via configured embedder model (Gemini text-embedding-001)~~
  - [TB-07d] ~~Write chunk vectors to storage DAO (`chunks/` collection)~~
  - [TB-07e] ~~Respect `edgar.freshness_days` — skip ticker if index is fresh~~
  - [TB-07f] ~~Expose as `EDGARRetriever(app_config, dao).index_ticker(symbol, dry_run)` in `screener/edgar/retriever.py`~~

- **[TB-01]** Firestore vector index costs — assess whether to migrate EDGAR to OpenSearch for cost savings — _priority: low_
  - [TB-01a] Cost modeling: estimate monthly Firestore vector index cost at S&P 500 scale
  - [TB-01b] OpenSearch POC: stand up self-hosted cluster, benchmark vector search latency
  - [TB-01c] Decision gate: if OpenSearch is <50% cost, migrate

- **[TB-02]** S3 vector search scalability — implement k-NN approximation (LSH or similar) for >50k chunks — _priority: low_
  - [TB-02a] Profile in-memory cosine similarity at 50k chunks (memory, latency)
  - [TB-02b] If unacceptable, implement LSH (Locality Sensitive Hashing) or FAISS
  - [TB-02c] Benchmark new approach

- **[TB-03]** EDGAR refresh logic — currently always re-indexes on freshness_days boundary; consider full-text hash-based detection — _priority: low_
  - [TB-03a] Hash content of 10-K/10-Q on fetch
  - [TB-03b] Skip re-indexing if hash unchanged
  - [TB-03c] Reduce redundant vector embeddings

- **[TB-04]** Config validation — add JSON schema or Pydantic validation to config.yaml at startup — _priority: medium_
  - [TB-04a] Define Pydantic model for config.yaml schema
  - [TB-04b] Validate at app startup; fail fast with clear error messages
  - [TB-04c] Unit test: valid + invalid configs

- **[TB-05]** Sector taxonomy drift — GICS sectors change; add automation to sync tickers.yaml yearly — _priority: low_
  - [TB-05a] Fetch current GICS sector assignments (SEC or Bloomberg source)
  - [TB-05b] Detect tickers moved to new sectors
  - [TB-05c] Auto-update tickers.yaml, raise PR for review

- **[TB-06]** LLM provider fallback — if primary LLM unavailable, fall back to secondary provider gracefully — _priority: medium_
  - [TB-06a] Config: add secondary_model option
  - [TB-06b] Debate node: catch provider errors, retry with secondary model
  - [TB-06c] Log fallback event
  - [TB-06d] Unit test: mock primary failure, verify fallback works

---

## Open Questions

_None currently. All tickets are grounded in README.md, AGENT.md, and config files. Tickers are user-configured via config/tickers.yaml — no auto-fetch is planned or in scope._
