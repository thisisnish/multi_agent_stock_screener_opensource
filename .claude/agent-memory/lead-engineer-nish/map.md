# Project Map — Multi-Agent Stock Screener OSS

> WARNING: READ THIS FILE FIRST before touching any file in this repo.
> If a folder or file isn't listed here, run `ls` on it before modifying.

---

## Current State (as of 2026-04-28)
The repo is in early scaffold phase. Only root-level files exist. All source folders listed below are the TARGET structure from PLAN.md — they do not exist yet and must be created as part of P1-01.

---

## Top-level (target structure per PLAN.md)

```
multi_agent_stock_screener_opensource/
├── config/           — Runtime tunables: LLM models, signal weights, storage provider, email recipients
├── screener/         — Core library: agents, signals, scoring, storage, EDGAR, email, main pipeline
├── jobs/             — Cloud Run Job entry points (thin wrappers that call screener/ logic)
├── gcf/              — Cloud Functions (eval GCF only — scores prior month picks)
├── docker/           — One Dockerfile per Cloud Run Job
├── deploy/           — GCP setup scripts and Cloud Workflows YAML
├── tests/            — Unit and integration tests
├── AGENT.md          — Principal engineer overview (<=200 lines, read first for system context)
├── KANBAN.md         — All tickets by phase; check before starting any task
├── PLAN.md           — Authoritative implementation plan; check before modifying any interface
├── README.md         — Public-facing project documentation
└── requirements.txt  — Python dependencies
```

---

## config/ (target)
```
config/
├── config.yaml       — All runtime tunables: LLM models, signal weights, EDGAR, storage, notifications
├── tickers.yaml      — Static stock universe (S&P 500 default); NEVER auto-fetch replacements
└── .env.example      — All required secrets with comments (API keys, GCP creds, Resend key)
```

---

## screener/ (target)
```
screener/
├── main.py                        — run_screener(): full pipeline (score → top N → debate → picks → email)
├── agents/
│   ├── graph.py                   — LangGraph StateGraph definition and build_debate_graph()
│   ├── nodes.py                   — Node functions: debate_node (RunnableParallel), judge_node, conviction_node, etc.
│   ├── state.py                   — DebateState TypedDict
│   ├── prompts.py                 — System prompts for Bull, Bear, Judge, Narrator agents
│   └── news_agent.py              — News sentiment pipeline (DuckDuckGo → LLM)
├── metrics/
│   ├── technical.py               — Technical signal: RSI, MA50/200 cross, volume, momentum (Z-score, 0–100)
│   ├── earnings_yield.py          — Earnings yield (E/P) with sector-neutral imputation
│   ├── fcf_yield.py               — FCF yield from quarterly cache with fallback
│   ├── ebitda_ev.py               — EBITDA/EV with imputation fallback
│   ├── normalizer.py              — Signal normalisation utilities (moved from screener/lib/)
│   ├── confidence_scorer.py       — Confidence score: margin + ln(sources) − hedge penalty; HOLD forced <40
│   ├── conviction_scorer.py       — Conviction score: source diversity + hedge penalty (white-box)
│   ├── eval_scorer.py             — LLM rubric scoring of prior picks (0–100 decision quality)
│   ├── eval_rubric.py             — Rubric definitions for eval scoring
│   └── performance.py             — Win rate, alpha, bull/bear accuracy calculations
├── edgar/
│   ├── fetcher.py                 — Downloads 10-K/10-Q filings from EDGAR
│   ├── embedder.py                — Chunks (512 tokens, 10% overlap) and embeds via Gemini
│   └── retriever.py               — Cosine retrieval (threshold 0.7) from storage vector index
└── lib/
    ├── config.py                  — Pydantic Settings loader: reads config.yaml + .env; no Secret Manager client
    ├── models.py                  — Pydantic output models: BullCaseOutput, BearCaseOutput, JudgeOutput, ScoreResult
    ├── eval_metrics.py            — Aggregate accuracy, bias, calibration metrics (lives in lib/, NOT metrics/)
    ├── email_sender.py            — Resend API integration + HTML report templating
    └── storage/
        ├── __init__.py            — get_storage() factory: reads config.storage.provider, returns StorageClient
        ├── base.py                — Abstract StorageClient ABC (read, write, upsert, query, subcollection, vector_query)
        ├── firestore.py           — Firestore implementation (single DB: multi-agent-stock-screener)
        ├── s3.py                  — S3 implementation with brute-force cosine for ~10k EDGAR chunks
        └── opensearch.py          — OpenSearch implementation (k-NN vector search)
```

---

## jobs/ (target)
```
jobs/
├── screener/
│   └── main.py           — Cloud Run Job entry: calls run_screener(), sys.exit(0); ~5 lines
├── financial_update/
│   └── main.py           — Fetches quarterly fundamentals; storage calls adapted to StorageClient
└── edgar_disclosure/
    └── main.py           — Fetches and embeds 10-K/10-Q; storage calls adapted to StorageClient
```

---

## gcf/ (target)
```
gcf/
└── eval/
    └── main.py           — Monthly eval GCF: scores prior picks, writes eval/{MONTH_ID} to storage; triggered via POST after screener_job
```

---

## docker/ (target)
```
docker/
├── Dockerfile.screener           — Image for screener_job (Cloud Run)
├── Dockerfile.financial_update   — Image for financial_update_job (Cloud Run)
└── Dockerfile.edgar_disclosure   — Image for edgar_disclosure_job (Cloud Run)
```

---

## deploy/ (target)
```
deploy/
├── setup_gcp.sh                        — One-time GCP resource creation (buckets, DBs, Secret Manager, Artifact Registry)
├── deploy_all.sh                       — Docker build + Artifact Registry push + Cloud Run redeploy
└── workflows/
    └── monthly_pipeline.yaml           — 4-step Cloud Workflows DAG: financial_update → edgar_disclosure → screener → eval GCF
```

---

## tests/ (target)
```
tests/
├── signals/    — Unit tests: technical, earnings, FCF, EBITDA signal fetchers with edge cases [unknown structure, run ls before modifying]
├── scoring/    — Unit tests: composite score, MA200 gate, sector cap logic [unknown structure, run ls before modifying]
├── agents/     — Unit tests: debate nodes, graph compilation, LangGraph state transitions [unknown structure, run ls before modifying]
├── storage/    — Unit tests: StorageClient implementations, idempotent write, subcollection ops [unknown structure, run ls before modifying]
└── eval/       — Unit tests: eval scorer, rubric, metrics [unknown structure, run ls before modifying]
```

---

## .claude/ (exists)
```
.claude/
├── agent-memory/
│   ├── lead-engineer-nish/
│   │   ├── context.md    — Engineering context quick-load (this agent's memory)
│   │   └── map.md        — This file
│   └── cam/              — [unknown, run ls before modifying]
└── agents/               — [unknown, run ls before modifying]
```

---

## Key Firestore Collections (schema)

| Collection | Example Doc ID | Purpose |
|---|---|---|
| `tickers/{SYMBOL}` | `AAPL` | Master record per ticker |
| `tickers/{SYMBOL}/memory/{MONTH_ID}` | `AAPL/memory/2026-04` | Per-run verdict history (episodic memory) |
| `tickers/{SYMBOL}/scoring_weights/current` | — | Adaptive bull/bear weights after 4+ months |
| `screenings/{MONTH_ID}` | `2026-04` | Monthly top N + all scored tickers |
| `analysis/{TICKER}_{MONTH_ID}` | `AAPL_2026-04` | Cached debate output (skip if exists) |
| `signals/{TICKER}_{MONTH_ID}` | `AAPL_2026-04` | Fundamentals written by financial_update job |
| `picks/{TICKER}_{MONTH_ID}_{source}` | `AAPL_2026-04_judge` | Unified pick ledger |
| `performance/{MONTH_ID}_{source}` | `2026-04_judge` | Win rate, alpha, accuracy per run |
| `chunks/{DOC_ID}` | sha256-hash | EDGAR vector chunks |
| `eval/{MONTH_ID}` | `2026-04` | Monthly eval report; injected as eval_context into Judge next month |
| `events/{ID}` | uuid | Typed event log |

---

## Files Explicitly Deleted vs Private Repo (do not recreate)
- `server.py` — replaced by `jobs/screener/main.py`
- `screener/agents/llm/providers/` — replaced by `init_chat_model`
- `screener/agents/llm/llm_client.py` — replaced by `init_chat_model`
- `screener/lib/firestore_io.py` — replaced by `screener/lib/storage/`
- `screener/lib/sp500_tickers.py` — tickers are static YAML now
- `gcf/tickers/main.py` — tickers are static config, no auto-fetch
- `gcf/pending/main.py` — Firestore event bus / tickrly integration removed
- `config/weights.yaml` + `config/notify.yaml` — merged into `config/config.yaml`
