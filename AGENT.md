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

## Core Execution Loop (`screener/main.py:run_screener()`)

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
| `memory_read` | sync | Read per-ticker verdict history; score prior pick if price moved |
| `build_context` | sync | Assemble signal blocks + EDGAR disclosure block |
| `debate_node` | LLM × 2 (parallel) | Bull + Bear run simultaneously via `RunnableParallel` |
| `conviction_node` | white-box | Source diversity + hedge penalty score for Bull and Bear |
| `judge_node` | LLM × 1 | Adjudicates debate; declares BUY/SELL/HOLD + margin + decisive factor |
| `confidence_node` | white-box | `W1·margin + W2·ln(sources) − W3·hedge` — no LLM tokens |
| `hard_rules` | sync | Force HOLD if confidence < 40; set `contested_truth` if conviction gap > 30pts + NARROW/CONTESTED |
| `memory_write` | sync | Persist verdict to `tickers/{SYMBOL}/memory/{MONTH_ID}` |

**Episodic memory**: Each ticker accumulates a scored verdict history. After ≥4 scored months, the debate adapts — Bull/Bear weights shift toward whichever side has been more accurate for this ticker.

**Systemic memory (RAG)**: 10-K and 10-Q filings are chunked (512 tokens, 10% overlap), embedded (Gemini text-embedding-001, dim 3072), and injected into Bull + Bear context. Retrieval uses cosine similarity with ticker pre-filter, threshold 0.7.

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
  embedder_model: "google_genai:models/gemini-embedding-001"
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
| `tickers/{SYMBOL}/memory/{MONTH_ID}` | `AAPL/memory/2026-04` | Episodic verdict history |
| `tickers/{SYMBOL}/scoring_weights/current` | — | Adaptive bull/bear weights |
| `screenings/{MONTH_ID}` | `2026-04` | Full scoring run output |
| `analysis/{TICKER}_{MONTH_ID}` | `AAPL_2026-04` | Cached debate output |
| `signals/{TICKER}_{MONTH_ID}` | `AAPL_2026-04` | Quarterly fundamentals |
| `picks/{TICKER}_{MONTH_ID}_{source}` | `AAPL_2026-04_judge` | Unified pick ledger |
| `performance/{MONTH_ID}_{source}` | `2026-04_judge` | Win rate, alpha, bull/bear accuracy |
| `chunks/{DOC_ID}` | sha256 hash | EDGAR vector chunks |
| `eval/{MONTH_ID}` | `2026-04` | Monthly quality metrics + acid test |
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

Runs monthly (Step 4). Scores prior month's picks on decision quality (0–100) using an LLM rubric. Outputs:
- Overall accuracy, bull accuracy, bear accuracy, directional bias
- Confidence calibration (do high-confidence picks beat low-confidence?)
- Acid test: max drawdown by confidence tier (High ≥70 / Med 40–69 / Low <40)
- Disclosure citation rate (% of analyses that cited SEC filings)

Eval output feeds back into the Judge prompt next month as `eval_context`.

---

## Key Invariants

- **Idempotent writes**: `write()` raises if doc exists. Skip logic prevents re-analysis of already-cached tickers.
- **Graceful degrade**: Signal failures → impute 50.0. EDGAR failure → empty disclosure block. News failure → skip analysis.
- **Hard rule**: HOLD forced if Judge confidence < 40.
- **Sector cap**: Max 3 picks per GICS sector in top N.
- **No HTTP surface**: All entry points are `python main.py`. No Flask, no inbound webhooks.
- **Zero hardcoded secrets**: All API keys resolved from env vars at runtime.
