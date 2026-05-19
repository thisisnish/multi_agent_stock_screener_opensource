# Multi-Agent Stock Screener

An automated equity screener powered by a Bull/Bear/Judge multi-agent debate. Every month it scores your stock universe, runs a structured LLM debate on the top picks, tracks performance against SPY, and emails you a report.

Works with any LLM provider (Anthropic, OpenAI, Gemini, Ollama, Groq). Storage runs on Firestore, S3, or OpenSearch. You own all the API keys and infra.

---

## How It Works

1. **Score** — Every ticker is scored across four signals: technical momentum, earnings yield, FCF yield, and EBITDA/EV. Scores are normalised and weighted into a composite 0–100 rank.

2. **Debate** — The top 10 picks (max 3 per sector) go into a three-agent debate:
   - **Bull** makes the strongest possible case for the stock
   - **Bear** rebuts it and identifies hidden risks
   - **Judge** weighs both sides and declares BUY, SELL, or HOLD

3. **Memory** — The system tracks every verdict and whether it turned out to be correct. After enough history, the debate adapts — if Bull has been more accurate for a given ticker, its arguments carry more weight with the Judge.

4. **SEC filings** — 10-K and 10-Q filings are indexed monthly, section-tagged, deduplicated, and injected into the debate context with per-chunk relevance scores. Retrieval metrics (scores, dedup stats, empty-retrieval markers) are written to Firestore for observability. Bull and Bear must cite which sources they used.

5. **Eval** — Once a month, all closed picks are scored for decision quality (0–100). The score feeds back into the next month's Judge prompt so the system gets better over time.

6. **Report** — Results are emailed to your configured recipients.

> For a technical deep-dive into the agent architecture, data model, and invariants — see [AGENT.md](./AGENT.md).

---

## Verifying Loop Effectiveness

The eval loop and calibration loop produce structured telemetry you can query directly to measure whether the system is actually improving over time.

### Is the calibration loop converging?

```bash
python -m screener.lib.exporter calibration-trend --months 12
```

Returns JSON with `calibration_ok_rate` (fraction of months where High>Med>Low held without drift), `avg_drift_flags`, and a `weight_delta_trend` list showing `delta_magnitude` per month. A healthy system shows weight adjustments (`delta_magnitude`) shrinking over time — the loop is settling toward stable confidence weights. Constant large deltas indicate the confidence model is unstable or misaligned with actual pick quality.

### Is the confidence gap widening?

```bash
python -m screener.lib.exporter eval-trend --months 12
```

Returns monthly accuracy broken down by confidence tier. Key fields:
- `confidence_gap`: high-confidence accuracy minus low-confidence accuracy. This should widen as the system improves at discriminating strong vs weak picks.
- `confidence_calibration`: gap between average stated confidence and overall accuracy. This should shrink over time (confidence should be realistic).

If both metrics are stable or moving in the wrong direction, the Judge's confidence model needs recalibration.

### Are adaptive weights beating default weights?

Query Firestore `performance/{MONTH_ID}_judge` documents directly (or via your preferred Firestore client). Each `PerformanceSnapshotDoc` includes:
- `adaptive_picks_count`, `adaptive_win_rate`: accuracy of picks made with per-ticker adaptive bull/bear weights
- `default_picks_count`, `default_win_rate`: accuracy of picks made with default 50/50 weights

If `adaptive_win_rate > default_win_rate` consistently across months, the per-ticker learning loop is working. If they converge, you may not have enough historical data per ticker yet (the system needs ≥4 scored months per ticker to unlock adaptive weights).

### Optional: LLM reasoning quality

To measure reasoning quality directly via LLM assessment:

1. Update `config/config.yaml`:
```yaml
eval:
  rubric_sample_rate: 0.2  # Score 20% of picks with LLM rubric each month
```

2. Run eval as normal. Sampled LLM sub-scores (reasoning quality, citation density, argument structure) are averaged and written to `eval_trend/{MONTH_ID}` alongside the mathematical metrics.

3. Query via CLI:
```bash
python -m screener.lib.exporter eval-trend --months 12
```

If rubric sub-scores are improving month-over-month, the system is reasoning more rigorously.

---

## Prerequisites

- Python 3.11+
- A GCP project with billing enabled (for Firestore + Cloud Run) — or AWS/OpenSearch if you prefer
- At least one LLM API key (Anthropic, OpenAI, Gemini, or a local Ollama install)
- A [Resend](https://resend.com) account for email (free tier covers this)

---

## Quick Start

```bash
git clone https://github.com/thisisnish/multi_agent_stock_screener_opensource
cd multi_agent_stock_screener_opensource
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` with your API keys (see [Secrets](#secrets) below), then edit `config/config.yaml` with your settings.

Run a local dry-run (no storage writes, no email):

```bash
DRY_RUN=true python -m jobs.screener.main
```

---

## Configuration

All tunables live in `config/config.yaml`. You never need to touch application code.

### LLM Model

Set `llm.model` to any [LangChain-supported model string](https://python.langchain.com/docs/integrations/chat/):

```yaml
llm:
  model: "anthropic:claude-haiku-4-5-20251001"   # Anthropic (default)
  # model: "openai:gpt-4o-mini"                  # OpenAI
  # model: "google_genai:gemini-2.0-flash"        # Gemini
  # model: "ollama:mistral"                        # Local Ollama (no API key needed)
  # model: "groq:llama3-8b-8192"                  # Groq
```

You can also set a different model per agent:

```yaml
llm:
  model: "anthropic:claude-haiku-4-5-20251001"   # default for all agents
  judge_model: "openai:gpt-4o"                   # use a stronger model just for Judge
  narrator_model: "google_genai:gemini-2.0-flash" # email narrative
  embedder_model: "openai:text-embedding-3-large"  # EDGAR indexing (3072-dim)
```

### Stock Universe

The screener runs on whatever tickers are in `config/tickers.yaml`. Each entry is a flat record with a `symbol` and a `sector` (GICS sector string):

```yaml
tickers:
  - symbol: AAPL
    sector: Technology
  - symbol: MSFT
    sector: Technology
  - symbol: JPM
    sector: Financials
  - symbol: META
    sector: Communication Services
```

The `sector` field drives the per-sector concentration cap (default: max 3 picks from any one sector in the top 10). To update the universe, edit this file and redeploy.

### Signal Weights

Adjust how the four signals contribute to the composite score:

```yaml
signals:
  weights:
    technical: 0.20   # RSI, MA50/200, volume, momentum
    earnings:  0.30   # Earnings yield (E/P)
    fcf:       0.30   # Free cash flow yield
    ebitda:    0.20   # EBITDA/Enterprise Value
```

Weights must sum to 1.0.

### Screener Settings

```yaml
screener:
  top_n: 10                  # Number of picks to pass to debate
  max_picks_per_sector: 3    # Sector concentration cap
```

### Notifications

```yaml
notifications:
  email:
    enabled: true
    from_address: "${EMAIL_FROM_ADDRESS:-}"   # Must be a verified Resend sender domain
    recipients:
      - "${EMAIL_TO_ADDRESS:-}"
    subject_prefix: "[Stock Screener]"
```

Set `EMAIL_FROM_ADDRESS` and `EMAIL_TO_ADDRESS` in `.env`. Set `enabled: false` to suppress all email (e.g. during local testing).

### Storage

```yaml
storage:
  provider: firestore        # firestore | s3 | opensearch

  firestore:
    project_id: "${GCP_PROJECT_ID}"
    database: "multi-agent-stock-screener"

  s3:
    bucket: "${S3_BUCKET_NAME:-}"
    region: "us-east-1"

  opensearch:
    host: "${OPENSEARCH_HOST:-}"
    port: 9200
    index: "stock-screener-chunks"
```

### EDGAR (SEC Filings)

```yaml
edgar:
  freshness_days: 30         # Re-index a ticker's filings if older than this
  chunk_size: 512            # Tokens per chunk
  chunk_overlap: 0.10        # 10% overlap between consecutive chunks
  similarity_threshold: 0.7  # Minimum cosine similarity for retrieval
  top_k: 5                   # Max chunks injected per debate
  # One or more query templates used to retrieve disclosure chunks. Each is
  # embedded and searched independently; results are merged, deduplicated by
  # chunk identity, re-ranked by similarity, and capped at top_k. {ticker} is
  # substituted at runtime.
  retrieval_query_templates:
    - "SEC filing risk factors financial performance {ticker}"
    # - "revenue growth capital allocation outlook {ticker}"
    # - "management guidance forward looking statements {ticker}"

  # Optional: boost retrieval score (+0.05) for chunks from these sections.
  # Leave empty to disable (default). Valid names: "Risk Factors", "MD&A",
  # "Financial Statements", "Business", "Legal Proceedings",
  # "Quantitative and Qualitative Disclosures", "Controls and Procedures".
  retrieval_sections: []

  # Token budget for the disclosure block injected into Bull/Bear prompts.
  # Lowest-scoring chunks are dropped when the cumulative count would exceed
  # this value. Set to 0 to disable the cap entirely.
  max_disclosure_tokens: 2048
```

---

## Secrets

Copy `.env.example` to `.env` and fill in the values you need:

```bash
# LLM — set the key for whichever provider you use
ANTHROPIC_API_KEY=sk-ant-...
GOOGLE_API_KEY=...          # Gemini models
OPENAI_API_KEY=sk-...       # OpenAI models
# Ollama needs no key

# Email
RESEND_API_KEY=re_...
NOTIFY_FROM=reports@yourdomain.com

# GCP (only if using Firestore or deploying to Cloud Run)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Secrets are never baked into the container image. On GCP, inject them via Secret Manager (see [GCP Deploy](#deploying-to-gcp)).

---

## Running Locally

**Single dry run (no storage writes, no email):**
```bash
DRY_RUN=true python -m jobs.screener.main
```

**Full local run (writes to storage, sends email):**
```bash
python -m jobs.screener.main
```

**Monthly financial data refresh only:**
```bash
DRY_RUN=true python -m jobs.financial_update.main
```

**EDGAR indexing only:**
```bash
DRY_RUN=true python -m jobs.edgar_disclosure.main
```

**Tests:**
```bash
pytest tests/ -v
```

---

## Deploying to GCP

### One-time setup

```bash
# Create all GCP resources (run once)
bash deploy/setup_gcp.sh
```

This creates:
- A Firestore database named `multi-agent-stock-screener`
- Three Cloud Run Jobs (screener, financial_update, edgar_disclosure)
- One Cloud Function (eval)
- A Cloud Workflows pipeline
- A Cloud Scheduler trigger (1st Friday of each month, 9AM ET)
- Secrets in Secret Manager

### Deploy

```bash
bash deploy/deploy_all.sh
```

Builds Docker images for each job, pushes to Artifact Registry, and redeploys all Cloud Run Jobs and the eval GCF.

### Manual trigger

To run the full pipeline manually from the console:

1. Go to Cloud Workflows → `stock-screener-monthly-pipeline` → Execute
2. Or trigger individual jobs from Cloud Run → Jobs → Run Job

---

## Updating Tickers

The screener does not auto-fetch the S&P 500 constituent list. To update:

1. Edit `config/tickers.yaml` — add/remove tickers under the appropriate GICS sector heading
2. Run `bash deploy/deploy_all.sh` to redeploy with the updated config


---

## Switching Storage Backends

**From Firestore to S3:**

1. Update `config/config.yaml`:
```yaml
storage:
  provider: s3
  s3_bucket: your-bucket
  s3_prefix: multi-agent-stock-screener/
```

2. Add `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to `.env`
3. Redeploy

Note: S3 uses brute-force cosine similarity for EDGAR vector search (loads all chunks into memory). This is fine at S&P 500 scale (~10k chunks). For larger universes, use OpenSearch.

---

## Architecture Overview

```
monthly_pipeline (Cloud Workflows)
  │
  ├─ financial_update_job     refreshes FCF + EBITDA for all tickers
  ├─ edgar_disclosure_job     indexes 10-K/10-Q into vector store
  ├─ screener_job             scores + debates + emails + writes picks
  └─ eval GCF                 scores prior month's picks, feeds back into Judge
```

Each job is a standalone Docker container with its own `Dockerfile` in `docker/`. They share the `screener/` library package but install only the dependencies they need.

For the full technical spec — state machine, data schema, scoring formulas, invariants — see [AGENT.md](./AGENT.md).

---

## Cost Estimates (S&P 500, monthly)

| Component | Estimated cost |
|-----------|---------------|
| LLM debate (10 tickers × 3 agents) | ~$0.05–$0.50 depending on model |
| EDGAR embedding (500 tickers × ~20 chunks) | ~$0.02 (Gemini embedding) |
| Cloud Run Jobs (GCP, per run) | ~$0.10–$0.30 |
| Firestore (reads/writes/storage) | Free tier covers typical usage |
| Email (Resend) | Free tier (100 emails/day) |

Total: **under $1/month** at S&P 500 scale with Claude Haiku.

---

## License

MIT
