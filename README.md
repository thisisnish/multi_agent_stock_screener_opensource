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

4. **SEC filings** — 10-K and 10-Q filings are indexed monthly and injected into the debate as context. Bull and Bear must cite which sources they used.

5. **Eval** — Once a month, all closed picks are scored for decision quality (0–100). The score feeds back into the next month's Judge prompt so the system gets better over time.

6. **Report** — Results are emailed to your configured recipients.

> For a technical deep-dive into the agent architecture, data model, and invariants — see [AGENT.md](./AGENT.md).

---

## Prerequisites

- Python 3.11+
- A GCP project with billing enabled (for Firestore + Cloud Run) — or AWS/OpenSearch if you prefer
- At least one LLM API key (Anthropic, OpenAI, Gemini, or a local Ollama install)
- A [Resend](https://resend.com) account for email (free tier covers this)

---

## Quick Start

```bash
git clone https://github.com/your-org/multi-agent-stock-screener
cd multi-agent-stock-screener
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/.env.example .env
```

Edit `.env` with your API keys (see [Secrets](#secrets) below), then edit `config/config.yaml` with your settings.

Run a local dry-run:

```bash
python jobs/screener/main.py --dry-run
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
  embedder_model: "google_genai:models/gemini-embedding-001"  # EDGAR indexing
```

### Stock Universe

The screener runs on whatever tickers are in `config/tickers.yaml`. The default is the S&P 500, organised by GICS sector:

```yaml
Communication Services:
  - META
  - GOOGL
  - NFLX

Technology:
  - AAPL
  - MSFT
  - NVDA
```

To update the universe, edit this file and redeploy. The sector groupings enforce the max-3-per-sector cap in the top 10.

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
  top_n: 10             # Number of picks per run
  max_sector_picks: 3   # Sector concentration cap
```

### Notifications

```yaml
notifications:
  email:
    recipients:
      - you@example.com
    from: reports@yourdomain.com   # Must be a verified Resend sender
```

### Storage

```yaml
storage:
  provider: firestore        # firestore | s3 | opensearch
  database: multi-agent-stock-screener

  # Firestore (GCP):
  gcp_project: your-project-id
  gcp_region: us-west1

  # S3 (AWS):
  s3_bucket: your-bucket-name
  s3_prefix: multi-agent-stock-screener/

  # OpenSearch (self-hosted):
  opensearch_host: https://localhost:9200
```

### EDGAR (SEC Filings)

```yaml
edgar:
  enabled: true
  forms: ["10-K", "10-Q"]
  lookback_years: 2
  freshness_days: 30    # Re-index a ticker if its filings are older than this
```

Set `edgar.enabled: false` to skip SEC filing context entirely.

---

## Secrets

Copy `config/.env.example` to `.env` and fill in the values you need:

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
python jobs/screener/main.py --dry-run
```

**Full local run (writes to storage, sends email):**
```bash
python jobs/screener/main.py
```

**Monthly financial data refresh:**
```bash
python jobs/financial_update/main.py
```

**EDGAR indexing:**
```bash
python jobs/edgar_disclosure/main.py
```

**Tests:**
```bash
pytest tests/ -v -n auto
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

To find the current S&P 500 constituents:
```bash
# Quick fetch via Wikipedia (verify against official sources before trading)
python -c "import pandas as pd; print(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]['Symbol'].tolist())"
```

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
