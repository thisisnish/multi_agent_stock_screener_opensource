# Lead Engineer Context — Multi-Agent Stock Screener OSS

## What the system does
A monthly stock-screening pipeline that scores the user defined (config/tickers.yaml) on 4 fundamental/technical signals, selects the top N tickers, runs each through a multi-agent LangGraph debate (Bull vs Bear in parallel, then Judge with episodic memory + EDGAR RAG), writes picks to storage, sends an HTML email report, and feeds a monthly eval GCF that scores prior picks and injects accuracy feedback into the next Judge prompt.

## Key Architectural Decisions
- No Flask server — pipeline is a Cloud Run Job triggered by Cloud Workflows
- LangGraph linear state machine: memory_read → build_context → debate_node → conviction_node → judge_node → confidence_node → hard_rules → memory_write → END
- Bull + Bear run as `RunnableParallel` inside a single `debate_node` — NOT `Send()` (Send() is for dynamic fan-out)
- LLM provider routing via LangChain `init_chat_model("provider:model")` — no custom provider code
- Storage is abstracted behind `StorageClient` ABC; provider chosen from `config.storage.provider` (firestore | s3 | opensearch)
- Single Firestore DB (`multi-agent-stock-screener`) with subcollections for high-cardinality data
- All run-keyed IDs use `MONTH_ID` format: `2026-04`
- Tickers are a static YAML file — no auto-fetch GCF
- `write()` is idempotent-create (raises AlreadyExists); `upsert()` merges

## Tech Stack
- Python 3.11+
- LangChain >= 0.3, LangGraph >= 0.2
- LangChain provider packages: langchain-anthropic, langchain-google-genai, langchain-openai, langchain-ollama
- pydantic-settings >= 2.0 (config loading)
- yfinance (signal fetching)
- Resend (email)
- GCP: Cloud Run Jobs, Cloud Workflows, Cloud Scheduler, Cloud Functions, Secret Manager, Firestore, Artifact Registry
- AWS S3 (optional alternate storage backend)
- Docker (one image per job)

## Current Build Phase
Phase 1 — Core Engine. All P1-01 through P1-11 tickets are in Backlog/In Progress. The repo currently has only root-level files (AGENT.md, KANBAN.md, PLAN.md, README.md) — no source folders exist yet.

## Hard Constraints — Never Violate
1. Never use `Send()` for Bull/Bear — always `RunnableParallel`
2. Never call `init_chat_model` with a custom provider class — pass "provider:model" string only
3. Never write tickers logic that auto-fetches from the web — tickers.yaml is the sole source
4. Never add a second Firestore database — one DB named `multi-agent-stock-screener`
5. Never import from `screener/agents/llm/providers/` or `llm_client.py` — they are deleted in this repo
6. Never use `server.py` or Flask — the entry point is `jobs/screener/main.py`
7. `screener/main.py:run_screener()` must own the entire pipeline (score → analyze → email → picks)
8. `write()` on StorageClient must raise AlreadyExists; use `upsert()` for merges
9. No GCP Secret Manager client in application code — secrets come via env vars / .env only
10. `eval_metrics.py` lives in `screener/lib/`, not `screener/metrics/` — check PLAN.md table before moving files
