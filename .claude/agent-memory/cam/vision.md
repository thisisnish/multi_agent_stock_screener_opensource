---
name: Project vision & goals
description: Self-hosted Bull/Bear/Judge debate screener; MIT license; any-LLM + any-storage design; targets retail investors and small hedge funds
type: project
---

## What It Is

A self-hosted, open-source stock screener that applies multi-agent debate to S&P 500 stocks. Instead of a single algorithm producing a score, the system runs three LLM agents (Bull, Bear, Judge) through a structured debate graph. The Judge synthesizes their arguments, declares a verdict (BUY/SELL/HOLD), and assigns a confidence score.

**Key principles:**
- **Any LLM, any storage:** Pluggable providers (Anthropic, OpenAI, Gemini, Groq, Ollama) + storage backends (Firestore, S3, OpenSearch)
- **Transparent reasoning:** Every pick includes reasoning from all three agents + SEC filing context
- **Accountability through feedback loops:** Monthly eval of prior picks feeds into next month's Judge prompt
- **Self-hosted:** No SaaS lock-in. Deploy to your own GCP account via Cloud Run + Workflows

## Who It's For

1. **Retail investors** seeking a repeatable, opinionated screening framework backed by argument + evidence
2. **Small hedge funds** wanting a self-hosted alternative to commercial screeners
3. **Open source community:** Researchers, ML practitioners who want to experiment with LLM-driven market analysis

## The Problem It Solves

Traditional stock screeners produce scores (e.g., 65/100 = "moderate buy") without exposing the reasoning. Users can't challenge the logic or understand failure modes. This screener makes the reasoning explicit: you see what Bull said, what Bear said, how Judge adjudicated, and how accurate those verdicts have been historically.

## Core Workflow

Each month:
1. **Score** N number of stocks (config/ticker.yaml) stocks using Technical, Earnings, FCF, EBITDA signals
2. **Debate** top M (config/tickers.yaml) candidates: Bull + Bear present cases, Judge declares a verdict
3. **Contextualize** with EDGAR 10-K/10-Q excerpts (RAG)
4. **Inject feedback** from last month's accuracy metrics
5. **Report** via email with performance curves and confidence calibration data

## Success Metrics

- **Operational:** Runs reliably monthly, cost <$1/month on GCP free tier
- **Verdict quality:** >70% of verdicts cite SEC filings (disclosure rate)
- **Calibration:** High-confidence picks outperform low-confidence ones
- **Alpha:** System picks beat buy-and-hold SPY over 12+ months

## License & Governance

MIT license. Targets Phase 5 (2027) with OSS governance: CONTRIBUTING.md, CLA, issue triage, release process. Dual-licensing possible for proprietary use.
