"""
screener/agents/prompts.py — System prompts and context builders for the debate agents.

System prompts define the persona and instructions for each LLM agent.
Context builders assemble the human-turn message that each agent receives,
injecting signal data, EDGAR disclosures, episodic memory, and eval feedback.

Public API
----------
BULL_SYSTEM_PROMPT  — str
BEAR_SYSTEM_PROMPT  — str
JUDGE_SYSTEM_PROMPT — str
NEWS_SYSTEM_PROMPT  — str

build_ticker_context(ticker, ticker_name, signals, news, disclosure_block) -> str
build_judge_context(ticker, ticker_name, bull_output, bear_output, ...) -> str
build_disclosure_block(chunks) -> str | None
"""

from __future__ import annotations

import json

# Minimum months of history before adaptive weighting is applied in the Judge prompt.
SCORING_MIN_SAMPLE: int = 4

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

BULL_SYSTEM_PROMPT = """\
You are the Bull Analyst in a structured investment debate. Your role is to make \
the strongest possible bullish case for the stock you are given, using the \
quantitative signals and any SEC filing disclosures provided.

Guidelines:
- Provide at least 3 concrete bull_arguments grounded in the data.
- Identify 2-4 key_catalysts that could unlock upside within the investment horizon.
- Be intellectually honest: acknowledge the strongest bear counter-argument in \
bull_counter_argument.
- Cite which signal categories you draw on in signal_citations. \
Use only: Technical, Earnings, FCF, EBITDA, Sentiment, Disclosures.
- Set bull_confidence between 0.0 and 1.0 based on signal strength.

You are making the best possible bull case — not a balanced view. \
The Bear Analyst will challenge you, and a Judge will decide.
"""

BEAR_SYSTEM_PROMPT = """\
You are the Bear Analyst in a structured investment debate. Your role is to make \
the strongest possible bearish case for the stock you are given, using the \
quantitative signals and any SEC filing disclosures provided.

Guidelines:
- Provide at least 3 concrete bear_arguments grounded in the data.
- Populate counter_arguments with at least 2 bull arguments you explicitly reject, \
explaining why each is weaker than it appears.
- Be intellectually honest: acknowledge the strongest bull counter-argument in \
bear_counter_argument.
- Cite which signal categories you draw on in signal_citations. \
Use only: Technical, Earnings, FCF, EBITDA, Sentiment, Disclosures.
- Set bear_confidence between 0.0 and 1.0 based on signal strength.

You are making the best possible bear case — not a balanced view. \
The Bull Analyst will argue the other side, and a Judge will decide.
"""

JUDGE_SYSTEM_PROMPT = """\
You are the Judge in a structured investment debate. You have heard the Bull case \
and the Bear case for a stock. Your role is to adjudicate and issue a final verdict.

Guidelines:
- Weigh the Bull and Bear arguments against each other and the underlying signals.
- Declare a winning_side: BULL, BEAR, or NEUTRAL.
- Set margin_of_victory: DECISIVE (clear winner), NARROW (edge to one side), \
or CONTESTED (genuine toss-up).
- Name the single decisive_factor that most influenced your decision.
- Choose an action: BUY (bull wins), SELL (bear wins), HOLD (contested or neutral).
- Set horizon based on your conviction: "30d" (low), "60d" (medium), "90d" (high).
- Write a concise rationale (2-4 sentences) citing the signals and arguments that \
mattered most.
- List any signal categories you reference in judge_signal_citations.
- Set judge_self_confidence 0-100 based on your internal certainty.

Prioritise signals with high information content. Penalise arguments that rely \
solely on momentum without fundamental support.

If episodic memory is provided showing this ticker's historical verdict accuracy, \
incorporate that context — but do not let history override strong current signals.
"""

NEWS_SYSTEM_PROMPT = """\
You are a news sentiment analyst. You will be given recent news headlines and \
summaries for a publicly traded company. Your task is to assess the overall \
market sentiment and whether the news materially changes the investment outlook.

Guidelines:
- Assess sentiment as BULLISH, BEARISH, or NEUTRAL based on the news content.
- Set confidence between 0.0 and 1.0.
- Write a rationale of at most 300 characters summarising the key news driver.
- Set override_flag to True only if the news is strongly material and contradicts \
the prevailing technical/fundamental signal direction.
- Populate override_reason only when override_flag is True; leave it empty otherwise.

Focus on news that is company-specific and material. Ignore routine market noise.
"""

# ---------------------------------------------------------------------------
# Context builders
# ---------------------------------------------------------------------------


def build_ticker_context(
    ticker: str,
    ticker_name: str,
    signals: dict,
    news,
    disclosure_block: str | None = None,
) -> str:
    """Build the human-turn context string for Bull and Bear agents.

    Args:
        ticker: Upper-case ticker symbol, e.g. "AAPL".
        ticker_name: Company name, e.g. "Apple Inc.".
        signals: Dict of composite signal data from the scoring engine.
            Expected keys: technical, earnings, fcf, ebitda, composite_score,
            sector, price, above_ma200.
        news: NewsSentimentOutput instance or None. Injected as NEUTRAL if absent.
        disclosure_block: Pre-formatted EDGAR disclosure text or None.

    Returns:
        Formatted context string for the Bull/Bear human turn.
    """
    lines: list[str] = [
        f"## Ticker: {ticker} — {ticker_name}",
        "",
        "### Quantitative Signals",
        f"- Composite Score: {signals.get('composite_score', 'N/A')}",
        f"- Technical: {signals.get('technical', 'N/A')}",
        f"- Earnings Yield: {signals.get('earnings', 'N/A')}",
        f"- FCF Yield: {signals.get('fcf', 'N/A')}",
        f"- EBITDA/EV: {signals.get('ebitda', 'N/A')}",
        f"- Sector: {signals.get('sector', 'N/A')}",
        f"- Current Price: {signals.get('price', 'N/A')}",
        f"- Above MA200: {signals.get('above_ma200', 'N/A')}",
        "",
    ]

    # News sentiment block
    if news is not None:
        lines += [
            "### Recent News Sentiment",
            f"- Sentiment: {news.sentiment}",
            f"- Confidence: {news.confidence:.2f}",
            f"- Summary: {news.rationale}",
        ]
        if news.override_flag:
            lines.append(f"- Override Signal: {news.override_reason}")
    else:
        lines += [
            "### Recent News Sentiment",
            "- Sentiment: NEUTRAL (no news data available)",
        ]
    lines.append("")

    # EDGAR disclosures block
    if disclosure_block:
        lines += [
            "### SEC Filing Disclosures (EDGAR RAG)",
            disclosure_block,
            "",
        ]

    lines.append("Make the strongest possible case for your side using the data above.")

    return "\n".join(lines)


def build_judge_context(
    ticker: str,
    ticker_name: str,
    bull_output,
    bear_output,
    *,
    scoring_weights: dict | None = None,
    eval_context: dict | None = None,
    bull_conviction: float | None = None,
    bear_conviction: float | None = None,
    prior_months: dict | None = None,
) -> str:
    """Build the human-turn context string for the Judge agent.

    Args:
        ticker: Upper-case ticker symbol.
        ticker_name: Company name.
        bull_output: BullCaseOutput from the Bull agent.
        bear_output: BearCaseOutput from the Bear agent.
        scoring_weights: Optional adaptive weights dict with keys
            ``bull_weight``, ``bear_weight``, ``sample_size``.
        eval_context: Optional eval feedback dict from the prior month's
            eval pipeline (P1-08).
        bull_conviction: White-box conviction score 0–100 for the Bull case.
        bear_conviction: White-box conviction score 0–100 for the Bear case.
        prior_months: Dict of {month_id: verdict_dict} from episodic memory.
            Only injected when there are >= SCORING_MIN_SAMPLE entries.

    Returns:
        Formatted context string for the Judge human turn.
    """
    lines: list[str] = [
        f"## Judge Context: {ticker} — {ticker_name}",
        "",
        "### Bull Case",
        f"**Arguments:** {json.dumps(bull_output.bull_arguments, ensure_ascii=False)}",
        f"**Key Catalysts:** {json.dumps(bull_output.key_catalysts, ensure_ascii=False)}",
        f"**Concedes:** {bull_output.bull_counter_argument}",
        f"**Signal Citations:** {', '.join(bull_output.signal_citations) or 'none'}",
        "",
        "### Bear Case",
        f"**Arguments:** {json.dumps(bear_output.bear_arguments, ensure_ascii=False)}",
        f"**Counter-Arguments:** {json.dumps(bear_output.counter_arguments, ensure_ascii=False)}",
        f"**Concedes:** {bear_output.bear_counter_argument}",
        f"**Signal Citations:** {', '.join(bear_output.signal_citations) or 'none'}",
        "",
    ]

    # Conviction scores block
    if bull_conviction is not None and bear_conviction is not None:
        lines += [
            "### White-Box Conviction Scores",
            f"- Bull Conviction: {bull_conviction:.1f}/100",
            f"- Bear Conviction: {bear_conviction:.1f}/100",
            f"- Gap: {abs(bull_conviction - bear_conviction):.1f} points",
            "",
        ]

    # Adaptive weighting block — only shown when there's enough history
    if prior_months and len(prior_months) >= SCORING_MIN_SAMPLE and scoring_weights:
        bull_w = scoring_weights.get("bull_weight", 0.5)
        bear_w = scoring_weights.get("bear_weight", 0.5)
        sample_n = scoring_weights.get("sample_size", 0)
        lines += [
            "### Historical Performance (Episodic Memory)",
            f"Based on {sample_n} prior verdicts for {ticker}:",
            f"- Bull accuracy weight: {bull_w:.0%}",
            f"- Bear accuracy weight: {bear_w:.0%}",
            "",
            "Recent verdicts:",
        ]
        for month_id, verdict in sorted(prior_months.items(), reverse=True)[:6]:
            action = verdict.get("action", "?")
            confidence = verdict.get("confidence", 0.0)
            horizon = verdict.get("horizon", "?")
            lines.append(
                f"  - {month_id}: {action} (confidence {confidence:.0%}, horizon {horizon})"
            )
        lines.append("")

    # Eval context block — prior month feedback from eval pipeline
    if eval_context:
        lines += [
            "### Prior Month Eval Feedback",
        ]
        for key, val in eval_context.items():
            lines.append(f"- {key}: {val}")
        lines.append("")

    lines.append(
        "Weigh the arguments above and deliver your verdict. "
        "Be decisive where the evidence warrants it."
    )

    return "\n".join(lines)


def build_disclosure_block(chunks: list[dict] | None) -> str | None:
    """Format EDGAR chunk dicts into a concise disclosure block string.

    Args:
        chunks: List of chunk dicts from StorageDAO.vector_search(). Each dict
            should have at least a ``text`` key. An empty list or None returns None.

    Returns:
        Formatted multi-chunk disclosure string, or None if chunks is empty.
    """
    if not chunks:
        return None

    parts: list[str] = []
    for i, chunk in enumerate(chunks, start=1):
        text = chunk.get("text", "").strip()
        filing_type = chunk.get("filing_type", "SEC Filing")
        filing_date = chunk.get("filing_date", "")
        label = f"{filing_type} ({filing_date})" if filing_date else filing_type
        if text:
            parts.append(f"[{i}] {label}:\n{text}")

    if not parts:
        return None

    return "\n\n".join(parts)
