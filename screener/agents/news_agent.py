"""
screener/agents/news_agent.py — News sentiment agent using DuckDuckGo + newspaper3k.

Fetches recent news articles for a ticker, extracts article text, and passes the
content to the News LLM agent for structured sentiment analysis.

The news agent is optional in the debate pipeline — if news fetching fails or
returns no articles, the Bull/Bear agents receive a NEUTRAL placeholder.

Public API
----------
analyze_ticker_news(ticker, ticker_name, app_config, max_articles)
    -> tuple[NewsSentimentOutput, str, int]
"""

from __future__ import annotations

import logging

from langchain_core.messages import HumanMessage, SystemMessage

from screener.agents.prompts import NEWS_SYSTEM_PROMPT
from screener.lib.agent_creator import get_structured_llm
from screener.lib.models import NewsSentimentOutput

logger = logging.getLogger(__name__)

# Feed health labels
_FEED_HEALTHY = "healthy"
_FEED_PARTIAL = "partial"
_FEED_EMPTY = "empty"


def _fetch_news_urls(ticker: str, ticker_name: str, max_articles: int) -> list[str]:
    """Search DuckDuckGo News for recent articles about the ticker.

    Args:
        ticker: Upper-case ticker symbol.
        ticker_name: Company name for a richer search query.
        max_articles: Maximum number of article URLs to return.

    Returns:
        List of article URLs. Empty list if ddgs is not installed or search fails.
    """
    try:
        from duckduckgo_search import DDGS  # type: ignore[import]
    except ImportError:
        logger.warning(
            "duckduckgo_search not installed; news agent disabled. "
            "Install with: pip install duckduckgo-search"
        )
        return []

    query = f"{ticker_name} {ticker} stock news"
    urls: list[str] = []
    try:
        with DDGS() as ddgs:
            for result in ddgs.news(query, max_results=max_articles):
                url = result.get("url") or result.get("link", "")
                if url:
                    urls.append(url)
    except Exception:
        logger.exception("DuckDuckGo news search failed for ticker=%s", ticker)
    return urls


def _extract_article_text(url: str) -> str | None:
    """Download and extract the main text from a news article URL.

    Args:
        url: Article URL.

    Returns:
        Extracted article text, or None if extraction fails.
    """
    try:
        import newspaper  # type: ignore[import]

        article = newspaper.Article(url)
        article.download()
        article.parse()
        text = article.text.strip()
        return text if text else None
    except ImportError:
        logger.warning(
            "newspaper3k not installed; cannot extract article text. "
            "Install with: pip install newspaper3k"
        )
        return None
    except Exception:
        logger.debug("Article extraction failed for url=%s", url)
        return None


def _build_news_context(
    ticker: str,
    ticker_name: str,
    articles: list[str],
) -> str:
    """Format extracted article texts into a single context string.

    Args:
        ticker: Ticker symbol.
        ticker_name: Company name.
        articles: List of extracted article text strings.

    Returns:
        Formatted news context for the LLM.
    """
    lines = [
        f"## News Analysis: {ticker} — {ticker_name}",
        "",
        f"Recent news articles ({len(articles)} found):",
        "",
    ]
    for i, text in enumerate(articles, start=1):
        # Truncate to 800 chars per article to stay within context limits
        truncated = text[:800] + "..." if len(text) > 800 else text
        lines.append(f"### Article {i}")
        lines.append(truncated)
        lines.append("")

    lines.append(
        "Based on the articles above, assess the overall sentiment and "
        "whether any news is material enough to override the technical/fundamental signals."
    )
    return "\n".join(lines)


def analyze_ticker_news(
    ticker: str,
    ticker_name: str,
    app_config,
    max_articles: int = 5,
) -> tuple[NewsSentimentOutput, str, int]:
    """Fetch recent news and run the News sentiment LLM agent.

    Args:
        ticker: Upper-case ticker symbol.
        ticker_name: Company name.
        app_config: Validated AppConfig — used to get the news model.
        max_articles: Maximum number of articles to fetch and analyse.

    Returns:
        A tuple of:
          - NewsSentimentOutput: Structured sentiment result. NEUTRAL with
            confidence 0.5 if no articles could be fetched.
          - feed_health: "healthy" (≥3 articles), "partial" (1-2), or "empty" (0).
          - articles_analyzed: Count of articles successfully extracted.
    """
    _neutral_fallback = NewsSentimentOutput(
        sentiment="NEUTRAL",
        confidence=0.5,
        rationale="No news data available.",
        override_flag=False,
        override_reason="",
    )

    # Step 1: Fetch URLs
    urls = _fetch_news_urls(ticker, ticker_name, max_articles)
    if not urls:
        return _neutral_fallback, _FEED_EMPTY, 0

    # Step 2: Extract article text
    articles: list[str] = []
    for url in urls:
        text = _extract_article_text(url)
        if text:
            articles.append(text)

    articles_analyzed = len(articles)

    if articles_analyzed == 0:
        return _neutral_fallback, _FEED_EMPTY, 0

    # Step 3: Determine feed health
    if articles_analyzed >= 3:
        feed_health = _FEED_HEALTHY
    else:
        feed_health = _FEED_PARTIAL

    # Step 4: Call News LLM agent
    try:
        context = _build_news_context(ticker, ticker_name, articles)
        news_llm = get_structured_llm("news", NewsSentimentOutput, app_config)
        messages = [
            SystemMessage(content=NEWS_SYSTEM_PROMPT),
            HumanMessage(content=context),
        ]
        result: NewsSentimentOutput = news_llm.invoke(messages)
        logger.debug(
            "News sentiment for %s: %s (confidence=%.2f, override=%s)",
            ticker,
            result.sentiment,
            result.confidence,
            result.override_flag,
        )
        return result, feed_health, articles_analyzed
    except Exception:
        logger.exception("News LLM agent failed for ticker=%s", ticker)
        return _neutral_fallback, feed_health, articles_analyzed
