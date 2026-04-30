"""
screener/lib/models.py — Pydantic output schemas for Bull, Bear, Judge, and News agents.

These are the structured output contracts between LLM calls and the rest of the
debate pipeline. All models use sensible defaults so partial LLM output does not
crash the graph — the confidence_node and conviction_node compute their own
white-box scores rather than trusting the LLM's self-reported confidence.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class BullCaseOutput(BaseModel):
    """Structured output from the Bull agent.

    Attributes:
        bull_arguments: Core bullish arguments (fundamental, technical, or macro).
        key_catalysts: Specific upcoming events or triggers that support the bull case.
        bull_confidence: LLM self-reported confidence in 0.0–1.0 range (unused;
            white-box conviction scoring replaces this).
        bull_counter_argument: Strongest counter-argument the Bull concedes.
        signal_citations: Which signal categories the Bull draws on.
            Values from: ["Technical", "Earnings", "FCF", "EBITDA", "Sentiment", "Disclosures"].
    """

    bull_arguments: list[str] = Field(default_factory=list)
    key_catalysts: list[str] = Field(default_factory=list)
    bull_confidence: float = Field(default=0.5)
    bull_counter_argument: str = Field(default="")
    signal_citations: list[str] = Field(default_factory=list)


class BearCaseOutput(BaseModel):
    """Structured output from the Bear agent.

    Attributes:
        bear_arguments: Core bearish arguments (risk factors, valuation, macro headwinds).
        counter_arguments: Bull arguments the Bear explicitly rejects with reasoning.
        bear_confidence: LLM self-reported confidence in 0.0–1.0 range (unused).
        bear_counter_argument: Strongest counter-argument the Bear concedes.
        signal_citations: Which signal categories the Bear draws on.
    """

    bear_arguments: list[str] = Field(default_factory=list)
    counter_arguments: list[str] = Field(default_factory=list)
    bear_confidence: float = Field(default=0.5)
    bear_counter_argument: str = Field(default="")
    signal_citations: list[str] = Field(default_factory=list)


class JudgeOutput(BaseModel):
    """Structured output from the Judge agent, enriched by downstream nodes.

    The Judge LLM populates action through decisive_factor. The confidence_node
    then sets confidence_score and contested_truth. The conviction_node sets
    bull_conviction_score and bear_conviction_score. Citation lists are copied
    from Bull/Bear outputs by judge_node.

    Attributes:
        action: Final verdict — BUY, SELL, or HOLD.
        judge_self_confidence: LLM's self-reported 0–100 confidence (unused;
            white-box confidence_scorer replaces it).
        horizon: Expected holding horizon ("30d", "60d", or "90d").
        winning_side: Which side prevailed in the debate.
        margin_of_victory: How decisively — DECISIVE, NARROW, or CONTESTED.
        decisive_factor: The single factor that tipped the Judge's decision.
        rationale: Full reasoning paragraph.
        confidence_score: White-box computed score set by confidence_node (0–100).
        contested_truth: Set by hard_rules — True when conviction gap > 30pts
            and margin is NARROW or CONTESTED.
        bull_conviction_score: White-box score set by conviction_node.
        bear_conviction_score: White-box score set by conviction_node.
        bull_signal_citations: Copied from BullCaseOutput.signal_citations.
        bear_signal_citations: Copied from BearCaseOutput.signal_citations.
        judge_signal_citations: Citations the Judge itself references in rationale.
    """

    action: Literal["BUY", "SELL", "HOLD"]
    judge_self_confidence: int  # 0-100, self-reported (unused — white-box replaces it)
    horizon: str  # "30d" | "60d" | "90d"
    winning_side: Literal["BULL", "BEAR", "NEUTRAL"]
    margin_of_victory: Literal["DECISIVE", "NARROW", "CONTESTED"]
    decisive_factor: str
    rationale: str = Field(default="")
    confidence_score: Optional[float] = None  # set by confidence_node
    contested_truth: Optional[bool] = None  # set by hard_rules
    bull_conviction_score: Optional[float] = None  # set by conviction_node
    bear_conviction_score: Optional[float] = None
    bull_signal_citations: list[str] = Field(default_factory=list)
    bear_signal_citations: list[str] = Field(default_factory=list)
    judge_signal_citations: list[str] = Field(default_factory=list)


class NewsSentimentOutput(BaseModel):
    """Structured output from the News sentiment agent.

    Attributes:
        sentiment: Overall market sentiment for the ticker.
        confidence: How confident the LLM is in its assessment (0.0–1.0).
        rationale: Brief explanation, max 300 characters.
        override_flag: True when the news strongly contradicts the technical/fundamental
            signals and the agent recommends overriding the primary signal direction.
        override_reason: Populated only when override_flag is True; empty string otherwise.
    """

    sentiment: Literal["BULLISH", "BEARISH", "NEUTRAL"]
    confidence: float  # 0.0-1.0
    rationale: str  # max 300 chars
    override_flag: bool
    override_reason: str  # populated if override_flag=True, else ""
