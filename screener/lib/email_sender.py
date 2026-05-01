"""
screener/lib/email_sender.py — Email delivery for the screener via Resend API.

Sends the monthly report (top picks, verdicts, performance) to the recipient
list configured under ``notifications.email.recipients`` in config/config.yaml.

Public API
----------
build_picks_table_html(picks, date, quarterly_vintage) -> str
    Pure HTML builder for the scored-picks table block.

build_verdicts_table_html(verdicts) -> str
    Pure HTML builder for the judge-verdict block.

build_performance_html(performance_rows, spy_return_pct) -> str
    Pure HTML builder for the per-pick performance block.

build_email_html(picks, date, verdicts, performance_rows, spy_return_pct,
                 quarterly_vintage) -> str
    Assembles the full HTML email body from the above blocks.

send_email(cfg, picks, date, verdicts, performance_rows, spy_return_pct,
           quarterly_vintage) -> bool
    Posts the email via Resend API.  Returns True on success, False on
    graceful failure (missing config, API error, etc.).

Design notes
------------
- ``send_email`` reads RESEND_API_KEY from os.environ; the sender address and
  recipients come exclusively from the AppConfig object passed in (P1-09d).
- No narrative generation (P1-09c skipped).
- All builder functions are pure (no I/O) to keep them trivially testable.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from screener.lib.config_loader import AppConfig

logger = logging.getLogger(__name__)

# Resend v1 email endpoint.
_RESEND_API_URL = "https://api.resend.com/emails"

# ---------------------------------------------------------------------------
# Formatting helpers (private)
# ---------------------------------------------------------------------------


def _fmt(v: float | None) -> str:
    """Format a float to one decimal place, or em-dash when absent."""
    return f"{v:.1f}" if v is not None else "—"


def _fmt_pct(v: float | None, sign: bool = True) -> str:
    """Format a float as a percentage string with optional leading sign."""
    if v is None:
        return "—"
    prefix = "+" if sign and v > 0 else ""
    return f"{prefix}{v:.2f}%"


def _color(v: float | None) -> str:
    """Return green hex for non-negative values, red for negative, grey for None."""
    if v is None:
        return "#555"
    return "#27ae60" if v >= 0 else "#c0392b"


def _signed(v: float | None, suffix: str = "%") -> str:
    """Format a numeric value with sign and suffix, or em-dash when absent."""
    if v is None:
        return "—"
    prefix = "+" if v > 0 else ""
    return f"{prefix}{v:.2f}{suffix}"


# ---------------------------------------------------------------------------
# HTML block builders (public, pure functions)
# ---------------------------------------------------------------------------


def build_picks_table_html(
    picks: list[dict],
    date: str,
    quarterly_vintage: str | None = None,
) -> str:
    """Build the scored-picks table HTML block.

    Args:
        picks: List of pick dicts from the scoring engine.  Each entry is
            expected to have keys: rank, symbol, score, ma200_gate,
            composite_pre_gate, earnings_yield_score, fcf_yield_score,
            ebitda_ev_score, rsi, price, sector.
        date: Human-readable report date string (e.g. ``"2026-04-30"``).
        quarterly_vintage: Date string for the last quarterly signal refresh,
            or None if unavailable.

    Returns:
        HTML string for the picks table section.
    """
    rows = ""
    for entry in picks:
        gate = entry.get("ma200_gate", {})
        dampened = not gate.get("above_ma200", True)
        pre_gate = entry.get("composite_pre_gate")
        gate_cell = (
            '<td style="color:#c0392b;text-align:center" '
            'title="Score dampened — price below 200-day MA">⚠</td>'
            if dampened
            else '<td style="color:#27ae60;text-align:center">✓</td>'
        )
        rows += (
            f"<tr>"
            f"<td>{entry['rank']}</td>"
            f"<td><strong>{entry['symbol']}</strong></td>"
            f"<td><strong>{entry['score']:.1f}</strong></td>"
            f"{gate_cell}"
            f"<td>{_fmt(pre_gate)}</td>"
            f"<td>{_fmt(entry.get('earnings_yield_score'))}</td>"
            f"<td>{_fmt(entry.get('fcf_yield_score'))}</td>"
            f"<td>{_fmt(entry.get('ebitda_ev_score'))}</td>"
            f"<td>{entry['rsi']:.1f}</td>"
            f"<td>${entry['price']:.2f}</td>"
            f"<td>{entry['sector']}</td>"
            f"</tr>"
        )

    vintage_note = (
        f"Quarterly fundamental signals (FCF yield, EBITDA/EV) last refreshed: "
        f"<strong>{quarterly_vintage}</strong>."
        if quarterly_vintage
        else "Quarterly fundamental signals (FCF yield, EBITDA/EV): vintage unavailable."
    )

    return f"""<h2>Monthly S&amp;P 500 Top Picks — {date}</h2>
<p>Composite score: Technical 20% · Earnings Yield 30% · FCF Yield 30% · EBITDA/EV 20%,
all sector Z-score normalised (0–100). MA200 gate applies a 0.5× dampener when price
is below the 200-day moving average (⚠). Max 3 tickers per GICS sector.</p>
<table border="1" cellpadding="6" cellspacing="0"
       style="border-collapse:collapse;width:100%;font-size:13px">
  <thead style="background:#f0f0f0">
    <tr>
      <th>Rank</th><th>Symbol</th><th>Score</th><th>MA200</th><th>Pre-gate</th>
      <th>EY*</th><th>FCF†</th><th>EBITDA/EV†</th>
      <th>RSI</th><th>Price</th><th>Sector</th>
    </tr>
  </thead>
  <tbody>{rows}</tbody>
</table>
<p style="color:#888;font-size:12px;margin-top:20px">
  * EY: earnings yield (trailing E/P) sector Z-score. Uses trailing EPS from yfinance —
  may lag actual results by up to one quarter. “—” = data unavailable; treated as 50 (neutral).<br>
  † FCF/EBITDA/EV: sector Z-scores sourced from the quarterly snapshot. {vintage_note}<br>
  ⚠ = score dampened by MA200 gate (price below 200-day MA). Pre-gate shows undampened composite.<br>
  All factor scores 0–100 (higher is better). Not financial advice.
</p>"""


def build_verdicts_table_html(verdicts: list[dict]) -> str:
    """Build the judge-verdict table HTML block.

    Args:
        verdicts: List of verdict dicts.  Each entry is expected to have keys:
            symbol, verdict (BUY/SELL/HOLD), margin, confidence,
            decisive_factor.  All keys are optional with graceful fallback.

    Returns:
        HTML string for the verdicts section, or empty string if no verdicts.
    """
    if not verdicts:
        return ""

    rows = ""
    for v in verdicts:
        verdict = v.get("verdict", "—")
        verdict_color = (
            "#27ae60"
            if verdict == "BUY"
            else "#c0392b"
            if verdict == "SELL"
            else "#555"
        )
        confidence = v.get("confidence")
        confidence_str = f"{confidence:.0f}" if confidence is not None else "—"
        rows += (
            f"<tr>"
            f"<td><strong>{v.get('symbol', '?')}</strong></td>"
            f'<td style="color:{verdict_color};font-weight:bold">{verdict}</td>'
            f"<td>{_fmt(v.get('margin'))}</td>"
            f"<td>{confidence_str}</td>"
            f'<td style="font-size:12px">{v.get("decisive_factor", "—")}</td>'
            f"</tr>"
        )

    return f"""<h3 style="margin-top:28px">Judge Verdicts</h3>
<p style="font-size:13px;color:#555">
  Bull/Bear/Judge debate results for screener top picks. Confidence 0–100; HOLD is
  forced when confidence is below 40.
</p>
<table border="1" cellpadding="6" cellspacing="0"
       style="border-collapse:collapse;font-size:13px;width:100%">
  <thead style="background:#f0f0f0">
    <tr>
      <th>Symbol</th><th>Verdict</th><th>Margin</th>
      <th>Confidence</th><th>Decisive Factor</th>
    </tr>
  </thead>
  <tbody>{rows}</tbody>
</table>
<p style="font-size:11px;color:#aaa;margin-top:4px">Not financial advice.</p>"""


def build_performance_html(
    performance_rows: list[dict],
    spy_return_pct: float | None = None,
) -> str:
    """Build the prior-month pick-performance table HTML block.

    Args:
        performance_rows: List of per-pick performance dicts.  Expected keys:
            rank, symbol, entry_price, current_price, return_pct, alpha_pct,
            sector.  All keys optional with graceful fallback.
        spy_return_pct: SPY benchmark return over the same period, as a
            percentage float (e.g. ``2.5`` for +2.5%).

    Returns:
        HTML string for the performance section, or empty string if no rows.
    """
    if not performance_rows:
        return ""

    def _arrow(alpha: float | None) -> str:
        if alpha is None:
            return ""
        return (
            '<span style="color:#27ae60">▲</span>'
            if alpha >= 0
            else '<span style="color:#c0392b">▼</span>'
        )

    rows = ""
    for r in performance_rows:
        alpha = r.get("alpha_pct")
        entry = r.get("entry_price")
        current = r.get("current_price")
        rows += (
            f"<tr>"
            f"<td>{r.get('rank', '—')}</td>"
            f"<td><strong>{r.get('symbol', '?')}</strong></td>"
            f"<td>{'${:.2f}'.format(entry) if entry is not None else '—'}</td>"
            f"<td>{'${:.2f}'.format(current) if current is not None else '—'}</td>"
            f"<td>{_fmt_pct(r.get('return_pct'))}</td>"
            f"<td>{_arrow(alpha)} {_fmt_pct(alpha)}</td>"
            f"<td>{r.get('sector', '—')}</td>"
            f"</tr>"
        )

    spy_row = (
        f"<tr style='background:#f5f5f5;font-style:italic'>"
        f"<td colspan='4'>SPY (benchmark)</td>"
        f"<td>{_fmt_pct(spy_return_pct)}</td>"
        f"<td>—</td><td>—</td>"
        f"</tr>"
    )

    return f"""<h3 style="margin-top:28px">Prior Month Pick Performance</h3>
<table border="1" cellpadding="6" cellspacing="0"
       style="border-collapse:collapse;width:100%;font-size:13px">
  <thead style="background:#f0f0f0">
    <tr>
      <th>Rank</th><th>Symbol</th><th>Entry</th><th>Current</th>
      <th>Return</th><th>Alpha vs SPY</th><th>Sector</th>
    </tr>
  </thead>
  <tbody>
    {rows}
    {spy_row}
  </tbody>
</table>"""


def build_email_html(
    picks: list[dict],
    date: str,
    verdicts: list[dict] | None = None,
    performance_rows: list[dict] | None = None,
    spy_return_pct: float | None = None,
    quarterly_vintage: str | None = None,
) -> str:
    """Assemble the full HTML email body.

    Args:
        picks: Scored picks list (required).
        date: Report date string.
        verdicts: Optional list of judge verdict dicts.
        performance_rows: Optional list of prior-month performance dicts.
        spy_return_pct: Optional SPY return for the performance period.
        quarterly_vintage: Optional quarterly signal refresh date string.

    Returns:
        Complete HTML string suitable for the Resend ``html`` field.
    """
    picks_block = build_picks_table_html(picks, date, quarterly_vintage)
    verdicts_block = build_verdicts_table_html(verdicts or [])
    performance_block = build_performance_html(performance_rows or [], spy_return_pct)

    return (
        '<html><body style="font-family:Arial,sans-serif;max-width:900px;margin:auto">\n'
        f"{picks_block}\n"
        f"{verdicts_block}\n"
        f"{performance_block}\n"
        "</body></html>"
    )


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


def send_email(
    cfg: "AppConfig",
    picks: list[dict],
    date: str,
    verdicts: list[dict] | None = None,
    performance_rows: list[dict] | None = None,
    spy_return_pct: float | None = None,
    quarterly_vintage: str | None = None,
) -> bool:
    """Send the monthly report email via Resend API.

    Reads ``RESEND_API_KEY`` from the process environment.
    Sender address and recipients come from ``cfg.notifications.email``
    (P1-09a, P1-09d).

    Args:
        cfg: Validated AppConfig instance.
        picks: Scored picks for the current month.
        date: Human-readable report date string.
        verdicts: Optional judge verdicts list.
        performance_rows: Optional prior-month performance rows.
        spy_return_pct: Optional SPY benchmark return percentage.
        quarterly_vintage: Optional quarterly signal vintage date string.

    Returns:
        True on successful delivery, False on graceful failure.
        Errors are logged but never re-raised (graceful degrade).
    """
    email_cfg = cfg.notifications.email

    if not email_cfg.enabled:
        logger.info("email skipped — notifications.email.enabled is false")
        return False

    api_key = os.environ.get("RESEND_API_KEY", "")
    from_addr = email_cfg.from_address
    recipients = [r for r in email_cfg.recipients if r]

    missing = [
        k
        for k, v in {
            "RESEND_API_KEY": api_key,
            "notifications.email.from_address": from_addr,
        }.items()
        if not v
    ]
    if missing:
        logger.error(
            "email skipped — missing config",
            extra={"missing": missing},
        )
        return False

    if not recipients:
        logger.error(
            "email skipped — notifications.email.recipients is empty",
        )
        return False

    html = build_email_html(
        picks,
        date,
        verdicts=verdicts,
        performance_rows=performance_rows,
        spy_return_pct=spy_return_pct,
        quarterly_vintage=quarterly_vintage,
    )

    subject_prefix = email_cfg.subject_prefix or "[Stock Screener]"
    subject = f"{subject_prefix} Monthly Report — {date}"

    try:
        resp = requests.post(
            _RESEND_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": from_addr,
                "to": recipients,
                "subject": subject,
                "html": html,
            },
            timeout=15,
        )
        resp.raise_for_status()
        logger.info(
            "email sent",
            extra={"to": recipients, "status": resp.status_code},
        )
        return True
    except Exception as exc:
        logger.error("email failed", extra={"error": str(exc)})
        return False
