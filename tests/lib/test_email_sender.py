"""
tests/lib/test_email_sender.py — Unit tests for screener/lib/email_sender.py.

All HTML builder functions are pure (no I/O), so every test here is a
straightforward value test.  The send_email path is covered with a mocked
requests.post to avoid real HTTP calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from screener.lib.config_loader import (
    AppConfig,
    EmailConfig,
    NotificationsConfig,
    StorageConfig,
)
from screener.lib.email_sender import (
    _fmt,
    _fmt_pct,
    _month_label,
    build_email_html,
    build_performance_html,
    build_picks_table_html,
    build_verdicts_table_html,
    send_email,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_pick(
    rank: int = 1,
    symbol: str = "AAPL",
    score: float = 78.5,
    rsi: float = 55.0,
    price: float = 180.0,
    sector: str = "Technology",
    above_ma200: bool = True,
) -> dict:
    return {
        "rank": rank,
        "symbol": symbol,
        "score": score,
        "ma200_gate": {"above_ma200": above_ma200},
        "composite_pre_gate": score if above_ma200 else score * 2,
        "earnings_yield_score": 62.0,
        "fcf_yield_score": 71.5,
        "ebitda_ev_score": 58.0,
        "rsi": rsi,
        "price": price,
        "sector": sector,
    }


def _stub_verdict(
    symbol: str = "AAPL",
    verdict: str = "BUY",
    margin: float = 25.0,
    confidence: float = 72.0,
    decisive_factor: str = "Strong FCF yield",
) -> dict:
    return {
        "symbol": symbol,
        "verdict": verdict,
        "margin": margin,
        "confidence": confidence,
        "decisive_factor": decisive_factor,
    }


def _stub_perf_row(
    rank: int = 1,
    symbol: str = "AAPL",
    entry_price: float = 170.0,
    current_price: float = 180.0,
    return_pct: float = 5.88,
    alpha_pct: float = 3.2,
    sector: str = "Technology",
) -> dict:
    return {
        "rank": rank,
        "symbol": symbol,
        "entry_price": entry_price,
        "current_price": current_price,
        "return_pct": return_pct,
        "alpha_pct": alpha_pct,
        "sector": sector,
    }


def _stub_cfg(
    enabled: bool = True,
    from_address: str = "screener@example.com",
    recipients: list[str] | None = None,
) -> AppConfig:
    return AppConfig(
        storage=StorageConfig(
            provider="firestore",
            firestore={"project_id": "test-project"},
        ),
        notifications=NotificationsConfig(
            email=EmailConfig(
                enabled=enabled,
                from_address=from_address,
                recipients=recipients
                if recipients is not None
                else ["user@example.com"],
            )
        ),
    )


# ---------------------------------------------------------------------------
# _month_label helper
# ---------------------------------------------------------------------------


def test_month_label_formats_yyyy_mm():
    assert _month_label("2026-04") == "April 2026"


def test_month_label_january():
    assert _month_label("2026-01") == "January 2026"


def test_month_label_december():
    assert _month_label("2025-12") == "December 2025"


def test_month_label_fallback_on_plain_date():
    # A plain date string (not YYYY-MM) is returned unchanged.
    assert _month_label("2026-04-30") == "2026-04-30"


def test_month_label_fallback_on_arbitrary_string():
    assert _month_label("Q2-2026") == "Q2-2026"


def test_picks_table_heading_shows_month_name_for_month_id():
    html = build_picks_table_html([_stub_pick()], "2026-04")
    assert "April 2026" in html


def test_picks_table_heading_unchanged_for_plain_date():
    html = build_picks_table_html([_stub_pick()], "2026-04-30")
    assert "2026-04-30" in html


# ---------------------------------------------------------------------------
# _fmt / _fmt_pct helpers
# ---------------------------------------------------------------------------


def test_fmt_with_value():
    assert _fmt(72.456) == "72.5"


def test_fmt_with_none():
    assert _fmt(None) == "—"


def test_fmt_pct_positive_with_sign():
    assert _fmt_pct(3.14) == "+3.14%"


def test_fmt_pct_negative():
    assert _fmt_pct(-1.5) == "-1.50%"


def test_fmt_pct_zero():
    # Zero is not positive — no + prefix
    assert _fmt_pct(0.0) == "0.00%"


def test_fmt_pct_none():
    assert _fmt_pct(None) == "—"


def test_fmt_pct_no_sign():
    assert _fmt_pct(5.0, sign=False) == "5.00%"


# ---------------------------------------------------------------------------
# build_picks_table_html
# ---------------------------------------------------------------------------


def test_picks_table_contains_symbol():
    html = build_picks_table_html([_stub_pick()], "2026-04-30")
    assert "AAPL" in html


def test_picks_table_contains_date():
    html = build_picks_table_html([_stub_pick()], "2026-04-30")
    assert "2026-04-30" in html


def test_picks_table_ma200_above_shows_checkmark():
    html = build_picks_table_html([_stub_pick(above_ma200=True)], "2026-04-30")
    assert "✓" in html


def test_picks_table_ma200_below_shows_warning():
    html = build_picks_table_html([_stub_pick(above_ma200=False)], "2026-04-30")
    assert "⚠" in html


def test_picks_table_with_quarterly_vintage():
    html = build_picks_table_html(
        [_stub_pick()], "2026-04-30", quarterly_vintage="2026-03-31"
    )
    assert "2026-03-31" in html


def test_picks_table_without_quarterly_vintage():
    html = build_picks_table_html([_stub_pick()], "2026-04-30", quarterly_vintage=None)
    assert "vintage unavailable" in html


def test_picks_table_multiple_picks():
    picks = [_stub_pick(rank=i, symbol=f"T{i}") for i in range(1, 4)]
    html = build_picks_table_html(picks, "2026-04-30")
    assert "T1" in html
    assert "T2" in html
    assert "T3" in html


def test_picks_table_empty_list():
    html = build_picks_table_html([], "2026-04-30")
    # Table headers should still render; no rows
    assert "Rank" in html
    assert "<tr>" not in html.split("<tbody>")[1].split("</tbody>")[0]


# ---------------------------------------------------------------------------
# build_verdicts_table_html
# ---------------------------------------------------------------------------


def test_verdicts_table_buy_verdict():
    html = build_verdicts_table_html([_stub_verdict(verdict="BUY")])
    assert "BUY" in html
    assert "#27ae60" in html  # green


def test_verdicts_table_sell_verdict():
    html = build_verdicts_table_html([_stub_verdict(verdict="SELL")])
    assert "SELL" in html
    assert "#c0392b" in html  # red


def test_verdicts_table_hold_verdict():
    html = build_verdicts_table_html([_stub_verdict(verdict="HOLD")])
    assert "HOLD" in html


def test_verdicts_table_contains_symbol():
    html = build_verdicts_table_html([_stub_verdict(symbol="MSFT")])
    assert "MSFT" in html


def test_verdicts_table_contains_decisive_factor():
    html = build_verdicts_table_html(
        [_stub_verdict(decisive_factor="Strong FCF yield")]
    )
    assert "Strong FCF yield" in html


def test_verdicts_table_empty_returns_empty_string():
    assert build_verdicts_table_html([]) == ""


def test_verdicts_table_multiple_verdicts():
    verdicts = [
        _stub_verdict(symbol="AAPL", verdict="BUY"),
        _stub_verdict(symbol="GOOG", verdict="SELL"),
    ]
    html = build_verdicts_table_html(verdicts)
    assert "AAPL" in html
    assert "GOOG" in html


def test_verdicts_table_missing_keys_graceful():
    # Dicts with no expected keys should not raise
    html = build_verdicts_table_html([{}])
    assert "—" in html


# ---------------------------------------------------------------------------
# build_performance_html
# ---------------------------------------------------------------------------


def test_performance_html_contains_symbol():
    html = build_performance_html([_stub_perf_row()])
    assert "AAPL" in html


def test_performance_html_positive_alpha_up_arrow():
    html = build_performance_html([_stub_perf_row(alpha_pct=3.2)])
    assert "▲" in html


def test_performance_html_negative_alpha_down_arrow():
    html = build_performance_html([_stub_perf_row(alpha_pct=-2.1)])
    assert "▼" in html


def test_performance_html_spy_benchmark_row():
    html = build_performance_html([_stub_perf_row()], spy_return_pct=2.68)
    assert "SPY" in html
    assert "+2.68%" in html


def test_performance_html_spy_none():
    html = build_performance_html([_stub_perf_row()], spy_return_pct=None)
    assert "SPY" in html
    assert "—" in html


def test_performance_html_empty_returns_empty_string():
    assert build_performance_html([]) == ""


def test_performance_html_missing_keys_graceful():
    html = build_performance_html([{}])
    assert "—" in html


# ---------------------------------------------------------------------------
# build_email_html
# ---------------------------------------------------------------------------


def test_build_email_html_contains_all_sections():
    html = build_email_html(
        picks=[_stub_pick()],
        date="2026-04-30",
        verdicts=[_stub_verdict()],
        performance_rows=[_stub_perf_row()],
        spy_return_pct=1.5,
        quarterly_vintage="2026-03-31",
    )
    assert "<html>" in html
    assert "</html>" in html
    assert "AAPL" in html  # from picks
    assert "BUY" in html  # from verdicts
    assert "▲" in html  # from performance (positive alpha)
    assert "SPY" in html  # from performance benchmark row
    assert "2026-04-30" in html  # date in picks header
    assert "2026-03-31" in html  # quarterly vintage


def test_build_email_html_optional_sections_absent():
    html = build_email_html(
        picks=[_stub_pick()],
        date="2026-04-30",
    )
    # No verdicts or performance — those sections should be absent
    assert "Judge Verdicts" not in html
    assert "Prior Month Pick Performance" not in html


def test_build_email_html_is_valid_html_structure():
    html = build_email_html([_stub_pick()], "2026-04-30")
    assert html.startswith("<html>")
    assert html.strip().endswith("</html>")


# ---------------------------------------------------------------------------
# send_email
# ---------------------------------------------------------------------------


def test_send_email_disabled_returns_false():
    # Pass empty recipients so EmailConfig doesn't complain about validation —
    # but we override enabled=False so the send path short-circuits first.
    cfg_disabled = AppConfig(
        storage=StorageConfig(
            provider="firestore",
            firestore={"project_id": "test-project"},
        ),
        notifications=NotificationsConfig(
            email=EmailConfig(enabled=False, recipients=[])
        ),
    )
    result = send_email(cfg_disabled, [_stub_pick()], "2026-04-30")
    assert result is False


def test_send_email_missing_api_key_returns_false(monkeypatch):
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    cfg = _stub_cfg()
    result = send_email(cfg, [_stub_pick()], "2026-04-30")
    assert result is False


def test_send_email_missing_from_address_returns_false(monkeypatch):
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    cfg = _stub_cfg(from_address="")
    # Build cfg manually to avoid Pydantic validation of from_address
    cfg.notifications.email.from_address = ""
    result = send_email(cfg, [_stub_pick()], "2026-04-30")
    assert result is False


def test_send_email_empty_recipients_returns_false(monkeypatch):
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    # Use a cfg with recipients but clear them at the field level to simulate
    # a list that becomes empty after stripping blank strings.
    cfg = _stub_cfg()
    cfg.notifications.email.recipients = []
    result = send_email(cfg, [_stub_pick()], "2026-04-30")
    assert result is False


def test_send_email_success(monkeypatch):
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    cfg = _stub_cfg()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    with patch(
        "screener.lib.email_sender.requests.post", return_value=mock_resp
    ) as mock_post:
        result = send_email(cfg, [_stub_pick()], "2026-04-30")

    assert result is True
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    payload = (
        call_kwargs.kwargs["json"] if call_kwargs.kwargs else call_kwargs[1]["json"]
    )
    assert payload["to"] == ["user@example.com"]
    assert payload["from"] == "screener@example.com"
    assert "2026-04-30" in payload["subject"]


def test_send_email_subject_uses_prefix(monkeypatch):
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    cfg = AppConfig(
        storage=StorageConfig(
            provider="firestore",
            firestore={"project_id": "test-project"},
        ),
        notifications=NotificationsConfig(
            email=EmailConfig(
                enabled=True,
                from_address="screener@example.com",
                recipients=["user@example.com"],
                subject_prefix="[My Screener]",
            )
        ),
    )

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    with patch(
        "screener.lib.email_sender.requests.post", return_value=mock_resp
    ) as mock_post:
        send_email(cfg, [_stub_pick()], "2026-04-30")

    payload = mock_post.call_args.kwargs["json"]
    assert payload["subject"].startswith("[My Screener]")


def test_send_email_subject_shows_month_name(monkeypatch):
    """When date is a YYYY-MM month_id the subject shows 'Month YYYY'."""
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    cfg = _stub_cfg()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    with patch(
        "screener.lib.email_sender.requests.post", return_value=mock_resp
    ) as mock_post:
        send_email(cfg, [_stub_pick()], "2026-04")

    payload = mock_post.call_args.kwargs["json"]
    assert "April 2026" in payload["subject"]


def test_send_email_api_error_returns_false(monkeypatch):
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    cfg = _stub_cfg()

    with patch(
        "screener.lib.email_sender.requests.post",
        side_effect=Exception("connection refused"),
    ):
        result = send_email(cfg, [_stub_pick()], "2026-04-30")

    assert result is False


def test_send_email_http_error_returns_false(monkeypatch):
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    cfg = _stub_cfg()

    import requests as req_lib

    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = req_lib.HTTPError("403 Forbidden")

    with patch("screener.lib.email_sender.requests.post", return_value=mock_resp):
        result = send_email(cfg, [_stub_pick()], "2026-04-30")

    assert result is False


def test_send_email_posts_html_body(monkeypatch):
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    cfg = _stub_cfg()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    with patch(
        "screener.lib.email_sender.requests.post", return_value=mock_resp
    ) as mock_post:
        send_email(
            cfg,
            [_stub_pick(symbol="NVDA")],
            "2026-04-30",
            verdicts=[_stub_verdict(symbol="NVDA", verdict="BUY")],
        )

    payload = mock_post.call_args.kwargs["json"]
    assert "NVDA" in payload["html"]
    assert "BUY" in payload["html"]
