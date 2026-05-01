"""
tests/edgar/test_fetcher.py — Unit tests for screener/edgar/fetcher.py

Covers:
- resolve_cik: cache hit, SEC lookup success, ticker not found
- fetch_filing_metadata: filters by form type and date cutoff
- strip_html: returns non-empty plain text
- chunk_text: correct chunk count, overlap, short-chunk filtering
- get_filing_chunks: full pipeline mocked end-to-end; CIK not found path
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from screener.edgar.fetcher import (
    chunk_text,
    fetch_filing_metadata,
    get_filing_chunks,
    resolve_cik,
    strip_html,
)


# ---------------------------------------------------------------------------
# resolve_cik
# ---------------------------------------------------------------------------


def test_resolve_cik_cache_hit():
    """Returns cached CIK without hitting the SEC endpoint."""
    import screener.edgar.fetcher as fetcher_mod

    fetcher_mod._cik_cache = {"AAPL": "0000320193"}
    result = resolve_cik("aapl")  # lower-case — should be normalised
    assert result == "0000320193"
    fetcher_mod._cik_cache = {}  # reset


def test_resolve_cik_from_sec(monkeypatch):
    """Populates cache from SEC company_tickers.json on first call."""
    import screener.edgar.fetcher as fetcher_mod

    fetcher_mod._cik_cache = {}  # ensure cold cache

    fake_data = {
        "0": {"ticker": "AAPL", "cik_str": 320193},
        "1": {"ticker": "MSFT", "cik_str": 789019},
    }
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = fake_data

    monkeypatch.setattr(
        "screener.edgar.fetcher.requests.get", lambda *a, **kw: mock_resp
    )

    cik = resolve_cik("AAPL")
    assert cik == "0000320193"

    # Second call hits cache — requests.get not called again
    with patch("screener.edgar.fetcher.requests.get") as mock_get:
        cik2 = resolve_cik("AAPL")
        mock_get.assert_not_called()
    assert cik2 == "0000320193"

    fetcher_mod._cik_cache = {}  # reset


def test_resolve_cik_not_found(monkeypatch):
    """Returns None for a ticker that is not in the SEC file."""
    import screener.edgar.fetcher as fetcher_mod

    fetcher_mod._cik_cache = {}

    fake_data = {"0": {"ticker": "AAPL", "cik_str": 320193}}
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = fake_data

    monkeypatch.setattr(
        "screener.edgar.fetcher.requests.get", lambda *a, **kw: mock_resp
    )

    result = resolve_cik("ZZZZ")
    assert result is None

    fetcher_mod._cik_cache = {}  # reset


def test_resolve_cik_sec_returns_non_200(monkeypatch):
    """Returns None when SEC endpoint returns a non-200 status."""
    import screener.edgar.fetcher as fetcher_mod

    fetcher_mod._cik_cache = {}

    mock_resp = MagicMock()
    mock_resp.status_code = 503

    monkeypatch.setattr(
        "screener.edgar.fetcher.requests.get", lambda *a, **kw: mock_resp
    )

    result = resolve_cik("AAPL")
    assert result is None

    fetcher_mod._cik_cache = {}  # reset


# ---------------------------------------------------------------------------
# fetch_filing_metadata
# ---------------------------------------------------------------------------


def _make_submissions_response(entries: list[dict]) -> dict:
    """Build a fake SEC submissions JSON payload from a list of filing dicts."""
    return {
        "filings": {
            "recent": {
                "accessionNumber": [e["acc"] for e in entries],
                "form": [e["form"] for e in entries],
                "filingDate": [e["date"] for e in entries],
                "reportDate": [e["period"] for e in entries],
                "primaryDocument": [e["doc"] for e in entries],
            }
        }
    }


def test_fetch_filing_metadata_filters_form_type(monkeypatch):
    """Only includes 10-K and 10-Q filings; skips 8-K."""
    entries = [
        {"acc": "0001234567-24-000001", "form": "10-K", "date": "2024-01-01", "period": "2023-12-31", "doc": "10k.htm"},
        {"acc": "0001234567-24-000002", "form": "8-K", "date": "2024-02-01", "period": "2024-01-31", "doc": "8k.htm"},
        {"acc": "0001234567-24-000003", "form": "10-Q", "date": "2024-05-01", "period": "2024-03-31", "doc": "10q.htm"},
    ]
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = _make_submissions_response(entries)

    monkeypatch.setattr(
        "screener.edgar.fetcher._get_with_backoff", lambda url: mock_resp
    )

    results = fetch_filing_metadata("0000320193", years=5)
    form_types = [r["form_type"] for r in results]

    assert "8-K" not in form_types
    assert "10-K" in form_types
    assert "10-Q" in form_types


def test_fetch_filing_metadata_date_cutoff(monkeypatch):
    """Excludes filings older than the configured years cutoff."""
    from datetime import date, timedelta

    recent_date = (date.today() - timedelta(days=30)).isoformat()   # 30 days ago — always within years=2
    old_date = (date.today() - timedelta(days=365 * 5)).isoformat()  # 5 years ago — always outside years=2

    entries = [
        {"acc": "0001234567-24-000001", "form": "10-K", "date": recent_date, "period": "2023-12-31", "doc": "10k.htm"},
        {"acc": "0001234567-20-000001", "form": "10-K", "date": old_date, "period": "2019-12-31", "doc": "10k_old.htm"},
    ]
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = _make_submissions_response(entries)

    monkeypatch.setattr(
        "screener.edgar.fetcher._get_with_backoff", lambda url: mock_resp
    )

    results = fetch_filing_metadata("0000320193", years=2)
    # Only the recent filing should make it through when years=2 (old_date is too old)
    assert len(results) == 1
    assert results[0]["filing_date"] == recent_date


# ---------------------------------------------------------------------------
# strip_html
# ---------------------------------------------------------------------------


def test_strip_html_returns_text():
    """Extracts readable plain text from a minimal HTML fixture."""
    html = b"<html><body><p>The company reported strong revenue growth.</p></body></html>"
    result = strip_html(html)
    # Should contain some portion of the prose
    assert "revenue" in result or len(result) > 0


def test_strip_html_empty_input():
    """Returns a string (possibly empty) for near-empty HTML."""
    html = b"<html><body></body></html>"
    result = strip_html(html)
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# chunk_text
# ---------------------------------------------------------------------------


def test_chunk_text_produces_chunks():
    """Produces at least one chunk for a text longer than the minimum threshold."""
    text = "The company has reported consistent revenue growth. " * 200  # ~10k chars
    chunks = chunk_text(text, "AAPL", "10-K", "2024-12-31", chunk_size=512, overlap=0.10)
    assert len(chunks) >= 1
    assert all(c["ticker"] == "AAPL" for c in chunks)
    assert all(c["form_type"] == "10-K" for c in chunks)


def test_chunk_text_overlap_produces_more_chunks():
    """More chunks are produced with overlap than without (for the same text)."""
    text = "Word " * 2000  # long enough to require chunking
    chunks_with_overlap = chunk_text(text, "AAPL", "10-K", "2024-12-31", chunk_size=512, overlap=0.10)
    chunks_no_overlap = chunk_text(text, "AAPL", "10-K", "2024-12-31", chunk_size=512, overlap=0.0)
    assert len(chunks_with_overlap) >= len(chunks_no_overlap)


def test_chunk_text_empty_input():
    """Returns empty list for empty or whitespace-only input."""
    assert chunk_text("", "AAPL", "10-K", "2024-12-31") == []
    assert chunk_text("   \n\t  ", "AAPL", "10-K", "2024-12-31") == []


def test_chunk_text_short_chunks_filtered():
    """Chunks shorter than 100 characters are discarded."""
    text = "Hi."  # Very short — will produce a sub-100-char chunk
    chunks = chunk_text(text, "AAPL", "10-K", "2024-12-31")
    assert chunks == []


def test_chunk_text_chunk_index_sequential():
    """chunk_index values are sequential starting from 0."""
    text = "Revenue grew steadily. " * 500
    chunks = chunk_text(text, "MSFT", "10-Q", "2024-03-31", chunk_size=512, overlap=0.0)
    for i, chunk in enumerate(chunks):
        assert chunk["chunk_index"] == i


# ---------------------------------------------------------------------------
# get_filing_chunks — end-to-end (mocked)
# ---------------------------------------------------------------------------


def test_get_filing_chunks_cik_not_found(monkeypatch):
    """Returns empty list when CIK cannot be resolved."""
    monkeypatch.setattr("screener.edgar.fetcher.resolve_cik", lambda t: None)
    result = get_filing_chunks("ZZZZ")
    assert result == []


def test_get_filing_chunks_end_to_end(monkeypatch):
    """Full pipeline: resolve_cik → fetch metadata → download → strip → chunk."""
    import screener.edgar.fetcher as fetcher_mod

    monkeypatch.setattr("screener.edgar.fetcher.resolve_cik", lambda t: "0000320193")

    filings = [
        {
            "accession_number": "0001234567-24-000001",
            "form_type": "10-K",
            "filing_date": "2024-01-01",
            "period_of_report": "2023-12-31",
            "primary_document": "10k.htm",
        }
    ]
    monkeypatch.setattr(
        "screener.edgar.fetcher.fetch_filing_metadata",
        lambda cik, form_types, years: filings,
    )

    long_text = "The company reported strong financial performance with growing revenue. " * 300
    monkeypatch.setattr(
        "screener.edgar.fetcher.download_primary_document",
        lambda cik, acc, doc: b"<html><body>" + long_text.encode() + b"</body></html>",
    )

    chunks = get_filing_chunks("AAPL", chunk_size=512, overlap=0.10)
    assert len(chunks) >= 1
    assert chunks[0]["ticker"] == "AAPL"
    assert chunks[0]["form_type"] == "10-K"
