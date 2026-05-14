"""
screener/edgar/fetcher.py — SEC EDGAR fetch, parse, and chunking utilities.

Provides:
    EDGARFetchError         — raised on unrecoverable HTTP failures
    resolve_cik             — ticker → zero-padded 10-digit CIK string
    fetch_filing_metadata   — list of recent filings for a CIK
    download_primary_document — raw bytes of a filing document
    strip_html              — clean HTML/iXBRL bytes to plain text
    detect_section          — identify section heading from a single line
    annotate_sections       — produce (start, end, section_name) spans for text
    chunk_text              — sliding-window chunker with metadata + section labels
    get_filing_chunks       — top-level entrypoint combining all steps

Rate-limiting:
    The SEC EDGAR EDGAR fair-use policy requires a maximum of 10 requests
    per second from a single IP.  This module enforces a gap of at least
    0.11 s between requests (_RATE_LIMIT_GAP) and applies exponential
    back-off on HTTP 429 responses.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date, timedelta

import requests
from lxml.html.clean import Cleaner

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "multi-agent-stock-screener research@example.com"}

_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
_ARCHIVE_URL = (
    "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_dashes}/{primary_document}"
)

_cik_cache: dict[str, str] = {}
_last_request_time: float = 0.0

_RATE_LIMIT_GAP = 0.11  # seconds between requests — SEC allows ~10 req/s
_BACKOFF_START = 1.0
_BACKOFF_CAP = 60.0

# ---------------------------------------------------------------------------
# P2-08 — Section awareness
# ---------------------------------------------------------------------------

# Ordered list of (canonical_name, compiled_pattern) pairs.  We test each
# pattern against each line of the filing text.  The first match wins and
# sets the current section until the next heading is detected.
#
# Patterns are intentionally loose (case-insensitive, optional punctuation)
# so they match both the SEC XBRL exhibit style ("Item 1A.") and the free-text
# inline style ("ITEM 1A — RISK FACTORS").
_SECTION_PATTERNS_RAW: list[tuple[str, re.Pattern]] = [
    (
        "Risk Factors",
        re.compile(r"item\s+1a[\.\s\-–—]*risk\s+factors", re.IGNORECASE),
    ),
    (
        "Business",
        re.compile(r"item\s+1[\.\s\-–—]*business\b", re.IGNORECASE),
    ),
    (
        "Legal Proceedings",
        re.compile(r"item\s+3[\.\s\-–—]*legal\s+proceedings", re.IGNORECASE),
    ),
    (
        "MD&A",
        re.compile(
            r"item\s+7[\.\s\-–—]*(management.{0,10}discussion|md\s*&\s*a)",
            re.IGNORECASE,
        ),
    ),
    (
        "Quantitative and Qualitative Disclosures",
        re.compile(
            r"item\s+7a[\.\s\-–—]*quantitative",
            re.IGNORECASE,
        ),
    ),
    (
        "Financial Statements",
        re.compile(
            r"item\s+8[\.\s\-–—]*(financial\s+statements|consolidated\s+financial)",
            re.IGNORECASE,
        ),
    ),
    (
        "Controls and Procedures",
        re.compile(r"item\s+9a[\.\s\-–—]*controls", re.IGNORECASE),
    ),
    # 10-Q specific: Item 1 Financial Statements / Item 2 MD&A
    (
        "Financial Statements",
        re.compile(
            r"item\s+1[\.\s\-–—]*financial\s+statements",
            re.IGNORECASE,
        ),
    ),
    (
        "MD&A",
        re.compile(
            r"item\s+2[\.\s\-–—]*(management.{0,10}discussion|md\s*&\s*a)",
            re.IGNORECASE,
        ),
    ),
]

# De-duplicate: keep first occurrence per canonical name so 10-K patterns
# take precedence over 10-Q variants that share the same canonical name.
_seen_section_names: set[str] = set()
_SECTION_PATTERNS: list[tuple[str, re.Pattern]] = []
for _name, _pat in _SECTION_PATTERNS_RAW:
    if _name not in _seen_section_names:
        _seen_section_names.add(_name)
        _SECTION_PATTERNS.append((_name, _pat))


def detect_section(line: str) -> str | None:
    """Return the canonical section name if *line* matches a known heading.

    Args:
        line: A single line of plain-text filing content.

    Returns:
        Canonical section name string (e.g. ``"Risk Factors"``) or ``None`` if
        the line does not match any known heading pattern.
    """
    for name, pattern in _SECTION_PATTERNS:
        if pattern.search(line):
            return name
    return None


def annotate_sections(text: str) -> list[tuple[int, int, str]]:
    """Return a list of ``(start, end, section_name)`` character spans.

    Scans *text* line by line, tracking the current section.  Each span covers
    the character range where a particular section is active.  Spans do not
    overlap and together cover the full text.

    Args:
        text: Plain-text filing content.

    Returns:
        List of ``(start_char, end_char, section_name)`` tuples.  ``section_name``
        is ``""`` for text before the first detected heading.
    """
    spans: list[tuple[int, int, str]] = []
    current_section = ""
    current_start = 0

    pos = 0
    for line in text.splitlines(keepends=True):
        line_start = pos
        pos += len(line)

        detected = detect_section(line.strip())
        if detected is not None and detected != current_section:
            # Close the current span
            if line_start > current_start:
                spans.append((current_start, line_start, current_section))
            current_section = detected
            current_start = line_start

    # Close final span
    if pos > current_start:
        spans.append((current_start, pos, current_section))

    return spans


# ---------------------------------------------------------------------------
# HTML cleaning
# ---------------------------------------------------------------------------

_cleaner = Cleaner(
    scripts=True,
    javascript=True,
    comments=True,
    style=True,
    links=True,
    meta=True,
    page_structure=False,
    processing_instructions=True,
    embedded=True,
    frames=True,
    forms=False,
    annoying_tags=True,
    remove_unknown_tags=False,
    safe_attrs_only=False,
)


class EDGARFetchError(Exception):
    """Raised on unrecoverable HTTP failures when fetching from SEC EDGAR."""


def _throttle() -> None:
    """Enforce the minimum gap between consecutive SEC EDGAR requests."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < _RATE_LIMIT_GAP:
        time.sleep(_RATE_LIMIT_GAP - elapsed)
    _last_request_time = time.time()


def _get_with_backoff(url: str) -> requests.Response:
    """GET a URL with rate-limit throttling and exponential back-off on HTTP 429.

    Args:
        url: The URL to fetch.

    Returns:
        A successful :class:`requests.Response`.

    Raises:
        EDGARFetchError: On any non-200, non-429 status code.
    """
    delay = _BACKOFF_START
    while True:
        _throttle()
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp
        if resp.status_code == 429:
            logger.warning("SEC EDGAR rate-limited — back-off %.1fs", delay)
            time.sleep(min(delay, _BACKOFF_CAP))
            delay = min(delay * 2, _BACKOFF_CAP)
            continue
        raise EDGARFetchError(f"HTTP {resp.status_code} fetching {url}")


def resolve_cik(ticker: str) -> str | None:
    """Resolve a ticker symbol to a zero-padded 10-digit CIK string.

    Fetches the full company_tickers.json from SEC on first call and caches
    the result in-process for subsequent calls.

    Args:
        ticker: Upper-case ticker symbol, e.g. ``"AAPL"``.

    Returns:
        Zero-padded CIK string (e.g. ``"0000320193"``) or ``None`` if the
        ticker is not found in the SEC's ticker file.
    """
    global _cik_cache
    key = ticker.upper()
    if key in _cik_cache:
        return _cik_cache[key]

    if not _cik_cache:
        _throttle()
        resp = requests.get(_TICKERS_URL, headers=_HEADERS, timeout=30)
        if resp.status_code != 200:
            return None
        data = resp.json()
        for entry in data.values():
            t = str(entry.get("ticker", "")).upper()
            cik_int = entry.get("cik_str", entry.get("cik", 0))
            _cik_cache[t] = str(cik_int).zfill(10)

    return _cik_cache.get(key)


def fetch_filing_metadata(
    cik: str,
    form_types: list[str] | None = None,
    years: int = 2,
) -> list[dict]:
    """Fetch filing metadata for a CIK, filtered by form type and recency.

    Args:
        cik: Zero-padded 10-digit CIK string.
        form_types: Form types to include.  Defaults to ``["10-K", "10-Q"]``.
        years: How far back to look (in calendar years from today).

    Returns:
        List of dicts, each with keys: ``accession_number``, ``form_type``,
        ``filing_date``, ``period_of_report``, ``primary_document``.
    """
    if form_types is None:
        form_types = ["10-K", "10-Q"]

    url = _SUBMISSIONS_URL.format(cik=cik)
    resp = _get_with_backoff(url)
    data = resp.json()

    recent = data.get("filings", {}).get("recent", {})
    accession_numbers = recent.get("accessionNumber", [])
    form_type_list = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    periods = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])

    cutoff = date.today() - timedelta(days=years * 365)

    results = []
    for acc, ft, fd, period, doc in zip(
        accession_numbers, form_type_list, filing_dates, periods, primary_docs
    ):
        if ft not in form_types:
            continue
        try:
            filing_date = date.fromisoformat(fd)
        except (ValueError, TypeError):
            continue
        if filing_date < cutoff:
            continue
        acc_with_dashes = acc if "-" in acc else (f"{acc[:10]}-{acc[10:12]}-{acc[12:]}")
        results.append(
            {
                "accession_number": acc_with_dashes,
                "form_type": ft,
                "filing_date": fd,
                "period_of_report": period,
                "primary_document": doc,
            }
        )

    return results


def download_primary_document(
    cik: str, accession_number: str, primary_document: str
) -> bytes:
    """Download the raw bytes of a filing's primary document.

    Args:
        cik: Zero-padded 10-digit CIK string.
        accession_number: Accession number with dashes (e.g. ``"0001234567-24-000001"``).
        primary_document: Filename of the primary document (e.g. ``"form10k.htm"``).

    Returns:
        Raw bytes of the filing document.

    Raises:
        EDGARFetchError: On non-200 HTTP responses.
    """
    acc_no_dashes = accession_number.replace("-", "")
    url = _ARCHIVE_URL.format(
        cik=cik,
        acc_no_dashes=acc_no_dashes,
        primary_document=primary_document,
    )
    resp = _get_with_backoff(url)
    return resp.content


def strip_html(raw_bytes: bytes) -> str:
    """Strip HTML/iXBRL markup and return clean plain text.

    Uses lxml's ``Cleaner`` to remove scripts, styles, and other noise before
    extracting the text content.  A heuristic searches for the first position
    in the text that looks like natural-language prose (two consecutive
    lower-case words), skipping over preamble boilerplate.

    Args:
        raw_bytes: Raw HTML/iXBRL bytes of a filing document.

    Returns:
        Plain-text string.  May be empty if no prose is found.
    """
    from lxml import html as lxml_html

    cleaned = _cleaner.clean_html(raw_bytes)
    if isinstance(cleaned, bytes):
        tree = lxml_html.fromstring(cleaned)
        text = tree.text_content()
    else:
        text = cleaned.text_content()

    # Find the first position that looks like natural-language prose
    i = 0
    pattern = re.compile(r"\b[a-z][a-z]+\s+[a-z][a-z]+\b", re.IGNORECASE)
    while i < len(text):
        chunk = text[i:]
        lower_chunk = chunk.lower()
        m = pattern.search(lower_chunk)
        if m is None:
            break
        candidate_pos = i + m.start()
        candidate_text = text[candidate_pos : candidate_pos + 20]
        if not re.search(r"[A-Z]{2}[a-z]|[a-z][A-Z]|https?://|[:/]", candidate_text):
            return text[candidate_pos:]
        i = candidate_pos + m.end()

    return text


# ---------------------------------------------------------------------------
# Chunking helpers
# ---------------------------------------------------------------------------


def _section_at(char_pos: int, spans: list[tuple[int, int, str]]) -> str:
    """Return the section name active at character position *char_pos*.

    Performs a linear scan of *spans*.  The list is expected to be short
    (one entry per heading) so a binary search is not warranted.

    Args:
        char_pos: Character offset within the original filing text.
        spans: Output of :func:`annotate_sections`.

    Returns:
        Section name string, or ``""`` if no span covers *char_pos*.
    """
    for start, end, name in spans:
        if start <= char_pos < end:
            return name
    return ""


def chunk_text(
    text: str,
    ticker: str,
    form_type: str,
    period: str,
    chunk_size: int = 512,
    overlap: float = 0.10,
    section_spans: list[tuple[int, int, str]] | None = None,
) -> list[dict]:
    """Split plain text into overlapping chunks with filing metadata.

    Uses a character-based approximation: ``chunk_size * 4`` characters per
    chunk (roughly 4 chars/token).  Chunks shorter than 100 characters after
    stripping are discarded.

    P2-08: If ``section_spans`` is provided (from :func:`annotate_sections`),
    each chunk's ``section`` field is set to the detected heading active at
    that chunk's start position.  If omitted, ``section`` defaults to ``""``.

    Args:
        text: Plain-text filing content.
        ticker: Upper-case ticker symbol.
        form_type: Filing type (e.g. ``"10-K"``).
        period: Period-of-report date string (e.g. ``"2024-12-31"``).
        chunk_size: Target size in tokens.  Defaults to 512.
        overlap: Overlap fraction (0.0–1.0).  Defaults to 0.10 (10 %).
        section_spans: Optional list of ``(start, end, section_name)`` spans
            from :func:`annotate_sections`.  When supplied the ``section``
            field on each chunk is populated automatically.

    Returns:
        List of chunk dicts, each with keys: ``ticker``, ``form_type``,
        ``period``, ``section``, ``chunk_index``, ``text``.
    """
    if not text or not text.strip():
        return []

    char_size = chunk_size * 4  # ~4 chars per token
    overlap_chars = int(char_size * overlap)
    step = char_size - overlap_chars

    spans: list[tuple[int, int, str]] = section_spans or []

    chunks = []
    pos = 0
    chunk_index = 0

    while pos < len(text):
        end = pos + char_size
        chunk_str = text[pos:end]
        if len(chunk_str.strip()) >= 100:
            section = _section_at(pos, spans) if spans else ""
            chunks.append(
                {
                    "ticker": ticker,
                    "form_type": form_type,
                    "period": period,
                    "section": section,
                    "chunk_index": chunk_index,
                    "text": chunk_str,
                }
            )
            chunk_index += 1
        pos += step

    return chunks


def get_filing_chunks(
    ticker: str,
    form_types: list[str] | None = None,
    years: int = 2,
    chunk_size: int = 512,
    overlap: float = 0.10,
) -> list[dict]:
    """Fetch, parse, and chunk all recent filings for a ticker.

    Combines :func:`resolve_cik`, :func:`fetch_filing_metadata`,
    :func:`download_primary_document`, :func:`strip_html`,
    :func:`annotate_sections`, and :func:`chunk_text` into a single call.

    Args:
        ticker: Upper-case ticker symbol.
        form_types: Form types to include.  Defaults to ``["10-K", "10-Q"]``.
        years: How far back to look (in calendar years from today).
        chunk_size: Target chunk size in tokens.
        overlap: Overlap fraction between consecutive chunks.

    Returns:
        List of chunk dicts ready for embedding.  Returns an empty list if
        the CIK cannot be resolved or all filings fail to download.
    """
    if form_types is None:
        form_types = ["10-K", "10-Q"]

    cik = resolve_cik(ticker)
    if cik is None:
        logger.warning("EDGAR: CIK not found for ticker=%s — skipping", ticker)
        return []

    try:
        filings = fetch_filing_metadata(cik, form_types, years)
    except EDGARFetchError as exc:
        logger.warning("EDGAR fetch error [%s]: %s", ticker, exc)
        return []

    all_chunks: list[dict] = []
    for filing in filings:
        try:
            raw = download_primary_document(
                cik,
                filing["accession_number"],
                filing["primary_document"],
            )
            text = strip_html(raw)
            # P2-08: detect section boundaries before chunking so each chunk
            # carries the correct section label.
            spans = annotate_sections(text)
            chunks = chunk_text(
                text,
                ticker,
                filing["form_type"],
                filing["period_of_report"],
                chunk_size=chunk_size,
                overlap=overlap,
                section_spans=spans,
            )
            all_chunks.extend(chunks)
            logger.debug(
                "EDGAR: fetched %d chunks from %s %s for %s",
                len(chunks),
                filing["form_type"],
                filing["period_of_report"],
                ticker,
            )
        except EDGARFetchError as exc:
            logger.warning("EDGAR fetch error [%s]: %s", ticker, exc)
            continue

    return all_chunks
