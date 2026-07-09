"""StaticFileClient — downloads and parses static published files.

Deliberately dumb: it knows how to fetch bytes politely (streaming to a temp
file, with retries) and turn CSV/XLSX on disk into rows of strings, and nothing
else. Everything source-specific (which URLs, which sheet, which delimiter) lives
on the spec's FileRefs.

Bot-defense note: some publishers (e.g. ahrq.gov) sit behind AWS WAF JavaScript
challenges that this client cannot solve. The ``cookies`` parameter exists so a
manually obtained token (e.g. an aws-waf-token copied from a browser) can be
injected; if that's too fragile, FileRef supports ``file://`` URLs for manually
downloaded copies. No WAF logic lives here.
"""

from __future__ import annotations

import csv
import logging
import re
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError, HTTPError, ReadTimeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from datadongle.collectors.static.spec import FileRef

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:151.0) Gecko/20100101 Firefox/151.0"
)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class StaticFileDownloadError(RuntimeError):
    """A download failed or returned something other than the expected file."""


def _is_retryable(exc: BaseException) -> bool:
    """Retry predicate: transient HTTP statuses and connection/read errors.

    Deliberately excludes StaticFileDownloadError (an HTML challenge page won't
    fix itself on retry) and 4xx (a bad URL is a bad URL)."""
    if isinstance(exc, HTTPError) and exc.response is not None:
        return exc.response.status_code in RETRYABLE_STATUS
    return isinstance(exc, (ConnectionError, ReadTimeout, ChunkedEncodingError))


def sanitize_column_name(name: str) -> str:
    """Normalize a source column name for the warehouse: lowercase, runs of
    non-alphanumerics collapsed to single underscores."""
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return cleaned or "unnamed"


def _looks_like_html(content_type: str, head: bytes) -> bool:
    """A data URL returning HTML almost always means a bot-defense challenge
    page or an error page — never parse it as data."""
    start = head[:512].lstrip().lower()
    return "text/html" in content_type or start.startswith((b"<!doctype", b"<html"))


class StaticFileClient:
    """Fetches published files (streamed to disk) and parses CSV/XLSX from disk.

    Parameters
    ----------
    user_agent : str
        Sent on every request. Defaults to an honest, identified UA (the polite
        norm for government data sites); swap in a browser UA only if challenged.
    cookies : dict[str, str] | None
        Cookies to preload into the session (e.g. a manually obtained
        aws-waf-token). If using a browser-minted WAF token, set ``user_agent``
        to match the browser it came from.
    delay_seconds : float
        Minimum spacing between HTTP requests.
    timeout : int
        Per-request timeout in seconds.
    """

    def __init__(
        self,
        user_agent: str = DEFAULT_USER_AGENT,
        cookies: dict[str, str] | None = None,
        delay_seconds: float = 1.0,
        timeout: int = 60,
    ):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        for name, value in (cookies or {}).items():
            self.session.cookies.set(name, value)
        self.delay_seconds = delay_seconds
        self.timeout = timeout
        self._last_request_at = 0.0

    # ------------------------------------------------------------------
    # Downloading
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def download_to_tempfile(self, url: str, suffix: str = ".download") -> Path:
        """Stream an ``https`` URL to a temp file in chunks. Returns the path.

        Retries transient failures; raises immediately on a 4xx or on an HTML
        response (a WAF challenge or error page). ``file://`` URLs are handled by
        the reader, which reads them in place."""
        self._throttle()
        resp = self.session.get(url, stream=True, timeout=self.timeout)
        resp.raise_for_status()

        chunks = resp.iter_content(chunk_size=8192)
        first = next(chunks, b"")
        if _looks_like_html(resp.headers.get("Content-Type", ""), first):
            raise StaticFileDownloadError(
                f"Expected a data file but got an HTML page from {url} "
                f"(Content-Type: {resp.headers.get('Content-Type')!r}). This is likely "
                "a WAF challenge or an expired token; see the module docstring."
            )

        tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix="static_", delete=False)
        try:
            if first:
                tmp.write(first)
            for chunk in chunks:
                tmp.write(chunk)
            tmp.close()
            logger.info("Downloaded %s to %s", url, tmp.name)
            return Path(tmp.name)
        except Exception:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)
        self._last_request_at = time.monotonic()

    # ------------------------------------------------------------------
    # Parsing (from a local file path)
    # ------------------------------------------------------------------

    def parse_file(self, filepath: Path, file_ref: FileRef) -> Iterator[dict[str, str]]:
        """Parse a local CSV/XLSX file, yielding rows as dicts of strings keyed
        by sanitized column names. Streams from disk (bounded memory)."""
        if file_ref.file_format == "csv":
            yield from self._iter_csv(filepath, file_ref)
        else:
            yield from self._iter_xlsx(filepath, file_ref)

    @staticmethod
    def _iter_csv(filepath: Path, file_ref: FileRef) -> Iterator[dict[str, str]]:
        with open(filepath, encoding=file_ref.encoding, newline="") as f:
            for _ in range(file_ref.skip_rows):
                f.readline()
            reader = csv.DictReader(f, delimiter=file_ref.delimiter)
            for raw in reader:
                yield {
                    sanitize_column_name(key): (value or "").strip()
                    for key, value in raw.items()
                    if key is not None  # DictReader uses key None for overflow cells
                }

    @staticmethod
    def _iter_xlsx(filepath: Path, file_ref: FileRef) -> Iterator[dict[str, str]]:
        import openpyxl  # imported lazily; only needed for xlsx manifests

        workbook = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        try:
            if isinstance(file_ref.sheet, int):
                sheet = workbook.worksheets[file_ref.sheet]
            else:
                sheet = workbook[file_ref.sheet]

            rows = sheet.iter_rows(min_row=file_ref.skip_rows + 1, values_only=True)
            header_cells = next(rows, None)
            if header_cells is None:
                return
            columns = [sanitize_column_name(str(c)) for c in header_cells if c is not None]

            for cells in rows:
                yield {col: _cell_to_str(value) for col, value in zip(columns, cells, strict=True)}
        finally:
            workbook.close()


def _cell_to_str(value) -> str:
    """Render an openpyxl cell value as text without Excel artifacts: integral
    floats lose the trailing '.0' Excel gives them, dates become ISO 8601, None
    becomes empty string."""
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value).strip()
