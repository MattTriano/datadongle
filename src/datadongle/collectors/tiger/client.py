"""TigerClient — HTTP mechanics for the Census TIGER/Line file server.

Owns the session, retry/backoff, directory-listing fetches, and streamed zip
downloads. It knows nothing about specs, schemas, write modes, or storage;
``TigerMetadata`` uses it for discovery and ``TigerReader`` uses it for
downloads.
"""

from __future__ import annotations

import logging
import tempfile
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

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _is_retryable(exc: BaseException) -> bool:
    """Retry predicate: transient HTTP statuses and connection/read errors."""
    if isinstance(exc, HTTPError) and exc.response is not None:
        return exc.response.status_code in RETRYABLE_STATUS
    return isinstance(exc, (ConnectionError, ReadTimeout, ChunkedEncodingError))


class TigerClient:
    """Fetches directory listings and downloads shapefile zips from the TIGER server."""

    def __init__(self, request_timeout: int = 300, listing_timeout: int = 30) -> None:
        self.request_timeout = request_timeout
        self.listing_timeout = listing_timeout
        self._session = requests.Session()
        self.logger = logging.getLogger("tiger_client")

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_text(self, url: str) -> str:
        """Fetch an HTML directory listing."""
        resp = self._session.get(url, timeout=self.listing_timeout)
        resp.raise_for_status()
        return resp.text

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=10, max=120),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def download_to_tempfile(self, url: str, suffix: str = ".zip") -> Path:
        """Stream a URL to a temp file in chunks. Returns the file path."""
        self.logger.info("Downloading %s", url)
        resp = self._session.get(url, stream=True, timeout=self.request_timeout)
        resp.raise_for_status()

        tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix="tiger_", delete=False)
        try:
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.close()
            filepath = Path(tmp.name)
            size_mb = filepath.stat().st_size / (1024 * 1024)
            self.logger.info("Downloaded %s to %s (%.1f MB)", url, filepath, size_mb)
            return filepath
        except Exception:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise
