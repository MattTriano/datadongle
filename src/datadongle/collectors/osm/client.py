# /loci_platform/platform/airflow/dags/loci/collectors/osm/client.py
"""
HTTP client for the OSM Overpass API.

Responsibilities:
- POST queries to the Overpass endpoint, with retries and backoff.
- Parse the JSON response and return the raw Overpass payload.

This is the HTTP boundary only; turning Overpass elements into stage-ready row
dicts (geometry assembly, tag promotion, JSON encoding) is the OSMReader's job.

Usage:
    client = OSMClient()
    response = client.fetch(query)          # raw Overpass JSON
    elements = response["elements"]
"""

from __future__ import annotations

import logging
import re
from typing import Any

import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from datadongle.collectors.osm.query import OverpassAPIQuery

logger = logging.getLogger(__name__)


DEFAULT_ENDPOINT = "https://overpass-api.de/api/interpreter"
DEFAULT_USER_AGENT = "loci-osm-collector/1.0"


class OverpassError(RuntimeError):
    """Raised when the Overpass API returns an error response."""


class OverpassRateLimited(OverpassError):
    """Raised on HTTP 429 — caller may retry after a delay."""


class OverpassServerBusy(OverpassError):
    """Raised on HTTP 504 — server is overloaded."""


class OSMClient:
    """
    Thin client over the Overpass API.

    Parameters
    ----------
    endpoint : str
        Overpass API URL. Defaults to the public instance.
    user_agent : str
        User-Agent header sent with every request.
    extra_timeout : int
        Seconds added to the query's own timeout for the HTTP-level
        timeout — gives the server its full budget plus headroom.
    """

    def __init__(
        self,
        endpoint: str = DEFAULT_ENDPOINT,
        user_agent: str = DEFAULT_USER_AGENT,
        extra_timeout: int = 60,
    ) -> None:
        self.endpoint = endpoint
        self.extra_timeout = extra_timeout
        self.logger = logging.getLogger("osm_client")

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch(
        self,
        query: OverpassAPIQuery,
        date_filter: str | None = None,
    ) -> dict[str, Any]:
        """
        Render the query, POST it, and return the parsed JSON response.
        """
        ql = query.to_ql(date_filter=date_filter)
        return self._post(ql, http_timeout=query.timeout + self.extra_timeout)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type(
            (
                ChunkedEncodingError,
                ConnectionError,
                ReadTimeout,
                OverpassRateLimited,
                OverpassServerBusy,
            )
        ),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=10, max=120),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _post(self, ql: str, http_timeout: int) -> dict[str, Any]:
        """POST one query to Overpass and return the parsed JSON."""
        self.logger.info(
            "POST %s (query=%d chars, timeout=%ds)",
            self.endpoint,
            len(ql),
            http_timeout,
        )

        resp = self._session.post(
            self.endpoint,
            data={"data": ql},
            timeout=http_timeout,
        )

        if resp.status_code == 429:
            raise OverpassRateLimited(
                f"Overpass rate-limited (429).\nResponse: {_extract_overpass_error(resp.text)}"
            )
        if resp.status_code == 504:
            raise OverpassServerBusy(
                f"Overpass server busy (504).\nResponse: {_extract_overpass_error(resp.text)}"
            )
        if resp.status_code >= 400:
            raise OverpassError(
                f"Overpass returned HTTP {resp.status_code}.\n"
                f"Response: {_extract_overpass_error(resp.text)}\n"
                f"Query was:\n{ql}"
            )

        try:
            return resp.json()
        except ValueError as exc:
            raise OverpassError(f"Overpass returned non-JSON response: {resp.text[:500]}") from exc


# ----------------------------------------------------------------------
# Error extraction
# ----------------------------------------------------------------------


_OVERPASS_ERROR_RE = re.compile(
    r"<p><strong[^>]*>Error</strong>:?\s*(.*?)</p>",
    re.DOTALL | re.IGNORECASE,
)


def _extract_overpass_error(body: str) -> str:
    """
    Pull human-readable error messages out of Overpass's HTML response.

    Overpass returns errors as HTML pages with one or more
    `<p><strong>Error</strong>: ...</p>` blocks. This pulls those out
    and joins them. Falls back to the raw body (truncated) if no
    error blocks are found.
    """
    matches = _OVERPASS_ERROR_RE.findall(body)
    if matches:
        return "\n".join(m.strip() for m in matches)
    return body[:1000].strip()
