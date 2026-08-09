"""CourtListenerClient — thin HTTP client for the CourtListener API and bulk data.

Knows three things:
  1. how to page results out of the REST API v4 (cursor pagination: follow
     each payload's ``next`` URL until it is null)
  2. how to list and stream the monthly bulk CSV exports from the public
     S3 bucket
  3. how to peek a bulk file's CSV header without downloading the whole file

Auth is an optional API token sent as ``Authorization: Token <token>``
(anonymous requests work at a lower rate limit). Unlike EIA, the token never
appears in a URL, so no scrubbing is needed — just don't log request headers.

API docs:  https://www.courtlistener.com/help/api/rest/
Bulk docs: https://www.courtlistener.com/help/api/bulk-data/

NOTE (developed offline — verify against the live source; see the network
tests in ``tests/collectors/courtlistener/test_live.py``):
  - the ``page_size`` query parameter and its maximum

Verified against the live source: the bulk bucket URL and its ``list-type=2``
listing, and ``BULK_CSV_QUOTECHAR`` (standard ``"`` quoting, not the backtick
the bulk-data docs describe).

Usage:
    from datadongle.collectors.courtlistener.client import CourtListenerClient

    client = CourtListenerClient()  # reads COURTLISTENER_API_TOKEN if set
    client.api_root()               # endpoint name -> URL
    for page in client.iter_pages("courts", params={"jurisdiction": "F"}):
        ...
    client.list_bulk_exports("courts")
"""

from __future__ import annotations

import bz2
import csv
import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://www.courtlistener.com/api/rest/v4"

# Public S3 bucket holding the monthly bulk exports, under the bulk-data/ key
# prefix. Files are named "<prefix>-<YYYY-MM-DD>.csv.bz2".
BULK_STORAGE_URL = "https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/"
BULK_KEY_PREFIX = "bulk-data/"

# Bulk CSVs are standard comma-delimited, double-quote-quoted CSV.
#
# CourtListener's bulk-data docs describe a backtick quotechar (chosen, they
# say, because court text is full of double-quotes), and this collector
# believed them until a live run against the `courts` export came back with
# values like '"2016-09-08 20:38:41.131652+00"' — quotes retained, because a
# backtick quotechar leaves '"' as an ordinary character. The header line is
# *not* quoted while data rows are, which is what PostgreSQL's
# `COPY ... WITH (FORMAT csv, HEADER, FORCE_QUOTE *)` emits.
#
# Getting this wrong is quiet: only the timestamp columns are validated, so
# every other column would land wrapped in literal quotes, and any field
# containing a comma would be split across columns. `check_bulk_quoting`
# below turns that into a loud failure.
BULK_CSV_QUOTECHAR = '"'

# Quote characters a bulk export might plausibly use, for the parse check.
_PLAUSIBLE_QUOTECHARS = ('"', "`")

_BULK_FILENAME = re.compile(r"^(?P<prefix>.+)-(?P<date>\d{4}-\d{2}-\d{2})\.csv\.bz2$")


def parse_bulk_header_line(line: str) -> list[str]:
    """Parse one bulk-CSV header line into column names."""
    return next(csv.reader([line], delimiter=",", quotechar=BULK_CSV_QUOTECHAR))


def check_bulk_quoting(row: dict[str, Any]) -> None:
    """Raise if ``row`` looks like it was parsed with the wrong quote character.

    A parsed value that still carries matching quotes around it means the
    quotechar we used isn't the one the file was written with — the quotes
    were treated as ordinary characters instead of being stripped. Left
    unchecked that corrupts every text column silently (and splits any field
    containing a comma), so it is worth one cheap look at the first row.
    """
    values = [v for v in row.values() if isinstance(v, str) and len(v) >= 2]
    if len(values) < 2:
        return
    for quotechar in _PLAUSIBLE_QUOTECHARS:
        if quotechar == BULK_CSV_QUOTECHAR:
            continue
        if all(v.startswith(quotechar) and v.endswith(quotechar) for v in values):
            raise ValueError(
                f"Bulk CSV values still carry {quotechar!r} quotes after parsing "
                f"with quotechar {BULK_CSV_QUOTECHAR!r} — the export's quoting has "
                f"changed. Set BULK_CSV_QUOTECHAR to {quotechar!r}. Sample: "
                f"{dict(list(row.items())[:3])}"
            )


def _parse_bucket_listing(xml_bytes: bytes) -> tuple[list[dict[str, Any]], str | None]:
    """Parse one S3 ``ListObjectsV2`` XML page.

    Returns ``(entries, continuation_token)`` where each entry has ``key`` and
    ``size`` and the token is ``None`` on the last page. Tags are matched by
    local name so the S3 XML namespace doesn't matter.
    """
    root = ElementTree.fromstring(xml_bytes)

    def local(tag: str) -> str:
        return tag.rpartition("}")[2]

    entries: list[dict[str, Any]] = []
    token: str | None = None
    truncated = False
    for el in root:
        name = local(el.tag)
        if name == "Contents":
            fields = {local(c.tag): (c.text or "") for c in el}
            entries.append({"key": fields.get("Key", ""), "size": int(fields.get("Size") or 0)})
        elif name == "IsTruncated":
            truncated = (el.text or "").strip().lower() == "true"
        elif name == "NextContinuationToken":
            token = (el.text or "").strip() or None
    return entries, (token if truncated else None)


class CourtListenerClient:
    """Thin ``requests`` wrapper for CourtListener's API v4 and bulk exports.

    Parameters
    ----------
    api_token : str | None
        CourtListener API token. Falls back to the ``COURTLISTENER_API_TOKEN``
        environment variable; ``None`` (anonymous) works at a lower rate limit.
    timeout : int
        Per-request timeout in seconds (bulk listing/downloads can be slow).
    page_size : int | None
        Rows per API page. ``None`` (default) uses the server default; the
        server caps whatever is requested.
    """

    def __init__(
        self,
        api_token: str | None = None,
        timeout: int = 120,
        page_size: int | None = None,
    ):
        self.api_token = api_token or os.environ.get("COURTLISTENER_API_TOKEN")
        self.timeout = timeout
        self.page_size = page_size

        retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "OPTIONS"),
        )

        # API session: carries the token.
        self.session = requests.Session()
        if self.api_token:
            self.session.headers["Authorization"] = f"Token {self.api_token}"

        # Bulk session: public S3 bucket, anonymous by construction. Never give
        # this session credentials — S3 rejects a "Token ..." Authorization header
        # with 400 InvalidArgument and echoes the token back in the error body.
        self.bulk_session = requests.Session()

        for session in (self.session, self.bulk_session):
            session.mount("https://", HTTPAdapter(max_retries=retry))

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> dict:
        """GET ``url`` and return the JSON payload."""
        resp = self.session.get(url, params=params or None, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # -- API v4 ----------------------------------------------------------

    def api_root(self) -> dict[str, str]:
        """The API root: a map of endpoint name → endpoint URL."""
        return self.get_json(f"{API_BASE}/")

    def options(self, endpoint: str) -> dict:
        """The endpoint's OPTIONS metadata (name, description, field info)."""
        url = f"{API_BASE}/{endpoint.strip('/')}/"
        resp = self.session.options(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_page(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        """One API page: ``{"count": ..., "next": url|None, "results": [...]}``."""
        merged = dict(params or {})
        if self.page_size is not None:
            merged.setdefault("page_size", self.page_size)
        return self.get_json(f"{API_BASE}/{endpoint.strip('/')}/", merged)

    def iter_pages(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield pages of ``results``, following ``next`` URLs until exhausted.

        v4 uses cursor pagination: the ``next`` URL carries the cursor (and
        re-encodes the original filters), so ``params`` are sent only on the
        first request.
        """
        payload = self.get_page(endpoint, params)
        while True:
            results = payload.get("results") or []
            if results:
                yield results
            next_url = payload.get("next")
            if not next_url:
                return
            payload = self.get_json(next_url)

    # -- bulk exports ------------------------------------------------------

    def list_bulk_exports(self, file_prefix: str | None = None) -> list[dict[str, Any]]:
        """List available bulk CSV exports, oldest first.

        Each entry: ``{"prefix", "date", "filename", "url", "size"}``.
        ``file_prefix`` filters to one resource's files (exact prefix match);
        ``None`` returns every ``<prefix>-<date>.csv.bz2`` in the bucket.
        Non-CSV artifacts (e.g. ``schema-<date>.sql``) are skipped.
        """
        params: dict[str, str] = {
            "list-type": "2",
            "prefix": f"{BULK_KEY_PREFIX}{file_prefix or ''}",
        }
        keys: list[dict[str, Any]] = []
        token: str | None = None
        while True:
            page_params = dict(params)
            if token:
                page_params["continuation-token"] = token
            resp = self.bulk_session.get(BULK_STORAGE_URL, params=page_params, timeout=self.timeout)
            resp.raise_for_status()
            entries, token = _parse_bucket_listing(resp.content)
            keys.extend(entries)
            if token is None:
                break

        exports = []
        for entry in keys:
            filename = entry["key"].rpartition("/")[2]
            match = _BULK_FILENAME.match(filename)
            if not match:
                continue
            if file_prefix is not None and match["prefix"] != file_prefix:
                continue
            exports.append(
                {
                    "prefix": match["prefix"],
                    "date": match["date"],
                    "filename": filename,
                    "url": f"{BULK_STORAGE_URL}{entry['key']}",
                    "size": entry["size"],
                }
            )
        exports.sort(key=lambda e: (e["prefix"], e["date"]))
        return exports

    def download_bulk(self, url: str, dest_path: str | Path) -> Path:
        """Stream a bulk export to ``dest_path`` and return the path."""
        dest_path = Path(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with self.bulk_session.get(url, stream=True, timeout=self.timeout) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
        return dest_path

    def read_bulk_header(self, url: str) -> list[str]:
        """Return a bulk export's column names from its CSV header line.

        Streams and bz2-decompresses only until the first newline, then closes
        the connection — so schema discovery never downloads a multi-GB file.
        (Header lines are plain column identifiers, so an embedded newline
        inside a quoted header field is not a concern.)
        """
        decompressor = bz2.BZ2Decompressor()
        buffer = b""
        with self.bulk_session.get(url, stream=True, timeout=self.timeout) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=1 << 16):
                buffer += decompressor.decompress(chunk)
                if b"\n" in buffer:
                    break
        if b"\n" not in buffer:
            raise ValueError(f"No header line found in bulk export at {url}")
        return parse_bulk_header_line(buffer.split(b"\n", 1)[0].decode("utf-8"))
