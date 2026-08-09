"""Fakes and test-data helpers for the CourtListener collector suite.

``FakeCourtListenerClient`` overrides the HTTP boundary (``get_json`` plus the
bulk methods), serving canned JSON and in-memory bz2 CSV bytes. Everything
above it — cursor-pagination in ``iter_pages``, the reader's transforms,
projection, and cursor filtering — runs the real code with no network. The
fake emulates the server-side behaviors the reader relies on: ``__gte``
filtering, ``order_by``, and ``next``-URL cursor pagination.
"""

from __future__ import annotations

import bz2
import csv
import io
from typing import Any, cast
from urllib.parse import parse_qsl, urlencode, urlsplit

from datadongle.collectors.courtlistener.client import (
    BULK_CSV_QUOTECHAR,
    CourtListenerClient,
    parse_bulk_header_line,
)
from datadongle.collectors.courtlistener.reader import (
    CourtListenerReader,
    _tie_key,
    normalize_timestamp,
)
from datadongle.collectors.courtlistener.spec import CourtListenerDatasetSpec

API = "https://www.courtlistener.com/api/rest/v4"

API_ROOT = {
    "courts": f"{API}/courts/",
    "dockets": f"{API}/dockets/",
    "opinions": f"{API}/opinions/",
}

# A dockets-shaped bulk export: the raw DB column set (court_id, no computed
# fields), Postgres-style timestamps, and values exercising the backtick
# quoting (embedded commas and double-quotes) plus empty-string → None.
BULK_HEADER = ["id", "date_created", "date_modified", "court_id", "case_name", "pacer_case_id"]
BULK_ROWS = [
    {
        "id": "1",
        "date_created": "2024-01-01 08:00:00+00",
        "date_modified": "2024-01-01 08:00:00+00",
        "court_id": "scotus",
        "case_name": 'In re "Complex" Litig., Inc.',
        "pacer_case_id": "P1",
    },
    {
        "id": "2",
        "date_created": "2024-01-02 08:00:00+00",
        "date_modified": "2024-01-02 08:00:00+00",
        "court_id": "ca9",
        "case_name": "Roe v. Wade",
        "pacer_case_id": "P2",
    },
    {
        "id": "3",
        "date_created": "2024-01-03 08:00:00+00",
        "date_modified": "2024-01-03 08:00:00+00",
        "court_id": "ca9",
        "case_name": "Smith v. Jones",
        "pacer_case_id": "",  # ⇒ None
    },
]

BULK_URL = (
    "https://com-courtlistener-storage.s3-us-west-2.amazonaws.com"
    "/bulk-data/dockets-2024-01-31.csv.bz2"
)
BULK_EXPORTS = [
    {
        "prefix": "dockets",
        "date": "2024-01-31",
        "filename": "dockets-2024-01-31.csv.bz2",
        "url": BULK_URL,
        "size": 1234,
    },
]

# API-shaped rows for the incremental path: hyperlinked FKs, computed fields,
# a nested list, ISO-T/Z timestamps. Docket 2 is a post-backfill update,
# docket 4 is new, docket 1 predates the high-water mark.
API_ROWS = [
    {
        "resource_uri": f"{API}/dockets/1/",
        "id": 1,
        "court": f"{API}/courts/scotus/",
        "absolute_url": "/docket/1/in-re-complex/",
        "case_name": 'In re "Complex" Litig., Inc.',
        "pacer_case_id": "P1",
        "date_created": "2024-01-01T08:00:00Z",
        "date_modified": "2024-01-01T08:00:00Z",
        "parties": [],
    },
    {
        "resource_uri": f"{API}/dockets/2/",
        "id": 2,
        "court": f"{API}/courts/ca9/",
        "absolute_url": "/docket/2/roe/",
        "case_name": "Roe v. Wade (amended)",
        "pacer_case_id": "P2",
        "date_created": "2024-01-02T08:00:00Z",
        "date_modified": "2024-02-01T09:30:00Z",
        "parties": [11, 12],
    },
    {
        "resource_uri": f"{API}/dockets/4/",
        "id": 4,
        "court": f"{API}/courts/scotus/",
        "absolute_url": "/docket/4/new/",
        "case_name": "New v. Docket",
        "pacer_case_id": None,
        "date_created": "2024-02-02T10:00:00Z",
        "date_modified": "2024-02-02T10:00:00Z",
        "parties": [],
    },
]

OPTIONS_PAYLOAD = {
    "name": "Dockets",
    "description": "A docket in a court of law.",
    "actions": {
        "POST": {
            "id": {"type": "integer", "required": False, "help_text": "primary key"},
            "case_name": {"type": "string", "required": False, "help_text": "case name"},
        }
    },
}


def make_bulk_bz2(rows: list[dict[str, Any]], columns: list[str] | None = None) -> bytes:
    """Render rows as a backtick-quoted CSV and bz2-compress it."""
    columns = columns or BULK_HEADER
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=columns,
        delimiter=",",
        quotechar=BULK_CSV_QUOTECHAR,
        quoting=csv.QUOTE_MINIMAL,
    )
    writer.writeheader()
    writer.writerows(rows)
    return bz2.compress(buffer.getvalue().encode("utf-8"))


class FakeCourtListenerClient(CourtListenerClient):
    """Serves canned API pages and bulk bytes; records requests it received."""

    def __init__(
        self,
        api_rows: dict[str, list[dict]] | None = None,
        bulk_files: dict[str, bytes] | None = None,
        exports: list[dict] | None = None,
        root: dict[str, str] | None = None,
        options_payload: dict | None = None,
        fake_page_size: int = 2,
    ):
        super().__init__(api_token="test-token")
        self.api_rows = api_rows if api_rows is not None else {"dockets": list(API_ROWS)}
        self.bulk_files = (
            bulk_files if bulk_files is not None else {BULK_URL: make_bulk_bz2(BULK_ROWS)}
        )
        self.exports = exports if exports is not None else list(BULK_EXPORTS)
        self.root = root or dict(API_ROOT)
        self.options_payload = options_payload or dict(OPTIONS_PAYLOAD)
        self.fake_page_size = fake_page_size
        self.calls: list[tuple[str, dict]] = []

    # -- HTTP boundary ---------------------------------------------------

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> dict:
        self.calls.append((url, dict(params or {})))
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query))
        query.update({k: str(v) for k, v in (params or {}).items()})

        path = parts.path.rstrip("/")
        if path.endswith("/v4"):
            return self.root
        endpoint = path.rpartition("/")[2]
        rows = self._filtered(list(self.api_rows.get(endpoint, [])), query)

        offset = int(query.get("cursor", 0))
        page = rows[offset : offset + self.fake_page_size]
        remaining = offset + len(page) < len(rows)
        base = f"{parts.scheme}://{parts.netloc}{parts.path}"
        next_query = {k: v for k, v in query.items() if k != "cursor"}
        next_query["cursor"] = str(offset + len(page))
        return {
            "count": len(rows),
            "next": f"{base}?{urlencode(next_query)}" if remaining else None,
            "previous": None,
            "results": page,
        }

    def _filtered(self, rows: list[dict], query: dict[str, str]) -> list[dict]:
        """Emulate server-side __gte filters, equality filters, and order_by."""
        for key, wanted in query.items():
            if key in ("cursor", "order_by", "page_size"):
                continue
            if key.endswith("__gte"):
                field = key[: -len("__gte")]
                rows = [r for r in rows if self._comparable(r.get(field)) >= wanted]
            elif "__" not in key:
                rows = [r for r in rows if str(r.get(key)) == wanted]
        order_by = query.get("order_by", "id")
        for field in reversed(order_by.split(",")):
            if field in ("date_created", "date_modified"):
                rows.sort(key=lambda r, f=field: self._comparable(r.get(f)))
            else:
                rows.sort(key=lambda r, f=field: _tie_key(r.get(f)))
        return rows

    @staticmethod
    def _comparable(value: Any) -> str:
        """Timestamps in canonical form so __gte compares across formats."""
        if value is None:
            return ""
        try:
            return normalize_timestamp(str(value), "fake")
        except ValueError:
            return str(value)

    # -- endpoint metadata / bulk ------------------------------------------

    def options(self, endpoint: str) -> dict:
        self.calls.append((f"OPTIONS {endpoint}", {}))
        return self.options_payload

    def list_bulk_exports(self, file_prefix: str | None = None) -> list[dict]:
        self.calls.append(("list_bulk_exports", {"prefix": file_prefix}))
        if file_prefix is None:
            return list(self.exports)
        return [e for e in self.exports if e["prefix"] == file_prefix]

    def download_bulk(self, url: str, dest_path):
        from pathlib import Path

        dest = Path(dest_path)
        dest.write_bytes(self.bulk_files[url])
        return dest

    def read_bulk_header(self, url: str) -> list[str]:
        first_line = bz2.decompress(self.bulk_files[url]).decode("utf-8").split("\n", 1)[0]
        return parse_bulk_header_line(first_line)


def fake_client(reader: CourtListenerReader) -> FakeCourtListenerClient:
    """The fake behind a reader, typed so its canned data is visible."""
    return cast(FakeCourtListenerClient, reader.client)


def make_spec(**overrides) -> CourtListenerDatasetSpec:
    """Build a dockets spec with sensible defaults."""
    defaults: dict[str, Any] = dict(
        name="test_cl_dockets",
        target_table="test_cl_dockets",
        resource="dockets",
        backfill="bulk",
    )
    defaults.update(overrides)
    return CourtListenerDatasetSpec(**defaults)
