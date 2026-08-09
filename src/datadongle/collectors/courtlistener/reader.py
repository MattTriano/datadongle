"""CourtListenerReader — the CourtListener source adapter for the shared driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the
CourtListener ``client``/``spec``. One reader serves any resource: a full read
(``since=None``) uses the spec's ``backfill`` path — the monthly bulk CSV
export or an API walk — and an incremental read always pages the API filtered
by ``date_modified``. Both paths land in the same target table.

Three things about CourtListener shape this reader:

  - **The bulk CSV column set is the canonical schema.** Bulk files are raw
    database dumps (foreign keys as ``court_id``, no computed fields), while
    the API decorates rows with hyperlinks (``"court": "https://.../courts/
    scotus/"``) and computed fields (``absolute_url``, ``resource_uri``). API
    rows are therefore normalized *toward* the bulk shape: detail-URL values
    are collapsed to their trailing key and the field renamed ``<field>_id``,
    computed fields are dropped, nested lists/objects are JSON-encoded, and
    with ``backfill="bulk"`` rows are projected onto the bulk header columns.

  - **Everything lands as TEXT.** Bulk CSVs are untyped strings and the two
    retrieval paths must produce comparable SCD2 content, so API scalars are
    stringified (bools as ``"true"``/``"false"``) and casting is a downstream
    concern. The exception that matters is **timestamps**: ``date_created``/
    ``date_modified`` are canonicalized to ``YYYY-MM-DD HH:MM:SS.ffffff+00:00``
    in both paths, so the ``date_modified`` high-water mark read back from a
    TEXT column is lexicographically correct across bulk- and API-sourced rows.

  - **Incremental cursor is ``date_modified`` with an ``id`` tiebreak.** The
    API is asked for ``date_modified__gte=<hwm>`` ordered by
    ``(date_modified, id)``, and rows are filtered strictly-after client-side
    (numeric-aware id comparison, since ids land as text). Because increments
    only return rows that actually changed, representation differences between
    a bulk seed and API updates never create spurious SCD2 versions.

**Caveat (same-table bulk + API):** bulk columns the API does not return land
as NULL in post-backfill versions of a changed row, and a *full API* re-pull
over a bulk-seeded table would re-version every row whose bulk formatting
differs from the API's. Stick to bulk-full + API-incremental for big tables.
"""

from __future__ import annotations

import bz2
import csv
import json
import logging
import re
import tempfile
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from datadongle.collectors.courtlistener.client import (
    BULK_CSV_QUOTECHAR,
    CourtListenerClient,
)
from datadongle.collectors.courtlistener.spec import CourtListenerDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, Append, WriteMode

logger = logging.getLogger(__name__)

# Rows per batch when parsing a bulk CSV.
BULK_BATCH_SIZE = 10_000

# Every CourtListener table carries this source-timestamp pair; both are
# bookkeeping (metadata=True) and both are canonicalized (see _normalize_ts).
TS_COLUMNS = ("date_created", "date_modified")

# API-computed fields with no bulk-table counterpart; always dropped.
DROP_FIELDS = {"resource_uri", "absolute_url"}

# The default cursor column; extract_cursor works batch-only (no spec arg in
# the protocol), so it reads this column and returns None where it's absent.
CURSOR_COLUMN = "date_modified"

# A v4 detail URL: ".../api/rest/v4/<endpoint>/<key>/". The trailing key is
# the related row's primary key (integer for most tables, a slug for courts).
_DETAIL_URL = re.compile(r"^https?://\S+/api/rest/v\d+/[a-z0-9-]+/(?P<key>[^/?#\s]+)/$")


def normalize_timestamp(value: str, column: str) -> str:
    """Canonicalize an ISO-ish timestamp to ``YYYY-MM-DD HH:MM:SS[.ffffff]+00:00``.

    Bulk dumps render timestamps as ``"2024-05-06 12:34:56.789+00"`` and the
    API as ``"2024-05-06T12:34:56.789000Z"``; both must land identically so
    the TEXT high-water mark orders correctly. Fails loud on unparseable
    input — a silent passthrough would corrupt incremental cursors.
    """
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat(sep=" ")
    except ValueError:
        raise ValueError(
            f"CourtListener column {column!r} has value {value!r} that does not "
            f"parse as an ISO timestamp; refusing to guess."
        ) from None


def _tie_key(value: str | None) -> tuple[int, Any]:
    """Comparable key for the ``id`` tiebreak: numeric where possible.

    Ids land as TEXT, but most CourtListener ids are integers — comparing
    ``"100" > "99"`` as strings would be wrong. Courts use slug ids, so
    non-numeric values fall back to string ordering.
    """
    if value is None:
        return (-1, "")
    text = str(value)
    return (0, int(text)) if text.isdigit() else (1, text)


class CourtListenerReader:
    """Adapts one CourtListener resource to the shared collection driver."""

    source = "courtlistener"

    def __init__(
        self,
        api_token: str | None = None,
        timeout: int = 120,
        client: CourtListenerClient | None = None,
    ) -> None:
        self.api_token = api_token
        self.timeout = timeout
        self._client = client
        # dataset_id -> schema column names, so an incremental run's read()
        # doesn't repeat the header-peek/sample the driver's schema() call did.
        self._columns_cache: dict[str, list[str]] = {}
        # (prefix, pinned-date) -> resolved bulk export entry.
        self._export_cache: dict[tuple[str, str | None], dict[str, Any]] = {}

    @property
    def client(self) -> CourtListenerClient:
        if self._client is None:
            self._client = CourtListenerClient(api_token=self.api_token, timeout=self.timeout)
        return self._client

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: CourtListenerDatasetSpec) -> str:
        return spec.dataset_id

    def target(self, spec: CourtListenerDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: CourtListenerDatasetSpec) -> TableSchema:
        """The target's columns — all TEXT (see module docstring).

        ``backfill="bulk"``: the bulk export's CSV header (a cheap streamed
        peek, not a download) is the canonical column set. ``backfill="api"``:
        columns come from one transformed sample row. ``id`` is non-nullable;
        the source-timestamp pair is flagged ``metadata=True`` so a re-pull
        that only bumps ``date_modified`` doesn't create a spurious version.
        """
        names = self._column_names(spec)
        return TableSchema(
            columns=[
                Column(
                    name,
                    ColumnType.TEXT,
                    nullable=name != "id",
                    metadata=name in TS_COLUMNS,
                )
                for name in names
            ]
        )

    def write_mode(self, spec: CourtListenerDatasetSpec, *, mode: str = "incremental") -> WriteMode:
        # entity_key (default ["id"]) ⇒ versioned history; None ⇒ append.
        if spec.entity_key:
            return SCD2(entity_key=spec.entity_key)
        return Append()

    def cursor_spec(self, spec: CourtListenerDatasetSpec) -> CursorSpec | None:
        if spec.cursor_column is None:
            return None
        return CursorSpec(column=spec.cursor_column, tiebreak="id")

    def read(
        self, spec: CourtListenerDatasetSpec, *, since: Cursor | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield ready-to-stage batches.

        ``since=None`` (full read) uses the spec's backfill path; otherwise
        the API is paged from the high-water mark (``__gte`` server-side to
        keep equal-timestamp/higher-id rows reachable, strictly-after
        client-side so the boundary row itself is not re-emitted).
        """
        if since is None:
            if spec.backfill == "bulk":
                yield from self._read_bulk(spec)
            else:
                yield from self._read_api(spec, params=dict(spec.filters), since=None)
            return

        cursor_column = spec.cursor_column or CURSOR_COLUMN
        params = dict(spec.filters)
        params[f"{cursor_column}__gte"] = since.value
        yield from self._read_api(spec, params=params, since=since)

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        """The max ``(date_modified, id)`` in an already-transformed batch."""
        rows = [r for r in batch if r.get(CURSOR_COLUMN) is not None]
        if not rows:
            return None
        best = max(rows, key=lambda r: (r[CURSOR_COLUMN], _tie_key(r.get("id"))))
        tiebreak = best.get("id")
        return Cursor(
            value=best[CURSOR_COLUMN],
            tiebreak=str(tiebreak) if tiebreak is not None else None,
        )

    # ------------------------------------------------------------------
    # Bulk path
    # ------------------------------------------------------------------

    def _read_bulk(self, spec: CourtListenerDatasetSpec) -> Iterator[list[dict[str, Any]]]:
        """Download the bulk export to a temp file, stream-parse, delete."""
        export = self._bulk_export(spec)
        tmp = tempfile.NamedTemporaryFile(suffix=".csv.bz2", prefix="courtlistener_", delete=False)
        tmp.close()
        filepath = Path(tmp.name)
        try:
            logger.info("Downloading %s (%s bytes)", export["url"], export["size"])
            self.client.download_bulk(export["url"], filepath)
            yield from self._parse_bulk_csv(filepath)
        finally:
            filepath.unlink(missing_ok=True)

    @staticmethod
    def _parse_bulk_csv(filepath: Path) -> Iterator[list[dict[str, Any]]]:
        """Stream-parse a bz2 CSV into batches, decompressing on the fly."""
        # Opinion-text fields run to megabytes; lift the csv module's default
        # 128 KiB field cap (bounded so it fits a C long on every platform).
        csv.field_size_limit(int(2**31 - 1))
        batch: list[dict[str, Any]] = []
        with bz2.open(filepath, mode="rt", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=",", quotechar=BULK_CSV_QUOTECHAR)
            for row in reader:
                clean = {k: (v if v != "" else None) for k, v in row.items()}
                for column in TS_COLUMNS:
                    value = clean.get(column)
                    if value:
                        clean[column] = normalize_timestamp(value, column)
                batch.append(clean)
                if len(batch) >= BULK_BATCH_SIZE:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _bulk_export(self, spec: CourtListenerDatasetSpec) -> dict[str, Any]:
        """Resolve the spec's bulk export (pinned date, else latest). Cached."""
        cache_key = (spec.file_prefix, spec.bulk_date)
        if cache_key not in self._export_cache:
            exports = self.client.list_bulk_exports(spec.file_prefix)
            if spec.bulk_date is not None:
                exports = [e for e in exports if e["date"] == spec.bulk_date]
            if not exports:
                raise ValueError(
                    f"No bulk export found for prefix {spec.file_prefix!r}"
                    + (f" dated {spec.bulk_date}" if spec.bulk_date else "")
                    + ". Use CourtListenerMetadata.bulk_exports() to see what exists."
                )
            self._export_cache[cache_key] = max(exports, key=lambda e: e["date"])
        return self._export_cache[cache_key]

    # ------------------------------------------------------------------
    # API path
    # ------------------------------------------------------------------

    def _read_api(
        self,
        spec: CourtListenerDatasetSpec,
        *,
        params: dict[str, Any],
        since: Cursor | None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Page the API, transform rows, project onto the schema, filter."""
        cursor_column = spec.cursor_column
        if since is not None and cursor_column is not None:
            # Stable (cursor, tiebreak) ordering, per the collector interface.
            params["order_by"] = f"{cursor_column},id"
        else:
            params["order_by"] = "id"

        columns = self._column_names(spec)
        for page in self.client.iter_pages(spec.endpoint, params):
            batch = [self._project(self._transform_api_row(row), columns) for row in page]
            if since is not None and cursor_column is not None:
                batch = [r for r in batch if self._is_after(r, cursor_column, since)]
            if batch:
                yield batch

    def _transform_api_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Normalize one API row toward the bulk-CSV column shape.

        Detail-URL values collapse to their trailing key under ``<field>_id``
        (skipped when the API already provides that sibling, as v4 does for
        e.g. ``court``/``court_id``); computed fields are dropped; nested
        lists/objects are JSON-encoded; scalars are stringified.
        """
        keys = set(row)
        out: dict[str, Any] = {}
        for key, value in row.items():
            if key in DROP_FIELDS:
                continue
            if isinstance(value, str):
                match = _DETAIL_URL.match(value)
                if match and not key.endswith("_id"):
                    fk_name = f"{key}_id"
                    if fk_name in keys:
                        continue  # the sibling *_id field carries the value
                    out[fk_name] = match["key"]
                    continue
                out[key] = value
            elif value is None:
                out[key] = None
            elif isinstance(value, bool):
                out[key] = "true" if value else "false"
            elif isinstance(value, (int, float)):
                out[key] = str(value)
            else:  # list / dict
                out[key] = json.dumps(value)
        for column in TS_COLUMNS:
            if out.get(column):
                out[column] = normalize_timestamp(out[column], column)
        return out

    @staticmethod
    def _project(row: dict[str, Any], columns: list[str]) -> dict[str, Any]:
        """Keep exactly the schema's columns (missing ⇒ None, extras dropped)."""
        return {c: row.get(c) for c in columns}

    @staticmethod
    def _is_after(row: dict[str, Any], cursor_column: str, since: Cursor) -> bool:
        """Strictly-after test on ``(cursor, id)`` (rows with no cursor kept)."""
        value = row.get(cursor_column)
        if value is None:
            return True
        if value != since.value:
            return value > since.value
        if since.tiebreak is None:
            return False
        return _tie_key(row.get("id")) > _tie_key(since.tiebreak)

    # ------------------------------------------------------------------
    # Column discovery
    # ------------------------------------------------------------------

    def _column_names(self, spec: CourtListenerDatasetSpec) -> list[str]:
        """The target's column names, per the spec's backfill path. Cached."""
        cache_key = spec.dataset_id
        if cache_key not in self._columns_cache:
            if spec.backfill == "bulk":
                names = self.client.read_bulk_header(self._bulk_export(spec)["url"])
            else:
                page = self.client.get_page(spec.endpoint, dict(spec.filters))
                results = page.get("results") or []
                if not results:
                    raise ValueError(
                        f"CourtListener {spec.dataset_id!r} returned no rows for the "
                        f"given filters; its schema cannot be discovered."
                    )
                names = list(self._transform_api_row(results[0]))
            self._columns_cache[cache_key] = names
        return self._columns_cache[cache_key]
