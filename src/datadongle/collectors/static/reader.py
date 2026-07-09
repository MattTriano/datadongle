"""StaticFileReader — the static-file source adapter for the shared driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the static
``client``/``spec``. It replaces the per-file download/parse/ingest logic that
used to live in ``StaticFileCollector``; the family-level orchestration (the
per-file fan-out, the per-file write session, error isolation) lives in
:func:`run_static_collection` in ``driver.py``.

This is the most generic collector: a spec is just a manifest of file URLs that
all land in one table, so there is no ``metadata.py`` (the manifest *is* the
metadata) and no ``cursor_spec`` (static files have no row cursor). A reader
handles **one file at a time**: the family driver narrows a multi-file spec to a
single ``FileRef`` before calling any reader method.

Two things shape this reader:

  - **Everything is text.** Source columns and ``vintage`` are all ``TEXT`` — a
    deliberate choice so identifier columns keep their leading zeros; typing is a
    downstream concern. The schema is discovered from the first file's header.
  - **Immutable-ish, full-refresh.** ``cursor_spec`` is ``None`` (always a full
    read); with an ``entity_key`` SCD2 makes re-collection a no-op merge, so an
    in-place publisher revision versions only the rows that changed.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import url2pathname

from datadongle.collectors.static.client import StaticFileClient
from datadongle.collectors.static.spec import FileRef, StaticFileDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, Append, WriteMode

logger = logging.getLogger(__name__)

_BATCH_SIZE = 5000


class StaticFileReader:
    """Adapts one static file to the shared collection driver."""

    source = "static_file"

    def __init__(self, client: StaticFileClient | None = None) -> None:
        self._client = client

    @property
    def client(self) -> StaticFileClient:
        if self._client is None:
            self._client = StaticFileClient()
        return self._client

    @staticmethod
    def _file(spec: StaticFileDatasetSpec) -> FileRef:
        """The single file this call operates on (spec is narrowed by the driver)."""
        return spec.files[0]

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: StaticFileDatasetSpec) -> str:
        return f"{spec.target_table}/{self._file(spec).vintage}"

    def target(self, spec: StaticFileDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: StaticFileDatasetSpec) -> TableSchema:
        """All source columns (from the first file's header) + vintage, all TEXT.

        Every value is stored as text so identifier columns keep leading zeros;
        the manifest's files must share a column layout, so one file's header is
        the table's schema."""
        columns = [Column(name, ColumnType.TEXT) for name in self._header(spec.files[0])]
        columns.append(Column("vintage", ColumnType.TEXT, nullable=False))
        return TableSchema(columns=columns)

    def write_mode(self, spec: StaticFileDatasetSpec, *, mode: str = "full") -> WriteMode:
        if spec.entity_key:
            return SCD2(entity_key=spec.entity_key)
        return Append()

    def cursor_spec(self, spec: StaticFileDatasetSpec) -> CursorSpec | None:
        # Static files have no row cursor — the driver always runs a full read.
        return None

    def read(
        self, spec: StaticFileDatasetSpec, *, since: Cursor | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        # ``since`` is always None (cursor_spec is None). ``spec`` is narrowed to
        # one file; download it (or read a file:// path in place), parse, and
        # stream batches, stamping the vintage.
        file_ref = self._file(spec)
        path, is_temp = self._resolve_path(file_ref.url)
        try:
            batch: list[dict[str, Any]] = []
            for row in self.client.parse_file(path, file_ref):
                row["vintage"] = file_ref.vintage
                batch.append(row)
                if len(batch) >= _BATCH_SIZE:
                    yield batch
                    batch = []
            if batch:
                yield batch
        finally:
            if is_temp:
                path.unlink(missing_ok=True)

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _header(self, file_ref: FileRef) -> list[str]:
        """The sanitized column names of a file, from its first row."""
        path, is_temp = self._resolve_path(file_ref.url)
        try:
            first = next(self.client.parse_file(path, file_ref), None)
            if first is None:
                raise ValueError(f"Could not read any rows from {file_ref.url} to derive columns")
            return list(first.keys())
        finally:
            if is_temp:
                path.unlink(missing_ok=True)

    def _resolve_path(self, url: str) -> tuple[Path, bool]:
        """Return ``(local_path, is_temp)``. ``file://`` reads in place (never
        deleted); ``https`` streams to a temp file the caller must delete."""
        parsed = urlparse(url)
        if parsed.scheme == "file":
            return Path(url2pathname(parsed.path)), False
        return self.client.download_to_tempfile(url), True
