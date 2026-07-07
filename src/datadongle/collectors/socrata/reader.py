"""SocrataReader — the Socrata source adapter for the shared collection driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the
existing Socrata ``client``/``metadata``/``spec``. It replaces the
mode-dispatch logic that used to live in ``SocrataCollector.collect``: the
driver decides full vs. incremental, and this reader supplies the schema,
write policy, incremental cursor, and the batch stream (applying Socrata's
system-field rename, computed-region drop, and location→EWKT transforms).

Two source modes, both handled here:
  - ``api``:           paginated SODA, incrementally queryable via the
                       ``:updated_at`` / ``:id`` cursor.
  - ``file_download``: a bulk CSV/GeoJSON export. Not incrementally queryable
                       (``cursor_spec`` is ``None``), so the driver always
                       runs a full read.
"""

from __future__ import annotations

import json
import logging
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from datadongle.collectors.socrata.client import SocrataClient
from datadongle.collectors.socrata.metadata import SocrataTableMetadata
from datadongle.collectors.socrata.spec import SocrataDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema
from datadongle.core.write_mode import SCD2, Append, WriteMode

logger = logging.getLogger(__name__)

# Socrata system fields (``:id`` etc.) renamed to plain, storable columns.
SYSTEM_FIELD_RENAMES = {
    ":id": "socrata_id",
    ":updated_at": "socrata_updated_at",
    ":created_at": "socrata_created_at",
    ":version": "socrata_version",
}

# The renamed cursor columns for the API path.
CURSOR_COLUMN = "socrata_updated_at"
TIEBREAK_COLUMN = "socrata_id"

# The socrata_* system columns present on every target table. They are stored
# but marked ``metadata=True`` so they never enter the SCD2 content hash.
_SYSTEM_COLUMNS = [
    Column("socrata_id", ColumnType.TEXT, metadata=True),
    Column("socrata_updated_at", ColumnType.TIMESTAMPTZ, metadata=True),
    Column("socrata_created_at", ColumnType.TIMESTAMPTZ, metadata=True),
    Column("socrata_version", ColumnType.TEXT, metadata=True),
]

# Socrata datatype -> neutral ColumnType.
_SOCRATA_TO_COLUMN_TYPE: dict[str, ColumnType] = {
    "text": ColumnType.TEXT,
    "url": ColumnType.TEXT,
    "blob": ColumnType.TEXT,
    "photo": ColumnType.TEXT,
    "document": ColumnType.TEXT,
    "html": ColumnType.TEXT,
    "email": ColumnType.TEXT,
    "phone": ColumnType.TEXT,
    "number": ColumnType.NUMERIC,
    "percent": ColumnType.NUMERIC,
    "money": ColumnType.NUMERIC,
    "double": ColumnType.DOUBLE,
    "checkbox": ColumnType.BOOLEAN,
    "calendar_date": ColumnType.TIMESTAMPTZ,
    "date": ColumnType.TIMESTAMPTZ,
    "fixed_timestamp": ColumnType.TIMESTAMPTZ,
    "floating_timestamp": ColumnType.TIMESTAMP,
}

# Socrata geospatial datatype -> OGC geometry kind.
_SOCRATA_GEOMETRY_KIND: dict[str, str] = {
    "point": "Point",
    "location": "Point",
    "multipoint": "MultiPoint",
    "line": "LineString",
    "multiline": "MultiLineString",
    "polygon": "Polygon",
    "multipolygon": "MultiPolygon",
}


class SocrataReader:
    """Adapts a Socrata dataset to the shared collection driver."""

    source = "socrata"

    def __init__(
        self,
        app_token: str | None = None,
        page_size: int = 25000,
    ) -> None:
        self.app_token = app_token
        self.page_size = page_size
        self._client: SocrataClient | None = None
        self._metadata_cache: dict[str, SocrataTableMetadata] = {}

    @property
    def client(self) -> SocrataClient:
        if self._client is None:
            self._client = SocrataClient(app_token=self.app_token, page_size=self.page_size)
        return self._client

    def _metadata(self, dataset_id: str) -> SocrataTableMetadata:
        if dataset_id not in self._metadata_cache:
            self._metadata_cache[dataset_id] = SocrataTableMetadata(dataset_id)
        return self._metadata_cache[dataset_id]

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: SocrataDatasetSpec) -> str:
        return spec.dataset_id

    def target(self, spec: SocrataDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: SocrataDatasetSpec) -> TableSchema:
        """Neutral schema: source columns + the socrata_* metadata columns."""
        columns = [self._column_for(c) for c in self._metadata(spec.dataset_id).columns]
        columns.extend(_SYSTEM_COLUMNS)
        return TableSchema(columns=columns)

    @staticmethod
    def _column_for(col) -> Column:
        kind = _SOCRATA_GEOMETRY_KIND.get(col.datatype)
        if kind is not None:
            return Column(
                col.field_name,
                ColumnType.GEOMETRY,
                geometry=GeometrySpec(kind=kind, srid=4326),
            )
        col_type = _SOCRATA_TO_COLUMN_TYPE.get(col.datatype, ColumnType.TEXT)
        return Column(col.field_name, col_type)

    def write_mode(self, spec: SocrataDatasetSpec, *, mode: str = "incremental") -> WriteMode:
        # Socrata's policy doesn't depend on the collection mode.
        if spec.entity_key:
            return SCD2(entity_key=spec.entity_key)
        return Append()

    def cursor_spec(self, spec: SocrataDatasetSpec) -> CursorSpec | None:
        # File exports carry no system fields and can't be filtered — the
        # driver falls back to a full read.
        if spec.full_update_mode == "file_download":
            return None
        column = SYSTEM_FIELD_RENAMES.get(spec.incremental_column, spec.incremental_column)
        return CursorSpec(column=column, tiebreak=TIEBREAK_COLUMN)

    def read(
        self, spec: SocrataDatasetSpec, *, since: Cursor | None
    ) -> Iterator[list[dict[str, Any]]]:
        if spec.full_update_mode == "file_download":
            yield from self._read_file(spec)
        else:
            yield from self._read_api(spec, since)

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        """Max (socrata_updated_at, socrata_id) in an already-renamed batch."""
        values = [
            (row[CURSOR_COLUMN], str(row.get(TIEBREAK_COLUMN) or ""))
            for row in batch
            if row.get(CURSOR_COLUMN) is not None
        ]
        if not values:
            return None
        best = max(values, key=lambda x: (x[0], x[1]))
        return Cursor(value=str(best[0]), tiebreak=best[1] or None)

    # ------------------------------------------------------------------
    # API path
    # ------------------------------------------------------------------

    def _read_api(
        self, spec: SocrataDatasetSpec, since: Cursor | None
    ) -> Iterator[list[dict[str, Any]]]:
        meta = self._metadata(spec.dataset_id)
        inc_col = spec.incremental_column
        where = self._build_where(inc_col, since)
        order_by = f"{inc_col}, :id"
        location_columns = self._location_columns(spec.dataset_id)

        for batch in self.client.paginate(
            domain=meta.domain,
            dataset_id=spec.dataset_id,
            where=where,
            order_by=order_by,
            include_system_fields=True,
        ):
            if location_columns:
                self._convert_location_fields(batch, location_columns)
            batch = self._rename_system_fields(batch)
            batch = self._drop_computed_region_columns(batch)
            yield batch

    @staticmethod
    def _build_where(inc_col: str, since: Cursor | None) -> str | None:
        """SoQL filter for rows strictly after ``since`` (cursor + tiebreak)."""
        if since is None:
            return None
        if since.tiebreak:
            return (
                f"({inc_col} = '{since.value}' AND :id > '{since.tiebreak}') "
                f"OR ({inc_col} > '{since.value}')"
            )
        return f"{inc_col} > '{since.value}'"

    # ------------------------------------------------------------------
    # File-download path
    # ------------------------------------------------------------------

    def _read_file(self, spec: SocrataDatasetSpec) -> Iterator[list[dict[str, Any]]]:
        from datadongle.parsers.csv_parser import parse_csv
        from datadongle.parsers.geojson import parse_geojson

        meta = self._metadata(spec.dataset_id)
        download_format = meta.download_format
        now_utc = datetime.now(UTC).isoformat()
        location_columns = self._location_columns(spec.dataset_id)

        filepath = self._download_to_tempfile(meta.data_download_url, download_format)
        try:
            if download_format == "GeoJSON":
                batches, _result = parse_geojson(
                    filepath, geometry_column=self._geometry_column_name(spec.dataset_id)
                )
            else:
                batches = parse_csv(filepath)

            for batch in batches:
                batch = self._rename_file_columns(batch, spec.dataset_id)
                batch = self._add_system_field_defaults(batch, now_utc)
                if location_columns:
                    self._convert_location_fields(batch, location_columns)
                yield batch
        finally:
            filepath.unlink(missing_ok=True)

    @retry(
        retry=retry_if_exception_type((ChunkedEncodingError, ConnectionError, ReadTimeout)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=30, max=300),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _download_to_tempfile(self, url: str, download_format: str) -> Path:
        suffix = ".geojson" if download_format == "GeoJSON" else ".csv"
        logger.info("Downloading %s", url)
        resp = self.client._session.get(url, stream=True, timeout=600)
        resp.raise_for_status()

        tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix="socrata_", delete=False)
        try:
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.close()
            return Path(tmp.name)
        except Exception:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------
    # Shared transforms (lifted from SocrataCollector)
    # ------------------------------------------------------------------

    def _location_columns(self, dataset_id: str) -> set[str]:
        meta = self._metadata(dataset_id)
        return {c.field_name for c in meta.columns if c.datatype in ("location", "point")}

    def _geometry_column_name(self, dataset_id: str) -> str:
        meta = self._metadata(dataset_id)
        for c in meta.columns:
            if c.datatype in _SOCRATA_GEOMETRY_KIND:
                return c.field_name
        return "geom"

    @staticmethod
    def _rename_system_fields(rows: list[dict]) -> list[dict]:
        return [{SYSTEM_FIELD_RENAMES.get(k, k): v for k, v in row.items()} for row in rows]

    def _rename_file_columns(self, rows: list[dict], dataset_id: str) -> list[dict]:
        rename_map = self._metadata(dataset_id).column_rename_map
        if not rename_map:
            return rows
        return [{rename_map.get(k, k): v for k, v in row.items()} for row in rows]

    @staticmethod
    def _drop_computed_region_columns(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            for k in [k for k in row if k.startswith(":@computed_region")]:
                del row[k]
        return rows

    @staticmethod
    def _add_system_field_defaults(rows: list[dict[str, Any]], timestamp: str) -> list[dict[str, Any]]:
        """File exports lack system fields; default socrata_updated_at to now."""
        for row in rows:
            row.setdefault("socrata_updated_at", timestamp)
            row.setdefault("socrata_created_at", None)
            row.setdefault("socrata_id", None)
            row.setdefault("socrata_version", None)
        return rows

    @staticmethod
    def _convert_location_fields(
        rows: list[dict[str, Any]], location_columns: set[str]
    ) -> list[dict[str, Any]]:
        """Convert Socrata location/point JSON objects to EWKT strings."""
        for row in rows:
            for col in location_columns:
                val = row.get(col)
                if val is None:
                    continue
                if isinstance(val, str):
                    try:
                        val = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        row[col] = None
                        continue
                if isinstance(val, dict):
                    lat = val.get("latitude")
                    lon = val.get("longitude")
                    if lat is None or lon is None:
                        coords = val.get("coordinates")
                        if coords and len(coords) >= 2:
                            lon, lat = coords[0], coords[1]
                    if lat is not None and lon is not None:
                        row[col] = f"SRID=4326;POINT({lon} {lat})"
                    else:
                        row[col] = None
                else:
                    row[col] = None
        return rows
