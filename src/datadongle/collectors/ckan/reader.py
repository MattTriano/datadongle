"""CKANReader — the CKAN source adapter for the shared collection driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the
existing CKAN ``client``/``metadata``/``spec``. It replaces the orchestration
that used to live in ``CKANCollector``: the shared driver runs the collection,
and this reader supplies the schema, write policy, and the batch stream.

Three things about CKAN shape this reader:

  - **Full-refresh-only.** CKAN resources are files (CSV, GeoJSON, ...) with
    no reliable update cursor, so ``cursor_spec`` is ``None`` and the driver
    always runs a full read, even under ``mode="incremental"``.
  - **The schema is discovered, not fixed.** If the first resource is in
    CKAN's DataStore, its typed field metadata becomes the schema. Otherwise
    the resource file is downloaded and its header scanned: CSV columns are
    all text; GeoJSON contributes the first feature's properties plus a
    ``geom`` geometry column. The download is cached on the reader so
    ``read()`` consumes it instead of re-fetching.
  - **Source column names are dirty.** Raw names are normalized (lowercased,
    non-alphanumerics -> ``_``) with ``_2``/``_3`` suffixes on collisions, and
    the same mapping renames batch keys in ``read()``. A resource whose
    columns drift from the discovered schema is warned about — the engines
    ignore extra columns and load missing ones as NULL.
"""

from __future__ import annotations

import csv
import logging
import re
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import ijson

from datadongle.collectors.ckan.client import CKANClient
from datadongle.collectors.ckan.metadata import CKANResource
from datadongle.collectors.ckan.spec import CKANDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema
from datadongle.core.write_mode import SCD2, Append, WriteMode
from datadongle.parsers.csv_parser import parse_csv
from datadongle.parsers.geojson import parse_geojson

logger = logging.getLogger(__name__)

# Columns CKAN's DataStore adds internally — never part of the actual data.
# _id: auto-increment row ID, reset on every DataPusher re-import.
# _full_text: tsvector column used for full-text search.
CKAN_INTERNAL_COLUMNS = {"_id", "_full_text"}

# The target column GeoJSON feature geometries are parsed into.
GEOMETRY_COLUMN = "geom"

# CKAN DataStore field type -> neutral ColumnType. Unmapped types (e.g.
# "time", which has no neutral equivalent) fall back to TEXT.
_DATASTORE_TO_COLUMN_TYPE: dict[str, ColumnType] = {
    "text": ColumnType.TEXT,
    "int": ColumnType.INTEGER,
    "int4": ColumnType.INTEGER,
    "int8": ColumnType.BIGINT,
    "float": ColumnType.DOUBLE,
    "float8": ColumnType.DOUBLE,
    "numeric": ColumnType.NUMERIC,
    "bool": ColumnType.BOOLEAN,
    "json": ColumnType.JSON,
    "jsonb": ColumnType.JSON,
    "date": ColumnType.DATE,
    "timestamp": ColumnType.TIMESTAMP,  # DataStore timestamps are naive
    "timestamptz": ColumnType.TIMESTAMPTZ,
}


class CKANReader:
    """Adapts a CKAN dataset to the shared collection driver."""

    source = "ckan"

    def __init__(self, client_factory: Callable[[str], CKANClient] | None = None) -> None:
        self._client_factory = client_factory or CKANClient
        self._clients: dict[str, CKANClient] = {}
        self._resource_cache: dict[tuple, list[CKANResource]] = {}
        self._schema_cache: dict[tuple, TableSchema] = {}
        # Files downloaded during schema discovery, keyed by resource id;
        # read() pops and consumes them so each run downloads a file once.
        self._downloads: dict[str, Path] = {}

    def _client(self, base_url: str) -> CKANClient:
        if base_url not in self._clients:
            self._clients[base_url] = self._client_factory(base_url)
        return self._clients[base_url]

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: CKANDatasetSpec) -> str:
        return spec.dataset_id

    def target(self, spec: CKANDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: CKANDatasetSpec) -> TableSchema:
        """Discovered schema: DataStore fields, or the first resource's header."""
        key = self._spec_key(spec)
        if key not in self._schema_cache:
            self._schema_cache[key] = self._discover_schema(spec)
        return self._schema_cache[key]

    def write_mode(self, spec: CKANDatasetSpec, *, mode: str = "incremental") -> WriteMode:
        # CKAN's policy doesn't depend on the collection mode (every run is
        # effectively a full refresh regardless).
        if spec.entity_key:
            return SCD2(entity_key=spec.entity_key)
        return Append()

    def cursor_spec(self, spec: CKANDatasetSpec) -> CursorSpec | None:
        # Resource files carry no update cursor — the driver falls back to a
        # full read.
        return None

    def read(
        self, spec: CKANDatasetSpec, *, since: Cursor | None
    ) -> Iterator[list[dict[str, Any]]]:
        # ``since`` is always None here: cursor_spec() is None, so the driver
        # never reads a high-water mark.
        client = self._client(spec.base_url)
        schema_columns = set(self.schema(spec).column_names())
        for resource in self._resolve_resources(spec):
            yield from self._read_resource(client, resource, schema_columns)

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        return None

    # ------------------------------------------------------------------
    # Resource resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _spec_key(spec: CKANDatasetSpec) -> tuple:
        """The fields that determine which resources a spec selects."""
        return (
            spec.base_url,
            spec.dataset_id,
            tuple(spec.resource_ids or ()),
            spec.resource_format,
        )

    def _resolve_resources(self, spec: CKANDatasetSpec) -> list[CKANResource]:
        """Resolve the spec's resource selection to CKANResource objects.

        If resource_ids is set, fetches each resource by ID. Otherwise,
        filters the dataset's resources by resource_format. Raises ValueError
        if nothing matches.
        """
        key = self._spec_key(spec)
        if key in self._resource_cache:
            return self._resource_cache[key]

        meta = self._client(spec.base_url).metadata
        if spec.resource_ids:
            resources = [meta.get_resource(rid) for rid in spec.resource_ids]
        else:
            # The spec requires resource_ids or resource_format; this is the latter.
            assert spec.resource_format is not None
            resources = meta.find_resources(spec.dataset_id, spec.resource_format)

        if not resources:
            fmt = spec.resource_format or "(by ID)"
            raise ValueError(
                f"No resources found for dataset {spec.dataset_id!r} "
                f"with format {fmt} on {spec.base_url}"
            )

        logger.info(
            "Resolved %d resource(s) for %s: %s",
            len(resources),
            spec.dataset_id,
            [r.id for r in resources],
        )
        self._resource_cache[key] = resources
        return resources

    # ------------------------------------------------------------------
    # Schema discovery
    # ------------------------------------------------------------------

    def _discover_schema(self, spec: CKANDatasetSpec) -> TableSchema:
        client = self._client(spec.base_url)
        first = self._resolve_resources(spec)[0]

        if first.datastore_active or client.metadata.has_datastore(first.id):
            logger.info("Using DataStore fields for schema discovery on %s", first.id)
            raw = self._columns_from_datastore(client, first.id)
        elif (first.format or "").upper() == "GEOJSON":
            logger.info("Scanning GeoJSON file for schema discovery on %s", first.id)
            raw = self._columns_from_geojson(client, first)
        else:
            logger.info("Scanning CSV header for schema discovery on %s", first.id)
            raw = self._columns_from_csv(client, first)

        name_map = normalize_column_names([name for name, _ in raw])
        # Preserve order and types; drop names that normalized to empty.
        columns = [
            self._column_for(name_map[name], col_type) for name, col_type in raw if name in name_map
        ]
        return TableSchema(columns=columns)

    @staticmethod
    def _column_for(name: str, col_type: ColumnType) -> Column:
        if col_type is ColumnType.GEOMETRY:
            return Column(
                name,
                ColumnType.GEOMETRY,
                geometry=GeometrySpec(kind="Geometry", srid=4326),
            )
        return Column(name, col_type)

    @staticmethod
    def _columns_from_datastore(
        client: CKANClient, resource_id: str
    ) -> list[tuple[str, ColumnType]]:
        fields = client.metadata.get_datastore_fields(resource_id)
        return [
            (
                f["id"],
                _DATASTORE_TO_COLUMN_TYPE.get(f.get("type", "text"), ColumnType.TEXT),
            )
            for f in fields
            if f["id"] not in CKAN_INTERNAL_COLUMNS
        ]

    def _columns_from_csv(
        self, client: CKANClient, resource: CKANResource
    ) -> list[tuple[str, ColumnType]]:
        """Read the CSV header; every column is text."""
        filepath = self._cached_download(client, resource)
        with open(filepath, encoding="utf-8", newline="") as f:
            headers = next(csv.reader(f), None)
        if not headers:
            raise ValueError(f"Resource {resource.id} file has no header row")
        return [
            (h.strip(), ColumnType.TEXT)
            for h in headers
            if h.strip() and h.strip() not in CKAN_INTERNAL_COLUMNS
        ]

    def _columns_from_geojson(
        self, client: CKANClient, resource: CKANResource
    ) -> list[tuple[str, ColumnType]]:
        """Read the first feature's properties (text) plus a geometry column."""
        filepath = self._cached_download(client, resource)
        with open(filepath, "rb") as f:
            for feature in ijson.items(f, "features.item"):
                props = feature.get("properties") or {}
                columns = [(k, ColumnType.TEXT) for k in props if k not in CKAN_INTERNAL_COLUMNS]
                columns.append((GEOMETRY_COLUMN, ColumnType.GEOMETRY))
                return columns
        raise ValueError(f"Resource {resource.id} GeoJSON has no features")

    # ------------------------------------------------------------------
    # Downloads
    # ------------------------------------------------------------------

    def _cached_download(self, client: CKANClient, resource: CKANResource) -> Path:
        """Download a resource file once; read() pops and consumes the cache."""
        if resource.id not in self._downloads:
            self._downloads[resource.id] = self._fetch(client, resource)
        return self._downloads[resource.id]

    @staticmethod
    def _fetch(client: CKANClient, resource: CKANResource) -> Path:
        if not resource.url:
            raise ValueError(f"Resource {resource.id} has no download URL")
        suffix = client.suffix_for_format(resource.format or "")
        return client.download_to_tempfile(resource.url, suffix=suffix)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def _read_resource(
        self, client: CKANClient, resource: CKANResource, schema_columns: set[str]
    ) -> Iterator[list[dict[str, Any]]]:
        """Download and parse one resource, yielding normalized batches."""
        filepath = self._downloads.pop(resource.id, None) or self._fetch(client, resource)
        try:
            if (resource.format or "").upper() == "GEOJSON":
                batches, _result = parse_geojson(filepath, geometry_column=GEOMETRY_COLUMN)
            else:
                batches = parse_csv(filepath)

            # Built from the first batch's keys, reused for the whole resource.
            name_map: dict[str, str] | None = None
            for batch in batches:
                batch = _strip_internal_columns(batch)
                if name_map is None and batch:
                    name_map = normalize_column_names(list(batch[0].keys()))
                    self._warn_on_drift(resource.id, set(name_map.values()), schema_columns)
                if name_map:
                    batch = _rename_batch_keys(batch, name_map)
                yield batch
        finally:
            filepath.unlink(missing_ok=True)

    @staticmethod
    def _warn_on_drift(resource_id: str, actual: set[str], schema_columns: set[str]) -> None:
        extra = actual - schema_columns
        missing = schema_columns - actual
        if extra:
            logger.warning(
                "Resource %s has columns not in the discovered schema "
                "(the engines ignore them): %s",
                resource_id,
                sorted(extra),
            )
        if missing:
            logger.warning(
                "Resource %s is missing schema columns (loaded as NULL): %s",
                resource_id,
                sorted(missing),
            )


# ----------------------------------------------------------------------
# Row / column-name helpers
# ----------------------------------------------------------------------


def _strip_internal_columns(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove CKAN internal columns (_id, _full_text) from rows."""
    for row in rows:
        for col in CKAN_INTERNAL_COLUMNS:
            row.pop(col, None)
    return rows


def _rename_batch_keys(
    rows: list[dict[str, Any]], name_map: dict[str, str]
) -> list[dict[str, Any]]:
    """Rename dict keys using the raw -> normalized mapping.

    Keys not in the mapping are dropped (they produced empty normalized
    names, e.g. a header of whitespace only).
    """
    return [{name_map[k]: v for k, v in row.items() if k in name_map} for row in rows]


def normalize_column_name(raw: str) -> str:
    """Normalize a column name: lowercase, non-alphanumerics -> '_', trim '_'."""
    return re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")


def normalize_column_names(raw_names: list[str]) -> dict[str, str]:
    """Build an ordered raw -> normalized mapping with collision handling.

    When two raw names normalize to the same value, the later one is suffixed
    with '_2', '_3', etc., and a warning lists the raw names involved. Empty
    normalized names (e.g. from a header of '   ') are skipped.
    """
    mapping: dict[str, str] = {}
    taken: dict[str, list[str]] = {}  # normalized -> [raw names that produced it]

    for raw in raw_names:
        base = normalize_column_name(raw)
        if not base:
            continue

        if base not in taken:
            taken[base] = [raw]
            mapping[raw] = base
            continue

        # Collision: suffix _2, _3, ...
        taken[base].append(raw)
        suffix = len(taken[base])
        candidate = f"{base}_{suffix}"
        # Very unlikely but possible: the suffixed name also clashes.
        while candidate in taken:
            suffix += 1
            candidate = f"{base}_{suffix}"
        taken[candidate] = [raw]
        mapping[raw] = candidate

    collisions = {n: rs for n, rs in taken.items() if len(rs) > 1}
    if collisions:
        logger.warning("Column name collisions after normalization: %s", collisions)

    return mapping
