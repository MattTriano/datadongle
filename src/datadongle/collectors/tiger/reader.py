"""TigerReader — the TIGER/Line source adapter for the shared collection driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the TIGER
``client``/``metadata``/``spec``. It replaces the per-file download/parse/ingest
logic that used to live in ``TigerCollector``; the family-level orchestration
(the ``vintages × units`` fan-out, the union-of-vintages table, per-file error
isolation) lives in :func:`run_tiger_collection` in ``driver.py``.

A reader instance handles **one file at a time**: the family driver narrows a
multi-vintage/multi-unit spec to a single vintage (for schema discovery) or a
single unit — one ``(vintage, fips)`` file — before calling any reader method.
A unit's ``state_fips`` is the 2-digit state FIPS for state-scoped layers, the
5-digit county FIPS for county-scoped layers (county URLs are constructible from
it exactly like state URLs), or ``None`` for national layers.

Four things shape this reader:

  - **Schema is discovered from a sample shapefile.** ``schema`` downloads one
    representative file per vintage and maps its fiona schema to neutral
    ``ColumnType``s; the family driver unions those across vintages.
  - **Immutable, full-refresh-only.** Vintages don't change and there's no row
    cursor, so ``cursor_spec`` is ``None`` (always a full read) and re-collecting
    an unchanged file is a no-op SCD2 merge.
  - **Write mode is data-dependent.** With no explicit ``entity_key`` the reader
    auto-detects a stable ID column; if none exists the layer is append-only.
  - **Geometry passes through as WKB-hex.** The shapefile parser emits EWKB-hex,
    which both engines accept, so ``read`` stamps ``vintage`` (and synthetic
    ``statefp``/``countyfp`` for county files that lack them) but leaves geometry
    untouched.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from typing import Any

from datadongle.collectors.tiger.client import TigerClient
from datadongle.collectors.tiger.metadata import TigerMetadata
from datadongle.collectors.tiger.spec import TigerDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema
from datadongle.core.write_mode import SCD2, Append, WriteMode

logger = logging.getLogger(__name__)

# Column names (lowercased) that are stable feature identifiers in TIGER
# shapefiles, checked in priority order for entity-key auto-detection. Matched
# exactly or as a prefix (so "geoid20" matches "geoid").
_CANDIDATE_ID_COLUMNS = ["geoid", "geoidfq", "linearid", "tlid", "areaid"]

# Fiona property type (prefix before any ":width") -> neutral ColumnType.
_FIONA_TO_COLUMN_TYPE: dict[str, ColumnType] = {
    "str": ColumnType.TEXT,
    "int": ColumnType.BIGINT,
    "int32": ColumnType.INTEGER,
    "int64": ColumnType.BIGINT,
    "float": ColumnType.DOUBLE,
    "date": ColumnType.DATE,
    "datetime": ColumnType.TIMESTAMPTZ,
    "time": ColumnType.TEXT,
    "bytes": ColumnType.TEXT,
}

# Fiona geometry type -> OGC geometry kind. Promoted to the Multi* form because
# shapefiles often mix single and multi geometries (the parser promotes to
# match), so a Multi-typed column accepts both.
_FIONA_GEOM_TO_KIND: dict[str, str] = {
    "Point": "MultiPoint",
    "MultiPoint": "MultiPoint",
    "LineString": "MultiLineString",
    "MultiLineString": "MultiLineString",
    "Polygon": "MultiPolygon",
    "MultiPolygon": "MultiPolygon",
    "3D Point": "MultiPointZ",
    "3D MultiPoint": "MultiPointZ",
    "3D LineString": "MultiLineStringZ",
    "3D MultiLineString": "MultiLineStringZ",
    "3D Polygon": "MultiPolygonZ",
    "3D MultiPolygon": "MultiPolygonZ",
}

_GEOMETRY_COLUMN = "geom"


class TigerReader:
    """Adapts one TIGER/Line file to the shared collection driver."""

    source = "tiger"

    def __init__(self, client_factory: Callable[[], TigerClient] | None = None) -> None:
        self._client_factory = client_factory or TigerClient
        self._client: TigerClient | None = None
        self._metadata: TigerMetadata | None = None
        # (source, layer, vintage) -> discovered fiona column names (lowercased
        # per lowercase_columns). Populated by schema(); used by write_mode() so
        # the entity key is resolved from every vintage's columns, once.
        self._columns_cache: dict[tuple[str, str, int], list[str]] = {}
        # (source, layer, vintage) -> [(county_fips, url), ...] for county scope.
        self._county_files_cache: dict[tuple[str, str, int], list[tuple[str, str]]] = {}

    @property
    def client(self) -> TigerClient:
        if self._client is None:
            self._client = self._client_factory()
        return self._client

    @property
    def metadata(self) -> TigerMetadata:
        if self._metadata is None:
            self._metadata = TigerMetadata(self.client)
        return self._metadata

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: TigerDatasetSpec) -> str:
        fips = spec.state_fips[0] if spec.state_fips else "us"
        return f"{spec.target_table}/{spec.vintages[0]}/{fips}"

    def target(self, spec: TigerDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: TigerDatasetSpec) -> TableSchema:
        """The single vintage's schema, discovered from a representative sample.

        Downloads one file for the vintage, reads its fiona schema, and maps it
        to neutral columns; adds synthetic ``statefp``/``countyfp`` for county
        layers that lack them, a ``vintage`` column, and a geometry column when
        the layer has geometry.
        """
        vintage = spec.vintages[0]
        url = self._sample_url(spec, vintage)
        properties, geom_type = self._inspect_sample_from_url(url)

        lower = spec.lowercase_columns
        col_names = [(k.lower() if lower else k) for k in properties]
        self._columns_cache[self._cache_key(spec, vintage)] = col_names

        columns: list[Column] = []
        for name, fiona_type in zip(col_names, properties.values(), strict=True):
            columns.append(Column(name, _fiona_to_column_type(fiona_type)))

        # County files sometimes encode the county only in the filename (e.g.
        # ADDR), so ensure statefp/countyfp exist to filter on later.
        for synth in self._synthetic_fips_columns(spec, {c.lower() for c in col_names}):
            columns.append(synth)

        columns.append(Column("vintage", ColumnType.INTEGER, nullable=False))

        if _has_geometry(geom_type):
            kind = _FIONA_GEOM_TO_KIND.get(geom_type, "Geometry")
            columns.append(
                Column(
                    _GEOMETRY_COLUMN,
                    ColumnType.GEOMETRY,
                    geometry=GeometrySpec(kind=kind, srid=4326),
                )
            )
        return TableSchema(columns=columns)

    def write_mode(self, spec: TigerDatasetSpec, *, mode: str = "full") -> WriteMode:
        """SCD2 keyed on the resolved entity key, or Append when there is none.

        The entity key is explicit ``spec.entity_key`` if set, else auto-detected
        from the union of every vintage's discovered columns (so the whole family
        shares one key), else ``None`` -> append-only.
        """
        entity_key = self._resolve_entity_key(spec)
        if entity_key:
            return SCD2(entity_key=entity_key)
        return Append()

    def cursor_spec(self, spec: TigerDatasetSpec) -> CursorSpec | None:
        # Vintages are immutable and shapefiles have no row cursor — always full.
        return None

    def read(
        self, spec: TigerDatasetSpec, *, since: Cursor | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        # ``since`` is always None (cursor_spec is None). ``spec`` is narrowed to
        # one unit; download that file, parse it, and stream batches.
        from datadongle.parsers.shapefile import parse_shapefile

        vintage = spec.vintages[0]
        fips = spec.state_fips[0] if spec.state_fips else None
        url = self._download_url(spec, vintage, fips)
        synthetic_fips = self._synthetic_fips_values(spec, fips)

        filepath = self.client.download_to_tempfile(url)
        try:
            batches, _result = parse_shapefile(
                filepath=filepath,
                geometry_column=_GEOMETRY_COLUMN,
                batch_size=5000,
                lowercase_columns=spec.lowercase_columns,
            )
            for batch in batches:
                yield self._prepare_batch(batch, vintage, synthetic_fips)
        finally:
            filepath.unlink(missing_ok=True)

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        return None

    # ------------------------------------------------------------------
    # Unit enumeration (for the family driver)
    # ------------------------------------------------------------------

    def units(self, spec: TigerDatasetSpec, vintage: int) -> list[str | None]:
        """The unit FIPS codes to collect for one vintage.

        ``[None]`` for national layers (one file), the requested state FIPS for
        state layers, and the discovered 5-digit county FIPS (filtered to the
        requested states) for county layers.
        """
        if spec.scope == "national":
            return [None]
        if spec.scope == "county":
            return [fips for fips, _url in self._county_files(spec, vintage)]
        return list(spec.states)

    # ------------------------------------------------------------------
    # URL / sample helpers
    # ------------------------------------------------------------------

    def _download_url(self, spec: TigerDatasetSpec, vintage: int, fips: str | None) -> str:
        return self.metadata.get_download_url(
            vintage=vintage,
            layer=spec.layer,
            source=spec.source,
            state_fips=fips,
            resolution=spec.resolution,
        )

    def _sample_url(self, spec: TigerDatasetSpec, vintage: int) -> str:
        """The URL of a representative file to sample the vintage's schema from."""
        if spec.scope == "national":
            return self._download_url(spec, vintage, None)
        if spec.scope == "county":
            county_files = self._county_files(spec, vintage)
            if not county_files:
                raise RuntimeError(
                    f"No files found for {spec.layer} vintage={vintage} "
                    f"(source={spec.source}); cannot inspect schema."
                )
            return county_files[0][1]
        return self._download_url(spec, vintage, spec.states[0])

    def _county_files(self, spec: TigerDatasetSpec, vintage: int) -> list[tuple[str, str]]:
        """Discovered ``(county_fips, url)`` for a county layer, filtered to states."""
        key = self._cache_key(spec, vintage)
        if key not in self._county_files_cache:
            files_df = self.metadata.list_files(vintage, spec.layer, source=spec.source)
            requested = set(spec.states)
            out: list[tuple[str, str]] = []
            for _, row in files_df.iterrows():
                county_fips = _extract_county_fips(row["filename"], vintage)
                if county_fips is None:
                    continue
                if county_fips[:2] not in requested:
                    continue
                out.append((county_fips, row["url"]))
            self._county_files_cache[key] = out
        return self._county_files_cache[key]

    @staticmethod
    def _inspect_sample(path: str) -> tuple[dict, str]:
        """Read a local shapefile zip's (fiona properties, geometry type)."""
        import fiona

        with fiona.open(f"zip://{path}", "r") as src:
            properties = dict(src.schema.get("properties", {}))
            geom_type = src.schema.get("geometry", "Polygon")
        return properties, geom_type

    def _inspect_sample_from_url(self, url: str) -> tuple[dict, str]:
        """Download a sample file and read its schema, then delete the file."""
        filepath = self.client.download_to_tempfile(url)
        try:
            return self._inspect_sample(str(filepath))
        finally:
            filepath.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Entity key / synthetic FIPS
    # ------------------------------------------------------------------

    def _resolve_entity_key(self, spec: TigerDatasetSpec) -> list[str] | None:
        if spec.entity_key is not None:
            return spec.entity_key
        columns = self._discovered_columns(spec)
        return _auto_detect_entity_key(columns, spec.lowercase_columns)

    def _discovered_columns(self, spec: TigerDatasetSpec) -> list[str]:
        """Union of columns discovered across the spec's vintages.

        Uses the schema() cache when populated (the family driver discovers every
        vintage's schema first); otherwise samples the max vintage on demand.
        """
        cached = [
            cols
            for (src, layer, _v), cols in self._columns_cache.items()
            if src == spec.source and layer == spec.layer.upper()
        ]
        if not cached:
            self.schema(_only_vintage(spec, max(spec.vintages)))
            cached = [
                cols
                for (src, layer, _v), cols in self._columns_cache.items()
                if src == spec.source and layer == spec.layer.upper()
            ]
        seen: set[str] = set()
        union: list[str] = []
        for cols in cached:
            for c in cols:
                if c not in seen:
                    seen.add(c)
                    union.append(c)
        return union

    def _synthetic_fips_columns(
        self, spec: TigerDatasetSpec, present_lower: set[str]
    ) -> list[Column]:
        """statefp/countyfp columns to add for county layers that lack them."""
        if spec.scope != "county":
            return []
        cols = []
        if "statefp" not in present_lower:
            cols.append(Column(self._fips_col(spec, "statefp"), ColumnType.TEXT))
        if "countyfp" not in present_lower:
            cols.append(Column(self._fips_col(spec, "countyfp"), ColumnType.TEXT))
        return cols

    def _synthetic_fips_values(self, spec: TigerDatasetSpec, fips: str | None) -> dict[str, str]:
        """statefp/countyfp values to stamp on county rows that lack them."""
        if spec.scope != "county" or not fips or len(fips) != 5:
            return {}
        return {
            self._fips_col(spec, "statefp"): fips[:2],
            self._fips_col(spec, "countyfp"): fips[2:],
        }

    @staticmethod
    def _fips_col(spec: TigerDatasetSpec, name: str) -> str:
        return name if spec.lowercase_columns else name.upper()

    def _prepare_batch(
        self, batch: list[dict[str, Any]], vintage: int, synthetic_fips: dict[str, str]
    ) -> list[dict[str, Any]]:
        """Stamp vintage (and synthetic FIPS) onto each row; leave geometry as-is."""
        for row in batch:
            row["vintage"] = vintage
            for col, val in synthetic_fips.items():
                row.setdefault(col, val)
        return batch

    @staticmethod
    def _cache_key(spec: TigerDatasetSpec, vintage: int) -> tuple[str, str, int]:
        return (spec.source, spec.layer.upper(), vintage)


# ----------------------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------------------


def _only_vintage(spec: TigerDatasetSpec, vintage: int) -> TigerDatasetSpec:
    import dataclasses

    return dataclasses.replace(spec, vintages=[vintage])


def _fiona_to_column_type(fiona_type: str) -> ColumnType:
    base = fiona_type.split(":")[0]
    return _FIONA_TO_COLUMN_TYPE.get(base, ColumnType.TEXT)


def _has_geometry(geom_type: str | None) -> bool:
    return geom_type not in (None, "None")


def _auto_detect_entity_key(columns: list[str], lowercase: bool) -> list[str] | None:
    """Return ``[id_column, "vintage"]`` for the first known ID column, else None."""
    lowered = [(c.lower(), c) for c in columns]
    for candidate in _CANDIDATE_ID_COLUMNS:
        for col_lower, col_original in lowered:
            if col_lower == candidate or col_lower.startswith(candidate):
                return [(col_lower if lowercase else col_original), "vintage"]
    return None


def _extract_county_fips(filename: str, vintage: int) -> str | None:
    """Extract the 5-digit county FIPS from ``tl_{year}_{ssccc}_{layer}.zip``."""
    import re

    match = re.match(rf"tl_{vintage}_(\d{{5}})_", filename)
    return match.group(1) if match else None
