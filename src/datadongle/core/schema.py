"""Engine-neutral table schema.

Collectors describe their target table with a ``TableSchema`` of typed
``Column``s. Each engine renders these into its native DDL/schema:
``PostgresEngine`` emits PostgreSQL types (geometry → PostGIS
``geometry(<kind>,<srid>)``); ``IcebergEngine`` emits an Iceberg/Arrow schema
(geometry → WKB ``binary`` + SRID in field metadata).

This replaces the implicit "read ``information_schema``" coupling and the
per-source ``generate_ddl`` string building.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ColumnType(StrEnum):
    """Neutral column types, mapped per-engine to concrete storage types."""

    TEXT = "text"
    INTEGER = "integer"
    BIGINT = "bigint"
    NUMERIC = "numeric"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"  # naive (no timezone)
    TIMESTAMPTZ = "timestamptz"
    DATE = "date"
    JSON = "json"
    GEOMETRY = "geometry"
    RASTER = "raster"


@dataclass(frozen=True)
class GeometrySpec:
    """Geometry column detail: OGC geometry kind and SRID.

    ``kind`` is an OGC type name (``"Point"``, ``"MultiPolygon"``, or the
    catch-all ``"Geometry"``); ``srid`` defaults to WGS84 (4326).
    """

    kind: str = "Geometry"
    srid: int = 4326


@dataclass(frozen=True)
class Column:
    """A single typed column.

    ``geometry`` is set iff ``type`` is GEOMETRY. ``metadata`` marks a
    source-provided bookkeeping column (a row id, source timestamps, a version
    counter) that is stored but excluded from the SCD2 content hash — so a
    re-pull that only bumps such a column does not create a spurious new record
    version.
    """

    name: str
    type: ColumnType
    nullable: bool = True
    geometry: GeometrySpec | None = None
    metadata: bool = False

    def __post_init__(self) -> None:
        if self.type is ColumnType.GEOMETRY and self.geometry is None:
            raise ValueError(f"Geometry column {self.name!r} requires a GeometrySpec.")
        if self.type is not ColumnType.GEOMETRY and self.geometry is not None:
            raise ValueError(f"Column {self.name!r} has a GeometrySpec but type is {self.type}.")


@dataclass
class TableSchema:
    """A target table's columns and their types — structure only.

    The natural key (``entity_key``) lives on the :class:`WriteMode` the engine
    consumes, not here, and the SCD2/pipeline columns
    (``record_hash``/``valid_from``/``valid_to``/``ingested_at``) are added by
    the engine at ``ensure_table`` time, not declared by the source.
    """

    columns: list[Column]

    @property
    def geometry(self) -> dict[str, GeometrySpec]:
        """Map of geometry column name → its ``GeometrySpec``."""
        return {
            c.name: c.geometry
            for c in self.columns
            if c.type is ColumnType.GEOMETRY and c.geometry is not None
        }

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def metadata_column_names(self) -> set[str]:
        """Names of source-metadata columns (excluded from the SCD2 hash)."""
        return {c.name for c in self.columns if c.metadata}

    def raster_column_names(self) -> set[str]:
        """Names of raster columns.

        Raster values travel through the pipeline as raster hex-WKB strings.
        ``PostgresEngine`` lands them in a PostGIS ``raster`` column (parsed on
        COPY); ``IcebergEngine`` stores the decoded WKB bytes, like geometry.
        """
        return {c.name for c in self.columns if c.type is ColumnType.RASTER}


@dataclass(frozen=True)
class TypeMismatch:
    """A column present in both schema and table, but with differing types.

    ``expected`` and ``actual`` are engine-native type names, already
    canonicalized by the engine so that spelling variants (PostgreSQL's
    ``timestamptz`` vs ``timestamp with time zone``) don't read as drift.
    """

    column: str
    expected: str
    actual: str


@dataclass(frozen=True)
class SchemaDiff:
    """How a live table differs from the ``TableSchema`` a collector wants.

    Engine-neutral, so an engine that can introspect a table can report drift
    in one shape. Pipeline columns the engine adds itself (``ingested_at``, the
    SCD2 trio) are the engine's business and never appear here.

    The additive/non-additive split is what drives policy: missing columns can
    be resolved by an ``ADD COLUMN``, while dropped or retyped columns need a
    human decision (backfill, rewrite, or a new table version).
    """

    missing_columns: list[Column]
    unexpected_columns: list[str]
    type_mismatches: list[TypeMismatch]

    @property
    def is_empty(self) -> bool:
        """True when the table already matches the schema."""
        return not (self.missing_columns or self.unexpected_columns or self.type_mismatches)

    @property
    def is_additive_only(self) -> bool:
        """True when the only drift is columns the schema has and the table lacks."""
        return bool(self.missing_columns) and not (self.unexpected_columns or self.type_mismatches)

    def describe(self) -> str:
        """A short human-readable summary, for error messages and logs."""
        if self.is_empty:
            return "no drift"
        parts = []
        if self.missing_columns:
            names = ", ".join(c.name for c in self.missing_columns)
            parts.append(f"missing from table: {names}")
        if self.unexpected_columns:
            parts.append(
                f"present in table but not in schema: {', '.join(self.unexpected_columns)}"
            )
        for m in self.type_mismatches:
            parts.append(f"{m.column} is {m.actual}, schema wants {m.expected}")
        return "; ".join(parts)
