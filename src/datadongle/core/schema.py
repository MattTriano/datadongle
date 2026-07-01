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
from enum import Enum


class ColumnType(str, Enum):
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
    """A single typed column. ``geometry`` is set iff ``type`` is GEOMETRY."""

    name: str
    type: ColumnType
    nullable: bool = True
    geometry: GeometrySpec | None = None

    def __post_init__(self) -> None:
        if self.type is ColumnType.GEOMETRY and self.geometry is None:
            raise ValueError(f"Geometry column {self.name!r} requires a GeometrySpec.")
        if self.type is not ColumnType.GEOMETRY and self.geometry is not None:
            raise ValueError(
                f"Column {self.name!r} has a GeometrySpec but type is {self.type}."
            )


@dataclass
class TableSchema:
    """A target table's columns plus its natural key.

    ``entity_key`` is the natural key used for SCD2/upsert. ``None`` means
    append-only.
    """

    columns: list[Column]
    entity_key: list[str] | None = None

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
