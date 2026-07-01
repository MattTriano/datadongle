"""Core primitives shared across collectors, engines, and the load layer."""

from __future__ import annotations

from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema
from datadongle.core.write_mode import Append, Scd2, Upsert, WriteMode

__all__ = [
    "Cursor",
    "CursorSpec",
    "Column",
    "ColumnType",
    "GeometrySpec",
    "TableSchema",
    "Append",
    "Scd2",
    "Upsert",
    "WriteMode",
]
