"""Core primitives shared across collectors, engines, and the load layer."""

from __future__ import annotations

from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import Engine, TableRef, WriteSession
from datadongle.core.reader import SourceReader
from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema
from datadongle.core.write_mode import Append, SCD2, Upsert, WriteMode

__all__ = [
    "Cursor",
    "CursorSpec",
    "Column",
    "ColumnType",
    "GeometrySpec",
    "TableSchema",
    "Append",
    "SCD2",
    "Upsert",
    "WriteMode",
    "Engine",
    "TableRef",
    "WriteSession",
    "SourceReader",
]
