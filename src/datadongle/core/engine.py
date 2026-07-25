"""The storage-engine interface.

An ``Engine`` is the *mechanism* half of the load contract: it knows how to
create a table, open a write session for a given ``WriteMode``, read a
high-water mark back from a table, and run queries. Collectors and the shared
load driver depend only on this Protocol, so ``PostgresEngine`` and
``IcebergEngine`` are interchangeable.

Return types that would otherwise pull in heavy dependencies (a pandas /
GeoPandas frame from ``query``) are typed ``Any`` to keep this module
import-light.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.schema import TableSchema
from datadongle.core.write_mode import WriteMode


@dataclass(frozen=True)
class TableRef:
    """A target table: ``name`` plus an optional ``namespace``.

    ``namespace`` is the PostgreSQL schema or the Iceberg namespace. Building
    a ``TableRef`` from a spec's ``target_schema``/``target_table`` keeps the
    schema/namespace vocabulary out of collectors.
    """

    name: str
    namespace: str | None = None

    def __str__(self) -> str:
        return f"{self.namespace}.{self.name}" if self.namespace else self.name


@runtime_checkable
class WriteSession(Protocol):
    """A staged write, opened by :meth:`Engine.open_write`.

    Batches are written incrementally; the merge/append into the target
    happens on a clean context-manager exit. Row counters are readable after
    exit.
    """

    rows_staged: int
    rows_merged: int
    rows_invalidated: int

    def write_batch(self, rows: list[dict[str, Any]]) -> int:
        """Stage a batch of rows. Returns the number written."""

    def __enter__(self) -> WriteSession: ...

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool: ...


@runtime_checkable
class Engine(Protocol):
    """Storage backend: table lifecycle, staged writes, HWM reads, queries."""

    def open_write(self, target: TableRef, schema: TableSchema, mode: WriteMode) -> WriteSession:
        """Open a staged write session that realizes ``mode`` on this engine."""

    def query(self, sql: str, params: Any | None = None) -> Any:
        """Run a read query; returns a DataFrame/GeoDataFrame (engine-specific)."""

    def ensure_table(self, target: TableRef, schema: TableSchema, mode: WriteMode) -> None:
        """Idempotently create ``target`` for ``schema`` under ``mode``.

        The physical shape depends on ``mode``: every table gets an
        ``ingested_at`` column; a keyed :class:`SCD2` table also gets the
        engine's SCD2 columns (e.g. ``record_hash``/``valid_from``/``valid_to``
        for Postgres) plus the matching uniqueness constraint and
        current-version index.
        """

    def table_exists(self, target: TableRef) -> bool: ...

    def table_columns(self, target: TableRef) -> set[str]:
        """Column names of ``target`` (used for schema-drift preflight)."""

    def geometry_columns(self, target: TableRef) -> dict[str, int]:
        """Map of geometry column name → SRID for ``target`` (empty if none)."""

    def read_high_water_mark(self, target: TableRef, cursor: CursorSpec) -> Cursor | None:
        """Read the max cursor value from ``target`` (``None`` if empty/absent).

        Derived from the table itself, so it self-heals across drop/rebuild.
        """
