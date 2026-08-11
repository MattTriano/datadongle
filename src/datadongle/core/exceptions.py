"""Errors raised when a table's shape doesn't match what a collector needs.

These exist for the case where an external migration tool (Flyway, sqitch, a
hand-run SQL script) owns the DDL and datadongle is only allowed to *verify*.
Both carry the SQL that would resolve them, so the error message can be pasted
straight into a migration script.
"""

from __future__ import annotations

from datadongle.core.schema import SchemaDiff


class TableNotFoundError(Exception):
    """A target table is absent and this engine is not permitted to create it."""

    def __init__(self, target: str, ddl: str) -> None:
        self.target = target
        self.ddl = ddl
        super().__init__(
            f"Table {target} does not exist and this engine is not managing DDL "
            f"(manage_ddl=False). Apply this with your migration tool:\n\n{ddl}"
        )


class SchemaDriftError(Exception):
    """A target table exists but its shape has diverged from the schema.

    ``migration`` holds the ``ALTER TABLE`` statements that would fix the drift
    when it is additive-only; it is ``None`` when the drift needs a human
    decision (a dropped or retyped column).
    """

    def __init__(self, target: str, diff: SchemaDiff, migration: str | None = None) -> None:
        self.target = target
        self.diff = diff
        self.migration = migration
        message = f"Table {target} has drifted from the collector's schema — {diff.describe()}."
        if migration:
            message += (
                f"\n\nThe drift is additive. Apply this with your migration tool:\n\n{migration}"
            )
        else:
            message += (
                "\n\nThis drift is not additive, so datadongle will not render a migration "
                "for it: dropping or retyping a column needs a decision about existing rows. "
                "At a raw ingestion layer, writing to a new table version is usually safer "
                "than an in-place change."
            )
        super().__init__(message)
