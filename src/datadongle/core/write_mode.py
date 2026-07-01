"""Write modes: the *policy* half of the load contract.

A ``WriteMode`` says *how* incoming rows should be reconciled with a target
table. Each engine translates a mode into its own *mechanism* — a staged
merge for ``PostgresEngine``, a DuckDB-detect + PyIceberg-append for
``IcebergEngine`` — so a collector picks a policy without knowing the storage.
"""

from __future__ import annotations

from dataclasses import dataclass


class WriteMode:
    """Base class for load policies."""


@dataclass(frozen=True)
class Append(WriteMode):
    """Insert every incoming row. No deduplication, no versioning."""


@dataclass(frozen=True)
class Upsert(WriteMode):
    """Insert-or-update keyed by ``keys``.

    ``on_conflict`` is ``"update"`` (overwrite non-key columns) or
    ``"nothing"`` (keep the existing row).
    """

    keys: list[str]
    on_conflict: str = "update"

    def __post_init__(self) -> None:
        if not self.keys:
            raise ValueError("Upsert requires at least one key column.")
        if self.on_conflict not in ("update", "nothing"):
            raise ValueError(
                f"on_conflict must be 'update' or 'nothing', got {self.on_conflict!r}"
            )


@dataclass(frozen=True)
class Scd2(WriteMode):
    """Keep versioned history keyed by ``entity_key`` + a content hash.

    Each engine realizes this in its native shape: physical
    ``valid_from``/``valid_to`` for ``PostgresEngine``; an append-only
    satellite with derived end-dating for ``IcebergEngine``.

    ``invalidate_missing`` closes out (Postgres) or tombstones (Iceberg)
    entities absent from a full-refresh pull.
    """

    entity_key: list[str]
    invalidate_missing: bool = False

    def __post_init__(self) -> None:
        if not self.entity_key:
            raise ValueError("Scd2 requires at least one entity_key column.")
