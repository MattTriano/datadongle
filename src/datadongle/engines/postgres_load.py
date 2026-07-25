"""Postgres staged-ingest load logic.

``StagedIngest`` accumulates batches into a temporary staging table via COPY,
then integrates them into the target on a clean context-manager exit. The
integration semantics (simple insert / upsert / SCD2 versioning) are chosen by
how the stager is configured — ``PostgresEngine.open_write`` translates a
:class:`~datadongle.core.write_mode.WriteMode` into that configuration.

This module is where the per-mode merge SQL lives, kept separate from the
engine's connection/query primitives in ``engines.postgres``.
"""

from __future__ import annotations

import io
import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datadongle.engines.postgres import PostgresEngine


class StagedIngest:
    """
    Accumulates batches into a staging table via COPY, then merges into
    the target table on context-manager exit.

    Usage:
        with engine.staged_ingest(
            target_table="crimes",
            target_schema="raw_data",
            conflict_column=["case_number"],
            conflict_action="UPDATE",
        ) as stager:
            for batch in source.paginate(...):
                stager.write_batch(batch)

        print(stager.rows_staged, stager.rows_merged)
    """

    def __init__(
        self,
        engine: PostgresEngine,
        target_table: str,
        target_schema: str,
        conflict_column: str | list[str] | None = None,
        conflict_action: str = "NOTHING",
        entity_key: list[str] | None = None,
        metadata_columns: set[str] | None = None,
        hash_exclude_columns: set[str] | None = None,
        invalidate_missing: bool = False,
    ) -> None:
        self._engine = engine
        self._target_table = target_table
        self._target_schema = target_schema
        self._fqn = f"{target_schema}.{target_table}"

        if isinstance(conflict_column, str):
            self._conflict_columns = [conflict_column]
        else:
            self._conflict_columns = conflict_column

        self._conflict_action = conflict_action

        # SCD2 config
        self._entity_key = entity_key
        self._metadata_columns = metadata_columns
        self._hash_exclude_columns = hash_exclude_columns or metadata_columns
        self._invalidate_missing = invalidate_missing

        if entity_key and conflict_column:
            raise ValueError(
                "Specify either entity_key (SCD2) or conflict_column (simple merge), not both."
            )

        if invalidate_missing and not entity_key:
            raise ValueError("invalidate_missing requires entity_key (SCD2 mode).")

        # Use a short random suffix so parallel ingests don't collide
        suffix = uuid.uuid4().hex[:8]

        max_len = 63
        prefix = "_staging_"
        max_table_len = max_len - len(prefix) - len(suffix)
        self._staging_table = f"{prefix}{target_table[:max_table_len]}{suffix}"

        self._columns: list[str] | None = None
        self._col_list: str | None = None
        self._created = False

        self.rows_staged = 0
        self.rows_merged = 0
        self.rows_invalidated = 0

        self._engine.logger.info(
            "StagedIngest: staging table %s for target %s (mode: %s%s)",
            self._staging_table,
            self._fqn,
            "scd2" if entity_key else "simple",
            ", invalidate_missing" if invalidate_missing else "",
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_batch(self, rows: list[dict[str, Any]]) -> int:
        """
        COPY a batch of rows into the staging table.

        The first call creates the staging table and locks in the column list.

        Returns the number of rows written.
        """
        if not rows:
            return 0

        rows = self._engine._normalize_json_values(rows)

        if not self._created:
            self._columns = self._get_target_columns()
            self._col_list = ", ".join(f'"{c}"' for c in self._columns)
            self._create_staging_table()

        buf = self._rows_to_copy_buffer(rows)
        with self._engine.cursor() as cur:
            cur.copy_expert(
                f"copy {self._staging_table} ({self._col_list}) "
                f"from stdin with (format text, NULL '\\N')",
                buf,
            )

        count = len(rows)
        self.rows_staged += count
        return count

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> StagedIngest:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            if self.rows_staged > 0:
                self._cast_geometry_if_needed()
                self._run_merge()
                if exc_type is not None:
                    self._engine.logger.warning(
                        "StagedIngest: merged %d rows from %s despite error: %s",
                        self.rows_merged,
                        self._staging_table,
                        exc_val,
                    )
        except Exception as merge_err:
            self._engine.logger.error(
                "StagedIngest: merge failed for %s: %s",
                self._staging_table,
                merge_err,
            )
            if exc_type is None:
                raise
        finally:
            self._drop_staging_table()
        return False

    # ------------------------------------------------------------------
    # Merge dispatch
    # ------------------------------------------------------------------

    def _run_merge(self) -> None:
        """Integrate staging into the target using the configured write mode.

        Picks one of the module-level merge routines by how the stager was
        configured: ``entity_key`` -> SCD2, ``conflict_column`` -> upsert,
        otherwise a plain append. The whole merge runs in a single
        transaction so it is atomic.
        """
        assert self._columns is not None and self._col_list is not None  # set in __enter__
        with self._engine.cursor() as cur:
            if self._entity_key:
                result = scd2_merge(
                    cur,
                    fqn=self._fqn,
                    staging_table=self._staging_table,
                    columns=self._columns,
                    col_list=self._col_list,
                    entity_key=self._entity_key,
                    hash_exclude=self._hash_exclude_columns,
                    invalidate_missing=self._invalidate_missing,
                    rows_staged=self.rows_staged,
                    logger=self._engine.logger,
                )
                self.rows_merged = result.rows_merged
                self.rows_invalidated = result.rows_invalidated
            elif self._conflict_columns:
                self.rows_merged = upsert_merge(
                    cur,
                    fqn=self._fqn,
                    col_list=self._col_list,
                    columns=self._columns,
                    staging_table=self._staging_table,
                    conflict_columns=self._conflict_columns,
                    conflict_action=self._conflict_action,
                    logger=self._engine.logger,
                )
            else:
                self.rows_merged = append_merge(
                    cur,
                    fqn=self._fqn,
                    col_list=self._col_list,
                    staging_table=self._staging_table,
                    logger=self._engine.logger,
                )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_staging_table(self) -> None:
        with self._engine.cursor() as cur:
            cur.execute(
                f"create temp table {self._staging_table} "
                f"(like {self._fqn} including defaults excluding indexes)"
            )
            if self._entity_key:
                cur.execute(
                    f'alter table {self._staging_table} alter column "record_hash" drop not null'
                )
        self._created = True
        self._engine.logger.info("Created staging table %s", self._staging_table)

    def _drop_staging_table(self) -> None:
        if not self._created:
            return
        try:
            with self._engine.cursor() as cur:
                cur.execute(f"drop table if exists {self._staging_table}")
            self._engine.logger.info("Dropped staging table %s", self._staging_table)
        except Exception as e:
            self._engine.logger.warning(
                "Failed to drop staging table %s: %s", self._staging_table, e
            )

    def _get_target_columns(self) -> list[str]:
        """Get column names from the target table, excluding metadata columns."""
        with self._engine.cursor() as cur:
            cur.execute(
                "select column_name from information_schema.columns "
                "where table_schema = %s and table_name = %s "
                "order by ordinal_position",
                (self._target_schema, self._target_table),
            )
            all_columns = [row[0] for row in cur.fetchall()]

        if self._metadata_columns:
            return [c for c in all_columns if c not in self._metadata_columns]
        return all_columns

    def _rows_to_copy_buffer(self, rows: list[dict[str, Any]]) -> io.StringIO:
        assert self._columns is not None  # set in __enter__
        buf = io.StringIO()
        for row in rows:
            vals = []
            for c in self._columns:
                v = row.get(c)
                if v is None:
                    vals.append("\\N")
                else:
                    vals.append(
                        str(v)
                        .replace("\\", "\\\\")
                        .replace("\t", " ")
                        .replace("\n", " ")
                        .replace("\r", " ")
                    )
            buf.write("\t".join(vals) + "\n")
        buf.seek(0)
        return buf

    def _cast_geometry_if_needed(self) -> None:
        """If the target table has a PostGIS geometry column, cast it in staging."""
        try:
            geom_col = self._engine._get_geometry_column(self._target_table, self._target_schema)
        except ValueError:
            return  # no geometry column — nothing to do

        self._engine.logger.info("Casting geometry column '%s' in staging table", geom_col)
        with self._engine.cursor() as cur:
            cur.execute(
                f"alter table {self._staging_table} "
                f'alter column "{geom_col}" type geometry '
                f'using "{geom_col}"::geometry'
            )


# ----------------------------------------------------------------------
# Per-mode merge routines
#
# Each takes an open cursor and the resolved table/column names, runs the
# integration SQL for one write mode, and returns what it changed. They are
# module-level (not methods) so the SQL for each mode reads top-to-bottom in
# one place; ``StagedIngest._run_merge`` selects between them.
# ----------------------------------------------------------------------


@dataclass
class Scd2Result:
    """What an :func:`scd2_merge` call changed in the target table."""

    rows_merged: int = 0
    rows_invalidated: int = 0


def append_merge(
    cur,
    *,
    fqn: str,
    col_list: str,
    staging_table: str,
    logger: logging.Logger,
) -> int:
    """``Append``: INSERT every staged row into the target.

    No key, no conflict handling — the target accumulates every row it is
    given, duplicates included. Returns the number of rows inserted.
    """
    cur.execute(f"insert into {fqn} ({col_list}) select {col_list} from {staging_table}")
    rows_merged = cur.rowcount
    logger.info("Merged %d rows into %s", rows_merged, fqn)
    return rows_merged


def upsert_merge(
    cur,
    *,
    fqn: str,
    col_list: str,
    columns: list[str],
    staging_table: str,
    conflict_columns: list[str],
    conflict_action: str,
    logger: logging.Logger,
) -> int:
    """``Upsert``: INSERT ... ON CONFLICT into the target.

    ``conflict_action`` is ``"NOTHING"`` (keep the existing row, ignore the
    incoming duplicate) or ``"UPDATE"`` (overwrite the conflicting row's
    non-key columns from the incoming row). Returns the number of rows
    inserted or updated.
    """
    insert_sql = f"insert into {fqn} ({col_list}) select {col_list} from {staging_table}"
    conflict_clause = ", ".join(f'"{c}"' for c in conflict_columns)
    if conflict_action.upper() == "UPDATE":
        update_cols = [c for c in columns if c not in conflict_columns]
        set_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in update_cols)
        insert_sql += f" on conflict ({conflict_clause}) do update set {set_clause}"
    else:
        insert_sql += f" on conflict ({conflict_clause}) do nothing"

    cur.execute(insert_sql)
    rows_merged = cur.rowcount
    logger.info("Upserted %d rows into %s", rows_merged, fqn)
    return rows_merged


def scd2_merge(
    cur,
    *,
    fqn: str,
    staging_table: str,
    columns: list[str],
    col_list: str,
    entity_key: list[str],
    hash_exclude: set[str] | None,
    invalidate_missing: bool,
    rows_staged: int,
    logger: logging.Logger,
) -> Scd2Result:
    """``SCD2``: append a new version only when an entity's content changes.

    Order matters:

    1. Compute record_hash on staging rows.
    2. If invalidate_missing is True, invalidate current target rows
       whose entity_key is absent from staging. This step must run
       BEFORE the dedupe in step 3, because dedupe removes rows from
       staging that would otherwise prove an entity is still present.
    3. Drop staging rows whose (entity_key, record_hash) already
       exists anywhere in target history. These are not new versions
       — they're either duplicates of the current row or replays of
       a previously-seen historical version. Keeping them would
       cause the close-out in step 4 to invalidate the current row
       without a replacement landing, since the insert in step 5
       would no-op on the unique constraint.
    4. Close out current versions in target whose hash differs from
       staging (entity exists in both, but content changed).
    5. Insert new versions (new entities + changed entities).
    """
    hash_columns = _hash_columns(columns, entity_key, hash_exclude, logger)
    hash_expr = _build_hash_expression(hash_columns)
    entity_join = " and ".join(f't."{k}" = s."{k}"' for k in entity_key)
    entity_conflict = ", ".join(f'"{k}"' for k in entity_key)
    result = Scd2Result()

    # 1. Compute record_hash on staging rows
    cur.execute(f'alter table {staging_table} add column if not exists "record_hash" text')
    cur.execute(f'update {staging_table} set "record_hash" = {hash_expr}')

    # 2. Invalidate current target rows whose entity_key is absent
    #    from staging. Must run before dedupe so staging still
    #    contains evidence that unchanged entities are present.
    if invalidate_missing:
        cur.execute(f"""
            update {fqn} t
            set "valid_to" = now() at time zone 'utc'
            where "valid_to" is null
              and not exists (
                select 1 from {staging_table} s
                where {entity_join}
              )
        """)
        result.rows_invalidated = cur.rowcount
        logger.info(
            "SCD2: invalidated %d removed entities in %s",
            result.rows_invalidated,
            fqn,
        )

    # 3. Dedupe staging against target history. Any (entity_key,
    #    record_hash) that already exists in target — current or
    #    closed — is not a new version and should not drive the
    #    close-out below.
    cur.execute(f"""
        delete from {staging_table} s
        using {fqn} t
        where {entity_join}
          and t."record_hash" = s."record_hash"
    """)
    rows_deduped = cur.rowcount
    logger.info(
        "SCD2: dropped %d staging rows whose (entity_key, record_hash) already exists in %s",
        rows_deduped,
        fqn,
    )

    # 4. Close out current versions that have a new incoming version
    #    (entity exists in both, but hash differs)
    cur.execute(f"""
        update {fqn} t
        set "valid_to" = now() at time zone 'utc'
        where "valid_to" is null
          and exists (
            select 1 from {staging_table} s
            where {entity_join}
              and s."record_hash" != t."record_hash"
          )
    """)
    rows_closed = cur.rowcount
    logger.info("SCD2: closed out %d superseded versions in %s", rows_closed, fqn)

    # 5. Insert new versions. The on conflict clause is
    #    belt-and-suspenders — step 3 already removed any staging
    #    rows that would conflict — but it's cheap and guards
    #    against any future code path that might bypass dedupe.
    select_cols = ", ".join(f's."{c}"' for c in columns)
    insert_col_list = f'{col_list}, "record_hash"'
    cur.execute(f"""
        insert into {fqn} ({insert_col_list})
        select {select_cols}, s."record_hash"
        from {staging_table} s
        on conflict ({entity_conflict}, "record_hash") do nothing
    """)
    result.rows_merged = cur.rowcount

    logger.info(
        "SCD2: inserted %d new versions into %s (staged %d, deduped %d, invalidated %d, closed %d)",
        result.rows_merged,
        fqn,
        rows_staged,
        rows_deduped,
        result.rows_invalidated,
        rows_closed,
    )
    return result


def _hash_columns(
    columns: list[str],
    entity_key: list[str],
    hash_exclude: set[str] | None,
    logger: logging.Logger,
) -> list[str]:
    """The columns whose values define an entity version (drive the hash).

    Excludes the entity_key (identity, not content) and any metadata columns
    (which change every run regardless of content).
    """
    exclude = set(entity_key) | set(hash_exclude or set())
    hash_cols = [c for c in columns if c not in exclude]
    if not hash_cols:
        raise ValueError(
            f"No columns to hash after excluding entity_key {entity_key} "
            f"and metadata columns {sorted(hash_exclude or set())}"
        )
    logger.info("SCD2: hashing columns: %s", hash_cols)
    return hash_cols


def _build_hash_expression(columns: list[str]) -> str:
    """A SQL MD5 expression over the given columns (null-safe, '|'-joined)."""
    parts = [f"""coalesce("{c}"::text, '')""" for c in columns]
    concatenated = " || '|' || ".join(parts)
    return f"md5({concatenated})"
