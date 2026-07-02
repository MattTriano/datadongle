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
import uuid
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
        engine: "PostgresEngine",
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

        # buf = self._rows_to_copy_buffer(rows)
        import time

        t0 = time.monotonic()
        buf = self._rows_to_copy_buffer(rows)
        t1 = time.monotonic()

        with self._engine.cursor() as cur:
            cur.copy_expert(
                f"copy {self._staging_table} ({self._col_list}) "
                f"from stdin with (format text, NULL '\\N')",
                buf,
            )
        t2 = time.monotonic()

        self._engine.logger.info(
            "write_batch: %d rows (buffer=%.2fs, copy=%.2fs)",
            len(rows),
            t1 - t0,
            t2 - t1,
        )

        count = len(rows)
        self.rows_staged += count
        return count

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "StagedIngest":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            if self.rows_staged > 0:
                self._cast_geometry_if_needed()
                if self._entity_key:
                    self._scd2_merge()
                else:
                    self._merge()
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
    # Simple merge
    # ------------------------------------------------------------------

    def _merge(self) -> None:
        """INSERT from staging into target, with optional ON CONFLICT."""
        insert_sql = (
            f"insert into {self._fqn} ({self._col_list}) "
            f"select {self._col_list} from {self._staging_table}"
        )

        if self._conflict_columns:
            conflict_clause = ", ".join(f'"{c}"' for c in self._conflict_columns)

            if self._conflict_action.upper() == "UPDATE":
                update_cols = [c for c in self._columns if c not in self._conflict_columns]
                set_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in update_cols)
                insert_sql += f" on conflict ({conflict_clause}) do update set {set_clause}"
            else:
                insert_sql += f" on conflict ({conflict_clause}) do nothing"

        with self._engine.cursor() as cur:
            cur.execute(insert_sql)
            self.rows_merged = cur.rowcount

        self._engine.logger.info(
            "Merged %d rows into %s (staged %d)",
            self.rows_merged,
            self._fqn,
            self.rows_staged,
        )

    # ------------------------------------------------------------------
    # SCD2 merge
    # ------------------------------------------------------------------

    def _scd2_merge(self) -> None:
        """
        SCD Type 2 merge. Order matters:

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
        hash_columns = self._get_hash_columns()
        hash_expr = self._build_hash_expression(hash_columns)
        entity_join = " and ".join(f't."{k}" = s."{k}"' for k in self._entity_key)
        entity_conflict = ", ".join(f'"{k}"' for k in self._entity_key)

        with self._engine.cursor() as cur:
            # 1. Compute record_hash on staging rows
            cur.execute(
                f'alter table {self._staging_table} add column if not exists "record_hash" text'
            )
            cur.execute(f'update {self._staging_table} set "record_hash" = {hash_expr}')

            # 2. Invalidate current target rows whose entity_key is absent
            #    from staging. Must run before dedupe so staging still
            #    contains evidence that unchanged entities are present.
            if self._invalidate_missing:
                cur.execute(f"""
                    update {self._fqn} t
                    set "valid_to" = now() at time zone 'utc'
                    where "valid_to" is null
                      and not exists (
                        select 1 from {self._staging_table} s
                        where {entity_join}
                      )
                """)
                self.rows_invalidated = cur.rowcount
                self._engine.logger.info(
                    "SCD2: invalidated %d removed entities in %s",
                    self.rows_invalidated,
                    self._fqn,
                )

            # 3. Dedupe staging against target history. Any (entity_key,
            #    record_hash) that already exists in target — current or
            #    closed — is not a new version and should not drive the
            #    close-out below.
            cur.execute(f"""
                delete from {self._staging_table} s
                using {self._fqn} t
                where {entity_join}
                  and t."record_hash" = s."record_hash"
            """)
            rows_deduped = cur.rowcount
            self._engine.logger.info(
                "SCD2: dropped %d staging rows whose (entity_key, record_hash) "
                "already exists in %s",
                rows_deduped,
                self._fqn,
            )

            # 4. Close out current versions that have a new incoming version
            #    (entity exists in both, but hash differs)
            cur.execute(f"""
                update {self._fqn} t
                set "valid_to" = now() at time zone 'utc'
                where "valid_to" is null
                  and exists (
                    select 1 from {self._staging_table} s
                    where {entity_join}
                      and s."record_hash" != t."record_hash"
                  )
            """)
            rows_closed = cur.rowcount
            self._engine.logger.info(
                "SCD2: closed out %d superseded versions in %s",
                rows_closed,
                self._fqn,
            )

            # 5. Insert new versions. The on conflict clause is
            #    belt-and-suspenders — step 3 already removed any staging
            #    rows that would conflict — but it's cheap and guards
            #    against any future code path that might bypass dedupe.
            select_cols = ", ".join(f's."{c}"' for c in self._columns)
            insert_col_list = f'{self._col_list}, "record_hash"'

            cur.execute(f"""
                insert into {self._fqn} ({insert_col_list})
                select {select_cols}, s."record_hash"
                from {self._staging_table} s
                on conflict ({entity_conflict}, "record_hash") do nothing
            """)
            self.rows_merged = cur.rowcount

        self._engine.logger.info(
            "SCD2: inserted %d new versions into %s "
            "(staged %d, deduped %d, invalidated %d, closed %d)",
            self.rows_merged,
            self._fqn,
            self.rows_staged,
            rows_deduped,
            self.rows_invalidated,
            rows_closed,
        )

    def _get_hash_columns(self) -> list[str]:
        """Determine which columns to include in the record hash."""
        exclude = set(self._entity_key) | set(self._hash_exclude_columns or set())
        hash_cols = [c for c in self._columns if c not in exclude]
        if not hash_cols:
            raise ValueError(
                f"No columns to hash after excluding entity_key {self._entity_key} "
                f"and metadata columns {self._metadata_columns}"
            )
        self._engine.logger.info("SCD2: hashing columns: %s", hash_cols)
        return hash_cols

    @staticmethod
    def _build_hash_expression(columns: list[str]) -> str:
        """Build a SQL MD5 expression over the given columns."""
        parts = [f"""coalesce("{c}"::text, '')""" for c in columns]
        concatenated = " || '|' || ".join(parts)
        return f"md5({concatenated})"

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
