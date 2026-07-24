"""PostgreSQL storage engine.

``PostgresEngine`` is the connection/query/DDL workhorse for Postgres +
PostGIS. It implements the ``datadongle.core`` Engine protocol
(``open_write``/``ensure_table``/``read_high_water_mark``/...) on top of a
small set of DB primitives, and retains the lower-level ``staged_ingest`` and
``query`` helpers used directly by the not-yet-migrated collectors.

The per-mode staged-merge SQL lives in ``engines.postgres_load``; credentials,
logging, and retry helpers stay in ``db.core`` (shared with MySQL).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2.extensions import connection as Psycopg2Connection

from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, Append, Upsert, WriteMode
from datadongle.db.core import DatabaseCredentials, get_logger, pg_retry
from datadongle.engines.postgres_load import StagedIngest

# Neutral column type -> PostgreSQL type.
_PG_TYPES: dict[ColumnType, str] = {
    ColumnType.TEXT: "text",
    ColumnType.INTEGER: "integer",
    ColumnType.BIGINT: "bigint",
    ColumnType.NUMERIC: "numeric",
    ColumnType.DOUBLE: "double precision",
    ColumnType.BOOLEAN: "boolean",
    ColumnType.TIMESTAMP: "timestamp",
    ColumnType.TIMESTAMPTZ: "timestamptz",
    ColumnType.DATE: "date",
    ColumnType.JSON: "jsonb",
    ColumnType.RASTER: "raster",
}

# Pipeline columns the engine fills itself (excluded from the staged COPY
# column list). Every table gets ``ingested_at``; SCD2 tables also get the
# versioning trio.
_INGESTED_AT = "ingested_at"
_SCD2_COLUMNS = ("record_hash", "valid_from", "valid_to")

_TIMESTAMP_TYPES = {"timestamp with time zone", "timestamp without time zone"}
# Canonical ISO-8601 microsecond form for a timestamp high-water mark.
_HWM_TS_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.US'


class PostgresEngine:
    def __init__(self, creds: DatabaseCredentials, db_name: str | None = None) -> None:
        self.creds = creds
        self.db_name = db_name or creds.database
        self._conn: Psycopg2Connection | None = None
        self.logger = get_logger("postgres_engine")
        self._geometry_info_cache: dict[tuple[str, str], dict[str, int]] = {}

    def _connect(self) -> Psycopg2Connection:
        return psycopg2.connect(
            host=self.creds.host,
            port=self.creds.port,
            dbname=self.db_name,
            user=self.creds.username,
            password=self.creds.password,
            # Pin the session to UTC so naive timestamp strings COPYed into
            # TIMESTAMPTZ columns are read as UTC instants (not the server's
            # local zone) and round-trip identically through
            # ``read_high_water_mark``. Socrata cursors are UTC; this keeps the
            # engine deterministic regardless of the server's ``timezone`` GUC
            # and conformant with IcebergEngine.
            options="-c timezone=UTC",
        )

    @contextmanager
    def transaction(self):
        try:
            yield self.connection
            self.connection.commit()
        except Exception as e:
            self.logger.error(f"Transaction failed with error {e}")
            self.connection.rollback()
            raise

    @contextmanager
    def cursor(self):
        with self.transaction():
            cur = self.connection.cursor()
            try:
                yield cur
            finally:
                cur.close()

    @property
    def connection(self) -> Psycopg2Connection:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def staged_ingest(
        self,
        target_table: str,
        target_schema: str,
        conflict_column: str | list[str] | None = None,
        conflict_action: str = "NOTHING",
        entity_key: list[str] | None = None,
        metadata_columns: set[str] | None = None,
        hash_exclude_columns: set[str] | None = None,
        invalidate_missing: bool = False,
    ) -> StagedIngest:
        """
        Return a StagedIngest context manager that accumulates batches
        into a staging table, then merges into the target on exit.

        For simple upsert:
            with engine.staged_ingest("crimes", "raw_data",
                                       conflict_column=["case_number"],
                                       conflict_action="UPDATE") as stager:
                stager.write_batch(rows)

        For SCD Type 2:
            with engine.staged_ingest("crimes", "raw_data",
                                       entity_key=["case_number"]) as stager:
                stager.write_batch(rows)

        For SCD Type 2 with full-refresh invalidation:
            with engine.staged_ingest("edges", "raw_data",
                                       entity_key=["u", "v", "key"],
                                       invalidate_missing=True) as stager:
                stager.write_batch(rows)

        print(stager.rows_staged, stager.rows_merged, stager.rows_invalidated)
        """
        return StagedIngest(
            engine=self,
            target_table=target_table,
            target_schema=target_schema,
            conflict_column=conflict_column,
            conflict_action=conflict_action,
            entity_key=entity_key,
            metadata_columns=metadata_columns,
            hash_exclude_columns=hash_exclude_columns,
            invalidate_missing=invalidate_missing,
        )

    @pg_retry()
    def query(
        self,
        sql: str,
        params: dict[str, Any] | tuple | None = None,
        as_dicts: bool = False,
    ) -> pd.DataFrame | list[dict]:
        """
        Execute a SELECT and return results as a DataFrame or list of dicts.

        If the result set includes a PostGIS geometry column, returns a
        GeoDataFrame with the geometry parsed and CRS set from the table's
        SRID. The geometry detection uses a cached lookup against the
        PostGIS geometry_columns catalog, so overhead on non-spatial
        queries is negligible after the first call per table.

        Args:
            sql:      SQL string. Use %(name)s for named params or %s for positional.
            params:   Dict for named params, tuple for positional, or None.
            as_dicts: If True, return a list of dicts instead of a DataFrame.
        """
        with self.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            type_oids = [desc[1] for desc in cur.description]
            rows = cur.fetchall()

        if as_dicts:
            return [dict(zip(columns, row, strict=True)) for row in rows]

        df = pd.DataFrame(rows, columns=columns)

        if df.empty:
            return df

        geom_col, srid = self._detect_geometry_by_oid(columns, type_oids)
        if geom_col is None:
            return df

        return self._to_geodataframe(df, geom_col, srid)

    def _detect_geometry_by_oid(
        self, columns: list[str], type_oids: list[int]
    ) -> tuple[str | None, int]:
        """Detect geometry columns by checking the Postgres type OID, not the column name."""
        geom_oid = self._get_geometry_type_oid()
        if geom_oid is None:
            return None, 0

        for col_name, oid in zip(columns, type_oids, strict=True):
            if oid == geom_oid:
                # Check SRID cache first
                for (_schema, _table), info in self._geometry_info_cache.items():
                    if col_name in info:
                        return col_name, info[col_name]

                # Fall back to geometry_columns catalog
                try:
                    with self.cursor() as cur:
                        cur.execute(
                            "select srid from geometry_columns "
                            "where f_geometry_column = %s limit 1",
                            (col_name,),
                        )
                        row = cur.fetchone()
                        if row:
                            return col_name, row[0]
                except Exception:
                    pass

                return col_name, 0

        return None, 0

    def _get_geometry_type_oid(self) -> int | None:
        """Look up the OID for the PostGIS geometry type. Cached after first call."""
        if not hasattr(self, "_geometry_oid"):
            try:
                with self.cursor() as cur:
                    cur.execute("select 'geometry'::regtype::oid")
                    self._geometry_oid = cur.fetchone()[0]
            except Exception:
                self._geometry_oid = None
        return self._geometry_oid

    @staticmethod
    def _to_geodataframe(df: pd.DataFrame, geom_col: str, srid: int) -> gpd.GeoDataFrame:
        """Convert a DataFrame with a WKB/EWKB hex geometry column to a GeoDataFrame."""
        import shapely

        try:

            def _parse_geom(v):
                if v is None:
                    return None
                try:
                    return shapely.from_wkb(v)
                except Exception:
                    # EWKB with embedded SRID: strip the SRID flag and 4 SRID bytes
                    # Byte 4 (hex chars 6-7) has flag 0x20 set; remove it and the
                    # 4-byte SRID that follows (hex chars 8-15)
                    byte4 = int(v[6:8], 16)
                    if byte4 & 0x20:
                        byte4 &= ~0x20
                        v = v[:6] + f"{byte4:02x}" + v[16:]
                    return shapely.from_wkb(v)

            df[geom_col] = df[geom_col].apply(_parse_geom)
            crs = f"EPSG:{srid}" if srid else None
            return gpd.GeoDataFrame(df, geometry=geom_col, crs=crs)
        except Exception as e:
            print(f"Encountered exception {e}")
            print(f"geom_col:  {geom_col}")
            print(f"srid:      {srid}")
            raise

    @pg_retry()
    def execute(
        self,
        sql: str,
        params: dict[str, Any] | tuple | None = None,
    ) -> None:
        """
        Execute a DDL/DML statement (no result set).

        Args:
            sql:    SQL string. Use %(name)s for named params or %s for positional.
            params: Dict for named params, tuple for positional, or None.
        """
        try:
            with self.cursor() as cur:
                cur.execute(sql, params)
        except Exception as e:
            self.logger.error(f"Command failed with error {e}")
            raise

    @pg_retry()
    def query_batches(
        self,
        sql: str,
        params: dict[str, Any] | tuple | None = None,
        batch_size: int = 10000,
        as_dicts: bool = True,
    ) -> Iterator[list[dict] | pd.DataFrame]:
        """
        Server-side cursor for large result sets, yielded in batches.

        Args:
            sql:        SQL string with optional parameter placeholders.
            params:     Dict for named params, tuple for positional, or None.
            batch_size: Rows per batch.
            as_dicts:   If True, yield list[dict]; otherwise yield DataFrames.
        """
        cursor = self.connection.cursor(name="batch_cursor")
        cursor.itersize = batch_size
        try:
            cursor.execute(sql, params)
            columns = None

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                if columns is None:
                    columns = [desc[0] for desc in cursor.description]

                if as_dicts:
                    yield [dict(zip(columns, row, strict=True)) for row in rows]
                else:
                    yield pd.DataFrame(rows, columns=columns)
        except Exception as e:
            self.logger.error(f"Query batches failed: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def _get_geometry_info(self, target_table: str, target_schema: str) -> dict[str, int]:
        """
        Return a dict of {geometry_column_name: srid} for the given table.

        Results are cached per (schema, table) for the lifetime of the engine.
        Returns an empty dict if the table has no geometry columns.
        """
        cache_key = (target_schema, target_table)
        if cache_key in self._geometry_info_cache:
            return self._geometry_info_cache[cache_key]

        try:
            df = self.query(
                """
                select f_geometry_column, srid
                from geometry_columns
                where f_table_schema = %(schema)s and f_table_name = %(table)s
                """,
                {"schema": target_schema, "table": target_table},
            )
            info = {row["f_geometry_column"]: row["srid"] for _, row in df.iterrows()}
        except Exception:
            info = {}

        self._geometry_info_cache[cache_key] = info
        return info

    def _get_geometry_column(self, target_table: str, target_schema: str) -> str:
        """Look up the geometry column name from PostGIS metadata."""
        info = self._get_geometry_info(target_table, target_schema)
        if not info:
            raise ValueError(f"No geometry column found for {target_schema}.{target_table}")
        return next(iter(info))

    def stream_to_destination(
        self,
        sql: str,
        process_batch: Callable[..., Any],
        params: dict[str, Any] | tuple | None = None,
        batch_size: int = 10000,
    ) -> int:
        total = 0
        for batch in self.query_batches(sql, params=params, batch_size=batch_size, as_dicts=True):
            process_batch(batch)
            total += len(batch)
        return total

    @staticmethod
    def _normalize_json_values(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        import ast
        import json

        def _to_json(val: Any) -> Any:
            if isinstance(val, dict):
                return json.dumps({k: _to_json(v) for k, v in val.items()})
            if isinstance(val, str) and val.startswith("{"):
                try:
                    parsed = ast.literal_eval(val)
                    if isinstance(parsed, dict):
                        return json.dumps({k: _to_json(v) for k, v in parsed.items()})
                except (ValueError, SyntaxError):
                    pass
            return val

        for row in rows:
            for key, val in row.items():
                row[key] = _to_json(val)
        return rows

    # ------------------------------------------------------------------
    # Engine protocol (datadongle.core.engine)
    # ------------------------------------------------------------------

    @staticmethod
    def _schema_of(target: TableRef) -> str:
        """The PostgreSQL schema for ``target`` (namespace, default ``public``)."""
        return target.namespace or "public"

    def _fqn(self, target: TableRef) -> str:
        return f"{self._schema_of(target)}.{target.name}"

    def open_write(
        self, target: TableRef, schema: TableSchema, mode: WriteMode
    ) -> StagedIngest:
        """Open a staged write realizing ``mode`` on this table."""
        kwargs: dict[str, Any] = {
            "target_table": target.name,
            "target_schema": self._schema_of(target),
        }
        if isinstance(mode, SCD2):
            kwargs.update(
                entity_key=mode.entity_key,
                invalidate_missing=mode.invalidate_missing,
                metadata_columns={_INGESTED_AT, *_SCD2_COLUMNS},
                hash_exclude_columns=schema.metadata_column_names(),
            )
        elif isinstance(mode, Upsert):
            kwargs.update(
                conflict_column=list(mode.keys),
                conflict_action=mode.on_conflict.upper(),
                metadata_columns={_INGESTED_AT},
            )
        elif isinstance(mode, Append):
            kwargs.update(metadata_columns={_INGESTED_AT})
        else:
            raise TypeError(f"Unsupported write mode for PostgresEngine: {mode!r}")
        return self.staged_ingest(**kwargs)

    def ensure_table(
        self, target: TableRef, schema: TableSchema, mode: WriteMode
    ) -> None:
        """Idempotently create ``target`` for ``schema`` under ``mode``."""
        self.execute(self._render_create_table(target, schema, mode))

    def _render_create_table(
        self, target: TableRef, schema: TableSchema, mode: WriteMode
    ) -> str:
        fqn = self._fqn(target)
        col_defs = [self._render_column(c) for c in schema.columns]
        col_defs.append(
            f'"{_INGESTED_AT}" timestamptz not null default (now() at time zone \'UTC\')'
        )
        if isinstance(mode, SCD2):
            col_defs.append('"record_hash" text not null')
            col_defs.append(
                "\"valid_from\" timestamptz not null default (now() at time zone 'utc')"
            )
            col_defs.append('"valid_to" timestamptz')

        ddl = f"create table if not exists {fqn} (\n  " + ",\n  ".join(col_defs) + "\n);\n"

        if isinstance(mode, SCD2):
            ek = ", ".join(f'"{k}"' for k in mode.entity_key)
            ddl += (
                f"create unique index if not exists uq_{target.name}_entity_hash\n"
                f'    on {fqn} ({ek}, "record_hash");\n'
            )
            ddl += (
                f"create index if not exists ix_{target.name}_current\n"
                f'    on {fqn} ({ek}) where "valid_to" is null;\n'
            )

        # A raster column gets a GiST index on its convex hull — that is what
        # serves ST_Intersects(rast, point) sampling. Under SCD2 it is partial
        # over current rows, since sampling queries filter to them.
        for name in schema.raster_column_names():
            ddl += (
                f"create index if not exists ix_{target.name}_{name}\n"
                f'    on {fqn} using gist (ST_ConvexHull("{name}"))'
            )
            if isinstance(mode, SCD2):
                ddl += ' where "valid_to" is null'
            ddl += ";\n"
        return ddl

    @staticmethod
    def _render_column(col) -> str:
        if col.type is ColumnType.GEOMETRY:
            g = col.geometry
            pg_type = f"geometry({g.kind},{g.srid})"
        else:
            pg_type = _PG_TYPES[col.type]
        frag = f'"{col.name}" {pg_type}'
        if not col.nullable:
            frag += " not null"
        return frag

    def table_exists(self, target: TableRef) -> bool:
        df = self.query("select to_regclass(%(fqn)s) as reg", {"fqn": self._fqn(target)})
        return bool(df["reg"].iloc[0] is not None)

    def table_columns(self, target: TableRef) -> set[str]:
        df = self.query(
            """
            select column_name
            from information_schema.columns
            where table_schema = %(schema)s and table_name = %(table)s
            """,
            {"schema": self._schema_of(target), "table": target.name},
        )
        return set(df["column_name"]) if not df.empty else set()

    def geometry_columns(self, target: TableRef) -> dict[str, int]:
        return dict(self._get_geometry_info(target.name, self._schema_of(target)))

    def read_high_water_mark(
        self, target: TableRef, cursor: CursorSpec
    ) -> Cursor | None:
        """Read the max cursor value (and tiebreak at that max) from the table.

        Timestamp cursor columns are formatted to a canonical ISO-8601
        microsecond UTC string; other types are cast to text. Returns ``None``
        if the table is absent or empty.
        """
        if not self.table_exists(target):
            return None

        fqn = self._fqn(target)
        value_expr = self._hwm_value_expr(target, cursor.column)

        df = self.query(
            f'select {value_expr} as hwm_value from {fqn} '
            f'where "{cursor.column}" is not null '
            f'order by "{cursor.column}" desc limit 1'
        )
        if df.empty or df["hwm_value"].iloc[0] is None:
            return None
        hwm_value = str(df["hwm_value"].iloc[0])

        tiebreak: str | None = None
        if cursor.tiebreak:
            df_tb = self.query(
                f'select "{cursor.tiebreak}"::text as tb from {fqn} '
                f"where {value_expr} = %(v)s and \"{cursor.tiebreak}\" is not null "
                f'order by "{cursor.tiebreak}" desc limit 1',
                {"v": hwm_value},
            )
            if not df_tb.empty and df_tb["tb"].iloc[0] is not None:
                tiebreak = str(df_tb["tb"].iloc[0])

        return Cursor(value=hwm_value, tiebreak=tiebreak)

    def _hwm_value_expr(self, target: TableRef, column: str) -> str:
        """SQL expression yielding the cursor column as a comparable string."""
        df = self.query(
            """
            select data_type
            from information_schema.columns
            where table_schema = %(schema)s and table_name = %(table)s
              and column_name = %(col)s
            """,
            {"schema": self._schema_of(target), "table": target.name, "col": column},
        )
        data_type = df["data_type"].iloc[0] if not df.empty else None
        if data_type in _TIMESTAMP_TYPES:
            return f"to_char(\"{column}\" at time zone 'UTC', '{_HWM_TS_FORMAT}')"
        return f'"{column}"::text'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
