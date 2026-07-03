"""Local Iceberg storage engine (Shape-B, append-only SCD2).

``IcebergEngine`` collects datasets into a local-filesystem Iceberg warehouse
(PyIceberg + a SQLite catalog) and queries them via DuckDB. It implements the
``datadongle.core`` Engine protocol so it is interchangeable with
``PostgresEngine``.

Shape B: every distinct record version is *appended* (never updated). There is
no physical ``valid_to``/``is_current`` column — the current version of an
entity is derived at read time as the latest ``effective_from`` per
``entity_key``. This keeps writes cheap (pure appends, snapshots share files)
and lets ``expire_snapshots`` run freely since history lives in rows, not
snapshots.

No DuckDB native extensions are used (see the datadongle-iceberg-approach
memory): PyIceberg does all Iceberg I/O, core DuckDB does the change-detection
join, and shapely handles WKB geometry.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, Append, Upsert, WriteMode

logger = logging.getLogger(__name__)

# Pipeline columns the engine adds.
_INGESTED_AT = "ingested_at"
_SCD2_EXTRA = ("record_hash", "effective_from", "load_id")

# Table-property keys (persist the grain/geometry so reads are self-describing).
_PROP_MODE = "datadongle.mode"
_PROP_ENTITY_KEY = "datadongle.entity_key"
_PROP_METADATA_COLS = "datadongle.metadata_columns"
_PROP_GEOMETRY = "datadongle.geometry"

# Neutral column type -> pyarrow type.
_ARROW_TYPES: dict[ColumnType, pa.DataType] = {
    ColumnType.TEXT: pa.string(),
    ColumnType.JSON: pa.string(),
    ColumnType.INTEGER: pa.int32(),
    ColumnType.BIGINT: pa.int64(),
    ColumnType.NUMERIC: pa.float64(),
    ColumnType.DOUBLE: pa.float64(),
    ColumnType.BOOLEAN: pa.bool_(),
    ColumnType.TIMESTAMP: pa.timestamp("us"),
    ColumnType.TIMESTAMPTZ: pa.timestamp("us", tz="UTC"),
    ColumnType.DATE: pa.date32(),
    ColumnType.GEOMETRY: pa.binary(),
}

# Neutral column type -> DuckDB cast target (for coercing string input).
_DUCKDB_CASTS: dict[ColumnType, str] = {
    ColumnType.TEXT: "varchar",
    ColumnType.JSON: "varchar",
    ColumnType.INTEGER: "integer",
    ColumnType.BIGINT: "bigint",
    ColumnType.NUMERIC: "double",
    ColumnType.DOUBLE: "double",
    ColumnType.BOOLEAN: "boolean",
    ColumnType.TIMESTAMP: "timestamp",
    ColumnType.TIMESTAMPTZ: "timestamptz",
    ColumnType.DATE: "date",
}

_HWM_TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def _to_wkb(value: Any) -> bytes | None:
    """Normalize a geometry value (EWKT / WKT / WKB-hex / WKB) to WKB bytes."""
    if value is None:
        return None
    import shapely

    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    text = str(value).strip()
    if text.upper().startswith("SRID="):
        text = text.split(";", 1)[1]
    try:
        geom = shapely.from_wkt(text)
    except Exception:
        geom = shapely.from_wkb(bytes.fromhex(text))
    return shapely.to_wkb(geom)


class IcebergEngine:
    """Local Iceberg warehouse engine (Shape-B SCD2 + DuckDB queries)."""

    def __init__(self, warehouse: str, catalog_name: str = "datadongle") -> None:
        import os

        from pyiceberg.catalog.sql import SqlCatalog

        self.warehouse = str(warehouse)
        os.makedirs(self.warehouse, exist_ok=True)
        self.catalog = SqlCatalog(
            catalog_name,
            **{
                "uri": f"sqlite:///{self.warehouse}/catalog.db",
                "warehouse": f"file://{self.warehouse}",
            },
        )

    # ------------------------------------------------------------------
    # Identity helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _namespace(target: TableRef) -> str:
        return target.namespace or "default"

    def _identifier(self, target: TableRef) -> str:
        return f"{self._namespace(target)}.{target.name}"

    # ------------------------------------------------------------------
    # Schema construction
    # ------------------------------------------------------------------

    def _arrow_schema(self, schema: TableSchema, mode: WriteMode) -> pa.Schema:
        fields = [pa.field(c.name, _ARROW_TYPES[c.type]) for c in schema.columns]
        fields.append(pa.field(_INGESTED_AT, pa.timestamp("us", tz="UTC")))
        if isinstance(mode, SCD2):
            fields.append(pa.field("record_hash", pa.string()))
            fields.append(pa.field("effective_from", pa.timestamp("us", tz="UTC")))
            fields.append(pa.field("load_id", pa.string()))
        return pa.schema(fields)

    def ensure_table(
        self, target: TableRef, schema: TableSchema, mode: WriteMode
    ) -> None:
        if self.table_exists(target):
            return
        self.catalog.create_namespace_if_not_exists(self._namespace(target))
        properties = {
            _PROP_MODE: "scd2" if isinstance(mode, SCD2) else "append",
            _PROP_METADATA_COLS: ",".join(sorted(schema.metadata_column_names())),
            _PROP_GEOMETRY: json.dumps(
                {name: {"kind": g.kind, "srid": g.srid} for name, g in schema.geometry.items()}
            ),
        }
        if isinstance(mode, SCD2):
            properties[_PROP_ENTITY_KEY] = ",".join(mode.entity_key)
        self.catalog.create_table(
            self._identifier(target),
            schema=self._arrow_schema(schema, mode),
            properties=properties,
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def table_exists(self, target: TableRef) -> bool:
        return self.catalog.table_exists(self._identifier(target))

    def _load(self, target: TableRef):
        return self.catalog.load_table(self._identifier(target))

    def table_columns(self, target: TableRef) -> set[str]:
        if not self.table_exists(target):
            return set()
        return set(self._load(target).schema().column_names)

    def geometry_columns(self, target: TableRef) -> dict[str, int]:
        if not self.table_exists(target):
            return {}
        geom = json.loads(self._load(target).properties.get(_PROP_GEOMETRY, "{}"))
        return {name: spec["srid"] for name, spec in geom.items()}

    def _entity_key(self, table) -> list[str]:
        raw = table.properties.get(_PROP_ENTITY_KEY, "")
        return [c for c in raw.split(",") if c]

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def open_write(
        self, target: TableRef, schema: TableSchema, mode: WriteMode
    ) -> "IcebergWriteSession":
        if isinstance(mode, Upsert):
            raise NotImplementedError("IcebergEngine does not support Upsert yet.")
        if isinstance(mode, SCD2) and mode.invalidate_missing:
            raise NotImplementedError(
                "IcebergEngine does not support SCD2 invalidate_missing yet."
            )
        return IcebergWriteSession(self, target, schema, mode)

    # ------------------------------------------------------------------
    # Reads / queries
    # ------------------------------------------------------------------

    def _duckdb(self):
        import duckdb

        return duckdb.connect()

    def _history_arrow(self, target: TableRef, columns: tuple[str, ...] | None = None):
        table = self._load(target)
        scan = table.scan(selected_fields=columns) if columns else table.scan()
        return scan.to_arrow()

    def read_history(self, target: TableRef):
        """Every stored version as a (Geo)DataFrame."""
        return self._to_frame(target, self._history_arrow(target).to_pandas())

    def read_current(self, target: TableRef):
        """The current version of each entity as a (Geo)DataFrame."""
        table = self._load(target)
        entity_key = self._entity_key(table)
        con = self._duckdb()
        con.register("hist", table.scan().to_arrow())
        if not entity_key:
            df = con.sql("select * from hist").to_df()
        else:
            partition = ", ".join(f'"{k}"' for k in entity_key)
            df = con.sql(
                f"select * exclude (rn) from ("
                f"  select *, row_number() over ("
                f"    partition by {partition} "
                f"    order by effective_from desc, ingested_at desc, load_id desc"
                f"  ) rn from hist"
                f") where rn = 1"
            ).to_df()
        return self._to_frame(target, df)

    def query(self, sql: str, params: Any | None = None):
        """Run DuckDB SQL with every table registered as a view.

        Each table is available under its bare name (full history) and
        ``<name>_current`` (derived current versions).
        """
        con = self._duckdb()
        for ident in self._all_identifiers():
            target = self._target_from_identifier(ident)
            table = self._load(target)
            con.register(target.name, table.scan().to_arrow())
            entity_key = self._entity_key(table)
            if entity_key:
                partition = ", ".join(f'"{k}"' for k in entity_key)
                con.sql(
                    f'create view "{target.name}_current" as '
                    f"select * exclude (rn) from ("
                    f"  select *, row_number() over (partition by {partition} "
                    f"    order by effective_from desc, ingested_at desc, load_id desc) rn "
                    f"  from \"{target.name}\") where rn = 1"
                )
        return con.sql(sql).to_df()

    def _all_identifiers(self) -> list[str]:
        idents = []
        for ns in self.catalog.list_namespaces():
            for ident in self.catalog.list_tables(ns):
                idents.append(".".join(ident))
        return idents

    @staticmethod
    def _target_from_identifier(ident: str) -> TableRef:
        ns, _, name = ident.rpartition(".")
        return TableRef(name, ns or None)

    def _to_frame(self, target: TableRef, df):
        """Parse WKB geometry columns into shapely geometries (GeoDataFrame)."""
        geom = self.geometry_columns(target)
        if df.empty or not geom:
            return df
        import geopandas as gpd
        import shapely

        first = next(iter(geom))
        for name in geom:
            df[name] = df[name].apply(lambda v: shapely.from_wkb(bytes(v)) if v is not None else None)
        srid = geom[first]
        return gpd.GeoDataFrame(df, geometry=first, crs=f"EPSG:{srid}" if srid else None)

    def read_high_water_mark(
        self, target: TableRef, cursor: CursorSpec
    ) -> Cursor | None:
        if not self.table_exists(target):
            return None
        arrow = self._history_arrow(target)
        if arrow.num_rows == 0:
            return None
        con = self._duckdb()
        con.register("hist", arrow)
        value_expr = self._hwm_value_expr(arrow.schema, cursor.column)
        row = con.sql(
            f'select {value_expr} as hwm from hist '
            f'where "{cursor.column}" is not null '
            f'order by "{cursor.column}" desc limit 1'
        ).fetchone()
        if row is None or row[0] is None:
            return None
        hwm_value = str(row[0])

        tiebreak = None
        if cursor.tiebreak:
            tb = con.sql(
                f'select "{cursor.tiebreak}"::varchar as tb from hist '
                f"where {value_expr} = ? and \"{cursor.tiebreak}\" is not null "
                f'order by "{cursor.tiebreak}" desc limit 1',
                params=[hwm_value],
            ).fetchone()
            if tb is not None and tb[0] is not None:
                tiebreak = str(tb[0])
        return Cursor(value=hwm_value, tiebreak=tiebreak)

    @staticmethod
    def _hwm_value_expr(arrow_schema: pa.Schema, column: str) -> str:
        field = arrow_schema.field(column)
        if pa.types.is_timestamp(field.type):
            return f"strftime(\"{column}\" AT TIME ZONE 'UTC', '{_HWM_TS_FORMAT}')"
        return f'"{column}"::varchar'


class IcebergWriteSession:
    """Buffers batches, then Shape-B appends on a clean exit."""

    def __init__(
        self,
        engine: IcebergEngine,
        target: TableRef,
        schema: TableSchema,
        mode: WriteMode,
    ) -> None:
        self._engine = engine
        self._target = target
        self._schema = schema
        self._mode = mode
        self._rows: list[dict[str, Any]] = []
        self.rows_staged = 0
        self.rows_merged = 0
        self.rows_invalidated = 0

    def write_batch(self, rows: list[dict[str, Any]]) -> int:
        self._rows.extend(rows)
        self.rows_staged += len(rows)
        return len(rows)

    def __enter__(self) -> "IcebergWriteSession":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is None and self._rows:
            self._flush()
        return False

    def _flush(self) -> None:
        table = self._engine._load(self._target)
        run_ts = datetime.now(UTC)
        load_id = uuid.uuid4().hex

        incoming = self._incoming_arrow()
        con = self._engine._duckdb()
        con.register("incoming", incoming)

        select_typed = self._typed_select(run_ts, load_id)

        if isinstance(self._mode, SCD2):
            entity_key = self._mode.entity_key
            hist = table.scan(selected_fields=(*entity_key, "record_hash")).to_arrow()
            con.register("hist", hist)
            join = " and ".join(f't."{k}" = h."{k}"' for k in entity_key)
            partition = ", ".join(f't."{k}"' for k in entity_key)
            new_rows = con.sql(
                f"with typed as ({select_typed}) "
                f"select * exclude (rn) from ("
                f"  select t.*, row_number() over ("
                f"    partition by {partition}, t.record_hash order by t.effective_from"
                f"  ) rn "
                f"  from typed t "
                f"  left join hist h on {join} and t.record_hash = h.record_hash "
                f"  where h.record_hash is null"
                f") where rn = 1"
            ).to_arrow_table()
        else:
            new_rows = con.sql(select_typed).to_arrow_table()

        if new_rows.num_rows:
            table.append(new_rows.cast(table.schema().as_arrow()))
        self.rows_merged = new_rows.num_rows

    def _incoming_arrow(self) -> pa.Table:
        """Incoming rows as Arrow: geometry -> WKB binary, everything else string."""
        geom_cols = set(self._schema.geometry)
        columns: dict[str, pa.Array] = {}
        for col in self._schema.columns:
            if col.name in geom_cols:
                values = [_to_wkb(r.get(col.name)) for r in self._rows]
                columns[col.name] = pa.array(values, type=pa.binary())
            else:
                values = [
                    None if r.get(col.name) is None else str(r.get(col.name))
                    for r in self._rows
                ]
                columns[col.name] = pa.array(values, type=pa.string())
        return pa.table(columns)

    def _typed_select(self, run_ts: datetime, load_id: str) -> str:
        """DuckDB SELECT casting the string/blob incoming rows to typed columns."""
        geom_cols = set(self._schema.geometry)
        select_cols = []
        for col in self._schema.columns:
            if col.name in geom_cols:
                select_cols.append(f'"{col.name}"')
            else:
                cast = _DUCKDB_CASTS[col.type]
                select_cols.append(f'"{col.name}"::{cast} as "{col.name}"')

        ts = run_ts.isoformat()
        select_cols.append(f"timestamptz '{ts}' as {_INGESTED_AT}")

        if isinstance(self._mode, SCD2):
            select_cols.append(f"{self._hash_expr()} as record_hash")
            select_cols.append(f"timestamptz '{ts}' as effective_from")
            select_cols.append(f"'{load_id}' as load_id")

        return f"select {', '.join(select_cols)} from incoming"

    def _hash_expr(self) -> str:
        """MD5 over the semantic columns (excludes entity_key + metadata cols)."""
        exclude = set(self._mode.entity_key) | self._schema.metadata_column_names()
        geom_cols = set(self._schema.geometry)
        parts = []
        for col in self._schema.columns:
            if col.name in exclude:
                continue
            if col.name in geom_cols:
                parts.append(f"""coalesce(md5("{col.name}"), '')""")
            else:
                parts.append(f"""coalesce("{col.name}"::varchar, '')""")
        if not parts:
            raise ValueError("No columns to hash for SCD2 after excluding key + metadata.")
        return "md5(" + " || '|' || ".join(parts) + ")"
