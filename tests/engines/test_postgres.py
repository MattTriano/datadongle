"""Unit tests for the PostgresEngine Engine-protocol surface.

These mock the DB cursor / query so they run without a Postgres. The
staged-ingest merge SQL itself is covered by tests/db/test_core.py.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema
from datadongle.core.write_mode import SCD2, Append, Upsert, WriteMode
from datadongle.db.core import DatabaseCredentials
from datadongle.engines.postgres import PostgresEngine
from datadongle.engines.postgres_load import StagedIngest


@pytest.fixture
def creds():
    return DatabaseCredentials(
        host="h", port=5432, database="d", username="u", password="p"
    )


@pytest.fixture
def mock_cursor():
    cur = MagicMock()
    cur.description = [("x", 23, None, None, None, None, None)]
    cur.fetchall.return_value = []
    cur.fetchone.return_value = None
    cur.rowcount = 0
    return cur


@pytest.fixture
def mock_conn(mock_cursor):
    conn = MagicMock()
    conn.closed = False
    conn.cursor.return_value = mock_cursor
    return conn


@pytest.fixture
def engine(mock_conn, creds):
    eng = PostgresEngine(creds)
    eng._conn = mock_conn
    return eng


def _executed_sql(mock_cursor) -> str:
    """All SQL executed, concatenated (ensure_table emits one multi-statement string)."""
    return "\n".join(
        str(c[0][0]) for c in mock_cursor.execute.call_args_list if c[0]
    )


# ---------------------------------------------------------------- ensure_table


SAMPLE_COLUMNS = [
    Column("id", ColumnType.TEXT, nullable=False),
    Column("amount", ColumnType.DOUBLE),
    Column("payload", ColumnType.JSON),
    Column("socrata_id", ColumnType.TEXT, metadata=True),
    Column("geom", ColumnType.GEOMETRY, geometry=GeometrySpec(kind="Point", srid=4326)),
]


def test_ensure_table_scd2_ddl(engine, mock_cursor):
    schema = TableSchema(columns=SAMPLE_COLUMNS)
    engine.ensure_table(
        TableRef("crimes", "raw_data"), schema, SCD2(entity_key=["id"])
    )
    sql = _executed_sql(mock_cursor)

    assert "create table if not exists raw_data.crimes" in sql
    # every table gets ingested_at
    assert '"ingested_at" timestamptz not null' in sql
    # SCD2 versioning columns
    assert '"record_hash" text not null' in sql
    assert '"valid_from" timestamptz not null' in sql
    assert '"valid_to" timestamptz' in sql
    # a not-null source column keeps its constraint
    assert '"id" text not null' in sql
    # type mapping + geometry rendering
    assert '"amount" double precision' in sql
    assert '"payload" jsonb' in sql
    assert '"geom" geometry(Point,4326)' in sql
    # constraint + current-version index, both idempotent
    assert 'create unique index if not exists uq_crimes_entity_hash' in sql
    assert '"id", "record_hash"' in sql
    assert 'create index if not exists ix_crimes_current' in sql
    assert 'where "valid_to" is null' in sql


def test_ensure_table_append_has_no_scd2_apparatus(engine, mock_cursor):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT)])
    engine.ensure_table(TableRef("events", "raw_data"), schema, Append())
    sql = _executed_sql(mock_cursor)

    assert "create table if not exists raw_data.events" in sql
    assert '"ingested_at" timestamptz not null' in sql
    assert "record_hash" not in sql
    assert "valid_to" not in sql
    assert "create unique index" not in sql
    assert "create index" not in sql


def test_ensure_table_defaults_namespace_to_public(engine, mock_cursor):
    engine.ensure_table(
        TableRef("t"), TableSchema(columns=[Column("id", ColumnType.TEXT)]), Append()
    )
    assert "create table if not exists public.t" in _executed_sql(mock_cursor)


# ------------------------------------------------------------------ open_write


def test_open_write_scd2_configures_stager(engine):
    schema = TableSchema(columns=SAMPLE_COLUMNS)
    ws = engine.open_write(
        TableRef("crimes", "raw_data"), schema, SCD2(entity_key=["id"])
    )
    assert isinstance(ws, StagedIngest)
    assert ws._entity_key == ["id"]
    assert ws._invalidate_missing is False
    # engine-owned pipeline columns are excluded from the COPY column list
    assert ws._metadata_columns == {"ingested_at", "record_hash", "valid_from", "valid_to"}
    # source-metadata columns are excluded from the content hash
    assert ws._hash_exclude_columns == {"socrata_id"}


def test_open_write_scd2_passes_invalidate_missing(engine):
    schema = TableSchema(columns=SAMPLE_COLUMNS)
    ws = engine.open_write(
        TableRef("edges", "raw_data"),
        schema,
        SCD2(entity_key=["id"], invalidate_missing=True),
    )
    assert ws._invalidate_missing is True


def test_open_write_upsert_configures_conflict(engine):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT)])
    ws = engine.open_write(
        TableRef("t", "s"), schema, Upsert(keys=["id"], on_conflict="nothing")
    )
    assert ws._conflict_columns == ["id"]
    assert ws._conflict_action == "NOTHING"
    assert ws._entity_key is None


def test_open_write_append_configures_plain_insert(engine):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT)])
    ws = engine.open_write(TableRef("t", "s"), schema, Append())
    assert ws._entity_key is None
    assert ws._conflict_columns is None
    assert ws._metadata_columns == {"ingested_at"}


def test_open_write_rejects_unknown_mode(engine):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT)])
    with pytest.raises(TypeError, match="Unsupported write mode"):
        engine.open_write(TableRef("t", "s"), schema, WriteMode())


# ------------------------------------------------ introspection + high-water mark


def test_table_exists_true(engine):
    engine.query = MagicMock(return_value=pd.DataFrame({"reg": ["s.t"]}))
    assert engine.table_exists(TableRef("t", "s")) is True


def test_table_exists_false(engine):
    engine.query = MagicMock(return_value=pd.DataFrame({"reg": [None]}))
    assert engine.table_exists(TableRef("t", "s")) is False


def test_table_columns(engine):
    engine.query = MagicMock(
        return_value=pd.DataFrame({"column_name": ["id", "val", "ingested_at"]})
    )
    assert engine.table_columns(TableRef("t", "s")) == {"id", "val", "ingested_at"}


def test_geometry_columns(engine):
    engine._geometry_info_cache[("s", "t")] = {"geom": 4326}
    assert engine.geometry_columns(TableRef("t", "s")) == {"geom": 4326}


def test_read_high_water_mark_timestamp_cursor(engine):
    cursor = CursorSpec("socrata_updated_at", "socrata_id")
    engine.query = MagicMock(
        side_effect=[
            pd.DataFrame({"reg": ["raw_data.crimes"]}),  # table_exists
            pd.DataFrame({"data_type": ["timestamp with time zone"]}),  # _hwm_value_expr
            pd.DataFrame({"hwm_value": ["2024-01-03T00:00:00.000000"]}),  # max
            pd.DataFrame({"tb": ["42"]}),  # tiebreak at max
        ]
    )
    hwm = engine.read_high_water_mark(TableRef("crimes", "raw_data"), cursor)
    assert hwm == Cursor(value="2024-01-03T00:00:00.000000", tiebreak="42")

    # a timestamp cursor is formatted to canonical ISO, not raw ::text
    max_sql = engine.query.call_args_list[2][0][0]
    assert "to_char" in max_sql


def test_read_high_water_mark_absent_table_returns_none(engine):
    engine.query = MagicMock(return_value=pd.DataFrame({"reg": [None]}))
    assert engine.read_high_water_mark(TableRef("t", "s"), CursorSpec("c")) is None


def test_read_high_water_mark_empty_table_returns_none(engine):
    engine.query = MagicMock(
        side_effect=[
            pd.DataFrame({"reg": ["s.t"]}),  # table_exists
            pd.DataFrame({"data_type": ["text"]}),  # _hwm_value_expr
            pd.DataFrame({"hwm_value": []}),  # max — empty
        ]
    )
    assert engine.read_high_water_mark(TableRef("t", "s"), CursorSpec("c")) is None


def test_read_high_water_mark_no_tiebreak(engine):
    engine.query = MagicMock(
        side_effect=[
            pd.DataFrame({"reg": ["s.t"]}),
            pd.DataFrame({"data_type": ["text"]}),
            pd.DataFrame({"hwm_value": ["zzz"]}),
        ]
    )
    hwm = engine.read_high_water_mark(TableRef("t", "s"), CursorSpec("c"))
    assert hwm == Cursor(value="zzz", tiebreak=None)
