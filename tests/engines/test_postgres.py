"""Unit tests for the PostgresEngine Engine-protocol surface.

These mock the DB cursor / query so they run without a Postgres. The
staged-ingest merge SQL itself is covered by tests/db/test_core.py.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.exceptions import SchemaDriftError, TableNotFoundError
from datadongle.core.schema import (
    Column,
    ColumnType,
    GeometrySpec,
    TableSchema,
    TypeMismatch,
)
from datadongle.core.write_mode import SCD2, Append, Upsert, WriteMode
from datadongle.db.core import DatabaseCredentials
from datadongle.engines.postgres import PostgresEngine
from datadongle.engines.postgres_load import StagedIngest


@pytest.fixture
def creds():
    return DatabaseCredentials(host="h", port=5432, database="d", username="u", password="p")


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
    return "\n".join(str(c[0][0]) for c in mock_cursor.execute.call_args_list if c[0])


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
    engine.ensure_table(TableRef("crimes", "raw_data"), schema, SCD2(entity_key=["id"]))
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
    assert "create unique index if not exists uq_crimes_entity_hash" in sql
    assert '"id", "record_hash"' in sql
    assert "create index if not exists ix_crimes_current" in sql
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


def test_ensure_table_raster_column_gets_convex_hull_index(engine, mock_cursor):
    schema = TableSchema(
        columns=[
            Column("tile_id", ColumnType.TEXT, nullable=False),
            Column("rast", ColumnType.RASTER, nullable=False, metadata=True),
            Column("checksum", ColumnType.TEXT),
        ]
    )
    engine.ensure_table(TableRef("elevation", "raw_data"), schema, SCD2(entity_key=["tile_id"]))
    sql = _executed_sql(mock_cursor)

    assert '"rast" raster not null' in sql
    # The GiST convex-hull index that serves ST_Intersects sampling, partial
    # over current rows under SCD2.
    assert (
        'on raw_data.elevation using gist (ST_ConvexHull("rast")) where "valid_to" is null' in sql
    )


def test_ensure_table_raster_index_is_full_without_scd2(engine, mock_cursor):
    schema = TableSchema(columns=[Column("rast", ColumnType.RASTER)])
    engine.ensure_table(TableRef("tiles", "raw_data"), schema, Append())
    sql = _executed_sql(mock_cursor)

    assert 'using gist (ST_ConvexHull("rast"));' in sql
    assert "valid_to" not in sql


def test_ensure_table_defaults_namespace_to_public(engine, mock_cursor):
    engine.ensure_table(
        TableRef("t"), TableSchema(columns=[Column("id", ColumnType.TEXT)]), Append()
    )
    assert "create table if not exists public.t" in _executed_sql(mock_cursor)


def test_render_create_table_is_bare_and_needs_no_connection(engine):
    """The public renderer is what you paste into a migration, so no `if not exists`."""
    ddl = engine.render_create_table(
        TableRef("crimes", "raw_data"), TableSchema(columns=SAMPLE_COLUMNS), SCD2(entity_key=["id"])
    )
    assert "create table raw_data.crimes" in ddl
    assert "if not exists" not in ddl


# --------------------------------------------------- manage_ddl=False (verify only)


@pytest.fixture
def verifying_engine(mock_conn, creds):
    """An engine that must not create tables — an external tool owns the DDL."""
    eng = PostgresEngine(creds, manage_ddl=False)
    eng._conn = mock_conn
    return eng


def _live_table(engine, columns: dict[str, str]) -> None:
    """Pretend ``columns`` (name -> information_schema data_type) is the live table."""
    engine.table_exists = lambda target: True
    engine.table_column_types = lambda target: columns


SCD2_PIPELINE = {
    "ingested_at": "timestamp with time zone",
    "record_hash": "text",
    "valid_from": "timestamp with time zone",
    "valid_to": "timestamp with time zone",
}


def test_verifying_engine_executes_no_ddl_when_the_table_matches(verifying_engine, mock_cursor):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT, nullable=False)])
    _live_table(verifying_engine, {"id": "text", **SCD2_PIPELINE})

    verifying_engine.ensure_table(TableRef("crimes", "raw_data"), schema, SCD2(entity_key=["id"]))

    assert "create table" not in _executed_sql(mock_cursor)


def test_verifying_engine_raises_with_the_ddl_when_the_table_is_missing(verifying_engine):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT, nullable=False)])
    verifying_engine.table_exists = lambda target: False

    with pytest.raises(TableNotFoundError) as exc:
        verifying_engine.ensure_table(
            TableRef("crimes", "raw_data"), schema, SCD2(entity_key=["id"])
        )

    assert "create table raw_data.crimes" in exc.value.ddl
    assert "manage_ddl=False" in str(exc.value)


def test_verifying_engine_offers_a_migration_for_additive_drift(verifying_engine):
    schema = TableSchema(
        columns=[
            Column("id", ColumnType.TEXT, nullable=False),
            Column("case_name", ColumnType.TEXT),
        ]
    )
    _live_table(verifying_engine, {"id": "text", **SCD2_PIPELINE})  # case_name not yet added

    with pytest.raises(SchemaDriftError) as exc:
        verifying_engine.ensure_table(
            TableRef("crimes", "raw_data"), schema, SCD2(entity_key=["id"])
        )

    migration = exc.value.migration
    assert migration is not None
    assert 'add column "case_name" text;' in migration
    assert exc.value.diff.is_additive_only


def test_verifying_engine_withholds_a_migration_for_non_additive_drift(verifying_engine):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT, nullable=False)])
    _live_table(verifying_engine, {"id": "text", "dropped_upstream": "text", **SCD2_PIPELINE})

    with pytest.raises(SchemaDriftError) as exc:
        verifying_engine.ensure_table(
            TableRef("crimes", "raw_data"), schema, SCD2(entity_key=["id"])
        )

    assert exc.value.migration is None
    assert "not additive" in str(exc.value)


# ------------------------------------------------------------------- diff_table


def test_diff_table_ignores_engine_owned_pipeline_columns(engine):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT, nullable=False)])
    _live_table(engine, {"id": "text", **SCD2_PIPELINE})

    diff = engine.diff_table(TableRef("crimes", "raw_data"), schema, SCD2(entity_key=["id"]))
    assert diff.is_empty


def test_diff_table_folds_type_spelling_variants(engine):
    """The schema says `timestamptz`; information_schema says the long form."""
    schema = TableSchema(columns=[Column("seen_at", ColumnType.TIMESTAMPTZ)])
    _live_table(engine, {"seen_at": "timestamp with time zone", "ingested_at": "text"})

    diff = engine.diff_table(TableRef("crimes", "raw_data"), schema, Append())
    assert diff.type_mismatches == []


def test_diff_table_reports_a_real_type_change(engine):
    schema = TableSchema(columns=[Column("amount", ColumnType.DOUBLE)])
    _live_table(engine, {"amount": "text", "ingested_at": "timestamp with time zone"})

    diff = engine.diff_table(TableRef("crimes", "raw_data"), schema, Append())
    assert diff.type_mismatches == [
        TypeMismatch(column="amount", expected="double precision", actual="text")
    ]
    assert not diff.is_additive_only


def test_diff_table_checks_postgis_columns_by_presence_only(engine):
    """Geometry and raster both report as USER-DEFINED, so their type says nothing."""
    schema = TableSchema(
        columns=[Column("geom", ColumnType.GEOMETRY, geometry=GeometrySpec(kind="Point"))]
    )
    _live_table(engine, {"geom": "USER-DEFINED", "ingested_at": "timestamp with time zone"})

    diff = engine.diff_table(TableRef("crimes", "raw_data"), schema, Append())
    assert diff.is_empty


def test_render_migration_goes_through_the_live_diff(engine):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT), Column("added", ColumnType.TEXT)])
    _live_table(engine, {"id": "text", "ingested_at": "timestamp with time zone"})

    sql = engine.render_migration(TableRef("crimes", "raw_data"), schema, Append())
    assert sql == 'alter table raw_data.crimes add column "added" text;\n'


# ------------------------------------------------------------------ open_write


def test_open_write_scd2_configures_stager(engine):
    schema = TableSchema(columns=SAMPLE_COLUMNS)
    ws = engine.open_write(TableRef("crimes", "raw_data"), schema, SCD2(entity_key=["id"]))
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
    ws = engine.open_write(TableRef("t", "s"), schema, Upsert(keys=["id"], on_conflict="nothing"))
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
        return_value=pd.DataFrame(
            {
                "column_name": ["id", "val", "ingested_at"],
                "data_type": ["text", "double precision", "timestamp with time zone"],
            }
        )
    )
    assert engine.table_columns(TableRef("t", "s")) == {"id", "val", "ingested_at"}


def test_table_column_types(engine):
    engine.query = MagicMock(
        return_value=pd.DataFrame(
            {"column_name": ["id", "geom"], "data_type": ["text", "USER-DEFINED"]}
        )
    )
    assert engine.table_column_types(TableRef("t", "s")) == {
        "id": "text",
        "geom": "USER-DEFINED",
    }


def test_table_column_types_is_empty_for_a_missing_table(engine):
    engine.query = MagicMock(return_value=pd.DataFrame())
    assert engine.table_column_types(TableRef("t", "s")) == {}


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
