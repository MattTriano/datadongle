"""Unit tests for the connection-free PostgreSQL DDL renderer.

Nothing here touches a database or an engine instance — that is the point of
the module: rendering the DDL a collector needs must be possible with only a
spec in hand, so it can be pasted into a Flyway/sqitch migration.
"""

from __future__ import annotations

import pytest

from datadongle.core.engine import TableRef
from datadongle.core.schema import (
    Column,
    ColumnType,
    GeometrySpec,
    SchemaDiff,
    TableSchema,
    TypeMismatch,
)
from datadongle.core.write_mode import SCD2, Append
from datadongle.engines.postgres_ddl import (
    MAX_IDENTIFIER_LENGTH,
    canonical_type,
    expected_information_schema_type,
    index_name,
    pipeline_columns,
    render_create_table,
    render_migration,
)

TARGET = TableRef("crimes", "raw_data")
SCD2_MODE = SCD2(entity_key=["id"])

SAMPLE_COLUMNS = [
    Column("id", ColumnType.TEXT, nullable=False),
    Column("amount", ColumnType.DOUBLE),
    Column("payload", ColumnType.JSON),
    Column("geom", ColumnType.GEOMETRY, geometry=GeometrySpec(kind="Point", srid=4326)),
]


def _schema(columns=None) -> TableSchema:
    return TableSchema(columns=columns if columns is not None else SAMPLE_COLUMNS)


# ------------------------------------------------------------- render_create_table


def test_renders_bare_ddl_by_default():
    """A versioned migration runs once, so an existing object should fail loudly."""
    ddl = render_create_table(TARGET, _schema(), SCD2_MODE)

    assert "if not exists" not in ddl
    assert "create table raw_data.crimes" in ddl
    assert "create unique index uq_crimes_entity_hash" in ddl
    assert "create index ix_crimes_current" in ddl


def test_if_not_exists_makes_every_statement_idempotent():
    ddl = render_create_table(TARGET, _schema(), SCD2_MODE, if_not_exists=True)

    assert "create table if not exists raw_data.crimes" in ddl
    assert "create unique index if not exists uq_crimes_entity_hash" in ddl
    assert "create index if not exists ix_crimes_current" in ddl


def test_renders_pipeline_columns_the_source_never_declares():
    ddl = render_create_table(TARGET, _schema(), SCD2_MODE)

    assert '"ingested_at" timestamptz not null' in ddl
    assert '"record_hash" text not null' in ddl
    assert '"valid_from" timestamptz not null' in ddl
    assert '"valid_to" timestamptz' in ddl


def test_renders_types_and_constraints():
    ddl = render_create_table(TARGET, _schema(), SCD2_MODE)

    assert '"id" text not null' in ddl
    assert '"amount" double precision' in ddl
    assert '"payload" jsonb' in ddl
    assert '"geom" geometry(Point,4326)' in ddl


def test_append_has_no_scd2_apparatus():
    ddl = render_create_table(TableRef("events", "raw_data"), _schema(), Append())

    assert "record_hash" not in ddl
    assert "valid_to" not in ddl
    assert "create unique index" not in ddl
    assert "create index" not in ddl


def test_namespace_defaults_to_public():
    ddl = render_create_table(TableRef("t"), _schema(), Append())
    assert "create table public.t" in ddl


def test_include_schema_prepends_namespace_ddl():
    ddl = render_create_table(TARGET, _schema(), Append(), include_schema=True)
    assert ddl.startswith("create schema if not exists raw_data;")


def test_include_schema_is_off_by_default():
    assert "create schema" not in render_create_table(TARGET, _schema(), Append())


def test_raster_column_gets_partial_convex_hull_index_under_scd2():
    schema = _schema([Column("tile_id", ColumnType.TEXT), Column("rast", ColumnType.RASTER)])
    ddl = render_create_table(
        TableRef("elevation", "raw_data"), schema, SCD2(entity_key=["tile_id"])
    )

    assert '"rast" raster' in ddl
    assert (
        'on raw_data.elevation using gist (ST_ConvexHull("rast")) where "valid_to" is null' in ddl
    )


def test_raster_index_is_full_without_scd2():
    schema = _schema([Column("rast", ColumnType.RASTER)])
    ddl = render_create_table(TableRef("tiles", "raw_data"), schema, Append())

    assert 'using gist (ST_ConvexHull("rast"));' in ddl
    assert "valid_to" not in ddl


# -------------------------------------------------------------------- index_name


def test_short_index_names_are_unchanged():
    assert index_name("uq", "crimes", "entity_hash") == "uq_crimes_entity_hash"


def test_long_index_names_are_truncated_within_the_identifier_limit():
    """PostgreSQL truncates past 63 chars silently; we do it deliberately instead."""
    table = "courtlistener_financial_disclosure_non_investment_income_rows"
    name = index_name("uq", table, "entity_hash")

    assert len(name) <= MAX_IDENTIFIER_LENGTH
    assert name.startswith("uq_courtlistener_financial_disclosure_")
    assert name.endswith("_entity_hash")


def test_long_index_names_are_deterministic():
    table = "a" * 80
    assert index_name("uq", table, "entity_hash") == index_name("uq", table, "entity_hash")


def test_long_index_names_do_not_collide_on_a_shared_prefix():
    """A plain truncation would map both of these to the same identifier."""
    common = "courtlistener_financial_disclosure_reimbursements_"
    first = index_name("uq", common + "domestic", "entity_hash")
    second = index_name("uq", common + "foreign", "entity_hash")

    assert first != second


def test_index_name_rejects_a_name_that_cannot_fit():
    with pytest.raises(ValueError, match="shorten the target table name"):
        index_name("uq", "t" * 100, "x" * 60)


def test_generated_ddl_uses_truncated_index_names():
    table = "courtlistener_financial_disclosure_non_investment_income_rows"
    ddl = render_create_table(TableRef(table, "raw_data"), _schema(), SCD2_MODE)

    for line in ddl.splitlines():
        if line.startswith(("create index ", "create unique index ")):
            assert len(line.split()[-1]) <= MAX_IDENTIFIER_LENGTH


# ---------------------------------------------------------------- render_migration


def _diff(missing=(), unexpected=(), mismatches=()) -> SchemaDiff:
    return SchemaDiff(
        missing_columns=list(missing),
        unexpected_columns=list(unexpected),
        type_mismatches=list(mismatches),
    )


def test_migration_renders_add_column_for_each_missing_column():
    diff = _diff(missing=[Column("case_name", ColumnType.TEXT), Column("n", ColumnType.INTEGER)])
    sql = render_migration(TARGET, diff)

    assert 'alter table raw_data.crimes add column "case_name" text;' in sql
    assert 'alter table raw_data.crimes add column "n" integer;' in sql


def test_migration_defers_not_null_to_a_commented_follow_up():
    """ADD COLUMN ... NOT NULL fails on a populated table, so it can't be inlined."""
    diff = _diff(missing=[Column("id", ColumnType.TEXT, nullable=False)])
    sql = render_migration(TARGET, diff)

    assert 'add column "id" text;' in sql
    assert "not null;" not in sql.split("--")[0]
    assert '-- alter table raw_data.crimes alter column "id" set not null;' in sql


def test_migration_is_empty_when_there_is_no_drift():
    assert render_migration(TARGET, _diff()) == ""


def test_migration_refuses_dropped_columns():
    with pytest.raises(ValueError, match="non-additive"):
        render_migration(TARGET, _diff(unexpected=["stale"]))


def test_migration_refuses_type_changes():
    diff = _diff(mismatches=[TypeMismatch("amount", "double precision", "text")])
    with pytest.raises(ValueError, match="non-additive"):
        render_migration(TARGET, diff)


# ------------------------------------------------------------------- type helpers


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        (Column("a", ColumnType.TIMESTAMPTZ), "timestamp with time zone"),
        (Column("b", ColumnType.TIMESTAMP), "timestamp without time zone"),
        (Column("c", ColumnType.DOUBLE), "double precision"),
        (Column("d", ColumnType.JSON), "jsonb"),
        (Column("e", ColumnType.TEXT), "text"),
    ],
)
def test_expected_type_matches_information_schema_spelling(column, expected):
    assert expected_information_schema_type(column) == expected


@pytest.mark.parametrize(
    "column",
    [
        Column("geom", ColumnType.GEOMETRY, geometry=GeometrySpec()),
        Column("rast", ColumnType.RASTER),
    ],
)
def test_postgis_columns_opt_out_of_type_comparison(column):
    """Both report as USER-DEFINED, so comparing them says nothing."""
    assert expected_information_schema_type(column) is None


def test_canonical_type_folds_spelling_variants():
    assert canonical_type("TIMESTAMPTZ") == canonical_type("timestamp with time zone")
    assert canonical_type("float8") == "double precision"
    assert canonical_type("unknown_type") == "unknown_type"


# --------------------------------------------------------------- pipeline_columns


def test_pipeline_columns_depend_on_the_write_mode():
    assert pipeline_columns(Append()) == {"ingested_at"}
    assert pipeline_columns(SCD2_MODE) == {
        "ingested_at",
        "record_hash",
        "valid_from",
        "valid_to",
    }
