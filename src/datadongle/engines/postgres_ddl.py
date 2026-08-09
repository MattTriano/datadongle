"""PostgreSQL DDL rendering — the single source of truth for target shape.

``PostgresEngine.ensure_table`` uses these functions to create tables itself.
They are also the *public* way to obtain that DDL as text, so a project whose
schema is owned by an external migration tool (Flyway, sqitch, a checked-in
SQL script) can version-control the exact DDL datadongle expects instead of
hand-transcribing it::

    from datadongle.engines.postgres_ddl import render_create_table

    print(render_create_table(reader.target(spec), reader.schema(spec), mode))

Nothing here touches a connection — every function is a pure function of
``(TableRef, TableSchema, WriteMode)`` — so rendering DDL needs no credentials
and no database.

Two details make hand-writing this DDL error-prone, which is why it is
generated: the target carries pipeline columns the source never declares
(``ingested_at`` always, plus ``record_hash``/``valid_from``/``valid_to`` under
SCD2), and SCD2/raster tables carry indexes whose exact shape the merge SQL
depends on.
"""

from __future__ import annotations

import hashlib

from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, SchemaDiff, TableSchema
from datadongle.core.write_mode import SCD2, WriteMode

# Neutral column type -> PostgreSQL type.
PG_TYPES: dict[ColumnType, str] = {
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

# Pipeline columns the engine fills itself. Every table gets ``ingested_at``;
# SCD2 tables also get the versioning trio. A source never declares these, and
# they are excluded from drift reporting.
INGESTED_AT = "ingested_at"
SCD2_COLUMNS = ("record_hash", "valid_from", "valid_to")

# PostgreSQL truncates identifiers past NAMEDATALEN-1 silently. Generated index
# names are truncated deliberately instead, so the DDL that lands in a
# migration script is the DDL the server will actually store.
MAX_IDENTIFIER_LENGTH = 63

# ``information_schema.columns.data_type`` spells some types differently from
# the CREATE TABLE syntax. Both sides are pushed through this map before being
# compared, so ``timestamptz`` and ``timestamp with time zone`` are one type.
_TYPE_ALIASES = {
    "timestamptz": "timestamp with time zone",
    "timestamp": "timestamp without time zone",
    "double": "double precision",
    "float8": "double precision",
    "int4": "integer",
    "int8": "bigint",
    "bool": "boolean",
    "varchar": "character varying",
    "json": "jsonb",
}

# PostGIS types report as USER-DEFINED in information_schema, so their real
# detail (geometry kind, SRID) lives in the geometry_columns view instead.
# Drift for these columns is checked by presence only.
_OPAQUE_TYPES = {ColumnType.GEOMETRY, ColumnType.RASTER}


def schema_of(target: TableRef) -> str:
    """The PostgreSQL schema for ``target`` (its namespace, default ``public``)."""
    return target.namespace or "public"


def fqn(target: TableRef) -> str:
    """``schema.table`` for ``target``."""
    return f"{schema_of(target)}.{target.name}"


def index_name(prefix: str, table: str, suffix: str) -> str:
    """A deterministic index name that fits PostgreSQL's identifier limit.

    Short names render as ``<prefix>_<table>_<suffix>``. Longer ones keep a
    truncated table name plus a hash of the full one, so two long tables with a
    common prefix can't collide on a silently-truncated index name.
    """
    name = f"{prefix}_{table}_{suffix}"
    if len(name) <= MAX_IDENTIFIER_LENGTH:
        return name

    digest = hashlib.sha256(table.encode()).hexdigest()[:8]
    # 3 underscores join the four parts.
    budget = MAX_IDENTIFIER_LENGTH - len(prefix) - len(suffix) - len(digest) - 3
    if budget < 1:
        raise ValueError(
            f"Cannot build an index name for table {table!r} within "
            f"{MAX_IDENTIFIER_LENGTH} characters; shorten the target table name."
        )
    return f"{prefix}_{table[:budget]}_{digest}_{suffix}"


def render_column(col: Column) -> str:
    """One column definition, e.g. ``"geom" geometry(Point,4326) not null``."""
    if col.type is ColumnType.GEOMETRY:
        g = col.geometry
        assert g is not None  # guaranteed by Column.__post_init__
        pg_type = f"geometry({g.kind},{g.srid})"
    else:
        pg_type = PG_TYPES[col.type]
    frag = f'"{col.name}" {pg_type}'
    if not col.nullable:
        frag += " not null"
    return frag


def render_create_table(
    target: TableRef,
    schema: TableSchema,
    mode: WriteMode,
    *,
    if_not_exists: bool = False,
    include_schema: bool = False,
) -> str:
    """Render the full DDL for ``target``: table, then any indexes.

    Parameters
    ----------
    if_not_exists : bool
        Add ``if not exists`` to every statement. Defaults to ``False`` — a
        versioned migration runs exactly once, so an object that already exists
        should fail loudly rather than no-op, since it means the migration
        history and the database disagree. ``ensure_table`` passes ``True``.
    include_schema : bool
        Prepend ``create schema if not exists <namespace>;``, for when the
        namespace isn't provisioned by an earlier migration.
    """
    table_fqn = fqn(target)
    exists_clause = "if not exists " if if_not_exists else ""

    ddl = ""
    if include_schema:
        ddl += f"create schema if not exists {schema_of(target)};\n\n"

    col_defs = [render_column(c) for c in schema.columns]
    col_defs.append(f"\"{INGESTED_AT}\" timestamptz not null default (now() at time zone 'UTC')")
    if isinstance(mode, SCD2):
        col_defs.append('"record_hash" text not null')
        col_defs.append("\"valid_from\" timestamptz not null default (now() at time zone 'utc')")
        col_defs.append('"valid_to" timestamptz')

    ddl += f"create table {exists_clause}{table_fqn} (\n  " + ",\n  ".join(col_defs) + "\n);\n"

    if isinstance(mode, SCD2):
        ek = ", ".join(f'"{k}"' for k in mode.entity_key)
        uq = index_name("uq", target.name, "entity_hash")
        ix = index_name("ix", target.name, "current")
        ddl += (
            f"\ncreate unique index {exists_clause}{uq}\n"
            f'    on {table_fqn} ({ek}, "record_hash");\n'
        )
        ddl += (
            f"\ncreate index {exists_clause}{ix}\n"
            f'    on {table_fqn} ({ek}) where "valid_to" is null;\n'
        )

    # A raster column gets a GiST index on its convex hull — that is what
    # serves ST_Intersects(rast, point) sampling. Under SCD2 it is partial
    # over current rows, since sampling queries filter to them.
    for name in schema.raster_column_names():
        ddl += (
            f"\ncreate index {exists_clause}{index_name('ix', target.name, name)}\n"
            f'    on {table_fqn} using gist (ST_ConvexHull("{name}"))'
        )
        if isinstance(mode, SCD2):
            ddl += ' where "valid_to" is null'
        ddl += ";\n"
    return ddl


def render_migration(target: TableRef, diff: SchemaDiff, *, if_not_exists: bool = False) -> str:
    """Render ``ALTER TABLE`` statements resolving an **additive** ``diff``.

    Raises ``ValueError`` for non-additive drift (dropped or retyped columns):
    those need a decision about existing rows that datadongle can't make.

    A ``not null`` column can't simply be added to a populated table, so its
    constraint is emitted as a commented-out follow-up rather than inlined —
    backfill first, then uncomment.
    """
    if diff.is_empty:
        return ""
    if not diff.is_additive_only:
        raise ValueError(
            f"Cannot render a migration for non-additive drift on {fqn(target)}: {diff.describe()}."
        )

    table_fqn = fqn(target)
    exists_clause = "if not exists " if if_not_exists else ""
    statements = []
    for col in diff.missing_columns:
        # Render nullable regardless: ADD COLUMN ... NOT NULL fails on a table
        # that already has rows.
        addition = render_column(col).removesuffix(" not null")
        statements.append(f"alter table {table_fqn} add column {exists_clause}{addition};")
        if not col.nullable:
            statements.append(
                f'-- "{col.name}" is declared not null; backfill existing rows, then:\n'
                f'-- alter table {table_fqn} alter column "{col.name}" set not null;'
            )
    return "\n".join(statements) + "\n"


def pipeline_columns(mode: WriteMode) -> set[str]:
    """Columns the engine adds itself under ``mode`` (never source-declared)."""
    names = {INGESTED_AT}
    if isinstance(mode, SCD2):
        names.update(SCD2_COLUMNS)
    return names


def expected_information_schema_type(col: Column) -> str | None:
    """``col``'s type as ``information_schema`` would report it.

    ``None`` means "don't compare": PostGIS geometry and raster columns both
    report as ``USER-DEFINED``, so comparing them says nothing.
    """
    if col.type in _OPAQUE_TYPES:
        return None
    return canonical_type(PG_TYPES[col.type])


def canonical_type(data_type: str) -> str:
    """Normalize a PostgreSQL type name so spelling variants compare equal."""
    normalized = data_type.strip().lower()
    return _TYPE_ALIASES.get(normalized, normalized)
