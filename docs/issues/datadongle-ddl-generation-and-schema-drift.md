# Generate version-controllable DDL, stop creating tables when an external tool owns them, and report schema drift

## Motivation

datadongle currently owns table creation outright. `PostgresEngine.ensure_table` runs a `create table if not exists` built by `_render_create_table` (`src/datadongle/engines/postgres.py:445`), and the shared driver calls it on every run (`src/datadongle/load/driver.py:55`). That DDL is the only definition of the target table's shape, and it exists only as a runtime side effect — I cannot review it, diff it, replay it, or point at the commit that introduced a column.

The project I primarily want to support manages its database schema with [Flyway](https://documentation.red-gate.com/flyway), which expects DDL to be checked in as versioned `V<n>__<description>.sql` scripts and to be the sole author of schema changes. Today datadongle and Flyway would both claim that role. A 3am ingestion run can create a table that Flyway's schema history has no record of, and the next `flyway migrate` then either fails on an object it did not create or records a version for one it did not author. I want datadongle to *describe* the table it needs and let Flyway *apply* it.

I want this at the engine layer, not in the CourtListener collector. The renderer's inputs are `(TableRef, TableSchema, WriteMode)` — exactly what every `SourceReader` already produces — so a solution here serves TIGER, Socrata, CKAN, EIA, and every future collector equally.

## The problem

### 1. The DDL is not something a person can reasonably hand-transcribe

The target table is not just the source's columns. `_render_create_table` also emits:

- an `ingested_at timestamptz not null default (now() at time zone 'UTC')` column on every table;
- under `SCD2`, a `record_hash text not null`, `valid_from timestamptz not null default (now() at time zone 'utc')`, and `valid_to timestamptz`;
- under `SCD2`, a unique index on `(entity_key…, record_hash)` and a partial index on the entity key `where valid_to is null`;
- for any raster column, a GiST index on `ST_ConvexHull(col)`, made partial over current rows under `SCD2`;
- for geometry columns, a PostGIS `geometry(<kind>,<srid>)` type rather than a plain type name.

Someone writing the Flyway migration by hand will get part of this wrong. The failure does not surface at migration time — it surfaces later as a merge failure or a silently degenerate SCD2 table, because `StagedIngest` creates its staging table with `like <target> including defaults` (`src/datadongle/engines/postgres_load.py:226`) and derives its column list from the live table, so it inherits whatever the hand-written DDL got wrong.

### 2. Schema drift is silent, and behaves differently on each engine

Neither engine notices when the source's shape stops matching the table's. `create table if not exists` no-ops, and `IcebergEngine.ensure_table` returns early when the table exists (`src/datadongle/engines/iceberg.py:186`). What happens next depends on the engine:

| Drift | PostgresEngine | IcebergEngine |
|---|---|---|
| Source **adds** a column | **Silently dropped.** `StagedIngest._get_target_columns()` (`postgres_load.py:248`) reads the column list from `information_schema` — the *existing* table — so the new column is never staged and never lands. No error, no log. | Raises on append (PyIceberg schema validation) |
| Source **removes** a column | Reader's projection fills `None`, the content hash changes, and **every row re-versions** under SCD2 | Same |

The additive case is the one that worries me: silent data loss on Postgres and a hard failure on Iceberg, from the same reader and spec. That directly contradicts the invariant in `docs/spec/collector-interface.md:174` that the same reader+spec runs unchanged against both engines.

### 3. CourtListener made this acute, but it is not a CourtListener problem

`CourtListenerReader.schema()` derives its column set from the network at runtime — a CSV header peek on whatever bulk export Free Law Project published most recently (`src/datadongle/collectors/courtlistener/reader.py:360`), or the keys of a single sampled API row (`reader.py:369`). Free Law Project publishes monthly, so the table shape can change under me without any commit on my side. That is what made me unwilling to ship create-if-not-exists as the whole story, but every collector has some version of the same exposure whenever an upstream source adds or renames a field.

## Options I explored

### Where the DDL comes from

**Option A — Keep `ensure_table` as the only path and hand-write the Flyway scripts.** Rejected. It duplicates the table definition in two places that will drift, and per problem 1 the duplicate is easy to get subtly wrong in ways that fail late.

**Option B — Adopt a migration framework inside datadongle (Alembic, sqitch).** Rejected on three grounds. It is the wrong layer: the consuming project already has Flyway, and I would be asking it to run two migration tools against one database. It is engine-specific: datadongle's premise is that one reader runs against both `PostgresEngine` and `IcebergEngine`, and Alembic has nothing to say about Iceberg, so I would be maintaining two migration trees. And Alembic assumes a declarative model (SQLAlchemy metadata) that datadongle does not have — its schema model is `TableSchema`, rendered per engine.

**Option C — Expose the existing renderer as a public API and let an external tool own execution.** Chosen. The generator stays the single source of truth for what the table should look like; Flyway owns *when* and *whether* it is applied. This is a small change, because `_render_create_table` uses only `_fqn` and `_schema_of` — both pure functions of `TableRef` — and `_render_column` is already a `@staticmethod`. **There is no connection state anywhere in the DDL path**, so it lifts to a module-level function verbatim and can be called without credentials.

### Whether generated DDL should be idempotent

I chose **bare DDL** (`create table …`, `create unique index …`) as the default for the public renderer, with an `if_not_exists: bool = False` parameter for the other case. A Flyway versioned migration runs exactly once against a given database, so if the object already exists I want a loud failure — that means the schema history and the database disagree, which is precisely the condition I am trying to make visible. `ensure_table` keeps calling the renderer with `if_not_exists=True`, so its behavior is unchanged and no existing test moves.

### Who is allowed to create tables

**Option A — Convention only** ("run Flyway before you run a collection"). Rejected: it fails open. Forgetting the convention silently reintroduces the exact dual-ownership problem, and the symptom appears in Flyway's history days later.

**Option B — A driver-level flag.** Rejected. There are six `ensure_table` call sites: the shared driver plus five family drivers (`tiger`, `threedep`, `static`, `census`, `dkan`, `cms`). A flag would have to be threaded through every one of them and every caller of them.

**Option C — An engine-level flag, `PostgresEngine(creds, manage_ddl=True)`.** Chosen. Whether Flyway owns a database is a property of the *deployment*, not of any dataset or run, so it belongs where the credentials are configured — set once, honored by all six call sites with no signature churn. With `manage_ddl=False`, `ensure_table` executes no DDL; it verifies the table exists and that its columns cover the schema, and raises with the DDL to apply if not.

### How far to take drift detection

I am going with the full reporting scope: `diff_table` for detection and `render_migration` for the additive fix. But I am deliberately keeping `render_migration` **additive-only** — it emits `alter table … add column …` and nothing else. Dropped columns, type changes, and entity-key changes are reported in the diff but not auto-rendered, because at a raw ingestion layer with SCD2 history a destructive in-place migration is risk I do not need to take. The escape hatch for a genuinely breaking change is a new table version (`courtlistener_dockets_v2`, v1 left read-only), which needs no new machinery at all.

## Implementation outline

### Part 1 — `src/datadongle/engines/postgres_ddl.py` (new)

Move the rendering logic out of `PostgresEngine` into connection-free module functions:

```python
def render_create_table(
    target: TableRef,
    schema: TableSchema,
    mode: WriteMode,
    *,
    if_not_exists: bool = False,
    include_schema: bool = False,
) -> str
```

- `include_schema=True` prepends `create schema if not exists <namespace>;` for the case where the namespace is not already provisioned by an earlier migration.
- `PostgresEngine.render_create_table(...)` becomes a thin public delegate, so the method is discoverable from an engine instance; the module function is the path that needs no credentials.
- `PostgresEngine.ensure_table` delegates with `if_not_exists=True` — behavior and existing tests unchanged.
- A convenience wrapper, `render_create_table_for_spec(reader, spec, mode="full")`, collapses the `reader.target(spec)` / `reader.schema(spec)` / `reader.write_mode(spec, mode=mode)` trio into the one call a user actually wants. (Cuttable if it reads as sugar.)

Target output:

```sql
create table raw_data.courtlistener_dockets (
  "id" text not null,
  "case_name" text,
  "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
  "record_hash" text not null,
  "valid_from" timestamptz not null default (now() at time zone 'utc'),
  "valid_to" timestamptz
);

create unique index uq_courtlistener_dockets_entity_hash
    on raw_data.courtlistener_dockets ("id", "record_hash");

create index ix_courtlistener_dockets_current
    on raw_data.courtlistener_dockets ("id") where "valid_to" is null;
```

### Part 2 — Verify-only mode

- `PostgresEngine.__init__` gains `manage_ddl: bool = True`.
- With `manage_ddl=False`, `ensure_table` executes nothing. It checks `table_exists`; if absent it raises with the rendered `create table` in the message. If present it runs the Part 3 diff and raises on drift.
- New exceptions in `datadongle/collectors/exceptions.py` (or a new `datadongle/core/exceptions.py` if that module is collector-scoped by intent): `TableNotFoundError` and `SchemaDriftError`, both carrying the DDL that would resolve them, so the error message is directly pasteable into a migration script.
- `IcebergEngine` is unaffected — `manage_ddl` is not a member of the `Engine` protocol, since Iceberg creates tables through the PyIceberg catalog API rather than SQL DDL and has native schema evolution.

### Part 3 — Drift detection and migration rendering

- A `SchemaDiff` value type in `datadongle/core/schema.py` (engine-neutral, alongside `TableSchema`): `missing_columns: list[Column]`, `unexpected_columns: list[str]`, `type_mismatches: list[tuple[str, str, str]]`, and an `is_empty` / `is_additive_only` property pair.
- `PostgresEngine.diff_table(target, schema, mode) -> SchemaDiff`. `table_columns` (`postgres.py:500`) already queries `information_schema.columns`; this extends it to pull `data_type` too, and compares against the schema's expected types.
- `PostgresEngine.render_migration(target, schema, mode) -> str` emitting `alter table … add column …` for each missing column. Non-additive diffs raise rather than render.

The fiddly part is type comparison: it needs an inverse of `_PG_TYPES` (`postgres.py:32`) plus normalization, because `information_schema` reports `timestamp with time zone` for `timestamptz` and `double precision` for `double`, and geometry columns report as `USER-DEFINED`. I will normalize through a canonicalization map rather than string-comparing raw `data_type` values, and treat geometry/raster columns as matched-by-name only (their detail already comes from `_get_geometry_info`).

### Part 4 — Docs and tests

- `docs/spec/collector-interface.md` §7 gains a paragraph on DDL ownership: `ensure_table` is the default, `manage_ddl=False` hands authorship to an external tool, and the renderer is the single source of truth either way.
- A README section walking the Flyway workflow end to end: build the spec, render the DDL, paste into `V<n>__create_<table>.sql`, `flyway migrate`, then run collections against a `manage_ddl=False` engine.
- `tests/engines/test_postgres.py` additions: bare vs `if_not_exists` rendering; SCD2 apparatus present/absent; raster index; `include_schema`; `manage_ddl=False` raising on a missing table; `diff_table` detecting added, removed, and retyped columns; `render_migration` emitting adds and refusing non-additive diffs. These mock `psycopg2.connect` as the existing tests do, so they run without a database.

## Context and things worth noting

**A latent bug the generator will make visible.** `_render_create_table` builds index names as `uq_{target.name}_entity_hash` and `ix_{target.name}_current` with no length guard, while `StagedIngest` explicitly truncates to Postgres's 63-character identifier limit (`postgres_load.py:84`). A target table name over ~44 characters therefore produces an index name that Postgres silently truncates. That is tolerable while the DDL is invisible, but once it is checked into a migration script the truncation becomes a real reproducibility hazard — two long, similarly-named tables could collide. I will add the same truncation the staging path uses.

**This gives snapshots, not a schema history.** `render_create_table` renders whatever `TableSchema` it is handed. For a collector whose schema comes from a typed metadata API that is deterministic; for CourtListener it still requires a live source call, so the DDL I paste into a migration is "whatever the source looked like when I ran the generator." Making the *declared* schema a checked-in artifact (a per-dataset YAML the reader loads instead of probing, with an explicit sync command to update it) is the natural follow-on, and it is what would let `diff_table` compare source-against-declared as well as declared-against-table. I am deliberately not doing it in this issue — the DDL generator is useful on its own, and the declared-schema work is a larger change that should be motivated separately.

**Related decisions elsewhere.** The CourtListener spec's `entity_key` default of `["id"]` is correct for its core entity tables but wrong for its M2M/through tables (the citation map, opinion-cluster panel, `joined_by`), which should key on their natural FK tuple and set `cursor_column=None`. That is a separate issue; I note it here only because the entity key is an input to the DDL this generator emits, so the two land in the same file.

### Resources

- Flyway migration naming and versioning: https://documentation.red-gate.com/flyway/flyway-concepts/migrations
- PostgreSQL identifier length limit (`NAMEDATALEN - 1` = 63): https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
- Files this touches: `src/datadongle/engines/postgres.py`, `src/datadongle/engines/postgres_load.py`, `src/datadongle/core/schema.py`, `src/datadongle/load/driver.py`, `docs/spec/collector-interface.md`
