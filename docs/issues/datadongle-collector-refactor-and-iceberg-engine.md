# Extract the collector tooling into an installable `datadongle` package and add a Shape‑B `IcebergEngine`

## Motivation

The collector tooling under `loci/collectors` has matured into a consistent, well‑factored pattern — every source (Socrata, OSM, ArcGIS Hub, CMS, …) ships a `spec.py`, `client.py`, `metadata.py`, `collector.py`, and `taskflow.py`, and every collector orchestrates ingestion through a shared `engine` object (`loci/db/core.py`). I want to do two things with it:

1. Extract that tooling into a standalone, installable Python package named **`datadongle`** with a clearly defined, storage‑agnostic interface, managed with `uv`.
2. Add an **`IcebergEngine`** so I can collect datasets to a local filesystem behind Apache Iceberg tables and query those files, as an alternative to the current PostgreSQL warehouse.

The deeper goal is to decouple three concerns that are currently entangled inside each collector so that they vary independently — any collector should run against any engine in any collection mode:

- **The collector** (source adapter: how to talk to Socrata vs. OSM).
- **The engine** (storage/query backend: PostgreSQL vs. Iceberg).
- **The collection‑mode / staged‑ingest logic** (full vs. incremental; append vs. upsert vs. SCD2 versioning).

## Problem: where the current design couples these three axes

The `engine` seam already exists and is the right boundary — collectors call `engine.query(...)`, `engine.execute(...)`, `engine.staged_ingest(...) -> stager`, and `engine._get_geometry_column(...)`. But the three axes above leak across it in several places:

- **Collection‑mode logic lives inside each collector.** `SocrataCollector.collect(spec, force)` hard‑codes the dispatch between full‑refresh‑via‑file, full‑refresh‑via‑API, and incremental, and each of the other collectors re‑implements its own near‑identical version of "read prior high‑water mark, filter the source, page, extract the new high‑water mark." This is source‑agnostic orchestration that should be shared, not copied per collector.

- **The high‑water mark read leaks storage dialect into the collector.** `SocrataCollector._get_hwm_from_table` runs `to_char(socrata_updated_at at time zone 'UTC', ...)` directly against the target table. Reading the HWM *from the target table* is the right design — it self‑heals if the table is dropped and rebuilt, where a tracker/log value would go stale — but that PostgreSQL‑specific SQL is embedded in a collector, so it cannot work against an engine with a different dialect. The fix keeps the read against the table and moves it behind `engine.read_high_water_mark(target, cursor_spec)`, where each engine supplies its own dialect (PostgreSQL `to_char(...)`; DuckDB `strftime(...)` over an Iceberg scan).

- **The SCD2 merge is split between the collector and the engine.** The collector picks the *policy* (it passes `entity_key` for SCD2, or `conflict_column` for a simple upsert), while `StagedIngest` in `loci/db/core.py` implements the *mechanism* (temp table + `COPY` + the five‑step SCD2 merge). That split is fine, but the mechanism is written entirely in PostgreSQL terms (temp tables, `information_schema`, `md5(...)` SQL, a geometry cast), so today "SCD2" and "PostgreSQL" are inseparable.

- **The package can't be installed cleanly because it imports Airflow.** Every `taskflow.py` imports `airflow.sdk`, and `sources/update_configs.py` carries the cron schedules. A reusable library should not depend on the orchestrator that happens to call it.

- **Geometry handling is implicit and PostGIS‑shaped.** Geometry arrives as EWKT (`SRID=4326;POINT(lon lat)`) or WKB‑hex (from `parsers/geojson.py`), and `StagedIngest._cast_geometry_if_needed` casts the staging text column to a PostGIS `geometry` on the way in. There is no explicit, engine‑neutral notion of "this column is geometry with this SRID," which a non‑PostGIS engine needs.

## Goals and non‑goals

**Goals**

- A `datadongle` package, `uv`‑managed, importable with **no Airflow dependency**, shipping both `PostgresEngine` and `IcebergEngine`, all collectors, and the shared load/strategy layer.
- A small, documented **standard interface** — `Engine`, `WriteSession`, and `SourceReader` protocols plus a neutral `WriteMode` — such that collector × engine × mode compose freely.
- An **`IcebergEngine`** that writes to a local‑filesystem warehouse and reads it back with SQL, using **Shape B** SCD2 (append‑only "satellite"; see decisions below).
- **Geometry as a first‑class concern**, portable across PostGIS and Iceberg/Parquet.
- **Socrata migrated end‑to‑end** onto both engines as the first vertical slice, with the existing test suite passing and new Iceberg tests added.

**Non‑goals (for this first cut)**

- Migrating the other ~12 collectors (they follow once the interface is proven).
- Running Trino/Spark or any query service — the Iceberg path is embedded (PyIceberg + DuckDB) only.
- Distributed/remote object stores — local filesystem warehouse only for now.
- Moving the Airflow DAGs; `loci` keeps them and becomes a *consumer* of `datadongle`.

## Design decisions explored

### dbt on Iceberg — ruled out as a transformation path

I checked whether I could keep the same physical SCD2 table shape across engines by relying on dbt to transform Iceberg tables. Per dbt's own docs, materializing Iceberg tables is supported only on **Snowflake, Databricks, and BigQuery** (via `catalogs.yml`), plus engine‑backed adapters (`dbt-spark`, `dbt-trino`, `dbt-athena`). The embedded `dbt-duckdb` path **reads** Iceberg but does not write/materialize it, and dbt's native SCD2 (`snapshot`) on Iceberg is still an open feature request. There is no local, no‑service dbt→Iceberg write path. Since I'm not adopting a cloud warehouse, dbt‑on‑Iceberg is a non‑starter, and I've accepted that transformation will run on a separate engine (DuckDB, or Spark later) and that Iceberg current‑record semantics need not match the PostgreSQL tables.

References: [dbt Apache Iceberg support](https://docs.getdbt.com/docs/mesh/iceberg/apache-iceberg-support), [dbt‑adapters #341 (view materialization)](https://github.com/dbt-labs/dbt-adapters/issues/341), [dbt‑snowflake #1270 (snapshots on Iceberg)](https://github.com/dbt-labs/dbt-snowflake/issues/1270).

### SCD2 on Iceberg — Shape B (append‑only satellite), not Shape A

My original worry was that snapshot‑based history would duplicate data and blow up storage. The key realization: Iceberg snapshots **share** data files — they don't duplicate them — and the real duplication risk is copy‑on‑write (COW) row updates. The fix is to **keep history in rows, not snapshots**, so snapshots stay cheap and `expire_snapshots` can run freely.

- **Shape A (rejected):** physical `valid_to`, mutated on change. On immutable Parquet this is a delete‑then‑rewrite (COW) or a merge‑on‑read delete file (MOR). It matches the PostgreSQL table shape exactly, but depends on PyIceberg MOR‑delete maturity and adds compaction/read‑merge cost.
- **Shape B (chosen):** **append‑only**. One row per distinct record version, keyed by `(entity_key, record_hash)` with an `effective_from`. Never update, never delete. `valid_to` and `is_current` are **derived at read time** with a window function. This is the Data Vault satellite pattern. Storage ≈ the sum of distinct versions — the theoretical floor for keeping history — with no COW churn and no memory pressure.

The trade‑off I accepted: "current record" is a view, not a physical column, so consumers of the Iceberg tables query slightly differently than the PostgreSQL tables. That's fine given dbt‑on‑Iceberg is out anyway.

### Query and change‑detection engine — DuckDB, embedded

`IcebergEngine` uses **PyIceberg for writes** (transactional `append` against the catalog) and **DuckDB for reads and change detection**. DuckDB reads Iceberg/Parquet and runs the anti‑join that identifies new/changed records **out‑of‑core** (it spills to `temp_directory`, bounded by `memory_limit`), so large datasets are processed largely from storage rather than loaded into memory. DuckDB's spatial extension also gives us geometry predicates at query time. (DuckDB's Iceberg *write* support is immature, which is exactly why writes stay on PyIceberg.)

### Geometry — WKB in storage, DuckDB spatial for queries

Geometry is a first‑class requirement. In Iceberg/Parquet I'll store geometry as **WKB `binary` columns** carrying an SRID in column metadata (EWKB), rather than GeoParquet (PyIceberg has no native GeoParquet awareness, so GeoParquet would mean custom handling for little near‑term gain). Queries use DuckDB spatial (`ST_GeomFromWKB`, `ST_AsText`, spatial predicates). The neutral type system (below) carries a `Geometry(kind, srid)` type so each engine renders it natively: PostGIS `geometry(Point,4326)` on one side, WKB `binary` + metadata on the other.

## Proposed architecture

### The three axes and the neutral contract between them

```
SourceReader (Socrata)        WriteMode (policy)          Engine (mechanism)
  what to fetch,          →   Append | Upsert(keys) |  →   PostgresEngine
  how to page,                SCD2(entity_key)            IcebergEngine
  source transforms                                       (any WriteMode, natively)
        │                            │                          │
        └──────── RecordBatch = list[dict[str, Any]] ───────────┘
                  + TableSchema (typed, geometry-aware)
```

- **`RecordBatch`** stays `list[dict[str, Any]]` — the existing neutral interchange format that every client already yields. No change to clients.
- **`TableSchema`** is a new, explicit, engine‑neutral schema (typed columns, entity key, geometry columns). It replaces the implicit "read `information_schema`" coupling and the per‑source `generate_ddl`.
- **`WriteMode`** is a small value object describing the merge *policy*, chosen from the spec. The engine translates it into its native *mechanism*.

### Protocols (the "standard interface")

```python
# datadongle/core/schema.py
@dataclass(frozen=True)
class Column:
    name: str
    type: ColumnType          # neutral type; see geometry section
    nullable: bool = True
    metadata: bool = False    # source bookkeeping (row id, source ts/version); excluded from the SCD2 hash

@dataclass
class TableSchema:
    columns: list[Column]     # structure only; the natural key rides on the WriteMode
    # .geometry is a derived {col -> GeometrySpec} property; SCD2/pipeline columns are engine-added

# datadongle/core/write_mode.py
class WriteMode: ...                      # base
@dataclass class Append(WriteMode): ...
@dataclass class Upsert(WriteMode): keys: list[str]; on_conflict: str = "update"
@dataclass class SCD2(WriteMode): entity_key: list[str]; invalidate_missing: bool = False

# datadongle/core/engine.py
class WriteSession(Protocol):             # what staged_ingest returns today, generalized
    def write_batch(self, rows: list[dict]) -> int: ...
    def __enter__(self) -> "WriteSession": ...
    def __exit__(self, *exc) -> bool: ...      # commit/merge on clean exit
    rows_staged: int
    rows_merged: int
    rows_invalidated: int

class Engine(Protocol):
    def open_write(self, target: TableRef, schema: TableSchema, mode: WriteMode) -> WriteSession: ...
    def query(self, sql: str, params=None): ...            # DataFrame/GeoDataFrame
    def ensure_table(self, target: TableRef, schema: TableSchema, mode: WriteMode) -> None: ...  # mode-aware DDL
    def table_columns(self, target: TableRef) -> set[str]: ...
    def geometry_columns(self, target: TableRef) -> dict[str, int]: ...  # col -> srid
    def read_high_water_mark(self, target: TableRef, cursor: CursorSpec) -> Cursor | None: ...  # from the table

# datadongle/core/reader.py
class SourceReader(Protocol):
    source: str
    def target(self, spec) -> TableRef: ...
    def schema(self, spec) -> TableSchema: ...
    def write_mode(self, spec) -> WriteMode: ...
    def cursor_spec(self, spec) -> CursorSpec | None: ...   # None => not incrementally queryable
    def read(self, spec, *, since: Cursor | None) -> Iterator[list[dict]]: ...  # applies source transforms
    def extract_cursor(self, batch: list[dict]) -> Cursor | None: ...           # max cursor in a batch
```

`ensure_table` is mode‑aware because the physical shape depends on the write policy: every table gets `ingested_at`; a keyed `SCD2` table also gets the engine's versioning columns (physical `record_hash`/`valid_from`/`valid_to` for PostgreSQL; an append‑only satellite for Iceberg). The natural key rides on the `WriteMode` (`Upsert.keys` / `SCD2.entity_key`), which the reader builds from `spec.entity_key`; `TableSchema` stays purely structural.

`open_write(...)` is the generalization of today's `engine.staged_ingest(...)`; the returned `WriteSession` is the generalization of `StagedIngest`. The PostgreSQL implementation of `WriteSession` is essentially the current `StagedIngest`, unchanged.

### The shared collection‑mode driver (decoupling axis 3)

The full‑vs‑incremental orchestration that is currently copied into every collector becomes one shared function, parameterized only by the `SourceReader`:

```python
# datadongle/load/driver.py
def run_collection(reader, spec, engine, tracker=None, *, mode: Literal["full", "incremental"]):
    target, schema, write_mode = reader.target(spec), reader.schema(spec), reader.write_mode(spec)
    engine.ensure_table(target, schema, write_mode)              # idempotent; replaces generate_ddl copy/paste

    since = None
    if mode == "incremental":
        cursor_spec = reader.cursor_spec(spec)                   # None => source isn't incrementally queryable
        if cursor_spec is not None:
            since = engine.read_high_water_mark(target, cursor_spec)   # read from the target table

    with tracker.track(reader.source, spec.dataset_id, str(target)) as run, \
         engine.open_write(target, schema, write_mode) as ws:
        high = since
        for batch in reader.read(spec, since=since):
            ws.write_batch(batch)
            high = _max_cursor(high, reader.extract_cursor(batch))
        run.rows_staged, run.rows_merged = ws.rows_staged, ws.rows_merged
        run.high_water_mark = _encode(high)                      # recorded for observability only
    return {"mode": mode, "rows_merged": ws.rows_merged}
```

This is where the three axes finally separate: the driver knows nothing about Socrata or SQL; the reader knows nothing about staging or merges; the engine knows nothing about pagination. Socrata's existing `file_download` vs `api` distinction becomes an internal detail of the reader: a `file_download` source returns `cursor_spec(spec) is None`, so the driver never reads a high‑water mark and the reader's `read(...)` ignores `since` and yields the whole export.

The HWM read moves out of the collector and into the *engine* (`engine.read_high_water_mark(target, cursor_spec)`), still sourced from the **target table itself** — so it self‑heals across a drop/rebuild — with the dialect‑specific formatting (`to_char` / `strftime`) living inside each engine. The tracker no longer supplies the cursor; it only records the resulting HWM for observability.

### Package layout

```
datadongle/
  pyproject.toml            # uv-managed; optional-deps: [postgres] [iceberg] [geo]
  src/datadongle/
    core/
      schema.py             # Column, TableSchema, ColumnType, GeometrySpec
      write_mode.py         # Append, Upsert, SCD2
      engine.py             # Engine, WriteSession protocols
      reader.py             # SourceReader protocol; Cursor
      spec.py              # DatasetSpec base (moved from base_spec.py)
    load/
      driver.py             # run_collection(...) — the shared mode logic
      tracking.py           # IngestionTracker (moved; engine-agnostic)
      schema_drift.py       # preflight check via engine.table_columns
    engines/
      postgres.py           # PostgresEngine + PgWriteSession (today's StagedIngest)
      iceberg.py            # IcebergEngine + IcebergWriteSession (Shape B)
      _duckdb.py            # DuckDB session helpers (read + change detection)
    geometry/
      types.py              # neutral Geometry type; EWKT/EWKB helpers
    collectors/
      socrata/              # spec.py, client.py, metadata.py, reader.py
      ...                   # (others migrate later)
  tests/                    # mirrors loci/tests layout
```

`loci` keeps `tasks/`, `sources/update_configs.py`, and the DAGs; each `taskflow.py` shrinks to "build engine + tracker, call `run_collection(reader, spec, engine, tracker, mode=...)`," and imports `datadongle`.

## The `IcebergEngine` (Shape B)

**Warehouse & catalog.** A local warehouse directory plus a PyIceberg `SqlCatalog` backed by a local SQLite file — no external service, fully hermetic (this is also what makes the Iceberg tests run without skips).

```python
IcebergEngine(warehouse="/data/warehouse", catalog_db="/data/catalog.db",
              memory_limit="4GB", temp_dir="/data/duckdb_tmp")
```

**Schema / `ensure_table`.** Translate `TableSchema` → PyIceberg schema. Data columns as their Parquet/Arrow types; geometry columns as `binary` (WKB) with `srid` recorded in field metadata. Append‑only satellites additionally carry `record_hash` (string), `effective_from` (timestamptz), `ingested_at`, and `load_id`. No `valid_to`/`is_current` columns.

**Write path (`IcebergWriteSession`, `SCD2` mode).**
1. `write_batch(rows)` stages incoming rows to a temporary Parquet file (streamed; not held in memory).
2. On `__exit__` (clean): open a temporary DuckDB connection, register the staged Parquet and the target Iceberg table, and run the change‑detection anti‑join, computing `record_hash` in SQL:

   ```sql
   WITH incoming AS (SELECT *, md5(<content cols>) AS record_hash FROM read_parquet($staged)),
        current AS (                       -- derive current version per entity
          SELECT entity_key, record_hash FROM (
            SELECT entity_key, record_hash,
                   row_number() OVER (PARTITION BY entity_key ORDER BY effective_from DESC) rn
            FROM iceberg_scan($target)) WHERE rn = 1)
   SELECT i.* FROM incoming i
   LEFT JOIN current c USING (entity_key)
   WHERE c.entity_key IS NULL OR c.record_hash <> i.record_hash;
   ```
3. Hand the delta (new + changed versions, as an Arrow table) to PyIceberg `table.append(...)`. `rows_merged` = appended row count. Nothing is deleted or rewritten.

`Append` mode skips the anti‑join; `Upsert` mode reduces to "keep only the latest per key" and is a thin variant of the same query.

**Read path (`query`).** Execute SQL through DuckDB against `iceberg_scan(...)`, loading the spatial extension so geometry columns work. Return a DataFrame, or a GeoDataFrame when a geometry column is present (mirroring `PostgresEngine.query`'s current behavior). Ship canned current/timeline views per satellite:

```sql
CREATE VIEW permits_current  AS SELECT ... WHERE rn = 1;      -- latest per entity
CREATE VIEW permits_timeline AS SELECT ...,                  -- adds derived valid_to
       lead(effective_from) OVER (PARTITION BY entity_key ORDER BY effective_from) AS valid_to;
```

**Maintenance (documented, deferred).** Per‑pull appends create many small Parquet files. Provide `IcebergEngine.maintain(target)` wrapping `rewrite_data_files` (compaction) and `expire_snapshots` (safe now — history is in rows). Start with the window‑derived `current`; add a compact "current‑hash per entity" side table only if pull latency demands it — not before.

## Geometry as a first‑class type

- Introduce a neutral `Geometry(kind, srid)` column type in `datadongle/geometry/types.py`, plus EWKT⇄EWKB helpers (reusing the existing `parsers/geojson.py` WKB output and the Socrata point→EWKT conversion).
- Collectors declare geometry columns in the `TableSchema` they return from `SourceReader.schema(spec)` — replacing the implicit PostGIS detection.
- `PostgresEngine` renders `Geometry` as `geometry(<kind>,<srid>)` and keeps the existing staging cast; `IcebergEngine` renders it as WKB `binary` + SRID metadata and reads it back via DuckDB spatial.
- The SCD2 `record_hash` must be geometry‑normalization‑stable: hash the canonical WKB, not the text, so equivalent geometries don't create spurious versions.

## `uv` project setup

`uv` is not yet installed in this environment; install it first (`curl -LsSf https://astral.sh/uv/install.sh | sh`), then:

- `uv init --package datadongle` (src layout) and author `pyproject.toml` with a core dependency set plus **optional‑dependency extras** so consumers install only what they use: `[postgres]` → `psycopg2`; `[iceberg]` → `pyiceberg[sql-sqlite]`, `duckdb`, `pyarrow`; `[geo]` → `shapely`, `geopandas`.
- `uv add` / `uv add --optional <extra>` to manage deps; `uv sync` to resolve; `uv run pytest` to run tests. Commit `uv.lock`.
- Keep the package importable with **zero** Airflow deps; `loci` depends on `datadongle` (path/editable dependency during development).

## Testing strategy (suite must pass)

- Mirror the existing `tests/` layout and reuse its patterns: DB‑backed PostgreSQL tests **skip** when no test database is reachable (as `tests/collectors/conftest.py` already does), and the `NoopTracker` fake from `tests/collectors/common.py` carries over.
- **Iceberg tests are hermetic** — a `tmp_path` warehouse + SQLite catalog needs no external service, so they run everywhere with no skips. This is a concrete win over the Postgres‑only tests.
- Add an **engine‑conformance suite**: parametrize the same collector run over `[PostgresEngine, IcebergEngine]` and assert identical *logical* outcomes (row counts, that a changed record produces a second version, that an unchanged re‑pull is a no‑op). This is the test that proves the decoupling.
- Geometry round‑trip tests: EWKT/EWKB in → correct geometry out of `query` on both engines; `record_hash` stability across equivalent geometries.
- Port the existing Socrata collector tests to the new `SourceReader` + driver structure; keep `tests/db/test_core.py`'s SCD2 coverage for the PostgreSQL `WriteSession`.

## Phased implementation plan

1. **Scaffold `datadongle`** with `uv` (src layout, extras, lockfile); move `base_spec.py`, `config.py`, `exceptions.py`, `utils.py`, and `IngestionTracker` in unchanged; get `uv run pytest` green on the moved pieces.
2. **Define the interface**: `core/schema.py`, `core/write_mode.py`, `core/engine.py`, `core/reader.py`, and `geometry/types.py`.
3. **Wrap PostgreSQL**: implement `PostgresEngine.open_write` returning a `WriteSession` backed by the existing `StagedIngest`; add `ensure_table`/`table_columns`/`geometry_columns`; add `read_high_water_mark` reading from the target table (lifting `_get_hwm_from_table`, generalized to `cursor_spec`).
4. **Shared driver**: implement `load/driver.run_collection` and `load/schema_drift`.
5. **Migrate Socrata** to a `SocrataReader` (spec/client/metadata kept; `collector.py`'s mode logic deleted in favor of the driver); make the conformance and geometry tests pass on PostgreSQL.
6. **Implement `IcebergEngine`** (PyIceberg write + DuckDB detect/query, Shape B, WKB geometry); run Socrata end‑to‑end against it; make the hermetic Iceberg tests and the two‑engine conformance suite pass.
7. **Re‑point `loci`**: rewrite `collectors/socrata/taskflow.py` to build an engine + tracker and call `run_collection`; confirm no Airflow import leaks into `datadongle`.

## Open questions / future work

- **Current‑detection at scale**: window‑derived `current` re‑scans the satellite each pull. If pull latency grows, introduce a compact current‑hash side table (a Data‑Vault‑style "current" mirror). Deferred until measured.
- **Compaction cadence**: when/how often to run `rewrite_data_files` + `expire_snapshots` — likely a scheduled `loci` task once volumes are known.
- **`invalidate_missing` semantics** (full‑refresh removals) need a Shape‑B analogue — probably a tombstone version row rather than a physical close‑out; design when the first full‑refresh source needs it.
- **Raster** (`raster/wkb.py` PostGIS raster WKB) has no Iceberg analogue yet; out of scope until a raster source needs the Iceberg path.
- **Cross‑engine `query` dialect**: `query` still passes raw SQL, which differs between PostgreSQL and DuckDB. Collectors no longer issue raw SQL (HWM and column checks are abstracted), but ad‑hoc consumers should treat `query` as engine‑specific, or we add a tiny neutral query builder later if a need appears.
