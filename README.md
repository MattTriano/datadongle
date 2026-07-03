# datadongle

Installable data-collector tooling: **source collectors**, **pluggable storage engines**, and **staged-ingest / SCD2 load strategies** — decoupled so you can mix and match.

A collector says *what* to pull (a Socrata dataset, say). A **write mode** says *how* new rows should reconcile with what's already stored (append, upsert, or keep versioned history). A **storage engine** decides *where* and *physically how* that happens (Postgres/PostGIS or a local Iceberg warehouse). These three axes are independent: the same collector runs unchanged onto either engine, under any compatible write mode.

```
   SourceReader           WriteMode            Engine
  (what to collect)   (how to integrate)   (where it lands)
        │                    │                   │
 SocrataReader ──▶  Append / Upsert / SCD2  ──▶  PostgresEngine
                                                 IcebergEngine
```

---

## Installation

datadongle is a [`uv`](https://docs.astral.sh/uv/)-managed package targeting **Python ≥ 3.13**. Storage backends and geo support are optional extras — install only what you need.

| Extra | Pulls in | Needed for |
|-----------|-------------------------------------------------|-----------------------------------------------|
| `postgres` | `psycopg2-binary`, `pymysql` | `PostgresEngine` (and `MySQLEngine`) |
| `iceberg` | `pyiceberg[sql-sqlite]`, `duckdb`, `pyarrow` | `IcebergEngine` |
| `geo` | `shapely`, `geopandas`, `fiona`, `rasterio`, … | geometry columns on **either** engine |

```bash
# Postgres target with geometry support
uv sync --extra postgres --extra geo

# Local Iceberg target with geometry support
uv sync --extra iceberg --extra geo

# everything
uv sync --all-extras
```

`IcebergEngine` uses **no DuckDB native extensions** — PyIceberg does all Iceberg I/O, core DuckDB does the change-detection join, and shapely handles WKB geometry. It runs fully offline against a local-filesystem warehouse.

---

## Quickstart

Collect a Socrata dataset into a local Iceberg warehouse. This example is self-contained (no database to stand up):

```python
from datadongle.collectors.socrata.reader import SocrataReader
from datadongle.collectors.socrata.spec import SocrataDatasetSpec
from datadongle.engines.iceberg import IcebergEngine
from datadongle.load.driver import run_collection

spec = SocrataDatasetSpec(
    name="chicago_building_permits",
    dataset_id="ydr8-5enu",       # Socrata 4x4 id
    target_table="building_permits",
    target_schema="raw_data",
    entity_key=["permit_"],       # non-empty entity_key ⇒ SCD2 history
)

reader = SocrataReader()                       # optional: app_token=..., page_size=...
engine = IcebergEngine("/data/warehouse")      # local warehouse directory

# First load: read everything.
run_collection(reader, spec, engine, mode="full")

# Later runs: read only what changed since the last load.
summary = run_collection(reader, spec, engine, mode="incremental")
print(summary)
# {'source': 'socrata', 'dataset_id': 'ydr8-5enu', 'mode': 'incremental',
#  'rows_staged': 42, 'rows_merged': 3, 'rows_invalidated': 0, 'high_water_mark': ...}

# Query the result (current version of each permit, geometry parsed to shapely):
target = reader.target(spec)
current = engine.read_current(target)
```

Point the same collection at Postgres instead — nothing else changes:

```python
from datadongle.db.core import DatabaseCredentials
from datadongle.engines.postgres import PostgresEngine

creds = DatabaseCredentials(
    host="localhost", port=5432, database="dwh",
    username="etl", password="…",
)
engine = PostgresEngine(creds)

run_collection(reader, spec, engine, mode="full")
```

`run_collection` returns a summary `dict` with `rows_staged`, `rows_merged`, `rows_invalidated`, and the resulting `high_water_mark`. Pass an optional `tracker` to record each run for observability — it is **not** the source of truth for incremental resumption (see below).

---

## Collection modes: `full` vs `incremental`

The **collection mode** controls *how much* of the source to read on a given run. It is chosen per-call via `run_collection(..., mode=...)` (default `"incremental"`).

- **`full`** — read the entire source (`since=None`). Use for the first load, for full refreshes, and whenever the source isn't incrementally queryable.

- **`incremental`** — read only rows newer than what's already stored. The driver asks the engine for the target table's **high-water mark** (the max cursor value, e.g. `max(socrata_updated_at)`), and the reader turns that into a source-side filter.

The high-water mark is read **from the target table itself**, never from a run log:

```python
engine.read_high_water_mark(target, cursor_spec)   # max(cursor) + tiebreak, in the engine's dialect
```

This is deliberately **self-healing**: drop and rebuild the table and the next incremental run automatically restarts from the correct point, because the mark lives with the data. A tracker, if supplied, records the mark only for observability.

If the source has no cursor (`reader.cursor_spec(spec)` returns `None` — e.g. a Socrata `file_download` export, which carries no system fields), an `incremental` request transparently falls back to a full read.

> **Timestamps are UTC.** Both engines store `TIMESTAMPTZ` columns as UTC instants and pin their session/connection to UTC, so high-water marks round-trip identically regardless of the host or server timezone.

---

## Write modes: `Append`, `Upsert`, `SCD2`

The **write mode** is the *policy* for reconciling incoming rows with the target — independent of the engine, which supplies the *mechanism*. A collector selects a policy without knowing the storage. `SocrataReader`, for instance, returns `SCD2(entity_key=...)` when the spec has an `entity_key`, otherwise `Append`.

```python
from datadongle.core.write_mode import Append, Upsert, SCD2
```

### `Append()`
Insert every incoming row. No key, no deduplication, no versioning — the target accumulates everything it's given, duplicates included. Good for immutable event/log data.

### `Upsert(keys, on_conflict="update")`
Insert-or-update keyed by `keys`.
- `on_conflict="update"` — overwrite the conflicting row's non-key columns from the incoming row (last write wins).
- `on_conflict="nothing"` — keep the existing row, ignore the incoming duplicate.

Keeps exactly one row per key; **no history**. *(Supported by `PostgresEngine`; `IcebergEngine` support is not yet implemented.)*

### `SCD2(entity_key, invalidate_missing=False)`
Keep **versioned history** keyed by `entity_key` plus a content hash. A new version is written **only when an entity's content actually changes**:

- The engine computes a `record_hash` over the entity's *data* columns, **excluding** the `entity_key` (identity, not content) and any **metadata columns** (e.g. Socrata's `socrata_id` / `socrata_updated_at`, which change every run regardless of content).
- An unchanged re-pull is a **no-op** — same hash ⇒ no new version.
- A metadata-only change (e.g. a bumped `updated_at` with identical data) does **not** create a version.
- A genuine data change appends a new version and the entity's "current" pointer moves to it.

`invalidate_missing=True` additionally closes out / tombstones entities that are **absent** from the pull. Because "absent" can only be judged against a complete snapshot, this requires `mode="full"` — the driver raises if you request it incrementally.

**How each engine realizes SCD2:**

| | `PostgresEngine` | `IcebergEngine` (Shape B) |
|--------------------|------------------------------------------------|-----------------------------------------------|
| Physical shape | `valid_from` / `valid_to` columns updated in place | Append-only satellite; **no** `valid_to` |
| "Current" version | `WHERE valid_to IS NULL` | Derived at read time: latest `effective_from` per `entity_key` (window function) |
| Version columns | `record_hash`, `valid_from`, `valid_to` | `record_hash`, `effective_from`, `ingested_at`, `load_id` |
| Integrity | unique index on `(entity_key, record_hash)` + partial index for current | dedupe via DuckDB anti-join against history |
| `invalidate_missing` | sets `valid_to` on vanished entities | not yet implemented |

Both engines yield the **same logical outcome** — identical row counts, the same no-op/version decisions, the same current-state — verified by the two-engine conformance suite (`tests/engines/test_conformance.py`).

---

## Storage engines

Both engines implement the same `Engine` protocol (`ensure_table`, `open_write`, `query`, `read_high_water_mark`, `table_columns`, `geometry_columns`, …), so they are interchangeable under `run_collection`.

### `PostgresEngine(creds, db_name=None)`
Postgres + PostGIS. `ensure_table` renders `CREATE TABLE IF NOT EXISTS` DDL (geometry columns become `geometry(<kind>,<srid>)`); writes go through a `COPY`-into-staging then per-mode merge (`append_merge` / `upsert_merge` / `scd2_merge` in `engines/postgres_load.py`). `query(...)` returns a `DataFrame`, or a `GeoDataFrame` when a PostGIS geometry column is present. Needs the `postgres` extra (and `geo` for geometry).

### `IcebergEngine(warehouse, catalog_name="datadongle")`
A local-filesystem Iceberg warehouse (PyIceberg + a SQLite catalog) queried through DuckDB. Geometry is stored as WKB `binary` with the SRID retained in table properties. Shape-B SCD2 keeps writes cheap (pure appends). Reads:

```python
engine.read_current(target)     # latest version per entity  → (Geo)DataFrame
engine.read_history(target)     # every stored version       → (Geo)DataFrame
engine.query("select … from <table>_current where …")   # DuckDB SQL; <table> and <table>_current views registered
```

Needs the `iceberg` extra (and `geo` for geometry). No native DuckDB extensions required.

---

## Testing

```bash
uv run pytest                      # hermetic tests (Iceberg + unit); network + DB tests skip
uv run pytest -m network           # opt in to the network-marked tests
```

- **Iceberg tests are hermetic** — they build a warehouse under a `tmp_path`, so they run anywhere with no external service.
- **Postgres-backed tests skip** unless a database is configured. Set `DWH_TEST_PGHOST`, `DWH_TEST_PGPORT`, `DWH_TEST_PGDATABASE`, `DWH_TEST_PGUSER`, `DWH_TEST_PGPASSWORD` and they light up — including the Postgres arm of the two-engine conformance suite:

  ```bash
  DWH_TEST_PGHOST=localhost DWH_TEST_PGPORT=5432 \
  DWH_TEST_PGDATABASE=dwh_test DWH_TEST_PGUSER=postgres DWH_TEST_PGPASSWORD=… \
  uv run pytest tests/engines/test_conformance.py
  ```

- **Network-marked tests are deselected by default** (they need egress); run them explicitly with `-m network`.
