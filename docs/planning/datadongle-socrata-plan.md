# Plan: `datadongle` package + Socrata slice + IcebergEngine (Shape B)

## Context

The collector tooling in `/workspace/loci/collectors` is well-factored but entangles three concerns inside each collector: the **source adapter** (client/metadata/spec), the **collection-mode logic** (full vs. incremental orchestration, copied per collector), and the **storage engine** (`loci/db/core.py`). I want to extract this into an installable, `uv`-managed package named **`datadongle`** with a storage-agnostic interface, and add an **`IcebergEngine`** that collects to a local-filesystem Iceberg warehouse (append-only Shape-B SCD2) and queries it via DuckDB. The full design rationale lives in `/workspace/docs/issues/datadongle-collector-refactor-and-iceberg-engine.md`; this plan is the executable slice: init the project, migrate Socrata end-to-end onto both `PostgresEngine` and `IcebergEngine`, and keep the relevant tests passing.

Two corrections from review, folded in below:
1. **High-water mark is read from the target table, not the tracker** — a tracker HWM goes stale if the table is dropped/rebuilt; the table's max cursor value is self-healing.
2. **`metadata.py`, `client.py`, and `DatasetSpec` are collector tooling** — they move into `datadongle` as first-class parts of each collector, not left behind in `loci`.

## Where things live

- New package at **`/workspace/datadongle`** (its own `uv` project, `src/datadongle/` layout), a sibling to `loci`.
- `loci` becomes a **consumer**: its `collectors/socrata/taskflow.py` (the only Airflow-coupled file) is rewritten to build an engine + tracker and call `datadongle`'s shared driver. `datadongle` itself imports **no Airflow**.
- The original `loci/collectors/socrata/*` and `loci/db/core.py` are left in place for this slice (loci keeps working); the taskflow re-points to `datadongle`. A later phase removes the duplicated loci code once all collectors migrate.

## Git workflow

- `git log` to confirm history state. Since the tree is currently all-untracked on `main`, make a **baseline commit** of the existing tree on `main` first (`chore: baseline existing loci tree`) so the refactor is a legible diff.
- Create and check out branch **`feat/datadongle-socrata`**.
- Commit at these points (one per completed, test-green step): (a) uv scaffold + moved core primitives; (b) protocols + PostgresEngine adapter; (c) shared driver + Socrata reader on Postgres; (d) IcebergEngine + two-engine conformance; (e) loci taskflow re-point + spec-doc HWM correction.
- End PR-style commit messages with the `Co-Authored-By: Claude Opus 4.8 (1M context)` trailer. Do not push (not requested).

## uv setup

- Install uv: `curl -LsSf https://astral.sh/uv/install.sh | sh` (fallback if network-blocked: author `pyproject.toml` by hand + `python3 -m venv`, `pip install -e`).
- `uv init --package datadongle`; author `pyproject.toml`: `requires-python = ">=3.11"`, core deps minimal; **optional extras**: `[postgres]` → `psycopg2-binary`; `[iceberg]` → `pyiceberg[sql-sqlite]`, `duckdb`, `pyarrow`; `[geo]` → `shapely`, `geopandas`; `[dev]` → `pytest`, `pandas`, `requests`, `tenacity`.
- `uv sync --extra iceberg --extra geo --extra dev`; run tests with `uv run pytest`. Commit `uv.lock`.

## Module plan (`src/datadongle/`)

Mirrors the layout in the spec doc. Concrete pieces:

- `core/spec.py` — `DatasetSpec` base (from `loci/collectors/base_spec.py`), unchanged.
- `core/schema.py` — `Column`, `ColumnType`, `GeometrySpec`, `TableSchema(columns, entity_key, geometry)`.
- `core/write_mode.py` — `Append`, `Upsert(keys, on_conflict)`, `Scd2(entity_key, invalidate_missing=False)`.
- `core/cursor.py` — `CursorSpec(column, tiebreak=None)` and `Cursor` value (the HWM abstraction; see below).
- `core/engine.py` — `Engine` + `WriteSession` protocols.
- `core/reader.py` — `SourceReader` protocol.
- `load/driver.py` — `run_collection(reader, spec, engine, tracker, *, mode)` — the shared full/incremental orchestration.
- `load/tracking.py` — `IngestionTracker` (from `loci/tracking/ingestion_tracker.py`), used only for **run logging** now, not HWM.
- `load/schema_drift.py` — preflight column check via `engine.table_columns` (extracted from `SocrataCollector._preflight_column_check`).
- `geometry/types.py` — neutral `Geometry(kind, srid)`, EWKT⇄EWKB helpers (reuse `loci/parsers/geojson.py` WKB output + Socrata point→EWKT logic).
- `engines/postgres.py` — `PostgresEngine` + `PgWriteSession` (wraps today's `StagedIngest` from `loci/db/core.py`, largely lifted verbatim) + `read_high_water_mark`.
- `engines/iceberg.py` — `IcebergEngine` + `IcebergWriteSession` (PyIceberg append + DuckDB detect) + `read_high_water_mark`.
- `engines/_duckdb.py` — DuckDB connection/session helpers (memory_limit, temp_directory, spatial + iceberg extensions).
- `collectors/socrata/` — `spec.py`, `client.py`, `metadata.py` moved in (adjusted imports), plus new `reader.py` implementing `SourceReader` around them.

## HWM-from-table design (correction #1)

- `SourceReader` exposes `cursor_spec(spec) -> CursorSpec | None` — Socrata returns `CursorSpec(column="socrata_updated_at", tiebreak="socrata_id")`. `None` ⇒ source isn't incrementally queryable (Socrata `file_download` mode) ⇒ always full.
- `Engine.read_high_water_mark(target, cursor_spec) -> Cursor | None` reads `max(cursor_column)` (and the tiebreak value at that max) **from the target table**, in the engine's own dialect. This lifts the existing `SocrataCollector._get_hwm_from_table` SQL into `PostgresEngine`, and implements the DuckDB/`iceberg_scan` equivalent in `IcebergEngine`.
- `driver.run_collection`: `since = None if mode=="full" else engine.read_high_water_mark(spec.target, reader.cursor_spec(spec))`; the reader turns `since` into the source filter, and yields batches; the tracker still logs the resulting HWM for observability but is **not** the source of truth.

## Socrata reader (decoupling axis 3)

`SocrataReader` keeps the spec/client/metadata but the mode-dispatch logic from `SocrataCollector.collect` is deleted in favor of the shared driver. The reader supplies: `schema(spec)` (from `SocrataTableMetadata`, geometry-aware), `write_mode(spec)` (`Scd2(entity_key)` when `entity_key`, else `Append`/`Upsert`), `cursor_spec(spec)`, `read(spec, since)` (paginate SODA or stream the file export, applying the existing system-field rename, computed-region drop, and location→EWKT transforms), and `extract_cursor(batch)`.

## IcebergEngine (Shape B) essentials

- Local warehouse dir + PyIceberg `SqlCatalog` on a local SQLite file (hermetic).
- `ensure_table` translates `TableSchema` → PyIceberg schema (geometry → `binary` WKB + SRID in field metadata); satellites add `record_hash`, `effective_from`, `ingested_at`, `load_id`; no `valid_to`/`is_current`.
- `IcebergWriteSession`: `write_batch` streams rows to a temp Parquet; on clean exit, DuckDB computes `record_hash` and anti-joins incoming vs. derived-current to emit only new/changed versions, handed to PyIceberg `table.append`. `Append`/`Upsert` are query variants.
- `query` runs DuckDB SQL over `iceberg_scan(...)` with the spatial extension; returns DataFrame/GeoDataFrame. Ship `*_current` / `*_timeline` views per satellite.
- `maintain(target)` wrapping `rewrite_data_files` + `expire_snapshots` — stub for now, exercised later.

## Testing (must pass)

- Recreate the `tests/` patterns under `datadongle/tests/`: reuse the DB-backed-skip `engine`/`schema` fixtures (`tests/collectors/conftest.py`) and the `NoopTracker` fake (`tests/collectors/common.py`).
- **Iceberg tests are hermetic** (tmp_path warehouse + SQLite catalog) — run with no external service.
- **Engine-conformance suite**: parametrize the same Socrata reader run over `[PostgresEngine, IcebergEngine]`; assert identical logical outcomes — row counts, a changed record produces a second version, an unchanged re-pull is a no-op, and HWM is read back from the table correctly after a simulated drop/rebuild.
- Geometry round-trip tests (EWKT/EWKB in → geometry out) on both engines; `record_hash` stability across equivalent geometries.
- Port the existing Socrata collector tests to the reader+driver structure; keep `tests/db/test_core.py` SCD2 coverage for `PgWriteSession`.
- Postgres-backed tests skip without a DB (as today); the Iceberg + conformance-on-Iceberg tests are the ones that must be green everywhere.

## Verification (end-to-end)

1. `cd /workspace/datadongle && uv run pytest` — all hermetic (Iceberg + unit) tests green; Postgres tests skip or pass if `DWH_TEST_PG*` is set.
2. Manual Iceberg smoke: build an `IcebergEngine` on a tmp warehouse, run `SocrataReader` for one small dataset via `run_collection(mode="full")`, then `mode="incremental"` again → second run appends 0 new versions (no-op); mutate a staged record → one new version appears; `engine.query("select * from <ds>_current")` returns latest rows with geometry parsed.
3. Confirm `python -c "import datadongle"` pulls in **no** `airflow`.
4. In `loci`, confirm `collectors/socrata/taskflow.py` imports `datadongle` and calls `run_collection`, and that removing the old mode logic didn't break `tests/tasks/test_task_utils.py`.

## Sequenced steps (each ends green + a commit)

1. Baseline commit on `main`; branch `feat/datadongle-socrata`.
2. uv scaffold + move `core/spec.py`, `core/schema.py`, `core/write_mode.py`, `core/cursor.py`, `load/tracking.py`, `geometry/types.py`; unit tests for these. **Commit.**
3. `core/engine.py`/`reader.py` protocols + `engines/postgres.py` (wrap `StagedIngest`, add `read_high_water_mark`, `ensure_table`, `table_columns`, `geometry_columns`); port `tests/db/test_core.py`. **Commit.**
4. `load/driver.py` + `load/schema_drift.py` + `collectors/socrata/` (moved spec/client/metadata + new `reader.py`); Socrata-on-Postgres tests. **Commit.**
5. `engines/iceberg.py` + `engines/_duckdb.py`; hermetic Iceberg tests + two-engine conformance + geometry round-trip. **Commit.**
6. Re-point `loci/collectors/socrata/taskflow.py` to `datadongle`; apply the HWM-from-table correction to the spec doc; final checks. **Commit.**

## Resume notes — end of session 2026-07-01

**Done so far:**
- Baseline commit on `main` (`5785fa9`), branch **`feat/datadongle-socrata`** checked out.
- Design doc committed at `docs/issues/datadongle-collector-refactor-and-iceberg-engine.md`; this plan committed at `docs/planning/datadongle-socrata-plan.md` (`020d054`).
- Host-side `uv init` left artifacts at repo root: `pyproject.toml` (`name = "datadongle"`, `requires-python = ">=3.13"`), `main.py`, `README.md`, `.python-version` → `3.13`. These still need adjusting (below).

**Environment blockers to clear before resuming implementation:**
- `uv` binary is **not installed inside this container** (only host-side); add it to the Dockerfile for this Claude Code env.
- Container Python is **3.11.2**; `pyproject` pins `>=3.13`. Either add a 3.13 interpreter (uv can fetch one) or set `requires-python = ">=3.11"`. Decide when resuming.

**Layout decision (updates the earlier "Where things live" section):**
- The repo root IS the `datadongle` project. Create **`src/datadongle/`** and migrate the existing `loci/` code into it so **`loci/` no longer exists**. Relocate `tests/` sensibly (e.g. repo-root `tests/` mapping onto the new `src/datadongle/` modules). Remove the `uv init` stub `main.py`.
- **OPEN QUESTION to confirm first thing on resume:** does the *entire* `loci/` package move into `datadongle` (including the Airflow-coupled `*/taskflow.py` and `sources/update_configs.py`), or only the collector tooling + engines + shared load layer, with the Airflow glue extracted to a separate location? This must be reconciled with the hard rule that **`datadongle` imports no Airflow**. Likely answer: taskflows/DAGs live in a separate top-level (e.g. `airflow/` or a `datadongle-airflow` extra) that depends on `datadongle`; confirm with the user.

**How to resume:** from `/workspace`, run `claude --continue` (resumes this session if `/root/.claude` persisted) or `claude --resume` to pick it. If history was lost in the rebuild, point a fresh session at `docs/issues/…` + `docs/planning/…` and say "continue implementing the datadongle plan." Next actionable step is **step 2 (uv scaffold + core primitives)** below.

## Out of scope (this slice)

Other ~12 collectors; Trino/Spark; remote object stores; removing the now-duplicated `loci/db/core.py` and `loci/collectors/socrata/*` (a follow-up once all collectors migrate); `invalidate_missing` Shape-B analogue; raster on Iceberg.
