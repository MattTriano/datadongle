# TIGER collector

The Census Bureau publishes the nation's geography as **TIGER/Line** shapefiles (full detail) and **Cartographic Boundary** files (simplified, coastline-clipped) at `www2.census.gov/geo/tiger`, organized by vintage (year), layer (`TRACT`, `ROADS`, `PRIMARYROADS`, …), and geography. This collector pulls a layer, across a set of vintages and states, into one `raw_data` table with a geometry column.

Reach for this collector when you need Census geometry — boundaries, roads, water, landmarks — as opposed to Census *tables* (use the `census` collector for those).

The tooling is a client, metadata, spec, reader, and family driver: `TigerMetadata` (browse vintages/layers/files, build URLs), `TigerDatasetSpec` (declare what to collect), `TigerClient` (HTTP: directory listings + streamed downloads), `TigerReader` (adapt one file to datadongle's engines), and `run_tiger_collection` (fan the spec out over `vintages × units` into one table on any engine — `PostgresEngine`, `IcebergEngine`).

## Why a family driver

A TIGER spec is a *matrix*: `vintages × units`, where a "unit" depends on the layer's scope — **national** (one file per vintage), **state** (one file per state), or **county** (one file per county, discovered by listing a directory). Those files all land in one table, and the shapefile schema can drift across vintages. That doesn't fit the shared `run_collection` contract, so TIGER ships `run_tiger_collection`, which discovers each vintage's schema from a sample file, ensures a **union-of-vintages** table, resolves **one write mode** for the whole table, then collects each file into its own staged write session with **per-file error isolation**.

## 1. Explore the source

```python
from datadongle.collectors.tiger.metadata import TigerMetadata

tm = TigerMetadata()
tm.list_vintages()                       # TIGER/Line years (source="cartographic" for CB)
tm.list_layers(2024, keyword="road")     # layers + their scope (state/county/national)
tm.list_files(2024, "TRACT")             # downloadable files for a layer
tm.get_download_url(2024, "TRACT", state_fips="17")
```

`list_layers` reports each layer's `scope`, which is what determines the fan-out. The values here feed the spec.

## 2. Write the spec

```python
from datadongle.collectors.tiger.spec import TigerDatasetSpec

CENSUS_TRACTS_SPEC = TigerDatasetSpec(
    name="census_tracts",
    layer="TRACT",
    vintages=[2023, 2024],
    target_table="census_tracts",
    target_schema="raw_data",
    state_fips=["17"],          # omit for all states; ignored for national layers
)
```

Field notes:

- `layer` — a TIGER layer (`TRACT`, `ROADS`, `PRIMARYROADS`, …), case-insensitive. Its scope is looked up automatically.
- `source` — `"tiger"` (default) or `"cartographic"`; `resolution` (e.g. `"500k"`) applies to cartographic files only.
- `state_fips` — the states to collect; `None` means all. For county layers, the driver lists the server and keeps counties whose state is requested.
- `entity_key` — the columns uniquely identifying a feature, used for SCD2 versioning. **If you leave it unset, the reader auto-detects a stable ID column** (`geoid`/`geoidfq`/`linearid`/`tlid`/`areaid`, including year-suffixed variants like `geoid20`) from the shapefile and uses `[id, "vintage"]`. If no ID column exists (e.g. `COASTLINE`), the layer is collected **append-only**. Set `entity_key` explicitly to override.
- `lowercase_columns` — lowercase shapefile column names (default `True`).

## 3. Collect

```python
from datadongle.collectors.tiger.reader import TigerReader
from datadongle.collectors.tiger.driver import run_tiger_collection
from datadongle.engines.iceberg import IcebergEngine   # or engines.postgres.PostgresEngine

reader = TigerReader()
engine = IcebergEngine("/data/warehouse")              # or PostgresEngine(creds)

summary = run_tiger_collection(reader, CENSUS_TRACTS_SPEC, engine)
# {"spec_name", "vintages_processed", "files_processed",
#  "total_rows_staged", "total_rows_merged", "total_rows_invalidated", "errors"}
```

The table gets one column per shapefile field (lowercased), a `vintage` column, synthetic `statefp`/`countyfp` for county files that lack them, a geometry column (`geom`, created as PostGIS `geometry` or Iceberg WKB automatically), an `ingested_at` column, and — when an entity key is resolved — the engine's SCD2 columns and indexes. Geometry is promoted to the `Multi*` form (shapefiles mix single/multi).

### Modes and re-runs

There is no incremental mode: a vintage is **immutable** and shapefiles have no row cursor, so every collection is a full read. With an entity key (SCD2), re-collecting an unchanged file is a no-op merge, so a half-finished run just needs re-running. The collector keeps **no already-ingested skip** (it can't be expressed portably across both engines), so a re-run **re-downloads and re-parses** every file — which is costly for large layers (national roads, block groups). Collect only the vintages you need, and for **append-only** (no-ID) layers avoid re-running at all, since a second run duplicates rows.

## Gotchas

- **Schema discovery samples one file per vintage** (the first requested state, or the national/first-county file). If that specific file is unavailable, the vintage is skipped — pick reachable states or set an explicit vintage list.
- **Geometry passes through as WKB-hex** from the parser; both engines accept it. No SRID other than 4326 is assumed.
- **`ensure_table` won't alter an existing table.** If a later vintage adds a field after the table exists, collect all vintages together the first time so the union table has every column.

The `Append` / `SCD2` write-mode behaviors and the collector interface as a whole are documented in [`docs/spec/collector-interface.md`](../../../../docs/spec/collector-interface.md).
