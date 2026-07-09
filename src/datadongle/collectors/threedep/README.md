# 3DEP collector

Collection tooling for [USGS 3DEP](https://www.usgs.gov/3d-elevation-program) seamless elevation (DEM) rasters. Given a bounding box (one per city, mirroring the per-city OSM raw tables), it downloads the 1-degree GeoTIFF tiles covering it, cuts them into sub-tiles clipped to the bbox, and lands them SCD2-versioned in one raster table per city.

The tooling is four pieces: `ThreeDEPDatasetSpec` (declare what to collect and where), `ThreeDEPClient` (tile addressing + streamed downloads), `ThreeDEPMetadata` (coverage exploration), and `ThreeDEPReader` — which adapts the source to datadongle's shared collection driver. Because a spec fans out over multi-hundred-MB tile downloads, 3DEP ships a thin **family driver**, [`run_threedep_collection`](driver.py), rather than using the shared `run_collection` directly (see "Why a family driver" below).

## The publication model

3DEP's seamless products are pre-staged as 1° × 1° GeoTIFF tiles at predictable URLs under the public TNM S3 bucket, with no API or auth required:

    https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/current/n42w088/USGS_13_n42w088.tif

Tiles are named by their **northwest corner** (`nNNwWWW`, so `n42w088` covers latitude 41–42° N and longitude 88–87° W), are distributed in NAD83 (EPSG:4269), and elevations are NAVD88 meters. Because the URLs are predictable there is no catalog API to query: given a bbox we enumerate the tiles it touches and build URLs directly. Two seamless products are supported — `"13"` (1/3 arc-second, ~10 m; the default, right for street grade) and `"1"` (1 arc-second, ~30 m). The 1 m product uses a different, project-based tiling and is out of scope.

The products are essentially **static**: a tile changes only when USGS re-stages it, and tiles missing from the grid (ocean, gaps) simply aren't there.

## 1. Explore

`ThreeDEPMetadata` answers "which tiles does my bbox need, and are they staged?" before a collect run:

```python
from datadongle.collectors.threedep.metadata import ThreeDEPMetadata
from datadongle.geo import BBox

bbox = BBox(south=41.62, west=-87.97, north=42.05, east=-87.5)  # Chicago
meta = ThreeDEPMetadata()
meta.products()             # {'13': '1/3 arc-second (~10 m)', '1': '1 arc-second (~30 m)'}
meta.tiles_for_bbox(bbox)   # ['n42w088', 'n43w088']  (pure; no network)
meta.coverage(bbox)         # {'n42w088': True, 'n43w088': True}  (HEAD checks)
meta.missing(bbox)          # tiles the bbox needs that aren't staged
meta.describe(bbox)         # printed coverage summary for notebook use
```

There is no `search()` — 3DEP has no dataset catalog, just a fixed grid of tiles.

## 2. Write the spec

```python
from datadongle.collectors.threedep.spec import ThreeDEPDatasetSpec

spec = ThreeDEPDatasetSpec(
    name="chicago_elevation",
    target_table="chicago_elevation",
    target_schema="raw_data",
    bbox=BBox(south=41.62, west=-87.97, north=42.05, east=-87.5),
    product="13",
)
```

- `bbox` — must be NAD83 (EPSG:4269, the `BBox` default) so tile selection and clipping line up with the tiles' CRS; the spec rejects anything else.
- `entity_key` is fixed to `["tile_id"]` — every stored sub-tile is keyed on its stable id (`"<1-degree tile>/<row>_<col>"`); overriding raises.
- `tiles` — normally `None` (all tiles the bbox touches). The family driver uses it to narrow a spec to one tile per write session; you can also set it to collect specific tiles.

## 3. Collect

```python
from datadongle.collectors.threedep.driver import run_threedep_collection
from datadongle.collectors.threedep.reader import ThreeDEPReader

reader = ThreeDEPReader()   # optionally: tile_size=512, batch_size=16
summary = run_threedep_collection(reader, spec, engine, tracker, mode="incremental")
```

Works unchanged against `PostgresEngine` and `IcebergEngine`. The driver `ensure_table`s the target, so there is no manual DDL step (the old `print_ddl` workflow is gone).

- **`mode="incremental"`** — the cheap path: collects only 1-degree tiles not yet in the target, skipping the multi-hundred-MB re-download of tiles already held.
- **`mode="full"`** — re-downloads every tile. SCD2 dedupes unchanged sub-tiles away and versions any USGS actually changed, so a periodic full run is how re-staged tiles get picked up.

The reader is a fully conformant `SourceReader`, so the shared `run_collection(reader, spec, engine, tracker, mode=...)` also works — you just get one write session for the whole bbox instead of per-tile isolation.

## How incremental works (and its edges)

The high-water mark is `source_tile` — the 1-degree tile name. Tiles are processed in sorted name order, so the max name in the target is the frontier of a completed prefix, and an incremental run collects only tiles **strictly after** it. The mark is read from the target table itself, so it self-heals if the table is dropped and rebuilt.

Consequences to know about:

- **Widening the bbox** to a tile that sorts *before* the frontier (e.g. adding `n42w087` when `n42w088` is already collected) is invisible to incremental runs — run `mode="full"` once to pick it up.
- **A tile missing at the source** (ocean/gap) is counted and skipped; if USGS stages it later and larger-named tiles have since landed, only a full run will fetch it.
- **A failing tile stops an incremental run** (fail-stop): letting later tiles merge would advance the mark past the failed tile and hide it from future runs. The next incremental run resumes exactly there. A full run isolates the failure and continues instead.

## The table

Each row is one sub-tile (default 512 px edge): `tile_id`, `source_tile`, `rast`, `checksum` (md5 of raw pixels + georeference), `srid`, and the `min_x`/`min_y`/`max_x`/`max_y` extent, plus the engine's `ingested_at` and SCD2 columns.

Two deliberate deviations from the usual column conventions:

- **`rast` is flagged `metadata=True`** — not because it is bookkeeping, but to keep the multi-hundred-KB raster value out of the SCD2 content hash; the cheap `checksum` column already carries the change signal.
- **`rast` is engine-shaped** (`ColumnType.RASTER`). On Postgres it lands as a PostGIS `raster` column (parsed from hex-WKB on COPY) with a GiST `ST_ConvexHull` index over current rows; on Iceberg the decoded WKB bytes are stored as `binary` — faithful storage, but sampling there needs a client-side WKB parser.

Downstream sampling on Postgres:

```sql
select n.node_id,
       ST_Value(r.rast, n.geom, resample => 'bilinear') as elevation_m
from   raw_data.chicago_elevation r
join   nodes n on ST_Intersects(r.rast, n.geom)
where  r.valid_to is null;
```

## Why a family driver

Each 1-degree tile is a multi-hundred-MB download, so `run_threedep_collection` deviates from the shared one-spec/one-write-session shape in one way: **per-tile write sessions with per-tile error isolation**. A failed download doesn't discard other tiles' already-merged work, and a crashed run resumes at the failed tile (see fail-stop above). It is built from the same primitives — the reader, the `Engine` protocol, the tracker contract — and issues no storage-specific SQL.

Memory stays bounded throughout: tiles are downloaded to a temp file, rasterio reads sub-tile windows off disk, and rows are staged in small batches (each carries a large raster field), so peak memory is one sub-tile regardless of bbox size.
