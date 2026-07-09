# Bike Index collector

[Bike Index](https://bikeindex.org) is a stolen-bike registry with a public API v3. There's no catalog of datasets to browse — you define a geographic search (a location and a radius), and the collector pulls matching stolen-bike reports into a `raw_data` table, enriching each bike with its full record along the way. It supports incremental updates keyed off each bike's `date_stolen`.

Reach for this collector when you want stolen-bike reports for an area.

The tooling is three classes: `BikeIndexDatasetSpec` (declare what to collect and where), `BikeIndexClient` (talk to the API; `BikeIndexSearchParams` is its search descriptor), and `BikeIndexReader` — which adapts the dataset to datadongle's shared [`run_collection`](../../load/driver.py) driver so the same collection lands on any storage engine (`PostgresEngine`, `IcebergEngine`).

There is **no `metadata.py`**: Bike Index has no column-catalog API, so the target schema is fixed in the reader (`BikeIndexReader` defines the column list) and there's no per-dataset column discovery.

## 1. Two calls per bike, one pass

The API splits a bike across two endpoints:

- `GET /api/v3/search` enumerates bikes near a location, returning **summary** fields (`id`, `title`, `serial`, `manufacturer_name`, `frame_model`, `date_stolen`, `stolen_coordinates`, …).
- `GET /api/v3/bikes/{id}` returns the **full** record — the `stolen_record` (precise `latitude`/`longitude`, theft/locking descriptions, police-report fields), plus components, photos, and the detail-only registration fields.

A complete row therefore needs both calls. `BikeIndexReader.read` makes them in one pass: it pages the search API and, for each bike past the incremental cursor, immediately calls `get_bike(id)` and yields one fully-enriched row (detail-only columns filled in, not left `NULL`). There is no separate "detail phase" and no family driver — this fits the shared `run_collection` directly, and gives each bike a single SCD2 version per change. If a detail fetch fails, the bike still lands as a summary-only row (detail columns `NULL`) rather than being dropped.

## 2. Explore: shape a search and gauge volume

"Finding the dataset" means tuning a search. Start with the count endpoint to see how many reports a location/radius will return before pulling anything:

```python
from datadongle.collectors.bike_index.client import BikeIndexClient, BikeIndexSearchParams

client = BikeIndexClient()   # access_token optional; anonymous works for read-only search
search = BikeIndexSearchParams(location="Chicago, IL", distance=10, stolenness="proximity")

client.search_count(search)          # {"proximity": N, "stolen": N, "non": N}
client.search(search, page=1)["bikes"]   # one page of summary dicts
client.get_bike(some_id)             # full detail dict for one bike
```

`stolenness` controls scope: `"proximity"` (stolen near the location), `"stolen"`, `"non"` (recovered/not stolen), or `"all"`. `query` adds a free-text filter (brand, model, color). Adjust `location`/`distance` until `search_count` looks right.

## 3. Write the spec

`BikeIndexDatasetSpec` describes what to pull and where it lands:

```python
from datadongle.collectors.bike_index.spec import BikeIndexDatasetSpec

CHICAGO_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="chicago_bikeindex_bike_thefts",
    target_table="chicago_bikeindex_bike_thefts",
    target_schema="raw_data",
    entity_key=["id"],        # Bike Index's stable per-bike id -> SCD2 versioning
    location="Chicago, IL",   # city, zip, address, or "lat,lon"
    distance=10,              # radius in miles
    stolenness="proximity",   # "proximity" | "stolen" | "non" | "all"
    query=None,               # optional free-text (brand, model, color)
    per_page=100,             # search page size (API max 100)
)
```

Field by field:

- `entity_key` — the column(s) that uniquely identify a record, used for SCD2 versioning. The Bike Index `id` is the natural unique key and is the default (`["id"]`); leave it as is. `None` (or `[]`) means append-only (no versioning).
- `location` / `distance` / `stolenness` / `query` / `per_page` — the search parameters. `stolenness` is validated in `__post_init__` (anything outside the four allowed values raises at construction).

`spec.to_search_params()` is what the reader feeds into `BikeIndexSearchParams`, so the spec and your exploration search stay in sync.

## 4. Collect

`BikeIndexReader` adapts the dataset to the shared `run_collection` driver. Hand the driver a reader, the spec, and a storage engine; the engine creates the table for you and lands the data — no manual DDL step.

```python
import os
from datadongle.collectors.bike_index.reader import BikeIndexReader
from datadongle.engines.postgres import PostgresEngine   # or engines.iceberg.IcebergEngine
from datadongle.load.driver import run_collection

reader = BikeIndexReader(access_token=os.environ.get("BIKEINDEX_ACCESS_TOKEN"))
engine = PostgresEngine(creds)                            # or IcebergEngine("/data/warehouse")

run_collection(reader, CHICAGO_BIKE_THEFTS_SPEC, engine, mode="full")            # full refresh
summary = run_collection(reader, CHICAGO_BIKE_THEFTS_SPEC, engine, mode="incremental")
```

`run_collection` calls `engine.ensure_table(...)` first, deriving the table shape from the reader's fixed schema: one column per bike field (core search fields, `stolen_record` fields, detail-only scalars, and `frame_colors`/`components`/`public_images` as JSON), an `ingested_at` column, and — when `entity_key` is set — the engine's SCD2 columns (physical `valid_from`/`valid_to` on Postgres, an append-only satellite on Iceberg) plus the matching uniqueness / current-version indexes. The call returns a summary dict (`rows_staged`, `rows_merged`, `rows_invalidated`, `high_water_mark`).

An access token is optional — unauthenticated reads work but hit stricter rate limits. The client paces itself with a per-request delay and retries `429`/`5xx` with backoff (honoring `Retry-After`).

What the mode means:

- `mode="full"` → page the entire search result and enrich every bike.
- `mode="incremental"` → resume from the target table's max `date_stolen` (with `id` as a tiebreak), read back from the table itself so it self-heals across a drop/rebuild. The search API has no server-side ordering to stop early on, so an incremental run still pages every result — but it only calls the expensive `get_bike` endpoint for bikes newer than the high-water mark.

### The `date_stolen` cursor

`date_stolen` is a unix timestamp (bigint). The cursor filter is applied per-row, client-side, strictly after `(date_stolen, id)` — a bike stolen at the same second as the high-water mark is disambiguated by its `id`, so no boundary row is re-emitted or skipped. Bikes with a null `date_stolen` are always kept (they carry no cursor value).

The `Append` / `Upsert` / `SCD2` write-mode behaviors (chosen here by whether the spec has an `entity_key`) and the `full` vs `incremental` collection modes are shared across all collectors and documented in the [top-level README](../../../../README.md).
