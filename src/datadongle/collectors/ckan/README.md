# CKAN collector

CKAN powers open-data portals run by many governments and institutions (data.gov and countless municipal portals expose a CKAN action API). A CKAN dataset — a "package" — bundles one or more *resources* (files: CSV, GeoJSON, …), and some resources are also loaded into CKAN's *DataStore*, a queryable table sitting behind the file. This collector downloads the resource file(s), parses them, and SCD2-merges into a `raw_data` table.

It is full-refresh-only: every run re-downloads and re-merges, with no incremental path. That's fine for the small-to-medium datasets CKAN typically serves.

Reach for this collector when the portal exposes a CKAN action API at `/api/3/action/...`.

The four classes: `CKANMetadata` (browse the portal, inspect resources), `CKANDatasetSpec`, `CKANClient` (download files, query the DataStore), and `CKANReader` — which adapts the source to datadongle's shared [`run_collection`](../../load/driver.py) driver so the same collection lands on any storage engine (`PostgresEngine`, `IcebergEngine`).

## 1. Find the dataset

`CKANMetadata` browses the portal; reach it through a client:

```python
from datadongle.collectors.ckan.client import CKANClient

client = CKANClient("https://data.cityofchicago.org")
meta = client.metadata

meta.search_datasets("food inspections")   # browse the catalog
```

A CKAN dataset is identified by its package name (URL slug) or UUID. Once you have it, list its resources:

```python
resources = meta.find_resources("4ijn-s7e5", "CSV")   # or meta.get_resource(resource_uuid)
for r in resources:
    print(r.id, r.format, r.datastore_active, r.url)
```

Each `CKANResource` carries `id`, `name`, `format`, `url`, and `datastore_active` (whether it's queryable via the DataStore). The spec selects resources either by these `id`s or by format.

## 2. Inspect the dataset

For a DataStore-backed resource you can read its schema without downloading the file:

```python
meta.get_datastore_fields(resource_id)   # [{"id": "inspection_id", "type": "text"}, ...]
```

And preview a few rows straight from the DataStore:

```python
client.datastore_search(resource_id, limit=5)
```

`get_datastore_fields` already strips CKAN's internal columns (`_id`, `_full_text`), so what you see is the actual data schema. For file-only resources (no DataStore) there's no server-side preview — download the file (`client.download_to_tempfile(resource.url)`) and eyeball the first rows.

## 3. Write the spec

```python
from datadongle.collectors.ckan.spec import CKANDatasetSpec

CHICAGO_FOOD_INSPECTIONS_SPEC = CKANDatasetSpec(
    name="chicago_food_inspections",
    base_url="https://data.cityofchicago.org",
    dataset_id="4ijn-s7e5",
    target_table="chicago_food_inspections",
    entity_key=["inspection_id"],
    resource_format="CSV",
)
```

Field by field:

- `base_url` — the portal root (trailing slash is stripped automatically).
- `dataset_id` — the package slug or UUID.
- `resource_ids` vs `resource_format` — provide exactly one. `resource_ids` (explicit UUIDs) takes precedence and pins specific resources; `resource_format` (e.g. `"CSV"`, `"GeoJSON"`) ingests every matching resource on the dataset.
- `entity_key` — the SCD2 key. `None` means append-only.

### Gotchas

- **Provide `resource_ids` or `resource_format`.** Omitting both raises in `__post_init__` — there's no default selection.
- **Multiple resources feeding one table should share columns.** The table schema is discovered from the *first* resolved resource; a later resource whose columns drift is warned about, its extra columns are ignored, and its missing columns load as NULL. Keep multi-resource specs (e.g. yearly CSVs) column-consistent.
- **CKAN's internal columns are handled for you.** `_id` and `_full_text` are stripped throughout — don't put them in the spec.
- **Column names are normalized.** Raw headers are lowercased with non-alphanumerics collapsed to `_` (collisions get `_2`/`_3` suffixes), so the `entity_key` must use the *normalized* names.

### Finding the `entity_key`

Same principle as Socrata — verify uniqueness against the source rather than guessing — but the available tool depends on the resource:

- **DataStore-backed resources** support raw SQL via CKAN's `datastore_search_sql` action, so the uniqueness check is a server-side `GROUP BY ... HAVING count(*) > 1`, no ingest required:

  ```sql
  SELECT "inspection_id", count(*) AS n
  FROM "<resource_id>"
  GROUP BY "inspection_id" HAVING count(*) > 1 LIMIT 5
  ```

  An empty result means the column is unique. (This isn't wrapped in a helper yet — a CKAN analog of the Socrata `find_duplicate_keys` would be the natural addition.)
- **File-only resources** have no server-side query, so fall back to a one-time ingest plus a warehouse `group by ... having count(*) > 1`, or trust an obvious domain id.

## 4. Collect

`CKANReader` adapts the source to the shared `run_collection` driver. Hand the driver a reader, the spec, and a storage engine; the engine creates the table for you and lands the data — no manual DDL or migration step.

```python
from datadongle.collectors.ckan.reader import CKANReader
from datadongle.engines.postgres import PostgresEngine   # or engines.iceberg.IcebergEngine
from datadongle.load.driver import run_collection

reader = CKANReader()
engine = PostgresEngine(creds)                            # or IcebergEngine("/data/warehouse")

summary = run_collection(reader, CHICAGO_FOOD_INSPECTIONS_SPEC, engine, mode="full")
```

`run_collection` calls `engine.ensure_table(...)` first, deriving the table shape from the source: if the first resource is in the DataStore, its typed field metadata becomes the schema; otherwise the resource file is downloaded and its header scanned — CSV columns are all `text`, GeoJSON contributes the first feature's properties plus a `geom` geometry column (`geometry(Geometry, 4326)` on PostGIS, WKB on Iceberg). A file downloaded for schema discovery is cached and consumed by the read, so each run fetches each resource once. On top of the discovered columns the engine adds `ingested_at` and — when `entity_key` is set — its SCD2 columns and indexes. The call returns a summary dict (`rows_staged`, `rows_merged`, `rows_invalidated`, `high_water_mark`).

CKAN has no update cursor (`cursor_spec` is `None`), so the mode barely matters: `mode="incremental"` logs that the source isn't incrementally queryable and runs a full read anyway. Use `mode="full"` for clarity.

On the first parsed batch of each resource the reader compares the resource's normalized columns against the discovered schema and warns on drift — a resource that gained a column since the table was created gets flagged rather than silently dropped (the engines ignore columns the table doesn't have).

The `Append` / `Upsert` / `SCD2` write-mode behaviors and the `full` vs `incremental` collection modes are shared across all collectors and documented in the [top-level README](../../../../README.md).

## Scheduling

In production the spec is wrapped in a `DatasetUpdateConfig` and a scheduled taskflow calls `run_collection(reader, spec, engine, mode="full")`. The CKAN taskflow is single-path — there's no incremental mode to choose — a deliberate divergence from the other sources. Set whatever cadence you want via `update_cron`.
