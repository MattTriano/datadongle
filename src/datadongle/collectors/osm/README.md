# OSM (Overpass) collector

OpenStreetMap data, fetched from the Overpass API. Unlike the catalog-backed sources, OSM has nothing to browse: you declare *what* to fetch with an Overpass query — element types, tag filters, and a spatial extent — and the collector runs it, assembles geometry, promotes selected tags to typed columns, and SCD2-merges into a `raw_data` table. It supports incremental updates via Overpass's `(newer:)` filter.

Reach for this collector when you want features straight from OSM (cafes, bike racks, building footprints, …) rather than from a government portal.

This source diverges from the four-class shape: there's **no Metadata class** (no catalog exists). The pieces are `OverpassAPIQuery` (a declarative query builder), `OSMDatasetSpec` (declare what to collect and where), `OSMClient` (talk to the Overpass API), and `OSMReader` — which adapts the query to datadongle's shared [`run_collection`](../../load/driver.py) driver so the same collection lands on any storage engine (`PostgresEngine`, `IcebergEngine`). Geometry assembly lives in the `geometry` module.

## 1. Explore: prototype an Overpass query

"Finding the dataset" here means building a query that returns what you want. `OverpassAPIQuery` is declarative — element types, tag filters, and a spatial extent:

```python
from datadongle.collectors.osm.query import OverpassAPIQuery, Regex

cafes = OverpassAPIQuery(
    element_types=["node", "way"],
    tag_filters=[{"amenity": "cafe"}],
)
```

A query with no `bbox`/`area_name` is a reusable *template*; calling `.to_ql()` on it raises. Bind it to a place with `.for_bbox(BBox(south, west, north, east))` or `.for_area("Chicago")`:

```python
from datadongle.geo import BBox

chicago_cafes = cafes.for_bbox(BBox(41.62, -87.97, 42.05, -87.5))
print(chicago_cafes.to_ql())   # inspect the rendered Overpass QL before running it
```

Tag-filter value semantics, per group dict (groups are OR'd, entries within a group are AND'd):

- `None` — key exists with any value (`{"amenity": None}`).
- `str` — exact match (`{"amenity": "cafe"}`).
- `list[str]` — any of these values (`{"shop": ["supermarket", "convenience"]}`).
- `Regex("...")` — raw POSIX-ERE pattern, passed through unescaped; set `case_insensitive=True` for the `,i` modifier (Overpass doesn't support `(?i)`).

Preview what the query actually returns before wiring up a table — `OSMClient.fetch` gives you the raw Overpass elements:

```python
from datadongle.collectors.osm.client import OSMClient

client = OSMClient()
response = client.fetch(chicago_cafes)   # raw Overpass JSON
for element in response["elements"][:5]:
    print(element["type"], element["id"], element.get("tags"))
```

## 2. Inspect: decide which tags to promote

Every row always carries the fixed columns — `osm_type`, `osm_id`, `osm_version`, `osm_timestamp`, `geom`, `node_ids`, and the full tag dict in a `tags` JSON column. "Inspecting" is really deciding which tag keys to lift out of `tags` into their own typed columns. Look at the `tags` dict on a few previewed elements and pick the keys you'll query on (`name`, `addr:street`, `cuisine`, …). Everything stays in `tags` regardless, so promotion is purely about query convenience.

## 3. Write the spec

```python
from datadongle.collectors.osm.spec import OSMDatasetSpec

CHICAGO_CAFES_SPEC = OSMDatasetSpec(
    name="chicago_cafes",
    target_table="chicago_cafes",
    target_schema="raw_data",
    query=chicago_cafes,
    promoted_tags=["name", "addr:street", "cuisine"],
)
```

Field by field:

- `query` — an `OverpassAPIQuery` with a spatial extent set. A bare template (no `bbox`/`area_name`) will raise at collection time, so bind it with `.for_bbox`/`.for_area` first.
- `promoted_tags` — tag keys lifted into typed `text` columns. Keys that aren't valid Postgres identifiers are auto-normalized for the column name (`addr:street` → `addr_street`, `name:en` → `name_en`) while the original key is kept for the OSM lookup. May be empty.
- `entity_key` — defaults to `["osm_type", "osm_id"]`, which is the natural stable key for OSM. Overriding it emits a warning.

### Gotchas

- **Bind the query before collecting.** A template query (no extent) raises on `.to_ql()`.
- **`promoted_tags` collisions raise.** If two keys normalize to the same column name (or a key normalizes to empty / starts with a digit), the spec rejects it in `__post_init__` — rename one manually.
- **All tags are retained.** Promotion never drops data; unpromoted tags live in `tags`. If you mix subtypes in one query (e.g. `amenity` and `shop`), promote each and `COALESCE` at query time.

### Finding the `entity_key`

Nothing to discover here — `["osm_type", "osm_id"]` uniquely identifies an OSM element and is the default. Leave it alone unless you have a specific reason not to (and expect the warning if you do).

## 4. Collect

`OSMReader` adapts the query to the shared `run_collection` driver. Hand the driver a reader, the spec, and a storage engine; the engine creates the table for you and lands the data — no manual DDL or migration step.

```python
from datadongle.collectors.osm.reader import OSMReader
from datadongle.engines.postgres import PostgresEngine   # or engines.iceberg.IcebergEngine
from datadongle.load.driver import run_collection

reader = OSMReader()
engine = PostgresEngine(creds)                            # or IcebergEngine("/data/warehouse")

run_collection(reader, CHICAGO_CAFES_SPEC, engine, mode="full")          # full pull
summary = run_collection(reader, CHICAGO_CAFES_SPEC, engine, mode="incremental")
```

`run_collection` calls `engine.ensure_table(...)` first, deriving the table shape from the spec: the fixed OSM columns (`osm_type`, `osm_id`, `osm_version`, `osm_timestamp`, `geom`, `tags`, `node_ids`), one `text` column per promoted tag, an `ingested_at` column, and — because OSM keys on `["osm_type", "osm_id"]` — the engine's SCD2 columns (physical `valid_from`/`valid_to` on Postgres, an append-only satellite on Iceberg) plus the uniqueness / current-version indexes and a GIST index on `geom`. The `geom` column is a PostGIS `geometry(Geometry, 4326)` (or Iceberg WKB) created automatically. The call returns a summary dict (`rows_staged`, `rows_merged`, `rows_invalidated`, `high_water_mark`).

`osm_version` and `osm_timestamp` are stored but excluded from the SCD2 content hash — they bump on every OSM edit, so hashing them would spuriously version every unchanged element on a re-pull.

What the mode means:

- `mode="full"` → runs the query over the whole extent with SCD2 `invalidate_missing=True`, so elements that have disappeared from OSM are closed out (SCD2 `valid_to` set) — this is how deletions are caught.
- `mode="incremental"` → resumes from the target table's `max(ingested_at)` — "when we last collected" — and passes it as the Overpass `(newer:)` floor, fetching only elements edited since. `invalidate_missing` is off (an incremental pull can't observe which entities are absent). If the table is empty there's no floor, so it transparently reads everything.

The high-water mark is `ingested_at`, not an OSM timestamp — the incremental floor is "when we last collected," which is what you want for catching edits since the previous run. Because a full pull is what catches deletions, it's worth scheduling a periodic `mode="full"` run rather than relying on incrementals forever.

`ensure_table` uses `create table if not exists`, so it won't alter an existing table — if you add a promoted tag later, evolve the table before collecting.

The `Append` / `Upsert` / `SCD2` write-mode behaviors and the `full` vs `incremental` collection modes are shared across all collectors and documented in the [top-level README](../../../../README.md).

## Scheduling

In production the spec is wrapped in a `DatasetUpdateConfig` and a scheduled taskflow calls `run_collection(reader, spec, engine, mode=…)`, deciding full vs. incremental per run — scheduling a periodic full pull so deletions are caught.
