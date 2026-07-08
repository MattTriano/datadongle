# ArcGIS Hub collector

ArcGIS Hub powers open-data sites backed by Esri feature services (many city and police portals). A Hub catalog *item* wraps a feature service, and a service can contain one or more *layers* — each a queryable table of features (attributes plus geometry). This collector queries the layer(s), flattens each feature into attributes + geometry, and SCD2-merges into a `raw_data` table. It supports incremental updates when the layer has a date field to key off.

Reach for this collector when the portal is an ArcGIS Hub / Esri site — you'll see `/api/search/v1/...` catalog endpoints and `FeatureServer` layer URLs.

The pieces: `ArcGISHubMetadata` (browse the catalog), `ArcGISHubDatasetSpec`, `ArcGISHubClient` (HTTP/JSON + pagination), and `ArcGISHubReader` (`reader.py`) — the `SourceReader` adapter that discovers the schema, resolves the layer(s), and streams flattened rows to the shared `run_collection` driver. There is no `Collector` class: the driver decides full vs. incremental and the engine creates the table.

A single spec can name several layers (`layer_index` as a list or `"all"`) that union into one table. Unlike DKAN's family driver, ArcGIS layers are collected through the plain `run_collection`: the reader returns the *union* schema across layers and iterates them into the one write session (ArcGIS has no `invalidate_missing`, so per-layer write sessions aren't needed).

## 1. Find the dataset

`ArcGISHubMetadata` browses the catalog (the Hub OGC API - Records endpoint), paginating for you:

```python
from datadongle.collectors.arcgishub.client import ArcGISHubClient
from datadongle.collectors.arcgishub.metadata import ArcGISHubMetadata

client = ArcGISHubClient("https://data.tps.ca")
meta = ArcGISHubMetadata(client)

for item in meta.search(q="arrests", limit=50):
    print(item["id"], item["properties"]["title"])
```

Each item's `id` is the Hub item id you'll put in the spec. `meta.get_dataset(item_id)` returns the full item dict, including the underlying feature service URL in its properties.

## 2. Inspect the dataset / layers

A single item can wrap a multi-layer service, so the cleanest way to see what you'll actually get is to draft a spec and let the reader resolve the layer(s) and show the neutral column schema:

```python
from datadongle.collectors.arcgishub.reader import ArcGISHubReader

reader = ArcGISHubReader()
for col in reader.schema(spec).columns:      # resolves the layer(s), unions their fields
    print(col.name, col.type.value)
```

If you want the raw layer fields first, the feature service URL lives on the item, and an Esri layer reports its fields directly:

```python
item = meta.get_dataset(item_id)
service_url = item["properties"]["url"]              # the FeatureServer URL
layer0 = client.get_json(f"{service_url}/0", params={"f": "json"})
for f in layer0["fields"]:
    print(f["name"], f["type"])
```

## 3. Write the spec

```python
from datadongle.collectors.arcgishub.spec import ArcGISHubDatasetSpec

TPS_ARRESTS_SPEC = ArcGISHubDatasetSpec(
    name="tps_arrests",
    base_url="https://data.tps.ca",
    item_id="4702e79fd2404f7d93dd9866f45d7ec2",
    target_table="tps_arrests",
    entity_key=["Event_Unique_Id"],
    layer_index=0,
    incremental_column="last_edited_date",
)
```

Field by field:

- `base_url` / `item_id` — the Hub site and the catalog item id.
- `layer_index` — which layer(s) to pull: an `int` (default `0`), a `list[int]` for specific layers, or `"all"` to auto-discover and collect every layer in the service.
- `where` — a server-side SQL filter applied to every request (default `"1=1"`).
- `incremental_column` — a date/time field (e.g. `"last_edited_date"`) used for incrementals; `None` means a full refresh on every run.
- `out_fields` — a subset of fields to request; `None` means all.
- `layer_column` — if set, adds a column holding each row's layer name, useful when collecting multiple layers into one table.
- `min_field_overlap` — when collecting multiple layers, the minimum fraction of fields they must share (default `0.8`); `reader.schema(spec)` raises if overlap is lower.

### Gotchas

- **Do NOT use `OBJECTID` as the `entity_key`.** OBJECTID is not stable across service refreshes or republishes, so it makes a broken SCD2 key — a republish would version every row. Use a domain identifier (a case/event id). The reader marks the OID field as metadata (stored, but excluded from the SCD2 content hash) for the same reason.
- **Multi-layer collections require field overlap.** Layers combined into one table must share at least `min_field_overlap` of their fields, or `reader.schema(spec)` raises.
- **Esri returns errors as HTTP 200.** A feature service often replies `200` with an `{"error": ...}` body; the reader detects this and raises, which is why a "successful" request can still carry an error.

### Finding the `entity_key`

ArcGIS query layers support server-side aggregation, so you can check a candidate key's uniqueness without ingesting: query the layer with `groupByFieldsForStatistics` set to your candidate column(s) and an `outStatistics` count, then look for any group with a count above 1 (some services also honor a `having` parameter). This is the same "verify against the source" idea as the Socrata `find_duplicate_keys` check, just expressed in Esri's query grammar — and it's the constructive flip side of the OBJECTID warning: OBJECTID is unique but unstable, so confirm a *domain* column is unique and key on that. (Not wrapped in a helper yet.)

## 4. Collect

There's no manual DDL step — `run_collection` calls `engine.ensure_table(...)`, deriving the table from `reader.schema(spec)` (Esri field types mapped to neutral column types, a geometry column, the optional `layer_column`, and — added by the engine — `ingested_at` plus the SCD2 columns when `entity_key` is set).

```python
from datadongle.collectors.arcgishub.reader import ArcGISHubReader
from datadongle.load.driver import run_collection
from datadongle.engines.postgres import PostgresEngine   # or engines.iceberg.IcebergEngine

reader = ArcGISHubReader()
engine = PostgresEngine(creds)                 # or IcebergEngine("/data/warehouse")

summary = run_collection(reader, spec, engine, mode="full")          # full refresh
summary = run_collection(reader, spec, engine, mode="incremental")   # requires incremental_column
```

`run_collection` returns a summary dict (`rows_staged`, `rows_merged`, ...). In `incremental` mode it resumes from the target table's high-water mark in `incremental_column`: the engine reads it back as an ISO timestamp and the reader converts it to the epoch-millisecond value ArcGIS filters on. A spec with no `incremental_column` isn't incrementally queryable, so the driver runs a full read regardless of `mode`.

`ensure_table` is create-if-not-exists — it won't evolve an existing table, so if a layer gains a field after the table exists, evolve the table before collecting.

> **Migration note.** The previous `ArcGISHubCollector` had a *freshness-skip* (in full-refresh mode it skipped when any current rows existed) and a strict schema-drift preflight that *raised* on any new source column. Both were dropped in the reader migration, consistent with the OSM/CKAN/DKAN migrations: SCD2 re-pulls are idempotent no-op merges, and the engines tolerate schema drift (extra source columns are ignored, missing ones load as NULL). One consequence: a spec with **no** `entity_key` is append-only, so re-running it is additive — reach for `entity_key` (SCD2) whenever the layer has a stable domain key.

## Scheduling

In `loci`, the spec is wrapped in a `DatasetUpdateConfig` and the ArcGIS taskflow chooses the `mode` (full vs. incremental) for each scheduled run, then calls `run_collection`.
