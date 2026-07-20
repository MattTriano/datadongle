# EIA collector

Collects energy data from the [US EIA Open Data API v2](https://www.eia.gov/opendata/) into a datadongle target table.

The EIA API is a uniform tree of **routes** (`electricity/retail-sales`, `natural-gas/pri/fut`, `petroleum/pri/gnd`, …). A route is a self-contained data series: **frequency** is a query parameter (monthly/quarterly/annual/daily/hourly) and **geography/sector** are *facets*, both scoped within the one route. So a single `(route, frequency, facet-filter)` is one grain and lands in one target table — one `EIAReader` serves every dataset via the shared `run_collection` (no family driver).

## Quickstart

You need a free API key ([register here](https://www.eia.gov/opendata/register.php)); set `EIA_API_KEY` or pass `api_key=` to the reader.

```python
from datadongle.collectors.eia.spec import EIADatasetSpec
from datadongle.collectors.eia.reader import EIAReader
from datadongle.engines.iceberg import IcebergEngine
from datadongle.load.driver import run_collection

spec = EIADatasetSpec(
    name="colorado_residential_electricity_prices",
    target_table="eia_electricity_retail_sales",
    route_path="electricity/retail-sales",
    frequency="monthly",
    data_columns=["price", "revenue", "sales", "customers"],   # EIA data[] measures
    facets={"stateid": ["CO"], "sectorid": ["RES"]},           # optional server-side filter
    entity_key=["stateid", "sectorid", "period"],              # non-empty ⇒ SCD2 history
)

reader = EIAReader()                       # reads EIA_API_KEY from the environment
engine = IcebergEngine("/data/warehouse")  # or PostgresEngine(creds)

run_collection(reader, spec, engine, mode="full")          # first load
run_collection(reader, spec, engine, mode="incremental")   # later runs: new periods only
```

The same spec+reader runs unchanged against `PostgresEngine`.

## What lands in the table

Columns are discovered by sampling the first data row, so the table mirrors what EIA returns: `period`, the facet id columns (`stateid`, `sectorid`), their description columns (`stateDescription` → `statedescription`), the requested measures, and their units columns (`price-units` → `price_units`).

- **Measures are `DOUBLE`.** EIA delivers every value as a JSON string; the reader casts them to float. A `null`/empty value becomes `NULL`; a value that is neither numeric nor null (e.g. a withheld/suppressed marker) **raises** rather than being silently nulled — so no signal is lost. If a series turns out to use such markers, keep that column as text instead.
- **Names are normalized** to lowercase with non-alphanumerics collapsed to `_`, so every column queries without double-quotes.

## Incremental collection and revisions

The incremental cursor is `period`. An `incremental` run reads the target's max `period`, asks EIA for data from there (`start=`), and keeps only strictly-later rows — so re-runs pull just new periods.

EIA **revises** historical values, and rows carry no per-row "updated" timestamp, so the `period` cursor cannot see a changed value in an already-collected period. Schedule a periodic `mode="full"` refresh to pick up revisions: under SCD2 an unchanged re-pull is a no-op, and a revised value appends exactly one new version for the affected entity.

## Discovering datasets — `EIAMetadata`

The API has no flat catalog or search endpoint; datasets live in a tree of routes. `EIAMetadata` walks that tree so you can find a series and read off the values a spec needs:

```python
from datadongle.collectors.eia.metadata import EIAMetadata

m = EIAMetadata()                          # reads EIA_API_KEY from the environment

m.browse()                                 # top categories (electricity, natural-gas, …)
m.browse("electricity")                    # child routes, each with a full `path`
m.describe("electricity/retail-sales")     # a leaf's full metadata
m.frequencies("electricity/retail-sales")  # → a spec's `frequency`
m.columns("electricity/retail-sales")      # measure columns → `data_columns` (with units)
m.facets("electricity/retail-sales")       # filterable dimensions → keys of `facets`
m.facet_values("electricity/retail-sales", "stateid")   # → values of `facets`
m.search("retail sales", max_depth=1)      # walk the tree for matching routes
```

`browse`/`columns`/`facets`/`search` return DataFrames for notebook display. `search` has no server-side support — it issues one request per internal node visited, so keep `max_depth` small.

## Building a spec — `EIASpecBuilder`

Most spec fields are just a route's own metadata copied by hand — and the SCD2 `entity_key` is especially error-prone because its names must match the reader's *normalized* output columns. `EIASpecBuilder` reads a route's metadata once, then **fills** whatever you omit and **validates** whatever you pass, so a typo fails at build time (with the valid options listed) instead of deep in a collection run:

```python
from datadongle.collectors.eia.builder import EIASpecBuilder

b = EIASpecBuilder()                       # reads EIA_API_KEY from the environment

spec = b.build(
    "electricity/retail-sales",
    facets={"stateid": ["CO"], "sectorid": ["RES"]},   # validated against the route
)
# Filled in from the route's metadata:
#   frequency     → the route's sole frequency (raises, listing them, if several)
#   data_columns  → every measure the route exposes
#   entity_key    → the route's facet-id columns + "period"  (⇒ SCD2 history)
#   name/target_table → eia_electricity_retail_sales_monthly
```

What the builder does with each argument:

- **`frequency`** — omit it to use the route's only frequency; passing an unoffered one (or omitting it when several exist) raises with the valid list.
- **`data_columns`** — omit to pull every measure; an unknown measure raises.
- **`facets`** — an unknown facet *key* raises. Pass `check_facet_values=True` to also validate each *value* (one extra request per facet).
- **`entity_key`** — omit to derive the SCD2 grain (facet-id columns + `period`); pass `entity_key=None` to opt out of history (Append); pass a list to set it yourself.
- **`name`/`target_table`/`target_schema`/`start`/`end`** — optional overrides; the name/table default to `eia_<route>_<frequency>`.

For an interactive starting point in a notebook, `print(b.template("electricity/retail-sales"))` returns a fully-populated, editable `EIADatasetSpec(...)` snippet — every measure (with units), the frequencies, the facet keys, and the period range laid out as comments to copy and trim.
