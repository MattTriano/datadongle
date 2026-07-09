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
