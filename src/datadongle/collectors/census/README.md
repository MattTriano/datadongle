# Census collector

The U.S. Census Bureau API (`api.census.gov/data`) serves tabular estimates from the American Community Survey, the Decennial Census, and dozens of other programs, keyed by *dataset* (`acs/acs5`), *vintage* (year), *variable* (`B24010_001E`), and *geography* (`tract`, `county`, …). This collector pulls a chosen set of variables/groups, across a set of vintages and states, into one `raw_data` table.

Reach for this collector when the source is the Census API — you'll be picking variables out of a Census data product rather than downloading a file.

The tooling is four classes plus a family driver: `CensusMetadata` (explore datasets/groups/variables/geographies), `CensusDatasetSpec` (declare what to collect and where), `CensusClient` (talk to the API), `CensusReader` (adapt one `(vintage, state)` to datadongle's engines), and `run_census_collection` (the family driver that fans the spec out over `vintages × states` and lands them in one union-of-vintages table on any engine — `PostgresEngine`, `IcebergEngine`).

## Why a family driver

A Census spec is not one dataset — it's a *matrix*. `vintages=[2019, 2020, 2021]` × `state_fips=[all 52]` is 156 API pulls that all belong in one table, and a variable group can gain or lose a variable between vintages. That doesn't fit the shared `run_collection` contract (one spec / one table / one write session), so Census ships `run_census_collection`, which:

- **discovers each vintage's schema and unions them**, so one table holds every vintage's columns (a variable only present in later years lands as `NULL` for earlier ones);
- **stages each `(vintage, state)` in its own write session**, the same granularity the pipeline reports at;
- **isolates failures per `(vintage, state)`** — a pull that errors (a transient API failure the client's retries couldn't ride out, or a vintage missing a group) is reported and skipped, and the rest still land.

## 1. Explore the source

`CensusMetadata` walks the catalog without touching the warehouse, so you can find the values that go into a spec:

```python
from datadongle.collectors.census.metadata import CensusMetadata

cm = CensusMetadata(api_key="YOUR_KEY")   # a key is optional for low-volume metadata

cm.search("occupation")                   # datasets matching a keyword (alias for list_datasets)
cm.list_vintages("acs/acs5")              # [2009, ..., 2022]
cm.list_groups("acs/acs5", 2022, "occupation")   # tables (groups) in a dataset+year
cm.list_variables("acs/acs5", 2022, group="B24010")  # variables in a group
cm.list_geographies("acs/acs5", 2022)     # geography levels + their FIPS requirements
```

`search`/`list_variables`/… are the discovery path that produces the `dataset`, `vintages`, `groups`, `variables`, and `geography_level` you write into a spec. (`CensusVariableMapper` can additionally compress cryptic variable labels into readable column names for downstream views — it doesn't affect what this collector stores, which is the raw variable codes.)

## 2. Write the spec

`CensusDatasetSpec` describes what to pull and where it lands:

```python
from datadongle.collectors.census.spec import CensusDatasetSpec

OCCUPATION_BY_SEX_SPEC = CensusDatasetSpec(
    name="occupation_by_sex",
    dataset="acs/acs5",
    vintages=[2019, 2020, 2021, 2022],
    groups=["B24010"],              # every estimate/MOE variable in the group
    variables=["B01001_001E"],      # plus any individual variables
    geography_level="tract",        # state | county | tract | block group | place | zip code tabulation area
    target_table="occupation_by_sex_tract",
    target_schema="raw_data",
    state_fips=["17", "18"],        # omit for all 50 states + DC + PR
)
```

Field notes:

- `groups` / `variables` — at least one is required. Groups are resolved per vintage to their estimate (`…E`) and margin-of-error (`…M`) variables; annotations (`…EA`/`…MA`) and percentages (`…PE`/`…PM`) are filtered out. Both land as their raw Census codes, typed `NUMERIC`.
- `geography_level` — determines the geo-id columns and, with `vintage`, the **entity key** (`state, county, tract, vintage` for tracts; ZCTA has no `state`). This is derived automatically — don't set `entity_key` yourself.
- `state_fips` — the collection is fanned out one state at a time to keep each pull small; `None` means all states.

A spec carries no credentials; the API key goes on the reader.

## 3. Collect

`CensusReader` + `run_census_collection` land the matrix on any engine. The engine creates the union table for you — no manual DDL.

```python
import os
from datadongle.collectors.census.reader import CensusReader
from datadongle.collectors.census.driver import run_census_collection
from datadongle.engines.iceberg import IcebergEngine   # or engines.postgres.PostgresEngine

reader = CensusReader(api_key=os.environ["CENSUS_API_KEY"])
engine = IcebergEngine("/data/warehouse")              # or PostgresEngine(creds)

summary = run_census_collection(reader, OCCUPATION_BY_SEX_SPEC, engine)
# {"spec_name", "vintages_processed", "states_processed",
#  "total_rows_staged", "total_rows_merged", "total_rows_invalidated", "errors"}
```

The table gets one column per geo id, `vintage`, `NAME`, one column per resolved variable, an `ingested_at` column, and — because the spec always derives an `entity_key` — the engine's SCD2 columns (physical `valid_from`/`valid_to` on Postgres, an append-only satellite on Iceberg) plus the uniqueness / current-version indexes.

### Modes and re-runs

There is no `mode="incremental"` here: a published vintage is **immutable** and the API has no row cursor, so every collection is a full read. Because writes are **SCD2**, re-collecting an unchanged vintage is a no-op merge (`total_rows_merged == 0`) — safe, but it *does* re-hit the API. The collector deliberately keeps no "already ingested" skip (it couldn't be expressed portably across both engines), so **avoid re-running vintages you've already collected**: pass only the new years in `vintages`. If a run half-completes, just re-run it — SCD2 makes it converge without duplicates.

> If you set `entity_key=None` (not derivable from a normal spec, but possible via `dataclasses.replace`), writes become append-only and every re-run duplicates rows. Only run keyless specs once.

## Gotchas

- **Values arrive as strings.** The API returns estimates as strings (including sentinels like `-666666666` for suppressed values); they cast into `NUMERIC` columns on write. They are stored as-is, not cleaned.
- **Spaced geo names are normalized.** `block group` / `zip code tabulation area` come back with spaces in the id column; the reader renames them to the underscore names the schema and entity key use.
- **`ensure_table` won't alter an existing table.** If a later vintage introduces a variable after the table exists, evolve the table before collecting it (or collect all vintages together the first time, so the union table is created with every column).

The `Append` / `Upsert` / `SCD2` write-mode behaviors and the collector interface as a whole are documented in [`docs/spec/collector-interface.md`](../../../../docs/spec/collector-interface.md).
