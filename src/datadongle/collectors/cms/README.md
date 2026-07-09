# CMS collector

Collection tooling for [data.cms.gov](https://data.cms.gov), the main CMS data portal — home of the Medicare fee-for-service payment datasets (inpatient payments by DRG, physician payments by HCPCS code, Part D prescribers, and so on).

This source covers **data.cms.gov only**. CMS runs two other portals on a different platform (DKAN): the Provider Data Catalog (`data.cms.gov/provider-data`, Care Compare data) and Open Payments (`openpaymentsdata.cms.gov`). Those are served by the `dkan` collector — see [`../dkan/README.md`](../dkan/README.md).

The tooling is four classes: `CMSDatasetSpec` (declare what to collect and where), `CMSClient` (talk to the catalog and data API), `CMSMetadata` (explore the catalog), and `CMSReader` — which adapts a dataset to datadongle's shared collection driver. Because a CMS dataset fans out over many published **vintages** into one table, CMS ships a thin **family driver**, [`run_cms_collection`](driver.py), rather than using the shared `run_collection` directly (see "Why a family driver" below).

## The publication model

data.cms.gov publishes a Project Open Data catalog at [`data.json`](https://data.cms.gov/data.json). Each dataset's `distribution` array contains **every published version** of the data in every available format: entries with `format: "API"` point at a versioned JSON API endpoint (each version has its own UUID, paged via `?size=N&offset=M`, max 5000 rows/page), and entries with `mediaType: "text/csv"` are direct CSV downloads.

Versions correspond to temporal coverage — typically calendar years, called **vintages** here — and each carries its own modified date. A single dataset therefore maps to many vintages that all land in one target table.

## 1. Explore

`CMSMetadata` browses the catalog without touching the warehouse:

```python
from datadongle.collectors.cms.client import CMSClient
from datadongle.collectors.cms.metadata import CMSMetadata

meta = CMSMetadata(CMSClient())
meta.titles("inpatient hospitals")                # matching dataset titles
meta.describe("Medicare Inpatient Hospitals - by Provider and Service", stats=True)
ds = meta.get_dataset("Medicare Inpatient Hospitals - by Provider and Service")
for v in meta.versions(ds):                        # one CMSDatasetVersion per vintage
    print(v.vintage, v.modified, v.api_uuid, v.csv_url)
```

`versions()` groups the distributions by temporal coverage into `CMSDatasetVersion` records (vintage label, modified date, API UUID, CSV URL), oldest first, excluding the undated "latest" duplicate. `columns(api_uuid)` samples a version's column names.

## 2. Write the spec

`CMSDatasetSpec` describes what to pull and where it lands:

```python
from datadongle.collectors.cms.spec import CMSDatasetSpec

spec = CMSDatasetSpec(
    name="medicare_inpatient_by_provider_and_service",
    dataset_title="Medicare Inpatient Hospitals - by Provider and Service",
    target_table="medicare_inpatient_by_provider_and_service",
    target_schema="raw_data",
    entity_key=["rndrng_prvdr_ccn", "drg_cd", "vintage"],   # normalized names; MUST include "vintage"
    retrieval="api",                                        # "api" or "csv"
    vintages=None,                                          # None = all published; or e.g. ["2022", "2023"]
)
```

- `entity_key` — the columns that uniquely identify a record for SCD2 versioning, using the **normalized** (lowercased) names. It **must include `"vintage"`**: the same provider × DRG appears in every year, so without `vintage` rows from different years would collide as "changed" versions of one entity and corrupt SCD2 history. The spec enforces this at construction time.
- `retrieval` — `"api"` pages rows out of the versioned JSON API (fine up to a few hundred thousand rows per vintage); `"csv"` downloads the vintage's CSV distribution and stream-parses it (use for the multimillion-row datasets).
- `vintages` — a filter over the catalog's published vintages; `None` means all of them.

## 3. Collect

`CMSReader` adapts the dataset to the family driver; `run_cms_collection` creates the table and lands every in-scope vintage.

```python
from datadongle.collectors.cms.reader import CMSReader
from datadongle.collectors.cms.driver import run_cms_collection
from datadongle.engines.postgres import PostgresEngine   # or engines.iceberg.IcebergEngine

reader = CMSReader()
engine = PostgresEngine(creds)                            # or IcebergEngine("/data/warehouse")

summary = run_cms_collection(reader, spec, engine)
```

The driver resolves the dataset's vintages from the catalog, discovers each vintage's columns, ensures the **union-of-vintages** table (every column any vintage has, all `TEXT`, plus a not-null `vintage`; the engine adds `ingested_at` and the SCD2 columns), then collects each vintage into its own write session. It returns a summary dict: `versions_processed`, `total_rows_staged`, `total_rows_merged`, `total_rows_invalidated`, and any per-vintage `errors`. A vintage that fails schema discovery or collection is reported and skipped; the others still land.

All ingestion is SCD2, so re-collecting is always safe — unchanged rows dedupe to a no-op merge, and a corrected re-release versions only the rows that actually changed.

### Retrieval modes are version-equivalent

`parse_csv` turns empty strings into `None` while the API serves empty strings, but the engine's content hash treats `NULL` and `''` identically, so switching a spec's `retrieval` mode never churns SCD2 versions.

### Column normalization

Source column names are lowercased, spaces become underscores, and any BOM is stripped, so columns are queryable without double quotes (`rndrng_prvdr_ccn`, not `"Rndrng_Prvdr_CCN"`) and the API and CSV paths land identically. Use the normalized names in `entity_key`. (This is simpler than the DKAN collector's rule — data.cms.gov column names contain no punctuation beyond underscores.)

## Why a family driver

The shared `run_collection` contract is one spec / one table / one write session. A CMS spec instead names one dataset whose many vintages all land in one table, with a schema that can drift across vintages and per-vintage error isolation — the same union-of-siblings shape as `tiger` and `dkan`. So CMS gets a thin driver built from the same primitives (the reader, the `Engine` protocol, the tracker), and the reader handles **one vintage at a time**: the driver narrows a multi-vintage spec to a single vintage before calling `schema`/`read`, and calls `reader.versions(spec)` to enumerate the fan-out.

Unlike the pre-refactor collector, the driver carries **no freshness-skip and no `_source_modified` advance**. That machinery avoided re-downloading unchanged vintages, but it was Postgres-only — it read the physical `valid_to` column and issued an in-place `UPDATE`, neither of which has an engine-neutral analogue (Iceberg stores SCD2 as an append-only satellite). Dropping it is what lets the same reader+spec run on both `PostgresEngine` and `IcebergEngine`, matching the `tiger`/`dkan`/`static` family drivers. The trade-off: **every run re-downloads each in-scope vintage** (unchanged content still dedupes to a no-op merge). For the multimillion-row CSV datasets that cost is real — scope a run with `vintages=[...]` when you don't need to revisit the whole history.

## Limitations

- Rows removed by a CMS correction remain current in the target. Per-vintage `invalidate_missing` isn't used — a single write session only ever holds one vintage, so it would close out every other vintage in the table.
- Schema discovery samples a vintage's **API** distribution; a vintage that has only a CSV distribution can't have its columns discovered and is reported as a per-vintage error.

## Tests

Tests live in `tests/collectors/cms/`. The HTTP boundary is faked (`FakeCMSClient`); `test_driver.py` runs end-to-end through `run_cms_collection` against a hermetic `IcebergEngine` (tmp-path warehouse) on every run, and additionally against Postgres when `DWH_TEST_PG*` is configured — both arms must agree.
