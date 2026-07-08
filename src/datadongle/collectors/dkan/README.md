# DKAN Collector

Collection tooling for [DKAN](https://getdkan.org/) data portals. One reader serves every DKAN portal; the portal is identified by `base_url` on the spec, and the reader caches one client per portal.

Known CMS portals running DKAN:

| Portal | base_url | Contents |
|---|---|---|
| Provider Data Catalog | `https://data.cms.gov/provider-data` | Care Compare data: hospital/nursing-home/etc. quality and facility info |
| Open Payments | `https://openpaymentsdata.cms.gov` | Industry payments to physicians and teaching hospitals |

Note: despite the name, DKAN is **not** API-compatible with CKAN (it's a Drupal-based platform inspired by CKAN). The CKAN collector won't talk to these portals and vice versa.

## Classes

* `DKANClient` (`client.py`) — thin HTTP client for one portal: cached metastore catalog fetch, datastore query paging (capped at 500 rows/page by DKAN), datastore row counts, and retrying streamed file downloads.
* `DKANMetadata` (`metadata.py`) — catalog exploration: title search, dataset lookup, distribution listing, column sampling, and a `describe()` summary for notebook use.
* `DKANDatasetSpec` (`spec.py`) — declares what to collect and where it goes. Subclass of `DatasetSpec`.
* `DKANReader` (`reader.py`) — the `SourceReader` adapter for **one** dataset: schema discovery, retrieval, DKAN normalization, provenance stamping.
* `run_dkan_collection` (`driver.py`) — the family driver that loops a spec's datasets into one table.

## Quick start

```python
from datadongle.collectors.dkan.client import DKANClient
from datadongle.collectors.dkan.metadata import DKANMetadata
from datadongle.collectors.dkan.reader import DKANReader
from datadongle.collectors.dkan.driver import run_dkan_collection
from datadongle.collectors.dkan.spec import DKANDatasetSpec
from datadongle.engines.postgres import PostgresEngine   # or engines.iceberg.IcebergEngine

# Explore
meta = DKANMetadata(DKANClient("https://data.cms.gov/provider-data"))
meta.titles("hospital")
meta.describe("Hospital General Information", stats=True)

# Specify
spec = DKANDatasetSpec(
    name="pdc_hospital_general_information",
    base_url="https://data.cms.gov/provider-data",
    dataset_identifiers=["xubh-q36u"],
    target_table="pdc_hospital_general_information",
    entity_key=["facility_id"],
    retrieval="datastore",
)

# Collect — the engine creates the table for you; no manual DDL step.
reader = DKANReader()
engine = PostgresEngine(creds)                 # or IcebergEngine("/data/warehouse")
summary = run_dkan_collection(reader, spec, engine)
```

Unlike a single-source reader (Socrata, OSM, CKAN), DKAN isn't driven through the shared `run_collection`: a spec is a *family* of datasets landing in one table, which the shared one-spec/one-table driver can't express. `run_dkan_collection` is a thin family driver over the same engine + reader primitives.

## Update semantics

The unit of work is one metastore **dataset**. DKAN datasets are refreshed in place — one identifier, a dataset-level `modified` date, no version array — and collection is **full-refresh-only**: there's no row cursor, so every run re-reads the dataset. All ingestion is SCD2, so this is safe and cheap in the steady state — an unchanged re-pull dedupes to zero merged rows and creates no new versions.

Every row is stamped with two hash-excluded provenance columns: `_source_dataset` (which dataset it came from — the grouping key across a family) and `_source_modified` (the dataset's `modified` date at collection time). Because they're hash-excluded, a re-publication that only bumps the modified date doesn't churn SCD2 history.

> **Migration note.** The previous `DKANCollector` had a *freshness-skip* — it read the target's row count and max `_source_modified` and skipped a dataset it judged already-current, and advanced `_source_modified` afterward so identical re-publications resettled. That was dropped in the reader migration: it saved a re-download only on the `file` path, was a footgun (silent "did nothing" runs, a heuristic that could misfire), and its post-merge UPDATE had no Shape-B/Iceberg analogue. Correctness never depended on it — SCD2 idempotency does the same job. If re-downloading a multi-GB `file` dataset every run proves costly in practice, a skip can be reintroduced behind this same driver.

## Multi-dataset families

A spec may carry several `dataset_identifiers` landing in one target table — e.g. Open Payments publishes one dataset per program year. Each sibling is collected independently, in its own write session, and **a failure in one (including a failure to sample its schema) doesn't block the others** — it's reported in `summary["errors"]`.

The target table is created once with the **union** of every sibling's columns, so a sibling that adds or drops a column relative to the others still gets a home for all of its data (a column a given sibling lacks is stored NULL for that sibling's rows).

**The entity key must distinguish entities across siblings** (e.g. `["record_id", "program_year"]` for Open Payments). If it doesn't, rows from sibling datasets collide as "changed" versions of one entity and silently corrupt SCD2 history.

Note that Open Payments republishes *every* program year each January (corrections and dispute resolutions apply retroactively), so expect an annual recollection of every year in the spec.

## Retrieval modes

* `retrieval="datastore"` — pages rows out of the datastore API at 500 rows/page. Fine up to a few hundred thousand rows per dataset.
* `retrieval="file"` — downloads the dataset's distribution file (assumed CSV) to a tempfile and stream-parses it. Use for the multimillion-row datasets (a single Open Payments year is ~11M rows / ~6 GB).

The two modes are hash-equivalent: switching a spec's retrieval mode never churns SCD2 versions (see normalization below; empty strings vs NULLs also hash identically). Schema discovery always samples the datastore (one row), regardless of the retrieval mode.

## Column normalization

DKAN's datastore serves normalized column names while distribution files keep the original headers, so file-mode headers are normalized with **DKAN's own rule** to keep the modes consistent: lowercase, whitespace → underscore, all other punctuation **dropped** (so `County/Parish` → `countyparish`, not `county_parish` — this deliberately differs from the CKAN collector's normalizer).

Names are also truncated to Postgres's 63-character identifier limit with `_2`/`_3` collision suffixing. This is load-bearing: Open Payments has a 64-character column that would otherwise be silently truncated by Postgres at DDL time and then dropped at ingest time.

Use the normalized names in `entity_key`.

## invalidate_missing

For single-dataset refresh-in-place specs, `invalidate_missing=True` closes out current rows whose entity is absent from the fresh pull — the correct semantics when disappearance means delisting (e.g. a hospital leaving Care Compare). On Postgres this sets `valid_to`; on Iceberg it appends a tombstone version. The default is `False`, consistent with the other collectors, in which case removed rows simply remain current.

The spec **forbids** the flag for multi-dataset families: each sibling is staged on its own, so invalidation would close out every other sibling's rows.

## Schema and DDL

There's no manual DDL step — `run_dkan_collection` calls `engine.ensure_table(...)`, deriving the table from a one-row datastore sample per dataset (unioned across a family). All source columns are `text` (the APIs serve strings; casting is a downstream concern), plus `_source_dataset`/`_source_modified` and the engine's SCD2 columns and indexes. `ensure_table` is create-if-not-exists — it won't evolve an existing table, so if a family gains a new column after the table exists, evolve the table before collecting. Keep `target_table` under ~48 characters so the generated constraint/index names stay within Postgres's identifier limit.

## Limitations

* File retrieval assumes the first distribution (`index 0`) is a CSV. No other formats or distribution selection yet.
* Datastore type metadata and data dictionaries are ignored; all columns are `text`.
* Schema discovery requires datastore-backed datasets (it samples a row). All CMS DKAN datasets qualify.
* Open Payments' `change_type` column participates in the record hash, so a record flipping e.g. `UNCHANGED` → `CHANGED` between publications creates a new SCD2 version even if payment fields are identical. That's arguably signal; if it churns too much history, mark it as a metadata column so it's hash-excluded.

## Tests

Behavior tests live in `tests/collectors/dkan/`. The HTTP boundary is faked (`FakeDKANClient`); the end-to-end driver tests run against a hermetic Iceberg warehouse (green everywhere) and, when `DWH_TEST_PG*` is configured, also against Postgres — both engines must agree.

```console
uv run --no-sync pytest tests/collectors/dkan -v
```
