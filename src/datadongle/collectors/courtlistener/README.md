# CourtListener collector

Collects US federal and state court data (dockets, opinions, judges, courts, oral arguments, …) from [CourtListener](https://www.courtlistener.com/) — the Free Law Project's database — into a datadongle target table.

CourtListener exposes the same database two ways, and this collector uses both:

- the **REST API v4** (`https://www.courtlistener.com/api/rest/v4/`): per-resource endpoints with Django-style filters, cursor pagination, and an optional token (`Authorization: Token …`). Rate-limited (~5,000 queries/hour authenticated), so it is the wrong tool for backfilling millions of rows.
- the **monthly bulk data exports** (public S3 bucket, `<table>-<YYYY-MM-DD>.csv.bz2`): bzip2-compressed CSV dumps of whole tables. The right tool for backfills — dockets alone is tens of GB — but only a monthly snapshot.

The collector composes them: a **full read uses the spec's `backfill` path** (bulk file or API walk) and an **incremental read always pages the API** filtered by `date_modified`, so the standard pattern for a big resource is *bulk seed once, cheap API increments forever*. Both paths are normalized to the same column shape and land in one target table via the shared `run_collection` (no family driver).

## Quickstart

An API token is optional but recommended (anonymous requests are throttled harder): create one at courtlistener.com and set `COURTLISTENER_API_TOKEN` or pass `api_token=` to the reader. Bulk downloads need no auth.

```python
from datadongle.collectors.courtlistener.spec import CourtListenerDatasetSpec
from datadongle.collectors.courtlistener.reader import CourtListenerReader
from datadongle.engines.iceberg import IcebergEngine
from datadongle.load.driver import run_collection

spec = CourtListenerDatasetSpec(
    name="courtlistener_dockets",
    target_table="courtlistener_dockets",
    resource="dockets",
    backfill="bulk",                 # full read = latest monthly bulk export
)                                    # entity_key defaults to ["id"] ⇒ SCD2

reader = CourtListenerReader()       # reads COURTLISTENER_API_TOKEN if set
engine = IcebergEngine("/data/warehouse")   # or PostgresEngine(creds)

run_collection(reader, spec, engine, mode="full")          # bulk backfill
run_collection(reader, spec, engine, mode="incremental")   # API: changed rows only
```

For a small resource, `backfill="api"` skips the bulk file entirely and is fresher than the monthly snapshot:

```python
spec = CourtListenerDatasetSpec(
    name="courtlistener_courts",
    target_table="courtlistener_courts",
    resource="courts",
    backfill="api",
    filters={"jurisdiction": "F"},   # server-side Django-lookup filters (API-only)
)
```

`filters` are rejected with `backfill="bulk"`: a bulk file is always the whole table, so filtered increments over an unfiltered seed would produce an incoherent table.

## What lands in the table

**The bulk CSV column set is the canonical schema** (with `backfill="bulk"`; with `backfill="api"`, columns come from one transformed sample row). All columns are `TEXT` — bulk CSVs are untyped strings and both retrieval paths must produce comparable SCD2 content — so casting is a downstream concern. API rows are normalized toward the bulk shape:

- **Related-object hyperlinks collapse to keys**: `"court": "https://…/api/rest/v4/courts/scotus/"` becomes `court_id = "scotus"` (skipped when the API already sends the `*_id` sibling field).
- **Computed fields** (`absolute_url`, `resource_uri`) are dropped; **nested lists/objects** are JSON-encoded; **booleans** land as `"true"`/`"false"`.
- **Timestamps are canonicalized**: `date_created`/`date_modified` become `YYYY-MM-DD HH:MM:SS.ffffff+00:00` in both paths, so the text-typed high-water mark orders correctly across bulk- and API-sourced rows. Both are flagged bookkeeping (`metadata=True`), so a re-pull that only bumps `date_modified` creates no spurious SCD2 version.

## Incremental collection

The cursor is `date_modified` (every CourtListener table carries it) with an `id` tiebreak. An incremental run reads the target's max `(date_modified, id)`, asks the API for `date_modified__gte=<hwm>` ordered by `(date_modified, id)`, and keeps only strictly-later rows — ids are compared numerically where they are integers (courts uses slugs). Since increments return only rows that actually changed, formatting differences between the bulk seed and API updates never create spurious versions.

Caveats of the same-table design:

- Bulk columns the API does not return land as `NULL` in post-backfill versions of a changed row.
- Don't run `mode="full"` with `backfill="api"` over a bulk-seeded table — it would re-version rows on representation differences alone. Re-seed from a newer bulk file instead (SCD2 makes an unchanged re-seed a no-op).
- Resources whose API endpoint and bulk prefix differ need one override, e.g. `resource="clusters", bulk_file_prefix="opinion-clusters"`. Bulk-only tables (no API endpoint) work with `cursor_column=None` — every run is then a full bulk read.

## Discovering resources — `CourtListenerMetadata`

```python
from datadongle.collectors.courtlistener.metadata import CourtListenerMetadata

m = CourtListenerMetadata()
m.endpoints()                 # every API resource and its URL
m.search("docket")            # endpoints matching a substring
m.describe("dockets")         # OPTIONS metadata (name, description)
m.columns("dockets")          # field names/types (OPTIONS, else a sampled row)
m.bulk_exports("dockets")     # available bulk files with dates and sizes
m.bulk_columns("dockets")     # a bulk export's header (streamed peek, no download)
```

## Developed offline — facts to verify against the live source

This collector was written without network access. The hermetic tests pin the *intended* behavior; `tests/collectors/courtlistener/test_live.py` (marked `@pytest.mark.network`, deselected by default) checks the assumptions below against the real service — run it once from a machine with egress:

```
uv run --no-sync pytest tests/collectors/courtlistener/test_live.py -m network
```

1. **Bulk CSV quoting**: bulk files are parsed with a backtick quotechar (`BULK_CSV_QUOTECHAR` in `client.py`), per CourtListener's bulk-data docs.
2. **Bulk bucket listing**: exports are listed from `https://com-courtlistener.s3-us-west-2.amazonaws.com/?list-type=2&prefix=bulk-data/…`.
3. **Ordering**: incremental reads request `order_by=date_modified,id`; confirm v4 cursor pagination accepts this compound ordering (if not, drop to `order_by=date_modified` — client-side strictly-after filtering still guards correctness, at the cost of tie-stability across pages).
4. **`page_size`**: the client sends it only when set; confirm the server's cap before tuning it.
