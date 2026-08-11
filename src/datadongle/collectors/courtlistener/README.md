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

## Bulk backfill + API increments

This is the collector's headline flow, and it needs no extra configuration — `backfill="bulk"` plus the shared driver:

```python
spec = CourtListenerDatasetSpec(
    name="courtlistener_clusters",
    target_table="courtlistener_clusters",
    resource="clusters",          # the API endpoint name
    backfill="bulk",              # a full read comes from the monthly export
)

run_collection(reader, spec, engine, mode="full")          # seed from bulk
run_collection(reader, spec, engine, mode="incremental")   # API deltas, forever after
```

**Name the resource once.** `resource` is the API endpoint; the bulk prefix is resolved through the rename registry in `resources.py`, so `clusters` finds the `opinion-clusters` export without you restating it. An explicit `bulk_file_prefix` still wins if you need to override, and for the majority of resources — where both names agree — there's nothing to set. Only the *name mapping* resolves automatically; `entity_key` stays explicit, because that's a modelling decision that belongs in the spec where you can see it.

**A scheduled job can just always run `mode="incremental"`.** The driver reads the high-water mark from the target table; on an empty or absent table that comes back `None`, which is a full read, which under `backfill="bulk"` is the bulk export. So the first run seeds itself from bulk and every subsequent run pages the API — one cron entry, no first-run special case.

To re-seed later from a newer monthly export, run `mode="full"` again. SCD2 makes that cheap: rows that haven't changed produce no new version.

## Incremental collection

The cursor is `date_modified` (every CourtListener table carries it) with an `id` tiebreak. An incremental run reads the target's max `(date_modified, id)`, asks the API for `date_modified__gte=<hwm>` ordered by `(date_modified, id)`, and keeps only strictly-later rows — ids are compared numerically where they are integers (courts uses slugs). Since increments return only rows that actually changed, formatting differences between the bulk seed and API updates never create spurious versions.

Caveats of the same-table design:

- Bulk columns the API does not return land as `NULL` in post-backfill versions of a changed row.
- Don't run `mode="full"` with `backfill="api"` over a bulk-seeded table — it would re-version rows on representation differences alone. Re-seed from a newer bulk file instead (SCD2 makes an unchanged re-seed a no-op).
- Resources whose API endpoint and bulk prefix differ need one override, e.g. `resource="clusters", bulk_file_prefix="opinion-clusters"` (`resources.py` records the ones this collector knows). Bulk-only tables (no API endpoint) work with `cursor_column=None` — every run is then a full bulk read.

## Entity keys: `["id"]` is right for most resources, not all

CourtListener's bulk exports are dumps of Django tables, so "what identifies a row" comes from the upstream schema. Three shapes:

| Shape | Examples | `entity_key` | `cursor_column` |
|---|---|---|---|
| **Entity table** | dockets, opinions, people, courts, financial disclosures | `["id"]` | `"date_modified"` |
| **Link table** (M2M through table) | citation map, opinion-cluster panels, `joined_by`, court `appeals_to` | its foreign-key pair | `None` |
| **Reference table** | races, sources | `["id"]` | `None` |

The spec's `["id"]` default is right for the entity tables that make up most of the catalog — including `courts`, whose `id` is a slug (`"scotus"`) rather than an integer.

**It is wrong for the link tables.** Those carry an `id` only because Django adds one to every through table; the row's identity is its foreign-key pair. Keying on the surrogate means an upstream rebuild renumbers every row, and SCD2 reads that as "every entity replaced" — a full spurious re-version, with the old entities never closed out. Link tables also have no timestamps, so they need `cursor_column=None` and are read in full every run. That is what makes `SCD2(invalidate_missing=True)` worth setting for them: it records when a citation edge or panel assignment *disappeared*.

Don't guess — ask, using the resource's actual columns:

```python
m.suggest_profile("dockets")
# dockets: entity_key=['id'], cursor_column='date_modified'
#   Entity table: 'id' is the upstream primary key.

m.suggest_profiles()      # every resource with a bulk export, as a DataFrame
```

`suggest_profile` derives the answer from a streamed header peek, so it is correct for resources this collector has never seen. Review the rationale, then splat it into a spec:

```python
spec = CourtListenerDatasetSpec(
    name="courtlistener_citation_map",
    target_table="courtlistener_citation_map",
    resource="citation-map",
    **m.suggest_profile("citation-map").spec_kwargs(),
)
```

`resources.py` holds a small registry for the cases column shape alone can't settle (the citation map has a `depth` payload, so it doesn't match the pure link-table signature) and where the API endpoint and bulk prefix differ (`clusters` → `opinion-clusters`). Everything else is derived.

**The reader validates before reading.** If `entity_key` names a column the resource doesn't have, `schema()` raises with the actual column list and a suggestion — SCD2 on a missing key would silently collapse the whole table into one entity. A missing *cursor* column is recoverable, so it warns and falls back to full reads instead.

## Discovering resources — `CourtListenerMetadata`

```python
from datadongle.collectors.courtlistener.metadata import CourtListenerMetadata

m = CourtListenerMetadata()
m.endpoints()                 # every API resource and its URL
m.search("docket")            # endpoints matching a substring
m.describe("dockets")         # OPTIONS metadata (name, description)
m.columns("dockets")          # field names/types (OPTIONS, else a sampled row)
m.bulk_exports("dockets")     # available bulk files with dates and sizes
m.bulk_datasets()             # every table published in bulk, one row each
m.coverage()                  # API endpoints vs bulk exports
m.bulk_columns("dockets")     # a bulk export's header (streamed peek, no download)
m.suggest_profile("dockets")  # recommended entity_key / cursor_column + rationale
m.suggest_profiles()          # the same for every resource, as a DataFrame
```

### Not every endpoint has a bulk export

The API root lists ~48 endpoints, but a good third of them are per-user state or RPC-style operations with no table behind them — `alerts`, `docket-alerts`, `tag`/`tags`, `api-usage`, `memberships`, `prayers`, `citation-lookup`, `search`, `visualizations`, and the `recap-*` family. Those are API-only by nature, not gaps in the bulk coverage.

**The two namespaces don't line up by name, in either direction.** `bulk_exports("clusters")` returns nothing because that table is published as `opinion-clusters`; `bulk_exports("agreements")` returns nothing because it's published under a `financial-disclosure-` qualifier. Going the other way, `describe("citation-map")` 404s because the API serves that table as `opinions-cited`. The bulk filter is a literal S3 key prefix, so an empty result means *"no file is named that"*, never *"this data isn't published in bulk."*

### Which name goes where

| You have | Use it with | Notes |
|---|---|---|
| API endpoint (`endpoints()`) | `describe`, `columns` | Hits the REST API |
| Bulk prefix (`bulk_datasets()`) | `bulk_exports`, `bulk_columns` | Hits the S3 bucket |
| Either | `suggest_profile`, `describe`, and a spec's `resource` | Resolved through `resources.py` |

A spec's `resource` accepts **either** name — `spec.endpoint` and `spec.file_prefix` each resolve through the registry independently, so `resource="citation-map"` reads the `citation-map` bulk file and pages the `opinions-cited` endpoint. Name it once. Explicit `api_endpoint` / `bulk_file_prefix` still override.

When a name has no endpoint behind it, `describe` raises with the nearest matches rather than a bare 404 — plenty of bulk tables genuinely have no API endpoint, and that's a fact about the source, not a typo.

`coverage()` reconciles the two and suggests likely renames:

```python
m.coverage()
#   name         api   bulk   bulk_prefix        candidates
#   agreements   True  False  None               ['financial-disclosure-agreements']
#   alerts       True  False  None               []
#   citation-map False True   citation-map       []
#   clusters     True  True   opinion-clusters   []
#   dockets      True  True   dockets            []
```

A non-empty `candidates` is a hint to check with `bulk_columns`, not a conclusion. Once confirmed, record the mapping as `bulk_file_prefix` on the spec (or add it to `resources.py` so it's version-controlled). Rows with `api=False` are bulk-only — the through tables — and need `backfill="bulk"` with `cursor_column=None`.

## Developed offline — facts to verify against the live source

This collector was written without network access. The hermetic tests pin the *intended* behavior; `tests/collectors/courtlistener/test_live.py` (marked `@pytest.mark.network`, deselected by default) checks the assumptions below against the real service — run it once from a machine with egress:

```
uv run --no-sync pytest tests/collectors/courtlistener/test_live.py -m network
```

1. ~~**Bulk CSV quoting**~~ — **verified, and the docs were wrong.** CourtListener's bulk-data docs describe a backtick quotechar; the live `courts` export is standard double-quote CSV (unquoted header, force-quoted data rows). `BULK_CSV_QUOTECHAR` is `"`. Because a quotechar mismatch corrupts every text column *silently* — only the timestamp columns are validated — `check_bulk_quoting` inspects the first parsed row and raises if values arrive still wrapped.
2. ~~**Bulk bucket listing**~~ — **verified.** Exports are listed from `https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/?list-type=2&prefix=bulk-data/…`. The bucket is public, and the client uses a separate credential-free `bulk_session` for it — S3 rejects a `Token …` Authorization header with a 400 that echoes the token back in the error body, so the API session must never be used for bulk requests.
3. **Ordering**: incremental reads request `order_by=date_modified,id`; confirm v4 cursor pagination accepts this compound ordering (if not, drop to `order_by=date_modified` — client-side strictly-after filtering still guards correctness, at the cost of tie-stability across pages).
4. **`page_size`**: the client sends it only when set; confirm the server's cap before tuning it.
