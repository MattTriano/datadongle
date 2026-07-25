# Add a CourtListener collector (bulk backfill + incremental API, one table)

## Motivation

I want US court data — dockets, opinions, opinion clusters, judges, courts, oral arguments — collectable through the same interface as every other datadongle source. CourtListener (the Free Law Project's database) is the best open corpus of this data, and it is my first legal-domain source, so it also serves as the template for how that domain gets collected going forward.

CourtListener is worth building against the full collector interface in [`docs/spec/collector-interface.md`](../spec/collector-interface.md) rather than a one-off script because it combines two access paths with sharply different economics, and composing them well is exactly what the SourceReader/Engine split is for:

- The **REST API v4** (`https://www.courtlistener.com/api/rest/v4/`) has per-resource endpoints, Django-lookup filters, cursor pagination, and per-row `date_modified` timestamps — ideal for incremental pulls, but rate-limited (~5,000 queries/hour authenticated). Backfilling tens of millions of dockets through it is infeasible: at best a few hundred rows per request, it would take months.
- The **monthly bulk data exports** (a public S3 bucket of `<table>-<YYYY-MM-DD>.csv.bz2` Postgres dumps) deliver whole tables cheaply — dockets alone is tens of GB — but only as a monthly snapshot with no incremental story.

The natural pattern is therefore *bulk file for the initial backfill, then incremental API pulls by `date_modified`*, landing in one target table. No existing collector composes two retrieval paths across the full/incremental boundary like this, so the design decisions here (documented below) are precedent-setting for future sources with the same shape (e.g. other "bulk snapshot + change API" providers).

## Problem: what makes this non-trivial

The two paths expose the *same rows in different shapes*, and SCD2 only works if both paths produce comparable content for the same entity:

- Bulk CSVs are raw database dumps: foreign keys as `court_id`, no computed fields, everything a string, Postgres timestamp rendering (`2024-05-06 12:34:56.789+00`), and a **backtick quote character** (the data is full of double-quotes).
- API rows are DRF JSON: related objects as hyperlinks (`"court": "https://…/api/rest/v4/courts/scotus/"`), computed fields (`absolute_url`, `resource_uri`), nested lists/objects (`parties`), typed scalars, and ISO-T/Z timestamps (`2024-05-06T12:34:56.789000Z`).

Beyond shape alignment, the incremental cursor has two sharp edges: `date_modified` lands in a TEXT column, so the high-water mark read back from the target is a *lexicographic* max — it is only correct if both paths write timestamps in one canonical, sortable format; and the `id` tiebreak is numeric for most tables (string comparison would order `"100" < "99"`) but a slug for courts.

Finally, this collector was developed **without network access**, so every source-specific fact (quote character, bucket URL, ordering parameters) is an assumption from documentation knowledge rather than a verified behavior — the design has to make those assumptions explicit and cheaply verifiable.

## Goals and non-goals

**Goals**

- A `CourtListenerReader` implementing the `SourceReader` protocol, driven by the shared `run_collection` (no family driver), running unchanged on `PostgresEngine` and `IcebergEngine`.
- One spec, one table: a full read seeds from the spec's `backfill` path (bulk file or API walk); every incremental read pages the API by `date_modified` — into the same table, with rows from both paths normalized to one column shape.
- Resource-generic from day one: `resource="dockets"` (or any endpoint/bulk table) works without per-resource curation; schema is discovered from the source.
- Streaming everywhere: bulk files download to a temp file and parse in batches through on-the-fly bz2 decompression; schema discovery peeks only the CSV header (never downloads a multi-GB file for DDL); API reads are page-by-page.
- A `CourtListenerMetadata` with the `search`/`describe`/`columns` core plus bulk-export discovery, and a `README.md` documenting quirks and caveats.
- Live smoke tests (`@pytest.mark.network`, deselected by default) that pin every offline assumption to a named test, runnable once from a machine with egress.

**Non-goals**

- Typed columns. Everything lands TEXT (see the decision below); casting is a downstream concern.
- Per-resource curated schemas or column mappings — deliberately traded away (see below).
- The `search/` endpoint, RECAP fetch, or webhook-based change feeds — the polling cursor is sufficient for now.
- Collecting the m2m join tables the API embeds as lists (e.g. a docket's `panel`); with a bulk-derived schema these are projected away, and collecting them properly means collecting the join table as its own resource later.

## Design decisions

### How to model two retrieval paths in one spec

Options I considered:

1. **CMS-style `retrieval="api"|"bulk"` field** — a dataset uses one mode for its lifetime. Simplest (it is exactly what the CMS collector does for its API-vs-CSV distributions), but it forces the headline use case into *two specs pointed at one table* (a bulk spec for seeding, an API spec for updates), which is error-prone and makes neither spec self-describing.
2. **Couple retrieval to the driver's mode** — `mode="full"` ⇒ bulk, `mode="incremental"` ⇒ API. No new spec field, but it silently forbids full API reads (which are the *right* full read for small resources like courts, being fresher than the monthly snapshot) and hides a large download behind an innocuous-looking flag.
3. **A `backfill="bulk"|"api"` spec field** — declares how a **full read** (`since=None`) is performed; incremental reads always use the API, because a bulk snapshot has no incremental story by construction. One spec fully describes the dataset's lifecycle, small resources can opt into API-only, and the field name says what it controls.

I chose (3). `backfill="bulk"` is the default since the bulk-seed flow is the reason this collector exists; `backfill="api"` covers small resources and endpoints with no bulk export. A pleasant consequence of the driver's contract: the *first* `mode="incremental"` run against an empty table has no high-water mark, reads with `since=None`, and therefore performs the bulk backfill automatically — seed and steady-state can be the same scheduled command.

`filters` (Django-lookup params, e.g. `{"court": "scotus"}`) are **rejected when `backfill="bulk"`**: a bulk file is always the whole table, so filtered increments over an unfiltered seed would produce a table that is neither a full mirror nor a filtered subset. Fail loudly at spec construction, per the interface.

### Resource-generic with discovered schemas, not curated per-resource schemas

Curated column maps per resource would give typed columns and vetted alignment, but each resource would need its schema written and verified against a live source I could not reach, and every new resource would need collector code changes. Instead the schema is discovered at collection time and **the bulk CSV header is the canonical column set** (with `backfill="bulk"`): it is the raw table schema, cheap to fetch (a streamed peek that bz2-decompresses only to the first newline), and by definition matches what the backfill will deliver. With `backfill="api"`, columns come from one transformed sample row. Any CourtListener resource — or a renamed/added column upstream — works without touching the collector.

### API rows are normalized toward the bulk shape

Alignment transforms, applied to every API row:

- Detail-URL values collapse to their trailing key and the field is renamed `<field>_id` (`"court": "…/courts/scotus/"` → `court_id="scotus"`). Where v4 already sends the `*_id` sibling alongside the hyperlink, the sibling wins and the URL field is dropped — never clobbered.
- Computed fields (`absolute_url`, `resource_uri`) are dropped; nested lists/objects are JSON-encoded.
- Scalars are stringified (bools as `"true"`/`"false"`); with a bulk-derived schema, rows are then projected onto exactly the header columns (extras dropped, missing ⇒ `NULL`).

A structural insight makes perfect representational equality unnecessary: **an incremental pull only returns rows whose `date_modified` advanced past the high-water mark — rows that genuinely changed and therefore warrant a new SCD2 version anyway.** So residual formatting differences between the bulk seed and API updates (e.g. a bulk `t` vs an API `"true"` for some boolean I did not special-case) can never create *spurious* versions. The corollary caveats are documented in the README: bulk columns the API does not return land as `NULL` in post-backfill versions of a changed row, and a full *API* re-pull over a bulk-seeded table would re-version rows on formatting alone — re-seed from a newer bulk file instead (SCD2 makes an unchanged re-seed a no-op).

### Everything TEXT, timestamps canonicalized

Bulk CSVs are untyped strings, and both paths must hash to comparable SCD2 content, so all columns are TEXT — matching the CMS precedent ("columns are text; casting is a downstream concern"). The deliberate exception is timestamps: `date_created`/`date_modified` are parsed and re-rendered to one canonical form (`YYYY-MM-DD HH:MM:SS[.ffffff]+00:00`) in **both** paths, because the incremental high-water mark is `max(date_modified)` over a TEXT column — lexicographic order is only chronological order if the format is uniform. Normalization fails loud on unparseable input; a silent passthrough would quietly corrupt the cursor. Both timestamp columns are flagged `Column(metadata=True)` so a re-pull that only bumps `date_modified` creates no spurious version.

### Cursor: `date_modified` with a numeric-aware `id` tiebreak

`cursor_spec` is `CursorSpec("date_modified", tiebreak="id")` (spec-overridable; `cursor_column=None` marks a resource non-incremental, and the driver then always runs full reads — which is also how bulk-only tables with no API endpoint are handled). Incremental reads request `date_modified__gte=<hwm>` server-side (`__gte`, not `__gt`, so equal-timestamp/higher-id rows stay reachable) ordered by `(date_modified, id)`, and filter strictly-after client-side per the interface. The tiebreak comparison is numeric where ids are digit-strings and lexicographic otherwise (courts uses slugs). One accepted imperfection: the engine's HWM tiebreak is a lexicographic max over the TEXT `id` column, which for digit-strings can be numerically *smaller* than the true max but never larger — so the failure mode is a harmless re-fetch of boundary rows (SCD2 no-op), never a skipped row.

### Developed offline: assumptions pinned as live tests

Every fact I could not verify (backtick quote character, S3 bucket URL and `list-type=2` listing, compound `order_by` under cursor pagination, the `page_size` parameter) is (a) isolated in one named constant or parameter, (b) called out in the client docstring and README, and (c) pinned by a specific test in `test_live.py` whose failure message says what to adjust. The hermetic suite pins the *intended* behavior; the live suite validates the *assumed* behavior. This costs one `pytest -m network` run from a networked machine before first production use.

## Implementation outline

Everything lives in `src/datadongle/collectors/courtlistener/`, per the interface's anatomy:

- **`spec.py`** — `CourtListenerDatasetSpec(DatasetSpec)`: `resource`, `backfill`, `filters`, `cursor_column` (default `date_modified`), `bulk_date` (pin an export; default latest), and `api_endpoint`/`bulk_file_prefix` overrides for resources where the endpoint and bulk prefix diverge (e.g. `clusters` vs `opinion-clusters`). `entity_key` defaults to `["id"]` (every CourtListener table has a stable `id` PK) ⇒ SCD2 by default; `None` opts into Append. Validation in `__post_init__` fails loudly on the filter/bulk conflict.
- **`client.py`** — `CourtListenerClient`: optional token (`COURTLISTENER_API_TOKEN`, sent as `Authorization: Token …`; anonymous works at a lower rate limit, and since the token never appears in a URL there is nothing to scrub), adapter-level retries on `{429, 500, 502, 503, 504}`, `iter_pages` following v4 `next` cursor URLs, S3 bulk listing with continuation-token pagination, streamed bulk downloads, and the header-peek.
- **`reader.py`** — `CourtListenerReader`: the protocol implementation plus the transforms above; bulk parsing lifts the csv module's 128 KiB field cap (opinion text runs to megabytes) and batches at 10k rows.
- **`metadata.py`** — `CourtListenerMetadata`: `endpoints`/`search` over the API root, `describe`/`columns` from OPTIONS metadata (falling back to a sampled row where the server exposes no field metadata), `bulk_exports`/`bulk_columns` for the discovery path that fills a spec.
- **`README.md`** — quickstart (bulk seed + incremental), the same-table caveats, and the offline-verification checklist.

## Testing

`tests/collectors/courtlistener/` mirrors the other suites (47 hermetic tests; the HTTP boundary faked at the client's `get_json`/bulk methods so pagination, transforms, projection, and cursor filtering run real code):

- `test_spec.py` — defaults and validation, including the filters-with-bulk rejection.
- `test_client.py` — cursor-pagination via `next` URLs, S3 XML parsing (namespace, truncation, continuation tokens, `schema-*.sql` and prefix-collision filtering), backtick header parsing, and the header-peek against a chunked fake stream.
- `test_metadata.py` — OPTIONS-vs-sample `columns` fallback, root parsing, bulk listing.
- `test_reader.py` — protocol conformance, bulk-header and API-sample schemas with the `metadata=True` flags, every transform rule, strictly-after filtering with the numeric tiebreak, and the headline end-to-end on a hermetic `IcebergEngine`: bulk seed of three rows → incremental API run picks up exactly one update and one insert → four current rows, five historical versions → re-run is a no-op.
- `test_live.py` — the offline-assumption smoke tests (`-m network`).

Behavioral invariants preserved: importing the package touches no network; peak memory is bounded by batch size on both paths; a failed run leaves the target untouched; the same reader+spec runs on both engines.

## Open questions / future work

- **Verify the offline assumptions** — run `uv run --no-sync pytest tests/collectors/courtlistener/test_live.py -m network` once from a networked machine. If compound `order_by=date_modified,id` is rejected, drop to `order_by=date_modified` (client-side strictly-after filtering still guards correctness, at the cost of tie-stability across page boundaries).
- **Resource alias map.** Endpoint-vs-bulk-prefix divergences are currently handled by spec overrides; if the same few pairs keep recurring, a small known-alias map in the collector would remove the foot-gun. Deferred until the live listing shows the actual divergences.
- **Deleted rows.** Neither path observes deletions incrementally. `SCD2(invalidate_missing=True)` on a full bulk re-seed would close out vanished entities; deferred until there is a consumer that cares.
- **Huge-table SCD2 merge cost.** A dockets-scale backfill stages tens of millions of rows through one write session; if the single merge proves painful on either engine, that is an engine concern, not a reader one — noting it here so the first full-scale run measures it.
- **m2m fields as first-class resources.** Fields like a docket's `panel` are join tables upstream; collecting them properly means a spec per join table (bulk exports exist for some). The generic design should handle this without code changes once the bulk prefixes are confirmed.

## References

- Collector interface: [`docs/spec/collector-interface.md`](../spec/collector-interface.md)
- CourtListener API help: <https://www.courtlistener.com/help/api/rest/>
- CourtListener bulk data help: <https://www.courtlistener.com/help/api/bulk-data/>
- Precedents drawn on: the CMS collector (two retrieval modes normalized to one shape), the EIA collector (client/reader/metadata layout, fail-loud casts), and the Census/TIGER decisions on skip-logic portability.
