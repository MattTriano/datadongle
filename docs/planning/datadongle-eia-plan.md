# datadongle EIA collector — plan & resumption notes

Working notes for building a US EIA (Energy Information Administration) energy-data collector against the datadongle collector interface. Captured mid-design so work can resume after the sandbox image is rebuilt (to whitelist outbound URLs). Status as of 2026-07-09.

## Where we are

**Built and passing** (2026-07-09), on branch `feat/datadongle-eia`. All design decisions locked; API facts confirmed against eia.gov. The collector ships `spec.py`, `client.py`, `reader.py`, `metadata.py`, `README.md` under `src/datadongle/collectors/eia/` and a test suite under `tests/collectors/eia/` (`test_spec.py`, `test_client.py`, `test_reader.py`, `test_metadata.py`, `helpers.py`). 38 EIA tests pass; ruff + ty clean; no full-suite regressions. No `driver.py` (single-table-per-spec on the shared `run_collection`, as decided).

`metadata.py` (`EIAMetadata`) was added after the initial collector-only cut, at the user's request — a route-tree explorer (`browse`/`describe`/`frequencies`/`columns`/`facets`/`facet_values`/`search`) returning DataFrames like `FredMetadata`. So decision 3 below ("collector only") was revisited: the explorer now ships.

`builder.py` (`EIASpecBuilder`) was added next, to cut the manual surface of a spec. It reads a route's metadata once and both **fills** omitted fields (all measures ⇒ `data_columns`; the sole frequency; `entity_key` = the route's facet-id columns + `period`; a derived `eia_<route>_<frequency>` name/table) and **validates** provided ones against the route (unknown frequency/measure/facet-key raises at build time, listing the valid options; opt-in `check_facet_values=True` validates facet values too). It also offers `template(route)` — an editable, fully-populated `EIADatasetSpec(...)` snippet for notebooks. This resolves the deferred "validate frequency/facets against `get_route_metadata` at spec construction" follow-up by locating that validation in the builder rather than the spec's `__post_init__`, keeping the spec network-free. The shared normalization rule was extracted to a module-level `normalize_column_name` in `reader.py` so the builder derives `entity_key` names identically to the reader's output columns. 17 builder tests added (55 EIA tests total); ruff/ty clean; no full-suite regressions.

Env note after the image rebuild: `uv run` can't rebuild the project offline (no cached `hatchling`). The in-repo `.venv` works once its dangling interpreter references are repaired — its `bin/python` symlink and `pyvenv.cfg` `home` were repointed to `/opt/uv/python/cpython-3.13.14-linux-x86_64-gnu`. Run tests with `/workspace/.venv/bin/python -m pytest` (pytest's `pythonpath=["src"]` handles the import; the editable `.pth` still points at the old `/home/matt` path but is unused).

## The interface we're building against

Authoritative spec: `docs/spec/collector-interface.md`. Code protocols in `src/datadongle/core/` are the source of truth for signatures. Summary of what a new collector must provide:

- **Three axes**, joined by neutral value types: `SourceReader` (what to collect) → `WriteMode` (Append/Upsert/SCD2) → `Engine` (Postgres/Iceberg). Interchange is a **batch** = `list[dict[str, Any]]` plus a `TableSchema`.
- **Module layout** under `src/datadongle/collectors/eia/`: `spec.py` (MUST), `client.py` (MUST), `reader.py` (MUST), `metadata.py` (SHOULD — skipping for now, see decisions), `driver.py` (MAY — only if semantics demand), `README.md` (SHOULD).
- **`SourceReader` protocol** (`core/reader.py`): `source: str`, `dataset_id(spec)`, `target(spec)`, `schema(spec)`, `write_mode(spec, *, mode)`, `cursor_spec(spec)`, `read(spec, *, since)`, `extract_cursor(batch)`.
- **Shared driver** `datadongle.load.driver.run_collection(reader, spec, engine, tracker=None, *, mode)` is the one orchestration path: resolves target/schema/write_mode, `ensure_table`, reads the high-water mark from the **target table** (self-healing, never a run log), loops `read(...)` batches into a staged `write_batch(...)`, merges once on clean exit, returns a summary dict.
- **Default write-mode selection**: non-empty `spec.entity_key` ⇒ `SCD2(entity_key=...)`; `entity_key=None` ⇒ `Append()`.
- **`read` must stream** page-by-page (never accumulate the whole dataset); do all source-specific transforms inside `read` so rows match `schema(spec)`; strictly-after cursor filtering.
- **Client duties**: owns the `requests.Session`, base URL, auth, timeouts; retry/backoff via `tenacity` with a **module-level retry predicate** over `{429, 500, 502, 503, 504}` + connection/read errors; pagination as a **generator of batches**; stream bulk downloads to a temp file. Knows nothing about specs/schemas/storage. Readers construct the client **lazily** so importing touches no network.
- **Bookkeeping columns** (source ids, source timestamps) are stored but flagged `Column(metadata=True)` so they're excluded from the SCD2 content hash.
- **Family driver** (`driver.py`) only when the one-spec/one-table/one-write-session shape genuinely doesn't fit — the established case is DKAN (one spec names several sibling datasets unioned into one table, needing per-sibling write sessions + error isolation). Spec says: *prefer bending the spec to fit `run_collection`; write a family driver only when semantics (not convenience) demand it.*

Closest existing analogs studied: `bike_index` (simplest migrated reader on the shared driver; API-token via constructor), `fred/metadata.py` (API-key-via-env pattern: `api_key or os.environ.get("FRED_API_KEY")`; still legacy/unmigrated), `cms` (sample-to-discover-schema + union pattern), `threedep` (per-unit family driver for large downloads).

## EIA API v2 — confirmed facts

Confirmed via `WebSearch` (2026-07): [EIA API technical documentation](https://www.eia.gov/opendata/documentation.php), [Rami Krispin's EIAapi intro](https://ramikrispin.github.io/EIAapi/articles/intro.html).

- Base `https://api.eia.gov/v2/`. Auth is an `api_key` query param (free registration at eia.gov/opendata/register.php).
- The API is a **hierarchical tree of routes**: `category/subcategory/.../data-series`, e.g. `electricity/rto/region-sub-ba-data` (`electricity` = category, `rto` = Electric Power Operations subcategory, `region-sub-ba-data` = the leaf data series). `electricity/retail-sales` is another leaf.
- **Frequency is a query parameter within a route** (`monthly`/`quarterly`/`annual`/`daily`/`hourly`), NOT a separate route. A single route can offer several frequencies of the same series.
- **Geography and sector are facets within a route**, not separate routes: `?facets[stateid][]=CO&facets[sectorid][]=RES`. Valid facet values are discoverable at `/v2/<route>/facet/<facetid>/`.
- **Data query shape**: `/v2/<route>/data/?api_key=…&frequency=monthly&data[0]=value&facets[stateid][]=CA&start=YYYY-MM&end=YYYY-MM&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000`.
- A metadata GET at `/v2/<route>/` (no `/data/`) returns the route's `facets`, available `frequency` options, `data` (measure) columns, and date range.

**Confirmed from eia.gov source docs** (`https://www.eia.gov/opendata/documentation.php`, API v2.1.0, 2026-07):

- **Max `length` = 5000** rows per request (JSON; XML caps at 300 and is irrelevant to us).
- **Response envelope**: `{"response": {…}, "request": {…}, "apiVersion": "2.1.0"}` — `response`, `request`, `apiVersion` are top-level siblings. Data rows live at **`response.data`** (an array). `response.total` is the total matching-row count **as a string** (e.g. `"251"`) — the client must `int()` it and page with `offset`/`length` until `offset >= total`.
- **Data row shape** (literal example from the docs):
  ```json
  {"period": "2001-01", "stateid": "CO", "stateDescription": "Colorado",
   "sectorid": "RES", "sectorName": "residential",
   "price": "6.71", "price-units": "cents per kilowatthour"}
  ```
  i.e. `period` + facet id columns (`stateid`, `sectorid`) + description columns (`stateDescription`, `sectorName`) + the requested measure columns + one `<measure>-units` column each.
- **Metadata (non-`/data/`) envelope**: `response` carries `id`, `name`, `frequency: [...]`, `facets: [...]`, `data: {...}` (the measure-column catalog). That's what `get_route_metadata` reads.

**Two findings that shaped decisions 5 and 6 above:**

- **Measure values arrive as JSON strings** (`"price": "6.71"`) — the eia.gov changelog notes values were "standardized to return data values as strings." Drives the `DOUBLE`-with-safe-cast rule (decision 5).
- **Units columns are hyphenated** (`price-units`) and description columns are camelCase (`stateDescription`) — both need normalization to be queried without quotes (decision 6).

## Decisions

1. **Scope — Generic v2 reader** (locked). One `EIAReader` collects *any* EIA v2 dataset; the spec supplies route path, frequency, data columns, and facet filters. Mirrors how `SocrataReader` serves any dataset. Justified by the API's uniformity.
2. **Schema — Sample the first data page** (locked). Fetch page one, union its keys; type the requested measure columns as `DOUBLE`, everything else (`period`, facet ids, descriptions, units) as `TEXT`. Full source fidelity incl. description columns. Matches the CMS approach; costs one call at `schema()` time.
3. **Discovery — Collector only** (locked). No `metadata.py` in the first cut. Add an `EIAMetadata` browser later if interactive route/facet exploration is actually needed.
4. **Fan-out — Single table per spec, shared `run_collection`, no family driver** (locked). "Table" = the datadongle **target table**; one spec → exactly one target table (spec §3). One EIA route/series at one frequency, optionally facet-filtered, is the source-side unit and lands in one target table. Rationale below.
5. **Measure typing — `DOUBLE` with a safe, fail-loud cast** (locked). In `read`, cast each measure string: JSON `null`/`""` → `None`; a float-parseable string → `float`; **any other non-empty string → raise**, naming column/value/route (never silently null a signal-bearing token). Precision is safe (EIA values sit within float64's ~15–17 significant digits and 2^53 exact-integer range). eia.gov docs confirm values are returned as strings but do NOT document suppressed/withheld encoding, so the fail-loud branch makes "no signal loss" hold by construction. Belief: v2 uses `null` for missing (alpha sentinels were v1/bulk-file), so the raise should rarely fire; if it does, learn the encoding and map known sentinels or drop that column to `TEXT`.
6. **Column-name normalization — lowercase, non-alphanumerics → `_`** (locked). Every output column (facet ids, description cols, measures, `-units` cols, `period`) is normalized so it needs no double-quoting: lowercase, replace any run of non-`[a-z0-9]` with a single `_`, strip leading/trailing `_`. So `stateDescription` → `statedescription`, `price-units` → `price_units`, `sectorName` → `sectorname`. Matches the CMS normalization precedent. The reader tracks the raw→normalized mapping during schema sampling so `read` can emit normalized keys.

### Fan-out rationale (the open question)

The user asked whether EIA splits a single dataset (same grain/columns) across multiple routes differing by area/time/frequency — because if so, they'd want a driver to unify a topic+frequency into one table.

The API structure answers this: **EIA does not split one grain across sibling routes.** Within a route, frequency is a query parameter and area/sector are facets, so a route is already self-contained across area/time/frequency. Different *routes* are genuinely different data series (different grains and columns — e.g. `retail-sales` vs `rto/region-data`), which you would not union into one table. So the DKAN-style "several siblings → one table" situation that justifies a family driver does not arise from EIA's routing. Per §8 of the spec ("prefer bending the spec to fit `run_collection`"), the single-table model is the correct fit: one `(route, frequency, facet-filter)` = one grain = one target table.

The *only* future scenario that might motivate a family driver is a very large single route (e.g. hourly RTO data, millions of rows) where splitting the pull into per-facet-value units would give error isolation + incremental resume (the `threedep`-per-tile rationale, NOT the DKAN-union rationale). But §8 says build a family driver only when semantics demand it, not for convenience/resilience — and the streaming reader + staged write already bound memory. So: **defer**; revisit only if a real large-route pull proves fragile.

## Proposed module layout (to build)

- **`spec.py` — `EIADatasetSpec(DatasetSpec)`**: fields `route_path: str`, `frequency: str`, `data_columns: list[str]`, `facets: dict[str, list[str]]` (default empty), `start: str | None`, `end: str | None`, `entity_key: list[str] | None`, `target_table`, `target_schema` (default `"raw_data"`), `name`, `source="eia"`. `__post_init__` validates non-empty `route_path`/`frequency`/`data_columns`. **Default `entity_key`**: the facet id columns present in the data + `["period"]` — this is the SCD2 grain of a time-series observation. (Decide whether to auto-derive from `facets` keys or require the user to declare it; auto-derive is friendlier but the row's facet-id column names must match the `facets` keys — confirm against a real response.)
- **`client.py` — `EIAClient`**: `__init__(api_key=None, timeout=…)` with `api_key or os.environ.get("EIA_API_KEY")`, failing loudly if absent. Module-level `tenacity` retry predicate over `{429,500,502,503,504}` + conn/read errors. Methods: `get_route_metadata(route_path) -> dict`; `iter_data(route_path, *, frequency, data_columns, facets, start, end) -> Iterator[list[dict]]` paginating by `offset`/`length=5000`, sorted `period` asc, stopping when `offset >= response.total`; `get_facet_values(route_path, facet) -> list[dict]`. Injects `api_key` on every call.
- **`reader.py` — `EIAReader`** (`source = "eia"`): lazy client. `cursor_spec` → `CursorSpec(column="period")` (period is the time-series HWM; multiple rows share a period across facets — no tiebreak needed because SCD2 dedups the re-pulled boundary period; incremental sets `start=since.value`, inclusive, and relies on SCD2 idempotency for the boundary). `write_mode` → `SCD2(entity_key)` if `entity_key` else `Append`. `schema` → sample first page via client; measures → `DOUBLE`, all else `TEXT`; no per-row metadata timestamp exists in EIA rows, so nothing is flagged `metadata=True` (revisit if a route carries one). `read` → page `client.iter_data(...)` with `start=since.value` when incremental, transform each row to the sampled schema's columns, yield batches. `extract_cursor` → max `period` string in the batch.
- **`README.md`**: quickstart (spec + reader + `run_collection` onto a hermetic Iceberg warehouse), and the two quirks — (a) **revisions**: EIA revises historical values and rows carry no per-row updated timestamp, so the `period` cursor catches new periods but not revisions; schedule periodic `mode="full"` refreshes, which SCD2 turns into new versions only where a value actually changed; (b) `EIA_API_KEY` env var / constructor.

### Tests (`tests/collectors/eia/`)

- `FakeEIAClient` serving canned JSON pages from an in-memory dict (subclass real client, override only the HTTP boundary — the `static`/`cms` helper pattern).
- `test_spec.py` — field validation/defaults; `test_client.py` — offset/length pagination + retry against mocked HTTP; `test_reader.py` — `isinstance(reader, SourceReader)`, schema sampled from a canned page, row transform, `extract_cursor`, and an **end-to-end `run_collection` against a hermetic `IcebergEngine` (tmp_path) with `NoopTracker`**, plus the incremental round-trip (collect → re-collect unchanged = SCD2 no-op → mutate a value → re-collect = one new version).

## Next actions

1. ~~Confirm max `length`, response envelope, and row/description/units column naming against eia.gov docs.~~ **Done** (see "Confirmed from eia.gov source docs" above).
2. ~~Sign-off on single-table-per-spec, measure typing, and name normalization.~~ **Done** (decisions 4–6 locked).
3. `entity_key` default derivation: the facet-id columns in the row (`stateid`, `sectorid`, …) + `["period"]`. Row confirms facet-id column names equal the `facets` dict keys, so auto-deriving `entity_key` from `spec.facets.keys()` + `period` is viable — but a spec may legitimately pull *without* a facet filter yet still have facet-id columns in the rows; so deriving from the sampled row's non-measure/non-description/non-period columns is more robust. Decide at build time.
4. ~~Build `spec.py` → `client.py` → `reader.py` → tests → `README.md`.~~ **Done and green.**

Remaining / possible follow-ups (only if a need arises): validate `frequency`/facets against `get_route_metadata` at spec construction; a live smoke test against the real API with a key (all current tests fake the HTTP boundary); an `EIAMetadata` explorer if interactive route/facet discovery is wanted.

## Dev-env caveats (from memory)

Offline container; `pyproject.toml`/`uv.lock` are edit-locked (don't add deps — `requests`/`tenacity` are already present). Run tests with `uv run pytest` (hermetic Iceberg + unit; network/Postgres tests skip). Network-marked tests are deselected by default.
