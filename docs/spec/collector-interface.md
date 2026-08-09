# The datadongle collector interface

This document is the authoritative description of the collector tooling contract: what a data-source integration consists of, what each piece is responsible for, and what behaviors are required versus recommended. It exists so that (1) legacy collectors have a fixed target to refactor to, (2) new sources are implemented the same way, and (3) a user who learns one collector can use them all.

The code protocols in `src/datadongle/core/` are the source of truth for exact signatures; this document is the source of truth for semantics, conventions, and responsibilities the type system can't express.

Keywords **MUST**, **SHOULD**, and **MAY** are used in the RFC 2119 sense.

---

## 1. The model: three independent axes

Every collection run composes three independently varying parts, joined by small neutral value types:

```
   SourceReader              WriteMode                Engine
  (what to collect)      (how to integrate)      (where it lands)
        │                       │                       │
  SocrataReader ───▶  Append / Upsert / SCD2  ───▶  PostgresEngine
  CKANReader                                        IcebergEngine
        │                                               │
        └───── batches: list[dict[str, Any]] ───────────┘
               + TableSchema (typed, geometry-aware)
               + Cursor / CursorSpec (incremental HWM)
```

- A **SourceReader** adapts one upstream source. It knows how to talk to the source, what the target table looks like, which write policy the dataset wants, and how to page incrementally. It knows nothing about staging, merges, or SQL.
- A **WriteMode** is a value object naming the reconciliation *policy*: `Append`, `Upsert(keys)`, or `SCD2(entity_key)`.
- An **Engine** is the storage *mechanism*: it creates tables, opens staged write sessions that realize a `WriteMode` natively, reads high-water marks back, and runs queries.
- The shared driver, `run_collection(reader, spec, engine, tracker, mode=...)`, is the only orchestration. It owns full-vs-incremental dispatch, the high-water-mark read, the batch loop, and run tracking.

The interchange format between reader and engine is a **batch**: `list[dict[str, Any]]`, with a `TableSchema` describing the column types. Any reader runs against any engine under any write mode its dataset supports.

## 2. Anatomy of a source integration

Each source lives in `src/datadongle/collectors/<source>/` and ships:

| File | Required | Role |
|---|---|---|
| `spec.py` | MUST | `<Source>DatasetSpec` — declares *what* to collect and *where* it lands |
| `client.py` | MUST¹ | `<Source>Client` — HTTP mechanics: session, retries, pagination, downloads |
| `reader.py` | MUST | `<Source>Reader` — implements the `SourceReader` protocol |
| `metadata.py` | SHOULD² | `<Source>Metadata` — interactive exploration of the source's catalog |
| `driver.py` | MAY³ | `run_<source>_collection` — only when the shared driver's shape doesn't fit |
| `README.md` | SHOULD | Source-specific usage, quirks, and any extra metadata methods |

¹ Omitted only when there is genuinely no client logic (a source read entirely from local files).
² Omitted when the source has no catalog/metadata API to expose (e.g. static file sources).
³ See §8. Needing one is the exception, not the norm.

Class and symbol naming is uniform: `SocrataDatasetSpec`, `SocrataClient`, `SocrataMetadata`*, `SocrataReader`, and a lowercase `reader.source` string (`"socrata"`). Tests mirror the layout under `tests/collectors/<source>/` (`test_spec.py`, `test_client.py`, `test_metadata.py`, `test_reader.py`).

\* Legacy names like `SocrataTableMetadata` converge on `<Source>Metadata` as sources are touched.

## 3. DatasetSpec — what to collect, where it lands

A spec is a frozen-in-intent dataclass subclassing `datadongle.collectors.base_spec.DatasetSpec`. It maps **a set of data assets from one source to exactly one target table** and carries:

- `source: str` — the source name, matching `reader.source`.
- `name: str` — the human-readable dataset name on *this* system.
- `target_table: str`, `target_schema: str` — where it lands (schema/namespace vocabulary stays out of readers via `TableRef`).
- `entity_key: list[str] | None` — the column set that uniquely identifies an entity in the data, when the source makes one identifiable. This drives the default write-mode selection (§5).
- Source-specific fields describing the asset(s): dataset ids, incremental column, download mode, row caps, etc.

**What belongs on the spec vs. the reader constructor:** the spec describes *the dataset* (varies per dataset, checked into config); the reader constructor takes *how to talk to the source* (credentials/app tokens, page sizes, timeouts — shared across every dataset pulled from that source). A spec MUST NOT carry credentials; a reader constructor MUST NOT carry dataset identity.

Specs SHOULD validate their fields in `__post_init__` and fail loudly at construction time, not mid-collection.

## 4. Client — internal, with fixed duties

The client is **not part of the public interface** — its methods are source-shaped (SoQL queries vs. datastore pages vs. bulk file URLs) and users interact with the Reader and Metadata instead. But every client has the same duties:

- Owns the HTTP `Session`, base URLs, auth headers/tokens, and timeouts.
- Implements retry/backoff for transient failures (the established pattern: `tenacity` with exponential backoff over retryable statuses `{429, 500, 502, 503, 504}` and connection/read errors, with a module-level retry predicate).
- Implements pagination as a **generator of batches** — never "fetch all pages, return a list".
- Streams bulk downloads to a temp file in chunks (`iter_content`) — never `resp.content` on a dataset-sized body.
- Knows nothing about specs, schemas, write modes, or storage.

A client MAY expose its `Metadata` companion as an attribute for convenience, and readers SHOULD construct their client lazily so importing a reader never opens a connection.

## 5. Metadata — exploring a source

Where a source has a catalog or metadata API, the `metadata.py` module lets a user answer, interactively: *what datasets does this portal have, what is this dataset, and what do its columns mean?* This is the discovery path that produces the values a user writes into a `DatasetSpec`.

Metadata classes SHOULD expose this **common core vocabulary**:

| Method | Returns | Question it answers |
|---|---|---|
| `search(text)` | iterable of catalog entries | "what's available matching this?" |
| `describe(dataset_id)` | dataset-level metadata (dict or dataclass) | "what is this dataset?" |
| `columns(dataset_id)` | list of column info (name, type, description where available) | "how do I interpret its fields?" |

Beyond the core, source-specific extras are freely allowed and encouraged (`distributions(...)` for DKAN, download-format probing for Socrata, CQL filters for ArcGIS Hub) — document them in the source README. The core names are a SHOULD, not a protocol: portals differ too much to force one signature, but a user should be able to type `meta.search("permits")` at any source and get somewhere.

Metadata is also the internal source of truth readers use to build their `TableSchema` (source column types → neutral `ColumnType`s). That mapping lives in the reader or metadata module, per source.

## 6. SourceReader — the source half of the load contract

`datadongle.core.reader.SourceReader` is a runtime-checkable Protocol. A reader is stateless with respect to any single run: all per-dataset variation comes in through the `spec` argument, so one reader instance serves many specs.

| Member | Contract |
|---|---|
| `source: str` | Stable lowercase source name, used in run logs. |
| `dataset_id(spec)` | Stable identifier for the spec within this source (for tracking). |
| `target(spec)` | The destination `TableRef` (built from `target_table`/`target_schema`). |
| `schema(spec)` | Engine-neutral `TableSchema` of the target — data columns plus any source bookkeeping columns, with geometry columns typed via `GeometrySpec` and bookkeeping columns flagged `metadata=True`. |
| `write_mode(spec, *, mode)` | The `WriteMode` policy. `mode` (`"full"`/`"incremental"`) is provided because a policy can legitimately depend on it (e.g. OSM enables `invalidate_missing` only on full pulls); most sources ignore it. |
| `cursor_spec(spec)` | `CursorSpec` naming the incremental HWM column (+ optional tiebreak), or `None` if the source/asset is not incrementally queryable. |
| `read(spec, *, since)` | Yields batches. `since=None` means a full read; otherwise yield only rows **strictly after** the cursor. |
| `extract_cursor(batch)` | The max cursor value in a batch (`None` if not applicable). Operates on already-transformed rows. |

Required behaviors:

- **`read` is a generator of ready-to-stage batches.** All source-specific transforms happen here — system-field renames, computed-column drops, geometry normalization to EWKT/WKB — so the driver and engine never see source idioms. Rows out of `read` MUST match the columns in `schema(spec)`.
- **`read` MUST stream.** Page-by-page for APIs; for bulk exports, download to a temp file (via the client), then parse in batches and delete the file. Never accumulate the whole dataset in memory.
- **Strictly-after cursor filtering.** With a tiebreak column, "after `since`" means `(col = since.value AND tiebreak > since.tiebreak) OR col > since.value`, and pagination MUST be ordered by `(col, tiebreak)` so pages are stable across equal timestamps.
- **Default write-mode selection:** non-empty `spec.entity_key` ⇒ `SCD2(entity_key=...)`; `entity_key=None` ⇒ `Append()`. A source MAY expose a spec field to opt into `Upsert` where versioned history is meaningless for that source; deviations are documented in the source README.
- **Bookkeeping columns** (source row ids, source timestamps, version counters) are stored but flagged `Column(metadata=True)` so a re-pull that only bumps a source timestamp doesn't create a spurious SCD2 version.

## 7. WriteMode, Engine, and staging

These are consumed, not implemented, by collector authors — but their semantics shape what a reader may assume.

**WriteMode** (`datadongle.core.write_mode`) is pure policy:

- `Append()` — insert every incoming row; no dedup, no versioning.
- `Upsert(keys, on_conflict="update"|"nothing")` — insert-or-update keyed rows.
- `SCD2(entity_key, invalidate_missing=False)` — keep every distinct record version, keyed by entity + content hash, without duplicating unchanged records. `invalidate_missing=True` closes out entities absent from a pull and is only meaningful on a full read — the driver rejects it under `mode="incremental"`.

**Engine** (`datadongle.core.engine`) realizes policy per backend: `ensure_table` (mode-aware, idempotent DDL; adds `ingested_at` and, for SCD2, the engine's versioning columns), `open_write`, `read_high_water_mark`, `table_exists` / `table_columns` / `geometry_columns`, and `query`. `PostgresEngine` implements SCD2 with physical `valid_from`/`valid_to`; `IcebergEngine` uses an append-only satellite with read-time end-dating. Same policy, engine-native mechanisms.

**Staging is mandatory.** `open_write` returns a `WriteSession` context manager. `write_batch` calls stage rows incrementally to a durable-adjacent scratch location — a temp table on Postgres, spooled-to-disk Parquet on Iceberg — and the merge into the real target happens **once, on clean context exit**. An exception inside the session MUST leave the target untouched. This, plus streaming readers, is the low-memory guarantee: at no point does the pipeline hold a full collection in RAM, so collectors run on small machines regardless of dataset size.

**High-water marks are read from the target table**, never from a run log (`engine.read_high_water_mark(target, cursor_spec)`), so incremental state self-heals if a table is dropped and rebuilt. The tracker records the HWM for observability only.

**DDL ownership is the deployment's choice, but the renderer is always the source of truth.** By default an engine creates its own tables. Where an external migration tool owns the schema, `PostgresEngine(creds, manage_ddl=False)` makes `ensure_table` verify instead of create — raising `TableNotFoundError` or `SchemaDriftError`, each carrying the SQL to apply. Either way the DDL comes from the same renderer (`engines.postgres_ddl`, a pure function of `(TableRef, TableSchema, WriteMode)` that needs no connection), so a checked-in migration and an engine-created table cannot disagree. A collector author needs to know only that the target carries pipeline columns it never declares — `ingested_at` always, plus the engine's SCD2 columns under `SCD2` — and so MUST NOT declare them in `schema(spec)`.

**Drift is reported, not silently absorbed.** `engine.diff_table(target, schema, mode)` returns an engine-neutral `SchemaDiff` (missing / unexpected / retyped columns, with engine-owned pipeline columns excluded). Only *additive* drift has a rendered fix (`render_migration`); a dropped or retyped column raises, because resolving it requires a decision about existing rows. This matters to readers because a schema that silently changes shape between runs re-versions every row under SCD2 — see the caveat about network-derived schemas in the CourtListener README.

## 8. The shared driver — and when you may deviate

`datadongle.load.driver.run_collection(reader, spec, engine, tracker=None, *, mode)` is the one orchestration path:

1. Resolve `target`, `schema`, `write_mode` from the reader; `ensure_table`.
2. `mode="full"` ⇒ `since=None`. `mode="incremental"` ⇒ read the HWM from the target; a source with `cursor_spec(spec) is None` falls back to a full read.
3. Open the tracker run and the write session; loop `read(...)` batches into `write_batch(...)`, tracking the max cursor seen.
4. On clean exit the session merges; the run records `rows_staged`/`rows_merged`/HWM; a summary dict is returned.

A `tracker`, when provided, exposes `track(source, dataset_id, target_table)` returning a context manager yielding a run object with settable `rows_staged`, `rows_merged`, `rows_ingested`, and `high_water_mark` fields (see `IngestionTracker`; tests use `NoopTracker`).

**Family drivers.** A source MAY ship its own `run_<source>_collection` in `driver.py` only when the shared contract's one-spec/one-table/one-write-session shape genuinely doesn't fit — the established example is DKAN, where one spec names several sibling datasets landing in one table, needing a union-of-siblings schema, per-sibling write sessions, and per-sibling error isolation. A family driver MUST be built from the same primitives (the source's Reader, the `Engine` protocol, the tracker contract) and MUST NOT reach around the engine to issue storage-specific SQL. Prefer bending the spec to fit `run_collection`; write a family driver only when semantics (not convenience) demand it.

## 9. New-collector checklist

Implementation:

- [ ] `spec.py`: `<Source>DatasetSpec(DatasetSpec)` with `source`, `name`, `target_table`, `target_schema`, `entity_key`, and source-specific fields; validation in `__post_init__`; no credentials.
- [ ] `client.py`: session + retries (module-level retry predicate, exponential backoff), pagination as a generator, bulk downloads streamed to temp files; no spec/storage knowledge.
- [ ] `metadata.py` (if the source has a catalog): `search` / `describe` / `columns` core, plus documented extras.
- [ ] `reader.py`: implements every `SourceReader` member; lazy client; all source transforms inside `read`; rows match `schema(spec)`; bookkeeping columns flagged `metadata=True`; geometry columns declared with `GeometrySpec`.
- [ ] Cursor correctness: `cursor_spec` returns `None` for non-incremental assets; `read` filters strictly-after with a tiebreak; pagination ordered by `(cursor, tiebreak)`; `extract_cursor` works on transformed rows.
- [ ] Write-mode mapping: `entity_key` ⇒ SCD2, else Append; any deviation documented in the README.
- [ ] `README.md`: quickstart (spec + reader + `run_collection`), source quirks, extra metadata methods.

Tests (`tests/collectors/<source>/`):

- [ ] `test_spec.py` — field validation and defaults.
- [ ] `test_client.py` — pagination, retry behavior, streaming (mocked HTTP; no live calls).
- [ ] `test_metadata.py` — catalog parsing against canned responses.
- [ ] `test_reader.py` — protocol conformance (`isinstance(reader, SourceReader)`), schema correctness, transform behavior, cursor extraction, and an end-to-end `run_collection` against a hermetic `IcebergEngine` (tmp-path warehouse) with a `NoopTracker`.
- [ ] Incremental round-trip: collect, re-collect unchanged (SCD2 no-op), mutate a record, re-collect (one new version).

Behavioral invariants any reviewer should be able to check:

- Importing the collector package touches no network and no storage backend.
- Peak memory is bounded by batch size, not dataset size, on both the read and write paths.
- A failed run leaves the target table exactly as it was.
- The same reader+spec runs unchanged against `PostgresEngine` and `IcebergEngine`.
