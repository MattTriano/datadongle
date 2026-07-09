# StaticFile collector

Collection tooling for datasets published as **static files at fixed URLs** — no catalog API, no query language, no pagination. The publisher overwrites a file when the data changes; everyone who requests the URL gets the same bytes. First concrete source: the [AHRQ Compendium of U.S. Health Systems](https://www.ahrq.gov/chsp/data-resources/compendium.html).

Reach for this collector when the source is "a download link on a webpage": annually published reference files, crosswalks, fee schedules, and the like. It also handles files you had to download by hand (e.g. from behind a bot-defense wall) via `file://` URLs — same parse and ingest path, no HTTP.

This is the most generic collector, and it diverges from the four-class shape in one way: there is **no Metadata class**. With no catalog to explore, the spec's manifest of files *is* the metadata. The pieces are `FileRef` + `StaticFileDatasetSpec` (`spec.py`), `StaticFileClient` (`client.py`), `StaticFileReader` (`reader.py`), and the `run_static_collection` family driver (`driver.py`) — which lands one spec's manifest of files in one table on any engine (`PostgresEngine`, `IcebergEngine`).

## Why a family driver

A static-file spec is a manifest: many files (one per vintage/edition) landing in one table. That one-spec/many-files shape doesn't fit the shared `run_collection` contract, so static files ship `run_static_collection`, which discovers the table schema from the first file, ensures the table, then collects each file into its own staged write session with per-file error isolation.

## 1. Find the files

Discovery is manual: locate the stable download URLs on the publisher's pages and record them in the manifest. Prefer CSV over XLSX when both are offered (smaller, no `openpyxl` dependency). Resist scraping the publisher's pages — hand-edited URLs and roughly-annual editions mean a manifest entry per year is cheaper than maintaining a scraper.

Two publisher quirks to check up front:

- **Bot defenses.** Some hosts (ahrq.gov among them) sit behind AWS WAF. The escalation path, cheapest first: pass a browser `user_agent` to the client; preload a browser-minted token via `StaticFileClient(cookies={"aws-waf-token": ...})` (the session UA must match the browser that minted it); or download by hand and use `file://` URLs. The client deliberately contains no WAF logic.
- **Revisions in place.** Publishers sometimes silently overwrite a file (AHRQ marks these with a `-rev` suffix, but not always). SCD2 handles this — see below.

## 2. Inspect the files

There's no metadata class, but you don't need the warehouse to look at a file — download it and parse it with the client:

```python
from itertools import islice
from datadongle.collectors.static.client import StaticFileClient
from datadongle.collectors.static.spec import FileRef

client = StaticFileClient()
ref = FileRef(url="https://.../chsp-hospital-linkage-2023.csv", vintage="2023", encoding="cp1252")
path = client.download_to_tempfile(ref.url)
for row in islice(client.parse_file(path, ref), 3):
    print(row)
path.unlink()
```

Column names arrive sanitized (lowercase, runs of non-alphanumerics collapsed to underscores) and every value is a stripped string. If the file's encoding is wrong you'll know immediately: parsing raises rather than silently mangling bytes (a cp1252 en dash under the default `utf-8-sig` is a `UnicodeDecodeError`, not a corrupted name).

## 3. Write the spec

```python
from datadongle.collectors.static.spec import FileRef, StaticFileDatasetSpec

AHRQ_HOSPITAL_LINKAGE = StaticFileDatasetSpec(
    name="ahrq_chsp_hospital_linkage",
    target_table="ahrq_chsp_hospital_linkage",
    entity_key=["ccn", "vintage"],
    files=[
        FileRef(url=".../chsp-hospital-linkage-2023.csv", vintage="2023", encoding="cp1252"),
    ],
)
```

Field by field:

- `files` — the manifest. Each `FileRef` carries one URL, its `vintage` label, and its parse options: `file_format` (`"csv"`/`"xlsx"`), `encoding` (default `utf-8-sig`; Windows-pipeline publishers are usually `cp1252`), `delimiter`, `sheet` (XLSX name/index), and `skip_rows` for preamble lines above the header. Adding a new edition later is one manifest line. Vintages must be distinct; all files in one manifest land in one table and must share a column layout (a divergent edition needs its own spec/table, reconciled downstream).
- `vintage` — stamped onto every row as the `vintage` column.
- `entity_key` — columns that uniquely identify a record for SCD2 versioning, in *sanitized* form. `None` means append-only. For edition-snapshot sources the key should include `vintage`: the same hospital in the 2022 and 2023 editions is two observations, not an update to one entity, so SCD2 only versions in-place revisions *within* an edition.
- `target_schema` — defaults to `raw_data`.

### Finding the `entity_key`

Check the publisher's data dictionary first (AHRQ ships a techdoc PDF per file). If documentation is thin, verify candidates empirically — download the file and count:

```python
from collections import Counter

rows = list(client.parse_file(client.download_to_tempfile(ref.url), ref))
counts = Counter(tuple(r[c] for c in ("ccn",)) for r in rows)
{k: n for k, n in counts.items() if n > 1}   # {} -> unique within the file: valid with vintage appended
```

Watch for key-poisoners: empty strings (all values are text, so nulls arrive as `""` and group together) and identifiers that only look unique because of leading zeros, which this pipeline preserves precisely so keys like CCN stay intact.

### Gotchas

- **Wrong encoding fails loudly by design.** Declare the right one per `FileRef` rather than adding `errors="replace"` — a crash beats silently corrupted names.
- **Everything is text.** `0895` stays `0895`; casting is a downstream concern.
- **XLSX needs `openpyxl`** (imported lazily). Excel has no date-only type, so date cells land as midnight ISO timestamps; integral floats lose Excel's trailing `.0`.
- **A data URL returning HTML raises.** That's the guard catching a WAF challenge or error page before it's parsed as data.

## 4. Collect

`StaticFileReader` + `run_static_collection` land the manifest on any engine, creating the table for you — no manual DDL step.

```python
from datadongle.collectors.static.reader import StaticFileReader
from datadongle.collectors.static.driver import run_static_collection
from datadongle.engines.iceberg import IcebergEngine   # or engines.postgres.PostgresEngine

reader = StaticFileReader()
engine = IcebergEngine("/data/warehouse")              # or PostgresEngine(creds)

summary = run_static_collection(reader, AHRQ_HOSPITAL_LINKAGE, engine)
# {"spec_name", "files_processed", "total_rows_staged",
#  "total_rows_merged", "total_rows_invalidated", "errors"}
```

The table gets one `text` column per source column (from the first file's header), a `vintage text not null` column, an `ingested_at` column, and — when `entity_key` is set — the engine's SCD2 columns and indexes.

### Re-runs

There is no incremental mode and **no already-ingested skip** (it can't be expressed portably across both engines). Every run re-downloads the manifest — cheap for edition-snapshot files — and with an `entity_key` SCD2 makes re-collection safe: unchanged rows dedupe to a no-op merge, and a file a publisher revised in place gets a new version per changed row with the old one closed out. The incremental story is the manifest: **grow it with the new edition and re-run.**

> **Append-only specs** (`entity_key=None`) have no dedup guard — a re-run duplicates every row. Only collect them once.

The `Append` / `SCD2` write-mode behaviors and the collector interface as a whole are documented in [`docs/spec/collector-interface.md`](../../../../docs/spec/collector-interface.md).
