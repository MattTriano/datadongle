"""The static-file family driver.

A static-file spec names a manifest of file URLs (one per vintage/edition) that
all land in one target table. That one-spec/many-files shape doesn't fit the
shared ``run_collection`` contract (one spec / one file / one write session), so
static files get this thin driver on top of the same engine + reader primitives.

What it adds over ``run_collection``:

  - **One table from the first file's header.** The manifest's files share a
    column layout, so the table is created once from the first file's schema
    (all columns text, plus a text ``vintage``).
  - **Per-file write sessions + error isolation.** Each file is staged and merged
    on its own; a file that fails download/parse is reported and skipped, and the
    others still land.

Like the other family drivers it keeps no already-ingested skip: with an
``entity_key`` SCD2 is idempotent, so re-collecting an unchanged edition is a
no-op merge (and an in-place revision versions only the changed rows). The
trade-off is that every run re-downloads the manifest; static-file editions are
small, and the mitigation is to grow the manifest rather than re-running.
(Append-only specs have no dedup guard — don't re-run them.)
"""

from __future__ import annotations

import dataclasses
import logging
from contextlib import nullcontext
from typing import Any

from datadongle.collectors.static.reader import StaticFileReader
from datadongle.collectors.static.spec import StaticFileDatasetSpec
from datadongle.core.engine import Engine

logger = logging.getLogger(__name__)


def run_static_collection(
    reader: StaticFileReader,
    spec: StaticFileDatasetSpec,
    engine: Engine,
    tracker: Any | None = None,
) -> dict[str, Any]:
    """Collect every file in a static-file ``spec`` into one target table.

    Discovers the table schema from the first file, ensures the table, then
    collects each file independently into its own write session. A file that
    fails is reported and skipped; the others still land. Returns a summary dict
    with family-level counts and any per-file errors.
    """
    target = reader.target(spec)
    write_mode = reader.write_mode(spec, mode="full")

    summary: dict[str, Any] = {
        "spec_name": spec.name,
        "files_processed": 0,
        "total_rows_staged": 0,
        "total_rows_merged": 0,
        "total_rows_invalidated": 0,
        "errors": [],
    }

    try:
        schema = reader.schema(spec)
    except Exception as e:
        logger.error("Static %s schema discovery failed: %s", spec.name, e)
        summary["errors"].append({"vintage": spec.files[0].vintage, "error": str(e)})
        return summary

    engine.ensure_table(target, schema, write_mode)

    for file_ref in spec.files:
        try:
            staged, merged, invalidated = _collect_one(
                reader, _narrow(spec, file_ref), engine, target, schema, write_mode, tracker
            )
            summary["files_processed"] += 1
            summary["total_rows_staged"] += staged
            summary["total_rows_merged"] += merged
            summary["total_rows_invalidated"] += invalidated
        except Exception as e:
            logger.error("Static %s vintage=%s failed: %s", spec.name, file_ref.vintage, e)
            summary["errors"].append({"vintage": file_ref.vintage, "error": str(e)})

    logger.info("Static collection complete for %r: %s", spec.name, summary)
    return summary


def _narrow(spec: StaticFileDatasetSpec, file_ref) -> StaticFileDatasetSpec:
    """A copy of ``spec`` scoped to a single file."""
    return dataclasses.replace(spec, files=[file_ref])


def _collect_one(
    reader: StaticFileReader,
    spec: StaticFileDatasetSpec,
    engine: Engine,
    target,
    schema,
    write_mode,
    tracker: Any | None,
) -> tuple[int, int, int]:
    """Collect one file into its own write session."""
    identifier = reader.dataset_id(spec)
    if tracker is not None:
        run_ctx = tracker.track(reader.source, identifier, str(target))
    else:
        run_ctx = nullcontext(None)

    with run_ctx as run, engine.open_write(target, schema, write_mode) as ws:
        for batch in reader.read(spec, since=None):
            ws.write_batch(batch)
        if run is not None:
            run.rows_staged = ws.rows_staged
            run.rows_merged = ws.rows_merged
            run.rows_ingested = ws.rows_merged

    return ws.rows_staged, ws.rows_merged, ws.rows_invalidated
