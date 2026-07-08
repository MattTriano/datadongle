"""The DKAN family driver.

A DKAN spec can name several dataset identifiers (e.g. one Open Payments
dataset per program year) that all land in one target table. That family
shape doesn't fit the shared ``run_collection`` contract, which is
one-spec/one-table/one-write-session, so DKAN gets this thin driver on top of
the same engine + reader primitives.

What it adds over ``run_collection``:

  - **A union-of-siblings table.** Siblings can have drifting columns
    (a program year adds or drops one); the table is created once with the
    union of every sibling's columns so no sibling's data is silently dropped.
  - **Per-dataset write sessions.** Each identifier is staged and merged on its
    own, which is what single-dataset ``invalidate_missing`` needs (it closes
    out entities absent from *that* dataset's pull, not the whole family).
  - **Per-dataset error isolation.** A failing sibling is caught and reported;
    the others still land.

It deliberately does *not* carry the old collector's freshness-skip or the
post-merge ``_source_modified`` advance: SCD2 is idempotent, so re-collecting
an unchanged dataset is a no-op merge, and the advance was an in-place UPDATE
with no Shape-B/Iceberg analogue.
"""

from __future__ import annotations

import dataclasses
import logging
from contextlib import nullcontext
from typing import Any

from datadongle.collectors.dkan.reader import PROVENANCE_COLUMNS, DKANReader
from datadongle.collectors.dkan.spec import DKANDatasetSpec
from datadongle.core.engine import Engine
from datadongle.core.schema import Column, TableSchema

logger = logging.getLogger(__name__)

_PROVENANCE_NAMES = {c.name for c in PROVENANCE_COLUMNS}


def run_dkan_collection(
    reader: DKANReader,
    spec: DKANDatasetSpec,
    engine: Engine,
    tracker: Any | None = None,
) -> dict[str, Any]:
    """Collect every dataset in a DKAN ``spec`` into one target table.

    Discovers each sibling's schema, ensures the union-of-siblings table, then
    collects each dataset identifier independently into its own write session.
    Every step is isolated per dataset: a sibling that fails schema discovery or
    collection is reported and skipped, and the others still land. Returns a
    summary dict with family-level counts and any per-dataset errors.
    """
    target = reader.target(spec)
    write_mode = reader.write_mode(spec, mode="full")

    summary: dict[str, Any] = {
        "spec_name": spec.name,
        "datasets_processed": 0,
        "total_rows_staged": 0,
        "total_rows_merged": 0,
        "total_rows_invalidated": 0,
        "errors": [],
    }

    # Discover each sibling's schema up front, isolating failures — a sibling
    # that can't be sampled is dropped from both the union table and the
    # collection loop rather than sinking the whole family.
    schemas: dict[str, TableSchema] = {}
    for identifier in spec.dataset_identifiers:
        try:
            schemas[identifier] = reader.schema(_narrow(spec, identifier))
        except Exception as e:
            logger.error("DKAN %s dataset=%s schema discovery failed: %s", spec.name, identifier, e)
            summary["errors"].append({"dataset_identifier": identifier, "error": str(e)})

    if not schemas:
        logger.info("DKAN %s: no collectable datasets; %s", spec.name, summary)
        return summary

    union_schema = _union_schema(schemas.values())
    engine.ensure_table(target, union_schema, write_mode)

    for identifier in schemas:
        try:
            staged, merged, invalidated = _collect_one(
                reader, _narrow(spec, identifier), engine, target, union_schema, write_mode, tracker
            )
            summary["datasets_processed"] += 1
            summary["total_rows_staged"] += staged
            summary["total_rows_merged"] += merged
            summary["total_rows_invalidated"] += invalidated
        except Exception as e:
            logger.error("DKAN %s dataset=%s failed: %s", spec.name, identifier, e)
            summary["errors"].append({"dataset_identifier": identifier, "error": str(e)})

    logger.info("DKAN collection complete for %r: %s", spec.name, summary)
    return summary


def _narrow(spec: DKANDatasetSpec, identifier: str) -> DKANDatasetSpec:
    """A copy of ``spec`` scoped to a single dataset identifier."""
    return dataclasses.replace(spec, dataset_identifiers=[identifier])


def _collect_one(
    reader: DKANReader,
    spec: DKANDatasetSpec,
    engine: Engine,
    target,
    union_schema: TableSchema,
    write_mode,
    tracker: Any | None,
) -> tuple[int, int, int]:
    """Collect one dataset into its own write session using the union schema."""
    identifier = reader.dataset_id(spec)
    if tracker is not None:
        run_ctx = tracker.track(reader.source, identifier, str(target))
    else:
        run_ctx = nullcontext(None)

    with run_ctx as run, engine.open_write(target, union_schema, write_mode) as ws:
        for batch in reader.read(spec, since=None):
            ws.write_batch(batch)
        if run is not None:
            run.rows_staged = ws.rows_staged
            run.rows_merged = ws.rows_merged
            run.rows_ingested = ws.rows_merged

    return ws.rows_staged, ws.rows_merged, ws.rows_invalidated


def _union_schema(schemas) -> TableSchema:
    """The union of every sibling's data columns, with provenance columns last.

    Data columns are merged in first-seen order so a family whose siblings
    drift still gets one table holding all of their columns.
    """
    data_columns: list[Column] = []
    seen: set[str] = set()
    for schema in schemas:
        for col in schema.columns:
            if col.name in _PROVENANCE_NAMES or col.name in seen:
                continue
            seen.add(col.name)
            data_columns.append(col)
    return TableSchema(columns=data_columns + PROVENANCE_COLUMNS)
