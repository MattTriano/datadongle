"""The CMS family driver.

A CMS spec names one data.cms.gov dataset whose published *vintages* (annual or
monthly versions) all land in one target table. That one-spec/many-vintages
shape doesn't fit the shared ``run_collection`` contract (one spec / one table /
one write session), so CMS gets this thin driver on top of the same engine +
reader primitives.

What it adds over ``run_collection``:

  - **A union-of-vintages table.** Vintages can have drifting columns (a year
    adds or drops one); the table is created once with the union of every
    vintage's columns so no vintage's data is silently dropped.
  - **Per-vintage write sessions + error isolation.** Each vintage is staged and
    merged on its own; a vintage that fails schema discovery or collection is
    reported and skipped, and the others still land.

Like the other family drivers it carries no already-ingested skip and no
post-merge ``_source_modified`` advance: SCD2 is idempotent, so re-collecting an
unchanged vintage is a no-op merge. The trade-off is that every run re-downloads
each in-scope vintage; the old collector's freshness check avoided that but was
Postgres-only (it read physical ``valid_to`` and issued an in-place UPDATE), with
no engine-neutral analogue.
"""

from __future__ import annotations

import dataclasses
import logging
from contextlib import nullcontext
from typing import Any

from datadongle.collectors.cms.reader import CMSReader
from datadongle.collectors.cms.spec import CMSDatasetSpec
from datadongle.core.engine import Engine
from datadongle.core.schema import Column, TableSchema

logger = logging.getLogger(__name__)


def run_cms_collection(
    reader: CMSReader,
    spec: CMSDatasetSpec,
    engine: Engine,
    tracker: Any | None = None,
) -> dict[str, Any]:
    """Collect every in-scope vintage of a CMS ``spec`` into one target table.

    Resolves the dataset's vintages from the catalog, discovers each vintage's
    schema, ensures the union-of-vintages table, then collects each vintage
    independently into its own write session. A vintage that can't be resolved,
    sampled, or collected is reported and skipped; the others still land. Returns
    a summary dict with family-level counts and any per-vintage errors.
    """
    target = reader.target(spec)
    write_mode = reader.write_mode(spec, mode="full")

    summary: dict[str, Any] = {
        "spec_name": spec.name,
        "versions_processed": 0,
        "total_rows_staged": 0,
        "total_rows_merged": 0,
        "total_rows_invalidated": 0,
        "errors": [],
    }

    try:
        vintages = reader.versions(spec)
    except Exception as e:
        logger.error("CMS %s vintage resolution failed: %s", spec.name, e)
        summary["errors"].append({"error": str(e)})
        return summary

    # Discover each vintage's schema up front, isolating failures — a vintage
    # that can't be sampled is dropped from both the union table and the
    # collection loop rather than sinking the whole family.
    schemas: dict[str, TableSchema] = {}
    for vintage in vintages:
        try:
            schemas[vintage] = reader.schema(_narrow(spec, vintage))
        except Exception as e:
            logger.error("CMS %s vintage=%s schema discovery failed: %s", spec.name, vintage, e)
            summary["errors"].append({"vintage": vintage, "error": str(e)})

    if not schemas:
        logger.info("CMS %s: no collectable vintages; %s", spec.name, summary)
        return summary

    union_schema = _union_schema(schemas.values())
    engine.ensure_table(target, union_schema, write_mode)

    for vintage in schemas:
        try:
            staged, merged, invalidated = _collect_one(
                reader, _narrow(spec, vintage), engine, target, union_schema, write_mode, tracker
            )
            summary["versions_processed"] += 1
            summary["total_rows_staged"] += staged
            summary["total_rows_merged"] += merged
            summary["total_rows_invalidated"] += invalidated
        except Exception as e:
            logger.error("CMS %s vintage=%s failed: %s", spec.name, vintage, e)
            summary["errors"].append({"vintage": vintage, "error": str(e)})

    logger.info("CMS collection complete for %r: %s", spec.name, summary)
    return summary


def _narrow(spec: CMSDatasetSpec, vintage: str) -> CMSDatasetSpec:
    """A copy of ``spec`` scoped to a single vintage."""
    return dataclasses.replace(spec, vintages=[vintage])


def _collect_one(
    reader: CMSReader,
    spec: CMSDatasetSpec,
    engine: Engine,
    target,
    union_schema: TableSchema,
    write_mode,
    tracker: Any | None,
) -> tuple[int, int, int]:
    """Collect one vintage into its own write session using the union schema."""
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
    """The union of every vintage's columns, in first-seen order."""
    columns: list[Column] = []
    seen: set[str] = set()
    for schema in schemas:
        for col in schema.columns:
            if col.name in seen:
                continue
            seen.add(col.name)
            columns.append(col)
    return TableSchema(columns=columns)
