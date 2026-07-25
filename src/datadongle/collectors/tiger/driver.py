"""The TIGER family driver.

A TIGER spec fans out over ``vintages × units`` (national / state / county
files) into one target table, and its shapefile schema can drift across
vintages. That family shape doesn't fit the shared ``run_collection`` contract
(one spec / one table / one write session), so TIGER gets this thin driver on
top of the same engine + reader primitives.

What it adds over ``run_collection``:

  - **A union-of-vintages table.** Each vintage's schema is discovered from a
    sample file and the table is created once with the union of every vintage's
    columns, so a vintage's extra column isn't silently dropped.
  - **A single, consistent write mode.** The entity key is resolved once (from
    the union of discovered columns) so the whole family shares one SCD2 key —
    or falls back to append-only when no ID column exists.
  - **Per-file write sessions + error isolation.** Each ``(vintage, unit)`` file
    is staged and merged on its own; a file that fails download/parse is reported
    and skipped, and the others still land.

Like the other family drivers it drops the legacy already-ingested skip: SCD2 is
idempotent, so re-collecting an unchanged file is a no-op merge. (Append-only
layers have no such guard — don't re-run them.)
"""

from __future__ import annotations

import dataclasses
import logging
from contextlib import nullcontext
from typing import Any

from datadongle.collectors.tiger.reader import TigerReader
from datadongle.collectors.tiger.spec import TigerDatasetSpec
from datadongle.core.engine import Engine
from datadongle.core.schema import Column, TableSchema

logger = logging.getLogger(__name__)


def run_tiger_collection(
    reader: TigerReader,
    spec: TigerDatasetSpec,
    engine: Engine,
    tracker: Any | None = None,
) -> dict[str, Any]:
    """Collect every ``(vintage, unit)`` file in a TIGER ``spec`` into one table.

    Discovers each vintage's schema, ensures the union-of-vintages table under a
    single resolved write mode, then collects each file independently into its
    own write session. A vintage whose sample can't be inspected is dropped from
    the union and the loop; a file that fails is reported and skipped. Returns a
    summary dict with family-level counts and any per-unit errors.
    """
    target = reader.target(spec)

    summary: dict[str, Any] = {
        "spec_name": spec.name,
        "vintages_processed": 0,
        "files_processed": 0,
        "total_rows_staged": 0,
        "total_rows_merged": 0,
        "total_rows_invalidated": 0,
        "errors": [],
    }

    # Discover each vintage's schema up front, isolating failures.
    schemas: dict[int, TableSchema] = {}
    for vintage in spec.vintages:
        try:
            schemas[vintage] = reader.schema(_narrow(spec, vintage))
        except Exception as e:
            logger.error("TIGER %s vintage=%d schema discovery failed: %s", spec.name, vintage, e)
            summary["errors"].append({"vintage": vintage, "error": str(e)})

    if not schemas:
        logger.info("TIGER %s: no collectable vintages; %s", spec.name, summary)
        return summary

    union_schema = _union_schema(schemas.values())
    # Resolve the write mode once, from the union of every vintage's columns
    # (schemas are now cached in the reader), so the whole table shares one key.
    write_mode = reader.write_mode(spec, mode="full")
    engine.ensure_table(target, union_schema, write_mode)
    summary["vintages_processed"] = len(schemas)

    for vintage in schemas:
        for fips in reader.units(spec, vintage):
            try:
                staged, merged, invalidated = _collect_one(
                    reader,
                    _narrow(spec, vintage, fips),
                    engine,
                    target,
                    union_schema,
                    write_mode,
                    tracker,
                )
                summary["files_processed"] += 1
                summary["total_rows_staged"] += staged
                summary["total_rows_merged"] += merged
                summary["total_rows_invalidated"] += invalidated
            except Exception as e:
                logger.error("TIGER %s vintage=%d unit=%s failed: %s", spec.name, vintage, fips, e)
                summary["errors"].append({"vintage": vintage, "state_fips": fips, "error": str(e)})

    logger.info("TIGER collection complete for %r: %s", spec.name, summary)
    return summary


def _narrow(spec: TigerDatasetSpec, vintage: int, fips: str | None = "__all__") -> TigerDatasetSpec:
    """A copy of ``spec`` scoped to one vintage (and optionally one unit).

    Schema discovery narrows to a vintage only (``fips="__all__"`` keeps the
    spec's states); collection narrows to a single unit — ``state_fips=[fips]``
    for a state/county file, or ``None`` for a national file.
    """
    if fips == "__all__":
        return dataclasses.replace(spec, vintages=[vintage])
    state = [fips] if fips is not None else None
    return dataclasses.replace(spec, vintages=[vintage], state_fips=state)


def _collect_one(
    reader: TigerReader,
    spec: TigerDatasetSpec,
    engine: Engine,
    target,
    union_schema: TableSchema,
    write_mode,
    tracker: Any | None,
) -> tuple[int, int, int]:
    """Collect one file into its own write session using the union schema."""
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
