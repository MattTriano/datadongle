"""The Census family driver.

A Census spec fans out over ``vintages × states`` into one target table, and its
variables can drift across vintages (a group gains or loses a variable between
years). That family shape doesn't fit the shared ``run_collection`` contract,
which is one-spec/one-table/one-write-session, so Census gets this thin driver on
top of the same engine + reader primitives.

What it adds over ``run_collection``:

  - **A union-of-vintages table.** Each vintage's schema is discovered
    independently and the table is created once with the union of every vintage's
    columns, so no vintage's data is silently dropped.
  - **Per-(vintage, state) write sessions.** Each state+vintage is staged and
    merged on its own — the same granularity the legacy collector tracked at.
  - **Per-(vintage, state) error isolation.** A failing pull (a transient API
    error the client's retries couldn't ride out, or a vintage without a variable
    group) is caught and reported; the others still land.

It deliberately does *not* carry the old collector's already-ingested skip: SCD2
is idempotent, so re-collecting an unchanged vintage is a no-op merge. The
trade-off is that every run re-fetches from the API; a caller that wants to avoid
that should simply not re-run vintages it has already collected.
"""

from __future__ import annotations

import dataclasses
import logging
from contextlib import nullcontext
from typing import Any

from datadongle.collectors.census.reader import CensusReader
from datadongle.collectors.census.spec import CensusDatasetSpec
from datadongle.core.engine import Engine
from datadongle.core.schema import Column, TableSchema

logger = logging.getLogger(__name__)


def run_census_collection(
    reader: CensusReader,
    spec: CensusDatasetSpec,
    engine: Engine,
    tracker: Any | None = None,
) -> dict[str, Any]:
    """Collect every ``(vintage, state)`` in a Census ``spec`` into one table.

    Discovers each vintage's schema, ensures the union-of-vintages table, then
    collects each ``(vintage, state)`` independently into its own write session.
    A vintage whose schema can't be discovered is dropped from both the union
    table and the collection loop; a ``(vintage, state)`` pull that fails is
    reported and skipped, and the others still land. Returns a summary dict with
    family-level counts and any per-unit errors.
    """
    target = reader.target(spec)
    write_mode = reader.write_mode(spec, mode="full")

    summary: dict[str, Any] = {
        "spec_name": spec.name,
        "vintages_processed": 0,
        "states_processed": 0,
        "total_rows_staged": 0,
        "total_rows_merged": 0,
        "total_rows_invalidated": 0,
        "errors": [],
    }

    # Discover each vintage's schema up front, isolating failures — a vintage
    # that can't be resolved is dropped from both the union table and the
    # collection loop rather than sinking the whole spec.
    schemas: dict[int, TableSchema] = {}
    for vintage in spec.vintages:
        try:
            schemas[vintage] = reader.schema(_narrow(spec, vintage))
        except Exception as e:
            logger.error("Census %s vintage=%d schema discovery failed: %s", spec.name, vintage, e)
            summary["errors"].append({"vintage": vintage, "error": str(e)})

    if not schemas:
        logger.info("Census %s: no collectable vintages; %s", spec.name, summary)
        return summary

    union_schema = _union_schema(schemas.values())
    engine.ensure_table(target, union_schema, write_mode)
    summary["vintages_processed"] = len(schemas)

    for vintage in schemas:
        for state_fips in spec.states:
            try:
                staged, merged, invalidated = _collect_one(
                    reader,
                    _narrow(spec, vintage, state_fips),
                    engine,
                    target,
                    union_schema,
                    write_mode,
                    tracker,
                )
                summary["states_processed"] += 1
                summary["total_rows_staged"] += staged
                summary["total_rows_merged"] += merged
                summary["total_rows_invalidated"] += invalidated
            except Exception as e:
                logger.error(
                    "Census %s vintage=%d state=%s failed: %s", spec.name, vintage, state_fips, e
                )
                summary["errors"].append(
                    {"vintage": vintage, "state_fips": state_fips, "error": str(e)}
                )

    logger.info("Census collection complete for %r: %s", spec.name, summary)
    return summary


def _narrow(
    spec: CensusDatasetSpec, vintage: int, state_fips: str | None = None
) -> CensusDatasetSpec:
    """A copy of ``spec`` scoped to one vintage (and optionally one state).

    Schema discovery narrows to a vintage only (schema is state-independent);
    collection narrows to a single ``(vintage, state)`` so each pair gets its
    own staged write session.
    """
    state = [state_fips] if state_fips is not None else spec.state_fips
    return dataclasses.replace(spec, vintages=[vintage], state_fips=state)


def _collect_one(
    reader: CensusReader,
    spec: CensusDatasetSpec,
    engine: Engine,
    target,
    union_schema: TableSchema,
    write_mode,
    tracker: Any | None,
) -> tuple[int, int, int]:
    """Collect one ``(vintage, state)`` into its own write session (union schema)."""
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
    """The union of every vintage's columns, in first-seen order.

    Geo-id, ``vintage``, and ``NAME`` come first (they lead every vintage's
    schema); variables accumulate as later vintages introduce new ones, so a spec
    whose vintages have drifting variable sets still gets one table holding all
    of their columns.
    """
    columns: list[Column] = []
    seen: set[str] = set()
    for schema in schemas:
        for col in schema.columns:
            if col.name in seen:
                continue
            seen.add(col.name)
            columns.append(col)
    return TableSchema(columns=columns)
