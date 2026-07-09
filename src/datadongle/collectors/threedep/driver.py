"""The 3DEP family driver.

A 3DEP spec fans out over the 1-degree source tiles its bbox touches, all
landing in one target table. Each tile is a multi-hundred-MB download, which is
why the shape deviates from the shared ``run_collection`` (one spec / one write
session):

  - **Per-tile write sessions + error isolation.** Each 1-degree tile is
    staged and merged on its own, so a failed download doesn't discard other
    tiles' already-merged work, and a crashed run resumes at the failed tile.
  - **The already-present skip survives, via the cursor.** Tiles are processed
    in sorted name order and ``source_tile`` is the high-water mark, so an
    incremental run collects only tiles strictly after the max name already in
    the target — no re-download of tiles we hold. ``mode="full"`` re-fetches
    every tile; SCD2 dedupes unchanged sub-tiles and versions re-staged ones,
    so a periodic full run is how USGS re-stages are picked up.
  - **Fail-stop under incremental.** If a tile fails on an incremental run, the
    remaining (larger-named) tiles are left for the next run: letting them merge
    would advance the high-water mark past the failed tile and future
    incremental runs would silently skip it. A full run isolates the failure
    and continues, since it never consults the mark.

Built from the same primitives as ``run_collection`` (the reader, the Engine
protocol, the tracker contract); no storage-specific SQL.
"""

from __future__ import annotations

import dataclasses
import logging
from contextlib import nullcontext
from typing import Any, Literal

from datadongle.collectors.threedep.reader import ThreeDEPReader
from datadongle.collectors.threedep.spec import ThreeDEPDatasetSpec
from datadongle.core.engine import Engine

logger = logging.getLogger(__name__)

Mode = Literal["full", "incremental"]


def run_threedep_collection(
    reader: ThreeDEPReader,
    spec: ThreeDEPDatasetSpec,
    engine: Engine,
    tracker: Any | None = None,
    *,
    mode: Mode = "incremental",
) -> dict[str, Any]:
    """Collect the 3DEP tiles covering ``spec.bbox`` into one target table.

    Ensures the table, reads the ``source_tile`` high-water mark (incremental
    only), then collects each remaining 1-degree tile into its own write
    session. A tile missing at the source (ocean/gap) is counted and skipped; a
    failing tile is reported and, under incremental, stops the run so the next
    one resumes there. Returns a summary dict with per-tile counts and errors.
    """
    target = reader.target(spec)
    schema = reader.schema(spec)
    write_mode = reader.write_mode(spec, mode=mode)
    engine.ensure_table(target, schema, write_mode)

    names = reader.tiles(spec)
    summary: dict[str, Any] = {
        "spec_name": spec.name,
        "mode": mode,
        "product": spec.product,
        "tiles_total": len(names),
        "tiles_collected": 0,
        "tiles_skipped_present": 0,
        "tiles_missing_at_source": 0,
        "total_rows_staged": 0,
        "total_rows_merged": 0,
        "errors": [],
    }

    if mode == "incremental":
        since = engine.read_high_water_mark(target, reader.cursor_spec(spec))
        if since is not None:
            remaining = [n for n in names if n > since.value]
            summary["tiles_skipped_present"] = len(names) - len(remaining)
            logger.info(
                "Resuming %s from high-water tile %s: %d of %d tile(s) to collect",
                target,
                since.value,
                len(remaining),
                len(names),
            )
            names = remaining

    for name in names:
        narrowed = _narrow(spec, name)
        try:
            if not reader.available(narrowed):
                logger.warning("Tile %s not available at source; skipping", name)
                summary["tiles_missing_at_source"] += 1
                continue
            staged, merged = _collect_one(
                reader, narrowed, engine, target, schema, write_mode, tracker
            )
            summary["tiles_collected"] += 1
            summary["total_rows_staged"] += staged
            summary["total_rows_merged"] += merged
        except Exception as e:  # noqa: BLE001 - reported per tile
            logger.error("3DEP %s tile=%s failed: %s", spec.name, name, e)
            summary["errors"].append({"tile": name, "error": str(e)})
            if mode == "incremental":
                logger.error(
                    "Stopping the incremental run so the next one resumes at %s "
                    "(collecting later tiles would advance the high-water mark past it).",
                    name,
                )
                break

    logger.info("3DEP collection complete for %r: %s", spec.name, summary)
    return summary


def _narrow(spec: ThreeDEPDatasetSpec, name: str) -> ThreeDEPDatasetSpec:
    """A copy of ``spec`` scoped to a single 1-degree tile."""
    return dataclasses.replace(spec, tiles=[name])


def _collect_one(
    reader: ThreeDEPReader,
    spec: ThreeDEPDatasetSpec,
    engine: Engine,
    target,
    schema,
    write_mode,
    tracker: Any | None,
) -> tuple[int, int]:
    """Collect one 1-degree tile into its own write session."""
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

    return ws.rows_staged, ws.rows_merged
