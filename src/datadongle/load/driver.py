"""The shared collection driver.

``run_collection`` is the full-vs-incremental orchestration that used to be
copied into every collector. It is source- and engine-agnostic: it drives a
``SourceReader`` into an ``Engine`` under a chosen mode, reading the
high-water mark from the target table (never a run log) and recording the run
via an optional tracker.
"""

from __future__ import annotations

import logging
from contextlib import nullcontext
from typing import Any, Literal

from datadongle.core.cursor import Cursor
from datadongle.core.engine import Engine
from datadongle.core.reader import SourceReader
from datadongle.core.write_mode import SCD2

logger = logging.getLogger(__name__)

Mode = Literal["full", "incremental"]


def run_collection(
    reader: SourceReader,
    spec: Any,
    engine: Engine,
    tracker: Any | None = None,
    *,
    mode: Mode = "incremental",
) -> dict[str, Any]:
    """Collect ``spec`` from ``reader`` into ``engine`` under ``mode``.

    - ``full``: read the entire source (``since=None``).
    - ``incremental``: read the target's high-water mark and read only newer
      rows. A source with no ``cursor_spec`` (not incrementally queryable)
      falls back to a full read.

    Returns a summary dict. ``tracker`` (if given) exposes a
    ``track(source, dataset_id, target_table)`` context manager yielding a run
    object whose row/HWM fields are populated here.
    """
    target = reader.target(spec)
    schema = reader.schema(spec)
    write_mode = reader.write_mode(spec, mode=mode)

    if (
        mode == "incremental"
        and isinstance(write_mode, SCD2)
        and write_mode.invalidate_missing
    ):
        raise ValueError(
            "SCD2(invalidate_missing=True) requires a full read: an incremental "
            "pull cannot observe which entities are absent. Use mode='full'."
        )

    engine.ensure_table(target, schema, write_mode)

    since: Cursor | None = None
    if mode == "incremental":
        cursor_spec = reader.cursor_spec(spec)
        if cursor_spec is None:
            logger.info(
                "%s/%s is not incrementally queryable; running a full read.",
                reader.source,
                reader.dataset_id(spec),
            )
        else:
            since = engine.read_high_water_mark(target, cursor_spec)
            logger.info("Resuming %s from high-water mark %s", target, since)

    if tracker is not None:
        run_ctx = tracker.track(reader.source, reader.dataset_id(spec), str(target))
    else:
        run_ctx = nullcontext(None)

    high = since
    with run_ctx as run, engine.open_write(target, schema, write_mode) as ws:
        for batch in reader.read(spec, since=since):
            ws.write_batch(batch)
            batch_cursor = reader.extract_cursor(batch)
            if batch_cursor is not None and (
                high is None or batch_cursor.sort_key > high.sort_key
            ):
                high = batch_cursor
        if run is not None:
            run.rows_staged = ws.rows_staged
            run.rows_merged = ws.rows_merged
            run.rows_ingested = ws.rows_merged
            run.high_water_mark = high.encode() if high else None

    summary = {
        "source": reader.source,
        "dataset_id": reader.dataset_id(spec),
        "mode": mode,
        "rows_staged": ws.rows_staged,
        "rows_merged": ws.rows_merged,
        "rows_invalidated": ws.rows_invalidated,
        "high_water_mark": high.encode() if high else None,
    }
    logger.info("Collection complete: %s", summary)
    return summary
