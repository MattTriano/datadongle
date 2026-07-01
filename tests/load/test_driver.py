"""Unit tests for the shared collection driver, using in-memory fakes.

No engine, database, or heavy dependency is involved — this exercises the
full-vs-incremental orchestration and high-water-mark flow in isolation.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace

from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import Scd2
from datadongle.load.driver import run_collection


class FakeWriteSession:
    def __init__(self):
        self.batches: list[list[dict]] = []
        self.rows_staged = 0
        self.rows_merged = 0
        self.rows_invalidated = 0

    def write_batch(self, rows):
        self.batches.append(rows)
        self.rows_staged += len(rows)
        # Fake merge: treat every staged row as merged.
        self.rows_merged = self.rows_staged
        return len(rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeEngine:
    def __init__(self, hwm: Cursor | None = None):
        self._hwm = hwm
        self.ensured: list[TableRef] = []
        self.hwm_reads: list[tuple[TableRef, CursorSpec]] = []
        self.session = FakeWriteSession()
        self.open_write_args = None

    def ensure_table(self, target, schema):
        self.ensured.append(target)

    def read_high_water_mark(self, target, cursor):
        self.hwm_reads.append((target, cursor))
        return self._hwm

    def open_write(self, target, schema, mode):
        self.open_write_args = (target, schema, mode)
        return self.session

    # unused by the driver in these tests
    def query(self, sql, params=None): ...
    def table_exists(self, target): return True
    def table_columns(self, target): return set()
    def geometry_columns(self, target): return {}


class FakeReader:
    source = "fake"

    def __init__(self, pages, cursor_spec=CursorSpec("updated_at", "id")):
        self._pages = pages
        self._cursor_spec = cursor_spec
        self.read_since = "unset"

    def dataset_id(self, spec):
        return spec["id"]

    def target(self, spec):
        return TableRef(spec["table"], spec["schema"])

    def schema(self, spec):
        return TableSchema(columns=[Column("id", ColumnType.TEXT)], entity_key=["id"])

    def write_mode(self, spec):
        return Scd2(entity_key=["id"])

    def cursor_spec(self, spec):
        return self._cursor_spec

    def read(self, spec, *, since):
        self.read_since = since
        yield from self._pages

    def extract_cursor(self, batch):
        vals = [(r["updated_at"], str(r["id"])) for r in batch if r.get("updated_at")]
        if not vals:
            return None
        best = max(vals)
        return Cursor(value=best[0], tiebreak=best[1])


class FakeTracker:
    def __init__(self):
        self.runs = []

    @contextlib.contextmanager
    def track(self, source, dataset_id, target_table, metadata=None):
        run = SimpleNamespace(
            rows_staged=0, rows_merged=0, rows_ingested=0, high_water_mark=None
        )
        self.runs.append((source, dataset_id, target_table, run))
        yield run


SPEC = {"id": "ds1", "table": "permits", "schema": "raw_data"}
PAGES = [
    [{"id": 1, "updated_at": "2024-01-01"}, {"id": 2, "updated_at": "2024-01-02"}],
    [{"id": 3, "updated_at": "2024-01-03"}],
]


def test_full_mode_reads_everything_and_ignores_hwm():
    engine = FakeEngine(hwm=Cursor("2099-01-01", "999"))
    reader = FakeReader(PAGES)
    summary = run_collection(reader, SPEC, engine, mode="full")

    assert reader.read_since is None  # full read never consults the HWM
    assert engine.hwm_reads == []
    assert engine.ensured == [TableRef("permits", "raw_data")]
    assert summary["rows_merged"] == 3
    assert summary["high_water_mark"] == "2024-01-03|3"


def test_incremental_reads_hwm_and_passes_since():
    hwm = Cursor("2023-12-31", "0")
    engine = FakeEngine(hwm=hwm)
    reader = FakeReader(PAGES)
    summary = run_collection(reader, SPEC, engine, mode="incremental")

    assert len(engine.hwm_reads) == 1
    assert reader.read_since == hwm
    assert summary["high_water_mark"] == "2024-01-03|3"


def test_incremental_without_cursor_spec_falls_back_to_full():
    engine = FakeEngine(hwm=Cursor("x", "y"))
    reader = FakeReader(PAGES, cursor_spec=None)
    run_collection(reader, SPEC, engine, mode="incremental")

    assert engine.hwm_reads == []  # nothing to read against
    assert reader.read_since is None


def test_hwm_carries_forward_when_no_new_rows_beat_prior():
    hwm = Cursor("2099-01-01", "999")
    engine = FakeEngine(hwm=hwm)
    reader = FakeReader(PAGES)
    summary = run_collection(reader, SPEC, engine, mode="incremental")
    # Prior HWM is higher than any batch cursor, so it is preserved.
    assert summary["high_water_mark"] == "2099-01-01|999"


def test_tracker_run_is_populated():
    engine = FakeEngine()
    reader = FakeReader(PAGES)
    tracker = FakeTracker()
    run_collection(reader, SPEC, engine, tracker, mode="full")

    assert len(tracker.runs) == 1
    source, dataset_id, target_table, run = tracker.runs[0]
    assert (source, dataset_id, target_table) == ("fake", "ds1", "raw_data.permits")
    assert run.rows_merged == 3
    assert run.high_water_mark == "2024-01-03|3"


def test_open_write_receives_the_readers_write_mode():
    engine = FakeEngine()
    reader = FakeReader(PAGES)
    run_collection(reader, SPEC, engine, mode="full")
    target, schema, mode = engine.open_write_args
    assert isinstance(mode, Scd2)
    assert mode.entity_key == ["id"]
