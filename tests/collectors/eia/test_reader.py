"""EIAReader — protocol conformance, transforms, and end-to-end collection.

The HTTP boundary is faked (rows served from memory); the Iceberg arm is
hermetic (a tmp-path warehouse) so it runs everywhere. Postgres is exercised by
the shared conformance suite, not here.
"""

from __future__ import annotations

import pytest

from datadongle.collectors.eia.reader import EIAReader
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.reader import SourceReader
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2, Append
from datadongle.engines.iceberg import IcebergEngine
from datadongle.load.driver import run_collection

from ..common import NoopTracker
from .helpers import FakeEIAClient, fake_client, make_spec


def _reader(rows=None) -> EIAReader:
    return EIAReader(client=FakeEIAClient(rows=rows))


# ------------------------------------------------------------------ protocol


def test_is_a_source_reader():
    assert isinstance(_reader(), SourceReader)


def test_cursor_spec_is_period():
    assert _reader().cursor_spec(make_spec()) == CursorSpec(column="period")


def test_write_mode_scd2_with_entity_key_else_append():
    reader = _reader()
    assert reader.write_mode(make_spec(), mode="full") == SCD2(
        entity_key=["stateid", "sectorid", "period"]
    )
    assert reader.write_mode(make_spec(entity_key=None), mode="full") == Append()


# --------------------------------------------------------------- schema/types


def test_schema_types_and_normalized_names():
    schema = _reader().schema(make_spec())
    types = {c.name: c.type for c in schema.columns}

    # camelCase and hyphens normalized so nothing needs quoting.
    assert set(types) == {
        "period",
        "stateid",
        "statedescription",
        "sectorid",
        "sectorname",
        "price",
        "price_units",
    }
    # Only the requested measure is DOUBLE; everything else is TEXT.
    assert types["price"] == ColumnType.DOUBLE
    assert types["price_units"] == ColumnType.TEXT
    assert types["statedescription"] == ColumnType.TEXT
    # period is the non-nullable cursor column.
    period = next(c for c in schema.columns if c.name == "period")
    assert period.nullable is False


def test_schema_raises_when_no_rows():
    with pytest.raises(ValueError, match="schema cannot be discovered"):
        _reader(rows=[]).schema(make_spec())


# ------------------------------------------------------------------ transforms


def test_read_normalizes_and_casts_measures():
    batches = list(_reader().read(make_spec(), since=None))
    rows = [r for b in batches for r in b]

    assert len(rows) == 2
    row = rows[0]
    assert row["price"] == 6.71  # string "6.71" cast to float
    assert isinstance(row["price"], float)
    assert row["statedescription"] == "Colorado"  # normalized key
    assert row["price_units"] == "cents per kilowatthour"  # stays text
    assert "price-units" not in row


def test_missing_measure_becomes_none():
    rows = [
        {"period": "2001-01", "stateid": "CO", "sectorid": "RES", "price": None},
        {"period": "2001-02", "stateid": "CO", "sectorid": "RES", "price": ""},
    ]
    out = [r for b in _reader(rows).read(make_spec(), since=None) for r in b]
    assert [r["price"] for r in out] == [None, None]


def test_non_numeric_measure_raises_rather_than_losing_signal():
    rows = [{"period": "2001-01", "stateid": "CO", "sectorid": "RES", "price": "W"}]
    with pytest.raises(ValueError, match="non-numeric value"):
        list(_reader(rows).read(make_spec(), since=None))


def test_incremental_filters_strictly_after_cursor():
    rows = [
        {"period": "2001-01", "stateid": "CO", "sectorid": "RES", "price": "1"},
        {"period": "2001-02", "stateid": "CO", "sectorid": "RES", "price": "2"},
        {"period": "2001-03", "stateid": "CO", "sectorid": "RES", "price": "3"},
    ]
    since = Cursor(value="2001-02")
    out = [r for b in _reader(rows).read(make_spec(), since=since) for r in b]
    # 2001-02 (the boundary) is dropped; only strictly-later 2001-03 remains.
    assert [r["period"] for r in out] == ["2001-03"]


def test_extract_cursor_is_max_period():
    batch = [
        {"period": "2001-01", "price": 1.0},
        {"period": "2001-03", "price": 3.0},
        {"period": "2001-02", "price": 2.0},
    ]
    assert _reader().extract_cursor(batch) == Cursor(value="2001-03")
    assert _reader().extract_cursor([]) is None


# ----------------------------------------------------- end-to-end (Iceberg)


@pytest.fixture
def engine(tmp_path):
    return IcebergEngine(str(tmp_path / "warehouse"))


def _current(engine, target):
    return engine.read_current(target)


def test_full_then_incremental_round_trip(engine):
    spec = make_spec()
    reader = EIAReader(client=FakeEIAClient())
    target = reader.target(spec)

    # Full load: two sector rows for 2001-01.
    summary = run_collection(reader, spec, engine, mode="full")
    assert summary["rows_merged"] == 2
    assert summary["high_water_mark"] == "2001-01"
    assert len(_current(engine, target)) == 2

    # Re-collect unchanged → SCD2 no-op.
    summary = run_collection(reader, spec, engine, mode="incremental")
    assert summary["rows_merged"] == 0
    assert len(_current(engine, target)) == 2

    # A new period appears → incremental picks up only the new row.
    fake_client(reader).rows.append(
        {
            "period": "2001-02",
            "stateid": "CO",
            "stateDescription": "Colorado",
            "sectorid": "RES",
            "sectorName": "residential",
            "price": "7.10",
            "price-units": "cents per kilowatthour",
        }
    )
    summary = run_collection(reader, spec, engine, mode="incremental")
    assert summary["rows_merged"] == 1
    assert summary["high_water_mark"] == "2001-02"
    assert len(_current(engine, target)) == 3


def test_full_refresh_versions_a_revised_value(engine):
    spec = make_spec()
    reader = EIAReader(client=FakeEIAClient())
    target = reader.target(spec)

    run_collection(reader, spec, engine, mode="full")

    # EIA revises 2001-01 RES price 6.71 -> 7.00. Revisions are only caught on a
    # full refresh (period cursor can't see in-place changes to old periods).
    fake_client(reader).rows[0] = {**fake_client(reader).rows[0], "price": "7.00"}
    summary = run_collection(reader, spec, engine, mode="full")

    assert summary["rows_merged"] == 1  # only the changed entity re-versioned
    current = _current(engine, target)
    assert len(current) == 2  # still two current rows
    res = current[current["sectorid"] == "RES"]
    assert res["price"].iloc[0] == 7.00
    # History carries the closed-out prior version too.
    assert len(engine.read_history(target)) == 3


def test_tracker_records_the_run(engine):
    tracker = NoopTracker()
    reader = EIAReader(client=FakeEIAClient())
    run_collection(reader, make_spec(), engine, tracker=tracker, mode="full")

    assert len(tracker.runs) == 1
    dataset_id, run = tracker.runs[0]
    assert dataset_id == "electricity/retail-sales/monthly"
    # rows_staged and the HWM are known before the write session exits; the
    # shared driver populates the tracker there, so rows_merged (set on the
    # merge at context exit) isn't visible to the tracker — assert what is.
    assert run.rows_staged == 2
    assert run.high_water_mark == "2001-01"
