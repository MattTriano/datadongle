"""CourtListenerReader — protocol conformance, transforms, and end-to-end.

The HTTP boundary is faked (API pages and bulk bz2 bytes served from memory);
the Iceberg arm is hermetic (a tmp-path warehouse) so it runs everywhere. The
end-to-end test exercises the collector's headline flow: bulk backfill, then
incremental API updates into the same table.
"""

from __future__ import annotations

import logging

import pytest

from datadongle.collectors.courtlistener.reader import CourtListenerReader, _tie_key
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.reader import SourceReader
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2, Append
from datadongle.engines.iceberg import IcebergEngine
from datadongle.load.driver import run_collection

from ..common import NoopTracker
from .helpers import API, FakeCourtListenerClient, fake_client, make_bulk_bz2, make_spec


def _reader(**client_kwargs) -> CourtListenerReader:
    return CourtListenerReader(client=FakeCourtListenerClient(**client_kwargs))


# ------------------------------------------------------------------ protocol


def test_is_a_source_reader():
    assert isinstance(_reader(), SourceReader)


def test_cursor_spec_date_modified_with_id_tiebreak():
    reader = _reader()
    assert reader.cursor_spec(make_spec()) == CursorSpec(column="date_modified", tiebreak="id")
    assert reader.cursor_spec(make_spec(cursor_column=None)) is None


# ------------------------------------------- resources without a date_modified

# A Django many-to-many through table: surrogate id, two foreign keys, no
# timestamps. Nothing about it is incrementally queryable.
LINK_COLUMNS = ["id", "opinioncluster_id", "person_id"]
LINK_URL = "https://example.invalid/bulk-data/panel-2024-01-31.csv.bz2"
LINK_EXPORTS = [
    {
        "prefix": "panel",
        "date": "2024-01-31",
        "filename": "panel-2024-01-31.csv.bz2",
        "url": LINK_URL,
        "size": 10,
    }
]


def _link_reader() -> CourtListenerReader:
    rows = [{"id": "1", "opinioncluster_id": "10", "person_id": "20"}]
    return CourtListenerReader(
        client=FakeCourtListenerClient(
            bulk_files={LINK_URL: make_bulk_bz2(rows, columns=LINK_COLUMNS)},
            exports=LINK_EXPORTS,
        )
    )


def _link_spec(**overrides):
    defaults = dict(resource="panel", entity_key=["opinioncluster_id", "person_id"])
    defaults.update(overrides)
    return make_spec(**defaults)


def test_cursor_spec_downgrades_to_full_reads_when_the_column_is_absent(caplog):
    """A through table has no date_modified; filtering on it would be nonsense."""
    reader = _link_reader()

    with caplog.at_level(logging.WARNING):
        assert reader.cursor_spec(_link_spec()) is None

    assert "cannot be read incrementally" in caplog.text


def test_entity_key_naming_a_missing_column_fails_before_any_rows_are_read():
    """SCD2 on a nonexistent key collapses the table into one entity — stop first."""
    reader = _link_reader()

    with pytest.raises(ValueError, match="entity_key") as exc:
        reader.schema(_link_spec(entity_key=["cluster_id"]))

    # The error names what's wrong, what exists, and what to use instead.
    assert "'cluster_id'" in str(exc.value)
    assert "opinioncluster_id" in str(exc.value)
    assert "Suggested entity_key" in str(exc.value)


def test_entity_key_opt_out_skips_validation():
    reader = _link_reader()
    assert reader.write_mode(_link_spec(entity_key=None)) == Append()
    assert reader.schema(_link_spec(entity_key=None)).column_names() == LINK_COLUMNS


def test_a_valid_natural_entity_key_passes_validation():
    reader = _link_reader()
    schema = reader.schema(_link_spec())

    assert schema.column_names() == LINK_COLUMNS
    assert reader.write_mode(_link_spec()) == SCD2(entity_key=["opinioncluster_id", "person_id"])


def test_write_mode_scd2_with_entity_key_else_append():
    reader = _reader()
    assert reader.write_mode(make_spec(), mode="full") == SCD2(entity_key=["id"])
    assert reader.write_mode(make_spec(entity_key=None), mode="full") == Append()


def test_dataset_id_matches_spec():
    assert _reader().dataset_id(make_spec()) == "dockets"


# --------------------------------------------------------------- schema


def test_schema_from_bulk_header():
    schema = _reader().schema(make_spec())
    names = [c.name for c in schema.columns]
    assert names == [
        "id",
        "date_created",
        "date_modified",
        "court_id",
        "case_name",
        "pacer_case_id",
    ]
    assert all(c.type == ColumnType.TEXT for c in schema.columns)
    by_name = {c.name: c for c in schema.columns}
    assert by_name["id"].nullable is False
    # Source timestamps are bookkeeping: excluded from the SCD2 content hash.
    assert by_name["date_created"].metadata is True
    assert by_name["date_modified"].metadata is True
    assert by_name["case_name"].metadata is False


def test_schema_from_api_sample_when_backfill_api():
    schema = _reader().schema(make_spec(backfill="api"))
    names = [c.name for c in schema.columns]
    # Transformed shape: computed fields gone, court URL collapsed to court_id.
    assert "resource_uri" not in names
    assert "absolute_url" not in names
    assert "court_id" in names
    assert "court" not in names
    assert "parties" in names  # nested field survives as a JSON-encoded column


def test_schema_raises_when_api_sample_empty():
    reader = _reader(api_rows={"dockets": []})
    with pytest.raises(ValueError, match="schema cannot be discovered"):
        reader.schema(make_spec(backfill="api"))


def test_bulk_export_missing_raises_with_hint():
    reader = _reader(exports=[])
    with pytest.raises(ValueError, match="No bulk export found"):
        reader.schema(make_spec())


# ------------------------------------------------------------------ transforms


def test_transform_api_row_normalizes_toward_bulk_shape():
    row = {
        "resource_uri": f"{API}/dockets/7/",
        "absolute_url": "/docket/7/x/",
        "id": 7,
        "court": f"{API}/courts/scotus/",
        "case_name": "A v. B",
        "blocked": False,
        "parties": [1, 2],
        "view_count": 3,
        "referred_to": None,
    }
    out = _reader()._transform_api_row(row)
    assert "resource_uri" not in out and "absolute_url" not in out
    assert out["court_id"] == "scotus" and "court" not in out
    assert out["id"] == "7"  # scalars stringified (TEXT columns)
    assert out["blocked"] == "false"
    assert out["parties"] == "[1, 2]"
    assert out["view_count"] == "3"
    assert out["referred_to"] is None


def test_transform_prefers_existing_id_sibling_over_url():
    # v4 sends both a hyperlink and its *_id sibling for some FKs; the sibling
    # wins and the URL field is dropped rather than clobbering it.
    row = {"id": 7, "court": f"{API}/courts/scotus/", "court_id": "scotus"}
    out = _reader()._transform_api_row(row)
    assert out["court_id"] == "scotus"
    assert "court" not in out


def test_transform_canonicalizes_timestamps():
    row = {"id": 1, "date_modified": "2024-02-01T09:30:00Z"}
    out = _reader()._transform_api_row(row)
    assert out["date_modified"] == "2024-02-01 09:30:00+00:00"


def test_unparseable_timestamp_raises_rather_than_corrupting_the_cursor():
    with pytest.raises(ValueError, match="does not parse as an ISO timestamp"):
        _reader()._transform_api_row({"id": 1, "date_modified": "last Tuesday"})


def test_full_bulk_read_parses_backtick_csv_and_normalizes():
    batches = list(_reader().read(make_spec(), since=None))
    rows = [r for b in batches for r in b]
    assert len(rows) == 3
    # Backtick quoting preserved the embedded comma and double-quotes.
    assert rows[0]["case_name"] == 'In re "Complex" Litig., Inc.'
    # Postgres-style timestamp canonicalized; empty string became None.
    assert rows[0]["date_modified"] == "2024-01-01 08:00:00+00:00"
    assert rows[2]["pacer_case_id"] is None


# ------------------------------------------------------------------ cursor


def test_incremental_filters_strictly_after_with_numeric_tiebreak():
    ts = "2024-01-03 08:00:00+00:00"
    reader = _reader(
        api_rows={
            "dockets": [
                {"id": 99, "case_name": "old", "date_modified": ts},
                {"id": 100, "case_name": "tied-later", "date_modified": ts},
                {"id": 5, "case_name": "newer", "date_modified": "2024-01-04 08:00:00+00:00"},
            ]
        }
    )
    since = Cursor(value=ts, tiebreak="99")
    rows = [r for b in reader.read(make_spec(), since=since) for r in b]
    # id 99 is the boundary row (dropped); 100 ties on timestamp but has a
    # numerically greater id (string comparison would wrongly drop it).
    assert [r["id"] for r in rows] == ["100", "5"]


def test_incremental_requests_gte_and_stable_ordering():
    reader = _reader()
    since = Cursor(value="2024-01-03 08:00:00+00:00", tiebreak="3")
    list(reader.read(make_spec(), since=since))
    api_calls = [p for url, p in fake_client(reader).calls if "date_modified__gte" in p]
    assert api_calls, "expected the incremental read to filter server-side"
    assert api_calls[0]["date_modified__gte"] == "2024-01-03 08:00:00+00:00"
    assert api_calls[0]["order_by"] == "date_modified,id"


def test_extract_cursor_max_by_timestamp_then_numeric_id():
    batch = [
        {"id": "9", "date_modified": "2024-01-03 08:00:00+00:00"},
        {"id": "10", "date_modified": "2024-01-03 08:00:00+00:00"},
        {"id": "2", "date_modified": "2024-01-01 08:00:00+00:00"},
    ]
    cursor = _reader().extract_cursor(batch)
    assert cursor == Cursor(value="2024-01-03 08:00:00+00:00", tiebreak="10")
    assert _reader().extract_cursor([]) is None
    assert _reader().extract_cursor([{"id": "1", "date_modified": None}]) is None


def test_tie_key_orders_numerically_then_lexically():
    assert _tie_key("100") > _tie_key("99")
    assert _tie_key("scotus") > _tie_key("ca9")  # slug ids fall back to strings
    assert _tie_key("99") < _tie_key("ca9")  # numeric ids sort before slugs


# ----------------------------------------------------- end-to-end (Iceberg)


@pytest.fixture
def engine(tmp_path):
    return IcebergEngine(str(tmp_path / "warehouse"))


def test_bulk_backfill_then_api_incremental_round_trip(engine):
    """The headline flow: bulk seed, SCD2 no-op re-run, API increments."""
    spec = make_spec()
    reader = _reader()
    target = reader.target(spec)

    # Full load from the bulk file: three dockets.
    summary = run_collection(reader, spec, engine, mode="full")
    assert summary["rows_merged"] == 3
    assert summary["high_water_mark"].startswith("2024-01-03 08:00:00+00:00")
    assert len(engine.read_current(target)) == 3

    # Incremental: the API serves docket 2 (updated case name) and docket 4
    # (new); docket 1 sits below the high-water mark and is filtered out.
    summary = run_collection(reader, spec, engine, mode="incremental")
    assert summary["rows_staged"] == 2
    assert summary["rows_merged"] == 2
    assert summary["high_water_mark"].startswith("2024-02-02 10:00:00+00:00")

    current = engine.read_current(target)
    assert len(current) == 4
    roe = current[current["id"] == "2"]
    assert roe["case_name"].iloc[0] == "Roe v. Wade (amended)"
    # The API's `parties` field was projected away: only bulk columns exist.
    assert "parties" not in current.columns
    # Docket 2 has two versions (bulk seed + API update); 3 + 2 = 5 total.
    assert len(engine.read_history(target)) == 5

    # Re-running the increment is a no-op: the HWM now covers both API rows.
    summary = run_collection(reader, spec, engine, mode="incremental")
    assert summary["rows_staged"] == 0


def test_api_backfill_full_read(engine):
    spec = make_spec(backfill="api")
    reader = _reader()
    summary = run_collection(reader, spec, engine, mode="full")
    assert summary["rows_merged"] == 3  # all canned API rows, walked by id
    current = engine.read_current(reader.target(spec))
    assert set(current["id"]) == {"1", "2", "4"}
    assert "court_id" in current.columns


def test_tracker_records_the_run(engine):
    tracker = NoopTracker()
    reader = _reader()
    run_collection(reader, make_spec(), engine, tracker=tracker, mode="full")
    assert len(tracker.runs) == 1
    dataset_id, run = tracker.runs[0]
    assert dataset_id == "dockets"
    assert run.rows_staged == 3
    assert run.high_water_mark.startswith("2024-01-03 08:00:00+00:00")
