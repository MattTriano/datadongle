"""Unit tests for BikeIndexReader (mocked client, no network)."""

from __future__ import annotations

import json

from datadongle.collectors.bike_index.reader import BikeIndexReader
from datadongle.collectors.bike_index.spec import BikeIndexDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.reader import SourceReader
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2, Append
from datadongle.load.driver import run_collection

from .conftest import make_detail_bike, make_search_bike

# ------------------------------------------------------------------ protocol / metadata


def test_conforms_to_source_reader_protocol(reader):
    assert isinstance(reader, SourceReader)


def test_source_name():
    assert BikeIndexReader.source == "bike_index"


def test_target_and_dataset_id(reader, spec):
    assert reader.dataset_id(spec) == "test_stolen_bikes"
    assert reader.target(spec) == TableRef("stolen_bikes", "raw_data")


def test_schema_columns_and_types(reader, spec):
    schema = reader.schema(spec)
    by_name = {c.name: c for c in schema.columns}

    # id is the not-null natural key; a sampling of the type mapping
    assert by_name["id"].type is ColumnType.INTEGER
    assert by_name["id"].nullable is False
    assert by_name["date_stolen"].type is ColumnType.BIGINT
    assert by_name["stolen"].type is ColumnType.BOOLEAN
    assert by_name["latitude"].type is ColumnType.DOUBLE
    assert by_name["frame_colors"].type is ColumnType.JSON
    assert by_name["components"].type is ColumnType.JSON

    # Search and detail rows must match the declared schema exactly.
    detail_keys = set(reader._flatten_detail(make_detail_bike(1)).keys())
    assert set(schema.column_names()) == detail_keys


def test_schema_has_no_bookkeeping_columns(reader, spec):
    # Every column enters the SCD2 content hash (no metadata=True columns);
    # the engine adds record_hash/valid_from/valid_to/ingested_at itself.
    assert reader.schema(spec).metadata_column_names() == set()


def test_write_mode_scd2_when_entity_key(reader, spec):
    assert reader.write_mode(spec, mode="full") == SCD2(entity_key=["id"])


def test_write_mode_append_without_entity_key(reader):
    spec = BikeIndexDatasetSpec(name="t", target_table="t", entity_key=[])
    assert reader.write_mode(spec, mode="incremental") == Append()


def test_cursor_spec(reader, spec):
    assert reader.cursor_spec(spec) == CursorSpec("date_stolen", "id")


# ------------------------------------------------------------------ strictly-after filter


def test_is_after_none_keeps_everything():
    assert BikeIndexReader._is_after(make_search_bike(1, date_stolen=50), None) is True


def test_is_after_filters_on_cursor_value():
    since = Cursor("100", None)
    assert BikeIndexReader._is_after(make_search_bike(1, date_stolen=200), since) is True
    assert BikeIndexReader._is_after(make_search_bike(2, date_stolen=100), since) is False
    assert BikeIndexReader._is_after(make_search_bike(3, date_stolen=50), since) is False


def test_is_after_uses_tiebreak_on_equal_value():
    since = Cursor("100", "5")
    # same second, larger id -> after; same/smaller id -> not
    assert BikeIndexReader._is_after(make_search_bike(6, date_stolen=100), since) is True
    assert BikeIndexReader._is_after(make_search_bike(5, date_stolen=100), since) is False
    assert BikeIndexReader._is_after(make_search_bike(4, date_stolen=100), since) is False


def test_is_after_keeps_rows_without_date_stolen():
    assert BikeIndexReader._is_after(make_search_bike(1, date_stolen=None), Cursor("100")) is True


# ------------------------------------------------------------------ extract_cursor


def test_extract_cursor_returns_max_with_tiebreak(reader):
    batch = [
        {"date_stolen": 100, "id": 9},
        {"date_stolen": 300, "id": 2},
        {"date_stolen": 300, "id": 7},
    ]
    assert reader.extract_cursor(batch) == Cursor("300", "7")


def test_extract_cursor_none_when_empty(reader):
    assert reader.extract_cursor([]) is None
    assert reader.extract_cursor([{"date_stolen": None, "id": 1}]) is None


# ------------------------------------------------------------------ flattening


def test_flatten_search_unpacks_coordinates(reader):
    row = reader._flatten_search(make_search_bike(1, stolen_coordinates=[41.88, -87.63]))
    assert row["stolen_coordinates_lat"] == 41.88
    assert row["stolen_coordinates_lon"] == -87.63


def test_flatten_search_handles_missing_coordinates(reader):
    row = reader._flatten_search(make_search_bike(1, stolen_coordinates=None))
    assert row["stolen_coordinates_lat"] is None
    assert row["stolen_coordinates_lon"] is None


def test_flatten_search_detail_fields_are_none(reader):
    row = reader._flatten_search(make_search_bike(1))
    assert row["latitude"] is None
    assert row["theft_description"] is None
    assert row["components"] is None
    assert row["frame_material_slug"] is None


def test_flatten_search_serializes_frame_colors(reader):
    row = reader._flatten_search(make_search_bike(1, frame_colors=["Red", "Black"]))
    assert row["frame_colors"] == '["Red", "Black"]'


def test_flatten_detail_populates_stolen_record(reader):
    row = reader._flatten_detail(make_detail_bike(1))
    assert row["latitude"] == 41.89
    assert row["longitude"] == -87.62
    assert row["theft_description"] == "Locked outside"
    assert row["police_report_number"] == "CPD-12345"


def test_flatten_detail_populates_scalars(reader):
    row = reader._flatten_detail(make_detail_bike(1))
    assert row["frame_material_slug"] == "aluminum"
    assert row["manufacturer_id"] == 42
    assert row["registration_created_at"] == 1700000100


def test_flatten_detail_serializes_nested_arrays(reader):
    row = reader._flatten_detail(make_detail_bike(1))
    assert json.loads(row["components"]) == [{"type": "wheel", "brand": "Shimano"}]
    assert json.loads(row["public_images"]) == []


def test_flatten_detail_handles_missing_stolen_record(reader):
    bike = make_detail_bike(1)
    bike["stolen_record"] = None
    row = reader._flatten_detail(bike)
    assert row["latitude"] is None
    assert row["theft_description"] is None


def test_search_and_detail_share_keys(reader):
    search_row = reader._flatten_search(make_search_bike(1))
    detail_row = reader._flatten_detail(make_detail_bike(1))
    assert set(search_row.keys()) == set(detail_row.keys())


# ------------------------------------------------------------------ read (inline enrichment)


def test_read_enriches_each_new_bike(reader, mock_client, spec):
    mock_client.search_all.return_value = iter([[make_search_bike(1), make_search_bike(2)]])
    mock_client.get_bike.side_effect = [make_detail_bike(1), make_detail_bike(2)]

    batches = list(reader.read(spec, since=None))

    rows = [r for b in batches for r in b]
    assert [r["id"] for r in rows] == [1, 2]
    # detail columns populated -> get_bike was used, not the summary fallback
    assert all(r["theft_description"] == "Locked outside" for r in rows)
    assert mock_client.get_bike.call_count == 2


def test_read_filters_before_enriching(reader, mock_client, spec):
    """Rows at or before the cursor are skipped without a get_bike call."""
    mock_client.search_all.return_value = iter(
        [[make_search_bike(1, date_stolen=50), make_search_bike(2, date_stolen=200)]]
    )
    mock_client.get_bike.side_effect = [make_detail_bike(2)]

    batches = list(reader.read(spec, since=Cursor("100")))

    rows = [r for b in batches for r in b]
    assert [r["id"] for r in rows] == [2]
    mock_client.get_bike.assert_called_once_with(2)


def test_read_batches_by_batch_size(mock_client, spec):
    reader = BikeIndexReader(client=mock_client, batch_size=2)
    bikes = [make_search_bike(i, date_stolen=1700000000 + i) for i in range(5)]
    mock_client.search_all.return_value = iter([bikes])
    mock_client.get_bike.side_effect = [make_detail_bike(b["id"]) for b in bikes]

    batches = list(reader.read(spec, since=None))

    # 5 rows at batch_size=2 -> 2, 2, 1
    assert [len(b) for b in batches] == [2, 2, 1]


def test_read_falls_back_to_summary_on_detail_failure(reader, mock_client, spec):
    mock_client.search_all.return_value = iter([[make_search_bike(1)]])
    mock_client.get_bike.side_effect = Exception("timeout")

    batches = list(reader.read(spec, since=None))

    row = batches[0][0]
    assert row["id"] == 1
    assert row["title"] == "Bike 1"  # summary field present
    assert row["theft_description"] is None  # detail field null


# ------------------------------------------------ integration through the driver


class _FakeWriteSession:
    def __init__(self):
        self.batches: list[list[dict]] = []
        self.rows_staged = 0
        self.rows_merged = 0
        self.rows_invalidated = 0

    def write_batch(self, rows):
        self.batches.append(rows)
        self.rows_staged += len(rows)
        self.rows_merged = self.rows_staged
        return len(rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def __init__(self, hwm=None):
        self._hwm = hwm
        self.session = _FakeWriteSession()
        self.calls = {}

    def ensure_table(self, target, schema, mode):
        self.calls["ensure"] = (target, schema, mode)

    # unused by the driver in these tests
    def query(self, sql, params=None): ...
    def table_exists(self, target): return True
    def table_columns(self, target): return set()
    def geometry_columns(self, target): return {}

    def read_high_water_mark(self, target, cursor):
        self.calls["hwm"] = (target, cursor)
        return self._hwm

    def open_write(self, target, schema, mode):
        self.calls["open"] = (target, schema, mode)
        return self.session


def test_reader_drives_incremental_through_run_collection(reader, mock_client, spec):
    page = [
        make_search_bike(1, date_stolen=1700000001),
        make_search_bike(2, date_stolen=1700000002),
    ]
    mock_client.search_all.return_value = iter([page])
    mock_client.get_bike.side_effect = [
        make_detail_bike(1, date_stolen=1700000001),
        make_detail_bike(2, date_stolen=1700000002),
    ]

    engine = _FakeEngine(hwm=Cursor("1700000000", "0"))
    summary = run_collection(reader, spec, engine, mode="incremental")

    # HWM read from the target with the bike-index cursor
    target, cursor = engine.calls["hwm"]
    assert target == TableRef("stolen_bikes", "raw_data")
    assert cursor == CursorSpec("date_stolen", "id")

    # both ensure_table and open_write got the SCD2 write mode
    assert isinstance(engine.calls["ensure"][2], SCD2)
    assert isinstance(engine.calls["open"][2], SCD2)

    # rows were enriched and written; HWM advanced to the max (date_stolen, id) seen
    written = [r for batch in engine.session.batches for r in batch]
    assert len(written) == 2
    assert all(r["theft_description"] == "Locked outside" for r in written)
    assert summary["rows_merged"] == 2
    assert summary["high_water_mark"] == "1700000002|2"


def test_reader_full_read_ignores_hwm(reader, mock_client, spec):
    mock_client.search_all.return_value = iter([[make_search_bike(1, date_stolen=50)]])
    mock_client.get_bike.side_effect = [make_detail_bike(1, date_stolen=50)]

    engine = _FakeEngine(hwm=Cursor("100", "0"))
    summary = run_collection(reader, spec, engine, mode="full")

    # full read never consults the high-water mark
    assert "hwm" not in engine.calls
    assert summary["rows_merged"] == 1
