"""Unit tests for OSMReader (mocked Overpass client, no network)."""

from __future__ import annotations

import json
from typing import Any, cast

from datadongle.collectors.osm.client import OSMClient
from datadongle.collectors.osm.query import OverpassAPIQuery
from datadongle.collectors.osm.reader import (
    OSMReader,
    _element_to_row,
    _to_overpass_timestamp,
)
from datadongle.collectors.osm.spec import OSMDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2
from datadongle.geo import BBox
from datadongle.load.driver import run_collection

BBOX = BBox(41.62, -87.97, 42.05, -87.5)


def _query() -> OverpassAPIQuery:
    return OverpassAPIQuery(
        element_types=["node", "way"],
        tag_filters=[{"amenity": "cafe"}],
    ).for_bbox(BBOX)


def _spec(**kw) -> OSMDatasetSpec:
    base: dict[str, Any] = dict(name="chi_cafes", target_table="chi_cafes", query=_query())
    base.update(kw)
    return OSMDatasetSpec(**base)


class _FakeClient:
    """Stands in for OSMClient: records the fetch call, returns a canned payload."""

    def __init__(self, elements):
        self._elements = elements
        self.calls = []

    def fetch(self, query, date_filter=None):
        self.calls.append({"query": query, "date_filter": date_filter})
        return {"elements": self._elements}


def _as_client(fake: _FakeClient) -> OSMClient:
    """_FakeClient duck-types OSMClient rather than subclassing it."""
    return cast(OSMClient, fake)


def _fake_client(reader: OSMReader) -> _FakeClient:
    """The fake behind a reader, typed so its recorded calls are visible."""
    return cast(_FakeClient, reader.client)


# ------------------------------------------------------------------ metadata bits


def test_target_and_dataset_id():
    reader = OSMReader(client=_as_client(_FakeClient([])))
    spec = _spec(target_schema="raw_data")
    assert reader.dataset_id(spec) == "chi_cafes"
    assert reader.target(spec) == TableRef("chi_cafes", "raw_data")


def test_schema_fixed_columns_plus_promoted_tags():
    reader = OSMReader(client=_as_client(_FakeClient([])))
    schema = reader.schema(_spec(promoted_tags=["name", "addr:street"]))

    assert schema.column_names() == [
        "osm_type",
        "osm_id",
        "osm_version",
        "osm_timestamp",
        "geom",
        "tags",
        "node_ids",
        "name",
        "addr_street",
    ]
    by_name = {c.name: c for c in schema.columns}
    assert by_name["osm_id"].type is ColumnType.BIGINT
    assert by_name["osm_id"].nullable is False
    assert by_name["tags"].type is ColumnType.JSON
    assert by_name["node_ids"].type is ColumnType.JSON
    assert by_name["geom"].type is ColumnType.GEOMETRY
    assert schema.geometry["geom"].srid == 4326
    assert by_name["name"].type is ColumnType.TEXT
    # osm_version/osm_timestamp are stored but excluded from the SCD2 hash
    assert schema.metadata_column_names() == {"osm_version", "osm_timestamp"}


def test_write_mode_full_enables_invalidate_missing():
    reader = OSMReader(client=_as_client(_FakeClient([])))
    assert reader.write_mode(_spec(), mode="full") == SCD2(
        entity_key=["osm_type", "osm_id"], invalidate_missing=True
    )


def test_write_mode_incremental_disables_invalidate_missing():
    reader = OSMReader(client=_as_client(_FakeClient([])))
    assert reader.write_mode(_spec(), mode="incremental") == SCD2(
        entity_key=["osm_type", "osm_id"], invalidate_missing=False
    )


def test_cursor_spec_is_ingested_at():
    reader = OSMReader(client=_as_client(_FakeClient([])))
    assert reader.cursor_spec(_spec()) == CursorSpec(column="ingested_at")


def test_extract_cursor_is_always_none():
    # The HWM is the engine-stamped ingested_at, read back from the table.
    reader = OSMReader(client=_as_client(_FakeClient([])))
    assert reader.extract_cursor([{"osm_id": 1}]) is None
    assert reader.extract_cursor([]) is None


# ------------------------------------------------------------------ overpass timestamp


def test_to_overpass_timestamp_naive_iso_treated_as_utc():
    assert _to_overpass_timestamp("2026-07-01T12:34:56.789012") == "2026-07-01T12:34:56Z"


def test_to_overpass_timestamp_offset_converted_to_utc():
    assert _to_overpass_timestamp("2026-07-01T09:00:00-03:00") == "2026-07-01T12:00:00Z"


# ------------------------------------------------------------------ element -> row


def test_element_to_row_transforms():
    spec = _spec(promoted_tags=["name", "addr:street"])
    element = {
        "type": "node",
        "id": 42,
        "version": 3,
        "timestamp": "2026-01-01T00:00:00Z",
        "lon": -87.6,
        "lat": 41.8,
        "tags": {"amenity": "cafe", "name": "Bourgeois Pig", "addr:street": "Fullerton"},
    }
    row = _element_to_row(element, spec)

    assert row["osm_type"] == "node"
    assert row["osm_id"] == 42
    assert row["osm_version"] == 3
    assert row["osm_timestamp"] == "2026-01-01T00:00:00Z"
    # geometry assembled as EWKT (SRID-tagged) so the PostGIS cast accepts it
    assert row["geom"] == "SRID=4326;POINT (-87.6 41.8)"
    # tags JSON-encoded, keys sorted for a stable content hash
    assert row["tags"] == json.dumps(
        {"addr:street": "Fullerton", "amenity": "cafe", "name": "Bourgeois Pig"},
        sort_keys=True,
        ensure_ascii=False,
    )
    # promoted columns lifted under their normalized names; original key kept for lookup
    assert row["name"] == "Bourgeois Pig"
    assert row["addr_street"] == "Fullerton"


def test_element_to_row_node_ids_json_encoded():
    row = _element_to_row({"type": "way", "id": 7, "nodes": [1, 2, 3], "geometry": []}, _spec())
    assert row["node_ids"] == "[1, 2, 3]"


def test_element_to_row_missing_geometry_and_nodes_are_none():
    # A relation that isn't a multipolygon yields no geometry; no nodes -> None.
    row = _element_to_row({"type": "relation", "id": 9, "tags": {}}, _spec())
    assert row["geom"] is None
    assert row["node_ids"] is None
    assert row["tags"] == "{}"


def test_element_to_row_missing_promoted_tag_is_none():
    row = _element_to_row(
        {"type": "node", "id": 1, "lon": 0.0, "lat": 0.0, "tags": {"amenity": "cafe"}},
        _spec(promoted_tags=["name"]),
    )
    assert row["name"] is None


# ------------------------------------------------------------------ read path


def test_read_full_sends_no_date_filter_and_batches():
    elements = [
        {"type": "node", "id": i, "lon": 0.1 * i, "lat": 0.1 * i, "tags": {"amenity": "cafe"}}
        for i in range(5)
    ]
    client = _FakeClient(elements)
    reader = OSMReader(client=_as_client(client), batch_size=2)

    batches = list(reader.read(_spec(), since=None))

    # 5 elements chunked into 2 + 2 + 1
    assert [len(b) for b in batches] == [2, 2, 1]
    assert client.calls[0]["date_filter"] is None
    all_rows = [r for b in batches for r in b]
    assert [r["osm_id"] for r in all_rows] == [0, 1, 2, 3, 4]


def test_read_incremental_passes_overpass_date_filter():
    client = _FakeClient([{"type": "node", "id": 1, "lon": 0.0, "lat": 0.0, "tags": {}}])
    reader = OSMReader(client=_as_client(client))

    list(reader.read(_spec(), since=Cursor("2026-07-01T00:00:00.000000")))

    assert client.calls[0]["date_filter"] == "2026-07-01T00:00:00Z"
    # the reader hands the client the spec's bound query
    assert client.calls[0]["query"] is not None


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
    def table_exists(self, target):
        return True

    def table_columns(self, target):
        return set()

    def geometry_columns(self, target):
        return {}

    def read_high_water_mark(self, target, cursor):
        self.calls["hwm"] = (target, cursor)
        return self._hwm

    def open_write(self, target, schema, mode):
        self.calls["open"] = (target, schema, mode)
        return self.session


def test_osm_reader_drives_full_through_run_collection():
    elements = [
        {"type": "node", "id": 1, "lon": 0.0, "lat": 0.0, "tags": {"amenity": "cafe"}},
        {"type": "node", "id": 2, "lon": 1.0, "lat": 1.0, "tags": {"amenity": "cafe"}},
    ]
    reader = OSMReader(client=_as_client(_FakeClient(elements)))
    engine = _FakeEngine()
    spec = _spec(target_schema="raw_data")

    summary = run_collection(reader, spec, engine, mode="full")

    # full read never consults the HWM
    assert "hwm" not in engine.calls
    # both ensure_table and open_write get the invalidate_missing SCD2 policy
    assert engine.calls["ensure"][2] == SCD2(
        entity_key=["osm_type", "osm_id"], invalidate_missing=True
    )
    assert engine.calls["open"][2].invalidate_missing is True

    written = [r for batch in engine.session.batches for r in batch]
    assert [r["osm_id"] for r in written] == [1, 2]
    assert summary["rows_merged"] == 2


def test_osm_reader_drives_incremental_and_uses_table_hwm():
    reader = OSMReader(
        client=_as_client(
            _FakeClient([{"type": "node", "id": 3, "lon": 0.0, "lat": 0.0, "tags": {}}])
        )
    )
    engine = _FakeEngine(hwm=Cursor("2026-06-01T00:00:00.000000"))
    spec = _spec(target_schema="raw_data")

    summary = run_collection(reader, spec, engine, mode="incremental")

    # HWM read from the target with the ingested_at cursor
    target, cursor = engine.calls["hwm"]
    assert target == TableRef("chi_cafes", "raw_data")
    assert cursor == CursorSpec(column="ingested_at")
    # incremental run does not invalidate missing entities
    assert engine.calls["open"][2].invalidate_missing is False
    # the table's HWM became the Overpass (newer:) floor
    assert _fake_client(reader).calls[0]["date_filter"] == "2026-06-01T00:00:00Z"
    assert summary["rows_merged"] == 1
