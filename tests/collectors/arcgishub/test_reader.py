"""Behavior tests for ArcGISHubReader (faked HTTP boundary, no network)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from datadongle.collectors.arcgishub.reader import (
    ArcGISHubReader,
    _iso_to_epoch_ms,
)
from datadongle.collectors.arcgishub.spec import ArcGISHubDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2, Append

from .conftest import (
    BASE_URL,
    ITEM_ID,
    FakeArcGISHubClient,
    layer_payload,
)


def _spec(**over) -> ArcGISHubDatasetSpec:
    base = {
        "name": "tps_arrests",
        "base_url": BASE_URL,
        "item_id": ITEM_ID,
        "target_table": "tps_arrests",
        "entity_key": ["Event_Unique_Id"],
        "layer_index": 0,
        "incremental_column": "Occurred_Date",
    }
    base.update(over)
    return ArcGISHubDatasetSpec(**base)


def _reader(fake: FakeArcGISHubClient) -> ArcGISHubReader:
    return ArcGISHubReader(client_factory=fake.factory())


def _feature(**attrs):
    geom = attrs.pop("_geom", {"x": -79.4, "y": 43.7})
    return {"attributes": attrs, "geometry": geom}


# --------------------------------------------------------------- identity / policy


def test_target_and_dataset_id():
    reader = _reader(FakeArcGISHubClient())
    spec = _spec()
    assert reader.target(spec) == TableRef("tps_arrests", "raw_data")
    assert reader.dataset_id(spec) == f"{ITEM_ID}:0"


def test_write_mode_scd2_lowercases_entity_key():
    reader = _reader(FakeArcGISHubClient())
    # Lowercased to match the stored (lowercased) columns.
    assert reader.write_mode(_spec(), mode="incremental") == SCD2(entity_key=["event_unique_id"])


def test_write_mode_append_without_entity_key():
    reader = _reader(FakeArcGISHubClient())
    assert reader.write_mode(_spec(entity_key=None), mode="full") == Append()


def test_cursor_spec_none_without_incremental_column():
    reader = _reader(FakeArcGISHubClient())
    assert reader.cursor_spec(_spec(incremental_column=None)) is None


def test_cursor_spec_lowercases_incremental_column():
    reader = _reader(FakeArcGISHubClient())
    assert reader.cursor_spec(_spec()) == CursorSpec(column="occurred_date")


def test_extract_cursor_is_none():
    reader = _reader(FakeArcGISHubClient())
    assert reader.extract_cursor([{"occurred_date": "2024-01-01T00:00:00+00:00"}]) is None


# ------------------------------------------------------------------------ schema


def test_schema_maps_types_geometry_and_oid_metadata():
    reader = _reader(FakeArcGISHubClient())
    schema = reader.schema(_spec())
    by_name = {c.name: c for c in schema.columns}

    assert by_name["objectid"].type is ColumnType.BIGINT
    assert by_name["objectid"].metadata is True  # OID stored but excluded from hash
    assert by_name["event_unique_id"].type is ColumnType.TEXT
    assert by_name["occurred_date"].type is ColumnType.TIMESTAMPTZ
    assert by_name["count"].type is ColumnType.INTEGER
    assert by_name["geom"].type is ColumnType.GEOMETRY
    assert by_name["geom"].geometry.kind == "Point"
    assert by_name["geom"].geometry.srid == 4326
    assert schema.metadata_column_names() == {"objectid"}


def test_schema_no_geometry_column_when_layer_has_none():
    fake = FakeArcGISHubClient(layer_infos={0: layer_payload(0, geometry_type=None)})
    schema = _reader(fake).schema(_spec())
    assert "geom" not in {c.name for c in schema.columns}


def test_schema_adds_layer_column():
    reader = _reader(FakeArcGISHubClient())
    schema = reader.schema(_spec(layer_column="Layer_Name"))
    assert any(c.name == "layer_name" and c.type is ColumnType.TEXT for c in schema.columns)


def test_schema_renames_field_colliding_with_geometry_column():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "geom", "type": "esriFieldTypeString"},  # collides with the geometry col
    ]
    fake = FakeArcGISHubClient(layer_infos={0: layer_payload(0, fields=fields)})
    names = {c.name for c in _reader(fake).schema(_spec(entity_key=None)).columns}
    assert "_orig_geom" in names  # source field renamed
    assert "geom" in names        # geometry column keeps the canonical name


# ------------------------------------------------------------- multi-layer schema


def _two_layers(fields0, fields1):
    return FakeArcGISHubClient(
        item=None,
        layer_infos={
            0: layer_payload(0, name="A", fields=fields0),
            1: layer_payload(1, name="B", fields=fields1),
        },
        layers=[{"id": 0, "name": "A"}, {"id": 1, "name": "B"}],
    )


def test_schema_unions_fields_across_layers():
    f0 = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "shared", "type": "esriFieldTypeString"},
        {"name": "only_a", "type": "esriFieldTypeString"},
    ]
    f1 = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "shared", "type": "esriFieldTypeString"},
        {"name": "only_b", "type": "esriFieldTypeString"},
    ]
    reader = _reader(_two_layers(f0, f1))
    spec = _spec(layer_index="all", entity_key=None, min_field_overlap=0.5)
    names = {c.name for c in reader.schema(spec).columns}
    assert {"objectid", "shared", "only_a", "only_b", "geom"} <= names


def test_schema_raises_when_field_overlap_too_low():
    f0 = [{"name": f"a{i}", "type": "esriFieldTypeString"} for i in range(10)]
    f1 = [{"name": f"b{i}", "type": "esriFieldTypeString"} for i in range(10)]
    reader = _reader(_two_layers(f0, f1))
    with pytest.raises(ValueError, match="Field overlap"):
        reader.schema(_spec(layer_index="all", entity_key=None, min_field_overlap=0.8))


# -------------------------------------------------------------------------- read


def test_read_flattens_attrs_dates_and_geometry():
    epoch_ms = 1704164645000  # 2024-01-02T03:04:05Z
    fake = FakeArcGISHubClient(
        pages={0: [[_feature(OBJECTID=1, Event_Unique_Id="E1", Occurred_Date=epoch_ms, Count=3)]]},
    )
    rows = list(_reader(fake).read(_spec(), since=None))
    assert len(rows) == 1
    (row,) = rows[0]
    assert row["objectid"] == 1                       # attribute names lowercased
    assert row["event_unique_id"] == "E1"
    assert row["occurred_date"] == datetime.fromtimestamp(epoch_ms / 1000, tz=UTC).isoformat()
    assert row["geom"] == "SRID=4326;POINT(-79.4 43.7)"


def test_read_paginates_until_short_page():
    fake = FakeArcGISHubClient(
        layer_infos={0: layer_payload(0, max_record_count=2)},
        pages={0: [
            [_feature(OBJECTID=1, Event_Unique_Id="E1"), _feature(OBJECTID=2, Event_Unique_Id="E2")],
            [_feature(OBJECTID=3, Event_Unique_Id="E3")],  # short page -> stop
        ]},
    )
    batches = list(_reader(fake).read(_spec(), since=None))
    assert [len(b) for b in batches] == [2, 1]


def test_read_incremental_since_builds_epoch_ms_where():
    fake = FakeArcGISHubClient(pages={0: [[]]})
    since = Cursor(value="2024-01-02T03:04:05+00:00")
    list(_reader(fake).read(_spec(where="Type='X'"), since=since))

    query_calls = [p for (u, p) in fake.calls if u.endswith("/query")]
    assert query_calls, "expected a /query call"
    where = query_calls[0]["where"]
    assert str(_iso_to_epoch_ms(since.value)) in where
    assert "Type='X'" in where          # static filter preserved
    assert "occurred_date" not in where  # uses the source-cased column name


def test_read_full_uses_base_where_only():
    fake = FakeArcGISHubClient(pages={0: [[]]})
    list(_reader(fake).read(_spec(where="1=1"), since=None))
    where = [p for (u, p) in fake.calls if u.endswith("/query")][0]["where"]
    assert where == "1=1"


def test_read_multi_layer_yields_all_and_tags_layer_column():
    f = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "Event_Unique_Id", "type": "esriFieldTypeString"},
    ]
    fake = FakeArcGISHubClient(
        layer_infos={
            0: layer_payload(0, name="2023", fields=f),
            1: layer_payload(1, name="2024", fields=f),
        },
        layers=[{"id": 0, "name": "2023"}, {"id": 1, "name": "2024"}],
        pages={
            0: [[_feature(OBJECTID=1, Event_Unique_Id="E1")]],
            1: [[_feature(OBJECTID=2, Event_Unique_Id="E2")]],
        },
    )
    spec = _spec(layer_index=[0, 1], layer_column="Layer_Name")
    rows = [r for batch in _reader(fake).read(spec, since=None) for r in batch]
    assert {r["event_unique_id"] for r in rows} == {"E1", "E2"}
    assert {r["layer_name"] for r in rows} == {"2023", "2024"}


def test_read_raises_on_esri_error_payload():
    class ErrorClient(FakeArcGISHubClient):
        def get_json(self, path_or_url, params=None):
            if path_or_url.endswith("/query"):
                return {"error": {"code": 400, "message": "Invalid where"}}
            return super().get_json(path_or_url, params)

    with pytest.raises(RuntimeError, match="ArcGIS query error 400"):
        list(_reader(ErrorClient()).read(_spec(), since=None))
