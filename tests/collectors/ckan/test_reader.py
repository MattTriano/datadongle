"""Unit tests for CKANReader (fake client, no network)."""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path

import pytest

from datadongle.collectors.ckan.metadata import CKANResource
from datadongle.collectors.ckan.reader import (
    CKANReader,
    normalize_column_name,
    normalize_column_names,
)
from datadongle.collectors.ckan.spec import CKANDatasetSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2, Append
from datadongle.load.driver import run_collection

CSV_URL = "https://portal.example.gov/download/food.csv"
GEOJSON_URL = "https://portal.example.gov/download/parcels.geojson"

CSV_BODY = b"Inspection ID,Risk Level,_id\n101,High,1\n102,Low,2\n"

GEOJSON_BODY = json.dumps(
    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"Name": "A", "Height M": "5", "_full_text": "junk"},
                "geometry": {"type": "Point", "coordinates": [-87.6, 41.8]},
            },
            {
                "type": "Feature",
                "properties": {"Name": "B", "Height M": "7", "_full_text": "junk"},
                "geometry": {"type": "Point", "coordinates": [-87.7, 41.9]},
            },
        ],
    }
).encode()


def _resource(
    id="res-1",
    fmt="CSV",
    url=CSV_URL,
    datastore=False,
) -> CKANResource:
    return CKANResource(
        id=id,
        name=None,
        format=fmt,
        url=url,
        size=None,
        last_modified=None,
        created=None,
        datastore_active=datastore,
    )


def _spec(**kw) -> CKANDatasetSpec:
    base = dict(
        name="food_inspections",
        base_url="https://portal.example.gov",
        dataset_id="food-inspections",
        target_table="food_inspections",
        entity_key=["inspection_id"],
        resource_format="CSV",
    )
    base.update(kw)
    return CKANDatasetSpec(**base)


class _FakeMetadata:
    """Stands in for CKANMetadata: canned resources and DataStore fields."""

    def __init__(self, resources, datastore_fields):
        self._resources = resources
        self._fields = datastore_fields  # resource id -> [{"id": ..., "type": ...}]

    def get_resource(self, rid):
        for r in self._resources:
            if r.id == rid:
                return r
        raise ValueError(f"unknown resource {rid}")

    def find_resources(self, dataset_id, fmt):
        return [r for r in self._resources if r.format == (fmt or "").upper()]

    def has_datastore(self, rid):
        return rid in self._fields

    def get_datastore_fields(self, rid):
        return self._fields[rid]


class _FakeClient:
    """Stands in for CKANClient: 'downloads' canned bytes to real tempfiles."""

    def __init__(self, resources=None, datastore_fields=None, files=None):
        self.metadata = _FakeMetadata(resources or [], datastore_fields or {})
        self._files = files or {}  # url -> bytes
        self.download_calls: list[str] = []
        self.downloaded_paths: list[Path] = []

    def suffix_for_format(self, fmt):
        return {"CSV": ".csv", "GEOJSON": ".geojson"}.get((fmt or "").upper(), ".csv")

    def download_to_tempfile(self, url, suffix=".csv"):
        self.download_calls.append(url)
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix="ckan_test_", delete=False)
        tmp.write(self._files[url])
        tmp.close()
        path = Path(tmp.name)
        self.downloaded_paths.append(path)
        return path


def _reader(client: _FakeClient) -> CKANReader:
    return CKANReader(client_factory=lambda base_url: client)


# ------------------------------------------------------------------ metadata bits


def test_target_and_dataset_id():
    reader = _reader(_FakeClient())
    spec = _spec(target_schema="raw_data")
    assert reader.dataset_id(spec) == "food-inspections"
    assert reader.target(spec) == TableRef("food_inspections", "raw_data")


def test_write_mode_scd2_when_entity_key():
    reader = _reader(_FakeClient())
    assert reader.write_mode(_spec(), mode="full") == SCD2(entity_key=["inspection_id"])
    assert reader.write_mode(_spec(entity_key=None), mode="full") == Append()


def test_cursor_spec_and_extract_cursor_are_none():
    # CKAN is full-refresh-only: no cursor, the driver always runs a full read.
    reader = _reader(_FakeClient())
    assert reader.cursor_spec(_spec()) is None
    assert reader.extract_cursor([{"inspection_id": "101"}]) is None


# ------------------------------------------------------------------ schema discovery


def test_schema_from_datastore_fields():
    fields = [
        {"id": "inspection_id", "type": "text"},
        {"id": "license", "type": "int4"},
        {"id": "total", "type": "int8"},
        {"id": "score", "type": "numeric"},
        {"id": "ratio", "type": "float8"},
        {"id": "passed", "type": "bool"},
        {"id": "details", "type": "jsonb"},
        {"id": "inspected_on", "type": "date"},
        {"id": "inspected_at", "type": "timestamp"},
        {"id": "shift_start", "type": "time"},  # unmapped -> TEXT
        {"id": "_id", "type": "int"},  # internal -> excluded
    ]
    client = _FakeClient(
        resources=[_resource(datastore=True)], datastore_fields={"res-1": fields}
    )
    schema = _reader(client).schema(_spec())

    by_name = {c.name: c for c in schema.columns}
    assert "_id" not in by_name
    assert by_name["inspection_id"].type is ColumnType.TEXT
    assert by_name["license"].type is ColumnType.INTEGER
    assert by_name["total"].type is ColumnType.BIGINT
    assert by_name["score"].type is ColumnType.NUMERIC
    assert by_name["ratio"].type is ColumnType.DOUBLE
    assert by_name["passed"].type is ColumnType.BOOLEAN
    assert by_name["details"].type is ColumnType.JSON
    assert by_name["inspected_on"].type is ColumnType.DATE
    assert by_name["inspected_at"].type is ColumnType.TIMESTAMP
    assert by_name["shift_start"].type is ColumnType.TEXT


def test_schema_from_csv_header_all_text():
    client = _FakeClient(resources=[_resource()], files={CSV_URL: CSV_BODY})
    schema = _reader(client).schema(_spec())

    # Names normalized, internal _id excluded, everything text.
    assert schema.column_names() == ["inspection_id", "risk_level"]
    assert all(c.type is ColumnType.TEXT for c in schema.columns)


def test_schema_from_geojson_props_plus_geom():
    client = _FakeClient(
        resources=[_resource(fmt="GEOJSON", url=GEOJSON_URL)],
        files={GEOJSON_URL: GEOJSON_BODY},
    )
    schema = _reader(client).schema(_spec(resource_format="GeoJSON"))

    assert schema.column_names() == ["name", "height_m", "geom"]
    by_name = {c.name: c for c in schema.columns}
    assert by_name["name"].type is ColumnType.TEXT
    assert by_name["geom"].type is ColumnType.GEOMETRY
    assert schema.geometry["geom"].srid == 4326


def test_schema_is_cached_and_download_reused_by_read():
    client = _FakeClient(resources=[_resource()], files={CSV_URL: CSV_BODY})
    reader = _reader(client)
    spec = _spec()

    reader.schema(spec)
    reader.schema(spec)
    assert client.download_calls == [CSV_URL]  # discovery downloaded once

    rows = [r for batch in reader.read(spec, since=None) for r in batch]
    assert len(rows) == 2
    assert client.download_calls == [CSV_URL]  # read consumed the cached file

    # The consumed tempfile is deleted; a fresh read downloads again.
    assert not client.downloaded_paths[0].exists()
    list(reader.read(spec, since=None))
    assert client.download_calls == [CSV_URL, CSV_URL]


def test_no_matching_resources_raises():
    client = _FakeClient(resources=[_resource(fmt="XLSX")])
    with pytest.raises(ValueError, match="No resources found"):
        _reader(client).schema(_spec())


def test_resource_ids_take_precedence():
    other = _resource(id="res-2", fmt="CSV", url=CSV_URL)
    client = _FakeClient(resources=[_resource(), other], files={CSV_URL: CSV_BODY})
    reader = _reader(client)
    spec = _spec(resource_format=None, resource_ids=["res-2"])

    batches = list(reader.read(spec, since=None))
    assert len([r for b in batches for r in b]) == 2
    assert client.download_calls == [CSV_URL]


# ------------------------------------------------------------------ name normalization


def test_normalize_column_name():
    assert normalize_column_name("Inspection ID") == "inspection_id"
    assert normalize_column_name("  Height (M)  ") == "height_m"
    assert normalize_column_name("___") == ""


def test_normalize_column_names_collision_suffixing():
    mapping = normalize_column_names(["Name", "name", "NAME", "   "])
    assert mapping == {"Name": "name", "name": "name_2", "NAME": "name_3"}


# ------------------------------------------------------------------ read path


def test_read_csv_strips_internal_and_renames():
    client = _FakeClient(resources=[_resource()], files={CSV_URL: CSV_BODY})
    rows = [r for batch in _reader(client).read(_spec(), since=None) for r in batch]

    assert rows == [
        {"inspection_id": "101", "risk_level": "High"},
        {"inspection_id": "102", "risk_level": "Low"},
    ]


def test_read_geojson_emits_geometry_column():
    client = _FakeClient(
        resources=[_resource(fmt="GEOJSON", url=GEOJSON_URL)],
        files={GEOJSON_URL: GEOJSON_BODY},
    )
    spec = _spec(resource_format="GeoJSON")
    rows = [r for batch in _reader(client).read(spec, since=None) for r in batch]

    assert [r["name"] for r in rows] == ["A", "B"]
    assert all("_full_text" not in r for r in rows)
    # Geometry parsed to a WKB hex string ready for the engines.
    assert all(isinstance(r["geom"], str) and r["geom"] for r in rows)


def test_read_multiple_resources_concatenates():
    second_url = "https://portal.example.gov/download/food_2024.csv"
    client = _FakeClient(
        resources=[_resource(), _resource(id="res-2", url=second_url)],
        files={CSV_URL: CSV_BODY, second_url: b"Inspection ID,Risk Level\n103,High\n"},
    )
    rows = [r for batch in _reader(client).read(_spec(), since=None) for r in batch]

    assert [r["inspection_id"] for r in rows] == ["101", "102", "103"]


def test_read_warns_on_drift_from_discovered_schema(caplog):
    # Schema comes from the DataStore, but the file has an extra column and
    # lacks a schema column.
    fields = [{"id": "inspection_id", "type": "text"}, {"id": "score", "type": "int"}]
    body = b"Inspection ID,Surprise\n101,x\n"
    client = _FakeClient(
        resources=[_resource(datastore=True)],
        datastore_fields={"res-1": fields},
        files={CSV_URL: body},
    )
    reader = _reader(client)
    spec = _spec()

    with caplog.at_level(logging.WARNING, logger="datadongle.collectors.ckan.reader"):
        rows = [r for batch in reader.read(spec, since=None) for r in batch]

    assert rows == [{"inspection_id": "101", "surprise": "x"}]
    messages = " ".join(r.message for r in caplog.records)
    assert "not in the discovered schema" in messages and "surprise" in messages
    assert "missing schema columns" in messages and "score" in messages


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
    def __init__(self):
        self.session = _FakeWriteSession()
        self.calls = {}

    def ensure_table(self, target, schema, mode):
        self.calls["ensure"] = (target, schema, mode)

    def read_high_water_mark(self, target, cursor):
        self.calls["hwm"] = (target, cursor)
        return None

    def open_write(self, target, schema, mode):
        self.calls["open"] = (target, schema, mode)
        return self.session


def test_ckan_reader_drives_full_through_run_collection():
    client = _FakeClient(resources=[_resource()], files={CSV_URL: CSV_BODY})
    reader = _reader(client)
    engine = _FakeEngine()

    summary = run_collection(reader, _spec(), engine, mode="full")

    target, schema, mode = engine.calls["ensure"]
    assert target == TableRef("food_inspections", "raw_data")
    assert schema.column_names() == ["inspection_id", "risk_level"]
    assert mode == SCD2(entity_key=["inspection_id"])
    # Schema discovery + read shared a single download.
    assert client.download_calls == [CSV_URL]
    written = [r for batch in engine.session.batches for r in batch]
    assert [r["inspection_id"] for r in written] == ["101", "102"]
    assert summary["rows_merged"] == 2


def test_ckan_reader_incremental_falls_back_to_full():
    client = _FakeClient(resources=[_resource()], files={CSV_URL: CSV_BODY})
    engine = _FakeEngine()

    summary = run_collection(_reader(client), _spec(), engine, mode="incremental")

    # No cursor_spec -> the driver never consults the high-water mark and
    # reads the whole source anyway.
    assert "hwm" not in engine.calls
    assert summary["rows_merged"] == 2
    assert summary["high_water_mark"] is None
