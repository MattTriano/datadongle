"""Unit tests for SocrataReader (mocked metadata + client, no network)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

from datadongle.collectors.socrata.reader import SocrataReader
from datadongle.collectors.socrata.spec import SocrataDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2, Append
from datadongle.load.driver import run_collection


def _col(field_name: str, datatype: str):
    return SimpleNamespace(field_name=field_name, datatype=datatype)


def _reader_with_meta(dataset_id: str, columns, domain="data.example.org"):
    reader = SocrataReader()
    meta = MagicMock()
    meta.columns = columns
    meta.domain = domain
    reader._metadata_cache[dataset_id] = meta
    return reader, meta


def _spec(**kw) -> SocrataDatasetSpec:
    base: dict[str, Any] = dict(name="permits", dataset_id="abcd-1234", target_table="permits")
    base.update(kw)
    return SocrataDatasetSpec(**base)


# ------------------------------------------------------------------ metadata bits


def test_target_and_dataset_id():
    reader = SocrataReader()
    spec = _spec(target_schema="raw_data")
    assert reader.dataset_id(spec) == "abcd-1234"
    assert reader.target(spec) == TableRef("permits", "raw_data")


def test_schema_maps_types_geometry_and_system_columns():
    cols = [_col("permit_", "text"), _col("amt", "number"), _col("loc", "point")]
    reader, _ = _reader_with_meta("abcd-1234", cols)
    schema = reader.schema(_spec())

    assert schema.column_names() == [
        "permit_",
        "amt",
        "loc",
        "socrata_id",
        "socrata_updated_at",
        "socrata_created_at",
        "socrata_version",
    ]
    by_name = {c.name: c for c in schema.columns}
    assert by_name["permit_"].type is ColumnType.TEXT
    assert by_name["amt"].type is ColumnType.NUMERIC
    assert by_name["loc"].type is ColumnType.GEOMETRY
    assert schema.geometry["loc"].kind == "Point"
    # source-metadata columns are excluded from the SCD2 hash
    assert schema.metadata_column_names() == {
        "socrata_id",
        "socrata_updated_at",
        "socrata_created_at",
        "socrata_version",
    }


def test_write_mode_scd2_when_entity_key():
    reader = SocrataReader()
    assert reader.write_mode(_spec(entity_key=["permit_"]), mode="full") == SCD2(
        entity_key=["permit_"]
    )


def test_write_mode_append_without_entity_key():
    reader = SocrataReader()
    assert reader.write_mode(_spec(), mode="incremental") == Append()


def test_cursor_spec_api():
    reader = SocrataReader()
    assert reader.cursor_spec(_spec()) == CursorSpec("socrata_updated_at", "socrata_id")


def test_cursor_spec_file_download_is_none():
    reader = SocrataReader()
    assert reader.cursor_spec(_spec(full_update_mode="file_download")) is None


# ------------------------------------------------------------------ extract_cursor


def test_extract_cursor_returns_max_with_tiebreak():
    reader = SocrataReader()
    batch = [
        {"socrata_updated_at": "2024-01-01", "socrata_id": "a"},
        {"socrata_updated_at": "2024-03-01", "socrata_id": "b"},
        {"socrata_updated_at": "2024-03-01", "socrata_id": "z"},
    ]
    assert reader.extract_cursor(batch) == Cursor("2024-03-01", "z")


def test_extract_cursor_none_when_no_cursor_values():
    reader = SocrataReader()
    assert reader.extract_cursor([{"socrata_updated_at": None}]) is None
    assert reader.extract_cursor([]) is None


# ------------------------------------------------------------------ where builder


def test_build_where_none_for_full_read():
    assert SocrataReader._build_where(":updated_at", None) is None


def test_build_where_with_tiebreak():
    where = SocrataReader._build_where(":updated_at", Cursor("2024-01-01", "5"))
    assert where == (
        "(:updated_at = '2024-01-01' AND :id > '5') OR (:updated_at > '2024-01-01')"
    )


def test_build_where_without_tiebreak():
    where = SocrataReader._build_where(":updated_at", Cursor("2024-01-01", None))
    assert where == ":updated_at > '2024-01-01'"


# ------------------------------------------------------------------ API read path


def test_read_api_applies_transforms_and_query():
    cols = [_col("name", "text"), _col("loc", "point")]
    reader, meta = _reader_with_meta("abcd-1234", cols)
    reader._client = MagicMock()
    raw = [
        {
            ":id": "row1",
            ":updated_at": "2024-02-01",
            ":version": "v1",
            "name": "Alice",
            ":@computed_region_abcd": "99",
            "loc": {"latitude": "41.8", "longitude": "-87.6"},
        }
    ]
    reader._client.paginate.return_value = iter([raw])

    batches = list(reader.read(_spec(entity_key=["name"]), since=Cursor("2024-01-01", "5")))

    assert len(batches) == 1
    row = batches[0][0]
    # system fields renamed
    assert row["socrata_id"] == "row1"
    assert row["socrata_updated_at"] == "2024-02-01"
    assert row["socrata_version"] == "v1"
    # computed-region column dropped
    assert not any(k.startswith(":@computed_region") for k in row)
    # location -> EWKT
    assert row["loc"] == "SRID=4326;POINT(-87.6 41.8)"
    assert row["name"] == "Alice"

    # query shape passed to the client
    _, kwargs = reader._client.paginate.call_args
    assert kwargs["domain"] == "data.example.org"
    assert kwargs["dataset_id"] == "abcd-1234"
    assert kwargs["order_by"] == ":updated_at, :id"
    assert kwargs["include_system_fields"] is True
    assert kwargs["where"] == (
        "(:updated_at = '2024-01-01' AND :id > '5') OR (:updated_at > '2024-01-01')"
    )


def test_read_api_full_read_has_no_where():
    reader, _ = _reader_with_meta("abcd-1234", [_col("name", "text")])
    reader._client = MagicMock()
    reader._client.paginate.return_value = iter([[{":id": "r", ":updated_at": "2024-01-01"}]])

    list(reader.read(_spec(), since=None))

    _, kwargs = reader._client.paginate.call_args
    assert kwargs["where"] is None


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


def test_socrata_reader_drives_incremental_through_run_collection():
    reader, _ = _reader_with_meta("abcd-1234", [_col("name", "text")])
    reader._client = MagicMock()
    page1 = [
        {":id": "1", ":updated_at": "2024-02-01", "name": "a"},
        {":id": "2", ":updated_at": "2024-02-02", "name": "b"},
    ]
    page2 = [{":id": "3", ":updated_at": "2024-02-03", "name": "c"}]
    reader._client.paginate.return_value = iter([page1, page2])

    engine = _FakeEngine(hwm=Cursor("2024-01-01", "0"))
    spec = _spec(entity_key=["name"], target_schema="raw_data")
    summary = run_collection(reader, spec, engine, mode="incremental")

    # HWM read from the target with the socrata cursor
    target, cursor = engine.calls["hwm"]
    assert target == TableRef("permits", "raw_data")
    assert cursor == CursorSpec("socrata_updated_at", "socrata_id")

    # both ensure_table and open_write got the SCD2 write mode
    assert isinstance(engine.calls["ensure"][2], SCD2)
    assert isinstance(engine.calls["open"][2], SCD2)

    # the prior HWM became the SoQL filter
    _, kwargs = reader._client.paginate.call_args
    assert "2024-01-01" in kwargs["where"]

    # rows were renamed and written, and the HWM advanced to the max seen
    written = [r for batch in engine.session.batches for r in batch]
    assert len(written) == 3
    assert all("socrata_id" in r for r in written)
    assert summary["rows_merged"] == 3
    assert summary["high_water_mark"] == "2024-02-03|3"
