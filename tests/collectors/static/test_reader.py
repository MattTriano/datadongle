"""Unit behaviors of StaticFileReader: protocol conformance, schema, transforms.

End-to-end collection through the family driver lives in ``test_driver.py``.
"""

from __future__ import annotations

import dataclasses

from datadongle.collectors.static.client import StaticFileClient
from datadongle.collectors.static.reader import StaticFileReader
from datadongle.collectors.static.spec import FileRef
from datadongle.core.engine import TableRef
from datadongle.core.reader import SourceReader
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2, Append

from .helpers import DEFAULT_URL_2023, FakeStaticFileClient, csv_bytes, default_files, make_spec


def _reader(files=None):
    return StaticFileReader(client=FakeStaticFileClient(files or default_files()))


def test_reader_satisfies_the_protocol():
    assert isinstance(_reader(), SourceReader)
    assert _reader().source == "static_file"


def test_target_from_spec():
    assert _reader().target(make_spec()) == TableRef("test_static_systems", "raw_data")


def test_dataset_id_encodes_vintage():
    reader = _reader()
    spec = dataclasses.replace(make_spec(), files=[FileRef(url=DEFAULT_URL_2023, vintage="2023")])
    assert reader.dataset_id(spec) == "test_static_systems/2023"


def test_schema_is_all_text_with_not_null_vintage():
    schema = _reader().schema(make_spec())
    by_name = {c.name: c for c in schema.columns}

    for name in ["sys_id", "sys_name", "beds"]:
        assert by_name[name].type is ColumnType.TEXT
    assert by_name["vintage"].type is ColumnType.TEXT
    assert by_name["vintage"].nullable is False


def test_write_mode_scd2_with_entity_key():
    mode = _reader().write_mode(make_spec(entity_key=["sys_id", "vintage"]), mode="full")
    assert isinstance(mode, SCD2)
    assert mode.entity_key == ["sys_id", "vintage"]


def test_write_mode_append_without_entity_key():
    assert isinstance(_reader().write_mode(make_spec(entity_key=None), mode="full"), Append)


def test_not_incrementally_queryable():
    reader = _reader()
    assert reader.cursor_spec(make_spec()) is None
    assert reader.extract_cursor([{"vintage": "2023"}]) is None


def test_read_stamps_vintage_and_sanitizes():
    reader = _reader()
    spec = dataclasses.replace(
        make_spec(), files=[FileRef(url=DEFAULT_URL_2023, vintage="2023", encoding="cp1252")]
    )
    rows = [r for batch in reader.read(spec, since=None) for r in batch]

    assert len(rows) == 2
    assert all(r["vintage"] == "2023" for r in rows)
    assert rows[0]["sys_id"] == "0895"  # leading zero preserved
    assert rows[1]["sys_name"] == "Example Health – Metro"  # cp1252 decoded


def test_read_batches_large_files():
    header = ["id"]
    body = [[str(i)] for i in range(12000)]
    url = "https://x.test/big.csv"
    reader = StaticFileReader(client=FakeStaticFileClient({url: csv_bytes(header, body)}))
    spec = dataclasses.replace(make_spec(), files=[FileRef(url=url, vintage="2023")])

    batches = list(reader.read(spec, since=None))
    assert len(batches) == 3  # 12000 / 5000 -> 5000, 5000, 2000
    assert sum(len(b) for b in batches) == 12000


def test_file_url_escape_hatch(tmp_path):
    path = tmp_path / "manual-download.csv"
    path.write_bytes(csv_bytes(["ccn", "name"], [["0895", "Adena"]]))
    reader = StaticFileReader(client=StaticFileClient(delay_seconds=0))  # no HTTP happens
    spec = dataclasses.replace(make_spec(), files=[FileRef(url=f"file://{path}", vintage="2023")])

    rows = [r for batch in reader.read(spec, since=None) for r in batch]
    assert rows == [{"ccn": "0895", "name": "Adena", "vintage": "2023"}]
    assert path.exists()  # a file:// source is read in place, never deleted
