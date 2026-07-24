"""Unit behaviors of CMSReader — schema discovery, column normalization,
vintage resolution, and the read transform — against a fake CMS API (no DB)."""

from __future__ import annotations

import dataclasses

from datadongle.collectors.cms.reader import CMSReader
from datadongle.core.cursor import CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.reader import SourceReader
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2

from .helpers import DATASET_TITLE, FakeCMSClient, as_client, make_spec, seeded_source


def _reader(source) -> CMSReader:
    return CMSReader(client=as_client(FakeCMSClient(source)))


def _narrow(spec, vintage):
    return dataclasses.replace(spec, vintages=[vintage])


# ------------------------------------------------------------------ protocol / metadata


def test_conforms_to_source_reader_protocol():
    assert isinstance(_reader(seeded_source()), SourceReader)


def test_source_name():
    assert CMSReader.source == "cms"


def test_target_and_dataset_id():
    reader = _reader(seeded_source())
    spec = _narrow(make_spec(schema="raw_data"), "2022")
    assert reader.target(spec) == TableRef("fake_payments", "raw_data")
    assert reader.dataset_id(spec) == f"{DATASET_TITLE}/2022"


def test_schema_normalizes_columns_and_adds_vintage():
    reader = _reader(seeded_source())
    schema = reader.schema(_narrow(make_spec(schema="raw_data"), "2022"))

    by_name = {c.name: c for c in schema.columns}
    # "Rndrng_Prvdr_CCN" -> lowercased, "Avg Payment Amt" -> spaces underscored
    assert schema.column_names() == [
        "rndrng_prvdr_ccn",
        "drg_cd",
        "avg_payment_amt",
        "vintage",
    ]
    assert all(c.type is ColumnType.TEXT for c in schema.columns)
    assert by_name["vintage"].nullable is False
    # vintage is part of entity_key -> a normal hashed column, not bookkeeping
    assert schema.metadata_column_names() == set()


def test_write_mode_is_scd2_on_entity_key():
    reader = _reader(seeded_source())
    spec = make_spec(schema="raw_data")
    assert reader.write_mode(spec, mode="full") == SCD2(
        entity_key=["rndrng_prvdr_ccn", "drg_cd", "vintage"]
    )


def test_cursor_spec_is_none():
    reader = _reader(seeded_source())
    assert reader.cursor_spec(make_spec(schema="raw_data")) is None
    assert isinstance(CursorSpec("x"), CursorSpec)  # import sanity


# ------------------------------------------------------------------ vintage resolution


def test_versions_lists_all_in_catalog_order():
    reader = _reader(seeded_source())
    assert reader.versions(make_spec(schema="raw_data")) == ["2022", "2023"]


def test_versions_filters_to_requested_vintages():
    reader = _reader(seeded_source())
    spec = make_spec(schema="raw_data", vintages=["2023"])
    assert reader.versions(spec) == ["2023"]


# ------------------------------------------------------------------ read transform


def test_read_normalizes_columns_and_stamps_vintage():
    reader = _reader(seeded_source())
    spec = _narrow(make_spec(schema="raw_data"), "2022")

    rows = [r for batch in reader.read(spec, since=None) for r in batch]

    assert len(rows) == 7
    assert all(r["vintage"] == "2022" for r in rows)
    assert set(rows[0]) == {"rndrng_prvdr_ccn", "drg_cd", "avg_payment_amt", "vintage"}
    # original-cased / spaced source keys are gone
    assert "Avg Payment Amt" not in rows[0]


def test_read_csv_and_api_produce_the_same_rows():
    source = seeded_source()
    reader = _reader(source)
    api_spec = _narrow(make_spec(schema="raw_data", retrieval="api"), "2023")
    csv_spec = _narrow(make_spec(schema="raw_data", retrieval="csv"), "2023")

    api_rows = [r for b in reader.read(api_spec, since=None) for r in b]
    csv_rows = [r for b in reader.read(csv_spec, since=None) for r in b]

    assert {r["rndrng_prvdr_ccn"] for r in api_rows} == {r["rndrng_prvdr_ccn"] for r in csv_rows}
    # 2023 has a blank payment: API yields "", the CSV parser yields None.
    api_blank = next(r for r in api_rows if r["rndrng_prvdr_ccn"] == "000002")
    csv_blank = next(r for r in csv_rows if r["rndrng_prvdr_ccn"] == "000002")
    assert api_blank["avg_payment_amt"] == ""
    assert csv_blank["avg_payment_amt"] is None
