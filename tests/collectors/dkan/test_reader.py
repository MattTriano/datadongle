"""Unit tests for DKANReader (fake client, no engine, no network)."""

from __future__ import annotations

from datadongle.collectors.dkan.metadata import DKANMetadata
from datadongle.collectors.dkan.reader import (
    PG_MAX_IDENTIFIER,
    SOURCE_DATASET_COLUMN,
    SOURCE_MODIFIED_COLUMN,
    DKANReader,
)
from datadongle.core.engine import TableRef
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2

from .helpers import (
    HOSPITAL_ID,
    LONG_RAW_HEADER,
    FakeDKANClient,
    dkan_normalize,
    make_hospital_spec,
    seeded_pdc_source,
)

LONG_COL = dkan_normalize(LONG_RAW_HEADER)[:PG_MAX_IDENTIFIER].rstrip("_")


def _reader(source) -> DKANReader:
    return DKANReader(client_factory=lambda base_url: FakeDKANClient(base_url, source))


# ------------------------------------------------------------------ metadata bits


def test_target_and_dataset_id():
    reader = _reader(seeded_pdc_source())
    spec = make_hospital_spec("raw_data")
    assert reader.dataset_id(spec) == HOSPITAL_ID
    assert reader.target(spec) == TableRef("fake_hospitals", "raw_data")


def test_cursor_spec_and_extract_cursor_are_none():
    # DKAN is full-refresh-only: no cursor, the driver always runs a full read.
    reader = _reader(seeded_pdc_source())
    assert reader.cursor_spec(make_hospital_spec("raw_data")) is None
    assert reader.extract_cursor([{"facility_id": "0"}]) is None


def test_write_mode_scd2_with_entity_key():
    reader = _reader(seeded_pdc_source())
    spec = make_hospital_spec("raw_data")
    assert reader.write_mode(spec, mode="full") == SCD2(entity_key=["facility_id"])


def test_write_mode_invalidate_missing_only_on_full():
    reader = _reader(seeded_pdc_source())
    spec = make_hospital_spec("raw_data", invalidate_missing=True)
    assert reader.write_mode(spec, mode="full").invalidate_missing is True
    # Defensive: an incremental mode would strip it (DKAN only ever runs full).
    assert reader.write_mode(spec, mode="incremental").invalidate_missing is False


# ------------------------------------------------------------------ schema discovery


def test_schema_uses_dkan_normalization_and_truncation():
    reader = _reader(seeded_pdc_source())
    schema = reader.schema(make_hospital_spec("raw_data"))

    names = schema.column_names()
    # DKAN's rule drops the slash rather than underscoring it.
    assert "countyparish" in names
    assert "county_parish" not in names
    # The 79-char header is truncated to a valid identifier.
    assert LONG_COL in names
    assert len(LONG_COL) <= PG_MAX_IDENTIFIER
    assert dkan_normalize(LONG_RAW_HEADER) not in names  # never emit a >63-char name
    # Every source column is text.
    data_cols = [c for c in schema.columns if c.name not in (SOURCE_DATASET_COLUMN, SOURCE_MODIFIED_COLUMN)]
    assert all(c.type is ColumnType.TEXT for c in data_cols)


def test_schema_has_provenance_columns_hash_excluded():
    reader = _reader(seeded_pdc_source())
    schema = reader.schema(make_hospital_spec("raw_data"))

    by_name = {c.name: c for c in schema.columns}
    assert by_name[SOURCE_DATASET_COLUMN].nullable is False
    assert by_name[SOURCE_MODIFIED_COLUMN].nullable is True
    # Both are excluded from the SCD2 content hash.
    assert schema.metadata_column_names() == {SOURCE_DATASET_COLUMN, SOURCE_MODIFIED_COLUMN}


# ------------------------------------------------------------------ name normalization


def test_normalize_name_drops_punctuation_and_truncates():
    assert DKANReader._normalize_name("County/Parish") == "countyparish"
    assert DKANReader._normalize_name("  Total Amount ($) ") == "total_amount"
    assert DKANReader._normalize_name("Program Year") == "program_year"
    long_norm = DKANReader._normalize_name(LONG_RAW_HEADER)
    assert len(long_norm) <= PG_MAX_IDENTIFIER


def test_build_name_map_suffixes_collisions():
    reader = _reader(seeded_pdc_source())
    # "A B" and "A/B" both normalize to "a_b" vs "ab"? no — pick real collisions.
    mapping = reader._build_name_map(["Rate", "rate", "RATE"])
    assert mapping == {"Rate": "rate", "rate": "rate_2", "RATE": "rate_3"}


# ------------------------------------------------------------------ read path


def test_read_datastore_normalizes_and_stamps_provenance():
    source = seeded_pdc_source()
    reader = _reader(source)
    spec = make_hospital_spec("raw_data", retrieval="datastore")

    rows = [r for batch in reader.read(spec, since=None) for r in batch]

    assert len(rows) == 7
    first = rows[0]
    assert first["facility_id"] == "000000"
    assert first["countyparish"] == "HOUSTON"
    assert first[SOURCE_DATASET_COLUMN] == HOSPITAL_ID
    assert first[SOURCE_MODIFIED_COLUMN] == "2026-04-28"
    assert LONG_COL in first


def test_read_file_and_datastore_produce_same_columns():
    source = seeded_pdc_source()
    reader = _reader(source)

    ds_rows = [
        r for b in reader.read(make_hospital_spec("raw_data", retrieval="datastore"), since=None) for r in b
    ]
    file_rows = [
        r for b in reader.read(make_hospital_spec("raw_data", retrieval="file"), since=None) for r in b
    ]

    # The two retrieval modes converge on identical normalized column sets.
    assert {k for k in ds_rows[0]} == {k for k in file_rows[0]}


# ------------------------------------------------------------------ distribution parsing


def test_distributions_parse_flat_and_nested_shapes():
    dataset = {
        "distribution": [
            {"mediaType": "text/csv", "downloadURL": "https://x/a.csv"},
            {"data": {"mediaType": "text/csv", "downloadURL": "https://x/b.csv"}},
        ]
    }
    dists = DKANMetadata.distributions(dataset)
    assert [d.index for d in dists] == [0, 1]
    assert [d.download_url for d in dists] == ["https://x/a.csv", "https://x/b.csv"]
