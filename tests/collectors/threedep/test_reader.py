"""Unit tests for ThreeDEPReader (fake client, synthetic GeoTIFFs, no network)."""

from __future__ import annotations

import dataclasses

from datadongle.collectors.threedep.reader import ThreeDEPReader
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.reader import SourceReader
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2

from .helpers import (
    SINGLE_TILE_BBOX,
    SUB_TILE,
    TWO_TILE_BBOX,
    FakeThreeDEPClient,
    as_client,
    fake_client,
    make_elevation_spec,
    seeded_source,
)


def _reader(source, batch_size=16) -> ThreeDEPReader:
    return ThreeDEPReader(
        client=as_client(FakeThreeDEPClient(source)), tile_size=SUB_TILE, batch_size=batch_size
    )


# ------------------------------------------------------------------ protocol / metadata


def test_conforms_to_source_reader_protocol():
    assert isinstance(_reader(seeded_source()), SourceReader)


def test_source_name():
    assert ThreeDEPReader.source == "3dep"


def test_target_and_dataset_id():
    reader = _reader(seeded_source())
    spec = make_elevation_spec("raw_data")
    assert reader.dataset_id(spec) == "fake_elevation"
    assert reader.target(spec) == TableRef("fake_elevation", "raw_data")


def test_dataset_id_of_a_narrowed_spec_names_the_tile():
    reader = _reader(seeded_source())
    spec = make_elevation_spec("raw_data", tiles=["n42w088"])
    assert reader.dataset_id(spec) == "fake_elevation/n42w088"


def test_schema_columns_and_types():
    schema = _reader(seeded_source()).schema(make_elevation_spec("raw_data"))
    by_name = {c.name: c for c in schema.columns}

    assert by_name["tile_id"].type is ColumnType.TEXT
    assert by_name["tile_id"].nullable is False
    assert by_name["source_tile"].type is ColumnType.TEXT
    assert by_name["source_tile"].nullable is False
    assert by_name["rast"].type is ColumnType.RASTER
    assert by_name["checksum"].type is ColumnType.TEXT
    assert by_name["srid"].type is ColumnType.INTEGER
    assert by_name["min_x"].type is ColumnType.DOUBLE
    assert schema.raster_column_names() == {"rast"}


def test_rast_is_excluded_from_the_content_hash():
    # checksum carries the change signal; the raster value itself is stored
    # but hash-excluded (metadata=True).
    schema = _reader(seeded_source()).schema(make_elevation_spec("raw_data"))
    assert schema.metadata_column_names() == {"rast"}


def test_write_mode_is_scd2_on_tile_id():
    reader = _reader(seeded_source())
    mode = reader.write_mode(make_elevation_spec("raw_data"), mode="full")
    assert isinstance(mode, SCD2)
    assert mode == SCD2(entity_key=["tile_id"])
    assert mode.invalidate_missing is False


def test_cursor_spec_is_the_source_tile():
    reader = _reader(seeded_source())
    assert reader.cursor_spec(make_elevation_spec("raw_data")) == CursorSpec("source_tile")


# ------------------------------------------------------------------ tile enumeration


def test_tiles_come_from_the_bbox_sorted():
    reader = _reader(seeded_source())
    assert reader.tiles(make_elevation_spec("raw_data", bbox=TWO_TILE_BBOX)) == [
        "n42w088",
        "n43w088",
    ]


def test_tiles_narrowed_by_the_spec():
    reader = _reader(seeded_source())
    spec = make_elevation_spec("raw_data", bbox=TWO_TILE_BBOX, tiles=["n43w088"])
    assert reader.tiles(spec) == ["n43w088"]


def test_available_reflects_source_staging():
    reader = _reader(seeded_source(["n42w088"]))
    spec = make_elevation_spec("raw_data", bbox=TWO_TILE_BBOX)
    assert reader.available(dataclasses.replace(spec, tiles=["n42w088"])) is True
    assert reader.available(dataclasses.replace(spec, tiles=["n43w088"])) is False


# ------------------------------------------------------------------ read


def test_read_rows_match_the_declared_schema():
    reader = _reader(seeded_source(["n42w088"]))
    spec = make_elevation_spec("raw_data", bbox=SINGLE_TILE_BBOX)
    schema = reader.schema(spec)

    rows = [r for b in reader.read(spec, since=None) for r in b]

    assert rows
    for row in rows:
        assert set(row.keys()) == set(schema.column_names())


def test_read_namespaces_tile_ids_and_stamps_source_tile():
    reader = _reader(seeded_source(["n42w088"]))
    rows = [
        r
        for b in reader.read(make_elevation_spec("raw_data", bbox=SINGLE_TILE_BBOX), since=None)
        for r in b
    ]
    assert all(r["tile_id"].startswith("n42w088/") for r in rows)
    assert all(r["source_tile"] == "n42w088" for r in rows)
    assert len({r["tile_id"] for r in rows}) == len(rows)


def test_read_clips_sub_tiles_to_the_bbox():
    reader = _reader(seeded_source(["n42w088"]))
    b = SINGLE_TILE_BBOX
    rows = [
        r
        for batch in reader.read(make_elevation_spec("raw_data", bbox=b), since=None)
        for r in batch
    ]

    # The bbox is a sub-degree slice of the tile, so clipping must drop some
    # of the 4x4 sub-tile grid — and every kept sub-tile intersects the bbox.
    assert 0 < len(rows) < 16
    for r in rows:
        assert not (
            r["max_x"] < b.west
            or r["min_x"] > b.east
            or r["max_y"] < b.south
            or r["min_y"] > b.north
        )


def test_read_batches_by_batch_size():
    reader = _reader(seeded_source(["n42w088"]), batch_size=3)
    batches = list(reader.read(make_elevation_spec("raw_data", bbox=SINGLE_TILE_BBOX), since=None))
    assert all(len(b) <= 3 for b in batches)
    assert sum(len(b) for b in batches) > 3  # more than one batch


def test_read_since_collects_only_tiles_strictly_after():
    source = seeded_source(["n42w088", "n43w088"])
    reader = _reader(source)
    spec = make_elevation_spec("raw_data", bbox=TWO_TILE_BBOX)

    rows = [r for b in reader.read(spec, since=Cursor("n42w088")) for r in b]

    assert fake_client(reader).downloaded == ["n43w088"]
    assert {r["source_tile"] for r in rows} == {"n43w088"}


def test_read_since_at_the_frontier_downloads_nothing():
    reader = _reader(seeded_source(["n42w088", "n43w088"]))
    spec = make_elevation_spec("raw_data", bbox=TWO_TILE_BBOX)

    assert list(reader.read(spec, since=Cursor("n43w088"))) == []
    assert fake_client(reader).downloaded == []


def test_read_skips_tiles_missing_at_source():
    reader = _reader(seeded_source(["n42w088"]))  # n43w088 not staged
    spec = make_elevation_spec("raw_data", bbox=TWO_TILE_BBOX)

    rows = [r for b in reader.read(spec, since=None) for r in b]

    assert {r["source_tile"] for r in rows} == {"n42w088"}
    assert fake_client(reader).downloaded == ["n42w088"]


# ------------------------------------------------------------------ extract_cursor


def test_extract_cursor_returns_max_source_tile():
    reader = _reader(seeded_source())
    batch = [
        {"source_tile": "n42w088"},
        {"source_tile": "n43w088"},
        {"source_tile": "n42w088"},
    ]
    assert reader.extract_cursor(batch) == Cursor("n43w088")


def test_extract_cursor_none_when_empty():
    reader = _reader(seeded_source())
    assert reader.extract_cursor([]) is None
