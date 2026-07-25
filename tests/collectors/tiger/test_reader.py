"""Unit behaviors of TigerReader: protocol conformance, schema, transforms.

End-to-end collection through the family driver lives in ``test_driver.py``.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest
from shapely.geometry import LineString, Polygon

from datadongle.collectors.tiger.reader import (
    TigerReader,
    _auto_detect_entity_key,
    _extract_county_fips,
    _fiona_to_column_type,
)
from datadongle.collectors.tiger.spec import TigerDatasetSpec
from datadongle.core.cursor import CursorSpec  # noqa: F401  (documents the None return)
from datadongle.core.engine import TableRef
from datadongle.core.reader import SourceReader
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2, Append

from .helpers import (
    FakeTigerClient,
    as_client_factory,
    tiger_dir_url,
    tiger_url,
    write_shapefile_zip,
)

# ---------------------------------------------------------------- pure helpers


class TestFionaTypeMapping:
    @pytest.mark.parametrize(
        "fiona_type, expected",
        [
            ("str:80", ColumnType.TEXT),
            ("int:10", ColumnType.BIGINT),
            ("int32", ColumnType.INTEGER),
            ("int64:20", ColumnType.BIGINT),
            ("float:24.15", ColumnType.DOUBLE),
            ("date", ColumnType.DATE),
            ("datetime", ColumnType.TIMESTAMPTZ),
            ("exotic:9", ColumnType.TEXT),
        ],
    )
    def test_maps(self, fiona_type, expected):
        assert _fiona_to_column_type(fiona_type) == expected


class TestAutoDetectEntityKey:
    def test_detects_geoid(self):
        assert _auto_detect_entity_key(["geoid", "statefp"], True) == ["geoid", "vintage"]

    def test_detects_linearid(self):
        assert _auto_detect_entity_key(["linearid", "fullname"], True) == ["linearid", "vintage"]

    def test_matches_year_suffixed_variant(self):
        assert _auto_detect_entity_key(["geoid20", "aland"], True) == ["geoid20", "vintage"]

    def test_returns_none_without_candidate(self):
        assert _auto_detect_entity_key(["name", "mtfcc"], True) is None

    def test_respects_lowercase_false(self):
        assert _auto_detect_entity_key(["GEOID", "STATEFP"], False) == ["GEOID", "vintage"]


class TestExtractCountyFips:
    def test_five_digit(self):
        assert _extract_county_fips("tl_2024_17031_roads.zip", 2024) == "17031"

    def test_state_level_is_none(self):
        assert _extract_county_fips("tl_2024_17_tract.zip", 2024) is None

    def test_national_is_none(self):
        assert _extract_county_fips("tl_2024_us_state.zip", 2024) is None

    def test_wrong_vintage_is_none(self):
        assert _extract_county_fips("tl_2023_17031_roads.zip", 2024) is None


# ---------------------------------------------------------------- fixtures


def _poly(i: int) -> Polygon:
    return Polygon([(i, i), (i, i + 1), (i + 1, i + 1), (i + 1, i), (i, i)])


def _tract_reader(tmp_path, *, vintage=2024, state="17"):
    """A reader over a fake client serving one state TRACT shapefile."""
    zip_path = write_shapefile_zip(
        tmp_path,
        f"tl_{vintage}_{state}_tract",
        [
            {"geometry": _poly(0), "GEOID": f"{state}031000100", "NAME": "Tract 1", "ALAND": "123"},
            {"geometry": _poly(1), "GEOID": f"{state}031000200", "NAME": "Tract 2", "ALAND": "456"},
        ],
        geom_type="Polygon",
        prop_schema={"GEOID": "str:11", "NAME": "str:50", "ALAND": "int:14"},
    )
    files = {tiger_url(vintage, "TRACT", state): zip_path}
    return TigerReader(client_factory=as_client_factory(FakeTigerClient(files, {})))


def _tract_spec(**overrides) -> TigerDatasetSpec:
    kwargs: dict[str, Any] = dict(
        name="census_tracts",
        layer="TRACT",
        vintages=[2024],
        target_table="census_tracts",
        target_schema="raw_data",
        state_fips=["17"],
    )
    kwargs.update(overrides)
    return TigerDatasetSpec(**kwargs)


# ---------------------------------------------------------------- protocol


def test_reader_satisfies_the_protocol(tmp_path):
    assert isinstance(_tract_reader(tmp_path), SourceReader)


def test_target_from_spec(tmp_path):
    reader = _tract_reader(tmp_path)
    assert reader.target(_tract_spec()) == TableRef("census_tracts", "raw_data")


def test_dataset_id_encodes_vintage_and_unit(tmp_path):
    reader = _tract_reader(tmp_path)
    spec = dataclasses.replace(_tract_spec(), vintages=[2024], state_fips=["17"])
    assert reader.dataset_id(spec) == "census_tracts/2024/17"


def test_not_incrementally_queryable(tmp_path):
    reader = _tract_reader(tmp_path)
    assert reader.cursor_spec(_tract_spec()) is None
    assert reader.extract_cursor([{"vintage": 2024}]) is None


# ---------------------------------------------------------------- schema


def test_schema_maps_types_and_declares_geometry(tmp_path):
    reader = _tract_reader(tmp_path)
    schema = reader.schema(_tract_spec())
    by_name = {c.name: c for c in schema.columns}

    assert by_name["geoid"].type is ColumnType.TEXT
    assert by_name["name"].type is ColumnType.TEXT
    assert by_name["aland"].type is ColumnType.BIGINT
    assert by_name["vintage"].type is ColumnType.INTEGER
    assert by_name["vintage"].nullable is False

    geom = by_name["geom"]
    assert geom.type is ColumnType.GEOMETRY
    assert geom.geometry.kind == "MultiPolygon"  # promoted from Polygon
    assert geom.geometry.srid == 4326


def test_write_mode_auto_detects_scd2(tmp_path):
    reader = _tract_reader(tmp_path)
    mode = reader.write_mode(_tract_spec(), mode="full")
    assert isinstance(mode, SCD2)
    assert mode.entity_key == ["geoid", "vintage"]


def test_explicit_entity_key_wins(tmp_path):
    reader = _tract_reader(tmp_path)
    mode = reader.write_mode(_tract_spec(entity_key=["geoid", "vintage"]), mode="full")
    assert isinstance(mode, SCD2)
    assert mode.entity_key == ["geoid", "vintage"]


def test_no_id_column_falls_back_to_append(tmp_path):
    zip_path = write_shapefile_zip(
        tmp_path,
        "tl_2024_us_coastline",
        [{"geometry": LineString([(0, 0), (1, 1)]), "NAME": "Atlantic", "MTFCC": "C10"}],
        geom_type="LineString",
        prop_schema={"NAME": "str:40", "MTFCC": "str:5"},
    )
    files = {tiger_url(2024, "COASTLINE", None): zip_path}
    reader = TigerReader(client_factory=as_client_factory(FakeTigerClient(files, {})))
    spec = _tract_spec(
        name="coastline", layer="COASTLINE", target_table="coastline", state_fips=None
    )

    assert isinstance(reader.write_mode(spec, mode="full"), Append)


# ---------------------------------------------------------------- read transforms


def test_read_stamps_vintage_and_passes_geometry_through(tmp_path):
    reader = _tract_reader(tmp_path)
    spec = dataclasses.replace(_tract_spec(), vintages=[2024], state_fips=["17"])

    batches = list(reader.read(spec, since=None))
    rows = [r for b in batches for r in b]

    assert len(rows) == 2
    assert all(r["vintage"] == 2024 for r in rows)
    assert all(r["geoid"].startswith("17031") for r in rows)
    # Geometry is EWKB-hex from the parser, promoted to MultiPolygon (type 0x06).
    assert rows[0]["geom"].startswith("0106000020")


def test_county_scope_injects_synthetic_fips(tmp_path):
    zip_path = write_shapefile_zip(
        tmp_path,
        "tl_2024_17031_roads",
        [{"geometry": LineString([(0, 0), (1, 1)]), "LINEARID": "1", "FULLNAME": "Main St"}],
        geom_type="LineString",
        prop_schema={"LINEARID": "str:22", "FULLNAME": "str:100"},
    )
    files = {tiger_url(2024, "ROADS", "17031"): zip_path}
    reader = TigerReader(client_factory=as_client_factory(FakeTigerClient(files, {})))
    spec = dataclasses.replace(
        _tract_spec(name="roads", layer="ROADS", target_table="roads"),
        vintages=[2024],
        state_fips=["17031"],
    )

    rows = [r for b in reader.read(spec, since=None) for r in b]
    assert rows[0]["statefp"] == "17"
    assert rows[0]["countyfp"] == "031"


def test_county_schema_adds_synthetic_fips_columns(tmp_path):
    zip_path = write_shapefile_zip(
        tmp_path,
        "tl_2024_17031_roads",
        [{"geometry": LineString([(0, 0), (1, 1)]), "LINEARID": "1", "FULLNAME": "Main St"}],
        geom_type="LineString",
        prop_schema={"LINEARID": "str:22", "FULLNAME": "str:100"},
    )
    files = {tiger_url(2024, "ROADS", "17031"): zip_path}
    listings = {tiger_dir_url(2024, "ROADS"): _roads_listing()}
    reader = TigerReader(client_factory=as_client_factory(FakeTigerClient(files, listings)))
    spec = _tract_spec(name="roads", layer="ROADS", target_table="roads", state_fips=["17"])

    names = {c.name for c in reader.schema(spec).columns}
    assert {"statefp", "countyfp", "linearid", "vintage", "geom"} <= names


def test_units_enumerates_counties_filtered_to_states(tmp_path):
    files = {}  # not needed for enumeration
    listings = {tiger_dir_url(2024, "ROADS"): _roads_listing()}
    reader = TigerReader(client_factory=as_client_factory(FakeTigerClient(files, listings)))
    spec = _tract_spec(name="roads", layer="ROADS", target_table="roads", state_fips=["17"])

    # 17031 and 17043 are in IL; 06037 (CA) is filtered out.
    assert reader.units(spec, 2024) == ["17031", "17043"]


def _roads_listing() -> str:
    from .helpers import _directory_html

    return _directory_html(
        ["tl_2024_17031_roads.zip", "tl_2024_17043_roads.zip", "tl_2024_06037_roads.zip"]
    )
