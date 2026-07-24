"""
Offline tests for the 3DEP client's tile addressing (name, bbox
enumeration, URL building). Network calls (tile_exists/download_tile)
are not exercised here — those belong in a live smoke test.
"""

from __future__ import annotations

import pytest

from datadongle.collectors.threedep.client import (
    tile_name,
    tile_url,
    tiles_for_bbox,
)
from datadongle.geo import BBox

# --------------------------------------------------------------------------
# tile_name
# --------------------------------------------------------------------------


def test_tile_name_zero_pads():
    assert tile_name(42, -88) == "n42w088"
    assert tile_name(7, -9) == "n07w009"
    assert tile_name(38, -123) == "n38w123"


def test_tile_name_rejects_eastern_or_southern():
    with pytest.raises(ValueError, match="western"):
        tile_name(42, 5)
    with pytest.raises(ValueError, match="northern"):
        tile_name(-1, -88)


# --------------------------------------------------------------------------
# tiles_for_bbox
# --------------------------------------------------------------------------


def test_chicago_spans_two_lat_tiles():
    # north 42.05 crosses into the [42,43] cell -> n42 and n43.
    bbox = BBox(south=41.62, west=-87.97, north=42.05, east=-87.5)
    assert tiles_for_bbox(bbox) == ["n42w088", "n43w088"]


def test_detroit_spans_two_lon_tiles():
    bbox = BBox(south=42.24, west=-83.29, north=42.46, east=-82.89)
    assert tiles_for_bbox(bbox) == ["n43w083", "n43w084"]


def test_bbox_spanning_grid_yields_all_cells():
    # lat [37,39): n38,n39 ; lon [-123,-121): w123,w122 -> 4 tiles
    bbox = BBox(south=37.2, west=-122.6, north=38.5, east=-121.7)
    assert tiles_for_bbox(bbox) == [
        "n38w122",
        "n38w123",
        "n39w122",
        "n39w123",
    ]


def test_integer_north_edge_does_not_pull_extra_tile():
    # north exactly 42.0 stays within [41,42] -> only n42, not n43.
    bbox = BBox(south=41.6, west=-87.9, north=42.0, east=-87.5)
    assert tiles_for_bbox(bbox) == ["n42w088"]


def test_single_cell_bbox():
    bbox = BBox(south=41.7, west=-87.8, north=41.9, east=-87.6)
    assert tiles_for_bbox(bbox) == ["n42w088"]


# --------------------------------------------------------------------------
# tile_url
# --------------------------------------------------------------------------


def test_tile_url_for_each_product():
    assert tile_url("n42w088", "13").endswith(
        "/Elevation/13/TIFF/current/n42w088/USGS_13_n42w088.tif"
    )
    assert tile_url("n42w088", "1").endswith("/Elevation/1/TIFF/current/n42w088/USGS_1_n42w088.tif")


def test_tile_url_rejects_unsupported_product():
    with pytest.raises(ValueError, match="unsupported product"):
        tile_url("n42w088", "1m")
