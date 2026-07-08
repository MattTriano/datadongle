"""Unit tests for OSM element -> geometry assembly."""

from __future__ import annotations

from shapely.geometry import LineString, Point, Polygon

from datadongle.collectors.osm.geometry import (
    element_to_wkt,
    is_area,
    _element_to_shape,
)


# ------------------------------------------------------------------ is_area


def test_area_tag_yes_forces_polygon():
    assert is_area({"area": "yes", "highway": "residential"}) is True


def test_area_tag_no_forces_line():
    assert is_area({"area": "no", "building": "yes"}) is False


def test_rule_all_makes_polygon():
    assert is_area({"building": "yes"}) is True


def test_rule_all_with_value_no_is_not_area():
    assert is_area({"building": "no"}) is False


def test_whitelist_rule():
    assert is_area({"highway": "services"}) is True
    assert is_area({"highway": "residential"}) is False


def test_blacklist_rule():
    assert is_area({"natural": "water"}) is True
    assert is_area({"natural": "coastline"}) is False


def test_untagged_way_is_not_area():
    assert is_area({}) is False


# ------------------------------------------------------------------ element_to_shape


def test_node_becomes_point():
    geom = _element_to_shape({"type": "node", "id": 1, "lon": -87.6, "lat": 41.8})
    assert isinstance(geom, Point)
    assert (geom.x, geom.y) == (-87.6, 41.8)


def test_node_missing_coords_is_none():
    assert _element_to_shape({"type": "node", "id": 1}) is None


def _way(coords, tags=None):
    return {
        "type": "way",
        "id": 1,
        "geometry": [{"lon": x, "lat": y} for x, y in coords],
        "tags": tags or {},
    }


def test_open_way_becomes_linestring():
    geom = _element_to_shape(_way([(0, 0), (1, 0), (2, 1)]))
    assert isinstance(geom, LineString)


def test_closed_area_way_becomes_polygon():
    ring = [(0, 0), (1, 0), (1, 1), (0, 0)]
    geom = _element_to_shape(_way(ring, tags={"building": "yes"}))
    assert isinstance(geom, Polygon)


def test_closed_non_area_way_stays_linestring():
    ring = [(0, 0), (1, 0), (1, 1), (0, 0)]
    geom = _element_to_shape(_way(ring, tags={"highway": "footway"}))
    assert isinstance(geom, LineString)


def test_non_multipolygon_relation_is_none():
    geom = _element_to_shape({"type": "relation", "id": 1, "tags": {"type": "route"}})
    assert geom is None


def test_simple_multipolygon_relation():
    outer = [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)]
    element = {
        "type": "relation",
        "id": 1,
        "tags": {"type": "multipolygon"},
        "members": [
            {
                "type": "way",
                "role": "outer",
                "geometry": [{"lon": x, "lat": y} for x, y in outer],
            }
        ],
    }
    geom = _element_to_shape(element)
    assert isinstance(geom, Polygon)
    assert geom.area == 16


# ------------------------------------------------------------------ element_to_wkt


def test_element_to_wkt_returns_wkt_string():
    assert element_to_wkt({"type": "node", "id": 1, "lon": 1.0, "lat": 2.0}) == "POINT (1 2)"


def test_element_to_wkt_none_geometry():
    assert element_to_wkt({"type": "relation", "id": 1, "tags": {}}) is None


def test_element_to_wkt_swallows_assembly_errors():
    # A malformed multipolygon (no valid outer rings) is logged and returns None.
    element = {
        "type": "relation",
        "id": 1,
        "tags": {"type": "multipolygon"},
        "members": [{"type": "way", "role": "outer", "geometry": [{"lon": 0, "lat": 0}]}],
    }
    assert element_to_wkt(element) is None
