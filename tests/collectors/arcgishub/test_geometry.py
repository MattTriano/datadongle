"""Unit tests for the ArcGIS geometry -> EWKT conversion."""

from __future__ import annotations

from datadongle.collectors.arcgishub.reader import _geometry_to_ewkt, _ring_is_clockwise


def test_point():
    ewkt = _geometry_to_ewkt({"x": -79.4, "y": 43.7}, "esriGeometryPoint", 4326)
    assert ewkt == "SRID=4326;POINT(-79.4 43.7)"


def test_point_missing_coordinate_is_none():
    assert _geometry_to_ewkt({"x": -79.4}, "esriGeometryPoint", 4326) is None


def test_multipoint():
    geom = {"points": [[0, 0], [1, 1]]}
    assert _geometry_to_ewkt(geom, "esriGeometryMultipoint", 4326) == (
        "SRID=4326;MULTIPOINT((0 0), (1 1))"
    )


def test_polyline_becomes_multilinestring():
    geom = {"paths": [[[0, 0], [1, 1]], [[2, 2], [3, 3]]]}
    assert _geometry_to_ewkt(geom, "esriGeometryPolyline", 4326) == (
        "SRID=4326;MULTILINESTRING((0 0, 1 1), (2 2, 3 3))"
    )


# ArcGIS convention: clockwise ring = outer, counter-clockwise = hole.
_OUTER: list[list[float]] = [[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]  # clockwise
_HOLE: list[list[float]] = [[2, 2], [4, 2], [4, 4], [2, 4], [2, 2]]  # counter-clockwise
_OUTER2: list[list[float]] = [[20, 20], [20, 30], [30, 30], [30, 20], [20, 20]]  # clockwise


def test_ring_orientation_helper():
    assert _ring_is_clockwise(_OUTER) is True
    assert _ring_is_clockwise(_HOLE) is False


def test_polygon_single_ring():
    ewkt = _geometry_to_ewkt({"rings": [_OUTER]}, "esriGeometryPolygon", 4326)
    assert ewkt == "SRID=4326;MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)))"


def test_polygon_hole_groups_with_preceding_outer():
    ewkt = _geometry_to_ewkt({"rings": [_OUTER, _HOLE]}, "esriGeometryPolygon", 4326)
    assert ewkt == (
        "SRID=4326;MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2)))"
    )


def test_polygon_second_outer_starts_new_polygon():
    ewkt = _geometry_to_ewkt({"rings": [_OUTER, _OUTER2]}, "esriGeometryPolygon", 4326)
    assert ewkt == (
        "SRID=4326;MULTIPOLYGON("
        "((0 0, 0 10, 10 10, 10 0, 0 0)), ((20 20, 20 30, 30 30, 30 20, 20 20)))"
    )


def test_none_geometry_is_none():
    assert _geometry_to_ewkt(None, "esriGeometryPoint", 4326) is None


def test_empty_rings_is_none():
    assert _geometry_to_ewkt({"rings": []}, "esriGeometryPolygon", 4326) is None


def test_unknown_geometry_type_is_none():
    assert _geometry_to_ewkt({"x": 1, "y": 2}, "esriGeometryEnvelope", 4326) is None
