"""Unit tests for the Overpass QL query builder."""

from __future__ import annotations

from typing import Any

import pytest

from datadongle.collectors.osm.query import OverpassAPIQuery, Regex
from datadongle.geo import BBox

BBOX = BBox(41.62, -87.97, 42.05, -87.5)


def _q(**kw) -> OverpassAPIQuery:
    base: dict[str, Any] = dict(element_types=["node", "way"], tag_filters=[{"amenity": "cafe"}])
    base.update(kw)
    return OverpassAPIQuery(**base)


# ------------------------------------------------------------------ validation


def test_empty_element_types_raises():
    with pytest.raises(ValueError, match="element_types must be non-empty"):
        _q(element_types=[])


def test_invalid_element_type_raises():
    with pytest.raises(ValueError, match="Invalid element_types"):
        _q(element_types=["node", "planet"])


def test_empty_tag_filters_raises():
    with pytest.raises(ValueError, match="tag_filters must be non-empty"):
        _q(tag_filters=[])


def test_empty_filter_group_raises():
    with pytest.raises(ValueError, match=r"tag_filters\[0\] is empty"):
        _q(tag_filters=[{}])


def test_bbox_and_area_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        _q(bbox=BBOX, area_name="Chicago")


def test_invalid_out_mode_raises():
    with pytest.raises(ValueError, match="out_mode must be"):
        _q(out_mode="full")


# ------------------------------------------------------------------ template state


def test_template_to_ql_raises():
    with pytest.raises(ValueError, match="no spatial extent"):
        _q().to_ql()


def test_for_bbox_and_for_area_are_mutually_clearing():
    template = _q()
    bound_bbox = template.for_bbox(BBOX)
    assert bound_bbox.bbox == BBOX and bound_bbox.area_name is None

    bound_area = bound_bbox.for_area("Chicago")
    assert bound_area.area_name == "Chicago" and bound_area.bbox is None
    # original template is unchanged (replace returns a copy)
    assert template.bbox is None and template.area_name is None


# ------------------------------------------------------------------ rendering


def test_to_ql_bbox_structure():
    ql = _q().for_bbox(BBOX).to_ql()
    assert ql.startswith("[out:json][timeout:180];")
    assert ql.endswith("out geom meta;")
    assert 'node["amenity"="cafe"](41.62,-87.97,42.05,-87.5);' in ql
    assert 'way["amenity"="cafe"](41.62,-87.97,42.05,-87.5);' in ql


def test_to_ql_area_structure():
    ql = _q().for_area("Chicago").to_ql()
    assert 'area["name"="Chicago"]->.searchArea;' in ql
    assert 'node["amenity"="cafe"](area.searchArea);' in ql


def test_to_ql_date_filter_appends_newer():
    ql = _q().for_bbox(BBOX).to_ql(date_filter="2026-04-01T00:00:00Z")
    assert '(newer:"2026-04-01T00:00:00Z")' in ql
    # the newer filter precedes the spatial extent in each selector
    assert '["amenity"="cafe"](newer:"2026-04-01T00:00:00Z")(41.62,' in ql


# ------------------------------------------------------------------ tag filters


def test_tag_filter_key_exists():
    ql = _q(tag_filters=[{"amenity": None}]).for_bbox(BBOX).to_ql()
    assert 'node["amenity"](41.62,' in ql


def test_tag_filter_exact_match():
    ql = _q(tag_filters=[{"amenity": "cafe"}]).for_bbox(BBOX).to_ql()
    assert '["amenity"="cafe"]' in ql


def test_tag_filter_value_list_escaped_regex():
    ql = _q(tag_filters=[{"shop": ["supermarket", "convenience"]}]).for_bbox(BBOX).to_ql()
    assert '["shop"~"^(supermarket|convenience)$"]' in ql


def test_tag_filter_list_escapes_metacharacters():
    # _regex_escape adds a backslash before each metachar; _quote then escapes
    # that backslash for the QL string literal, so it renders doubled.
    ql = _q(tag_filters=[{"name": ["a.b", "c+d"]}]).for_bbox(BBOX).to_ql()
    assert r'["name"~"^(a\\.b|c\\+d)$"]' in ql


def test_tag_filter_regex_passthrough_and_case_insensitive():
    ql = _q(tag_filters=[{"name": Regex("^cafe", case_insensitive=True)}]).for_bbox(BBOX).to_ql()
    assert '["name"~"^cafe",i]' in ql


def test_tag_filter_multiple_entries_in_group_are_anded():
    ql = _q(tag_filters=[{"amenity": "cafe", "cuisine": "coffee_shop"}]).for_bbox(BBOX).to_ql()
    assert '["amenity"="cafe"]["cuisine"="coffee_shop"]' in ql
