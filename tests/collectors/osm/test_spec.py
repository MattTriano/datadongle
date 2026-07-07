"""Unit tests for OSMDatasetSpec (validation + tag-key normalization)."""

from __future__ import annotations

import logging

import pytest

from datadongle.collectors.osm.query import OverpassAPIQuery
from datadongle.collectors.osm.spec import OSMDatasetSpec
from datadongle.geo import BBox


def _query() -> OverpassAPIQuery:
    return OverpassAPIQuery(
        element_types=["node"],
        tag_filters=[{"amenity": "cafe"}],
    ).for_bbox(BBox(41.62, -87.97, 42.05, -87.5))


def _spec(**kw) -> OSMDatasetSpec:
    base = dict(name="chi_cafes", target_table="chi_cafes", query=_query())
    base.update(kw)
    return OSMDatasetSpec(**base)


def test_defaults_and_dataset_id():
    spec = _spec()
    assert spec.source == "osm"
    assert spec.target_schema == "raw_data"
    assert spec.entity_key == ["osm_type", "osm_id"]
    assert spec.dataset_id == "chi_cafes"
    assert spec.promoted_columns == []


def test_promoted_tags_are_normalized_to_columns():
    spec = _spec(promoted_tags=["name", "addr:street", "name:en"])
    assert spec.tag_column_map == {
        "name": "name",
        "addr:street": "addr_street",
        "name:en": "name_en",
    }
    assert spec.promoted_columns == ["name", "addr_street", "name_en"]


def test_promoted_tag_collision_raises():
    # "addr:street" and "addr.street" both normalize to "addr_street"
    with pytest.raises(ValueError, match="normalize"):
        _spec(promoted_tags=["addr:street", "addr.street"])


def test_promoted_tag_normalizing_to_empty_raises():
    with pytest.raises(ValueError, match="empty column name"):
        _spec(promoted_tags=["***"])


def test_promoted_tag_starting_with_digit_raises():
    with pytest.raises(ValueError, match="starts with a digit"):
        _spec(promoted_tags=["3d"])


def test_empty_promoted_tag_string_raises():
    with pytest.raises(ValueError, match="empty string"):
        _spec(promoted_tags=[""])


@pytest.mark.parametrize("missing", ["name", "target_table"])
def test_required_fields(missing):
    kw = dict(name="x", target_table="x")
    kw[missing] = ""
    with pytest.raises(ValueError, match="required"):
        OSMDatasetSpec(query=_query(), **kw)


def test_query_required():
    with pytest.raises(ValueError, match="query is required"):
        OSMDatasetSpec(name="x", target_table="x", query=None)


def test_entity_key_override_warns(caplog):
    with caplog.at_level(logging.WARNING):
        _spec(entity_key=["osm_id"])
    assert any("entity_key" in r.message for r in caplog.records)


def test_standard_entity_key_does_not_warn(caplog):
    with caplog.at_level(logging.WARNING):
        _spec()
    assert not caplog.records
