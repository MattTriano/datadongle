"""Unit tests for CKANDatasetSpec validation."""

from __future__ import annotations

import pytest

from datadongle.collectors.ckan.spec import CKANDatasetSpec


def _spec(**kw) -> CKANDatasetSpec:
    base = dict(
        name="chicago_food_inspections",
        base_url="https://data.cityofchicago.org",
        dataset_id="4ijn-s7e5",
        target_table="chicago_food_inspections",
        resource_format="CSV",
    )
    base.update(kw)
    return CKANDatasetSpec(**base)


def test_defaults():
    spec = _spec()
    assert spec.target_schema == "raw_data"
    assert spec.source == "ckan"
    assert spec.entity_key is None
    assert spec.resource_ids is None


def test_base_url_trailing_slash_stripped():
    assert _spec(base_url="https://data.example.gov/").base_url == "https://data.example.gov"


def test_requires_resource_ids_or_format():
    with pytest.raises(ValueError, match="resource_ids .* or.*resource_format"):
        _spec(resource_format=None)


def test_resource_ids_alone_is_valid():
    spec = _spec(resource_format=None, resource_ids=["a1b2c3d4"])
    assert spec.resource_ids == ["a1b2c3d4"]
