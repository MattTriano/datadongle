"""ThreeDEPDatasetSpec validation and defaults (no network, no database)."""

from __future__ import annotations

import pytest

from datadongle.collectors.threedep.spec import ThreeDEPDatasetSpec
from datadongle.geo import BBox


def _bbox():
    return BBox(south=41.62, west=-87.97, north=42.05, east=-87.5)


def test_spec_defaults():
    spec = ThreeDEPDatasetSpec(
        name="chicago_elevation", target_table="chicago_elevation", bbox=_bbox()
    )
    assert spec.source == "3dep"
    assert spec.target_schema == "raw_data"
    assert spec.product == "13"
    assert spec.entity_key == ["tile_id"]
    assert spec.tiles is None
    assert spec.dataset_id == "chicago_elevation"


def test_spec_requires_name():
    with pytest.raises(ValueError, match="name is required"):
        ThreeDEPDatasetSpec(target_table="x", bbox=_bbox())


def test_spec_requires_target_table():
    with pytest.raises(ValueError, match="target_table is required"):
        ThreeDEPDatasetSpec(name="x", bbox=_bbox())


def test_spec_requires_bbox():
    with pytest.raises(ValueError, match="bbox is required"):
        ThreeDEPDatasetSpec(name="x", target_table="x")


def test_spec_rejects_bad_product():
    with pytest.raises(ValueError, match="product must be"):
        ThreeDEPDatasetSpec(name="x", target_table="x", bbox=_bbox(), product="1m")


def test_spec_rejects_overridden_entity_key():
    with pytest.raises(ValueError, match="fixed to"):
        ThreeDEPDatasetSpec(name="x", target_table="x", bbox=_bbox(), entity_key=["foo"])


def test_spec_rejects_non_nad83_bbox():
    with pytest.raises(ValueError, match="NAD83"):
        ThreeDEPDatasetSpec(
            name="x",
            target_table="x",
            bbox=BBox(south=41.62, west=-87.97, north=42.05, east=-87.5, srid=4326),
        )


def test_spec_accepts_tiles_the_bbox_needs():
    spec = ThreeDEPDatasetSpec(name="x", target_table="x", bbox=_bbox(), tiles=["n42w088"])
    assert spec.tiles == ["n42w088"]


def test_spec_rejects_tiles_outside_the_bbox():
    with pytest.raises(ValueError, match="not among the tiles"):
        ThreeDEPDatasetSpec(name="x", target_table="x", bbox=_bbox(), tiles=["n38w123"])
