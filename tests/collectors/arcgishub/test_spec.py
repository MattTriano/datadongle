"""Unit tests for ArcGISHubDatasetSpec."""

from __future__ import annotations

from typing import Any

from datadongle.collectors.arcgishub.spec import ArcGISHubDatasetSpec


def _spec(**over) -> ArcGISHubDatasetSpec:
    base: dict[str, Any] = {
        "name": "tps_arrests",
        "base_url": "https://data.example.ca",
        "item_id": "item123",
        "target_table": "tps_arrests",
    }
    base.update(over)
    return ArcGISHubDatasetSpec(**base)


def test_defaults():
    spec = _spec()
    assert spec.target_schema == "raw_data"
    assert spec.layer_index == 0
    assert spec.where == "1=1"
    assert spec.source == "arcgis_hub"
    assert spec.min_field_overlap == 0.8
    assert spec.entity_key is None


def test_dataset_id_single_layer_includes_index():
    assert _spec(layer_index=2).dataset_id == "item123:2"


def test_dataset_id_multi_layer_is_marked_multi():
    assert _spec(layer_index=[0, 1]).dataset_id == "item123:multi"
    assert _spec(layer_index="all").dataset_id == "item123:multi"


def test_is_multi_layer():
    assert _spec(layer_index=0).is_multi_layer is False
    assert _spec(layer_index=[0, 1]).is_multi_layer is True
    assert _spec(layer_index="all").is_multi_layer is True
