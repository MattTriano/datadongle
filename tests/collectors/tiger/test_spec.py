"""Tests for TigerDatasetSpec."""

from __future__ import annotations

from typing import Any

import pytest

from datadongle.collectors.tiger.spec import ALL_STATE_FIPS, TigerDatasetSpec


def _spec(**overrides) -> TigerDatasetSpec:
    kwargs: dict[str, Any] = dict(
        name="census_tracts",
        layer="TRACT",
        vintages=[2023, 2024],
        target_table="census_tracts",
        target_schema="raw_data",
    )
    kwargs.update(overrides)
    return TigerDatasetSpec(**kwargs)


class TestValidation:
    def test_rejects_unknown_source(self):
        with pytest.raises(ValueError, match="Unknown source"):
            _spec(source="geojson")

    def test_accepts_tiger_and_cartographic(self):
        assert _spec(source="tiger").source == "tiger"
        assert _spec(source="cartographic").source == "cartographic"

    def test_defaults(self):
        spec = _spec()
        assert spec.source == "tiger"
        assert spec.resolution == "500k"
        assert spec.lowercase_columns is True
        assert spec.entity_key is None


class TestScope:
    def test_state_layer(self):
        assert _spec(layer="TRACT").scope == "state"

    def test_national_layer(self):
        assert _spec(layer="PRIMARYROADS").scope == "national"

    def test_county_layer(self):
        assert _spec(layer="ROADS").scope == "county"

    def test_unknown_layer_defaults_to_state(self):
        assert _spec(layer="SOMETHINGNEW").scope == "state"

    def test_scope_is_case_insensitive(self):
        assert _spec(layer="tract").scope == "state"


class TestStates:
    def test_defaults_to_all_states(self):
        assert _spec().states == ALL_STATE_FIPS

    def test_uses_specified_state_fips(self):
        assert _spec(state_fips=["17", "18"]).states == ["17", "18"]


class TestDatasetId:
    def test_dataset_id_is_target_table(self):
        assert _spec(target_table="census_tracts").dataset_id == "census_tracts"
