"""EIADatasetSpec field validation and defaults."""

from __future__ import annotations

import pytest

from datadongle.collectors.eia.spec import EIADatasetSpec


def test_defaults_and_dataset_id():
    spec = EIADatasetSpec(
        name="x",
        target_table="x",
        route_path="electricity/retail-sales",
        frequency="monthly",
        data_columns=["price"],
    )
    assert spec.source == "eia"
    assert spec.target_schema == "raw_data"
    assert spec.facets == {}
    assert spec.entity_key is None
    assert spec.dataset_id == "electricity/retail-sales/monthly"


def test_route_path_is_stripped_of_slashes():
    spec = EIADatasetSpec(
        name="x",
        target_table="x",
        route_path="/electricity/retail-sales/",
        frequency="monthly",
        data_columns=["price"],
    )
    assert spec.route_path == "electricity/retail-sales"


@pytest.mark.parametrize(
    "overrides",
    [
        {"route_path": "   "},
        {"frequency": ""},
        {"data_columns": []},
    ],
)
def test_invalid_specs_raise(overrides):
    kwargs = dict(
        name="x",
        target_table="x",
        route_path="electricity/retail-sales",
        frequency="monthly",
        data_columns=["price"],
    )
    kwargs.update(overrides)
    with pytest.raises(ValueError):
        EIADatasetSpec(**kwargs)
