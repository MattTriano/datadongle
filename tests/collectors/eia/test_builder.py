"""EIASpecBuilder — fills a spec from route metadata and validates the choices."""

from __future__ import annotations

import pytest

from datadongle.collectors.eia.builder import EIASpecBuilder
from datadongle.collectors.eia.metadata import EIAMetadata

from .helpers import FACET_VALUES, ROUTE_TREE, FakeEIAMetadataClient

LEAF = "electricity/retail-sales"

# A single-frequency leaf, to exercise the "sole frequency ⇒ no choice needed"
# path (the shared ROUTE_TREE leaf offers two).
SINGLE_FREQ_TREE = {
    "electricity/only-monthly": {
        "id": "only-monthly",
        "name": "One-frequency series",
        "frequency": [{"id": "monthly", "format": "YYYY-MM"}],
        "facets": [{"id": "stateid", "description": "State"}],
        "data": {"value": {"alias": "Value", "units": "widgets"}},
        "startPeriod": "2001-01",
        "endPeriod": "2024-01",
    },
    "electricity/no-freq": {
        "id": "no-freq",
        "name": "Not a data series",
        "routes": [{"id": "child", "name": "Child"}],
    },
}


def _builder(routes=None, facet_values=None) -> EIASpecBuilder:
    client = FakeEIAMetadataClient(routes or ROUTE_TREE, facet_values or FACET_VALUES)
    return EIASpecBuilder(EIAMetadata(client=client))


# ----------------------------------------------------------------------
# Filling omitted fields from the route
# ----------------------------------------------------------------------


def test_build_fills_all_measures_and_derives_entity_key():
    spec = _builder().build(LEAF, frequency="monthly")
    assert spec.data_columns == ["price", "revenue"]  # every measure the route has
    assert spec.entity_key == ["stateid", "sectorid", "period"]  # facet ids + period
    assert spec.route_path == LEAF
    assert spec.frequency == "monthly"


def test_build_derives_name_and_target_table():
    spec = _builder().build(LEAF, frequency="monthly")
    assert spec.target_table == "eia_electricity_retail_sales_monthly"
    assert spec.name == "eia_electricity_retail_sales_monthly"


def test_build_uses_sole_frequency_when_only_one_offered():
    spec = _builder(SINGLE_FREQ_TREE).build("electricity/only-monthly")
    assert spec.frequency == "monthly"
    assert spec.data_columns == ["value"]


def test_build_respects_explicit_overrides():
    spec = _builder().build(
        LEAF,
        frequency="annual",
        data_columns=["price"],
        name="my_name",
        target_table="my_table",
        target_schema="staging",
        start="2010-01",
        end="2020-12",
    )
    assert (spec.frequency, spec.data_columns) == ("annual", ["price"])
    assert (spec.name, spec.target_table, spec.target_schema) == (
        "my_name",
        "my_table",
        "staging",
    )
    assert (spec.start, spec.end) == ("2010-01", "2020-12")


def test_valid_facet_filter_passes_through():
    spec = _builder().build(LEAF, frequency="monthly", facets={"stateid": ["CO"]})
    assert spec.facets == {"stateid": ["CO"]}


# ----------------------------------------------------------------------
# entity_key: derive vs. explicit
# ----------------------------------------------------------------------


def test_entity_key_none_opts_out_of_history():
    spec = _builder().build(LEAF, frequency="monthly", entity_key=None)
    assert spec.entity_key is None  # ⇒ Append, not SCD2


def test_entity_key_explicit_list_is_respected():
    spec = _builder().build(LEAF, frequency="monthly", entity_key=["period"])
    assert spec.entity_key == ["period"]


# ----------------------------------------------------------------------
# Validation: bad choices fail at build time
# ----------------------------------------------------------------------


def test_ambiguous_frequency_raises_listing_options():
    with pytest.raises(ValueError, match="monthly.*annual|annual.*monthly"):
        _builder().build(LEAF)  # leaf offers two frequencies, none chosen


def test_unknown_frequency_raises():
    with pytest.raises(ValueError, match="Valid: monthly, annual"):
        _builder().build(LEAF, frequency="hourly")


def test_unknown_measure_raises_listing_valid():
    with pytest.raises(ValueError, match="Valid data_columns: price, revenue"):
        _builder().build(LEAF, frequency="monthly", data_columns=["price", "bogus"])


def test_empty_data_columns_raises():
    with pytest.raises(ValueError, match="omit it to pull every measure"):
        _builder().build(LEAF, frequency="monthly", data_columns=[])


def test_unknown_facet_key_raises_listing_valid():
    with pytest.raises(ValueError, match="Valid facets: stateid, sectorid"):
        _builder().build(LEAF, frequency="monthly", facets={"stat": ["CO"]})


def test_non_leaf_route_raises_helpful_error():
    with pytest.raises(ValueError, match="no frequencies"):
        _builder(SINGLE_FREQ_TREE).build("electricity/no-freq")


# ----------------------------------------------------------------------
# Optional facet-value validation (opt-in, one extra request per facet)
# ----------------------------------------------------------------------


def test_check_facet_values_accepts_valid_value():
    spec = _builder().build(
        LEAF, frequency="monthly", facets={"stateid": ["CO"]}, check_facet_values=True
    )
    assert spec.facets == {"stateid": ["CO"]}


def test_check_facet_values_rejects_unknown_value():
    with pytest.raises(ValueError, match="no value.*'ZZ'"):
        _builder().build(
            LEAF,
            frequency="monthly",
            facets={"stateid": ["ZZ"]},
            check_facet_values=True,
        )


# ----------------------------------------------------------------------
# Notebook scaffold
# ----------------------------------------------------------------------


def test_template_lays_out_every_option():
    text = _builder().template(LEAF)
    assert "EIADatasetSpec(" in text
    assert 'route_path="electricity/retail-sales"' in text
    # both measures appear, with their units as comments
    assert '"price",' in text and '"revenue",' in text
    assert "cents per kilowatthour" in text
    # the alternate frequency and the facet keys are surfaced
    assert "annual" in text
    assert "stateid" in text and "sectorid" in text
    assert "2001-01" in text and "2024-01" in text  # period range


def test_template_is_syntactically_valid_python():
    import ast

    # Strip the leading comment lines; the rest must parse as a call expression.
    body = "\n".join(
        line for line in _builder().template(LEAF).splitlines() if not line.startswith("#")
    )
    ast.parse(body)
