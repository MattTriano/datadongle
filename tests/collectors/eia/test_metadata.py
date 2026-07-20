"""EIAMetadata — route-tree parsing and search against a canned tree."""

from __future__ import annotations

import pytest

from datadongle.collectors.eia.metadata import EIAMetadata

from .helpers import FACET_VALUES, ROUTE_TREE, FakeEIAMetadataClient

LEAF = "electricity/retail-sales"


def _meta() -> EIAMetadata:
    return EIAMetadata(client=FakeEIAMetadataClient(ROUTE_TREE, FACET_VALUES))


def test_browse_root_lists_categories_with_paths():
    df = _meta().browse()
    assert list(df["id"]) == ["electricity", "natural-gas"]
    assert list(df["path"]) == ["electricity", "natural-gas"]


def test_browse_child_route_builds_nested_path():
    df = _meta().browse("electricity")
    assert list(df["path"]) == [LEAF]


def test_browse_leaf_is_empty():
    assert _meta().browse(LEAF).empty


def test_describe_returns_full_metadata():
    meta = _meta().describe(LEAF)
    assert meta["id"] == "retail-sales"
    assert meta["startPeriod"] == "2001-01"


def test_frequencies():
    assert set(_meta().frequencies(LEAF)["id"]) == {"monthly", "annual"}


def test_columns_map_measures_with_units():
    df = _meta().columns(LEAF)
    assert set(df["id"]) == {"price", "revenue"}
    price = df[df["id"] == "price"].iloc[0]
    assert price["alias"] == "Average Price"
    assert price["units"] == "cents per kilowatthour"


def test_facets():
    assert set(_meta().facets(LEAF)["id"]) == {"stateid", "sectorid"}


def test_facet_values():
    df = _meta().facet_values(LEAF, "stateid")
    assert set(df["id"]) == {"CO", "CA"}


def test_search_finds_leaf_by_name():
    df = _meta().search("retail", max_depth=1)
    assert LEAF in set(df["path"])


def test_search_matches_a_top_category():
    df = _meta().search("natural gas", max_depth=0)
    # "natural gas" matches the natural-gas category's name at depth 0.
    assert "natural-gas" in set(df["path"])


def test_search_depth_bounds_requests():
    client = FakeEIAMetadataClient(ROUTE_TREE, FACET_VALUES)
    EIAMetadata(client=client).search("anything", max_depth=0)
    # depth 0 fetches only the root, never descending into categories.
    assert client.requests == [""]


def test_search_returns_stable_columns_when_no_match():
    df = _meta().search("no-such-dataset", max_depth=1)
    assert df.empty
    assert list(df.columns) == ["path", "id", "name", "description"]


def test_missing_route_raises():
    with pytest.raises(KeyError):
        _meta().describe("does/not/exist")
