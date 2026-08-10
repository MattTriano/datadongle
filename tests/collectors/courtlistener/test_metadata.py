"""CourtListenerMetadata catalog parsing against canned responses."""

from __future__ import annotations

from datadongle.collectors.courtlistener.metadata import CourtListenerMetadata

from .helpers import BULK_EXPORTS, BULK_HEADER, FakeCourtListenerClient


def _metadata(**client_kwargs) -> CourtListenerMetadata:
    return CourtListenerMetadata(client=FakeCourtListenerClient(**client_kwargs))


def test_endpoints_lists_the_api_root():
    df = _metadata().endpoints()
    assert list(df.columns) == ["name", "url"]
    assert set(df["name"]) == {"courts", "dockets", "opinions"}


def test_search_filters_by_substring_case_insensitive():
    df = _metadata().search("DOCK")
    assert list(df["name"]) == ["dockets"]


def test_describe_returns_options_metadata():
    meta = _metadata().describe("dockets")
    assert meta["name"] == "Dockets"


def test_columns_prefers_options_field_metadata():
    df = _metadata().columns("dockets")
    assert set(df["name"]) == {"id", "case_name"}
    assert df.set_index("name").loc["id", "type"] == "integer"


def test_columns_falls_back_to_sampling_a_row():
    meta = _metadata(options_payload={"name": "Dockets", "actions": {}})
    df = meta.columns("dockets")
    # Raw (untransformed) API field names, with observed JSON types.
    assert "resource_uri" in set(df["name"])
    assert df.set_index("name").loc["id", "type"] == "int"


def test_bulk_exports_dataframe():
    df = _metadata().bulk_exports("dockets")
    assert list(df.columns) == ["prefix", "date", "filename", "size", "url"]
    assert list(df["date"]) == [e["date"] for e in BULK_EXPORTS]


def test_bulk_columns_reads_the_header():
    assert _metadata().bulk_columns("dockets") == BULK_HEADER


# ------------------------------------------------- bulk_datasets / coverage

# The API root has three endpoints (courts, dockets, opinions). These exports
# cover: a matching name, the clusters -> opinion-clusters rename, a
# differently-named sibling, and a bulk-only through table.
MIXED_EXPORTS = [
    {"prefix": "dockets", "date": "2024-01-31", "filename": "f", "url": "u1", "size": 10},
    {"prefix": "dockets", "date": "2024-02-29", "filename": "f", "url": "u2", "size": 20},
    {"prefix": "opinion-clusters", "date": "2024-02-29", "filename": "f", "url": "u3", "size": 30},
    {
        "prefix": "financial-disclosure-agreements",
        "date": "2024-02-29",
        "filename": "f",
        "url": "u4",
        "size": 40,
    },
    {"prefix": "citation-map", "date": "2024-02-29", "filename": "f", "url": "u5", "size": 50},
]

MIXED_ROOT = {
    "dockets": "https://x/dockets/",
    "clusters": "https://x/clusters/",
    "agreements": "https://x/agreements/",
    "alerts": "https://x/alerts/",
}


def _mixed() -> CourtListenerMetadata:
    return _metadata(exports=MIXED_EXPORTS, root=MIXED_ROOT)


def test_bulk_datasets_collapses_exports_to_one_row_per_table():
    df = _mixed().bulk_datasets().set_index("prefix")

    assert list(df.columns) == ["exports", "first_date", "latest_date", "latest_size"]
    assert set(df.index) == {
        "dockets",
        "opinion-clusters",
        "financial-disclosure-agreements",
        "citation-map",
    }
    assert df.loc["dockets", "exports"] == 2
    assert df.loc["dockets", "first_date"] == "2024-01-31"
    assert df.loc["dockets", "latest_date"] == "2024-02-29"
    assert df.loc["dockets", "latest_size"] == 20  # newest file's size, not the oldest


def test_bulk_datasets_is_empty_but_shaped_when_nothing_is_published():
    df = _metadata(exports=[]).bulk_datasets()
    assert df.empty
    assert list(df.columns) == ["prefix", "exports", "first_date", "latest_date", "latest_size"]


def test_coverage_matches_endpoints_to_same_named_exports():
    row = _mixed().coverage().set_index("name").loc["dockets"]
    assert row["api"] and row["bulk"]
    assert row["bulk_prefix"] == "dockets"


def test_coverage_uses_the_registry_for_renamed_exports():
    """clusters is published as opinion-clusters; the registry knows."""
    row = _mixed().coverage().set_index("name").loc["clusters"]
    assert row["bulk"]
    assert row["bulk_prefix"] == "opinion-clusters"


def test_coverage_suggests_candidates_for_an_unmatched_endpoint():
    """The false negative that motivated this: agreements looked absent."""
    row = _mixed().coverage().set_index("name").loc["agreements"]
    assert not row["bulk"]
    assert row["candidates"] == ["financial-disclosure-agreements"]


def test_coverage_marks_operational_endpoints_as_api_only():
    row = _mixed().coverage().set_index("name").loc["alerts"]
    assert row["api"] and not row["bulk"]
    assert row["candidates"] == []


def test_coverage_lists_bulk_only_tables():
    """Through tables have no API endpoint but are collectable from bulk."""
    row = _mixed().coverage().set_index("name").loc["citation-map"]
    assert row["bulk"]
    assert not row["api"]


def test_coverage_does_not_double_count_a_renamed_export():
    names = list(_mixed().coverage()["name"])
    assert names.count("opinion-clusters") == 0  # it appears as its endpoint, "clusters"
    assert len(names) == len(set(names))
