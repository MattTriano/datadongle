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
