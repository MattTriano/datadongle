"""Live smoke tests for the offline-development assumptions.

This collector was written without network access; these tests verify the
source-specific facts it relies on against the real service. They are marked
``network`` and deselected by default — run them once from a machine with
egress:

    uv run --no-sync pytest tests/collectors/courtlistener/test_live.py -m network

Anonymous access suffices (set COURTLISTENER_API_TOKEN for friendlier rate
limits). Each test names the assumption it checks; a failure means the
corresponding constant/behavior in client.py or reader.py needs adjusting.
"""

from __future__ import annotations

import pytest

from datadongle.collectors.courtlistener.client import CourtListenerClient

pytestmark = pytest.mark.network


@pytest.fixture(scope="module")
def client() -> CourtListenerClient:
    return CourtListenerClient()


def test_api_root_lists_expected_endpoints(client):
    root = client.api_root()
    assert {"courts", "dockets", "opinions"} <= set(root)


def test_page_shape_and_row_fields(client):
    payload = client.get_page("courts")
    assert {"results", "next"} <= set(payload)
    row = payload["results"][0]
    # The reader's cursor and entity key depend on these fields existing.
    assert "id" in row
    assert "date_modified" in row


def test_compound_order_by_accepted(client):
    # reader._read_api requests (cursor, tiebreak) ordering; if this 400s,
    # drop the ",id" (see README §Developed offline).
    payload = client.get_page("courts", {"order_by": "date_modified,id"})
    assert payload["results"]


def test_date_modified_gte_filter_accepted(client):
    payload = client.get_page("courts", {"date_modified__gte": "2000-01-01T00:00:00Z"})
    assert payload["results"]


def test_options_metadata_available(client):
    meta = client.options("dockets")
    assert meta.get("name")


def test_bulk_bucket_listing_finds_courts_export(client):
    exports = client.list_bulk_exports("courts")
    assert exports, "S3 listing returned nothing — check BULK_STORAGE_URL/prefix"
    latest = exports[-1]
    assert latest["url"].endswith(".csv.bz2")


def test_bulk_header_peek_and_quotechar(client):
    # Validates BULK_CSV_QUOTECHAR indirectly: with the wrong quotechar the
    # header would come back mangled (embedded quotes or a single column).
    exports = client.list_bulk_exports("courts")
    header = client.read_bulk_header(exports[-1]["url"])
    assert "id" in header
    assert "date_modified" in header
    assert all(header), f"empty column name in parsed header: {header!r}"
