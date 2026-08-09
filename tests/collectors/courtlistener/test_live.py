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

import bz2
import csv

import pytest

from datadongle.collectors.courtlistener.client import (
    BULK_CSV_QUOTECHAR,
    CourtListenerClient,
    check_bulk_quoting,
)
from datadongle.collectors.courtlistener.reader import normalize_timestamp
from datadongle.collectors.courtlistener.resources import (
    CURSOR_COLUMN,
    RESOURCES,
    profile_from_columns,
)

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


def test_bulk_data_rows_parse_without_residual_quotes(client, tmp_path):
    """The header alone doesn't catch a quotechar mismatch — data rows do.

    The header line is unquoted while data rows are force-quoted, so a wrong
    BULK_CSV_QUOTECHAR parses the header cleanly and then silently wraps every
    data value in literal quotes. `courts` is the smallest export, so this
    downloads it in full and checks the first row.
    """
    exports = client.list_bulk_exports("courts")
    path = client.download_bulk(exports[-1]["url"], tmp_path / "courts.csv.bz2")

    with bz2.open(path, mode="rt", encoding="utf-8", newline="") as f:
        row = next(csv.DictReader(f, delimiter=",", quotechar=BULK_CSV_QUOTECHAR))

    check_bulk_quoting(row)  # raises if the quoting has changed
    normalize_timestamp(row["date_modified"], "date_modified")  # raises if still quoted


# --------------------------------------------------------- resource profiles

# resources.RESOURCES was compiled without network access. These check every
# entry against the live bulk exports; a failure means an entry is wrong, not
# that the source changed.


@pytest.mark.parametrize("resource", sorted(RESOURCES))
def test_registry_entry_matches_the_live_columns(client, resource):
    profile = RESOURCES[resource]
    prefix = profile.bulk_file_prefix or resource
    exports = client.list_bulk_exports(prefix)
    assert exports, f"no bulk export named {prefix!r} — the registry key or prefix is wrong"

    columns = client.read_bulk_header(exports[-1]["url"])

    missing = [c for c in (profile.entity_key or []) if c not in columns]
    assert not missing, f"{resource} entity_key names absent columns {missing}: {columns}"

    has_cursor = CURSOR_COLUMN in columns
    assert profile.is_incremental == has_cursor, (
        f"{resource} registry says cursor_column={profile.cursor_column!r} but the "
        f"live columns {'have' if has_cursor else 'lack'} {CURSOR_COLUMN!r}"
    )


def test_derivation_agrees_with_the_registry_where_both_apply(client):
    """Where shape alone suffices, the registry should be redundant, not contradictory."""
    columns = client.read_bulk_header(client.list_bulk_exports("courts")[-1]["url"])
    derived = profile_from_columns("courts", columns)

    assert derived.entity_key == ["id"]
    assert derived.cursor_column == CURSOR_COLUMN


def test_a_known_link_table_is_recognised_by_shape(client):
    """The citation map is the through table the registry exists for.

    If this starts passing `looks_like_link_table`, the registry entry has
    become redundant and can be dropped.
    """
    exports = client.list_bulk_exports("citation-map")
    if not exports:
        pytest.skip("no citation-map bulk export published")
    columns = client.read_bulk_header(exports[-1]["url"])

    assert CURSOR_COLUMN not in columns, "citation-map gained timestamps; revisit the registry"
    assert {"citing_opinion_id", "cited_opinion_id"} <= set(columns)
