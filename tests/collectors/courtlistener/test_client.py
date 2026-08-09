"""CourtListenerClient pagination, bulk listing/parsing, and auth handling."""

from __future__ import annotations

import bz2

import pytest

from datadongle.collectors.courtlistener.client import (
    CourtListenerClient,
    _parse_bucket_listing,
    parse_bulk_header_line,
)

from .helpers import API_ROWS, FakeCourtListenerClient, make_bulk_bz2

# ------------------------------------------------------------------ auth


def test_token_from_env(monkeypatch):
    monkeypatch.setenv("COURTLISTENER_API_TOKEN", "from-env")
    client = CourtListenerClient()
    assert client.api_token == "from-env"
    assert client.session.headers["Authorization"] == "Token from-env"


def test_anonymous_client_allowed(monkeypatch):
    monkeypatch.delenv("COURTLISTENER_API_TOKEN", raising=False)
    client = CourtListenerClient()
    assert client.api_token is None
    assert "Authorization" not in client.session.headers


def test_bulk_session_never_carries_credentials(monkeypatch):
    """The bulk bucket is public and anonymous by construction.

    S3 rejects a ``Token …`` Authorization header with 400 InvalidArgument and
    echoes the token back in the error body, so the bulk session must stay
    credential-free even when the API session is authenticated.
    """
    monkeypatch.setenv("COURTLISTENER_API_TOKEN", "secret-token")
    client = CourtListenerClient()

    assert client.session.headers["Authorization"] == "Token secret-token"
    assert "Authorization" not in client.bulk_session.headers
    assert client.bulk_session is not client.session


# ------------------------------------------------------------ pagination


def test_iter_pages_follows_next_until_exhausted():
    client = FakeCourtListenerClient(fake_page_size=2)  # 3 rows ⇒ pages of 2, 1
    pages = list(client.iter_pages("dockets"))
    assert [len(p) for p in pages] == [2, 1]
    # The second request came from the payload's next URL, cursor included.
    assert "cursor=2" in client.calls[1][0]


def test_iter_pages_sends_params_only_on_first_request():
    client = FakeCourtListenerClient(fake_page_size=2)
    list(client.iter_pages("dockets", params={"order_by": "id"}))
    first_url, first_params = client.calls[0]
    next_url, next_params = client.calls[1]
    assert first_params == {"order_by": "id"}
    assert next_params == {}  # the next URL re-encodes the filters itself
    assert "order_by=id" in next_url


def test_get_page_includes_page_size_only_when_set():
    client = FakeCourtListenerClient()
    client.get_page("dockets")
    assert "page_size" not in client.calls[-1][1]
    client.page_size = 50
    client.get_page("dockets")
    assert client.calls[-1][1]["page_size"] == 50


# ------------------------------------------------------------ bulk listing

_S3_PAGE_1 = b"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>tok-2</NextContinuationToken>
  <Contents><Key>bulk-data/courts-2024-01-31.csv.bz2</Key><Size>100</Size></Contents>
  <Contents><Key>bulk-data/schema-2024-01-31.sql</Key><Size>5</Size></Contents>
</ListBucketResult>"""

_S3_PAGE_2 = b"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents><Key>bulk-data/courts-2024-02-29.csv.bz2</Key><Size>200</Size></Contents>
  <Contents><Key>bulk-data/courthouses-2024-02-29.csv.bz2</Key><Size>300</Size></Contents>
</ListBucketResult>"""


def test_parse_bucket_listing_handles_namespace_and_truncation():
    entries, token = _parse_bucket_listing(_S3_PAGE_1)
    assert token == "tok-2"
    assert entries[0] == {"key": "bulk-data/courts-2024-01-31.csv.bz2", "size": 100}
    entries, token = _parse_bucket_listing(_S3_PAGE_2)
    assert token is None
    assert len(entries) == 2


class _DummyResponse:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        pass


def test_list_bulk_exports_paginates_filters_and_sorts(monkeypatch):
    client = CourtListenerClient(api_token="t")
    responses = iter([_DummyResponse(_S3_PAGE_1), _DummyResponse(_S3_PAGE_2)])
    requests_seen = []

    def fake_get(url, params=None, timeout=None):
        requests_seen.append(dict(params or {}))
        return next(responses)

    monkeypatch.setattr(client.bulk_session, "get", fake_get)

    exports = client.list_bulk_exports("courts")
    # Continuation token carried into the second request.
    assert requests_seen[1]["continuation-token"] == "tok-2"
    # schema-*.sql skipped; "courthouses" excluded by exact-prefix match;
    # results sorted oldest-first.
    assert [e["date"] for e in exports] == ["2024-01-31", "2024-02-29"]
    assert all(e["prefix"] == "courts" for e in exports)
    assert exports[0]["url"].endswith("bulk-data/courts-2024-01-31.csv.bz2")


# ------------------------------------------------------------ bulk header


def test_parse_bulk_header_line_uses_backtick_quoting():
    assert parse_bulk_header_line("id,`case,name`,court_id") == ["id", "case,name", "court_id"]


class _DummyStreamResponse:
    def __init__(self, payload: bytes, chunk_size: int):
        self._payload = payload
        self._chunk = chunk_size

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        for i in range(0, len(self._payload), self._chunk):
            yield self._payload[i : i + self._chunk]


def test_read_bulk_header_peeks_without_full_decompress(monkeypatch):
    client = CourtListenerClient(api_token="t")
    payload = make_bulk_bz2([{"id": "1", "case_name": "A, B"}], columns=["id", "case_name"])
    monkeypatch.setattr(
        client.bulk_session,
        "get",
        lambda url, stream=None, timeout=None: _DummyStreamResponse(payload, chunk_size=7),
    )
    assert client.read_bulk_header("https://example.invalid/x.csv.bz2") == ["id", "case_name"]


def test_read_bulk_header_raises_when_no_newline(monkeypatch):
    client = CourtListenerClient(api_token="t")
    payload = bz2.compress(b"no newline here")
    monkeypatch.setattr(
        client.bulk_session,
        "get",
        lambda url, stream=None, timeout=None: _DummyStreamResponse(payload, chunk_size=1024),
    )
    with pytest.raises(ValueError, match="No header line"):
        client.read_bulk_header("https://example.invalid/x.csv.bz2")


@pytest.mark.parametrize("method", ["download_bulk", "read_bulk_header"])
def test_bulk_downloads_go_through_the_bulk_session(monkeypatch, tmp_path, method):
    """Guards the session split.

    A bulk call routed through the authenticated API session would leak the
    token to S3 *and* slip past every test that patches ``bulk_session`` — it
    would reach the real network instead of failing.
    """
    client = CourtListenerClient(api_token="t")
    payload = make_bulk_bz2([{"id": "1", "case_name": "A"}], columns=["id", "case_name"])

    def fail(*args, **kwargs):
        raise AssertionError(f"{method} used the authenticated API session")

    monkeypatch.setattr(client.session, "get", fail)
    monkeypatch.setattr(
        client.bulk_session,
        "get",
        lambda url, stream=None, timeout=None: _DummyStreamResponse(payload, chunk_size=64),
    )

    url = "https://example.invalid/x.csv.bz2"
    if method == "download_bulk":
        assert client.download_bulk(url, tmp_path / "x.csv.bz2").exists()
    else:
        assert client.read_bulk_header(url) == ["id", "case_name"]


def test_list_bulk_exports_goes_through_the_bulk_session(monkeypatch):
    client = CourtListenerClient(api_token="t")

    def fail(*args, **kwargs):
        raise AssertionError("list_bulk_exports used the authenticated API session")

    monkeypatch.setattr(client.session, "get", fail)
    monkeypatch.setattr(
        client.bulk_session,
        "get",
        lambda url, params=None, timeout=None: _DummyResponse(_S3_PAGE_2),
    )

    assert [e["date"] for e in client.list_bulk_exports("courts")] == ["2024-02-29"]


def test_fake_serves_gte_filter_like_the_server():
    # Sanity-check the fake itself: __gte narrows to changed rows only.
    client = FakeCourtListenerClient()
    page = client.get_page("dockets", {"date_modified__gte": "2024-02-01 00:00:00+00:00"})
    ids = [r["id"] for r in page["results"]]
    assert ids == [2, 4]
    assert len(API_ROWS) == 3  # canned data untouched
