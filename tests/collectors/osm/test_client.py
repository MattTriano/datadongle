"""Unit tests for OSMClient (mocked requests session, no network)."""

from __future__ import annotations

import pytest
from tenacity import RetryError, wait_none

from datadongle.collectors.osm.client import (
    OSMClient,
    OverpassError,
    OverpassRateLimited,
    OverpassServerBusy,
    _extract_overpass_error,
)
from datadongle.collectors.osm.query import OverpassAPIQuery
from datadongle.geo import BBox


@pytest.fixture(autouse=True)
def _no_retry_backoff():
    """Keep tenacity's retry logic but drop the (long) exponential waits."""
    original = OSMClient._post.retry.wait
    OSMClient._post.retry.wait = wait_none()
    yield
    OSMClient._post.retry.wait = original


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.headers = {}
        self.posts = []

    def post(self, url, data=None, timeout=None):
        self.posts.append({"url": url, "data": data, "timeout": timeout})
        return self._responses.pop(0)


def _client(responses) -> OSMClient:
    client = OSMClient()
    client._session = _FakeSession(responses)
    return client


def _query() -> OverpassAPIQuery:
    return OverpassAPIQuery(
        element_types=["node"],
        tag_filters=[{"amenity": "cafe"}],
    ).for_bbox(BBox(41.62, -87.97, 42.05, -87.5))


# ------------------------------------------------------------------ fetch


def test_fetch_renders_query_and_returns_json():
    payload = {"elements": [{"type": "node", "id": 1}]}
    client = _client([_FakeResponse(200, payload)])

    result = client.fetch(_query())

    assert result == payload
    post = client._session.posts[0]
    assert post["url"] == client.endpoint
    assert post["data"]["data"].startswith("[out:json]")
    # http timeout is the query timeout plus the client's headroom
    assert post["timeout"] == _query().timeout + client.extra_timeout


def test_fetch_passes_date_filter_into_ql():
    client = _client([_FakeResponse(200, {"elements": []})])
    client.fetch(_query(), date_filter="2026-04-01T00:00:00Z")
    assert '(newer:"2026-04-01T00:00:00Z")' in client._session.posts[0]["data"]["data"]


# ------------------------------------------------------------------ error mapping


def test_429_retries_then_raises_rate_limited():
    client = _client([_FakeResponse(429, text="slow down")] * 3)
    with pytest.raises(RetryError) as exc:
        client.fetch(_query())
    # retried up to the stop limit (3 attempts), surfacing the mapped exception
    assert len(client._session.posts) == 3
    assert isinstance(exc.value.last_attempt.exception(), OverpassRateLimited)


def test_504_retries_then_raises_server_busy():
    client = _client([_FakeResponse(504, text="busy")] * 3)
    with pytest.raises(RetryError) as exc:
        client.fetch(_query())
    assert isinstance(exc.value.last_attempt.exception(), OverpassServerBusy)


def test_400_raises_overpass_error_without_retry():
    client = _client([_FakeResponse(400, text="<p><strong>Error</strong>: bad query</p>")])
    with pytest.raises(OverpassError, match="bad query"):
        client.fetch(_query())
    # 4xx (other than 429) is not retryable
    assert len(client._session.posts) == 1


def test_non_json_success_raises_overpass_error():
    client = _client([_FakeResponse(200, payload=None, text="<html>not json</html>")])
    with pytest.raises(OverpassError, match="non-JSON"):
        client.fetch(_query())


# ------------------------------------------------------------------ error extraction


def test_extract_overpass_error_pulls_error_blocks():
    body = "<html><p><strong style='x'>Error</strong>: line 1: syntax</p></html>"
    assert _extract_overpass_error(body) == "line 1: syntax"


def test_extract_overpass_error_falls_back_to_raw_body():
    assert _extract_overpass_error("plain text failure") == "plain text failure"
