from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import ConnectionError, HTTPError

from datadongle.collectors.socrata.client import SocrataClient, _is_retryable


class TestSocrataClient:
    @patch("datadongle.collectors.socrata.client.requests.Session")
    def test_paginate_single_page(self, MockSession):
        mock_session = MagicMock()
        MockSession.return_value = mock_session

        page_data = [{"id": "1"}, {"id": "2"}]
        mock_resp = MagicMock()
        mock_resp.json.return_value = page_data
        mock_session.get.return_value = mock_resp

        client = SocrataClient(page_size=100)
        client._session = mock_session

        pages = list(client.paginate("example.com", "abcd-1234"))
        assert len(pages) == 1
        assert pages[0] == page_data

    @patch("datadongle.collectors.socrata.client.requests.Session")
    def test_paginate_stops_after_partial_page(self, MockSession):
        mock_session = MagicMock()
        MockSession.return_value = mock_session

        page1 = [{"id": "1"}, {"id": "2"}]
        page2 = [{"id": "3"}]  # partial → last page
        mock_resp1 = MagicMock()
        mock_resp1.json.return_value = page1
        mock_resp2 = MagicMock()
        mock_resp2.json.return_value = page2
        mock_session.get.side_effect = [mock_resp1, mock_resp2]

        client = SocrataClient(page_size=2)
        client._session = mock_session

        pages = list(client.paginate("example.com", "abcd-1234"))
        assert len(pages) == 2
        assert len(pages[0]) == 2
        assert len(pages[1]) == 1

    def test_query_builds_soda_params(self):
        client = SocrataClient()
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"a": 1}]
        mock_resp.headers = {}
        mock_session.get.return_value = mock_resp
        client._session = mock_session

        result = client.query(
            "example.com",
            "abcd-1234",
            select="col1, col2",
            where="col1 > 5",
            order="col1 DESC",
            limit=10,
        )

        params = mock_session.get.call_args[1]["params"]
        assert params["$select"] == "col1, col2"
        assert params["$where"] == "col1 > 5"
        assert params["$order"] == "col1 DESC"
        assert params["$limit"] == "10"
        assert result == [{"a": 1}]


class TestRetryPredicate:
    """`_is_retryable` is tenacity's retry predicate; it must accept the
    exception as its single positional arg (the regression this guards)."""

    def _http_error(self, status: int) -> HTTPError:
        resp = MagicMock()
        resp.status_code = status
        return HTTPError(response=resp)

    def test_predicate_called_with_single_exception_arg(self):
        # Directly mirrors tenacity's call: predicate(exc). A signature that
        # still expected `self` would raise TypeError here.
        assert _is_retryable(self._http_error(503)) is True

    def test_connection_error_is_retryable(self):
        assert _is_retryable(ConnectionError()) is True

    def test_client_error_is_not_retryable(self):
        assert _is_retryable(self._http_error(404)) is False

    # `_request`'s wait is baked into the decorator at import time, so patch
    # tenacity's sleep to keep the retry path fast and deterministic.
    @patch("tenacity.nap.time.sleep", return_value=None)
    def test_request_retries_transient_then_succeeds(self, _sleep):
        client = SocrataClient(page_size=100)
        mock_session = MagicMock()
        bad_resp = MagicMock()
        bad_resp.raise_for_status.side_effect = self._http_error(503)
        good_resp = MagicMock()
        good_resp.raise_for_status.return_value = None
        good_resp.json.return_value = [{"id": "1"}]
        good_resp.headers = {}
        # First attempt raises a retryable HTTPError, second succeeds.
        mock_session.get.side_effect = [bad_resp, good_resp]
        client._session = mock_session

        result = client._request("example.com", "abcd-1234", {})
        assert result == [{"id": "1"}]
        assert mock_session.get.call_count == 2

    @patch("tenacity.nap.time.sleep", return_value=None)
    def test_request_does_not_retry_client_error(self, _sleep):
        client = SocrataClient(page_size=100)
        mock_session = MagicMock()
        resp = MagicMock()
        resp.raise_for_status.side_effect = self._http_error(404)
        mock_session.get.return_value = resp
        client._session = mock_session

        with pytest.raises(HTTPError):
            client._request("example.com", "abcd-1234", {})
        assert mock_session.get.call_count == 1
