"""Tests for TigerClient (mocked HTTP; no live calls)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from datadongle.collectors.tiger.client import TigerClient, _is_retryable


class TestIsRetryable:
    def test_retries_on_transient_status(self):
        resp = MagicMock(status_code=503)
        assert _is_retryable(requests.HTTPError(response=resp)) is True

    def test_does_not_retry_on_client_error(self):
        resp = MagicMock(status_code=404)
        assert _is_retryable(requests.HTTPError(response=resp)) is False

    def test_retries_on_connection_error(self):
        assert _is_retryable(requests.exceptions.ConnectionError()) is True


class TestGetText:
    def test_returns_body_and_retries(self):
        client = TigerClient()
        session = MagicMock()
        ok = MagicMock()
        ok.raise_for_status.return_value = None
        ok.text = "<html>listing</html>"
        session.get.side_effect = [requests.exceptions.ConnectionError(), ok]
        client._session = session

        assert client.get_text("https://example/dir/") == "<html>listing</html>"
        assert session.get.call_count == 2


class TestDownloadToTempfile:
    def test_streams_to_tempfile(self, tmp_path):
        client = TigerClient()
        session = MagicMock()
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.iter_content.return_value = [b"abc", b"def"]
        session.get.return_value = resp
        client._session = session

        path = client.download_to_tempfile("https://example/f.zip")
        try:
            assert path.read_bytes() == b"abcdef"
            # streamed, not buffered whole
            _, kwargs = session.get.call_args
            assert kwargs.get("stream") is True
        finally:
            Path(path).unlink(missing_ok=True)

    def test_cleans_up_on_failure(self):
        client = TigerClient()
        session = MagicMock()
        resp = MagicMock()
        resp.raise_for_status.return_value = None

        def _boom(chunk_size):
            raise OSError("disk full")

        resp.iter_content.side_effect = _boom
        session.get.return_value = resp
        client._session = session

        with pytest.raises(OSError):
            client.download_to_tempfile("https://example/f.zip")
