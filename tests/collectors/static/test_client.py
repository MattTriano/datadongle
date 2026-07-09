"""Unit behaviors of StaticFileClient: CSV/XLSX parsing, column sanitization,
encoding handling (the things that silently corrupt data when they go wrong),
the HTML-response guard, and the retry predicate. No HTTP or database.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests
from datadongle.collectors.static.client import (
    StaticFileClient,
    StaticFileDownloadError,
    _is_retryable,
    _looks_like_html,
    sanitize_column_name,
)
from datadongle.collectors.static.spec import FileRef

from .helpers import FakeStaticFileClient, csv_bytes


class TestSanitizeColumnName:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("Hospital Name ", "hospital_name"),
            ("CCN", "ccn"),
            ("Net Revenue ($)", "net_revenue"),
            ("sys_id", "sys_id"),
            ("  ", "unnamed"),
        ],
    )
    def test_normalization(self, raw, expected):
        assert sanitize_column_name(raw) == expected


class TestCsvParsing:
    def _rows(self, data: bytes, **ref_overrides):
        ref = FileRef(url="https://x.test/f.csv", vintage="2023", **ref_overrides)
        client = FakeStaticFileClient({ref.url: data})
        path = client.download_to_tempfile(ref.url)
        try:
            return list(client.parse_file(path, ref))
        finally:
            path.unlink(missing_ok=True)

    def test_values_are_stripped_strings_with_sanitized_keys(self):
        data = csv_bytes(["Sys ID", "Sys Name"], [["0895", "  Adena "]])
        assert self._rows(data) == [{"sys_id": "0895", "sys_name": "Adena"}]

    def test_leading_zeros_survive(self):
        data = csv_bytes(["ccn"], [["010001"], ["0895"]])
        assert [r["ccn"] for r in self._rows(data)] == ["010001", "0895"]

    def test_utf8_bom_does_not_corrupt_first_column_name(self):
        data = "﻿ccn,name\n0895,Adena\n".encode()
        assert "ccn" in self._rows(data)[0]

    def test_cp1252_en_dash_decodes_with_declared_encoding(self):
        data = csv_bytes(["name"], [["Example Health – Metro"]], encoding="cp1252")
        assert self._rows(data, encoding="cp1252")[0]["name"] == "Example Health – Metro"

    def test_wrong_encoding_fails_loudly_not_silently(self):
        data = csv_bytes(["name"], [["Example Health – Metro"]], encoding="cp1252")
        with pytest.raises(UnicodeDecodeError):
            self._rows(data)  # default encoding utf-8-sig

    def test_skip_rows_discards_preamble(self):
        data = b"Some Title\nGenerated 2026-01-01\nccn,name\n0895,Adena\n"
        assert self._rows(data, skip_rows=2) == [{"ccn": "0895", "name": "Adena"}]


class TestXlsxParsing:
    def test_cell_rendering(self):
        pytest.importorskip("openpyxl")
        import datetime

        from .helpers import xlsx_bytes

        data = xlsx_bytes(
            ["ID", "Beds", "Rate", "Updated", "Note"],
            [["A1", 470.0, 35.45, datetime.date(2023, 1, 2), None]],
        )
        ref = FileRef(url="https://x.test/f.xlsx", vintage="2023", file_format="xlsx")
        client = FakeStaticFileClient({ref.url: data})
        path = client.download_to_tempfile(ref.url)
        try:
            (row,) = list(client.parse_file(path, ref))
        finally:
            path.unlink(missing_ok=True)
        assert row == {
            "id": "A1",
            "beds": "470",  # integral float loses Excel's trailing .0
            "rate": "35.45",
            "updated": "2023-01-02T00:00:00",  # Excel has no date-only type
            "note": "",  # None becomes empty string
        }


class TestHtmlGuard:
    def test_html_content_type_detected(self):
        assert _looks_like_html("text/html", b"<html></html>") is True

    def test_html_body_detected_even_with_lying_content_type(self):
        assert _looks_like_html("text/csv", b"  <!DOCTYPE html><html>") is True

    def test_csv_passes(self):
        assert _looks_like_html("text/csv", b"a,b\n1,2\n") is False

    def test_download_raises_on_html_response(self):
        client = StaticFileClient(delay_seconds=0)
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.headers = {"Content-Type": "text/html"}
        resp.iter_content.return_value = iter([b"<!DOCTYPE html><html>challenge</html>"])
        client.session = MagicMock()
        client.session.get.return_value = resp

        with pytest.raises(StaticFileDownloadError, match="HTML"):
            client.download_to_tempfile("https://x.test/f.csv")


class TestRetryPredicate:
    def test_retries_on_transient_status(self):
        resp = MagicMock(status_code=503)
        assert _is_retryable(requests.HTTPError(response=resp)) is True

    def test_does_not_retry_on_client_error(self):
        resp = MagicMock(status_code=404)
        assert _is_retryable(requests.HTTPError(response=resp)) is False

    def test_retries_on_connection_error(self):
        assert _is_retryable(requests.exceptions.ConnectionError()) is True

    def test_does_not_retry_html_error(self):
        assert _is_retryable(StaticFileDownloadError("html")) is False
