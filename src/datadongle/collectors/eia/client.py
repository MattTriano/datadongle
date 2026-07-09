"""EIAClient — thin HTTP client for the EIA Open Data API v2.

Knows two things:
  1. how to read a route's metadata (facets, frequencies, data columns)
  2. how to page rows out of a route's ``/data/`` endpoint

Every request carries the ``api_key`` query parameter (EIA has no header auth
for the key). The data endpoint returns at most 5000 rows per request; pages
are walked by ``offset``/``length`` until ``response.total`` is reached.

API docs: https://www.eia.gov/opendata/documentation.php
Register for a free key: https://www.eia.gov/opendata/register.php

Usage:
    from datadongle.collectors.eia.client import EIAClient

    client = EIAClient()  # reads EIA_API_KEY from the environment
    meta = client.get_route_metadata("electricity/retail-sales")
    for page in client.iter_data(
        "electricity/retail-sales",
        frequency="monthly",
        data_columns=["price"],
        facets={"stateid": ["CO"]},
    ):
        ...
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.eia.gov/v2"

# The EIA data endpoint caps a JSON response at 5000 rows.
MAX_PAGE_SIZE = 5000


class EIAClient:
    """Thin ``requests`` wrapper for the EIA API v2.

    Parameters
    ----------
    api_key : str | None
        EIA API key. Falls back to the ``EIA_API_KEY`` environment variable.
    timeout : int
        Per-request timeout in seconds.
    page_size : int
        Rows per data page (the API maximum, and the default, is 5000).
    """

    def __init__(
        self,
        api_key: str | None = None,
        timeout: int = 60,
        page_size: int = MAX_PAGE_SIZE,
    ):
        self.api_key = api_key or os.environ.get("EIA_API_KEY")
        if not self.api_key:
            raise ValueError(
                "EIA API key required. Pass api_key= or set EIA_API_KEY. "
                "Get a free key at https://www.eia.gov/opendata/register.php"
            )
        self.timeout = timeout
        self.page_size = min(page_size, MAX_PAGE_SIZE)

        # Same retry shape as CMSClient: retry transient statuses and
        # connection/read errors at the adapter level, with backoff.
        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _get(self, path: str, params: Mapping[str, Any]) -> dict:
        """GET ``<BASE_URL>/<path>`` with ``api_key`` injected; return JSON."""
        url = f"{BASE_URL}/{path.lstrip('/')}"
        clean = {k: v for k, v in params.items() if v is not None}
        clean["api_key"] = self.api_key
        resp = self.session.get(url, params=clean, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # -- route metadata --------------------------------------------------

    def get_route_metadata(self, route_path: str) -> dict:
        """Return the ``response`` object for a route (no ``/data/``).

        Carries the route's ``id``/``name``, available ``frequency`` options,
        ``facets``, and ``data`` (measure) column catalog — the values a user
        needs to fill in an :class:`EIADatasetSpec`.
        """
        payload = self._get(f"{route_path.strip('/')}/", {})
        return payload["response"]

    # -- data ------------------------------------------------------------

    @staticmethod
    def _data_params(
        *,
        frequency: str,
        data_columns: Sequence[str],
        facets: Mapping[str, Sequence[str]] | None,
        start: str | None,
        end: str | None,
    ) -> dict[str, Any]:
        """Build the query params for a ``/data/`` request.

        Sorting by ``period`` ascending gives a deterministic order, so
        ``offset``/``length`` paging returns disjoint, complete slices within a
        single pull.
        """
        params: dict[str, Any] = {
            "frequency": frequency,
            "data[]": list(data_columns),
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "start": start,
            "end": end,
        }
        for facet_id, values in (facets or {}).items():
            params[f"facets[{facet_id}][]"] = list(values)
        return params

    def get_data_page(
        self,
        route_path: str,
        *,
        frequency: str,
        data_columns: Sequence[str],
        facets: Mapping[str, Sequence[str]] | None = None,
        start: str | None = None,
        end: str | None = None,
        offset: int = 0,
        length: int | None = None,
    ) -> dict:
        """Return the ``response`` object for one ``/data/`` page.

        The returned dict includes ``data`` (the row list) and ``total`` (the
        total matching-row count, as a string).
        """
        params = self._data_params(
            frequency=frequency,
            data_columns=data_columns,
            facets=facets,
            start=start,
            end=end,
        )
        params["offset"] = offset
        params["length"] = length or self.page_size
        payload = self._get(f"{route_path.strip('/')}/data/", params)
        return payload["response"]

    def iter_data(
        self,
        route_path: str,
        *,
        frequency: str,
        data_columns: Sequence[str],
        facets: Mapping[str, Sequence[str]] | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> Iterator[list[dict]]:
        """Yield pages of rows for a route until ``response.total`` is reached."""
        offset = 0
        while True:
            resp = self.get_data_page(
                route_path,
                frequency=frequency,
                data_columns=data_columns,
                facets=facets,
                start=start,
                end=end,
                offset=offset,
                length=self.page_size,
            )
            page = resp.get("data") or []
            if not page:
                return
            yield page
            offset += len(page)
            if offset >= int(resp.get("total") or 0):
                return
