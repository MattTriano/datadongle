"""Fakes and test-data helpers for the EIA collector suite.

``FakeEIAClient`` overrides only the HTTP boundary (``_get``), serving canned
JSON from an in-memory row list. Everything above it — param building, offset/
length paging in ``iter_data``, ``get_data_page``, the schema sample — runs the
real code with no network. It emulates the two server-side behaviors the reader
relies on: ``start`` filtering and ``offset``/``length`` paging.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from datadongle.collectors.eia.client import EIAClient
from datadongle.collectors.eia.reader import EIAReader
from datadongle.collectors.eia.spec import EIADatasetSpec

# A tiny electricity/retail-sales-shaped source: monthly price for one state,
# two sectors, with the camelCase description and hyphenated units columns the
# real API returns and measure values delivered as strings.
DEFAULT_ROWS = [
    {
        "period": "2001-01",
        "stateid": "CO",
        "stateDescription": "Colorado",
        "sectorid": "RES",
        "sectorName": "residential",
        "price": "6.71",
        "price-units": "cents per kilowatthour",
    },
    {
        "period": "2001-01",
        "stateid": "CO",
        "stateDescription": "Colorado",
        "sectorid": "COM",
        "sectorName": "commercial",
        "price": "5.88",
        "price-units": "cents per kilowatthour",
    },
]


class FakeEIAClient(EIAClient):
    """Serves canned rows from memory; records the requests it received."""

    def __init__(self, rows: list[dict] | None = None, metadata: dict | None = None):
        super().__init__(api_key="test-key")
        self.rows = list(DEFAULT_ROWS if rows is None else rows)
        self.metadata = metadata or {}
        self.calls: list[tuple[str, dict]] = []

    def _get(self, path: str, params: Mapping[str, Any]) -> dict:
        self.calls.append((path, dict(params)))
        if path.rstrip("/").endswith("/data"):
            return {"response": self._serve_data(params)}
        return {"response": self.metadata}

    def _serve_data(self, params: Mapping[str, Any]) -> dict:
        rows = self.rows
        start = params.get("start")
        if start is not None:
            rows = [r for r in rows if r["period"] >= start]
        end = params.get("end")
        if end is not None:
            rows = [r for r in rows if r["period"] <= end]
        offset = int(params.get("offset", 0))
        length = int(params.get("length", self.page_size))
        return {
            "total": str(len(rows)),
            "dateFormat": "YYYY-MM",
            "frequency": params.get("frequency"),
            "data": rows[offset : offset + length],
        }


class FakeEIAMetadataClient(EIAClient):
    """Serves canned route metadata / facet values by path (no HTTP).

    Overrides the two public methods ``EIAMetadata`` calls, so route-tree
    parsing and the search walk run the real code against a fixed tree.
    """

    def __init__(
        self,
        routes: dict[str, dict],
        facet_values: dict[tuple[str, str], list[dict]] | None = None,
    ):
        super().__init__(api_key="test-key")
        self.routes = routes
        self.facet_values = facet_values or {}
        self.requests: list[str] = []

    def get_route_metadata(self, route_path: str) -> dict:
        key = route_path.strip("/")
        self.requests.append(key)
        if key not in self.routes:
            raise KeyError(f"FakeEIAMetadataClient has no metadata for {key!r}")
        return self.routes[key]

    def get_facet_values(self, route_path: str, facet_id: str) -> list[dict]:
        key = route_path.strip("/")
        self.requests.append(f"{key}/facet/{facet_id}")
        return self.facet_values.get((key, facet_id), [])


# A tiny route tree: root → electricity/natural-gas → electricity/retail-sales
# (a leaf carrying frequency/facets/data).
ROUTE_TREE = {
    "": {
        "id": "",
        "routes": [
            {"id": "electricity", "name": "Electricity", "description": "power data"},
            {"id": "natural-gas", "name": "Natural Gas", "description": "gas data"},
        ],
    },
    "electricity": {
        "id": "electricity",
        "name": "Electricity",
        "routes": [
            {
                "id": "retail-sales",
                "name": "Electricity Sales to Ultimate Customers",
                "description": "monthly retail sales, revenue, price by state/sector",
            },
        ],
    },
    "electricity/retail-sales": {
        "id": "retail-sales",
        "name": "Electricity Sales to Ultimate Customers",
        "description": "retail sales of electricity",
        "frequency": [
            {
                "id": "monthly",
                "description": "Monthly",
                "query": "M",
                "format": "YYYY-MM",
            },
            {"id": "annual", "description": "Annual", "query": "A", "format": "YYYY"},
        ],
        "facets": [
            {"id": "stateid", "description": "State / Census Region"},
            {"id": "sectorid", "description": "Sector"},
        ],
        "data": {
            "price": {"alias": "Average Price", "units": "cents per kilowatthour"},
            "revenue": {"alias": "Revenue", "units": "million dollars"},
        },
        "startPeriod": "2001-01",
        "endPeriod": "2024-01",
    },
    "natural-gas": {"id": "natural-gas", "name": "Natural Gas", "routes": []},
}

FACET_VALUES = {
    ("electricity/retail-sales", "stateid"): [
        {"id": "CO", "name": "Colorado"},
        {"id": "CA", "name": "California"},
    ],
}


def fake_client(reader: EIAReader) -> FakeEIAClient:
    """The fake behind a reader, typed so its canned rows are visible."""
    return cast(FakeEIAClient, reader.client)


def make_spec(schema: str = "raw_data", **overrides) -> EIADatasetSpec:
    """Build a spec against the given schema with sensible defaults."""
    defaults: dict[str, Any] = dict(
        name="test_eia_retail_sales",
        target_table="test_eia_retail_sales",
        target_schema=schema,
        route_path="electricity/retail-sales",
        frequency="monthly",
        data_columns=["price"],
        facets={"stateid": ["CO"]},
        entity_key=["stateid", "sectorid", "period"],
    )
    defaults.update(overrides)
    return EIADatasetSpec(**defaults)
