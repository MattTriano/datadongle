"""Fakes and test-data helpers for the EIA collector suite.

``FakeEIAClient`` overrides only the HTTP boundary (``_get``), serving canned
JSON from an in-memory row list. Everything above it — param building, offset/
length paging in ``iter_data``, ``get_data_page``, the schema sample — runs the
real code with no network. It emulates the two server-side behaviors the reader
relies on: ``start`` filtering and ``offset``/``length`` paging.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from datadongle.collectors.eia.client import EIAClient
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


def make_spec(schema: str = "raw_data", **overrides) -> EIADatasetSpec:
    """Build a spec against the given schema with sensible defaults."""
    defaults = dict(
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
