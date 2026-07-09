"""EIAClient param-building and pagination, against a faked HTTP boundary."""

from __future__ import annotations

import os

import pytest

from datadongle.collectors.eia.client import EIAClient

from .helpers import DEFAULT_ROWS, FakeEIAClient


def test_requires_api_key(monkeypatch):
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    with pytest.raises(ValueError, match="EIA API key required"):
        EIAClient()


def test_api_key_from_env(monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", "from-env")
    assert EIAClient().api_key == "from-env"


def test_data_params_encode_lists_and_facets():
    client = FakeEIAClient()
    list(
        client.iter_data(
            "electricity/retail-sales",
            frequency="monthly",
            data_columns=["price", "revenue"],
            facets={"stateid": ["CO"], "sectorid": ["RES", "COM"]},
            start="2001-01",
        )
    )
    path, params = client.calls[0]
    assert path == "electricity/retail-sales/data/"
    assert params["frequency"] == "monthly"
    assert params["data[]"] == ["price", "revenue"]
    assert params["facets[stateid][]"] == ["CO"]
    assert params["facets[sectorid][]"] == ["RES", "COM"]
    assert params["sort[0][column]"] == "period"
    assert params["sort[0][direction]"] == "asc"
    assert params["start"] == "2001-01"


def test_iter_data_pages_until_total():
    # Nine rows, page size 4 → pages of 4, 4, 1 then stop.
    rows = [{"period": f"2001-{i:02d}", "price": str(i)} for i in range(1, 10)]
    client = FakeEIAClient(rows=rows)
    client.page_size = 4

    pages = list(
        client.iter_data(
            "electricity/retail-sales", frequency="monthly", data_columns=["price"]
        )
    )

    assert [len(p) for p in pages] == [4, 4, 1]
    offsets = [params["offset"] for _, params in client.calls]
    assert offsets == [0, 4, 8]


def test_get_data_page_returns_total_and_rows():
    client = FakeEIAClient()
    resp = client.get_data_page(
        "electricity/retail-sales",
        frequency="monthly",
        data_columns=["price"],
        length=1,
    )
    assert resp["total"] == str(len(DEFAULT_ROWS))
    assert len(resp["data"]) == 1


def test_get_route_metadata_unwraps_response():
    client = FakeEIAClient(metadata={"id": "retail-sales", "facets": []})
    meta = client.get_route_metadata("electricity/retail-sales")
    assert meta["id"] == "retail-sales"


def test_page_size_capped_at_max():
    if not os.environ.get("EIA_API_KEY"):
        client = EIAClient(api_key="x", page_size=100_000)
    else:
        client = EIAClient(page_size=100_000)
    assert client.page_size == 5000
