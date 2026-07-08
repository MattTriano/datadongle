"""Shared fakes/builders for the ArcGIS Hub reader tests.

The HTTP boundary is faked: ``FakeArcGISHubClient`` routes ``get_json`` calls to
canned item/layer/query payloads by URL shape, so the tests exercise the reader
without any network.
"""

from __future__ import annotations

from typing import Any

SERVICE_URL = "https://svc.example.com/FeatureServer"
ITEM_ID = "item123"
BASE_URL = "https://data.example.ca"


def item_payload(service_url: str = SERVICE_URL) -> dict[str, Any]:
    return {"id": ITEM_ID, "properties": {"url": service_url}}


def default_fields() -> list[dict[str, Any]]:
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "Event_Unique_Id", "type": "esriFieldTypeString"},
        {"name": "Occurred_Date", "type": "esriFieldTypeDate"},
        {"name": "Count", "type": "esriFieldTypeInteger"},
    ]


def layer_payload(
    idx: int = 0,
    *,
    name: str | None = None,
    geometry_type: str | None = "esriGeometryPoint",
    fields: list[dict[str, Any]] | None = None,
    max_record_count: int = 1000,
) -> dict[str, Any]:
    return {
        "id": idx,
        "name": name or f"Layer {idx}",
        "maxRecordCount": max_record_count,
        "geometryType": geometry_type,
        "spatialReference": {"wkid": 4326},
        "fields": fields if fields is not None else default_fields(),
    }


class FakeArcGISHubClient:
    """Routes get_json by URL: item metadata, /layers, /{idx}, /{idx}/query."""

    def __init__(
        self,
        *,
        item: dict[str, Any] | None = None,
        layer_infos: dict[int, dict[str, Any]] | None = None,
        pages: dict[int, list[list[dict[str, Any]]]] | None = None,
        layers: list[dict[str, Any]] | None = None,
    ) -> None:
        self._item = item if item is not None else item_payload()
        self._layer_infos = layer_infos or {0: layer_payload(0)}
        self._pages = pages or {}
        self._layers = layers
        self._page_seq: dict[int, int] = {}
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    def get_json(self, path_or_url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.calls.append((path_or_url, params))
        url = path_or_url
        if "/collections/dataset/items/" in url:
            return self._item
        if url.endswith("/layers"):
            return {"layers": self._layers if self._layers is not None else []}
        if url.endswith("/query"):
            idx = int(url[: -len("/query")].rsplit("/", 1)[1])
            seq = self._page_seq.get(idx, 0)
            self._page_seq[idx] = seq + 1
            pages = self._pages.get(idx, [[]])
            features = pages[seq] if seq < len(pages) else []
            return {"features": features}
        # Otherwise a layer-info request: .../{idx}
        idx = int(url.rsplit("/", 1)[1])
        return self._layer_infos[idx]

    def factory(self):
        """A client_factory that ignores base_url and returns this fake."""
        return lambda base_url: self
