"""EIAMetadata — explore the EIA API v2 route tree.

The EIA API has no flat catalog or full-text search endpoint. Datasets live in a
tree of **routes**: the root (``/v2/``) lists top categories (``electricity``,
``natural-gas``, …); each category lists child routes; a **leaf** route is a data
series carrying its ``frequency`` options, ``facets``, ``data`` (measure)
columns, and period range. This class walks that tree so a user can find a
dataset and read off the values that fill an :class:`EIADatasetSpec`
(``route_path``, ``frequency``, ``data_columns``, ``facets``).

List-returning methods hand back pandas DataFrames for notebook display, matching
``FredMetadata``.

Usage:
    from datadongle.collectors.eia.metadata import EIAMetadata

    m = EIAMetadata()                       # reads EIA_API_KEY from the environment

    m.browse()                              # top categories
    m.browse("electricity")                 # child routes of electricity
    m.describe("electricity/retail-sales")  # a leaf's full metadata
    m.frequencies("electricity/retail-sales")
    m.columns("electricity/retail-sales")   # measure columns → data_columns
    m.facets("electricity/retail-sales")    # filterable dimensions
    m.facet_values("electricity/retail-sales", "stateid")
    m.search("retail sales")                # walk the tree for matching routes
"""

from __future__ import annotations

from collections import deque

import pandas as pd

from datadongle.collectors.eia.client import EIAClient


class EIAMetadata:
    """Browse and search the EIA API v2 route tree."""

    def __init__(self, client: EIAClient | None = None, api_key: str | None = None):
        self.api_key = api_key
        self._client = client

    @property
    def client(self) -> EIAClient:
        if self._client is None:
            self._client = EIAClient(api_key=self.api_key)
        return self._client

    # ------------------------------------------------------------------
    # Browsing a node
    # ------------------------------------------------------------------

    def browse(self, route_path: str = "") -> pd.DataFrame:
        """Child routes under ``route_path`` (empty ⇒ the top categories).

        Returns a DataFrame with columns ``id``, ``name``, ``description`` and a
        ``path`` giving the full route to each child. Empty at a leaf (a data
        series has no child routes — use :meth:`describe`/:meth:`columns`).
        """
        resp = self.client.get_route_metadata(route_path)
        routes = resp.get("routes") or []
        df = pd.DataFrame(routes)
        if not df.empty:
            base = route_path.strip("/")
            df.insert(0, "path", [self._join(base, r.get("id")) for r in routes])
        return df

    def describe(self, route_path: str) -> dict:
        """The full metadata ``response`` for a node or leaf.

        For a leaf this carries ``id``/``name``/``description``, ``frequency``,
        ``facets``, ``data`` (measure columns), and ``startPeriod``/``endPeriod``.
        """
        return self.client.get_route_metadata(route_path)

    # ------------------------------------------------------------------
    # Reading a leaf's spec-filling pieces
    # ------------------------------------------------------------------

    def frequencies(self, route_path: str) -> pd.DataFrame:
        """A leaf's available frequencies (``id`` → a spec's ``frequency``)."""
        resp = self.client.get_route_metadata(route_path)
        return pd.DataFrame(resp.get("frequency") or [])

    def columns(self, route_path: str) -> pd.DataFrame:
        """A leaf's measure columns (its ``data`` map → a spec's ``data_columns``).

        Columns: ``id`` (the value to put in ``data_columns``), ``alias``, and
        ``units`` where the API provides them.
        """
        resp = self.client.get_route_metadata(route_path)
        data = resp.get("data") or {}
        # ``data`` is an object keyed by measure id; be tolerant of a list too.
        if isinstance(data, dict):
            rows = [
                {"id": key, "alias": val.get("alias"), "units": val.get("units")}
                for key, val in data.items()
            ]
        else:
            rows = list(data)
        return pd.DataFrame(rows)

    def facets(self, route_path: str) -> pd.DataFrame:
        """A leaf's facet dimensions (``id`` → keys of a spec's ``facets``)."""
        resp = self.client.get_route_metadata(route_path)
        return pd.DataFrame(resp.get("facets") or [])

    def facet_values(self, route_path: str, facet_id: str) -> pd.DataFrame:
        """The valid values for one facet (→ values of a spec's ``facets``)."""
        return pd.DataFrame(self.client.get_facet_values(route_path, facet_id))

    # ------------------------------------------------------------------
    # Searching the tree
    # ------------------------------------------------------------------

    def search(self, text: str, *, root: str = "", max_depth: int = 1) -> pd.DataFrame:
        """Walk the route tree, returning routes whose id/name/description match.

        ``max_depth`` bounds how deep the walk descends (``0`` ⇒ only the
        immediate children of ``root``; ``1`` also expands those children, etc.).

        This issues **one request per internal node visited** — the API has no
        server-side search — so keep ``max_depth`` small (each extra level fans
        out across every child route).
        """
        needle = text.lower()
        matches: list[dict] = []
        seen: set[str] = set()
        queue: deque[tuple[str, int]] = deque([(root.strip("/"), 0)])
        while queue:
            path, depth = queue.popleft()
            resp = self.client.get_route_metadata(path)
            for child in resp.get("routes") or []:
                child_path = self._join(path, child.get("id"))
                if child_path in seen:
                    continue
                seen.add(child_path)
                if self._entry_matches(child, needle):
                    matches.append(
                        {
                            "path": child_path,
                            "id": child.get("id"),
                            "name": child.get("name"),
                            "description": child.get("description"),
                        }
                    )
                if depth < max_depth:
                    queue.append((child_path, depth + 1))
        return pd.DataFrame(matches, columns=["path", "id", "name", "description"])

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _join(base: str, child_id: str | None) -> str:
        return f"{base}/{child_id}".strip("/") if base else (child_id or "")

    @staticmethod
    def _entry_matches(entry: dict, needle: str) -> bool:
        return any(
            needle in str(entry.get(field) or "").lower()
            for field in ("id", "name", "description")
        )
