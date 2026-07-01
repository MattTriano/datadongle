"""
FredMetadata — explore datasets available from the FRED API.

FRED organizes economic data through several overlapping hierarchies:
    - Categories: a tree (root id is 0)
    - Releases:   official data releases (e.g. "H.15 Selected Interest Rates")
    - Sources:    publishing organizations (e.g. "Board of Governors")
    - Tags:       flat labels (e.g. "usa", "monthly", "gdp")

A series can be reached from any of these. This class provides
DataFrame-returning methods for browsing each, plus full-text and
tag-based search for finding series directly.

Usage:
    from datadongle.collectors.fred.metadata import FredMetadata

    m = FredMetadata(api_key="YOUR_KEY")  # or set FRED_API_KEY env var

    # Browse the category tree
    m.list_categories()                       # top-level
    m.list_categories(parent_id=32991)        # children of "Money, Banking, & Finance"

    # Find series
    m.search_series("unemployment rate")
    m.list_series_in_release(release_id=10)
    m.list_series_by_tags(["usa", "monthly", "unemployment"])

    # Inspect a series
    m.describe_series("UNRATE")
    m.list_tags_for_series("UNRATE")
"""

from __future__ import annotations

import os
from functools import lru_cache

import pandas as pd
import requests


class FredMetadata:
    BASE = "https://api.stlouisfed.org/fred"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("FRED_API_KEY")
        if not self.api_key:
            raise ValueError(
                "FRED API key required. Pass api_key= or set FRED_API_KEY env var. "
                "Get a free key at https://fredaccountmanager.stlouisfed.org/apikey"
            )
        self._session = requests.Session()

    # ------------------------------------------------------------------ #
    #  Internal helpers
    # ------------------------------------------------------------------ #

    def _get_json(self, path: str, **params) -> dict:
        """Fetch JSON from a FRED endpoint, injecting api_key and file_type."""
        url = f"{self.BASE}/{path.lstrip('/')}"
        params = {k: v for k, v in params.items() if v is not None}
        params["api_key"] = self.api_key
        params["file_type"] = "json"
        resp = self._session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def _paginate(
        self,
        path: str,
        result_key: str,
        limit: int,
        fetch_all: bool,
        **params,
    ) -> list[dict]:
        """
        Fetch results from a paginated FRED endpoint.

        FRED caps responses at 1000 per page. If fetch_all is True, walks
        all pages until the API reports no more results. Otherwise stops
        after `limit` records.
        """
        page_size = 1000 if fetch_all else min(limit, 1000)
        offset = 0
        rows: list[dict] = []

        while True:
            data = self._get_json(path, limit=page_size, offset=offset, **params)
            page = data.get(result_key, [])
            rows.extend(page)
            total = data.get("count", len(rows))
            offset += len(page)
            if not fetch_all and len(rows) >= limit:
                rows = rows[:limit]
                break
            if not page or offset >= total:
                break
        return rows

    @staticmethod
    def _filter_keyword(df: pd.DataFrame, keyword: str | None, columns: list[str]) -> pd.DataFrame:
        """Case-insensitive substring filter across the given columns."""
        if not keyword or df.empty:
            return df
        kw = keyword.lower()
        mask = pd.Series(False, index=df.index)
        for col in columns:
            if col in df.columns:
                mask |= df[col].fillna("").str.lower().str.contains(kw, regex=False)
        return df[mask].reset_index(drop=True)

    # ------------------------------------------------------------------ #
    #  Categories
    # ------------------------------------------------------------------ #

    def list_categories(self, parent_id: int = 0) -> pd.DataFrame:
        """
        List child categories under a parent category.

        The FRED category tree is rooted at id 0 (the default).

        Returns a DataFrame with columns: id, name, parent_id.
        """
        data = self._get_json("category/children", category_id=parent_id)
        return pd.DataFrame(data.get("categories", []))

    def describe_category(self, category_id: int) -> dict:
        """Return high-level details for a single category."""
        data = self._get_json("category", category_id=category_id)
        cats = data.get("categories", [])
        return cats[0] if cats else {}

    # ------------------------------------------------------------------ #
    #  Releases
    # ------------------------------------------------------------------ #

    @lru_cache(maxsize=1)
    def _all_releases(self) -> pd.DataFrame:
        rows = self._paginate("releases", result_key="releases", limit=0, fetch_all=True)
        return pd.DataFrame(rows)

    def list_releases(self, keyword: str | None = None) -> pd.DataFrame:
        """
        List all FRED releases, optionally filtered by keyword.

        Returns a DataFrame with columns: id, name, press_release, link, notes.
        """
        df = self._all_releases().copy()
        return self._filter_keyword(df, keyword, columns=["name", "notes"])

    def describe_release(self, release_id: int) -> dict:
        """Return high-level details for a single release."""
        data = self._get_json("release", release_id=release_id)
        rels = data.get("releases", [])
        return rels[0] if rels else {}

    # ------------------------------------------------------------------ #
    #  Sources
    # ------------------------------------------------------------------ #

    @lru_cache(maxsize=1)
    def _all_sources(self) -> pd.DataFrame:
        rows = self._paginate("sources", result_key="sources", limit=0, fetch_all=True)
        return pd.DataFrame(rows)

    def list_sources(self, keyword: str | None = None) -> pd.DataFrame:
        """
        List all FRED sources (publishing organizations).

        Returns a DataFrame with columns: id, name, link, notes.
        """
        df = self._all_sources().copy()
        return self._filter_keyword(df, keyword, columns=["name", "notes"])

    def describe_source(self, source_id: int) -> dict:
        """Return high-level details for a single source."""
        data = self._get_json("source", source_id=source_id)
        srcs = data.get("sources", [])
        return srcs[0] if srcs else {}

    def list_releases_for_source(self, source_id: int) -> pd.DataFrame:
        """List the releases published by a given source."""
        data = self._get_json("source/releases", source_id=source_id)
        return pd.DataFrame(data.get("releases", []))

    # ------------------------------------------------------------------ #
    #  Tags
    # ------------------------------------------------------------------ #

    def list_tags(
        self,
        keyword: str | None = None,
        group_id: str | None = None,
        limit: int = 1000,
        fetch_all: bool = False,
    ) -> pd.DataFrame:
        """
        List FRED tags, optionally filtered by group or search text.

        Parameters
        ----------
        keyword : str, optional
            Server-side substring search on tag name/notes.
        group_id : str, optional
            Restrict to a tag group. Common values:
                "freq" — frequency (annual, monthly, etc.)
                "gen"  — general
                "geo"  — geography
                "geot" — geography type
                "rls"  — release
                "seas" — seasonal adjustment
                "src"  — source
        limit : int
            Max tags to return when fetch_all is False.
        fetch_all : bool
            If True, paginate through all matching tags.

        Returns a DataFrame with columns: name, group_id, notes, created,
        popularity, series_count.
        """
        rows = self._paginate(
            "tags",
            result_key="tags",
            limit=limit,
            fetch_all=fetch_all,
            search_text=keyword,
            tag_group_id=group_id,
        )
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------ #
    #  Series search
    # ------------------------------------------------------------------ #

    _SERIES_DISPLAY_COLS = [
        "id",
        "title",
        "frequency_short",
        "units_short",
        "seasonal_adjustment_short",
        "observation_start",
        "observation_end",
        "popularity",
    ]

    def search_series(
        self,
        text: str,
        limit: int = 25,
        fetch_all: bool = False,
        order_by: str = "search_rank",
    ) -> pd.DataFrame:
        """
        Full-text search across FRED series.

        Parameters
        ----------
        text : str
            Search query.
        limit : int
            Max results to return when fetch_all is False. Default 25.
        fetch_all : bool
            If True, paginate through all results.
        order_by : str
            FRED ordering: search_rank, series_id, title, units, frequency,
            seasonal_adjustment, realtime_start, realtime_end, last_updated,
            observation_start, observation_end, popularity, group_popularity.

        Returns a DataFrame of matching series.
        """
        rows = self._paginate(
            "series/search",
            result_key="seriess",
            limit=limit,
            fetch_all=fetch_all,
            search_text=text,
            order_by=order_by,
        )
        df = pd.DataFrame(rows)
        return self._reorder_series_columns(df)

    def list_series_in_category(
        self, category_id: int, limit: int = 25, fetch_all: bool = False
    ) -> pd.DataFrame:
        """List all series belonging to a category."""
        rows = self._paginate(
            "category/series",
            result_key="seriess",
            limit=limit,
            fetch_all=fetch_all,
            category_id=category_id,
        )
        df = pd.DataFrame(rows)
        return self._reorder_series_columns(df)

    def list_series_in_release(
        self, release_id: int, limit: int = 25, fetch_all: bool = False
    ) -> pd.DataFrame:
        """List all series in a release."""
        rows = self._paginate(
            "release/series",
            result_key="seriess",
            limit=limit,
            fetch_all=fetch_all,
            release_id=release_id,
        )
        df = pd.DataFrame(rows)
        return self._reorder_series_columns(df)

    def list_series_by_tags(
        self,
        tag_names: list[str],
        limit: int = 25,
        fetch_all: bool = False,
    ) -> pd.DataFrame:
        """
        List series matching ALL of the given tags.

        Example:
            m.list_series_by_tags(["usa", "monthly", "unemployment"])
        """
        rows = self._paginate(
            "tags/series",
            result_key="seriess",
            limit=limit,
            fetch_all=fetch_all,
            tag_names=";".join(tag_names),
        )
        df = pd.DataFrame(rows)
        return self._reorder_series_columns(df)

    @classmethod
    def _reorder_series_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Move the most useful columns to the front for notebook display."""
        if df.empty:
            return df
        front = [c for c in cls._SERIES_DISPLAY_COLS if c in df.columns]
        rest = [c for c in df.columns if c not in front]
        return df[front + rest]

    # ------------------------------------------------------------------ #
    #  Single-series inspection
    # ------------------------------------------------------------------ #

    def describe_series(self, series_id: str) -> dict:
        """
        Return detailed metadata for a single series.

        Includes title, units, frequency, seasonal adjustment, observation
        range, last updated, and notes.
        """
        data = self._get_json("series", series_id=series_id)
        results = data.get("seriess", [])
        return results[0] if results else {}

    def list_categories_for_series(self, series_id: str) -> pd.DataFrame:
        """List the categories a series belongs to."""
        data = self._get_json("series/categories", series_id=series_id)
        return pd.DataFrame(data.get("categories", []))

    def list_tags_for_series(self, series_id: str) -> pd.DataFrame:
        """List the tags assigned to a series."""
        data = self._get_json("series/tags", series_id=series_id)
        return pd.DataFrame(data.get("tags", []))

    def get_release_for_series(self, series_id: str) -> dict:
        """Return the release a series belongs to."""
        data = self._get_json("series/release", series_id=series_id)
        rels = data.get("releases", [])
        return rels[0] if rels else {}
