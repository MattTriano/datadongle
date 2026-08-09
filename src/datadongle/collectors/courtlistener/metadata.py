"""CourtListenerMetadata — explore the CourtListener API and bulk exports.

CourtListener's "catalog" is the API root: a flat map of resource endpoints
(``dockets``, ``opinions``, ``clusters``, ``courts``, ``people``, …). Each
endpoint answers an OPTIONS request with its name, description, and — where
the server exposes them — per-field metadata. The monthly bulk exports live
in a public S3 bucket, one ``<prefix>-<YYYY-MM-DD>.csv.bz2`` per table.

This class answers, interactively: *what resources exist, what is this
resource, what are its columns, and what bulk files are available?* — the
values a user writes into a :class:`CourtListenerDatasetSpec`.

List-returning methods hand back pandas DataFrames for notebook display,
matching ``EIAMetadata``/``FredMetadata``.

Usage:
    from datadongle.collectors.courtlistener.metadata import CourtListenerMetadata

    m = CourtListenerMetadata()      # reads COURTLISTENER_API_TOKEN if set

    m.endpoints()                    # every API resource and its URL
    m.search("docket")               # endpoints matching a substring
    m.describe("dockets")            # OPTIONS metadata for one endpoint
    m.columns("dockets")             # field names/types for one endpoint
    m.bulk_exports("dockets")        # available bulk files with dates/sizes
"""

from __future__ import annotations

import pandas as pd

from datadongle.collectors.courtlistener.client import CourtListenerClient
from datadongle.collectors.courtlistener.resources import (
    ProfileSuggestion,
    profile_from_columns,
)


class CourtListenerMetadata:
    """Browse CourtListener's API endpoints and bulk-data exports."""

    def __init__(self, client: CourtListenerClient | None = None, api_token: str | None = None):
        self.api_token = api_token
        self._client = client

    @property
    def client(self) -> CourtListenerClient:
        if self._client is None:
            self._client = CourtListenerClient(api_token=self.api_token)
        return self._client

    # ------------------------------------------------------------------
    # Core vocabulary: search / describe / columns
    # ------------------------------------------------------------------

    def endpoints(self) -> pd.DataFrame:
        """Every API resource, from the API root: columns ``name``, ``url``."""
        root = self.client.api_root()
        return pd.DataFrame([{"name": name, "url": url} for name, url in sorted(root.items())])

    def search(self, text: str) -> pd.DataFrame:
        """Endpoints whose name contains ``text`` (case-insensitive)."""
        df = self.endpoints()
        if df.empty:
            return df
        return df[df["name"].str.contains(text, case=False)].reset_index(drop=True)

    def describe(self, resource: str) -> dict:
        """One endpoint's OPTIONS metadata (its ``name``/``description``, etc.)."""
        return self.client.options(resource)

    def columns(self, resource: str) -> pd.DataFrame:
        """Field names for one endpoint, with types where the server tells us.

        Prefers the OPTIONS field metadata; where the server doesn't expose it
        (read-only endpoints may not), falls back to sampling one row and
        reporting the JSON types observed.
        """
        meta = self.client.options(resource)
        fields = self._options_fields(meta)
        if fields:
            rows = [
                {
                    "name": name,
                    "type": info.get("type"),
                    "required": info.get("required"),
                    "help_text": info.get("help_text"),
                }
                for name, info in fields.items()
            ]
            return pd.DataFrame(rows)

        page = self.client.get_page(resource)
        results = page.get("results") or []
        if not results:
            return pd.DataFrame(columns=["name", "type", "required", "help_text"])
        sample = results[0]
        return pd.DataFrame(
            [
                {"name": k, "type": type(v).__name__ if v is not None else None}
                for k, v in sample.items()
            ]
        )

    # ------------------------------------------------------------------
    # Bulk exports
    # ------------------------------------------------------------------

    def bulk_exports(self, resource: str | None = None) -> pd.DataFrame:
        """Available bulk CSV exports (→ a spec's ``bulk_file_prefix``/``bulk_date``).

        Columns: ``prefix``, ``date``, ``filename``, ``size``, ``url``.
        ``resource`` filters to one table's files; ``None`` lists everything.
        """
        exports = self.client.list_bulk_exports(resource)
        return pd.DataFrame(exports, columns=["prefix", "date", "filename", "size", "url"])

    def bulk_columns(self, resource: str, date: str | None = None) -> list[str]:
        """A bulk export's column names (a cheap streamed header peek)."""
        exports = self.client.list_bulk_exports(resource)
        if date is not None:
            exports = [e for e in exports if e["date"] == date]
        if not exports:
            raise ValueError(f"No bulk export found for {resource!r}.")
        latest = max(exports, key=lambda e: e["date"])
        return self.client.read_bulk_header(latest["url"])

    # ------------------------------------------------------------------
    # Spec suggestions
    # ------------------------------------------------------------------

    def suggest_profile(self, resource: str) -> ProfileSuggestion:
        """Recommend ``entity_key`` and ``cursor_column`` for one resource.

        Reads the resource's bulk header (a streamed peek, not a download) and
        derives the answer from its actual columns, so it is right even for
        resources datadongle has never seen. The result carries a rationale;
        review it before pasting into a spec.

            >>> m.suggest_profile("dockets")            # doctest: +SKIP
            dockets: entity_key=['id'], cursor_column='date_modified'
              Entity table: 'id' is the upstream primary key.
        """
        columns = self.bulk_columns(resource)
        return ProfileSuggestion(
            resource=resource,
            profile=profile_from_columns(resource, columns),
            columns=columns,
        )

    def suggest_profiles(self) -> pd.DataFrame:
        """``suggest_profile`` for every resource with a bulk export.

        One header peek per resource, so this is the slow-but-thorough way to
        produce a reviewed set of specs in one pass. Columns: ``resource``,
        ``entity_key``, ``cursor_column``, ``incremental``, ``rationale``.
        """
        rows = []
        for resource in sorted(self.bulk_exports()["prefix"].unique()):
            suggestion = self.suggest_profile(resource)
            rows.append(
                {
                    "resource": resource,
                    "entity_key": suggestion.profile.entity_key,
                    "cursor_column": suggestion.profile.cursor_column,
                    "incremental": suggestion.profile.is_incremental,
                    "rationale": suggestion.profile.rationale,
                }
            )
        return pd.DataFrame(
            rows, columns=["resource", "entity_key", "cursor_column", "incremental", "rationale"]
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _options_fields(meta: dict) -> dict:
        """Field metadata from an OPTIONS payload, whichever action carries it."""
        actions = meta.get("actions") or {}
        for action in ("GET", "POST", "PUT"):
            if actions.get(action):
                return actions[action]
        return {}
