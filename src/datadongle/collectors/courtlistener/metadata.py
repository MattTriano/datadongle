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
    m.bulk_datasets()                # every table published in bulk
    m.coverage()                     # API endpoints vs bulk exports

Not every API endpoint has a bulk export: many are per-user state or
RPC-style operations (``alerts``, ``search``, ``recap-fetch``) with no table
behind them. And the two namespaces don't line up by name — ``clusters`` is
exported as ``opinion-clusters`` — so a name-filtered lookup returning
nothing is not proof the data isn't published. :meth:`coverage` reconciles
the two.
"""

from __future__ import annotations

import pandas as pd
import requests

from datadongle.collectors.courtlistener.client import CourtListenerClient
from datadongle.collectors.courtlistener.resources import (
    RESOURCES,
    ProfileSuggestion,
    api_endpoint_for,
    bulk_prefix_for,
    profile_from_columns,
)


def _name_candidates(endpoint: str, prefixes: set[str]) -> list[str]:
    """Bulk prefixes that might be ``endpoint`` under a different name.

    Substring either way, since the bulk name is usually the endpoint name
    with a qualifier bolted on (``agreements`` →
    ``financial-disclosure-agreements``) and occasionally the reverse.
    """
    return sorted(p for p in prefixes if endpoint in p or p in endpoint)


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
        """One endpoint's OPTIONS metadata (its ``name``/``description``, etc.).

        Takes either name: an API endpoint, or a bulk prefix the registry can
        map to one. A name with no endpoint behind it raises with the nearest
        matches rather than a bare 404 — plenty of bulk tables have no API
        endpoint at all, and that's a fact about the source, not a typo.
        """
        return self._options(resource)

    def _options(self, resource: str) -> dict:
        """OPTIONS for ``resource``, resolving its API name and explaining 404s."""
        endpoint = api_endpoint_for(resource)
        try:
            return self.client.options(endpoint)
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 404:
                raise
            raise ValueError(self._no_endpoint_message(resource, endpoint)) from exc

    def _no_endpoint_message(self, resource: str, endpoint: str) -> str:
        via = "" if endpoint == resource else f" (resolved to {endpoint!r})"
        message = f"CourtListener has no API endpoint {resource!r}{via}."

        try:
            names = set(self.endpoints()["name"])
        except Exception:  # noqa: BLE001 - the 404 is the story, not this lookup
            return message

        near = sorted(n for n in names if resource in n or n in resource)
        if near:
            message += f" Closest endpoints: {', '.join(near)}."
        return message + (
            " Many bulk tables have no API endpoint — check m.coverage() to see "
            "which resources are bulk-only, and use m.bulk_columns() for their "
            "fields."
        )

    def columns(self, resource: str) -> pd.DataFrame:
        """Field names for one endpoint, with types where the server tells us.

        Prefers the OPTIONS field metadata; where the server doesn't expose it
        (read-only endpoints may not), falls back to sampling one row and
        reporting the JSON types observed.
        """
        meta = self._options(resource)
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

        page = self.client.get_page(api_endpoint_for(resource))
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

        The filter is a **literal key prefix**, so it only matches files whose
        name starts with ``resource``. An empty result means "no file is named
        that", not "this data isn't published in bulk" — the bulk prefix often
        differs from the API endpoint name (``clusters`` is exported as
        ``opinion-clusters``). Use :meth:`bulk_datasets` for the real list.
        """
        exports = self.client.list_bulk_exports(resource)
        return pd.DataFrame(exports, columns=["prefix", "date", "filename", "size", "url"])

    def bulk_datasets(self) -> pd.DataFrame:
        """Every dataset available for bulk collection, one row per table.

        This is the authoritative answer to "what can I backfill from bulk?" —
        it reads the whole bucket rather than guessing at a name. The
        ``prefix`` value goes into a spec's ``resource`` (or
        ``bulk_file_prefix`` where the API endpoint is named differently).

        Columns: ``prefix``, ``exports``, ``first_date``, ``latest_date``,
        ``latest_size``.
        """
        columns = ["prefix", "exports", "first_date", "latest_date", "latest_size"]
        df = self.bulk_exports()
        if df.empty:
            return pd.DataFrame(columns=columns)

        rows = []
        for prefix, group in df.sort_values("date").groupby("prefix"):
            rows.append(
                {
                    "prefix": prefix,
                    "exports": len(group),
                    "first_date": group["date"].iloc[0],
                    "latest_date": group["date"].iloc[-1],
                    "latest_size": group["size"].iloc[-1],
                }
            )
        return pd.DataFrame(rows, columns=columns).sort_values("prefix", ignore_index=True)

    def coverage(self) -> pd.DataFrame:
        """Reconcile API endpoints against bulk exports.

        Answers "which resources can I backfill from bulk, and which are
        API-only?" in one table. Many API endpoints are per-user state or
        RPC-style operations (``alerts``, ``search``, ``recap-fetch``) with no
        table behind them, so ``bulk=False`` is normal rather than a gap.

        Where an endpoint has no same-named export, ``candidates`` lists bulk
        prefixes whose name contains the endpoint's (or vice versa) — that is
        how ``clusters`` relates to ``opinion-clusters``. These are hints to
        check with :meth:`bulk_columns`, not conclusions.

        Columns: ``name``, ``api``, ``bulk``, ``bulk_prefix``, ``candidates``.
        """
        endpoints = set(self.endpoints()["name"])
        datasets = self.bulk_datasets()
        prefixes = set(datasets["prefix"]) if not datasets.empty else set()

        # API endpoint -> bulk prefix, for the renames this collector knows.
        # Registry keys are canonical names, which may be either namespace, so
        # both directions are indexed.
        known = {}
        for name, profile in RESOURCES.items():
            endpoint = profile.api_endpoint or name
            prefix = profile.bulk_file_prefix or name
            if endpoint != prefix:
                known[endpoint] = prefix

        rows = []
        matched: set[str] = set()
        for name in sorted(endpoints):
            prefix = known.get(name, name)
            has_bulk = prefix in prefixes
            if has_bulk:
                matched.add(prefix)
            rows.append(
                {
                    "name": name,
                    "api": True,
                    "bulk": has_bulk,
                    "bulk_prefix": prefix if has_bulk else None,
                    "candidates": [] if has_bulk else _name_candidates(name, prefixes),
                }
            )

        # Bulk tables with no same-named endpoint — the through tables and
        # anything the API doesn't surface. These are bulk-only by nature.
        for prefix in sorted(prefixes - matched):
            rows.append(
                {
                    "name": prefix,
                    "api": False,
                    "bulk": True,
                    "bulk_prefix": prefix,
                    "candidates": [],
                }
            )

        return pd.DataFrame(
            rows, columns=["name", "api", "bulk", "bulk_prefix", "candidates"]
        ).sort_values("name", ignore_index=True)

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

        Accepts either name: an API endpoint (``clusters``) or a bulk prefix
        (``opinion-clusters``). The bucket lookup goes through the rename
        registry, so ``clusters`` doesn't fail for want of a same-named file.

            >>> m.suggest_profile("dockets")            # doctest: +SKIP
            dockets: entity_key=['id'], cursor_column='date_modified'
              Entity table: 'id' is the upstream primary key.
        """
        columns = self.bulk_columns(bulk_prefix_for(resource))
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
