"""CourtListenerDatasetSpec — defines one CourtListener resource to collect.

CourtListener (Free Law Project) exposes the same underlying database two
ways: a REST API (v4) of per-resource endpoints (``dockets``, ``opinions``,
``clusters``, ``courts``, ``people``, …) and monthly **bulk data** exports —
bzip2-compressed CSV dumps of whole tables. The API is rate-limited (~5,000
queries/hour), so backfilling a large resource through it is infeasible; the
intended pattern for big resources is *bulk file for the initial backfill,
then incremental API pulls by* ``date_modified``.

A spec names one resource and how its **full** reads are performed
(``backfill="bulk"`` or ``"api"``); incremental reads always use the API.
Both paths land in the same target table: the reader normalizes API rows to
the bulk-CSV column shape.

Usage:
    from datadongle.collectors.courtlistener.spec import CourtListenerDatasetSpec

    spec = CourtListenerDatasetSpec(
        name="courtlistener_dockets",
        target_table="courtlistener_dockets",
        resource="dockets",
        backfill="bulk",              # full read = latest monthly bulk file
    )                                 # entity_key defaults to ["id"] ⇒ SCD2
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from datadongle.collectors.base_spec import DatasetSpec
from datadongle.collectors.courtlistener.resources import bulk_prefix_for

_BULK_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class CourtListenerDatasetSpec(DatasetSpec):
    """Defines a CourtListener resource to collect.

    Parameters
    ----------
    name : str
        Human-readable dataset name on this system.
    target_table : str
        Destination table name.
    resource : str
        The resource to collect, e.g. ``"dockets"``, ``"courts"``,
        ``"opinions"``, ``"people"``. By default this is used as both the API
        endpoint name and the bulk-file prefix; where the two differ (e.g. the
        API endpoint ``clusters`` vs the bulk file ``opinion-clusters``), set
        ``api_endpoint`` / ``bulk_file_prefix`` explicitly.
    target_schema : str
        Destination schema/namespace. Default ``"raw_data"``.
    entity_key : list[str] | None
        Columns uniquely identifying an entity. Defaults to ``["id"]`` (⇒ SCD2
        history), which is right for the entity tables that make up most of the
        catalog. It is **wrong for the many-to-many through tables** (the
        citation map, opinion-cluster panels, ``joined_by``): those carry an
        ``id`` only because Django adds one, and keying on it means an upstream
        rebuild renumbers every row and SCD2 re-versions the whole table. Key
        those on their foreign-key pair instead. Use
        ``CourtListenerMetadata.suggest_profile(resource)`` to get the right
        value for any resource. Pass ``None`` to opt out of versioning
        (Append). The reader validates these columns exist before reading.
    backfill : str
        How a **full** read (``mode="full"``, or the first incremental run
        against an empty table) is performed. ``"bulk"`` (default) downloads
        the monthly bulk CSV export — the only feasible path for large
        resources like dockets or opinions. ``"api"`` walks the endpoint
        page-by-page — fine for small resources (e.g. courts) and fresher
        than the monthly bulk snapshot. Incremental reads always use the API.
    filters : dict[str, Any]
        Optional server-side API filters in Django-lookup form, e.g.
        ``{"court": "scotus"}``. Only allowed with ``backfill="api"`` — a bulk
        file is always the whole table, so filtered increments on top of an
        unfiltered bulk seed would produce an incoherent table.
    api_endpoint : str | None
        Override the API endpoint name when it differs from ``resource``.
    bulk_file_prefix : str | None
        Override the bulk filename prefix when it differs from ``resource``.
    bulk_date : str | None
        Pin the bulk export date (``"YYYY-MM-DD"``, as it appears in the
        filename). ``None`` (default) means the latest available export.
    cursor_column : str | None
        The incremental high-water-mark column. Default ``"date_modified"``,
        which every *entity* table carries — but the through tables have no
        timestamps at all, so set ``None`` for those (every run is then a full
        read). A resource whose discovered columns lack the cursor is
        downgraded to full reads with a warning rather than failing.
    """

    name: str
    target_table: str
    resource: str
    target_schema: str = "raw_data"
    entity_key: list[str] | None = field(default_factory=lambda: ["id"])
    backfill: str = "bulk"
    filters: dict[str, Any] = field(default_factory=dict)
    api_endpoint: str | None = None
    bulk_file_prefix: str | None = None
    bulk_date: str | None = None
    cursor_column: str | None = "date_modified"
    source: str = "courtlistener"

    def __post_init__(self) -> None:
        self.resource = self.resource.strip().strip("/")
        if not self.resource:
            raise ValueError("resource is required, e.g. 'dockets' or 'courts'.")
        if self.backfill not in ("bulk", "api"):
            raise ValueError(f"backfill must be 'bulk' or 'api', got {self.backfill!r}")
        if self.filters and self.backfill == "bulk":
            raise ValueError(
                "filters require backfill='api': a bulk file is always the whole "
                "table, so a filtered API increment on top of an unfiltered bulk "
                "seed would produce an incoherent table."
            )
        if self.bulk_date is not None and not _BULK_DATE.match(self.bulk_date):
            raise ValueError(f"bulk_date must be 'YYYY-MM-DD', got {self.bulk_date!r}")

    @property
    def endpoint(self) -> str:
        """The API endpoint name (``api_endpoint`` override, else ``resource``)."""
        return (self.api_endpoint or self.resource).strip("/")

    @property
    def file_prefix(self) -> str:
        """The bulk filename prefix for this resource.

        An explicit ``bulk_file_prefix`` wins; otherwise the rename registry in
        ``resources.py`` is consulted, so ``resource="clusters"`` finds the
        ``opinion-clusters`` export without the caller restating it. Falls back
        to ``resource`` when the two names agree, which is the common case.

        Only the *name mapping* is resolved automatically — it's a mechanical
        fact about how files are published, checked into ``resources.py``.
        ``entity_key`` stays explicit, because that's a modelling decision that
        should be visible in the spec.
        """
        return self.bulk_file_prefix or bulk_prefix_for(self.resource)

    @property
    def dataset_id(self) -> str:
        if not self.filters:
            return self.resource
        suffix = "&".join(f"{k}={v}" for k, v in sorted(self.filters.items()))
        return f"{self.resource}?{suffix}"
