"""CensusReader — the Census source adapter for the shared collection driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the existing
Census ``client``/``metadata``/``spec``. It replaces the per-state-vintage load
logic that used to live in ``CensusCollector``; the family-level orchestration
(the ``vintages × states`` fan-out, the union-of-vintages table, per-pair error
isolation) lives in :func:`run_census_collection` in ``driver.py``.

A reader instance handles **one (vintage, state) at a time**: the family driver
narrows a multi-vintage/multi-state spec to a single vintage (for schema
discovery) or a single ``(vintage, state)`` (for collection) before calling any
reader method, so ``spec.vintages``/``spec.states`` have one entry each here.

Three things about Census shape this reader:

  - **Immutable, full-refresh-only.** A published vintage never changes, and the
    API has no row cursor, so ``cursor_spec`` is ``None`` and the driver always
    runs a full read. Re-collecting an unchanged vintage is a no-op SCD2 merge.
  - **Variables drift across vintages.** ``schema`` is resolved per vintage
    (groups → variables); the family driver unions those schemas so one table
    holds every vintage's columns.
  - **Everything arrives as strings.** The Census API returns estimates/MOEs as
    strings; they land in ``NUMERIC`` columns and the engine casts them (exactly
    as Socrata's number columns do). Geo-id columns returned with spaces
    ("block group") are normalized to the underscore names the schema declares.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from typing import Any

from datadongle.collectors.census.client import CensusClient
from datadongle.collectors.census.spec import GEOGRAPHY_CONFIG, CensusDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, Append, WriteMode

logger = logging.getLogger(__name__)


class CensusReader:
    """Adapts one Census ``(vintage, state)`` to the shared collection driver."""

    source = "census"

    def __init__(
        self,
        api_key: str | None = None,
        requests_per_second: float = 5.0,
        client_factory: Callable[[], CensusClient] | None = None,
    ) -> None:
        self.api_key = api_key
        self.requests_per_second = requests_per_second
        self._client_factory = client_factory
        self._client: CensusClient | None = None
        # (dataset, vintage) -> resolved variable list, so a vintage's groups are
        # resolved once even though the driver reads it state by state.
        self._variables_cache: dict[tuple[str, int], list[str]] = {}

    @property
    def client(self) -> CensusClient:
        if self._client is None:
            if self._client_factory is not None:
                self._client = self._client_factory()
            else:
                self._client = CensusClient(
                    api_key=self.api_key, requests_per_second=self.requests_per_second
                )
        return self._client

    def _variables(self, spec: CensusDatasetSpec, vintage: int) -> list[str]:
        key = (spec.dataset, vintage)
        if key not in self._variables_cache:
            self._variables_cache[key] = self.client.resolve_all_variables(spec, vintage)
        return self._variables_cache[key]

    @staticmethod
    def _vintage(spec: CensusDatasetSpec) -> int:
        """The single vintage this call operates on (spec is narrowed by the driver)."""
        return spec.vintages[0]

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: CensusDatasetSpec) -> str:
        return f"{spec.target_table}/{self._vintage(spec)}/{spec.states[0]}"

    def target(self, spec: CensusDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: CensusDatasetSpec) -> TableSchema:
        """The single vintage's schema: geo ids + vintage + NAME + variables.

        Geo-id columns and ``vintage`` are the entity key, so they are declared
        ``not null``. Estimate/MOE variables are ``NUMERIC`` (the API returns
        them as strings; the engine casts on write)."""
        vintage = self._vintage(spec)
        variables = self._variables(spec, vintage)
        geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]

        columns = [Column(c, ColumnType.TEXT, nullable=False) for c in geo_columns]
        columns.append(Column("vintage", ColumnType.INTEGER, nullable=False))
        columns.append(Column("NAME", ColumnType.TEXT))
        columns.extend(Column(v, ColumnType.NUMERIC) for v in variables)
        return TableSchema(columns=columns)

    def write_mode(self, spec: CensusDatasetSpec, *, mode: str = "full") -> WriteMode:
        # Census specs always derive an entity_key (geo ids + vintage), so this
        # is SCD2 in practice; Append is only the empty-entity_key fallback.
        if spec.entity_key:
            return SCD2(entity_key=spec.entity_key)
        return Append()

    def cursor_spec(self, spec: CensusDatasetSpec) -> CursorSpec | None:
        # Vintages are immutable and the API has no row cursor — always full.
        return None

    def read(
        self, spec: CensusDatasetSpec, *, since: Cursor | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        # ``since`` is always None: cursor_spec() is None, so the driver never
        # reads a high-water mark. One batch per state (bounded memory).
        vintage = self._vintage(spec)
        variables = self._variables(spec, vintage)
        for state_fips in spec.states:
            rows = self.client.fetch_variables(
                dataset=spec.dataset,
                vintage=vintage,
                variables=variables,
                geography_level=spec.geography_level,
                state_fips=state_fips,
            )
            if rows:
                yield self._prepare_batch(rows, vintage)

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        return None

    # ------------------------------------------------------------------
    # Transforms
    # ------------------------------------------------------------------

    @staticmethod
    def _prepare_batch(rows: list[dict[str, Any]], vintage: int) -> list[dict[str, Any]]:
        """Stamp the vintage and normalize geo-id column names to match the schema.

        The API returns some geography levels with spaces in the id column
        ("block group", "zip code tabulation area"); the schema and entity key
        use underscore names, so spaces are replaced. Variable codes and NAME
        contain no spaces, so this only touches geo columns."""
        prepared = []
        for row in rows:
            new = {k.replace(" ", "_"): v for k, v in row.items()}
            new["vintage"] = vintage
            prepared.append(new)
        return prepared
