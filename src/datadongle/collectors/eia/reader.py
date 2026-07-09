"""EIAReader — the EIA API v2 source adapter for the shared driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the EIA
``client``/``spec``. EIA's API is uniform across its whole route tree — a route
is a self-contained data series, frequency is a query parameter, and geography/
sector are facets — so one reader serves any dataset and each spec lands in one
target table via the shared ``run_collection`` (no family driver).

Three things about EIA shape this reader:

  - **Incremental cursor is ``period``.** EIA data is a time series; the driver
    reads the target's max ``period`` and this reader asks the API for rows from
    there (``start=since``), then filters strictly-after client-side. Periods
    are zero-padded, fixed-width strings within a frequency (``"2001-01"``,
    ``"2024"``), so string comparison orders them correctly.

  - **Measures arrive as strings; we cast to ``DOUBLE`` fail-loud.** EIA returns
    every value as a JSON string ("data values standardized to strings").
    ``null``/empty ⇒ ``None``; a numeric string ⇒ ``float``; anything else
    ⇒ a raised error, so a suppression/withheld marker is never silently nulled.

  - **Column names are normalized so they need no quoting.** Facet ids, their
    camelCase description columns (``stateDescription``), and hyphenated units
    columns (``price-units``) are all lowered and non-alphanumerics collapsed to
    ``_`` (``statedescription``, ``price_units``).

**Revisions.** EIA revises historical values and rows carry no per-row updated
timestamp, so the ``period`` cursor catches new periods but not revisions to old
ones. Schedule periodic ``mode="full"`` refreshes; SCD2 turns those into a new
version only where a value actually changed.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

from datadongle.collectors.eia.client import EIAClient
from datadongle.collectors.eia.spec import EIADatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, Append, WriteMode

# The incremental high-water-mark column. Every EIA data row carries ``period``.
CURSOR_COLUMN = "period"

_NON_IDENT = re.compile(r"[^a-z0-9]+")


class EIAReader:
    """Adapts one EIA API v2 data series to the shared collection driver."""

    source = "eia"

    def __init__(
        self,
        api_key: str | None = None,
        timeout: int = 60,
        client: EIAClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self._client = client

    @property
    def client(self) -> EIAClient:
        if self._client is None:
            self._client = EIAClient(api_key=self.api_key, timeout=self.timeout)
        return self._client

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: EIADatasetSpec) -> str:
        return f"{spec.route_path}/{spec.frequency}"

    def target(self, spec: EIADatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: EIADatasetSpec) -> TableSchema:
        """Discover columns by sampling one data row.

        The requested measure columns are typed ``DOUBLE``; every other column
        (``period``, facet ids, description columns, ``<measure>-units``) is
        ``TEXT``. Names are normalized. ``period`` is non-nullable.
        """
        resp = self.client.get_data_page(
            spec.route_path,
            frequency=spec.frequency,
            data_columns=spec.data_columns,
            facets=spec.facets,
            start=spec.start,
            end=spec.end,
            offset=0,
            length=1,
        )
        sample = resp.get("data") or []
        if not sample:
            raise ValueError(
                f"EIA {self.dataset_id(spec)!r} returned no rows for the given "
                f"facets/date range; its schema cannot be discovered."
            )
        measures = self._measure_columns(spec)
        columns = []
        for raw in sample[0]:
            name = self._normalize(raw)
            col_type = ColumnType.DOUBLE if name in measures else ColumnType.TEXT
            columns.append(Column(name, col_type, nullable=name != CURSOR_COLUMN))
        return TableSchema(columns=columns)

    def write_mode(
        self, spec: EIADatasetSpec, *, mode: str = "incremental"
    ) -> WriteMode:
        # entity_key (facet ids + period) ⇒ versioned history; else append.
        if spec.entity_key:
            return SCD2(entity_key=spec.entity_key)
        return Append()

    def cursor_spec(self, spec: EIADatasetSpec) -> CursorSpec | None:
        return CursorSpec(column=CURSOR_COLUMN)

    def read(
        self, spec: EIADatasetSpec, *, since: Cursor | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        """Page the route's data, normalize/cast rows, yield batches.

        Incremental: ``start`` is set to the high-water mark (server-side narrow)
        and rows are filtered strictly-after it client-side, so the boundary
        period isn't re-emitted.
        """
        measures = self._measure_columns(spec)
        start = since.value if since is not None else spec.start

        name_map: dict[str, str] | None = None
        for page in self.client.iter_data(
            spec.route_path,
            frequency=spec.frequency,
            data_columns=spec.data_columns,
            facets=spec.facets,
            start=start,
            end=spec.end,
        ):
            if name_map is None and page:
                name_map = {k: self._normalize(k) for k in page[0]}
            batch = self._prepare_batch(page, name_map or {}, measures, spec)
            if since is not None:
                batch = [r for r in batch if self._is_after(r, since)]
            if batch:
                yield batch

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        """The max ``period`` in an already-transformed batch."""
        periods = [r[CURSOR_COLUMN] for r in batch if r.get(CURSOR_COLUMN) is not None]
        if not periods:
            return None
        return Cursor(value=max(periods))

    # ------------------------------------------------------------------
    # Transforms
    # ------------------------------------------------------------------

    def _prepare_batch(
        self,
        page: list[dict[str, Any]],
        name_map: dict[str, str],
        measures: set[str],
        spec: EIADatasetSpec,
    ) -> list[dict[str, Any]]:
        """Normalize column names and cast measure columns to double."""
        prepared = []
        for row in page:
            new: dict[str, Any] = {}
            for raw, value in row.items():
                if raw not in name_map:
                    continue
                name = name_map[raw]
                if name in measures:
                    new[name] = self._to_double(value, name, spec)
                else:
                    new[name] = value
            prepared.append(new)
        return prepared

    @staticmethod
    def _is_after(row: dict[str, Any], since: Cursor) -> bool:
        """Strictly-after test on ``period`` (rows with no period are kept)."""
        period = row.get(CURSOR_COLUMN)
        if period is None:
            return True
        return period > since.value

    def _measure_columns(self, spec: EIADatasetSpec) -> set[str]:
        """The normalized names of the requested measure (``data[]``) columns."""
        return {self._normalize(c) for c in spec.data_columns}

    @staticmethod
    def _to_double(value: Any, column: str, spec: EIADatasetSpec) -> float | None:
        """Cast an EIA measure value to float, failing loud on lossy input.

        ``None``/empty ⇒ ``None`` (genuine missing). A numeric string or number
        ⇒ ``float``. Any other non-empty string is signal-bearing (e.g. a
        withheld/suppressed marker) and is **not** silently nulled — we raise so
        the encoding is surfaced rather than lost.
        """
        if value is None:
            return None
        if isinstance(value, bool):  # guard: bools are ints in Python
            raise ValueError(
                f"EIA {spec.route_path!r} column {column!r} has boolean value "
                f"{value!r}; expected a numeric measure."
            )
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if text == "":
            return None
        try:
            return float(text)
        except ValueError:
            raise ValueError(
                f"EIA {spec.route_path!r} column {column!r} has non-numeric value "
                f"{value!r} that casting to double would silently drop. If this is "
                f"a suppression/withheld marker, keep the column as TEXT instead."
            ) from None

    @staticmethod
    def _normalize(name: str) -> str:
        """Lowercase and collapse non-alphanumerics to ``_`` (queryable unquoted).

        A leading BOM (some sources prepend ``\\ufeff``) is non-alphanumeric, so
        the regex maps it to ``_`` and ``strip("_")`` drops it — no special case.
        """
        return _NON_IDENT.sub("_", name.strip().lower()).strip("_")
