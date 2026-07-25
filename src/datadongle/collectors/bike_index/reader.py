"""BikeIndexReader — the Bike Index source adapter for the shared driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the existing
Bike Index ``client``/``spec``. It replaces the two-phase ``BikeIndexCollector``
(a search pass that wrote summary rows, then a detail pass that read ids back out
of the target table and enriched them). The driver decides full vs. incremental;
this reader supplies the schema, write policy, incremental cursor, and a single
enriched batch stream.

**Enrichment is inline, not a second phase.** Bike Index's ``/search`` endpoint
enumerates bikes (summary fields), and ``/bikes/{id}`` returns the full record.
A full record therefore needs two calls, but the second can happen *during*
pagination: ``read`` pages the search API and, for each bike past the cursor,
immediately calls ``get_bike`` and yields one fully-enriched row. This fits the
shared ``run_collection`` (no family driver), and gives each bike a single SCD2
version per change instead of the summary-then-detail pair the old collector
produced on every first pull. If a detail fetch fails, the bike still lands as a
summary-only row (detail columns null) rather than being dropped.

**Incremental cursor: ``date_stolen`` (+ ``id`` tiebreak).** The search API has
no server-side ordering we can rely on, so ``read`` scans every page and filters
each row strictly-after the cursor client-side. That still avoids the expensive
part of a re-pull — a ``get_bike`` call per already-seen bike — because the
filter runs before enrichment.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from typing import Any

from datadongle.collectors.bike_index.client import BikeIndexClient, BikeIndexSearchParams
from datadongle.collectors.bike_index.spec import BikeIndexDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, Append, WriteMode

logger = logging.getLogger(__name__)

# The incremental high-water-mark columns. ``date_stolen`` is a unix timestamp
# (bigint); ``id`` breaks ties between bikes stolen at the same second so a
# boundary row is never re-emitted or skipped.
CURSOR_COLUMN = "date_stolen"
TIEBREAK_COLUMN = "id"

# Columns serialized to JSON text before staging: Bike Index returns these as
# lists/arrays, and neither engine serializes a top-level list for a JSON column
# (Postgres normalizes dicts only; Iceberg maps JSON -> string).
_JSON_COLUMNS = ("frame_colors", "components", "public_images")

# The target table's columns, in DDL order. Bike Index has no column-catalog
# API, so this schema is fixed here (there is no ``metadata.py``). The engine
# adds the pipeline columns (``ingested_at`` and, for SCD2,
# ``record_hash``/``valid_from``/``valid_to``) at ``ensure_table`` time.
_COLUMNS: list[Column] = [
    Column("id", ColumnType.INTEGER, nullable=False),
    Column("title", ColumnType.TEXT),
    Column("serial", ColumnType.TEXT),
    Column("manufacturer_name", ColumnType.TEXT),
    Column("frame_model", ColumnType.TEXT),
    Column("frame_colors", ColumnType.JSON),
    Column("year", ColumnType.INTEGER),
    Column("stolen", ColumnType.BOOLEAN),
    Column("date_stolen", ColumnType.BIGINT),
    Column("description", ColumnType.TEXT),
    Column("thumb", ColumnType.TEXT),
    Column("url", ColumnType.TEXT),
    Column("stolen_coordinates_lat", ColumnType.DOUBLE),
    Column("stolen_coordinates_lon", ColumnType.DOUBLE),
    Column("stolen_location", ColumnType.TEXT),
    Column("latitude", ColumnType.DOUBLE),
    Column("longitude", ColumnType.DOUBLE),
    Column("theft_description", ColumnType.TEXT),
    Column("locking_description", ColumnType.TEXT),
    Column("lock_defeat_description", ColumnType.TEXT),
    Column("police_report_number", ColumnType.TEXT),
    Column("police_report_department", ColumnType.TEXT),
    Column("propulsion_type_slug", ColumnType.TEXT),
    Column("cycle_type_slug", ColumnType.TEXT),
    Column("status", ColumnType.TEXT),
    Column("registration_created_at", ColumnType.BIGINT),
    Column("registration_updated_at", ColumnType.BIGINT),
    Column("manufacturer_id", ColumnType.INTEGER),
    Column("paint_description", ColumnType.TEXT),
    Column("frame_size", ColumnType.TEXT),
    Column("frame_material_slug", ColumnType.TEXT),
    Column("handlebar_type_slug", ColumnType.TEXT),
    Column("front_gear_type_slug", ColumnType.TEXT),
    Column("rear_gear_type_slug", ColumnType.TEXT),
    Column("rear_wheel_size_iso_bsd", ColumnType.INTEGER),
    Column("front_wheel_size_iso_bsd", ColumnType.INTEGER),
    Column("rear_tire_narrow", ColumnType.BOOLEAN),
    Column("front_tire_narrow", ColumnType.BOOLEAN),
    Column("extra_registration_number", ColumnType.TEXT),
    Column("additional_registration", ColumnType.TEXT),
    Column("components", ColumnType.JSON),
    Column("public_images", ColumnType.JSON),
]


class BikeIndexReader:
    """Adapts a Bike Index stolen-bike search to the shared collection driver."""

    source = "bike_index"

    def __init__(
        self,
        access_token: str | None = None,
        timeout: float = 30.0,
        request_delay: float = 0.2,
        batch_size: int = 50,
        client: BikeIndexClient | None = None,
    ) -> None:
        self.access_token = access_token
        self.timeout = timeout
        self.request_delay = request_delay
        self.batch_size = batch_size
        self._client = client

    @property
    def client(self) -> BikeIndexClient:
        if self._client is None:
            self._client = BikeIndexClient(
                access_token=self.access_token,
                timeout=self.timeout,
                request_delay=self.request_delay,
            )
        return self._client

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: BikeIndexDatasetSpec) -> str:
        return spec.name

    def target(self, spec: BikeIndexDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: BikeIndexDatasetSpec) -> TableSchema:
        """The fixed Bike Index target schema (source has no column catalog)."""
        return TableSchema(columns=list(_COLUMNS))

    def write_mode(self, spec: BikeIndexDatasetSpec, *, mode: str = "incremental") -> WriteMode:
        # Bike Index's policy doesn't depend on the collection mode.
        if spec.entity_key:
            return SCD2(entity_key=spec.entity_key)
        return Append()

    def cursor_spec(self, spec: BikeIndexDatasetSpec) -> CursorSpec | None:
        return CursorSpec(column=CURSOR_COLUMN, tiebreak=TIEBREAK_COLUMN)

    def read(
        self, spec: BikeIndexDatasetSpec, *, since: Cursor | None
    ) -> Iterator[list[dict[str, Any]]]:
        """Page the search API, enrich each new bike via ``get_bike``, yield batches."""
        search = BikeIndexSearchParams(**spec.to_search_params())
        batch: list[dict[str, Any]] = []
        for page in self.client.search_all(search):
            for summary in page:
                if not self._is_after(summary, since):
                    continue
                batch.append(self._enrich(summary))
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        """Max ``(date_stolen, id)`` in an already-flattened batch."""
        values = [
            (row[CURSOR_COLUMN], row.get(TIEBREAK_COLUMN))
            for row in batch
            if row.get(CURSOR_COLUMN) is not None
        ]
        if not values:
            return None
        best = max(values, key=lambda x: (int(x[0]), int(x[1]) if x[1] is not None else 0))
        return Cursor(
            value=str(best[0]),
            tiebreak=str(best[1]) if best[1] is not None else None,
        )

    # ------------------------------------------------------------------
    # Cursor filtering
    # ------------------------------------------------------------------

    @staticmethod
    def _is_after(summary: dict[str, Any], since: Cursor | None) -> bool:
        """Strictly-after test for a raw search result against ``since``.

        Rows with no ``date_stolen`` are always kept (they carry no cursor
        value, so they can't be positioned relative to the high-water mark).
        """
        if since is None:
            return True
        ds = summary.get("date_stolen")
        if ds is None:
            return True
        sv = int(since.value)
        if ds > sv:
            return True
        if ds == sv and since.tiebreak is not None:
            return int(summary.get("id") or 0) > int(since.tiebreak)
        return False

    # ------------------------------------------------------------------
    # Enrichment + flattening
    # ------------------------------------------------------------------

    def _enrich(self, summary: dict[str, Any]) -> dict[str, Any]:
        """Fetch full detail for a search result; fall back to summary on failure."""
        bike_id = summary.get("id")
        if bike_id is None:
            logger.warning("search result has no id; storing summary only")
            return self._flatten_search(summary)
        try:
            detail = self.client.get_bike(bike_id)
        except Exception:
            logger.warning(
                "detail fetch failed for bike %s; storing summary only",
                bike_id,
                exc_info=True,
            )
            return self._flatten_search(summary)
        return self._flatten_detail(detail)

    @staticmethod
    def _flatten_search(summary: dict[str, Any]) -> dict[str, Any]:
        """Flatten a search result into a row dict (summary-level fields only).

        Detail-only columns are included as ``None`` so every row has the same
        set of keys as :meth:`_flatten_detail`.
        """
        coords = summary.get("stolen_coordinates") or []
        sc_lat = coords[0] if len(coords) == 2 else None
        sc_lon = coords[1] if len(coords) == 2 else None

        return {
            "id": summary.get("id"),
            "title": summary.get("title"),
            "serial": summary.get("serial"),
            "manufacturer_name": summary.get("manufacturer_name"),
            "frame_model": summary.get("frame_model"),
            "frame_colors": json.dumps(summary.get("frame_colors")),
            "year": summary.get("year"),
            "stolen": summary.get("stolen", True),
            "date_stolen": summary.get("date_stolen"),
            "description": summary.get("description"),
            "thumb": summary.get("thumb"),
            "url": summary.get("url"),
            "stolen_coordinates_lat": sc_lat,
            "stolen_coordinates_lon": sc_lon,
            "stolen_location": summary.get("stolen_location"),
            # Detail-only fields — null for search rows
            "latitude": None,
            "longitude": None,
            "theft_description": None,
            "locking_description": None,
            "lock_defeat_description": None,
            "police_report_number": None,
            "police_report_department": None,
            "propulsion_type_slug": summary.get("propulsion_type_slug"),
            "cycle_type_slug": summary.get("cycle_type_slug"),
            "status": summary.get("status"),
            "registration_created_at": None,
            "registration_updated_at": None,
            "manufacturer_id": None,
            "paint_description": None,
            "frame_size": None,
            "frame_material_slug": None,
            "handlebar_type_slug": None,
            "front_gear_type_slug": None,
            "rear_gear_type_slug": None,
            "rear_wheel_size_iso_bsd": None,
            "front_wheel_size_iso_bsd": None,
            "rear_tire_narrow": None,
            "front_tire_narrow": None,
            "extra_registration_number": None,
            "additional_registration": None,
            "components": None,
            "public_images": None,
        }

    @staticmethod
    def _flatten_detail(detail: dict[str, Any]) -> dict[str, Any]:
        """Flatten a ``get_bike()`` response into a full row dict."""
        stolen = detail.get("stolen_record") or {}

        coords = detail.get("stolen_coordinates") or []
        sc_lat = coords[0] if len(coords) == 2 else None
        sc_lon = coords[1] if len(coords) == 2 else None

        return {
            "id": detail.get("id"),
            "title": detail.get("title"),
            "serial": detail.get("serial"),
            "manufacturer_name": detail.get("manufacturer_name"),
            "frame_model": detail.get("frame_model"),
            "frame_colors": json.dumps(detail.get("frame_colors")),
            "year": detail.get("year"),
            "stolen": detail.get("stolen", True),
            "date_stolen": detail.get("date_stolen"),
            "description": detail.get("description"),
            "thumb": detail.get("thumb"),
            "url": detail.get("url"),
            "stolen_coordinates_lat": sc_lat,
            "stolen_coordinates_lon": sc_lon,
            "stolen_location": detail.get("stolen_location"),
            # From stolen_record
            "latitude": stolen.get("latitude"),
            "longitude": stolen.get("longitude"),
            "theft_description": stolen.get("theft_description"),
            "locking_description": stolen.get("locking_description"),
            "lock_defeat_description": stolen.get("lock_defeat_description"),
            "police_report_number": stolen.get("police_report_number"),
            "police_report_department": stolen.get("police_report_department"),
            # Detail-only scalar fields
            "propulsion_type_slug": detail.get("propulsion_type_slug"),
            "cycle_type_slug": detail.get("cycle_type_slug"),
            "status": detail.get("status"),
            "registration_created_at": detail.get("registration_created_at"),
            "registration_updated_at": detail.get("registration_updated_at"),
            "manufacturer_id": detail.get("manufacturer_id"),
            "paint_description": detail.get("paint_description"),
            "frame_size": detail.get("frame_size"),
            "frame_material_slug": detail.get("frame_material_slug"),
            "handlebar_type_slug": detail.get("handlebar_type_slug"),
            "front_gear_type_slug": detail.get("front_gear_type_slug"),
            "rear_gear_type_slug": detail.get("rear_gear_type_slug"),
            "rear_wheel_size_iso_bsd": detail.get("rear_wheel_size_iso_bsd"),
            "front_wheel_size_iso_bsd": detail.get("front_wheel_size_iso_bsd"),
            "rear_tire_narrow": detail.get("rear_tire_narrow"),
            "front_tire_narrow": detail.get("front_tire_narrow"),
            "extra_registration_number": detail.get("extra_registration_number"),
            "additional_registration": detail.get("additional_registration"),
            # Nested arrays as JSON text
            "components": json.dumps(detail.get("components")),
            "public_images": json.dumps(detail.get("public_images")),
        }
