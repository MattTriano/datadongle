"""OSMReader — the OSM/Overpass source adapter for the shared collection driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the
``query``/``client``/``spec`` pieces. It replaces the orchestration that used to
live in ``OSMCollector.collect``: the driver decides full vs. incremental, and
this reader supplies the (fixed) schema, write policy, incremental cursor, and
the batch stream — applying the OSM-specific transforms (geometry assembly to
EWKT, tag promotion, JSON-encoding of ``tags``/``node_ids``).

Two things about OSM differ from a catalog-backed source like Socrata:

  - **No metadata class.** OSM has no catalog to inspect; the target schema is
    the fixed OSM columns plus one text column per promoted tag, derived
    entirely from the spec.
  - **The high-water mark is our own ``ingested_at``, not a source column.**
    Incremental pulls fetch elements edited since we last collected, so the
    driver reads ``max(ingested_at)`` back from the target table (via
    ``cursor_spec``) and this reader feeds it to Overpass's ``(newer:)`` filter.
    ``ingested_at`` is stamped by the engine, so it can't be recovered from a
    batch — ``extract_cursor`` returns ``None`` and the real HWM lives in the
    table.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from datadongle.collectors.osm.client import OSMClient
from datadongle.collectors.osm.geometry import element_to_wkt
from datadongle.collectors.osm.spec import OSMDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema
from datadongle.core.write_mode import SCD2, WriteMode

logger = logging.getLogger(__name__)

# Default flush threshold: Overpass returns the whole result in one response, so
# we chunk it into batches to bound the engine's per-write buffer (matters for
# the Postgres COPY; a no-op for Iceberg's in-memory append).
DEFAULT_BATCH_SIZE = 50000

# Our own ingest timestamp is the incremental floor — "everything edited since
# we last collected." Stamped by the engine, read back from the target table.
CURSOR_COLUMN = "ingested_at"

# The fixed OSM columns, in DDL order. Promoted-tag columns append after these.
# ``osm_version``/``osm_timestamp`` are ``metadata=True`` so they're stored but
# excluded from the SCD2 content hash — they bump on every OSM edit and would
# otherwise spuriously version every unchanged element on a re-pull.
_FIXED_COLUMNS: list[Column] = [
    Column("osm_type", ColumnType.TEXT, nullable=False),
    Column("osm_id", ColumnType.BIGINT, nullable=False),
    Column("osm_version", ColumnType.INTEGER, metadata=True),
    Column("osm_timestamp", ColumnType.TIMESTAMPTZ, metadata=True),
    Column("geom", ColumnType.GEOMETRY, geometry=GeometrySpec(kind="Geometry", srid=4326)),
    Column("tags", ColumnType.JSON),
    Column("node_ids", ColumnType.JSON),
]


class OSMReader:
    """Adapts an OSM Overpass dataset to the shared collection driver."""

    source = "osm"

    def __init__(
        self,
        client: OSMClient | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.client = client or OSMClient()
        self.batch_size = batch_size

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: OSMDatasetSpec) -> str:
        return spec.name

    def target(self, spec: OSMDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: OSMDatasetSpec) -> TableSchema:
        """Fixed OSM columns + one text column per promoted tag."""
        columns = list(_FIXED_COLUMNS)
        columns.extend(Column(name, ColumnType.TEXT) for name in spec.promoted_columns)
        return TableSchema(columns=columns)

    def write_mode(self, spec: OSMDatasetSpec, *, mode: str) -> WriteMode:
        """SCD2 keyed on the OSM element identity.

        ``invalidate_missing`` is enabled only on a ``"full"`` pull: a full pull
        observes every element in the extent, so elements absent from it are
        genuinely gone from OSM and get closed out. An incremental pull sees
        only recent edits, so absence proves nothing — the driver would reject
        ``invalidate_missing`` there anyway.
        """
        return SCD2(entity_key=spec.entity_key, invalidate_missing=(mode == "full"))

    def cursor_spec(self, spec: OSMDatasetSpec) -> CursorSpec | None:
        return CursorSpec(column=CURSOR_COLUMN)

    def read(self, spec: OSMDatasetSpec, *, since: Cursor | None) -> Iterator[list[dict[str, Any]]]:
        date_filter = _to_overpass_timestamp(since.value) if since is not None else None

        assert spec.query is not None  # OSMDatasetSpec requires it
        response = self.client.fetch(spec.query, date_filter=date_filter)
        elements = response.get("elements") or []
        logger.info(
            "Overpass returned %d elements for %r (date_filter=%s)",
            len(elements),
            spec.name,
            date_filter,
        )

        batch: list[dict[str, Any]] = []
        for element in elements:
            batch.append(_element_to_row(element, spec))
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        # OSM's HWM is the engine-stamped ``ingested_at``, read back from the
        # target table (see ``cursor_spec``) — not recoverable from a batch.
        return None


# ----------------------------------------------------------------------
# Element -> row dict  (OSM-specific transforms)
# ----------------------------------------------------------------------


def _element_to_row(element: dict, spec: OSMDatasetSpec) -> dict[str, Any]:
    """Turn one Overpass JSON element into a stage-ready row dict.

    Geometry is assembled and emitted as EWKT (``SRID=4326;...``) so the
    PostGIS ``geometry(...,4326)`` cast accepts it; ``tags`` and ``node_ids``
    are JSON-encoded so both engines store valid JSON text. Promoted tag columns
    are looked up by original key and written under their normalized name.
    """
    tags = element.get("tags") or {}
    nodes = element.get("nodes")
    wkt = element_to_wkt(element)

    row: dict[str, Any] = {
        "osm_type": element.get("type"),
        "osm_id": element.get("id"),
        "osm_version": element.get("version"),
        "osm_timestamp": element.get("timestamp"),
        "geom": f"SRID=4326;{wkt}" if wkt is not None else None,
        "tags": json.dumps(tags, sort_keys=True, ensure_ascii=False),
        "node_ids": json.dumps(nodes) if nodes else None,
    }

    for original_key, column_name in spec.tag_column_map.items():
        row[column_name] = tags.get(original_key)

    return row


def _to_overpass_timestamp(value: str) -> str:
    """Convert a target-table high-water-mark value to an Overpass timestamp.

    The engines hand back ``ingested_at`` as a canonical ISO string (UTC, no
    zone suffix). Overpass's ``(newer:)`` wants an ISO-8601 instant with a
    trailing ``Z``; we normalize to whole seconds for cleanliness.
    """
    dt = datetime.fromisoformat(value)
    dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
