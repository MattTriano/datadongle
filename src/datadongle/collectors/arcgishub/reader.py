"""ArcGISHubReader — the ArcGIS Hub source adapter for the shared driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the
``client``/``metadata``/``spec`` pieces, replacing the orchestration that used
to live in ``ArcGISHubCollector.collect``. The driver decides full vs.
incremental; this reader supplies the (discovered) schema, write policy,
incremental cursor, and the batch stream — applying the ArcGIS-specific
transforms (attribute lowercasing, epoch-ms dates → ISO, geometry → EWKT).

Two things shape this reader:

  - **Schema is discovered from the layer(s).** An Esri layer reports its
    ``fields``; ``schema`` maps those to neutral ``ColumnType``s and adds the
    geometry column. A Hub item can wrap several layers (``layer_index`` as a
    list or ``"all"``); those are collected into one table, so ``schema``
    returns the *union* of their fields (guarded by ``min_field_overlap``) and
    ``read`` iterates the layers into the single write session the driver opens.

  - **The high-water mark is a source date column, stored as ISO.** ArcGIS date
    fields arrive as epoch milliseconds and are flattened to ISO strings, so the
    engine reads ``max(incremental_column)`` back as ISO (via ``cursor_spec``)
    and this reader converts it back to epoch ms for the Esri ``where`` filter.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any

from datadongle.collectors.arcgishub.client import ArcGISHubClient
from datadongle.collectors.arcgishub.metadata import ArcGISHubMetadata
from datadongle.collectors.arcgishub.spec import ArcGISHubDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema
from datadongle.core.write_mode import SCD2, Append, WriteMode

logger = logging.getLogger(__name__)

# Esri field type -> neutral ColumnType. Unmapped types fall back to TEXT.
_ARCGIS_TO_COLUMN_TYPE: dict[str, ColumnType] = {
    "esriFieldTypeOID": ColumnType.BIGINT,
    "esriFieldTypeInteger": ColumnType.INTEGER,
    "esriFieldTypeSmallInteger": ColumnType.INTEGER,
    "esriFieldTypeBigInteger": ColumnType.BIGINT,
    "esriFieldTypeDouble": ColumnType.DOUBLE,
    "esriFieldTypeSingle": ColumnType.DOUBLE,
    "esriFieldTypeString": ColumnType.TEXT,
    "esriFieldTypeDate": ColumnType.TIMESTAMPTZ,
    "esriFieldTypeGUID": ColumnType.TEXT,
    "esriFieldTypeGlobalID": ColumnType.TEXT,
}

# Esri geometry type -> OGC geometry kind. Polylines/polygons are emitted as
# their Multi* forms since Esri paths/rings are multi by nature.
_ARCGIS_TO_GEOMETRY_KIND: dict[str, str] = {
    "esriGeometryPoint": "Point",
    "esriGeometryMultipoint": "MultiPoint",
    "esriGeometryPolyline": "MultiLineString",
    "esriGeometryPolygon": "MultiPolygon",
}

# Source field names that collide with the emitted geometry column; renamed to
# _orig_{name} in both the schema and the flattened rows.
_GEOMETRY_COLLISION_NAMES = {"geom", "geog"}

# Geometry always emitted in WGS84 — the query requests outSR=4326.
_OUT_SRID = 4326


class ArcGISHubReader:
    """Adapts an ArcGIS Hub feature service to the shared collection driver."""

    source = "arcgis_hub"

    def __init__(
        self,
        client_factory: Callable[[str], ArcGISHubClient] | None = None,
    ) -> None:
        # A client is bound to one Hub site, and the site lives on the spec's
        # ``base_url`` — so build (and cache) clients per base_url.
        self._client_factory = client_factory or (lambda base_url: ArcGISHubClient(base_url))
        self._clients: dict[str, ArcGISHubClient] = {}
        # Within a reader's lifetime, resolved service URLs / layer lists /
        # layer info are stable — cache them so schema() and read() don't
        # re-hit the server for the same spec.
        self._service_url_cache: dict[str, str] = {}
        self._layers_cache: dict[str, list[dict[str, Any]]] = {}
        self._layer_info_cache: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: ArcGISHubDatasetSpec) -> str:
        return spec.dataset_id

    def target(self, spec: ArcGISHubDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: ArcGISHubDatasetSpec) -> TableSchema:
        """Union of the resolved layers' fields + geometry + optional layer column."""
        layer_infos = [self._get_layer_info(spec, idx) for idx, _name in self._resolve_layers(spec)]
        if len(layer_infos) > 1:
            self._check_field_overlap(layer_infos, spec)
        return _build_schema(spec, layer_infos)

    def write_mode(self, spec: ArcGISHubDatasetSpec, *, mode: str) -> WriteMode:
        # SCD2 when the spec names an entity key, else append-only. ArcGIS has no
        # invalidate_missing analogue, so the collection mode doesn't matter. The
        # key is lowercased to match the stored columns (attributes are
        # lowercased in schema() and in the flattened rows).
        if spec.entity_key:
            return SCD2(entity_key=[c.lower() for c in spec.entity_key])
        return Append()

    def cursor_spec(self, spec: ArcGISHubDatasetSpec) -> CursorSpec | None:
        # No incremental column -> not incrementally queryable; the driver runs a
        # full read. The column is stored lowercased (attributes are lowercased).
        if not spec.incremental_column:
            return None
        return CursorSpec(column=spec.incremental_column.lower())

    def read(
        self, spec: ArcGISHubDatasetSpec, *, since: Cursor | None
    ) -> Iterator[list[dict[str, Any]]]:
        # The engine hands back the HWM as ISO; ArcGIS filters on epoch ms.
        since_epoch_ms = _iso_to_epoch_ms(since.value) if since is not None else None
        for layer_index, layer_name in self._resolve_layers(spec):
            layer_info = self._get_layer_info(spec, layer_index)
            where = _build_where(spec, since_epoch_ms)
            for page in self._paginate_features(spec, layer_info, where):
                yield [_flatten_feature(feat, layer_info, spec, layer_name) for feat in page]

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        # The incremental column is a source column stored on the table, so the
        # authoritative HWM is read back from there via ``cursor_spec`` — no need
        # to recover it from a batch (matches the OSM reader).
        return None

    # ------------------------------------------------------------------
    # Client / metadata
    # ------------------------------------------------------------------

    def _client(self, spec: ArcGISHubDatasetSpec) -> ArcGISHubClient:
        if spec.base_url not in self._clients:
            self._clients[spec.base_url] = self._client_factory(spec.base_url)
        return self._clients[spec.base_url]

    def _metadata(self, spec: ArcGISHubDatasetSpec) -> ArcGISHubMetadata:
        return ArcGISHubMetadata(self._client(spec))

    # ------------------------------------------------------------------
    # Layer discovery
    # ------------------------------------------------------------------

    def _resolve_layers(self, spec: ArcGISHubDatasetSpec) -> list[tuple[int, str]]:
        """Resolve ``layer_index`` (int / list[int] / "all") to (index, name) pairs."""
        if isinstance(spec.layer_index, int):
            info = self._get_layer_info(spec, spec.layer_index)
            return [(spec.layer_index, info["name"])]

        available = self._list_layers(spec)
        if spec.layer_index == "all":
            return available
        if isinstance(spec.layer_index, str):
            raise ValueError(
                f"Unknown layer_index {spec.layer_index!r}. Use an int, a list of ints, or 'all'."
            )

        available_by_id = {idx: name for idx, name in available}
        resolved = []
        for idx in spec.layer_index:
            if idx not in available_by_id:
                raise ValueError(
                    f"Layer {idx} not found in service. Available: {[i for i, _ in available]}"
                )
            resolved.append((idx, available_by_id[idx]))
        return resolved

    def _list_layers(self, spec: ArcGISHubDatasetSpec) -> list[tuple[int, str]]:
        """Available (index, name) pairs from the service ``/layers`` endpoint."""
        service_url = self._resolve_service_url(spec)
        if service_url not in self._layers_cache:
            payload = self._client(spec).get_json(f"{service_url}/layers", params={"f": "json"})
            if "error" in payload:
                err = payload["error"]
                raise RuntimeError(f"ArcGIS /layers error: {err.get('code')} - {err.get('message')}")
            self._layers_cache[service_url] = payload.get("layers") or []
        return [(layer["id"], layer.get("name", str(layer["id"]))) for layer in self._layers_cache[service_url]]

    def _resolve_service_url(self, spec: ArcGISHubDatasetSpec) -> str:
        """Feature service URL from the Hub item metadata."""
        if spec.item_id not in self._service_url_cache:
            item = self._metadata(spec).get_dataset(spec.item_id)
            props = item.get("properties") or {}
            url = props.get("url") or item.get("url")
            if not url:
                raise ValueError(
                    f"Could not find feature service URL in metadata for item {spec.item_id!r}"
                )
            self._service_url_cache[spec.item_id] = url.rstrip("/")
        return self._service_url_cache[spec.item_id]

    def _get_layer_info(self, spec: ArcGISHubDatasetSpec, layer_index: int) -> dict[str, Any]:
        """Fetch and cache one layer's fields, geometry type, and paging hints.

        Tries the direct ``/{layer_index}`` endpoint first; on an Esri error
        (returned as HTTP 200 with an ``error`` body) falls back to the layer's
        entry in ``/layers``.
        """
        cache_key = f"{spec.item_id}:{layer_index}"
        if cache_key in self._layer_info_cache:
            return self._layer_info_cache[cache_key]

        service_url = self._resolve_service_url(spec)
        layer_url = f"{service_url}/{layer_index}"
        layer = self._client(spec).get_json(layer_url, params={"f": "json"})
        if "error" in layer:
            logger.warning("Direct layer endpoint %s errored; falling back to /layers", layer_url)
            layer = self._layer_from_layers_endpoint(spec, layer_index)

        fields = layer.get("fields") or []
        if not fields:
            raise RuntimeError(f"No fields found for layer {layer_index} at {service_url}")

        info = {
            "service_url": service_url,
            "layer_url": layer_url,
            "name": layer.get("name", str(layer_index)),
            "fields": fields,
            "max_record_count": layer.get("maxRecordCount", 1000),
            "geometry_type": layer.get("geometryType"),
            "srid": _OUT_SRID,
            "oid_field": _find_oid_field(fields),
            "date_fields": {f["name"] for f in fields if f.get("type") == "esriFieldTypeDate"},
        }
        self._layer_info_cache[cache_key] = info
        return info

    def _layer_from_layers_endpoint(
        self, spec: ArcGISHubDatasetSpec, layer_index: int
    ) -> dict[str, Any]:
        available = self._list_layers(spec)
        for layer in self._layers_cache[self._resolve_service_url(spec)]:
            if layer["id"] == layer_index:
                return layer
        raise ValueError(
            f"Layer {layer_index} not found in /layers response. "
            f"Available: {[idx for idx, _ in available]}"
        )

    def _check_field_overlap(
        self, layer_infos: list[dict[str, Any]], spec: ArcGISHubDatasetSpec
    ) -> None:
        """Raise if the layers being unioned share too few fields."""
        field_sets = [{f["name"].lower() for f in info["fields"]} for info in layer_infos]
        all_fields = set().union(*field_sets)
        if not all_fields:
            return
        shared = set.intersection(*field_sets)
        overlap = len(shared) / len(all_fields)
        if overlap < spec.min_field_overlap:
            raise ValueError(
                f"Field overlap across layers is {overlap:.0%} "
                f"({len(shared)}/{len(all_fields)}), below threshold of "
                f"{spec.min_field_overlap:.0%}. Only in some layers: {all_fields - shared}"
            )
        logger.info(
            "Field overlap: %d/%d (%.0f%%); only in some layers: %s",
            len(shared), len(all_fields), overlap * 100, all_fields - shared or "none",
        )

    # ------------------------------------------------------------------
    # Feature paging
    # ------------------------------------------------------------------

    def _paginate_features(
        self, spec: ArcGISHubDatasetSpec, layer_info: dict[str, Any], where: str
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield pages of raw features from the layer ``/query`` endpoint.

        Orders by the OID field for stable paging (ArcGIS gives no stable order
        otherwise) and follows ``exceededTransferLimit``.
        """
        query_url = f"{layer_info['layer_url']}/query"
        max_records = layer_info["max_record_count"]
        oid_field = layer_info["oid_field"] or "OBJECTID"
        out_fields = ",".join(spec.out_fields) if spec.out_fields else "*"

        offset = 0
        while True:
            payload = self._client(spec).get_json(
                query_url,
                params={
                    "f": "json",
                    "where": where,
                    "outFields": out_fields,
                    "returnGeometry": "true",
                    "outSR": layer_info["srid"],
                    "orderByFields": oid_field,
                    "resultOffset": offset,
                    "resultRecordCount": max_records,
                },
            )
            if "error" in payload:
                err = payload["error"]
                raise RuntimeError(f"ArcGIS query error {err.get('code')}: {err.get('message')}")

            features = payload.get("features") or []
            if not features:
                return
            yield features

            if payload.get("exceededTransferLimit") is False:
                return
            if len(features) < max_records:
                return
            offset += len(features)


# ----------------------------------------------------------------------
# Schema construction
# ----------------------------------------------------------------------


def _build_schema(
    spec: ArcGISHubDatasetSpec, layer_infos: list[dict[str, Any]]
) -> TableSchema:
    """Neutral schema = union of layer fields + geometry + optional layer column.

    When unioning layers, the first layer's type wins for a shared field (the
    overlap check keeps them consistent). Fields only in some layers are still
    included — a row from a layer lacking a field simply omits it (NULL).
    """
    merged: dict[str, dict[str, Any]] = {}  # lowered name -> Esri field dict
    has_geometry = False
    geometry_type: str | None = None
    for info in layer_infos:
        for f in info["fields"]:
            merged.setdefault(f["name"].lower(), f)
        if info.get("geometry_type"):
            has_geometry = True
            geometry_type = info["geometry_type"]

    columns: list[Column] = []
    seen: set[str] = set()
    for name, f in merged.items():
        col_name = name
        if has_geometry and name in _GEOMETRY_COLLISION_NAMES:
            col_name = _rename_collision(name, seen)
        seen.add(col_name)
        # The OID field is stored but excluded from the SCD2 hash: OBJECTID is
        # not stable across service refreshes and would spuriously version rows.
        is_oid = f.get("type") == "esriFieldTypeOID"
        col_type = _ARCGIS_TO_COLUMN_TYPE.get(f.get("type"), ColumnType.TEXT)
        columns.append(Column(col_name, col_type, metadata=is_oid))

    if has_geometry:
        kind = _ARCGIS_TO_GEOMETRY_KIND.get(geometry_type, "Geometry")
        columns.append(
            Column("geom", ColumnType.GEOMETRY, geometry=GeometrySpec(kind=kind, srid=_OUT_SRID))
        )
    if spec.layer_column:
        columns.append(Column(spec.layer_column.lower(), ColumnType.TEXT))
    return TableSchema(columns=columns)


# ----------------------------------------------------------------------
# Feature -> row dict  (ArcGIS-specific transforms)
# ----------------------------------------------------------------------


def _flatten_feature(
    feat: dict[str, Any],
    layer_info: dict[str, Any],
    spec: ArcGISHubDatasetSpec,
    layer_name: str,
) -> dict[str, Any]:
    """Turn one ArcGIS feature into a stage-ready row dict.

    Attribute names are lowercased; fields colliding with the geometry column
    are renamed; date fields (epoch ms) become ISO UTC strings; geometry becomes
    EWKT; the layer name is added when ``layer_column`` is set.
    """
    attrs = feat.get("attributes") or {}
    date_fields_lower = {f.lower() for f in layer_info["date_fields"]}
    has_geometry = bool(layer_info.get("geometry_type"))

    row: dict[str, Any] = {}
    for k, v in attrs.items():
        key = k.lower()
        if has_geometry and key in _GEOMETRY_COLLISION_NAMES:
            key = _rename_collision(key, set(row.keys()))
        row[key] = _epoch_ms_to_iso(v) if key in date_fields_lower and v is not None else v

    if has_geometry:
        row["geom"] = _geometry_to_ewkt(
            feat.get("geometry"), layer_info["geometry_type"], layer_info["srid"]
        )
    if spec.layer_column:
        row[spec.layer_column.lower()] = layer_name
    return row


# ----------------------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------------------


def _build_where(spec: ArcGISHubDatasetSpec, since_epoch_ms: int | None) -> str:
    """Combine the spec's static filter with an incremental floor, if any."""
    base = spec.where or "1=1"
    if since_epoch_ms is None or not spec.incremental_column:
        return base
    return f"({base}) AND ({spec.incremental_column} > {since_epoch_ms})"


def _find_oid_field(fields: list[dict[str, Any]]) -> str | None:
    for f in fields:
        if f.get("type") == "esriFieldTypeOID":
            return f["name"]
    return None


def _rename_collision(name: str, existing: set[str]) -> str:
    """Rename a field colliding with the geometry column: _orig_{name}[_n]."""
    candidate = f"_orig_{name}"
    if candidate not in existing:
        return candidate
    n = 2
    while f"{candidate}_{n}" in existing:
        n += 1
    return f"{candidate}_{n}"


def _epoch_ms_to_iso(val: Any) -> str | None:
    """ArcGIS date (epoch ms) -> ISO UTC string."""
    try:
        return datetime.fromtimestamp(int(val) / 1000, tz=UTC).isoformat()
    except (TypeError, ValueError, OverflowError):
        return None


def _iso_to_epoch_ms(value: str) -> int:
    """Target-table HWM (ISO string) -> epoch ms for the Esri ``where`` filter."""
    dt = datetime.fromisoformat(value)
    dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return int(dt.timestamp() * 1000)


def _geometry_to_ewkt(
    geom: dict[str, Any] | None, geometry_type: str, srid: int
) -> str | None:
    """Convert an ArcGIS geometry object to EWKT (``SRID=<srid>;...``)."""
    if not geom:
        return None
    prefix = f"SRID={srid};"

    if geometry_type == "esriGeometryPoint":
        x, y = geom.get("x"), geom.get("y")
        if x is None or y is None:
            return None
        return f"{prefix}POINT({x} {y})"

    if geometry_type == "esriGeometryMultipoint":
        pts = geom.get("points") or []
        if not pts:
            return None
        inner = ", ".join(f"({p[0]} {p[1]})" for p in pts)
        return f"{prefix}MULTIPOINT({inner})"

    if geometry_type == "esriGeometryPolyline":
        paths = geom.get("paths") or []
        if not paths:
            return None
        parts = [f"({', '.join(f'{p[0]} {p[1]}' for p in path)})" for path in paths]
        return f"{prefix}MULTILINESTRING({', '.join(parts)})"

    if geometry_type == "esriGeometryPolygon":
        rings = geom.get("rings") or []
        if not rings:
            return None
        # ArcGIS convention: clockwise rings are outer, counter-clockwise are
        # holes. Group each hole with the preceding outer ring.
        polygons: list[list[str]] = []
        for ring in rings:
            coords = ", ".join(f"{p[0]} {p[1]}" for p in ring)
            if _ring_is_clockwise(ring) or not polygons:
                polygons.append([f"({coords})"])
            else:
                polygons[-1].append(f"({coords})")
        parts = [f"({', '.join(ring_list)})" for ring_list in polygons]
        return f"{prefix}MULTIPOLYGON({', '.join(parts)})"

    return None


def _ring_is_clockwise(ring: list[list[float]]) -> bool:
    """Sign of the shoelace sum: clockwise (outer ring) per the ArcGIS convention."""
    total = 0.0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i][0], ring[i][1]
        x2, y2 = ring[i + 1][0], ring[i + 1][1]
        total += (x2 - x1) * (y2 + y1)
    return total > 0
