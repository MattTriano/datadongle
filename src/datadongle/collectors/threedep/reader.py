"""ThreeDEPReader — the USGS 3DEP source adapter for the shared driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the existing
3DEP ``client``/``spec``. It replaces the per-tile load logic that used to live
in ``ThreeDEPCollector``; the family-level orchestration (per-tile write
sessions, error isolation) lives in :func:`run_threedep_collection` in
``driver.py``.

Three things about 3DEP shape this reader:

  - **The unit of work is one 1-degree source tile.** ``read`` downloads each
    in-scope tile to a temp file (rasterio reads windows off disk, so peak
    memory is one sub-tile), cuts it into sub-tiles clipped to the spec's bbox,
    and yields them as rows. ``tile_id`` is namespaced by the 1-degree tile
    name ("<name>/<row>_<col>") so sub-tiles stay unique across tiles.
  - **The incremental cursor is the 1-degree tile name.** The seamless products
    are essentially static, so "incremental" means "collect only tiles not yet
    in the target": ``source_tile`` (the nNNwWWW name) is the high-water mark,
    tiles are processed in sorted order, and an incremental read collects only
    tiles strictly after the max name already present. This preserves the cheap
    path — hundreds of MB per already-held tile are never re-downloaded. A
    ``mode="full"`` run re-fetches everything and is how a USGS re-stage is
    picked up (SCD2 dedupes unchanged sub-tiles away). Note one consequence:
    widening the bbox to a lexicographically smaller tile name needs one full
    run before incremental runs see it.
  - **``rast`` is flagged ``metadata=True``.** Not because it is bookkeeping,
    but to keep it out of the SCD2 content hash: the cheap ``checksum`` column
    (md5 of the tile's raw bytes + georeference) already carries the
    content-change signal, and hashing the multi-hundred-KB raster value on
    every row would be wasteful.
"""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from datadongle.collectors.threedep.client import ThreeDEPClient, tiles_for_bbox
from datadongle.collectors.threedep.spec import ThreeDEPDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, WriteMode
from datadongle.raster.ingest import RasterTile, iter_tiles

logger = logging.getLogger(__name__)

# Sub-tile edge for the big 1-degree source rasters. 512 keeps a 1-degree
# 1/3 arc-second tile (~10812 px) to ~480 rows rather than ~1800 at 256,
# while staying small enough for fast point sampling.
DEFAULT_TILE_SIZE = 512
# Sub-tiles per staged batch. Low by default — each is a large field.
DEFAULT_BATCH_SIZE = 16

# The raster tile table, in DDL order. The engine adds ingested_at and the
# SCD2 columns itself.
_COLUMNS: list[Column] = [
    Column("tile_id", ColumnType.TEXT, nullable=False),
    Column("source_tile", ColumnType.TEXT, nullable=False),
    Column("rast", ColumnType.RASTER, nullable=False, metadata=True),
    Column("checksum", ColumnType.TEXT, nullable=False),
    Column("srid", ColumnType.INTEGER),
    # Sub-tile extent in the raster's CRS, handy for debugging / sanity joins.
    Column("min_x", ColumnType.DOUBLE),
    Column("min_y", ColumnType.DOUBLE),
    Column("max_x", ColumnType.DOUBLE),
    Column("max_y", ColumnType.DOUBLE),
]

CURSOR_COLUMN = "source_tile"


class ThreeDEPReader:
    """Adapts USGS 3DEP seamless DEM tiles to the shared collection driver."""

    source = "3dep"

    def __init__(
        self,
        client: ThreeDEPClient | None = None,
        tile_size: int = DEFAULT_TILE_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._client = client
        self.tile_size = tile_size
        self.batch_size = batch_size

    @property
    def client(self) -> ThreeDEPClient:
        if self._client is None:
            self._client = ThreeDEPClient()
        return self._client

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: ThreeDEPDatasetSpec) -> str:
        if spec.tiles and len(spec.tiles) == 1:
            return f"{spec.name}/{spec.tiles[0]}"
        return spec.name

    def target(self, spec: ThreeDEPDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: ThreeDEPDatasetSpec) -> TableSchema:
        return TableSchema(columns=list(_COLUMNS))

    def write_mode(self, spec: ThreeDEPDatasetSpec, *, mode: str = "full") -> WriteMode:
        # SCD2 keyed on the sub-tile. Never invalidate_missing: the bbox clips
        # the pull, so a sub-tile absent from a run is out of scope, not gone.
        return SCD2(entity_key=list(spec.entity_key))

    def cursor_spec(self, spec: ThreeDEPDatasetSpec) -> CursorSpec | None:
        return CursorSpec(column=CURSOR_COLUMN)

    def read(
        self, spec: ThreeDEPDatasetSpec, *, since: Cursor | None
    ) -> Iterator[list[dict[str, Any]]]:
        names = self.tiles(spec)
        if since is not None:
            names = [n for n in names if n > since.value]
        for name in names:
            if not self.client.tile_exists(name, spec.product):
                logger.warning("Tile %s not available at source; skipping", name)
                continue
            yield from self._read_tile(spec, name)

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        values = [r["source_tile"] for r in batch if r.get("source_tile")]
        return Cursor(value=max(values)) if values else None

    # ------------------------------------------------------------------
    # Tile enumeration (also used by the family driver)
    # ------------------------------------------------------------------

    def tiles(self, spec: ThreeDEPDatasetSpec) -> list[str]:
        """The in-scope 1-degree tile names for ``spec``, sorted ascending.

        Sorted order is what makes ``source_tile`` a valid cursor: the max name
        in the target is always the frontier of a completed prefix.
        """
        return sorted(spec.tiles) if spec.tiles is not None else tiles_for_bbox(spec.bbox)

    def available(self, spec: ThreeDEPDatasetSpec) -> bool:
        """True iff every tile the (narrowed) spec needs is staged at the source."""
        return all(self.client.tile_exists(n, spec.product) for n in self.tiles(spec))

    # ------------------------------------------------------------------
    # Per-tile read
    # ------------------------------------------------------------------

    def _read_tile(
        self, spec: ThreeDEPDatasetSpec, name: str
    ) -> Iterator[list[dict[str, Any]]]:
        """Download one 1-degree tile to a temp file and yield its sub-tile rows,
        clipped to the spec's bbox; the file is deleted when the tile is done."""
        tmp = tempfile.NamedTemporaryFile(suffix=".tif", prefix=f"3dep_{name}_", delete=False)
        tmp.close()
        path = Path(tmp.name)
        try:
            self.client.download_tile(name, spec.product, path)
            batch: list[dict[str, Any]] = []
            b = spec.bbox
            for tile in iter_tiles(
                str(path),
                source_id=name,
                tile_size=self.tile_size,
                bounds=(b.west, b.south, b.east, b.north),
            ):
                batch.append(self._to_row(tile, name))
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
        finally:
            path.unlink(missing_ok=True)

    @staticmethod
    def _to_row(tile: RasterTile, source_tile: str) -> dict[str, Any]:
        return {
            "tile_id": tile.tile_id,
            "source_tile": source_tile,
            "rast": tile.rast_hexwkb,
            "checksum": tile.checksum,
            "srid": tile.srid,
            "min_x": tile.min_x,
            "min_y": tile.min_y,
            "max_x": tile.max_x,
            "max_y": tile.max_y,
        }
