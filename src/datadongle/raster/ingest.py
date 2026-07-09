# /loci_platform/platform/airflow/dags/loci/raster/ingest.py
"""
Tile a local raster file into ingest-ready sub-tiles.

``iter_tiles`` walks a GDAL-readable raster with rasterio windowed reads —
one sub-tile at a time, so peak memory is one tile, not the whole file. This
is why download-to-file-then-tile is the right shape: rasterio reads windows
from the file on disk; we never hold the full raster in memory.

Each ``RasterTile`` carries the sub-tile as a PostGIS raster hex-WKB string
(PostGIS parses it on COPY exactly as it parses geometry WKT; IcebergEngine
stores the decoded WKB bytes) plus a cheap ``checksum`` (md5 of the tile's raw
bytes + georeference). The checksum is what SCD2 change detection hashes —
hashing the multi-hundred-KB raster value itself on every row would be
wasteful.

The storage half lives in the engines: a reader (see the 3DEP collector)
turns these tiles into rows and the shared load path stages and merges them.

A `raster` table is the natural fit for downstream sampling:

    select n.node_id,
           ST_Value(r.rast, n.geom, resample => 'bilinear') as elevation_m
    from   <raster_table> r
    join   nodes n on ST_Intersects(r.rast, n.geom);

The ST_Intersects is served by the GiST index the engine creates, so each
node resolves to its one tile.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator
from dataclasses import dataclass

import numpy as np
import rasterio
from datadongle.raster.wkb import to_hexwkb
from rasterio.windows import Window

logger = logging.getLogger(__name__)

DEFAULT_TILE_SIZE = 256


@dataclass(frozen=True)
class RasterTile:
    """One tile read from a raster file, ready to become an ingest row."""

    tile_id: str
    rast_hexwkb: str
    checksum: str
    srid: int
    # Tile extent in the raster's CRS, handy for debugging / sanity joins.
    min_x: float
    min_y: float
    max_x: float
    max_y: float


def iter_tiles(
    path: str,
    *,
    source_id: str,
    band: int = 1,
    tile_size: int = DEFAULT_TILE_SIZE,
    bounds: tuple[float, float, float, float] | None = None,
) -> Iterator[RasterTile]:
    """
    Yield non-overlapping tiles covering the raster at `path`.

    Parameters
    ----------
    path : str
        Local path to a GDAL-readable raster (e.g. a downloaded GeoTIFF
        or COG).
    source_id : str
        Stable identifier for this source file/region, used to build
        tile_id. Must be stable across runs so SCD2 recognizes the same
        geographic tile (e.g. the DEM tile name or "<city>").
    band : int
        1-based band index to read. DEMs are single-band; default 1.
    tile_size : int
        Tile edge in pixels. Edge tiles are smaller. 256 keeps each
        tile's index entry tight without too many rows.
    bounds : tuple[float, float, float, float] | None
        Optional (min_x, min_y, max_x, max_y) clip extent, IN THE
        RASTER'S OWN CRS. Tiles whose extent does not intersect it are
        skipped (and their pixels never read). Use this to keep only the
        sub-tiles covering a city, rather than a whole source block. The
        caller is responsible for expressing the extent in the raster's
        CRS — for 3DEP (EPSG:4269) a NAD83 BBox is already correct.

    Yields
    ------
    RasterTile
        One per tile, row-major over the raster (top-left first).
    """
    with rasterio.open(path) as ds:
        srid = _resolve_srid(ds)
        nodata = ds.nodata

        for row_off in range(0, ds.height, tile_size):
            for col_off in range(0, ds.width, tile_size):
                w = min(tile_size, ds.width - col_off)
                h = min(tile_size, ds.height - row_off)
                window = Window(col_off, row_off, w, h)
                transform = ds.window_transform(window)

                # Tile extent first, so a clipped-out tile costs no read.
                left, top = transform * (0, 0)
                right, bottom = transform * (w, h)
                min_x, max_x = min(left, right), max(left, right)
                min_y, max_y = min(top, bottom), max(top, bottom)

                if bounds is not None and not _intersects((min_x, min_y, max_x, max_y), bounds):
                    continue

                pixels = ds.read(band, window=window)

                # affine: a=scale_x, b=skew_x, c=ip_x, d=skew_y, e=scale_y, f=ip_y
                rast_hexwkb = to_hexwkb(
                    pixels,
                    scale_x=transform.a,
                    scale_y=transform.e,
                    ip_x=transform.c,
                    ip_y=transform.f,
                    srid=srid,
                    nodata=nodata,
                    skew_x=transform.b,
                    skew_y=transform.d,
                )

                # Content checksum over the raw tile bytes + the
                # georeference, so a tile that moves or changes values
                # gets a new SCD2 version.
                checksum = _tile_checksum(pixels, transform, srid)

                yield RasterTile(
                    tile_id=f"{source_id}/{row_off}_{col_off}",
                    rast_hexwkb=rast_hexwkb,
                    checksum=checksum,
                    srid=srid,
                    min_x=min_x,
                    min_y=min_y,
                    max_x=max_x,
                    max_y=max_y,
                )


def _intersects(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> bool:
    """True if two (min_x, min_y, max_x, max_y) extents overlap (touching counts)."""
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def _resolve_srid(ds: rasterio.DatasetReader) -> int:
    if ds.crs is None:
        raise ValueError("raster has no CRS; cannot determine srid")
    epsg = ds.crs.to_epsg()
    if epsg is None:
        raise ValueError(f"raster CRS {ds.crs} has no EPSG code; reproject before ingest")
    return int(epsg)


def _tile_checksum(pixels: np.ndarray, transform, srid: int) -> str:
    h = hashlib.md5()
    h.update(np.ascontiguousarray(pixels, dtype="<f4").tobytes())
    h.update(
        repr(
            (transform.a, transform.b, transform.c, transform.d, transform.e, transform.f, srid)
        ).encode()
    )
    return h.hexdigest()
