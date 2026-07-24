"""Fakes and test-data helpers for the TIGER collector tests.

The boundary that gets faked is HTTP: ``FakeTigerClient`` duck-types
``TigerClient``, serving canned directory listings (``get_text``) and returning
locally-generated shapefile zips from ``download_to_tempfile``. The real
``TigerMetadata`` runs on top of it (URL construction is pure; ``list_files``
parses the fake listings), the real ``parse_shapefile`` parses the generated
zips, and ingestion runs against a real hermetic ``IcebergEngine`` — so the
parser, geometry path, and SCD2 semantics are all part of the behavior under
test.
"""

from __future__ import annotations

import shutil
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import cast

from datadongle.collectors.tiger.client import TigerClient
from datadongle.collectors.tiger.metadata import TigerMetadata

# The canonical TIGER base URL, so tests build the same URLs the reader does.
BASE = TigerMetadata.BASE


def tiger_url(vintage: int, layer: str, fips: str | None) -> str:
    """The TIGER/Line download URL for a unit (2-digit state or 5-digit county)."""
    layer_u = layer.upper()
    f = fips or "us"
    return f"{BASE}/TIGER{vintage}/{layer_u}/tl_{vintage}_{f}_{layer.lower()}.zip"


def tiger_dir_url(vintage: int, layer: str) -> str:
    return f"{BASE}/TIGER{vintage}/{layer.upper()}/"


def _directory_html(filenames: list[str]) -> str:
    """Apache-style directory listing that TigerMetadata._parse_directory_links reads."""
    rows = [
        f'<a href="{name}">{name}</a></td>'
        f'<td align="right">2024-01-01 00:00</td>'
        f'<td align="right">1.0M</td>'
        for name in filenames
    ]
    return "<html><body>" + "\n".join(rows) + "</body></html>"


def write_shapefile_zip(
    directory: Path,
    stem: str,
    features: list[dict],
    geom_type: str = "Polygon",
    prop_schema: dict | None = None,
    crs: str = "EPSG:4326",
) -> Path:
    """Write a shapefile from ``features`` and zip its sidecars. Returns the zip path.

    ``features`` is a list of ``{"geometry": shapely-or-None, <prop>: <val>, ...}``.
    ``prop_schema`` is the fiona property schema (e.g. ``{"GEOID": "str:11"}``);
    inferred as all-``str`` from the first feature's non-geometry keys if omitted.
    """
    import fiona
    from shapely.geometry import mapping

    if prop_schema is None:
        keys = [k for k in features[0] if k != "geometry"]
        prop_schema = {k: "str:80" for k in keys}

    shp = directory / f"{stem}.shp"
    fiona_schema = {"geometry": geom_type, "properties": prop_schema}
    with fiona.open(str(shp), "w", driver="ESRI Shapefile", schema=fiona_schema, crs=crs) as dst:
        for feat in features:
            geom = feat.get("geometry")
            props = {k: v for k, v in feat.items() if k != "geometry"}
            dst.write({"geometry": mapping(geom) if geom is not None else None, "properties": props})

    zip_path = directory / f"{stem}.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        for sidecar in directory.glob(f"{stem}.*"):
            if sidecar.suffix != ".zip":
                z.write(sidecar, sidecar.name)
    return zip_path


class FakeTigerClient:
    """Duck-types TigerClient against an in-memory URL map. No HTTP."""

    def __init__(self, files: dict[str, Path], listings: dict[str, str]) -> None:
        self._files = files          # download URL -> local zip path
        self._listings = listings    # directory URL -> HTML
        self.fail_urls: set[str] = set()
        self.download_calls: list[str] = []

    def get_text(self, url: str) -> str:
        if url not in self._listings:
            raise KeyError(f"No fake listing for {url}")
        return self._listings[url]

    def download_to_tempfile(self, url: str, suffix: str = ".zip") -> Path:
        self.download_calls.append(url)
        if url in self.fail_urls:
            raise ConnectionError(f"Injected download failure for {url}")
        if url not in self._files:
            raise KeyError(f"No fake file for {url}")
        # Copy to a fresh temp file — the reader deletes what it downloads.
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix="tiger_fake_", delete=False)
        tmp.close()
        shutil.copy(self._files[url], tmp.name)
        return Path(tmp.name)


def as_client_factory(client: FakeTigerClient) -> Callable[[], TigerClient]:
    """Wrap a fake as a ``TigerReader`` client_factory.

    ``FakeTigerClient`` duck-types ``TigerClient`` rather than subclassing it,
    so the cast is where that intent is stated for the type checker.
    """
    return lambda: cast(TigerClient, client)
