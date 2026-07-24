"""ThreeDEPMetadata coverage checks against the fake client (no network)."""

from __future__ import annotations

from datadongle.collectors.threedep.client import SUPPORTED_PRODUCTS
from datadongle.collectors.threedep.metadata import ThreeDEPMetadata

from .helpers import TWO_TILE_BBOX, FakeThreeDEPClient, as_client, seeded_source


def _meta(staged=("n42w088",)) -> ThreeDEPMetadata:
    return ThreeDEPMetadata(client=as_client(FakeThreeDEPClient(seeded_source(staged))))


def test_products_lists_the_seamless_layers():
    assert _meta().products() == SUPPORTED_PRODUCTS


def test_tiles_for_bbox_enumerates_needed_tiles():
    assert _meta().tiles_for_bbox(TWO_TILE_BBOX) == ["n42w088", "n43w088"]


def test_coverage_maps_each_tile_to_staged_status():
    cov = _meta(staged=("n42w088",)).coverage(TWO_TILE_BBOX)
    assert cov == {"n42w088": True, "n43w088": False}


def test_missing_lists_unstaged_tiles():
    assert _meta(staged=("n42w088",)).missing(TWO_TILE_BBOX) == ["n43w088"]


def test_describe_summarizes_coverage(capsys):
    summary = _meta(staged=("n42w088",)).describe(TWO_TILE_BBOX)
    assert summary["needed"] == 2
    assert summary["present"] == ["n42w088"]
    assert summary["missing"] == ["n43w088"]
    assert "MISSING" in capsys.readouterr().out
