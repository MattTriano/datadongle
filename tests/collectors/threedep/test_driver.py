"""End-to-end behaviors of the 3DEP family driver.

Each test names one behavior the tooling must keep exhibiting, asserting only
on the target table and the driver summary. Tiles are small synthetic GeoTIFFs
served through a fake client, so the real rasterio tiling, hex-WKB encoding,
and SCD2 merge paths run. The Iceberg arm is hermetic (tmp warehouse) so it
runs everywhere; the Postgres arm (which also covers the raster COPY and
ST_Value sampling) runs only when DWH_TEST_PG* is configured.
"""

from __future__ import annotations

import os
import uuid

import pytest

from datadongle.collectors.threedep.driver import run_threedep_collection
from datadongle.collectors.threedep.reader import ThreeDEPReader
from datadongle.core.engine import TableRef
from datadongle.engines.iceberg import IcebergEngine

from ..common import NoopTracker
from .helpers import (
    SINGLE_TILE_BBOX,
    SUB_TILE,
    TWO_TILE_BBOX,
    FakeThreeDEPClient,
    make_elevation_spec,
    seeded_source,
)

# --------------------------------------------------------------- engine fixture


@pytest.fixture(params=[
    "iceberg",
    pytest.param("postgres", marks=pytest.mark.postgres),
])
def threedep_engine(request, tmp_path):
    if request.param == "iceberg":
        yield IcebergEngine(str(tmp_path / "warehouse"))
        return

    if not os.environ.get("DWH_TEST_PGDATABASE"):
        pytest.skip("no test Postgres configured (set DWH_TEST_PG*)")
    from datadongle.db.core import DatabaseCredentials
    from datadongle.engines.postgres import PostgresEngine

    creds = DatabaseCredentials(
        host=os.environ.get("DWH_TEST_PGHOST", "localhost"),
        port=int(os.environ.get("DWH_TEST_PGPORT", "5432")),
        database=os.environ["DWH_TEST_PGDATABASE"],
        username=os.environ.get("DWH_TEST_PGUSER", ""),
        password=os.environ.get("DWH_TEST_PGPASSWORD", ""),
    )
    eng = PostgresEngine(creds)
    schema = f"threedep_test_{uuid.uuid4().hex[:8]}"
    try:
        eng.execute(f"create schema {schema}")
    except Exception as e:  # pragma: no cover - depends on external DB
        pytest.skip(f"no usable test Postgres: {e}")
    eng._test_schema = schema
    try:
        yield eng
    finally:
        try:
            eng.execute(f"drop schema {schema} cascade")
        finally:
            eng.close()


def _schema_name(engine) -> str:
    return getattr(engine, "_test_schema", "raw_data")


def _current(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_current(target)
    return engine.query(f'select * from {target} where "valid_to" is null')


def _history(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_history(target)
    return engine.query(f"select * from {target}")


def _reader(source) -> ThreeDEPReader:
    return ThreeDEPReader(client=FakeThreeDEPClient(source), tile_size=SUB_TILE)


def _spec_and_target(engine, bbox=SINGLE_TILE_BBOX):
    spec = make_elevation_spec(_schema_name(engine), bbox=bbox)
    return spec, TableRef(spec.target_table, spec.target_schema)


# --------------------------------------------------------------- the behaviors


def test_first_collect_lands_clipped_sub_tiles(threedep_engine):
    spec, target = _spec_and_target(threedep_engine)
    reader = _reader(seeded_source(["n42w088"]))

    summary = run_threedep_collection(reader, spec, threedep_engine, mode="full")

    assert summary["errors"] == []
    assert summary["tiles_collected"] == 1
    assert summary["total_rows_merged"] > 0

    cur = _current(threedep_engine, target)
    assert len(cur) == summary["total_rows_merged"]
    assert cur["rast"].notnull().all()
    # Clip: every stored sub-tile intersects the bbox.
    b = SINGLE_TILE_BBOX
    assert not (
        (cur["max_x"] < b.west)
        | (cur["min_x"] > b.east)
        | (cur["max_y"] < b.south)
        | (cur["min_y"] > b.north)
    ).any()


def test_incremental_skips_present_tiles_without_download(threedep_engine):
    spec, _target = _spec_and_target(threedep_engine)
    reader = _reader(seeded_source(["n42w088"]))
    run_threedep_collection(reader, spec, threedep_engine, mode="full")

    reader.client.downloaded.clear()
    summary = run_threedep_collection(reader, spec, threedep_engine, mode="incremental")

    assert summary["mode"] == "incremental"
    assert summary["tiles_skipped_present"] == 1
    assert summary["tiles_collected"] == 0
    assert reader.client.downloaded == []


def test_incremental_collects_only_tiles_after_the_frontier(threedep_engine):
    # Collect the single-tile bbox, then widen it to a second, larger-named
    # tile: an incremental run fetches only the new tile.
    source = seeded_source(["n42w088", "n43w088"])
    narrow_spec, _ = _spec_and_target(threedep_engine, bbox=SINGLE_TILE_BBOX)
    run_threedep_collection(_reader(source), narrow_spec, threedep_engine, mode="full")

    wide_spec, target = _spec_and_target(threedep_engine, bbox=TWO_TILE_BBOX)
    reader = _reader(source)
    summary = run_threedep_collection(reader, wide_spec, threedep_engine, mode="incremental")

    assert summary["tiles_skipped_present"] == 1
    assert summary["tiles_collected"] == 1
    assert reader.client.downloaded == ["n43w088"]
    assert set(_current(threedep_engine, target)["source_tile"]) == {"n42w088", "n43w088"}


def test_full_recollect_creates_no_duplicates(threedep_engine):
    spec, target = _spec_and_target(threedep_engine)
    source = seeded_source(["n42w088"])
    run_threedep_collection(_reader(source), spec, threedep_engine, mode="full")
    n = len(_history(threedep_engine, target))

    summary = run_threedep_collection(_reader(source), spec, threedep_engine, mode="full")

    assert summary["total_rows_merged"] == 0
    assert len(_history(threedep_engine, target)) == n


def test_restaged_tile_versions_then_resettles(threedep_engine):
    spec, target = _spec_and_target(threedep_engine)
    source = seeded_source(["n42w088"])
    run_threedep_collection(_reader(source), spec, threedep_engine, mode="full")

    current_before = len(_current(threedep_engine, target))
    total_before = len(_history(threedep_engine, target))

    source.set_tile("n42w088", base_value=5000.0)  # USGS re-stages with new values
    summary = run_threedep_collection(_reader(source), spec, threedep_engine, mode="full")

    assert summary["total_rows_merged"] == current_before  # every sub-tile changed
    assert len(_current(threedep_engine, target)) == current_before  # one current per sub-tile
    assert len(_history(threedep_engine, target)) == total_before + current_before

    resettled = run_threedep_collection(_reader(source), spec, threedep_engine, mode="full")
    assert resettled["total_rows_merged"] == 0


def test_missing_at_source_tile_is_skipped_not_fatal(threedep_engine):
    spec, target = _spec_and_target(threedep_engine, bbox=TWO_TILE_BBOX)
    reader = _reader(seeded_source(["n42w088"]))  # n43w088 not staged

    summary = run_threedep_collection(reader, spec, threedep_engine, mode="full")

    assert summary["tiles_collected"] == 1
    assert summary["tiles_missing_at_source"] == 1
    assert summary["errors"] == []
    assert len(_current(threedep_engine, target)) > 0


def test_full_run_isolates_a_failing_tile(threedep_engine):
    spec, target = _spec_and_target(threedep_engine, bbox=TWO_TILE_BBOX)
    reader = _reader(seeded_source(["n42w088", "n43w088"]))
    reader.client.fail_tiles.add("n42w088")

    summary = run_threedep_collection(reader, spec, threedep_engine, mode="full")

    assert summary["tiles_collected"] == 1
    assert len(summary["errors"]) == 1
    assert summary["errors"][0]["tile"] == "n42w088"
    # n43w088 still landed.
    assert set(_current(threedep_engine, target)["source_tile"]) == {"n43w088"}


def test_incremental_run_stops_at_a_failing_tile_and_resumes(threedep_engine):
    spec, target = _spec_and_target(threedep_engine, bbox=TWO_TILE_BBOX)
    source = seeded_source(["n42w088", "n43w088"])
    reader = _reader(source)
    reader.client.fail_tiles.add("n42w088")

    summary = run_threedep_collection(reader, spec, threedep_engine, mode="incremental")

    # Fail-stop: n43w088 must NOT land — merging it would advance the
    # high-water mark past the failed n42w088, hiding it from future runs.
    assert summary["tiles_collected"] == 0
    assert len(summary["errors"]) == 1
    assert len(_current(threedep_engine, target)) == 0

    reader.client.fail_tiles.clear()
    healed = run_threedep_collection(reader, spec, threedep_engine, mode="incremental")

    assert healed["errors"] == []
    assert healed["tiles_collected"] == 2
    assert set(_current(threedep_engine, target)["source_tile"]) == {"n42w088", "n43w088"}


def test_tracker_records_a_run_per_tile(threedep_engine):
    spec, _target = _spec_and_target(threedep_engine, bbox=TWO_TILE_BBOX)
    tracker = NoopTracker()

    run_threedep_collection(
        _reader(seeded_source(["n42w088", "n43w088"])),
        spec,
        threedep_engine,
        tracker=tracker,
        mode="full",
    )

    tracked = {dataset_id for dataset_id, _run in tracker.runs}
    assert tracked == {"fake_elevation/n42w088", "fake_elevation/n43w088"}


# ------------------------------------------------- Postgres-only: raster sampling


def test_postgres_st_value_samples_the_landed_raster(threedep_engine):
    if isinstance(threedep_engine, IcebergEngine):
        pytest.skip("PostGIS-only behavior (raster COPY + ST_Value)")
    spec, target = _spec_and_target(threedep_engine)
    run_threedep_collection(
        _reader(seeded_source(["n42w088"])), spec, threedep_engine, mode="full"
    )

    b = SINGLE_TILE_BBOX
    cx, cy = (b.west + b.east) / 2, (b.south + b.north) / 2
    df = threedep_engine.query(
        f"""
        select ST_Value(rast, ST_SetSRID(ST_Point(%(x)s, %(y)s), 4269)) as elev
        from {target}
        where "valid_to" is null
          and ST_Intersects(rast, ST_SetSRID(ST_Point(%(x)s, %(y)s), 4269))
        """,
        {"x": cx, "y": cy},
    )
    assert not df.empty
    assert df.iloc[0]["elev"] is not None
