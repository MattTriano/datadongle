"""End-to-end behaviors of the TIGER family driver.

Each test names one behavior the tooling must keep exhibiting, asserting only on
the target table and the driver summary. Files are tiny real shapefile zips
generated with fiona, served through a fake client, so the actual parser and
geometry path run. The Iceberg arm is hermetic (tmp warehouse) so it runs
everywhere; the Postgres arm runs only when DWH_TEST_PG* is configured.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from shapely.geometry import LineString, Polygon

from datadongle.collectors.tiger.driver import run_tiger_collection
from datadongle.collectors.tiger.reader import TigerReader
from datadongle.collectors.tiger.spec import TigerDatasetSpec
from datadongle.core.engine import TableRef
from datadongle.engines.iceberg import IcebergEngine

from ..common import NoopTracker
from .helpers import (
    FakeTigerClient,
    _directory_html,
    as_client_factory,
    tiger_dir_url,
    tiger_url,
    write_shapefile_zip,
)

# --------------------------------------------------------------- engine fixture


@pytest.fixture(
    params=[
        "iceberg",
        pytest.param("postgres", marks=pytest.mark.postgres),
    ]
)
def tiger_engine(request, tmp_path):
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
    schema = f"tiger_test_{uuid.uuid4().hex[:8]}"
    try:
        eng.execute(f"create schema {schema}")
    except Exception as e:  # pragma: no cover - depends on external DB
        pytest.skip(f"no usable test Postgres: {e}")
    # Stashed on the engine so tests can find it via _schema_name below.
    eng._test_schema = schema  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.execute(f"drop schema {schema} cascade")
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


def _poly(i: int) -> Polygon:
    return Polygon([(i, i), (i, i + 1), (i + 1, i + 1), (i + 1, i), (i, i)])


# --------------------------------------------------------------- scenario builders


def _tract_files(tmp_path, data: dict) -> dict:
    """data: {(vintage, state): [feature dicts]} -> {download_url: zip_path}."""
    files = {}
    for (vintage, state), feats in data.items():
        zp = write_shapefile_zip(
            tmp_path, f"tl_{vintage}_{state}_tract", feats, geom_type="Polygon"
        )
        files[tiger_url(vintage, "TRACT", state)] = zp
    return files


def _tract_feature(state: str, n: int, **extra) -> dict:
    return {"geometry": _poly(n), "GEOID": f"{state}031{n:06d}", "NAME": f"Tract {n}", **extra}


def _tract_spec(schema, **overrides) -> TigerDatasetSpec:
    kwargs: dict[str, Any] = dict(
        name="census_tracts",
        layer="TRACT",
        vintages=[2023, 2024],
        target_table="census_tracts",
        target_schema=schema,
        state_fips=["17", "18"],
    )
    kwargs.update(overrides)
    return TigerDatasetSpec(**kwargs)


def _reader(files, listings=None, fail_urls=None):
    client = FakeTigerClient(files, listings or {})
    if fail_urls:
        client.fail_urls |= set(fail_urls)
    return TigerReader(client_factory=as_client_factory(client))


def _default_tract_data():
    return {
        (v, s): [_tract_feature(s, 0), _tract_feature(s, 1)]
        for v in (2023, 2024)
        for s in ("17", "18")
    }


# --------------------------------------------------------------- the behaviors


def test_first_collect_lands_every_file_with_geometry(tiger_engine, tmp_path):
    schema = _schema_name(tiger_engine)
    files = _tract_files(tmp_path, _default_tract_data())
    reader = _reader(files)
    spec = _tract_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = run_tiger_collection(reader, spec, tiger_engine)

    assert summary["errors"] == []
    assert summary["vintages_processed"] == 2
    assert summary["files_processed"] == 4  # 2 vintages * 2 states
    cur = _current(tiger_engine, target)
    assert len(cur) == 8  # * 2 tracts
    assert "geom" in cur.columns
    assert cur["geom"].notnull().all()


def test_union_table_holds_a_vintages_extra_column(tiger_engine, tmp_path):
    schema = _schema_name(tiger_engine)
    data = {
        (2023, "17"): [_tract_feature("17", 0)],
        (2024, "17"): [_tract_feature("17", 0, POP="500")],  # extra column in 2024
    }
    reader = _reader(_tract_files(tmp_path, data))
    spec = _tract_spec(schema, state_fips=["17"])
    target = TableRef(spec.target_table, schema)

    summary = run_tiger_collection(reader, spec, tiger_engine)

    assert summary["errors"] == []
    cur = _current(tiger_engine, target)
    assert "pop" in cur.columns
    assert cur[cur["vintage"] == 2024]["pop"].notnull().all()
    assert cur[cur["vintage"] == 2023]["pop"].isnull().all()


def test_repeat_collect_is_idempotent(tiger_engine, tmp_path):
    schema = _schema_name(tiger_engine)
    files = _tract_files(tmp_path, _default_tract_data())
    spec = _tract_spec(schema)
    target = TableRef(spec.target_table, schema)
    run_tiger_collection(_reader(files), spec, tiger_engine)

    summary = run_tiger_collection(_reader(files), spec, tiger_engine)

    assert summary["total_rows_merged"] == 0
    assert len(_current(tiger_engine, target)) == 8
    assert len(_history(tiger_engine, target)) == 8


def test_changed_feature_versions_only_that_row(tiger_engine, tmp_path):
    schema = _schema_name(tiger_engine)
    data = _default_tract_data()
    spec = _tract_spec(schema)
    target = TableRef(spec.target_table, schema)
    run_tiger_collection(_reader(_tract_files(tmp_path, data)), spec, tiger_engine)

    # Change one tract's NAME (a non-key content column) and re-collect.
    data[(2024, "17")][0]["NAME"] = "Renamed Tract"
    summary = run_tiger_collection(_reader(_tract_files(tmp_path, data)), spec, tiger_engine)

    assert summary["total_rows_merged"] == 1
    cur = _current(tiger_engine, target)
    assert len(cur) == 8
    changed = cur[cur["geoid"] == "17031000000"]
    assert changed[changed["vintage"] == 2024]["name"].iloc[0] == "Renamed Tract"
    assert len(_history(tiger_engine, target)) == 9


def test_national_scope_collects_one_file_per_vintage(tiger_engine, tmp_path):
    schema = _schema_name(tiger_engine)
    files = {}
    for v in (2023, 2024):
        zp = write_shapefile_zip(
            tmp_path,
            f"tl_{v}_us_primaryroads",
            [{"geometry": LineString([(0, 0), (1, 1)]), "LINEARID": f"L{v}", "FULLNAME": "I-90"}],
            geom_type="LineString",
        )
        files[tiger_url(v, "PRIMARYROADS", None)] = zp
    reader = _reader(files)
    spec = _tract_spec(
        schema, name="primary_roads", layer="PRIMARYROADS", target_table="primary_roads"
    )
    target = TableRef(spec.target_table, schema)

    summary = run_tiger_collection(reader, spec, tiger_engine)

    assert summary["files_processed"] == 2  # one per vintage
    assert len(_current(tiger_engine, target)) == 2


def test_county_scope_enumerates_and_injects_fips(tiger_engine, tmp_path):
    schema = _schema_name(tiger_engine)
    files = {}
    for county in ("17031", "17043"):
        zp = write_shapefile_zip(
            tmp_path,
            f"tl_2024_{county}_roads",
            [
                {
                    "geometry": LineString([(0, 0), (1, 1)]),
                    "LINEARID": f"L{county}",
                    "FULLNAME": "Main",
                }
            ],
            geom_type="LineString",
        )
        files[tiger_url(2024, "ROADS", county)] = zp
    listings = {
        tiger_dir_url(2024, "ROADS"): _directory_html(
            ["tl_2024_17031_roads.zip", "tl_2024_17043_roads.zip", "tl_2024_06037_roads.zip"]
        )
    }
    reader = _reader(files, listings)
    spec = _tract_spec(
        schema,
        name="roads",
        layer="ROADS",
        target_table="roads",
        vintages=[2024],
        state_fips=["17"],
    )
    target = TableRef(spec.target_table, schema)

    summary = run_tiger_collection(reader, spec, tiger_engine)

    assert summary["files_processed"] == 2  # 06037 filtered out
    cur = _current(tiger_engine, target)
    assert len(cur) == 2
    assert set(cur["statefp"]) == {"17"}
    assert set(cur["countyfp"]) == {"031", "043"}


def test_failing_file_does_not_block_others(tiger_engine, tmp_path):
    schema = _schema_name(tiger_engine)
    files = _tract_files(tmp_path, _default_tract_data())
    # Fail both files for state 18. (State 17 is states[0], the schema-discovery
    # sample, so it must stay reachable for the table to be created at all.)
    fail = [tiger_url(v, "TRACT", "18") for v in (2023, 2024)]
    reader = _reader(files, fail_urls=fail)
    spec = _tract_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = run_tiger_collection(reader, spec, tiger_engine)

    assert summary["files_processed"] == 2  # state 17, both vintages
    assert len(summary["errors"]) == 2
    assert {e["state_fips"] for e in summary["errors"]} == {"18"}
    cur = _current(tiger_engine, target)
    assert set(cur["geoid"].str[:2]) == {"17"}


def test_no_id_layer_collects_append_only(tiger_engine, tmp_path):
    schema = _schema_name(tiger_engine)
    zp = write_shapefile_zip(
        tmp_path,
        "tl_2024_us_coastline",
        [{"geometry": LineString([(0, 0), (1, 1)]), "NAME": "Atlantic", "MTFCC": "C10"}],
        geom_type="LineString",
    )
    files = {tiger_url(2024, "COASTLINE", None): zp}
    spec = _tract_spec(
        schema, name="coastline", layer="COASTLINE", target_table="coastline", vintages=[2024]
    )
    target = TableRef(spec.target_table, schema)

    run_tiger_collection(_reader(files), spec, tiger_engine)
    run_tiger_collection(_reader(files), spec, tiger_engine)

    # Append-only: a second run duplicates rows (no SCD2 dedup, as documented).
    assert len(_history(tiger_engine, target)) == 2


def test_tracker_records_a_run_per_file(tiger_engine, tmp_path):
    schema = _schema_name(tiger_engine)
    files = _tract_files(tmp_path, _default_tract_data())
    tracker = NoopTracker()

    run_tiger_collection(_reader(files), _tract_spec(schema), tiger_engine, tracker=tracker)

    tracked = {dataset_id for dataset_id, _run in tracker.runs}
    assert tracked == {
        "census_tracts/2023/17",
        "census_tracts/2023/18",
        "census_tracts/2024/17",
        "census_tracts/2024/18",
    }
