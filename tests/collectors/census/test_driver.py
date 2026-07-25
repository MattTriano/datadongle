"""End-to-end behaviors of the Census family driver.

Each test names one behavior the tooling must keep exhibiting, asserting only on
the target table and the driver summary. The Iceberg arm is hermetic (tmp
warehouse) so it runs everywhere; the Postgres arm runs only when DWH_TEST_PG*
is configured, and both must agree.
"""

from __future__ import annotations

import os
import uuid
from typing import cast

import pytest

from datadongle.collectors.census.client import CensusClient
from datadongle.collectors.census.driver import run_census_collection
from datadongle.collectors.census.reader import CensusReader
from datadongle.core.engine import TableRef
from datadongle.engines.iceberg import IcebergEngine

from ..common import NoopTracker
from .helpers import DATASET, FakeCensusClient, make_spec, seeded_source

# --------------------------------------------------------------- engine fixture


@pytest.fixture(
    params=[
        "iceberg",
        pytest.param("postgres", marks=pytest.mark.postgres),
    ]
)
def census_engine(request, tmp_path):
    """An engine per param: hermetic Iceberg always, Postgres when configured."""
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
    schema = f"census_test_{uuid.uuid4().hex[:8]}"
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


def _run(engine, spec, source, tracker=None, fail_state=None):
    def factory() -> CensusClient:
        client = FakeCensusClient(source)
        if fail_state is not None:
            client.fail_states.add(fail_state)
        # FakeCensusClient duck-types CensusClient rather than subclassing it.
        return cast(CensusClient, client)

    reader = CensusReader(client_factory=factory)
    return run_census_collection(reader, spec, engine, tracker=tracker)


def _current(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_current(target)
    return engine.query(f'select * from {target} where "valid_to" is null')


def _history(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_history(target)
    return engine.query(f"select * from {target}")


# --------------------------------------------------------------- the behaviors


def test_first_collect_lands_every_vintage_and_state(census_engine):
    schema = _schema_name(census_engine)
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = _run(census_engine, spec, seeded_source())

    assert summary["errors"] == []
    assert summary["vintages_processed"] == 2
    assert summary["states_processed"] == 4  # 2 vintages * 2 states
    # 2 vintages * 2 states * 3 tracts
    assert len(_current(census_engine, target)) == 12
    assert summary["total_rows_merged"] == 12


def test_union_table_holds_a_vintages_extra_variable(census_engine):
    schema = _schema_name(census_engine)
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = _run(census_engine, spec, seeded_source())

    assert summary["errors"] == []
    cur = _current(census_engine, target)
    assert "B24010_002E" in cur.columns
    # 2022 rows carry the second variable; 2021 rows have it as NULL (padding).
    assert cur[cur["vintage"] == 2022]["B24010_002E"].notnull().all()
    assert cur[cur["vintage"] == 2021]["B24010_002E"].isnull().all()


def test_repeat_collect_is_idempotent(census_engine):
    schema = _schema_name(census_engine)
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)
    _run(census_engine, spec, seeded_source())

    summary = _run(census_engine, spec, seeded_source())

    # Re-collecting unchanged vintages merges nothing and creates no duplicates.
    assert summary["total_rows_merged"] == 0
    assert len(_current(census_engine, target)) == 12
    assert len(_history(census_engine, target)) == 12


def test_changed_estimate_versions_only_changed_row(census_engine):
    schema = _schema_name(census_engine)
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)
    source = seeded_source()
    _run(census_engine, spec, source)

    # Bump one tract's estimate in the 2022/state-17 pull.
    source.rows[(DATASET, 2022, "17")][0]["B24010_001E"] = "999"

    summary = _run(census_engine, spec, source)
    assert summary["total_rows_merged"] == 1

    cur = _current(census_engine, target)
    assert len(cur) == 12  # still 12 current entities
    changed = cur[(cur["vintage"] == 2022) & (cur["state"] == "17") & (cur["tract"] == "000100")]
    assert changed["B24010_001E"].iloc[0] == 999
    # The old version is retained in history.
    assert len(_history(census_engine, target)) == 13


def test_failing_state_does_not_block_others(census_engine):
    schema = _schema_name(census_engine)
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = _run(census_engine, spec, seeded_source(), fail_state="17")

    # State 17 fails in both vintages; state 18 still lands both.
    assert summary["states_processed"] == 2
    assert len(summary["errors"]) == 2
    assert {e["state_fips"] for e in summary["errors"]} == {"17"}
    cur = _current(census_engine, target)
    assert len(cur) == 6  # 2 vintages * 1 state * 3 tracts
    assert set(cur["state"]) == {"18"}


def test_tracker_records_a_run_per_vintage_state(census_engine):
    schema = _schema_name(census_engine)
    spec = make_spec(schema)
    tracker = NoopTracker()

    _run(census_engine, spec, seeded_source(), tracker=tracker)

    tracked = {dataset_id for dataset_id, _run in tracker.runs}
    assert tracked == {
        "fake_occupation_by_sex_tract/2021/17",
        "fake_occupation_by_sex_tract/2021/18",
        "fake_occupation_by_sex_tract/2022/17",
        "fake_occupation_by_sex_tract/2022/18",
    }
