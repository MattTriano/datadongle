"""End-to-end behaviors of the DKAN family driver.

Each test names one behavior the tooling must keep exhibiting, asserting only
on the target table and the driver summary. The Iceberg arm is hermetic (tmp
warehouse) so it runs everywhere; the Postgres arm runs only when DWH_TEST_PG*
is configured, and both must agree.
"""

from __future__ import annotations

import dataclasses
import os
import uuid

import pytest

from datadongle.collectors.dkan.driver import run_dkan_collection
from datadongle.collectors.dkan.reader import DKANReader
from datadongle.core.engine import TableRef
from datadongle.engines.iceberg import IcebergEngine

from ..common import NoopTracker
from .helpers import (
    HOSPITAL_ID,
    OP_2023_ID,
    OP_2024_ID,
    FakeDKANClient,
    make_hospital_rows,
    make_hospital_spec,
    make_payments_spec,
    seeded_op_source,
    seeded_pdc_source,
)

# --------------------------------------------------------------- engine fixture


@pytest.fixture(params=[
    "iceberg",
    pytest.param("postgres", marks=pytest.mark.postgres),
])
def dkan_engine(request, tmp_path):
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
    schema = f"dkan_test_{uuid.uuid4().hex[:8]}"
    try:
        eng.execute(f"create schema {schema}")
    except Exception as e:  # pragma: no cover - depends on external DB
        pytest.skip(f"no usable test Postgres: {e}")
    eng._test_schema = schema
    try:
        yield eng
    finally:
        eng.execute(f"drop schema {schema} cascade")
        eng.close()


def _schema_name(engine) -> str:
    return getattr(engine, "_test_schema", "raw_data")


def _run(engine, spec, source, tracker=None):
    reader = DKANReader(client_factory=lambda base_url: FakeDKANClient(base_url, source))
    return run_dkan_collection(reader, spec, engine, tracker=tracker)


def _current(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_current(target)
    return engine.query(f'select * from {target} where "valid_to" is null')


def _history(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_history(target)
    return engine.query(f"select * from {target}")


# --------------------------------------------------------------- the behaviors


def test_first_collect_ingests_with_normalization_and_provenance(dkan_engine):
    schema = _schema_name(dkan_engine)
    spec = make_hospital_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = _run(dkan_engine, spec, seeded_pdc_source())

    assert summary["errors"] == []
    assert summary["datasets_processed"] == 1
    cur = _current(dkan_engine, target)
    assert len(cur) == 7
    assert (cur["_source_dataset"] == HOSPITAL_ID).all()
    assert (cur["_source_modified"] == "2026-04-28").all()
    assert "countyparish" in cur.columns  # slash dropped, not underscored


def test_repeat_collect_is_idempotent(dkan_engine):
    schema = _schema_name(dkan_engine)
    spec = make_hospital_spec(schema)
    target = TableRef(spec.target_table, schema)
    _run(dkan_engine, spec, seeded_pdc_source())

    summary = _run(dkan_engine, spec, seeded_pdc_source())

    # Re-collecting unchanged data merges nothing and creates no duplicates.
    assert summary["total_rows_merged"] == 0
    assert len(_current(dkan_engine, target)) == 7
    assert len(_history(dkan_engine, target)) == 7


def test_changed_republication_versions_only_changed_row(dkan_engine):
    schema = _schema_name(dkan_engine)
    spec = make_hospital_spec(schema)
    target = TableRef(spec.target_table, schema)
    source = seeded_pdc_source()
    _run(dkan_engine, spec, source)

    changed = make_hospital_rows()
    changed[0]["Hospital Rating"] = "5"
    source.set_dataset(HOSPITAL_ID, "Hospital General Information", changed, "2026-07-01")

    summary = _run(dkan_engine, spec, source)
    assert summary["total_rows_merged"] == 1

    cur = _current(dkan_engine, target)
    assert len(cur) == 7  # still 7 current entities
    changed_row = cur[cur["facility_id"] == "000000"]
    assert changed_row["hospital_rating"].iloc[0] == "5"
    # The old version is retained in history (closed out).
    assert len(_history(dkan_engine, target)) == 8


def test_invalidate_missing_closes_delisted_entities(dkan_engine):
    schema = _schema_name(dkan_engine)
    spec = make_hospital_spec(schema, invalidate_missing=True)
    target = TableRef(spec.target_table, schema)
    source = seeded_pdc_source()
    _run(dkan_engine, spec, source)

    # Drop the first facility from the re-publication.
    source.set_dataset(
        HOSPITAL_ID, "Hospital General Information", make_hospital_rows()[1:], "2026-07-01"
    )

    summary = _run(dkan_engine, spec, source)
    assert summary["total_rows_invalidated"] == 1

    cur = _current(dkan_engine, target)
    assert len(cur) == 6
    assert "000000" not in set(cur["facility_id"])


def test_without_invalidation_removed_rows_stay_current(dkan_engine):
    schema = _schema_name(dkan_engine)
    spec = make_hospital_spec(schema)  # invalidate_missing defaults False
    target = TableRef(spec.target_table, schema)
    source = seeded_pdc_source()
    _run(dkan_engine, spec, source)

    source.set_dataset(
        HOSPITAL_ID, "Hospital General Information", make_hospital_rows()[1:], "2026-07-01"
    )

    summary = _run(dkan_engine, spec, source)
    assert summary["total_rows_invalidated"] == 0
    cur = _current(dkan_engine, target)
    assert len(cur) == 7  # the removed facility stays current
    assert "000000" in set(cur["facility_id"])


def test_family_siblings_coexist_in_one_table(dkan_engine):
    schema = _schema_name(dkan_engine)
    spec = make_payments_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = _run(dkan_engine, spec, seeded_op_source())

    assert summary["datasets_processed"] == 2
    cur = _current(dkan_engine, target)
    counts = cur.groupby("_source_dataset").size().to_dict()
    assert counts == {OP_2023_ID: 5, OP_2024_ID: 5}


def test_union_table_holds_a_siblings_extra_column(dkan_engine):
    schema = _schema_name(dkan_engine)
    source = seeded_op_source()
    # Give the 2024 sibling a column 2023 lacks.
    extra = [dict(r, **{"New 2024 Column": "x"}) for r in source.datasets[OP_2024_ID]["rows"]]
    source.set_dataset(OP_2024_ID, "2024 General Payment Data", extra, modified="2026-01-27")

    spec = make_payments_spec(schema)
    target = TableRef(spec.target_table, schema)
    summary = _run(dkan_engine, spec, source)

    assert summary["errors"] == []
    cur = _current(dkan_engine, target)
    assert "new_2024_column" in cur.columns
    # 2024 rows carry the value; 2023 rows have it as NULL (union padding).
    got = cur[cur["_source_dataset"] == OP_2024_ID]["new_2024_column"].tolist()
    assert set(got) == {"x"}
    missing = cur[cur["_source_dataset"] == OP_2023_ID]["new_2024_column"]
    assert missing.isnull().all()


def test_failing_sibling_does_not_block_others(dkan_engine):
    schema = _schema_name(dkan_engine)
    spec = make_payments_spec(schema)
    target = TableRef(spec.target_table, schema)
    source = seeded_op_source()

    reader = DKANReader(
        client_factory=lambda base_url: _failing_client(base_url, source, OP_2023_ID)
    )
    summary = run_dkan_collection(reader, spec, dkan_engine)

    assert summary["datasets_processed"] == 1
    assert len(summary["errors"]) == 1
    assert summary["errors"][0]["dataset_identifier"] == OP_2023_ID
    # The healthy sibling still landed.
    assert len(_current(dkan_engine, target)) == 5


def test_file_and_datastore_retrieval_are_version_equivalent(dkan_engine):
    schema = _schema_name(dkan_engine)
    spec = make_hospital_spec(schema, retrieval="datastore")
    target = TableRef(spec.target_table, schema)
    source = seeded_pdc_source()
    _run(dkan_engine, spec, source)

    file_spec = dataclasses.replace(spec, retrieval="file")
    summary = _run(dkan_engine, file_spec, source)

    # Same data via the other retrieval mode is zero spurious SCD2 versions.
    assert summary["total_rows_merged"] == 0
    assert len(_history(dkan_engine, target)) == 7


def test_tracker_records_a_run_per_dataset(dkan_engine):
    schema = _schema_name(dkan_engine)
    spec = make_payments_spec(schema)
    tracker = NoopTracker()

    _run(dkan_engine, spec, seeded_op_source(), tracker=tracker)

    tracked = {dataset_id for dataset_id, _run in tracker.runs}
    assert tracked == {OP_2023_ID, OP_2024_ID}


# --------------------------------------------------------------- helpers


def _failing_client(base_url, source, fail_identifier):
    client = FakeDKANClient(base_url, source)
    client.fail_identifiers.add(fail_identifier)
    return client
