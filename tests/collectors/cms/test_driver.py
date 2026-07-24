"""End-to-end behaviors of the CMS family driver.

Each test names one behavior the tooling must keep exhibiting, asserting only on
the target table and the driver summary. The Iceberg arm is hermetic (tmp
warehouse) so it runs everywhere; the Postgres arm runs only when DWH_TEST_PG*
is configured, and both must agree.
"""

from __future__ import annotations

import dataclasses
import os
import uuid

import pytest

from datadongle.collectors.cms.driver import run_cms_collection
from datadongle.collectors.cms.reader import CMSReader
from datadongle.core.engine import TableRef
from datadongle.engines.iceberg import IcebergEngine

from ..common import NoopTracker
from .helpers import (
    DATASET_TITLE,
    FakeCMSClient,
    FakeCMSSource,
    make_rows,
    make_spec,
    seeded_source,
)

# --------------------------------------------------------------- engine fixture


@pytest.fixture(params=[
    "iceberg",
    pytest.param("postgres", marks=pytest.mark.postgres),
])
def cms_engine(request, tmp_path):
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
    schema = f"cms_test_{uuid.uuid4().hex[:8]}"
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


def _run(engine, spec, source, tracker=None, client=None):
    reader = CMSReader(client=client or FakeCMSClient(source))
    return run_cms_collection(reader, spec, engine, tracker=tracker)


def _current(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_current(target)
    return engine.query(f'select * from {target} where "valid_to" is null')


def _history(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_history(target)
    return engine.query(f"select * from {target}")


def _current_counts(engine, target) -> dict[str, int]:
    cur = _current(engine, target)
    return {v: int((cur["vintage"] == v).sum()) for v in set(cur["vintage"])}


# --------------------------------------------------------------- the behaviors


def test_first_collect_ingests_every_vintage(cms_engine):
    schema = _schema_name(cms_engine)
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = _run(cms_engine, spec, seeded_source())

    assert summary["errors"] == []
    assert summary["versions_processed"] == 2
    assert _current_counts(cms_engine, target) == {"2022": 7, "2023": 7}
    cur = _current(cms_engine, target)
    # columns are normalized (queryable, lowercased, space -> underscore)
    assert "avg_payment_amt" in cur.columns
    assert "rndrng_prvdr_ccn" in cur.columns


def test_union_table_holds_a_vintages_extra_column(cms_engine):
    schema = _schema_name(cms_engine)
    source = FakeCMSSource()
    legacy = [
        {"Rndrng_Prvdr_CCN": "000001", "DRG_Cd": "001", "Legacy Col": "x", "Avg Payment Amt": "1"}
    ]
    source.set_version(DATASET_TITLE, "2013", legacy, modified="2023-05-10")
    source.set_version(DATASET_TITLE, "2023", make_rows(2), modified="2024-06-04")
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = _run(cms_engine, spec, source)

    assert summary["errors"] == []
    cur = _current(cms_engine, target)
    assert "legacy_col" in cur.columns
    assert cur[cur["vintage"] == "2013"]["legacy_col"].notnull().all()
    assert cur[cur["vintage"] == "2023"]["legacy_col"].isnull().all()


def test_repeat_collect_is_idempotent(cms_engine):
    schema = _schema_name(cms_engine)
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)
    _run(cms_engine, spec, seeded_source())

    summary = _run(cms_engine, spec, seeded_source())

    # Re-collecting unchanged data merges nothing and creates no duplicates.
    assert summary["total_rows_merged"] == 0
    assert len(_current(cms_engine, target)) == 14
    assert len(_history(cms_engine, target)) == 14


def test_changed_rerelease_versions_only_changed_row(cms_engine):
    schema = _schema_name(cms_engine)
    source = seeded_source()
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)
    _run(cms_engine, spec, source)

    corrected = make_rows(n=7)
    corrected[0]["Avg Payment Amt"] = "150.0"
    source.set_version(DATASET_TITLE, "2022", corrected, modified="2025-01-01")

    summary = _run(cms_engine, spec, source)

    assert summary["total_rows_merged"] == 1
    assert len(_current(cms_engine, target)) == 14
    assert len(_history(cms_engine, target)) == 15
    cur = _current(cms_engine, target)
    changed = cur[(cur["vintage"] == "2022") & (cur["rndrng_prvdr_ccn"] == "000000")]
    assert changed["avg_payment_amt"].iloc[0] == "150.0"


def test_vintages_filter_limits_scope(cms_engine):
    schema = _schema_name(cms_engine)
    spec = make_spec(schema, vintages=["2023"])
    target = TableRef(spec.target_table, schema)

    summary = _run(cms_engine, spec, seeded_source())

    assert summary["versions_processed"] == 1
    assert _current_counts(cms_engine, target) == {"2023": 7}


def test_failing_vintage_does_not_block_others(cms_engine):
    schema = _schema_name(cms_engine)
    source = seeded_source()
    client = FakeCMSClient(source)
    client.fail_uuids.add(source.uuid_for(DATASET_TITLE, "2022"))
    spec = make_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = _run(cms_engine, spec, source, client=client)

    assert summary["versions_processed"] == 1
    assert len(summary["errors"]) == 1
    assert summary["errors"][0]["vintage"] == "2022"
    assert _current_counts(cms_engine, target) == {"2023": 7}


def test_csv_and_api_retrieval_are_version_equivalent(cms_engine):
    schema = _schema_name(cms_engine)
    source = seeded_source()  # 2023 includes an empty-string payment value
    spec = make_spec(schema, retrieval="api")
    target = TableRef(spec.target_table, schema)
    _run(cms_engine, spec, source)

    csv_spec = dataclasses.replace(spec, retrieval="csv")
    summary = _run(cms_engine, csv_spec, source)

    # Same data via the other retrieval path is zero spurious SCD2 versions,
    # including API "" vs the CSV parser's NULL for the blank payment.
    assert summary["total_rows_merged"] == 0
    assert len(_history(cms_engine, target)) == 14


def test_tracker_records_a_run_per_vintage(cms_engine):
    schema = _schema_name(cms_engine)
    tracker = NoopTracker()

    _run(cms_engine, make_spec(schema), seeded_source(), tracker=tracker)

    tracked = {dataset_id for dataset_id, _run in tracker.runs}
    assert tracked == {f"{DATASET_TITLE}/2022", f"{DATASET_TITLE}/2023"}
