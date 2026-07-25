"""End-to-end behaviors of the static-file family driver.

Each test names one behavior the tooling must keep exhibiting, asserting only on
the target table and the driver summary. The HTTP boundary is faked (bytes served
from memory through a temp file, so parsing/encoding run the real code); the
Iceberg arm is hermetic (tmp warehouse) so it runs everywhere, and the Postgres
arm runs only when DWH_TEST_PG* is configured.
"""

from __future__ import annotations

import os
import uuid

import pytest

from datadongle.collectors.static.driver import run_static_collection
from datadongle.collectors.static.reader import StaticFileReader
from datadongle.collectors.static.spec import FileRef
from datadongle.core.engine import TableRef
from datadongle.engines.iceberg import IcebergEngine

from ..common import NoopTracker
from .helpers import (
    DEFAULT_HEADER,
    DEFAULT_URL_2022,
    DEFAULT_URL_2023,
    FakeStaticFileClient,
    csv_bytes,
    default_files,
    fake_client,
    make_spec,
)

# --------------------------------------------------------------- engine fixture


@pytest.fixture(
    params=[
        "iceberg",
        pytest.param("postgres", marks=pytest.mark.postgres),
    ]
)
def static_engine(request, tmp_path):
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
    schema = f"static_test_{uuid.uuid4().hex[:8]}"
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


def _current(engine, target, where=None):
    if isinstance(engine, IcebergEngine):
        df = engine.read_current(target)
    else:
        df = engine.query(f"select * from {target}")
        # SCD2 tables carry valid_to; append-only tables don't. When present,
        # current rows are the open ones; otherwise every row is "current".
        if "valid_to" in df.columns:
            df = df[df["valid_to"].isnull()]
    return df if where is None else df[where(df)]


def _history(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_history(target)
    return engine.query(f"select * from {target}")


def _run(engine, spec, files, tracker=None):
    reader = StaticFileReader(client=FakeStaticFileClient(files))
    return run_static_collection(reader, spec, engine, tracker=tracker)


def _two_file_spec(schema):
    return make_spec(
        schema,
        files=[
            FileRef(url=DEFAULT_URL_2022, vintage="2022", encoding="cp1252"),
            FileRef(url=DEFAULT_URL_2023, vintage="2023", encoding="cp1252"),
        ],
    )


# --------------------------------------------------------------- the behaviors


def test_multi_file_manifest_lands_all_vintages(static_engine, tmp_path):
    schema = _schema_name(static_engine)
    spec = _two_file_spec(schema)
    target = TableRef(spec.target_table, schema)

    summary = _run(static_engine, spec, default_files())

    assert summary["errors"] == []
    assert summary["files_processed"] == 2
    cur = _current(static_engine, target)
    assert set(cur["vintage"]) == {"2022", "2023"}
    assert len(cur) == 3  # 1 row in 2022 + 2 rows in 2023


def test_leading_zeros_and_cp1252_survive(static_engine, tmp_path):
    schema = _schema_name(static_engine)
    spec = make_spec(schema)  # 2023 only
    target = TableRef(spec.target_table, schema)

    _run(static_engine, spec, default_files())

    cur = _current(static_engine, target, where=lambda df: df["sys_id"] == "0895")
    assert len(cur) == 1  # would be 0 if the id had been numeric-parsed
    metro = _current(static_engine, target, where=lambda df: df["sys_id"] == "1001")
    assert metro["sys_name"].iloc[0] == "Example Health – Metro"


def test_repeat_collect_is_idempotent(static_engine, tmp_path):
    schema = _schema_name(static_engine)
    spec = _two_file_spec(schema)
    target = TableRef(spec.target_table, schema)
    _run(static_engine, spec, default_files())

    summary = _run(static_engine, spec, default_files())

    assert summary["total_rows_merged"] == 0
    assert len(_current(static_engine, target)) == 3
    assert len(_history(static_engine, target)) == 3


def test_revised_file_versions_only_the_changed_row(static_engine, tmp_path):
    schema = _schema_name(static_engine)
    spec = make_spec(schema)  # 2023 only
    target = TableRef(spec.target_table, schema)
    _run(static_engine, spec, default_files())

    # Publisher revises the file in place: 0895's bed count changes 298 -> 300.
    revised = dict(default_files())
    revised[DEFAULT_URL_2023] = csv_bytes(
        DEFAULT_HEADER,
        [["0895", "Adena Health System", "300"], ["1001", "Example Health – Metro", "512"]],
        encoding="cp1252",
    )
    summary = _run(static_engine, spec, revised)

    assert summary["total_rows_merged"] == 1
    cur = _current(static_engine, target, where=lambda df: df["sys_id"] == "0895")
    assert cur["beds"].iloc[0] == "300"
    assert len(_history(static_engine, target)) == 3  # 2 current + 1 closed-out


def test_append_only_spec_duplicates_on_rerun(static_engine, tmp_path):
    schema = _schema_name(static_engine)
    spec = make_spec(schema, entity_key=None)  # append-only
    target = TableRef(spec.target_table, schema)

    _run(static_engine, spec, default_files())
    _run(static_engine, spec, default_files())

    # No SCD2 dedup guard: a second run appends the same rows again.
    assert len(_current(static_engine, target)) == 4


def test_failing_file_does_not_block_others(static_engine, tmp_path):
    schema = _schema_name(static_engine)
    spec = _two_file_spec(schema)
    target = TableRef(spec.target_table, schema)

    reader = StaticFileReader(client=FakeStaticFileClient(default_files()))
    # Fail the 2023 file. (2022 is files[0], the schema-discovery sample, so it
    # must stay reachable for the table to be created at all.)
    fake_client(reader).fail_urls.add(DEFAULT_URL_2023)
    summary = run_static_collection(reader, spec, static_engine)

    assert summary["files_processed"] == 1
    assert len(summary["errors"]) == 1
    assert summary["errors"][0]["vintage"] == "2023"
    assert set(_current(static_engine, target)["vintage"]) == {"2022"}


def test_tracker_records_a_run_per_file(static_engine, tmp_path):
    schema = _schema_name(static_engine)
    tracker = NoopTracker()

    _run(static_engine, _two_file_spec(schema), default_files(), tracker=tracker)

    tracked = {dataset_id for dataset_id, _run in tracker.runs}
    assert tracked == {"test_static_systems/2022", "test_static_systems/2023"}
