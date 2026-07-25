"""Two-engine conformance: the same SocrataReader run through the shared driver
must produce the same logical outcomes on PostgresEngine and IcebergEngine.

The Iceberg arm is hermetic (tmp warehouse). The Postgres arm is skipped unless
a test database is configured via DWH_TEST_PG* env vars.
"""

from __future__ import annotations

import os
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from datadongle.collectors.socrata.reader import SocrataReader
from datadongle.collectors.socrata.spec import SocrataDatasetSpec
from datadongle.core.cursor import CursorSpec
from datadongle.core.engine import TableRef
from datadongle.engines.iceberg import IcebergEngine
from datadongle.load.driver import run_collection


def _reader():
    """A SocrataReader with mocked metadata + client (pages set per run)."""
    reader = SocrataReader()
    meta = MagicMock()
    meta.columns = [
        SimpleNamespace(field_name="permit", datatype="text"),
        SimpleNamespace(field_name="status", datatype="text"),
    ]
    meta.domain = "data.example.org"
    reader._metadata_cache["abcd-1234"] = meta
    reader._client = MagicMock()
    return reader


def _set_pages(reader, pages):
    reader._client.paginate.return_value = iter(pages)


CURSOR = CursorSpec("socrata_updated_at", "socrata_id")


def _spec_and_target(table_name):
    spec = SocrataDatasetSpec(
        name="permits",
        dataset_id="abcd-1234",
        target_table=table_name,
        target_schema="raw_data",
        entity_key=["permit"],
    )
    return spec, TableRef(table_name, "raw_data")


def _row(permit, status, sid, updated):
    return {":id": sid, ":updated_at": updated, "permit": permit, "status": status}


# --------------------------------------------------------------- engine fixtures


def _iceberg(tmp_path):
    return IcebergEngine(str(tmp_path / "warehouse"))


def _postgres():
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
    try:
        eng.execute("create schema if not exists raw_data")
    except Exception as e:  # pragma: no cover - depends on external DB
        pytest.skip(f"no usable test Postgres: {e}")
    return eng


@pytest.fixture(
    params=[
        "iceberg",
        pytest.param("postgres", marks=pytest.mark.postgres),
    ]
)
def env(request, tmp_path):
    """Yields (engine, spec, target) for each engine; drops the PG table after."""
    table_name = f"permits_{uuid.uuid4().hex[:8]}"
    spec, target = _spec_and_target(table_name)
    if request.param == "iceberg":
        yield SimpleNamespace(engine=_iceberg(tmp_path), spec=spec, target=target)
        return
    eng = _postgres()
    try:
        yield SimpleNamespace(engine=eng, spec=spec, target=target)
    finally:
        try:
            eng.execute(f"drop table if exists raw_data.{table_name}")
        finally:
            eng.close()


def _current(engine, target):
    if isinstance(engine, IcebergEngine):
        return engine.read_current(target)
    fqn = f"{target.namespace}.{target.name}"
    return engine.query(f"select * from {fqn} where valid_to is null")


def _status_of(engine, target, permit):
    df = _current(engine, target)
    return df[df["permit"] == permit]["status"].iloc[0]


# ------------------------------------------------------------------ the scenario


def test_incremental_scd2_conformance(env):
    engine, spec, target = env.engine, env.spec, env.target
    reader = _reader()

    # Run 1 (full): two entities land as current.
    _set_pages(
        reader,
        [
            [
                _row("P1", "open", "1", "2024-01-01T00:00:00.000000"),
                _row("P2", "open", "2", "2024-01-02T00:00:00.000000"),
            ]
        ],
    )
    s1 = run_collection(reader, spec, engine, mode="full")
    assert s1["rows_merged"] == 2
    assert len(_current(engine, target)) == 2

    # Run 2 (incremental, identical re-pull): a no-op, no new versions.
    _set_pages(
        reader,
        [
            [
                _row("P1", "open", "1", "2024-01-01T00:00:00.000000"),
                _row("P2", "open", "2", "2024-01-02T00:00:00.000000"),
            ]
        ],
    )
    s2 = run_collection(reader, spec, engine, mode="incremental")
    assert s2["rows_merged"] == 0
    assert len(_current(engine, target)) == 2

    # Run 3 (incremental, P1 changed): one new version; current reflects it.
    _set_pages(reader, [[_row("P1", "closed", "1", "2024-03-01T00:00:00.000000")]])
    s3 = run_collection(reader, spec, engine, mode="incremental")
    assert s3["rows_merged"] == 1
    assert len(_current(engine, target)) == 2
    assert _status_of(engine, target, "P1") == "closed"

    # HWM is read back from the table itself.
    hwm = engine.read_high_water_mark(target, CURSOR)
    assert hwm is not None
    assert hwm.value == "2024-03-01T00:00:00.000000"
    assert hwm.tiebreak == "1"
