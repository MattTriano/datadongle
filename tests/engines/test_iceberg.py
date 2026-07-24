"""Hermetic tests for the Shape-B IcebergEngine (tmp warehouse, no network)."""

from __future__ import annotations

import os

import pytest

from datadongle.core.cursor import CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema
from datadongle.core.write_mode import SCD2, Append, Upsert
from datadongle.engines.iceberg import IcebergEngine

TARGET = TableRef("permits", "raw_data")
SCD2_MODE = SCD2(entity_key=["permit_"])


def _schema() -> TableSchema:
    return TableSchema(
        columns=[
            Column("permit_", ColumnType.TEXT),
            Column("status", ColumnType.TEXT),
            Column("loc", ColumnType.GEOMETRY, geometry=GeometrySpec(kind="Point", srid=4326)),
            Column("socrata_id", ColumnType.TEXT, metadata=True),
            Column("socrata_updated_at", ColumnType.TIMESTAMPTZ, metadata=True),
        ]
    )


@pytest.fixture
def engine(tmp_path):
    return IcebergEngine(str(tmp_path / "warehouse"))


def _write(engine, target, schema, mode, rows):
    with engine.open_write(target, schema, mode) as ws:
        ws.write_batch(rows)
    return ws.rows_merged


def _rows(**over):
    base = {
        "permit_": "P1",
        "status": "open",
        "loc": "SRID=4326;POINT(-87.6 41.8)",
        "socrata_id": "1",
        "socrata_updated_at": "2024-01-01T00:00:00.000000",
    }
    base.update(over)
    return base


# ------------------------------------------------------------------ ensure_table


def test_ensure_table_creates_scd2_shape(engine):
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    assert engine.table_exists(TARGET)
    cols = engine.table_columns(TARGET)
    assert {"permit_", "status", "loc", "socrata_id", "socrata_updated_at"} <= cols
    # pipeline + SCD2 columns are engine-added
    assert {"ingested_at", "record_hash", "effective_from", "load_id"} <= cols
    assert engine.geometry_columns(TARGET) == {"loc": 4326}


def test_ensure_table_append_has_no_scd2_columns(engine):
    engine.ensure_table(TableRef("events", "raw_data"), _schema(), Append())
    cols = engine.table_columns(TableRef("events", "raw_data"))
    assert "ingested_at" in cols
    assert "record_hash" not in cols
    assert "effective_from" not in cols


def test_ensure_table_is_idempotent(engine):
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)  # no raise
    assert engine.table_exists(TARGET)


# ------------------------------------------------------------------ SCD2 writes


def test_scd2_first_load_inserts_all(engine):
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    merged = _write(engine, TARGET, _schema(), SCD2_MODE, [_rows(permit_="P1"), _rows(permit_="P2")])
    assert merged == 2
    assert len(engine.read_current(TARGET)) == 2
    assert len(engine.read_history(TARGET)) == 2


def test_scd2_identical_repull_is_a_noop(engine):
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    rows = [_rows(permit_="P1"), _rows(permit_="P2")]
    _write(engine, TARGET, _schema(), SCD2_MODE, rows)
    merged = _write(engine, TARGET, _schema(), SCD2_MODE, rows)
    assert merged == 0
    assert len(engine.read_history(TARGET)) == 2


def test_scd2_changed_record_adds_a_version(engine):
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    _write(engine, TARGET, _schema(), SCD2_MODE, [_rows(permit_="P1", status="open")])
    merged = _write(
        engine,
        TARGET,
        _schema(),
        SCD2_MODE,
        [_rows(permit_="P1", status="closed", socrata_updated_at="2024-02-01T00:00:00.000000")],
    )
    assert merged == 1
    assert len(engine.read_history(TARGET)) == 2
    current = engine.read_current(TARGET)
    assert len(current) == 1
    assert current[current["permit_"] == "P1"]["status"].iloc[0] == "closed"


def test_scd2_metadata_only_change_does_not_version(engine):
    """A new socrata_updated_at/version alone (metadata) must not create a version."""
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    _write(engine, TARGET, _schema(), SCD2_MODE, [_rows(permit_="P1", socrata_id="1")])
    merged = _write(
        engine,
        TARGET,
        _schema(),
        SCD2_MODE,
        [_rows(permit_="P1", socrata_id="9", socrata_updated_at="2025-01-01T00:00:00.000000")],
    )
    assert merged == 0
    assert len(engine.read_history(TARGET)) == 1


# ------------------------------------------------------------------ geometry


def test_geometry_round_trips_to_geodataframe(engine):
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    _write(engine, TARGET, _schema(), SCD2_MODE, [_rows(permit_="P2", loc="SRID=4326;POINT(-87.7 41.9)")])
    df = engine.read_current(TARGET)
    geom = df[df["permit_"] == "P2"]["loc"].iloc[0]
    assert geom.geom_type == "Point"
    assert round(geom.x, 1) == -87.7 and round(geom.y, 1) == 41.9


def test_record_hash_stable_across_equivalent_geometry(engine):
    """EWKT and WKB-hex for the same point must not create a spurious version."""
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    _write(engine, TARGET, _schema(), SCD2_MODE, [_rows(permit_="P1", loc="SRID=4326;POINT(-87.6 41.8)")])
    import shapely

    wkb_hex = shapely.to_wkb(shapely.from_wkt("POINT (-87.6 41.8)")).hex()
    merged = _write(engine, TARGET, _schema(), SCD2_MODE, [_rows(permit_="P1", loc=wkb_hex)])
    assert merged == 0


# ------------------------------------------------------------------ append


def test_append_mode_appends_everything(engine):
    schema = TableSchema(columns=[Column("id", ColumnType.TEXT), Column("v", ColumnType.INTEGER)])
    target = TableRef("events", "raw_data")
    engine.ensure_table(target, schema, Append())
    _write(engine, target, schema, Append(), [{"id": "a", "v": 1}, {"id": "b", "v": 2}])
    _write(engine, target, schema, Append(), [{"id": "a", "v": 1}])  # dup id, still appended
    assert len(engine.read_history(target)) == 3


# ------------------------------------------------------------------ high-water mark


def test_read_high_water_mark_from_table(engine):
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    _write(
        engine,
        TARGET,
        _schema(),
        SCD2_MODE,
        [
            _rows(permit_="P1", socrata_id="1", socrata_updated_at="2024-01-01T00:00:00.000000"),
            _rows(permit_="P2", socrata_id="2", socrata_updated_at="2024-03-05T12:00:00.000000"),
        ],
    )
    hwm = engine.read_high_water_mark(TARGET, CursorSpec("socrata_updated_at", "socrata_id"))
    assert hwm.value == "2024-03-05T12:00:00.000000"
    assert hwm.tiebreak == "2"


def test_read_high_water_mark_absent_table_is_none(engine):
    assert engine.read_high_water_mark(TARGET, CursorSpec("socrata_updated_at")) is None


def test_read_high_water_mark_self_heals_after_drop(engine):
    """HWM comes from the table, so dropping + rebuilding resets it."""
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    _write(engine, TARGET, _schema(), SCD2_MODE, [_rows(permit_="P1")])
    assert engine.read_high_water_mark(TARGET, CursorSpec("socrata_updated_at", "socrata_id")) is not None
    engine.catalog.drop_table(engine._identifier(TARGET))
    assert engine.read_high_water_mark(TARGET, CursorSpec("socrata_updated_at", "socrata_id")) is None


# ------------------------------------------------------------------ upsert


def _kv_schema() -> TableSchema:
    return TableSchema(columns=[Column("id", ColumnType.TEXT), Column("v", ColumnType.TEXT)])


def test_upsert_inserts_then_updates(engine):
    schema, target, mode = _kv_schema(), TableRef("things", "raw_data"), Upsert(keys=["id"])
    engine.ensure_table(target, schema, mode)
    _write(engine, target, schema, mode, [{"id": "a", "v": "1"}, {"id": "b", "v": "2"}])

    # Update a, insert c.
    merged = _write(engine, target, schema, mode, [{"id": "a", "v": "9"}, {"id": "c", "v": "3"}])
    assert merged == 2  # 1 updated + 1 inserted

    df = engine.read_current(target)  # no entity_key ⇒ the table is the current state
    assert dict(zip(df["id"], df["v"], strict=True)) == {"a": "9", "b": "2", "c": "3"}


def test_upsert_on_conflict_nothing_keeps_existing(engine):
    schema, target = _kv_schema(), TableRef("things", "raw_data")
    mode = Upsert(keys=["id"], on_conflict="nothing")
    engine.ensure_table(target, schema, mode)
    _write(engine, target, schema, mode, [{"id": "a", "v": "1"}])

    # a already present -> ignored; c is new -> inserted.
    merged = _write(engine, target, schema, mode, [{"id": "a", "v": "9"}, {"id": "c", "v": "3"}])
    assert merged == 1

    df = engine.read_current(target)
    assert dict(zip(df["id"], df["v"], strict=True)) == {"a": "1", "c": "3"}


# ------------------------------------------------------------- invalidate_missing


INVALIDATING = SCD2(entity_key=["permit_"], invalidate_missing=True)


def test_invalidate_missing_tombstones_absent_entity(engine):
    engine.ensure_table(TARGET, _schema(), INVALIDATING)
    _write(engine, TARGET, _schema(), INVALIDATING, [_rows(permit_="P1"), _rows(permit_="P2")])
    assert set(engine.read_current(TARGET)["permit_"]) == {"P1", "P2"}

    # A full pull missing P2: P1 unchanged (no new version), P2 tombstoned.
    with engine.open_write(TARGET, _schema(), INVALIDATING) as ws:
        ws.write_batch([_rows(permit_="P1")])
    assert ws.rows_merged == 0
    assert ws.rows_invalidated == 1

    # P2 drops out of current but its history (version + tombstone) remains.
    assert set(engine.read_current(TARGET)["permit_"]) == {"P1"}
    hist = engine.read_history(TARGET)
    assert (hist["permit_"] == "P2").sum() == 2


def test_invalidate_missing_reappearance_with_change_restores(engine):
    engine.ensure_table(TARGET, _schema(), INVALIDATING)
    _write(engine, TARGET, _schema(), INVALIDATING, [_rows(permit_="P1"), _rows(permit_="P2")])
    _write(engine, TARGET, _schema(), INVALIDATING, [_rows(permit_="P1")])  # P2 tombstoned
    assert set(engine.read_current(TARGET)["permit_"]) == {"P1"}

    # P2 returns with changed content -> a new version supersedes the tombstone.
    _write(engine, TARGET, _schema(), INVALIDATING,
           [_rows(permit_="P1"), _rows(permit_="P2", status="reopened")])
    current = engine.read_current(TARGET)
    assert set(current["permit_"]) == {"P1", "P2"}
    assert current[current["permit_"] == "P2"]["status"].iloc[0] == "reopened"


def test_invalidate_missing_does_not_double_tombstone(engine):
    engine.ensure_table(TARGET, _schema(), INVALIDATING)
    _write(engine, TARGET, _schema(), INVALIDATING, [_rows(permit_="P1"), _rows(permit_="P2")])
    _write(engine, TARGET, _schema(), INVALIDATING, [_rows(permit_="P1")])  # tombstones P2

    # P2 still absent: it is already a tombstone, so nothing new is written.
    with engine.open_write(TARGET, _schema(), INVALIDATING) as ws:
        ws.write_batch([_rows(permit_="P1")])
    assert ws.rows_invalidated == 0
    assert (engine.read_history(TARGET)["permit_"] == "P2").sum() == 2


# ------------------------------------------------------- staging / bounded memory


def _data_file_count(engine, target) -> int:
    return len(list(engine._load(target).scan().plan_files()))


def test_streaming_append_writes_in_chunks(engine):
    """A batch larger than the chunk size streams out over several appends —
    every row lands, and it produces more than one data file (proof the merge
    was not materialized whole)."""
    engine.STREAM_CHUNK_ROWS = 3
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    rows = [_rows(permit_=f"P{i}", socrata_id=str(i)) for i in range(10)]
    merged = _write(engine, TARGET, _schema(), SCD2_MODE, rows)
    assert merged == 10
    assert set(engine.read_current(TARGET)["permit_"]) == {f"P{i}" for i in range(10)}
    assert _data_file_count(engine, TARGET) > 1  # chunked, not one big file


def test_staging_accumulates_across_write_batches(engine):
    """Rows staged over several write_batch calls all merge on flush."""
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    with engine.open_write(TARGET, _schema(), SCD2_MODE) as ws:
        ws.write_batch([_rows(permit_="P1", socrata_id="1")])
        ws.write_batch([_rows(permit_="P2", socrata_id="2")])
        ws.write_batch([_rows(permit_="P3", socrata_id="3")])
    assert ws.rows_staged == 3
    assert ws.rows_merged == 3
    assert set(engine.read_current(TARGET)["permit_"]) == {"P1", "P2", "P3"}


def test_staging_file_is_removed_on_clean_exit(engine):
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    with engine.open_write(TARGET, _schema(), SCD2_MODE) as ws:
        ws.write_batch([_rows(permit_="P1")])
        assert os.path.exists(ws._stage_path)  # staged to disk mid-session
        stage_path = ws._stage_path
    assert not os.path.exists(stage_path)  # cleaned up after flush


def test_staging_file_is_removed_and_merge_skipped_on_error(engine):
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    stage_path = None
    with pytest.raises(RuntimeError):
        with engine.open_write(TARGET, _schema(), SCD2_MODE) as ws:
            ws.write_batch([_rows(permit_="P1")])
            stage_path = ws._stage_path
            raise RuntimeError("boom")
    assert stage_path is not None and not os.path.exists(stage_path)
    assert len(engine.read_history(TARGET)) == 0  # error exit wrote nothing


def test_custom_staging_dir_is_created_and_used(tmp_path):
    staging = tmp_path / "scratch"
    engine = IcebergEngine(str(tmp_path / "warehouse"), staging_dir=str(staging))
    assert engine.staging_dir == str(staging)
    assert staging.exists()
    engine.ensure_table(TARGET, _schema(), SCD2_MODE)
    with engine.open_write(TARGET, _schema(), SCD2_MODE) as ws:
        assert os.path.dirname(ws._stage_path) == str(staging)
        ws.write_batch([_rows(permit_="P1")])
    assert len(engine.read_current(TARGET)) == 1
