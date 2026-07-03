"""Hermetic tests for the Shape-B IcebergEngine (tmp warehouse, no network)."""

from __future__ import annotations

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


# ------------------------------------------------------------------ unsupported modes


def test_upsert_not_supported(engine):
    with pytest.raises(NotImplementedError):
        engine.open_write(TARGET, _schema(), Upsert(keys=["permit_"]))


def test_invalidate_missing_not_supported(engine):
    with pytest.raises(NotImplementedError):
        engine.open_write(TARGET, _schema(), SCD2(entity_key=["permit_"], invalidate_missing=True))
