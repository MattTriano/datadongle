from __future__ import annotations

import pytest

from datadongle.core.schema import Column, ColumnType, GeometrySpec, TableSchema


def test_plain_column():
    c = Column(name="permit_", type=ColumnType.TEXT)
    assert c.nullable is True
    assert c.geometry is None


def test_geometry_column_requires_spec():
    with pytest.raises(ValueError):
        Column(name="geom", type=ColumnType.GEOMETRY)


def test_non_geometry_column_rejects_spec():
    with pytest.raises(ValueError):
        Column(name="x", type=ColumnType.TEXT, geometry=GeometrySpec())


def test_geometry_spec_defaults():
    g = GeometrySpec()
    assert g.kind == "Geometry"
    assert g.srid == 4326


def test_table_schema_geometry_property():
    schema = TableSchema(
        columns=[
            Column(name="id", type=ColumnType.TEXT),
            Column(
                name="geom",
                type=ColumnType.GEOMETRY,
                geometry=GeometrySpec(kind="Point", srid=4326),
            ),
        ],
    )
    assert schema.column_names() == ["id", "geom"]
    assert set(schema.geometry) == {"geom"}
    assert schema.geometry["geom"].kind == "Point"


def test_table_schema_without_geometry():
    schema = TableSchema(columns=[Column(name="id", type=ColumnType.TEXT)])
    assert schema.geometry == {}


def test_metadata_column_names():
    schema = TableSchema(
        columns=[
            Column(name="id", type=ColumnType.TEXT),
            Column(name="value", type=ColumnType.INTEGER),
            Column(name="socrata_id", type=ColumnType.TEXT, metadata=True),
            Column(name="socrata_version", type=ColumnType.TEXT, metadata=True),
        ]
    )
    assert schema.metadata_column_names() == {"socrata_id", "socrata_version"}
    # a plain column is not metadata by default
    assert schema.columns[0].metadata is False
