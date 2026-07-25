"""Unit behaviors of CensusReader: protocol conformance, schema, transforms.

End-to-end collection through the family driver lives in ``test_driver.py``.
"""

from __future__ import annotations

import dataclasses

import pytest

from datadongle.collectors.census.reader import CensusReader
from datadongle.core.engine import TableRef
from datadongle.core.reader import SourceReader
from datadongle.core.schema import ColumnType
from datadongle.core.write_mode import SCD2

from .helpers import FakeCensusClient, as_client_factory, make_spec, seeded_source


@pytest.fixture
def reader():
    source = seeded_source()
    return CensusReader(client_factory=as_client_factory(FakeCensusClient(source)))


def _one(spec, vintage, state=None):
    """Narrow a spec the way the driver does before calling the reader."""
    states = [state] if state is not None else spec.state_fips
    return dataclasses.replace(spec, vintages=[vintage], state_fips=states)


def test_reader_satisfies_the_protocol(reader):
    assert isinstance(reader, SourceReader)
    assert reader.source == "census"


def test_target_from_spec(reader):
    spec = make_spec("raw_data")
    assert reader.target(spec) == TableRef("fake_occupation_by_sex_tract", "raw_data")


def test_schema_has_geo_vintage_name_and_variables(reader):
    spec = _one(make_spec("raw_data"), 2022)
    schema = reader.schema(spec)
    by_name = {c.name: c for c in schema.columns}

    # geo ids + vintage lead and are not-null (they are the entity key)
    for col in ["state", "county", "tract"]:
        assert by_name[col].type is ColumnType.TEXT
        assert by_name[col].nullable is False
    assert by_name["vintage"].type is ColumnType.INTEGER
    assert by_name["vintage"].nullable is False

    assert by_name["NAME"].type is ColumnType.TEXT
    # 2022 has both variables; they are NUMERIC
    assert by_name["B24010_001E"].type is ColumnType.NUMERIC
    assert by_name["B24010_002E"].type is ColumnType.NUMERIC


def test_schema_reflects_the_vintages_own_variables(reader):
    spec = make_spec("raw_data")
    names_2021 = {c.name for c in reader.schema(_one(spec, 2021)).columns}
    names_2022 = {c.name for c in reader.schema(_one(spec, 2022)).columns}

    assert "B24010_002E" not in names_2021  # 2021 lacks the second variable
    assert "B24010_002E" in names_2022


def test_write_mode_is_scd2_from_entity_key(reader):
    spec = _one(make_spec("raw_data"), 2022)
    mode = reader.write_mode(spec, mode="full")
    assert isinstance(mode, SCD2)
    assert mode.entity_key == ["state", "county", "tract", "vintage"]


def test_not_incrementally_queryable(reader):
    spec = _one(make_spec("raw_data"), 2022)
    assert reader.cursor_spec(spec) is None
    assert reader.extract_cursor([{"vintage": 2022}]) is None


def test_read_stamps_vintage_and_yields_per_state(reader):
    spec = _one(make_spec("raw_data"), 2022, state="17")
    batches = list(reader.read(spec, since=None))

    assert len(batches) == 1  # one state -> one batch
    rows = batches[0]
    assert all(r["vintage"] == 2022 for r in rows)
    assert all(r["state"] == "17" for r in rows)
    assert rows[0]["B24010_002E"] == "5"  # values stay as-is (strings)


def test_read_normalizes_spaced_geo_id_names():
    """A geography whose API id column has a space ("block group") is normalized
    to the underscore name the schema/entity_key declare."""
    from .helpers import FakeCensusSource

    source = FakeCensusSource()
    source.set_vintage(
        2022,
        ["B24010_001E"],
        {
            "17": [
                {
                    "NAME": "BG 1",
                    "state": "17",
                    "county": "031",
                    "tract": "000100",
                    "block group": "1",
                    "B24010_001E": "9",
                }
            ]
        },
    )
    reader = CensusReader(client_factory=as_client_factory(FakeCensusClient(source)))
    spec = _one(make_spec("raw_data", geography_level="block group"), 2022, state="17")

    rows = next(iter(reader.read(spec, since=None)))
    assert "block_group" in rows[0]
    assert "block group" not in rows[0]


def test_variables_resolved_once_per_vintage(reader):
    """Reading every state of a vintage resolves that vintage's variables once."""
    spec = make_spec("raw_data")
    for state in spec.states:
        list(reader.read(_one(spec, 2022, state=state), since=None))

    client = reader.client
    assert client.resolve_calls.count((spec.dataset, 2022)) == 1
