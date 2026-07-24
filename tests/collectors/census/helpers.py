"""Fakes and test-data helpers for the Census collector tests.

The boundary that gets faked is the Census API (FakeCensusClient duck-types the
two client methods CensusReader uses: ``resolve_all_variables`` and
``fetch_variables``). Ingestion runs against a real hermetic ``IcebergEngine``
(tmp-path warehouse) so SCD2's actual semantics are part of the behavior under
test.

The fake reproduces two things that matter for the contract: variables can
differ per vintage (so the union-of-vintages table is exercised), and the API
returns estimate values as strings (so ``NUMERIC`` casting is exercised).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from datadongle.collectors.census.client import CensusClient
from datadongle.collectors.census.spec import CensusDatasetSpec

DATASET = "acs/acs5"


class FakeCensusSource:
    """In-memory stand-in for the Census API for one dataset."""

    def __init__(self) -> None:
        # (dataset, vintage) -> variable list
        self.variables: dict[tuple[str, int], list[str]] = {}
        # (dataset, vintage, state) -> list of raw API row dicts
        self.rows: dict[tuple[str, int, str], list[dict]] = {}

    def set_vintage(
        self, vintage: int, variables: list[str], state_rows: dict[str, list[dict]]
    ) -> None:
        self.variables[(DATASET, vintage)] = list(variables)
        for state, rows in state_rows.items():
            self.rows[(DATASET, vintage, state)] = rows


class FakeCensusClient:
    """Duck-types the two CensusClient methods CensusReader uses. No HTTP."""

    def __init__(self, source: FakeCensusSource) -> None:
        self.source = source
        self.fetch_calls: list[tuple[int, str]] = []
        self.resolve_calls: list[tuple[str, int]] = []
        self.fail_states: set[str] = set()

    def resolve_all_variables(self, spec, vintage: int) -> list[str]:
        self.resolve_calls.append((spec.dataset, vintage))
        return list(self.source.variables[(spec.dataset, vintage)])

    def fetch_variables(
        self, dataset, vintage, variables, geography_level, state_fips
    ) -> list[dict]:
        self.fetch_calls.append((vintage, state_fips))
        if state_fips in self.fail_states:
            raise RuntimeError(f"Injected failure for state {state_fips}")
        return [dict(r) for r in self.source.rows.get((dataset, vintage, state_fips), [])]


def as_client_factory(client: FakeCensusClient) -> Callable[[], CensusClient]:
    """Wrap a fake as a ``CensusReader`` client_factory.

    ``FakeCensusClient`` duck-types ``CensusClient`` rather than subclassing it,
    so the cast is where that intent is stated for the type checker.
    """
    return lambda: cast(CensusClient, client)


# ---------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------


def make_tract_rows(state: str, values: dict[str, str], n: int = 3) -> list[dict]:
    """Raw-API tract rows: NAME + geo ids (as strings) + each variable's value."""
    rows = []
    for i in range(n):
        row = {
            "NAME": f"Census Tract {i} state {state}",
            "state": state,
            "county": "031",
            "tract": f"00010{i}",
        }
        row.update(values)
        rows.append(row)
    return rows


def seeded_source() -> FakeCensusSource:
    """One dataset, two vintages with drifting variables, two states each.

    2021 has one variable; 2022 adds a second — so the union table must hold both.
    """
    source = FakeCensusSource()
    source.set_vintage(
        2021,
        ["B24010_001E"],
        {
            "17": make_tract_rows("17", {"B24010_001E": "100"}),
            "18": make_tract_rows("18", {"B24010_001E": "200"}),
        },
    )
    source.set_vintage(
        2022,
        ["B24010_001E", "B24010_002E"],
        {
            "17": make_tract_rows("17", {"B24010_001E": "110", "B24010_002E": "5"}),
            "18": make_tract_rows("18", {"B24010_001E": "210", "B24010_002E": "6"}),
        },
    )
    return source


def make_spec(schema: str, **overrides) -> CensusDatasetSpec:
    kwargs: dict[str, Any] = dict(
        name="occupation_by_sex",
        dataset=DATASET,
        vintages=[2021, 2022],
        groups=["B24010"],
        geography_level="tract",
        target_table="fake_occupation_by_sex_tract",
        target_schema=schema,
        state_fips=["17", "18"],
    )
    kwargs.update(overrides)
    return CensusDatasetSpec(**kwargs)
