"""TigerDatasetSpec — declares what TIGER/Line (or Cartographic Boundary) data
to collect and where it lands.

One spec maps a layer, collected across vintages and states, to one target
table. The fan-out over ``vintages × units`` (national / state / county files),
the union-of-vintages schema, and schema discovery from a sample shapefile are
handled by ``run_tiger_collection`` (the TIGER family driver) and
``TigerReader``; this module only describes the dataset.

Usage:
    from datadongle.collectors.tiger.spec import TigerDatasetSpec

    spec = TigerDatasetSpec(
        name="census_tracts",
        layer="TRACT",
        vintages=[2023, 2024],
        target_table="census_tracts",
        target_schema="raw_data",
        state_fips=["17"],
    )
"""

from __future__ import annotations

from dataclasses import dataclass

from datadongle.collectors.base_spec import DatasetSpec
from datadongle.collectors.tiger.metadata import TIGER_LAYER_SCOPE

# All 50 states + DC + PR.
ALL_STATE_FIPS = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15",
    "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27",
    "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "44", "45", "46", "47", "48", "49", "50", "51", "53",
    "54", "55", "56", "72",
]  # fmt: skip


@dataclass
class TigerDatasetSpec(DatasetSpec):
    """Defines a TIGER/Line or Cartographic Boundary dataset to collect.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "census_tracts", "primary_roads").
    layer : str
        TIGER layer name (e.g. "TRACT", "BG", "PRIMARYROADS", "ROADS").
        Case-insensitive; uppercased for TIGER, lowercased for cartographic.
    vintages : list[int]
        Years to collect (e.g. [2023, 2024]).
    target_table : str
        Destination table name (e.g. "census_tracts").
    target_schema : str
        Destination schema name. Default "raw_data".
    source : str
        "tiger" for TIGER/Line shapefiles, "cartographic" for Cartographic
        Boundary files. Default "tiger".
    resolution : str
        For cartographic files only. Default "500k".
    state_fips : list[str] | None
        Specific state FIPS codes. None means all states. Ignored for
        national-scope layers.
    entity_key : list[str] | None
        Columns that uniquely identify a feature (used for SCD2 merge). If
        None, the reader auto-detects a stable ID column from the shapefile
        schema; if none is found, the layer is collected append-only.
    lowercase_columns : bool
        Whether to lowercase shapefile column names. Default True.
    """

    name: str
    layer: str
    vintages: list[int]
    target_table: str
    target_schema: str = "raw_data"
    source: str = "tiger"
    resolution: str = "500k"
    state_fips: list[str] | None = None
    entity_key: list[str] | None = None
    lowercase_columns: bool = True

    def __post_init__(self):
        if self.source not in ("tiger", "cartographic"):
            raise ValueError(f"Unknown source {self.source!r}. Use 'tiger' or 'cartographic'.")

    @property
    def dataset_id(self) -> str:
        return self.target_table

    @property
    def scope(self) -> str:
        """Geographic scope of the layer: 'national', 'state', or 'county'."""
        return TIGER_LAYER_SCOPE.get(self.layer.upper(), "state")

    @property
    def states(self) -> list[str]:
        return self.state_fips if self.state_fips else ALL_STATE_FIPS
