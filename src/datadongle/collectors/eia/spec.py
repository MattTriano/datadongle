"""EIADatasetSpec — defines one EIA API v2 dataset to collect.

A spec names a single EIA v2 data series (a route path), a single frequency,
the measure columns to pull, and an optional facet filter — and maps them to
exactly one target table. EIA's API is uniform across its whole tree, so one
``EIAReader`` serves any spec; the spec is the only thing that varies per
dataset.

Usage:
    from datadongle.collectors.eia.spec import EIADatasetSpec

    spec = EIADatasetSpec(
        name="us_electricity_retail_sales",
        target_table="electricity_retail_sales",
        route_path="electricity/retail-sales",
        frequency="monthly",
        data_columns=["price", "revenue", "sales", "customers"],
        facets={"stateid": ["CO"], "sectorid": ["RES"]},  # optional filter
        entity_key=["stateid", "sectorid", "period"],      # ⇒ SCD2 history
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field

from datadongle.collectors.base_spec import DatasetSpec


@dataclass
class EIADatasetSpec(DatasetSpec):
    """Defines an EIA API v2 dataset to collect.

    Parameters
    ----------
    name : str
        Human-readable dataset name on this system.
    target_table : str
        Destination table name.
    route_path : str
        The EIA v2 route to the data series, e.g. ``"electricity/retail-sales"``
        (no leading/trailing slashes needed; ``/data/`` is appended by the client).
    frequency : str
        A single periodicity the route offers, e.g. ``"monthly"``, ``"annual"``,
        ``"hourly"``. Frequency is a query parameter within a route, so one
        route + one frequency is one grain — hence one target table.
    data_columns : list[str]
        The measure columns to fetch (EIA's ``data[]`` params), e.g.
        ``["price", "revenue", "sales"]``. These land as ``DOUBLE`` columns.
    target_schema : str
        Destination schema/namespace. Default ``"raw_data"``.
    facets : dict[str, list[str]]
        Optional server-side filter: facet id → allowed values, e.g.
        ``{"stateid": ["CO"], "sectorid": ["RES"]}``. Empty means no filter.
    start, end : str | None
        Optional inclusive period bounds (in the route's period format, e.g.
        ``"2015-01"``). ``None`` means unbounded.
    entity_key : list[str] | None
        Columns uniquely identifying a time-series observation — typically the
        facet id columns plus ``"period"``. Non-empty ⇒ SCD2 history; ``None``
        ⇒ Append. Names must match the (normalized, lowercase) output columns.
    """

    name: str
    target_table: str
    route_path: str
    frequency: str
    data_columns: list[str]
    target_schema: str = "raw_data"
    facets: dict[str, list[str]] = field(default_factory=dict)
    start: str | None = None
    end: str | None = None
    entity_key: list[str] | None = None
    source: str = "eia"

    def __post_init__(self) -> None:
        self.route_path = self.route_path.strip().strip("/")
        if not self.route_path:
            raise ValueError("route_path is required, e.g. 'electricity/retail-sales'.")
        if not self.frequency:
            raise ValueError("frequency is required, e.g. 'monthly'.")
        if not self.data_columns:
            raise ValueError(
                "data_columns must name at least one measure to fetch "
                "(EIA's data[] parameters), e.g. ['price']."
            )

    @property
    def dataset_id(self) -> str:
        return f"{self.route_path}/{self.frequency}"
