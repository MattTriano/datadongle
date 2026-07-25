"""End-to-end: ArcGISHubReader through run_collection into a hermetic Iceberg
warehouse. Proves schema/ensure_table/SCD2 merge and the incremental HWM
round-trip (source date stored as ISO, read back, refiltered as epoch ms)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from datadongle.collectors.arcgishub.reader import ArcGISHubReader
from datadongle.collectors.arcgishub.spec import ArcGISHubDatasetSpec
from datadongle.core.engine import TableRef
from datadongle.engines.iceberg import IcebergEngine
from datadongle.load.driver import run_collection

from .conftest import BASE_URL, ITEM_ID, FakeArcGISHubClient, layer_payload, query_params

D1_MS = 1704164645000  # 2024-01-02T03:04:05Z
D2_MS = D1_MS + 86_400_000  # one day later


def _spec(**over) -> ArcGISHubDatasetSpec:
    base: dict[str, Any] = {
        "name": "arrests",
        "base_url": BASE_URL,
        "item_id": ITEM_ID,
        "target_table": "arrests",
        "entity_key": ["Event_Unique_Id"],
        "incremental_column": "Occurred_Date",
    }
    base.update(over)
    return ArcGISHubDatasetSpec(**base)


def _feature(oid, eid, date_ms):
    return {
        "attributes": {"OBJECTID": oid, "Event_Unique_Id": eid, "Occurred_Date": date_ms},
        "geometry": {"x": -79.4, "y": 43.7},
    }


@pytest.fixture
def engine(tmp_path):
    return IcebergEngine(str(tmp_path / "warehouse"))


def test_full_then_incremental_collection(engine):
    fake = FakeArcGISHubClient(
        layer_infos={0: layer_payload(0)},
        pages={
            0: [
                [_feature(1, "E1", D1_MS)],  # consumed by the full run
                [_feature(2, "E2", D2_MS)],  # consumed by the incremental run
            ]
        },
    )
    reader = ArcGISHubReader(client_factory=fake.factory())
    spec = _spec()
    target = TableRef("arrests", "raw_data")

    # Full run: E1 lands.
    summary = run_collection(reader, spec, engine, mode="full")
    assert summary["rows_merged"] == 1
    assert set(engine.read_current(target)["event_unique_id"]) == {"E1"}
    # The date field is stored as a TIMESTAMPTZ (UTC), not the raw epoch ms.
    assert engine.read_current(target)["occurred_date"].iloc[0] == datetime.fromtimestamp(
        D1_MS / 1000, tz=UTC
    )

    # Incremental run: the engine reads the HWM (max occurred_date) and the
    # reader passes it to the /query as an epoch-ms filter.
    summary2 = run_collection(reader, spec, engine, mode="incremental")
    assert summary2["rows_merged"] == 1
    assert set(engine.read_current(target)["event_unique_id"]) == {"E1", "E2"}

    # The second /query carried an incremental where derived from the HWM.
    query_wheres = [p["where"] for p in query_params(fake)]
    assert "Occurred_Date >" in query_wheres[-1]


def test_incremental_first_run_has_no_hwm_reads_all(engine):
    fake = FakeArcGISHubClient(
        layer_infos={0: layer_payload(0)},
        pages={0: [[_feature(1, "E1", D1_MS)]]},
    )
    reader = ArcGISHubReader(client_factory=fake.factory())
    # No prior table -> HWM is None -> full read; the where is just the base filter.
    run_collection(reader, _spec(), engine, mode="incremental")
    first_where = query_params(fake)[0]["where"]
    assert first_where == "1=1"
