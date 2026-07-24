"""Catalog parsing: the catalog's distributions resolve to one version
record per vintage, carrying both retrieval handles and the modified date,
oldest first, with the undated "latest" duplicate excluded."""

from __future__ import annotations

from datadongle.collectors.cms.metadata import CMSMetadata, vintage_from_temporal

from .helpers import DATASET_TITLE, FakeCMSClient, FakeCMSSource, as_client, make_rows


def test_versions_resolve_per_vintage_with_both_handles():
    source = FakeCMSSource()
    source.set_version(DATASET_TITLE, "2023", make_rows(2), modified="2024-06-04")
    source.set_version(DATASET_TITLE, "2022", make_rows(2), modified="2023-05-10")
    meta = CMSMetadata(as_client(FakeCMSClient(source)))

    versions = meta.versions(meta.get_dataset(DATASET_TITLE))

    assert [v.vintage for v in versions] == ["2022", "2023"]  # oldest first
    for v in versions:
        assert v.api_uuid == source.uuid_for(DATASET_TITLE, v.vintage)
        assert v.csv_url
        assert v.modified
    # Only the two dated vintages — the undated "latest" entry the fake
    # catalog includes (mirroring the real one) must not appear.
    assert len(versions) == 2


def test_non_calendar_year_temporal_falls_back_to_raw_label():
    assert vintage_from_temporal("2022-01-01/2022-12-31") == "2022"
    assert vintage_from_temporal("2022-07-01/2023-06-30") == "2022-07-01/2023-06-30"
