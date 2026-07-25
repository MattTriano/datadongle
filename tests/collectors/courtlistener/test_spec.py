"""CourtListenerDatasetSpec field validation and defaults."""

from __future__ import annotations

import pytest

from datadongle.collectors.courtlistener.spec import CourtListenerDatasetSpec

from .helpers import make_spec


def test_defaults():
    spec = make_spec()
    assert spec.source == "courtlistener"
    assert spec.target_schema == "raw_data"
    assert spec.entity_key == ["id"]  # every CL table has an id PK ⇒ SCD2
    assert spec.backfill == "bulk"
    assert spec.cursor_column == "date_modified"
    assert spec.endpoint == "dockets"  # api_endpoint defaults to resource
    assert spec.file_prefix == "dockets"  # bulk_file_prefix defaults to resource
    assert spec.bulk_date is None


def test_resource_required_and_normalized():
    with pytest.raises(ValueError, match="resource is required"):
        make_spec(resource="  /  ")
    assert make_spec(resource="/clusters/").resource == "clusters"


def test_backfill_must_be_bulk_or_api():
    with pytest.raises(ValueError, match="backfill must be"):
        make_spec(backfill="ftp")


def test_filters_forbidden_with_bulk_backfill():
    with pytest.raises(ValueError, match="filters require backfill='api'"):
        make_spec(backfill="bulk", filters={"court": "scotus"})
    spec = make_spec(backfill="api", filters={"court": "scotus"})
    assert spec.filters == {"court": "scotus"}


def test_bulk_date_format_checked():
    with pytest.raises(ValueError, match="bulk_date must be"):
        make_spec(bulk_date="Jan 31 2024")
    assert make_spec(bulk_date="2024-01-31").bulk_date == "2024-01-31"


def test_endpoint_and_prefix_overrides():
    spec = make_spec(
        resource="clusters",
        bulk_file_prefix="opinion-clusters",
    )
    assert spec.endpoint == "clusters"
    assert spec.file_prefix == "opinion-clusters"


def test_dataset_id_includes_sorted_filters():
    assert make_spec().dataset_id == "dockets"
    spec = make_spec(backfill="api", filters={"court": "scotus", "blocked": "false"})
    assert spec.dataset_id == "dockets?blocked=false&court=scotus"


def test_entity_key_opt_out():
    assert make_spec(entity_key=None).entity_key is None


def test_is_a_dataset_spec():
    from datadongle.collectors.base_spec import DatasetSpec

    assert isinstance(make_spec(), DatasetSpec)
    assert isinstance(make_spec(), CourtListenerDatasetSpec)
