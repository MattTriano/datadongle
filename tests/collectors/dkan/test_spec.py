"""Unit tests for DKANDatasetSpec validation."""

from __future__ import annotations

import pytest

from datadongle.collectors.dkan.spec import DKANDatasetSpec

from .helpers import make_hospital_spec, make_payments_spec


def test_requires_dataset_identifiers():
    with pytest.raises(ValueError, match="dataset_identifiers"):
        DKANDatasetSpec(name="x", base_url="https://p", target_table="x", entity_key=["id"])


def test_requires_target_table():
    with pytest.raises(ValueError, match="target_table"):
        DKANDatasetSpec(
            name="x", base_url="https://p", dataset_identifiers=["a"], entity_key=["id"]
        )


def test_requires_entity_key():
    with pytest.raises(ValueError, match="entity_key"):
        DKANDatasetSpec(name="x", base_url="https://p", dataset_identifiers=["a"], target_table="x")


def test_rejects_unknown_retrieval_mode():
    with pytest.raises(ValueError, match="retrieval"):
        make_hospital_spec("raw_data", retrieval="carrier_pigeon")


def test_forbids_invalidate_missing_for_multi_dataset_families():
    # Staging holds one sibling at a time, so invalidation would close out
    # every other sibling's rows — must fail at construction.
    with pytest.raises(ValueError, match="invalidate_missing"):
        make_payments_spec("raw_data", invalidate_missing=True)


def test_allows_invalidate_missing_for_single_dataset():
    assert make_hospital_spec("raw_data", invalidate_missing=True).invalidate_missing is True


def test_base_url_trailing_slash_stripped():
    spec = make_hospital_spec("raw_data", base_url="https://data.cms.gov/provider-data/")
    assert spec.base_url == "https://data.cms.gov/provider-data"
