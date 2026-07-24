"""Spec validation: specs that would corrupt SCD2 history or can't be
executed fail loudly at construction time."""

from __future__ import annotations

import pytest

from datadongle.collectors.cms.spec import CMSDatasetSpec


def test_spec_rejects_entity_key_without_vintage():
    with pytest.raises(ValueError, match="vintage"):
        CMSDatasetSpec(
            name="x",
            dataset_title="X",
            target_table="x",
            entity_key=["rndrng_prvdr_ccn", "drg_cd"],
        )


def test_spec_requires_entity_key():
    with pytest.raises(ValueError, match="entity_key"):
        CMSDatasetSpec(name="x", dataset_title="X", target_table="x")


def test_spec_rejects_unknown_retrieval_mode():
    with pytest.raises(ValueError, match="retrieval"):
        CMSDatasetSpec(
            name="x",
            dataset_title="X",
            target_table="x",
            entity_key=["vintage"],
            retrieval="ftp",
        )
