from __future__ import annotations

import pytest

from datadongle.core.write_mode import SCD2, Append, Upsert, WriteMode


def test_append_is_a_write_mode():
    assert isinstance(Append(), WriteMode)


def test_upsert_requires_keys():
    with pytest.raises(ValueError):
        Upsert(keys=[])


def test_upsert_validates_on_conflict():
    Upsert(keys=["id"], on_conflict="update")
    Upsert(keys=["id"], on_conflict="nothing")
    with pytest.raises(ValueError):
        Upsert(keys=["id"], on_conflict="merge")


def test_upsert_defaults_to_update():
    assert Upsert(keys=["id"]).on_conflict == "update"


def test_scd2_requires_entity_key():
    with pytest.raises(ValueError):
        SCD2(entity_key=[])


def test_scd2_defaults():
    m = SCD2(entity_key=["case_number"])
    assert m.entity_key == ["case_number"]
    assert m.invalidate_missing is False
