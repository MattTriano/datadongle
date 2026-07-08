from __future__ import annotations

from datadongle.core.cursor import Cursor, CursorSpec


def test_cursorspec_defaults():
    cs = CursorSpec(column="socrata_updated_at")
    assert cs.column == "socrata_updated_at"
    assert cs.tiebreak is None


def test_sort_key_orders_by_value_then_tiebreak():
    a = Cursor(value="2024-01-01", tiebreak="2")
    b = Cursor(value="2024-01-01", tiebreak="1")
    c = Cursor(value="2024-01-02", tiebreak="1")
    assert max([a, b, c], key=lambda x: x.sort_key) is c  # later value wins
    assert max([a, b], key=lambda x: x.sort_key) is a  # same value, larger tiebreak


def test_tiebreak_compares_lexically():
    # Matches Socrata's :id string comparison: "10" sorts BEFORE "9".
    lo = Cursor(value="v", tiebreak="10")
    hi = Cursor(value="v", tiebreak="9")
    assert max([lo, hi], key=lambda x: x.sort_key) is hi


def test_sort_key_handles_missing_tiebreak():
    a = Cursor(value="2024-01-01")
    assert a.sort_key == ("2024-01-01", "")


def test_encode_with_and_without_tiebreak():
    assert Cursor(value="v", tiebreak="t").encode() == "v|t"
    assert Cursor(value="v").encode() == "v"


def test_decode_roundtrip():
    assert Cursor.decode(None) is None
    assert Cursor.decode("") is None
    assert Cursor.decode("v") == Cursor(value="v")
    assert Cursor.decode("v|t") == Cursor(value="v", tiebreak="t")


def test_decode_rsplits_on_last_pipe():
    # A value containing a pipe keeps everything before the final pipe.
    assert Cursor.decode("a|b|t") == Cursor(value="a|b", tiebreak="t")
