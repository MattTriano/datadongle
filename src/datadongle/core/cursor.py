"""High-water-mark abstraction for incremental collection.

A ``CursorSpec`` declares *which* column(s) a source uses as its incremental
high-water mark; a ``Cursor`` is a concrete value read back from a target
table. The value is always derived from the target table (not a run log), so
it self-heals if the table is dropped and rebuilt.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CursorSpec:
    """Declares the column(s) a source uses as its incremental high-water mark.

    Parameters
    ----------
    column:
        The monotonically increasing column to filter on (e.g.
        ``"socrata_updated_at"``).
    tiebreak:
        Optional secondary column disambiguating rows that share the same
        ``column`` value (e.g. ``"socrata_id"``), so pagination is stable
        across equal timestamps. ``None`` means no tiebreak.
    """

    column: str
    tiebreak: str | None = None


@dataclass(frozen=True)
class Cursor:
    """A high-water-mark value read from a target table.

    ``value`` is the max of the cursor column; ``tiebreak`` is the max tiebreak
    value observed at that ``value``.
    """

    value: str
    tiebreak: str | None = None

    @property
    def sort_key(self) -> tuple[str, str]:
        """Comparable key so cursors can be ``max()``-ed regardless of tiebreak."""
        return (self.value, self.tiebreak or "")

    def encode(self) -> str:
        """Encode as ``"value|tiebreak"`` (or just ``"value"``) for logging."""
        return f"{self.value}|{self.tiebreak}" if self.tiebreak else self.value

    @classmethod
    def decode(cls, raw: str | None) -> Cursor | None:
        """Inverse of :meth:`encode`. Returns ``None`` for empty input."""
        if not raw:
            return None
        if "|" in raw:
            value, tiebreak = raw.rsplit("|", 1)
            return cls(value=value, tiebreak=tiebreak)
        return cls(value=raw)
