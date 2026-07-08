"""The source-adapter interface.

A ``SourceReader`` is the *source* half of the load contract: it knows how to
talk to one upstream (Socrata, ArcGIS Hub, ...), what its target table looks
like, which write policy it wants, and how to page incrementally. It knows
nothing about staging, merges, or SQL — the engine and the shared driver
handle those.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable

from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import TableSchema
from datadongle.core.write_mode import WriteMode


@runtime_checkable
class SourceReader(Protocol):
    """Adapts one data source to the shared collection driver.

    ``spec`` is a source-specific ``DatasetSpec`` describing what to collect.
    """

    source: str

    def dataset_id(self, spec: Any) -> str:
        """A stable identifier for ``spec`` within this source (for run logging)."""

    def target(self, spec: Any) -> TableRef:
        """The destination table for ``spec``."""

    def schema(self, spec: Any) -> TableSchema:
        """The engine-neutral schema of the target table."""

    def write_mode(self, spec: Any, *, mode: str) -> WriteMode:
        """The load policy (Append / Upsert / SCD2) for ``spec``.

        ``mode`` is the collection mode (``"full"`` / ``"incremental"``) the
        driver is about to run. Most sources ignore it, but a policy can
        legitimately depend on it — e.g. OSM enables SCD2 ``invalidate_missing``
        only on a ``"full"`` pull, where "which entities are absent" is
        observable.
        """

    def cursor_spec(self, spec: Any) -> CursorSpec | None:
        """Incremental cursor columns, or ``None`` if not incrementally queryable."""

    def read(self, spec: Any, *, since: Cursor | None) -> Iterator[list[dict[str, Any]]]:
        """Yield batches of rows. ``since=None`` means a full read.

        Source-specific transforms (renames, geometry normalization, dropping
        computed columns) are applied here, so batches are ready to stage.
        """

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        """Return the max cursor value in ``batch`` (``None`` if not applicable)."""
