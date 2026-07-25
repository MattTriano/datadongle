"""CMSReader — the data.cms.gov source adapter for the shared driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the existing
CMS ``client``/``metadata``/``spec``. It replaces the per-vintage load logic
that used to live in ``CMSCollector``; the family-level orchestration (resolving
a dataset's vintages from the catalog, the union-of-vintages table, per-vintage
error isolation) lives in :func:`run_cms_collection` in ``driver.py``.

A reader instance handles **one vintage at a time**: the family driver narrows a
multi-vintage spec to a single vintage (``spec.vintages == [vintage]``) before
calling ``schema``/``read``, and calls :meth:`versions` on the un-narrowed spec
to enumerate the fan-out.

Three things about CMS shape this reader:

  - **A vintage is the unit of work.** CMS publishes discrete annual/monthly
    versions rather than an append-only stream, so ``cursor_spec`` is ``None``
    (always a full read) and each vintage lands with its ``vintage`` stamped on
    every row. ``vintage`` is part of ``entity_key``, so it is a normal
    hashed/keyed data column, not bookkeeping.
  - **Two retrieval modes, one shape.** ``retrieval="api"`` pages rows out of the
    versioned JSON API; ``retrieval="csv"`` downloads the version's CSV
    distribution and streams it through the CSV parser. Both are normalized to
    the same column names so they produce identical SCD2 versions.
  - **Columns are text and drift across vintages.** All source columns are typed
    ``TEXT`` (casting is a downstream concern); the family driver unions each
    vintage's discovered columns so a column that appears or disappears across
    vintages still gets a home.

Unlike the old collector this carries no freshness-skip and no ``_source_modified``
advance: SCD2 is idempotent, so re-collecting an unchanged vintage is a no-op
merge, and the advance was an in-place UPDATE with no engine-neutral analogue.
The trade-off is that every run re-downloads each in-scope vintage.
"""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from datadongle.collectors.cms.client import CMSClient
from datadongle.collectors.cms.metadata import CMSDatasetVersion, CMSMetadata
from datadongle.collectors.cms.spec import CMSDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, WriteMode
from datadongle.parsers.csv_parser import parse_csv

logger = logging.getLogger(__name__)


class CMSReader:
    """Adapts one data.cms.gov vintage to the shared collection driver."""

    source = "cms"

    def __init__(self, client: CMSClient | None = None) -> None:
        self._client = client
        self._metadata: CMSMetadata | None = None
        # dataset_title -> its catalog versions (oldest first). Resolved once so
        # versions()/schema()/read() share one catalog fetch per title.
        self._versions_cache: dict[str, list[CMSDatasetVersion]] = {}

    @property
    def client(self) -> CMSClient:
        if self._client is None:
            self._client = CMSClient()
        return self._client

    @property
    def metadata(self) -> CMSMetadata:
        if self._metadata is None:
            self._metadata = CMSMetadata(self.client)
        return self._metadata

    @staticmethod
    def _vintage(spec: CMSDatasetSpec) -> str:
        """The single vintage this call operates on (spec is narrowed by the driver)."""
        assert spec.vintages, "spec must be narrowed to one vintage before this call"
        return spec.vintages[0]

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: CMSDatasetSpec) -> str:
        return f"{spec.dataset_title}/{self._vintage(spec)}"

    def target(self, spec: CMSDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: CMSDatasetSpec) -> TableSchema:
        """One vintage's discovered columns (normalized, text) plus ``vintage``.

        Columns are sampled from the version's API distribution (a one-row
        page); the family driver unions these across vintages. All source
        columns are text; casting is a downstream concern.
        """
        vintage = self._vintage(spec)
        version = self._version(spec, vintage)
        if not version.api_uuid:
            raise ValueError(
                f"Vintage {vintage!r} of {spec.dataset_title!r} has no API "
                f"distribution to sample columns from."
            )
        raw = self.metadata.columns(version.api_uuid)
        if not raw:
            raise ValueError(
                f"Vintage {vintage!r} of {spec.dataset_title!r} returned no "
                f"sample rows; its schema cannot be discovered."
            )
        columns = [Column(self._normalize_column(c), ColumnType.TEXT) for c in raw]
        columns.append(Column("vintage", ColumnType.TEXT, nullable=False))
        return TableSchema(columns=columns)

    def write_mode(self, spec: CMSDatasetSpec, *, mode: str = "full") -> WriteMode:
        # CMS is SCD2-only (the spec requires an entity_key that includes
        # "vintage"); the policy doesn't depend on the collection mode.
        assert spec.entity_key is not None  # CMSDatasetSpec requires it
        return SCD2(entity_key=spec.entity_key)

    def cursor_spec(self, spec: CMSDatasetSpec) -> CursorSpec | None:
        # A vintage is a discrete published version with no row cursor — the
        # driver always runs a full read.
        return None

    def read(
        self, spec: CMSDatasetSpec, *, since: Cursor | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        # ``since`` is always None (cursor_spec is None). ``spec`` is narrowed to
        # one vintage; page/parse its rows, normalize columns, and stamp vintage.
        vintage = self._vintage(spec)
        version = self._version(spec, vintage)

        name_map: dict[str, str] | None = None
        for batch in self._iter_source_batches(spec, version):
            if name_map is None and batch:
                name_map = {k: self._normalize_column(k) for k in batch[0]}
            yield self._prepare_batch(batch, name_map or {}, vintage)

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        return None

    # ------------------------------------------------------------------
    # Vintage enumeration (for the family driver)
    # ------------------------------------------------------------------

    def versions(self, spec: CMSDatasetSpec) -> list[str]:
        """The in-scope vintage labels for ``spec``, oldest first.

        Resolves the dataset's published versions from the catalog and applies
        ``spec.vintages`` as a filter, warning about any requested vintage the
        catalog doesn't have.
        """
        return [v.vintage for v in self._in_scope_versions(spec)]

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def _iter_source_batches(
        self, spec: CMSDatasetSpec, version: CMSDatasetVersion
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield raw source-row batches via the spec's retrieval mode."""
        if spec.retrieval == "api":
            if not version.api_uuid:
                raise ValueError(
                    f"Vintage {version.vintage!r} has no API distribution; "
                    f"set retrieval='csv' on the spec."
                )
            yield from self.client.iter_pages(version.api_uuid)
            return

        if not version.csv_url:
            raise ValueError(
                f"Vintage {version.vintage!r} has no CSV distribution; "
                f"set retrieval='api' on the spec."
            )
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", prefix="cms_", delete=False)
        tmp.close()
        filepath = Path(tmp.name)
        try:
            logger.info("Downloading %s", version.csv_url)
            self.client.download_csv(version.csv_url, filepath)
            yield from parse_csv(filepath)
        finally:
            filepath.unlink(missing_ok=True)

    def _prepare_batch(
        self, batch: list[dict[str, Any]], name_map: dict[str, str], vintage: str
    ) -> list[dict[str, Any]]:
        """Normalize column names and stamp the vintage onto each row."""
        prepared = []
        for row in batch:
            new = {name_map[k]: v for k, v in row.items() if k in name_map}
            new["vintage"] = vintage
            prepared.append(new)
        return prepared

    # ------------------------------------------------------------------
    # Catalog / version resolution
    # ------------------------------------------------------------------

    def _all_versions(self, spec: CMSDatasetSpec) -> list[CMSDatasetVersion]:
        title = spec.dataset_title
        if title not in self._versions_cache:
            dataset = self.metadata.get_dataset(title)
            self._versions_cache[title] = self.metadata.versions(dataset)
        return self._versions_cache[title]

    def _in_scope_versions(self, spec: CMSDatasetSpec) -> list[CMSDatasetVersion]:
        versions = self._all_versions(spec)
        if spec.vintages is not None:
            wanted = set(spec.vintages)
            found = {v.vintage for v in versions}
            missing = wanted - found
            if missing:
                logger.warning(
                    "Spec %r requests vintages not in the catalog: %s (available: %s)",
                    spec.name,
                    sorted(missing),
                    sorted(found),
                )
            versions = [v for v in versions if v.vintage in wanted]
        return versions

    def _version(self, spec: CMSDatasetSpec, vintage: str) -> CMSDatasetVersion:
        for v in self._all_versions(spec):
            if v.vintage == vintage:
                return v
        raise ValueError(
            f"Vintage {vintage!r} not found in the catalog for {spec.dataset_title!r}."
        )

    @staticmethod
    def _normalize_column(name: str) -> str:
        """Lowercase column names so they're queryable without quotes."""
        return name.strip().lstrip("\ufeff").lower().replace(" ", "_")
