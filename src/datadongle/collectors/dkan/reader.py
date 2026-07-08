"""DKANReader — the DKAN source adapter for the shared collection driver.

Implements the ``datadongle.core`` SourceReader protocol on top of the
existing DKAN ``client``/``metadata``/``spec``. It replaces the per-dataset
load logic that used to live in ``DKANCollector``; the family-level
orchestration (looping a spec's dataset identifiers, per-dataset error
isolation, the union-of-siblings table) lives in :func:`run_dkan_collection`
in ``driver.py``, and the actual full-vs-incremental machinery is the shared
driver's.

A reader instance handles **one dataset identifier at a time**: the family
driver narrows a multi-dataset spec to one identifier before calling any
reader method, so ``spec.dataset_identifiers`` always has a single entry here.

Three things about DKAN shape this reader:

  - **Full-refresh-only.** DKAN datasets are refreshed in place with no row
    cursor, so ``cursor_spec`` is ``None`` and the driver always runs a full
    read.
  - **Two retrieval modes.** ``retrieval="datastore"`` pages rows out of the
    datastore query API; ``retrieval="file"`` downloads the distribution file
    and streams it through the CSV parser. Both are normalized to the same
    column names so they produce identical SCD2 versions.
  - **DKAN's own name normalization.** The datastore serves normalized names
    (lowercase, whitespace -> ``_``, other punctuation *dropped* — "County/
    Parish" becomes "countyparish"); file headers keep the originals. Both are
    run through the same rule and truncated to Postgres's 63-char identifier
    limit, so the two retrieval modes converge. Every row is stamped with
    ``_source_dataset`` / ``_source_modified`` provenance (hash-excluded).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterator
from typing import Any

from datadongle.collectors.dkan.client import DKANClient
from datadongle.collectors.dkan.metadata import DKANMetadata
from datadongle.collectors.dkan.spec import DKANDatasetSpec
from datadongle.core.cursor import Cursor, CursorSpec
from datadongle.core.engine import TableRef
from datadongle.core.schema import Column, ColumnType, TableSchema
from datadongle.core.write_mode import SCD2, WriteMode
from datadongle.parsers.csv_parser import parse_csv

logger = logging.getLogger(__name__)

# Postgres's identifier length limit — normalized names are truncated to fit.
PG_MAX_IDENTIFIER = 63

# Provenance columns stamped on every row: which dataset it came from and that
# dataset's modified date at collection time. Both are hash-excluded
# (``metadata=True``) so re-publications and provenance don't create new
# SCD2 versions. ``_source_dataset`` is the grouping key across a family.
SOURCE_DATASET_COLUMN = "_source_dataset"
SOURCE_MODIFIED_COLUMN = "_source_modified"

PROVENANCE_COLUMNS: list[Column] = [
    Column(SOURCE_DATASET_COLUMN, ColumnType.TEXT, nullable=False, metadata=True),
    Column(SOURCE_MODIFIED_COLUMN, ColumnType.TEXT, metadata=True),
]


class DKANReader:
    """Adapts one DKAN dataset to the shared collection driver."""

    source = "dkan"

    def __init__(self, client_factory: Callable[[str], DKANClient] | None = None) -> None:
        self._client_factory = client_factory or DKANClient
        self._clients: dict[str, DKANClient] = {}
        self._dataset_cache: dict[tuple[str, str], dict] = {}

    def _client(self, base_url: str) -> DKANClient:
        base_url = base_url.rstrip("/")
        if base_url not in self._clients:
            self._clients[base_url] = self._client_factory(base_url)
        return self._clients[base_url]

    @staticmethod
    def _identifier(spec: DKANDatasetSpec) -> str:
        """The single dataset identifier this call operates on.

        The family driver narrows a multi-dataset spec to one identifier before
        calling the reader, so there is always exactly one here.
        """
        return spec.dataset_identifiers[0]

    def _dataset(self, spec: DKANDatasetSpec, identifier: str) -> dict:
        """Fetch and cache a dataset's metastore entry (for modified/distributions)."""
        key = (spec.base_url, identifier)
        if key not in self._dataset_cache:
            self._dataset_cache[key] = self._client(spec.base_url).get_dataset(identifier)
        return self._dataset_cache[key]

    # ------------------------------------------------------------------
    # SourceReader protocol
    # ------------------------------------------------------------------

    def dataset_id(self, spec: DKANDatasetSpec) -> str:
        return self._identifier(spec)

    def target(self, spec: DKANDatasetSpec) -> TableRef:
        return TableRef(spec.target_table, spec.target_schema)

    def schema(self, spec: DKANDatasetSpec) -> TableSchema:
        """Discovered schema: datastore-sampled column names + provenance.

        Column names come from a one-row datastore sample (works regardless of
        the retrieval mode) and are DKAN-normalized. All source columns are
        text; casting is a downstream concern.
        """
        identifier = self._identifier(spec)
        raw = DKANMetadata(self._client(spec.base_url)).columns(identifier)
        if not raw:
            raise ValueError(
                f"Dataset {identifier} returned no datastore sample; the schema "
                f"cannot be discovered. Is the datastore populated?"
            )
        name_map = self._build_name_map(raw)
        columns = [Column(name, ColumnType.TEXT) for name in name_map.values()]
        columns.extend(PROVENANCE_COLUMNS)
        return TableSchema(columns=columns)

    def write_mode(self, spec: DKANDatasetSpec, *, mode: str = "full") -> WriteMode:
        """SCD2 keyed on the spec's entity key.

        ``invalidate_missing`` is honored only on a ``"full"`` pull (which is
        the only mode DKAN runs) and the spec already forbids it for
        multi-dataset families, so a delisted entity is closed out only when
        the pull genuinely observed every entity.
        """
        return SCD2(
            entity_key=spec.entity_key,
            invalidate_missing=spec.invalidate_missing and mode == "full",
        )

    def cursor_spec(self, spec: DKANDatasetSpec) -> CursorSpec | None:
        # Refreshed in place with no row cursor — the driver runs a full read.
        return None

    def read(
        self, spec: DKANDatasetSpec, *, since: Cursor | None
    ) -> Iterator[list[dict[str, Any]]]:
        # ``since`` is always None: cursor_spec() is None, so the driver never
        # reads a high-water mark.
        identifier = self._identifier(spec)
        modified = self._dataset(spec, identifier).get("modified")

        name_map: dict[str, str] | None = None
        for batch in self._iter_source_batches(spec, identifier):
            if name_map is None and batch:
                name_map = self._build_name_map(list(batch[0].keys()))
            yield self._prepare_batch(batch, name_map or {}, identifier, modified)

    def extract_cursor(self, batch: list[dict[str, Any]]) -> Cursor | None:
        return None

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def _iter_source_batches(
        self, spec: DKANDatasetSpec, identifier: str
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield raw source-row batches via the spec's retrieval mode."""
        client = self._client(spec.base_url)
        if spec.retrieval == "datastore":
            yield from client.iter_pages(identifier)
            return

        distributions = DKANMetadata.distributions(self._dataset(spec, identifier))
        download_url = distributions[0].download_url if distributions else None
        if not download_url:
            raise ValueError(
                f"Dataset {identifier} has no distribution download URL; "
                f"set retrieval='datastore' on the spec."
            )
        filepath = client.download_to_tempfile(download_url, suffix=".csv")
        try:
            yield from parse_csv(filepath)
        finally:
            filepath.unlink(missing_ok=True)

    def _prepare_batch(
        self,
        batch: list[dict[str, Any]],
        name_map: dict[str, str],
        identifier: str,
        modified: str | None,
    ) -> list[dict[str, Any]]:
        """Normalize column names and stamp the provenance columns."""
        prepared = []
        for row in batch:
            new = {name_map[k]: v for k, v in row.items() if k in name_map}
            new[SOURCE_DATASET_COLUMN] = identifier
            new[SOURCE_MODIFIED_COLUMN] = modified
            prepared.append(new)
        return prepared

    # ------------------------------------------------------------------
    # Column normalization (DKAN-matching)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_name(raw: str) -> str:
        """Normalize a name the way DKAN's datastore does: lowercase, whitespace
        -> underscore, other punctuation *dropped* (so "County/Parish" ->
        "countyparish"), then truncated to Postgres's identifier limit.
        """
        name = raw.strip().lstrip("\ufeff").lower()
        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"[^a-z0-9_]", "", name)
        return name[:PG_MAX_IDENTIFIER].rstrip("_")

    def _build_name_map(self, raw_names: list[str]) -> dict[str, str]:
        """Ordered raw -> normalized mapping with collision handling.

        Collisions — including ones created by 63-char truncation — are
        suffixed ``_2``, ``_3``, ...; empty normalized names are skipped.
        """
        mapping: dict[str, str] = {}
        taken: dict[str, list[str]] = {}

        for raw in raw_names:
            base = self._normalize_name(raw)
            if not base:
                continue
            if base not in taken:
                taken[base] = [raw]
                mapping[raw] = base
                continue
            taken[base].append(raw)
            suffix = len(taken[base])
            while True:
                tail = f"_{suffix}"
                candidate = base[: PG_MAX_IDENTIFIER - len(tail)] + tail
                if candidate not in taken:
                    break
                suffix += 1
            taken[candidate] = [raw]
            mapping[raw] = candidate

        collisions = {n: rs for n, rs in taken.items() if len(rs) > 1}
        if collisions:
            logger.warning("Column name collisions after normalization: %s", collisions)
        return mapping
