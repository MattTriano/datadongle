"""EIASpecBuilder — turn a route's metadata into a ready :class:`EIADatasetSpec`.

Filling a spec by hand means copying a route's frequency, measure columns, facet
ids, and — most error-prone — the SCD2 ``entity_key`` (whose names must match the
reader's *normalized* output columns). But :class:`EIAMetadata` already knows all
of that for a leaf route. This builder reads a route's metadata once and:

  - **fills** anything you omit from what the route offers — all measures ⇒
    ``data_columns``; the sole frequency (when a route has just one); the
    ``entity_key`` (the route's facet-id columns + ``"period"``); a derived
    ``name``/``target_table`` — and
  - **validates** anything you pass against the route: an unknown frequency,
    measure, or facet key raises here, at build time, with the valid options
    listed, instead of failing deep inside a collection run.

The spec itself stays network-free (its ``__post_init__`` does no I/O); every
lookup lives here, behind :class:`EIAMetadata`'s lazy client.

Usage:
    from datadongle.collectors.eia.builder import EIASpecBuilder

    b = EIASpecBuilder()                       # reads EIA_API_KEY from the env

    spec = b.build(
        "electricity/retail-sales",
        facets={"stateid": ["CO"], "sectorid": ["RES"]},
    )
    # frequency (the sole option), data_columns (all measures), entity_key
    # (stateid, sectorid, period), and name/target_table are all filled in.

    print(b.template("electricity/retail-sales"))   # editable spec snippet
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from datadongle.collectors.eia.metadata import EIAMetadata
from datadongle.collectors.eia.reader import normalize_column_name
from datadongle.collectors.eia.spec import EIADatasetSpec

# Distinguishes "entity_key omitted (derive it)" from "entity_key=None (Append)".
_UNSET = object()


class EIASpecBuilder:
    """Assemble validated :class:`EIADatasetSpec`\\ s from route metadata.

    Parameters
    ----------
    metadata : EIAMetadata | None
        The route-tree explorer to read from. If omitted, one is created from
        ``api_key``/``client`` (same lazy, env-fallback pattern as everywhere
        else — constructing the builder touches no network).
    """

    def __init__(
        self,
        metadata: EIAMetadata | None = None,
        *,
        api_key: str | None = None,
        client=None,
    ) -> None:
        self.metadata = metadata or EIAMetadata(client=client, api_key=api_key)

    # ------------------------------------------------------------------
    # Building a spec
    # ------------------------------------------------------------------

    def build(
        self,
        route_path: str,
        *,
        frequency: str | None = None,
        data_columns: Sequence[str] | None = None,
        facets: Mapping[str, Sequence[str]] | None = None,
        entity_key=_UNSET,
        start: str | None = None,
        end: str | None = None,
        name: str | None = None,
        target_table: str | None = None,
        target_schema: str = "raw_data",
        check_facet_values: bool = False,
    ) -> EIADatasetSpec:
        """Build a spec for ``route_path``, filling omissions and validating input.

        Omit ``frequency`` to use the route's sole frequency (raises if it offers
        several). Omit ``data_columns`` to pull every measure. Omit ``entity_key``
        to derive the SCD2 grain (facet-id columns + ``"period"``); pass
        ``entity_key=None`` to opt out of history (Append). Set
        ``check_facet_values=True`` to also validate each facet *value* against
        the route (one extra request per facet).
        """
        route = route_path.strip().strip("/")
        meta = self.metadata.describe(route)

        frequency = self._resolve_frequency(route, meta, frequency)
        base = _default_table_name(route, frequency)
        return EIADatasetSpec(
            name=name or base,
            target_table=target_table or base,
            target_schema=target_schema,
            route_path=route,
            frequency=frequency,
            data_columns=self._resolve_data_columns(route, meta, data_columns),
            facets=self._validate_facets(route, meta, facets or {}, check_facet_values),
            start=start,
            end=end,
            entity_key=self._resolve_entity_key(meta, entity_key),
        )

    # ------------------------------------------------------------------
    # Notebook scaffold
    # ------------------------------------------------------------------

    def template(self, route_path: str) -> str:
        """Return an editable ``EIADatasetSpec(...)`` snippet for a route.

        Every option the route surfaces is laid out with the alternatives in
        comments (all measures with their units, the frequencies, the facet keys,
        the period range). ``print(...)`` it in a notebook, then copy and edit.
        """
        route = route_path.strip().strip("/")
        meta = self.metadata.describe(route)
        freqs = _ids(meta.get("frequency"))
        measures: Mapping[str, dict] = meta.get("data") or {}
        facet_ids = _ids(meta.get("facets"))

        header = f"# EIA {route} — {meta.get('name') or meta.get('description') or ''}".rstrip()
        span = f"# period range: {meta.get('startPeriod')} … {meta.get('endPeriod')}"
        freq_line = (
            f'    frequency="{freqs[0]}",'
            + (f"  # or: {', '.join(freqs[1:])}" if len(freqs) > 1 else "")
            if freqs
            else '    frequency="",  # route lists no frequencies — is it a data series?'
        )
        measure_lines = [
            f'        "{mid}",{_units_comment(info)}' for mid, info in measures.items()
        ] or ['        "",  # route lists no measures — is it a data series?']
        facets_line = (
            f"    facets={{}},  # optional filter; valid keys: {', '.join(facet_ids)}"
            if facet_ids
            else "    facets={},  # route has no facets"
        )
        entity_line = f"    entity_key={self._resolve_entity_key(meta, _UNSET)!r},  # non-empty ⇒ SCD2 history"

        base = _default_table_name(route, freqs[0] if freqs else "")
        return "\n".join(
            [
                header,
                span,
                "EIADatasetSpec(",
                f'    name="{base}",',
                f'    target_table="{base}",',
                f'    route_path="{route}",',
                freq_line,
                "    data_columns=[",
                *measure_lines,
                "    ],",
                facets_line,
                entity_line,
                ")",
            ]
        )

    # ------------------------------------------------------------------
    # Resolvers / validators
    # ------------------------------------------------------------------

    def _resolve_frequency(self, route: str, meta: dict, frequency: str | None) -> str:
        offered = _ids(meta.get("frequency"))
        if not offered:
            raise ValueError(
                f"EIA route {route!r} offers no frequencies — it may be a category, "
                f"not a data series. Use EIAMetadata.browse({route!r}) to find a leaf."
            )
        if frequency is None:
            if len(offered) == 1:
                return offered[0]
            raise ValueError(
                f"EIA route {route!r} offers {len(offered)} frequencies "
                f"({', '.join(offered)}); pass frequency= to choose one."
            )
        if frequency not in offered:
            raise ValueError(
                f"{frequency!r} is not a frequency of EIA route {route!r}. "
                f"Valid: {', '.join(offered)}."
            )
        return frequency

    def _resolve_data_columns(
        self, route: str, meta: dict, data_columns: Sequence[str] | None
    ) -> list[str]:
        available = list((meta.get("data") or {}).keys())
        if not available:
            raise ValueError(
                f"EIA route {route!r} exposes no measure columns — it may be a "
                f"category, not a data series. Use EIAMetadata.browse to find a leaf."
            )
        if data_columns is None:
            return available
        chosen = list(data_columns)
        if not chosen:
            raise ValueError("data_columns cannot be empty; omit it to pull every measure.")
        unknown = [c for c in chosen if c not in available]
        if unknown:
            raise ValueError(
                f"EIA route {route!r} has no measure(s) {', '.join(map(repr, unknown))}. "
                f"Valid data_columns: {', '.join(available)}."
            )
        return chosen

    def _validate_facets(
        self,
        route: str,
        meta: dict,
        facets: Mapping[str, Sequence[str]],
        check_values: bool,
    ) -> dict[str, list[str]]:
        valid = _ids(meta.get("facets"))
        unknown = [k for k in facets if k not in valid]
        if unknown:
            raise ValueError(
                f"EIA route {route!r} has no facet(s) {', '.join(map(repr, unknown))}. "
                f"Valid facets: {', '.join(valid)}."
            )
        result = {k: list(v) for k, v in facets.items()}
        if check_values:
            for facet_id, chosen in result.items():
                allowed = {
                    v.get("id") for v in self.metadata.client.get_facet_values(route, facet_id)
                }
                bad = [c for c in chosen if c not in allowed]
                if bad:
                    raise ValueError(
                        f"Facet {facet_id!r} of EIA route {route!r} has no value(s) "
                        f"{', '.join(map(repr, bad))}. Use "
                        f"EIAMetadata.facet_values({route!r}, {facet_id!r}) to list them."
                    )
        return result

    def _resolve_entity_key(self, meta: dict, entity_key):
        if entity_key is not _UNSET:
            return entity_key  # explicit: a caller-chosen list, or None for Append
        # The SCD2 grain of a time-series observation: the route's facet-id
        # columns (normalized to match the reader's output) plus ``period``.
        return [normalize_column_name(fid) for fid in _ids(meta.get("facets"))] + ["period"]


# ----------------------------------------------------------------------
# Module helpers
# ----------------------------------------------------------------------


def _ids(entries) -> list[str]:
    """The ``id`` values from a metadata list (``frequency``/``facets``)."""
    return [e.get("id") for e in (entries or []) if e.get("id")]


def _units_comment(info) -> str:
    """A trailing ``# alias — units`` comment for a measure, when metadata has it."""
    if not isinstance(info, Mapping):
        return ""
    parts = [str(p) for p in (info.get("alias"), info.get("units")) if p]
    return f"  # {' — '.join(parts)}" if parts else ""


def _default_table_name(route: str, frequency: str) -> str:
    """A collision-free default table name: ``eia_<route>_<frequency>``."""
    stem = normalize_column_name(route)
    freq = normalize_column_name(frequency)
    return f"eia_{stem}_{freq}" if freq else f"eia_{stem}"
