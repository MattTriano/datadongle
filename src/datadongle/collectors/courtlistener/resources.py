"""What each CourtListener resource's entity key and cursor should be.

CourtListener's bulk exports are dumps of Django tables, so "what identifies a
row" is a property of the upstream schema rather than something a user should
have to guess. The resources fall into three shapes:

**Entity tables** (dockets, opinions, people, courts, financial disclosures, …)
carry a stable primary key ``id`` and a ``date_modified`` timestamp. These are
the large majority: ``entity_key=["id"]``, ``cursor_column="date_modified"``.
``courts`` is the same shape with a slug ``id`` (``"scotus"``) rather than an
integer.

**Link tables** — Django's auto-generated many-to-many through tables (the
citation map, opinion-cluster panels, ``joined_by``, court ``appeals_to``) —
also carry an ``id``, because Django adds one, but that surrogate is an
artifact of the dump. The row's identity is its foreign-key pair. Keying such a
table on ``id`` means a rebuild upstream renumbers every row, and SCD2 reads
that as "every entity replaced" — a full spurious re-version, with the old
entities never closed out. These also have **no** ``date_modified``, so they
are not incrementally queryable: every run is a full bulk read, which is what
makes ``SCD2(invalidate_missing=True)`` useful for them (it records when a
citation edge or panel assignment *disappeared*).

**Reference tables** (races, sources) carry an ``id`` but often no timestamps —
entity-shaped key, no cursor.

:func:`profile_from_columns` derives the right answer from a resource's actual
column list, so it works for resources this module has never heard of. The
:data:`RESOURCES` registry only holds cases where the column list alone is not
enough to decide, or where the API endpoint and bulk prefix differ.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Every CourtListener table that is incrementally queryable uses this column.
CURSOR_COLUMN = "date_modified"

# The surrogate primary key Django puts on every table, link tables included.
SURROGATE_KEY = "id"


@dataclass(frozen=True)
class ResourceProfile:
    """How one resource should be collected.

    ``entity_key`` is ``None`` when the column list gives no basis for one —
    the caller must supply it (or accept ``Append``). ``cursor_column`` is
    ``None`` when the resource has no ``date_modified`` and so cannot be read
    incrementally.
    """

    entity_key: list[str] | None
    cursor_column: str | None
    api_endpoint: str | None = None
    bulk_file_prefix: str | None = None
    rationale: str = ""

    @property
    def is_incremental(self) -> bool:
        return self.cursor_column is not None


# Resources whose column list alone doesn't settle the question, or whose API
# endpoint and bulk-file prefix differ. Everything absent from this registry is
# handled correctly by profile_from_columns, so keep it small.
#
# The **key is the canonical name** — what goes in a spec's ``resource``.
# ``api_endpoint`` and ``bulk_file_prefix`` record the other namespace's name
# where it differs from the key; ``None`` means "same as the key".
#
# Compiled without network access; `tests/collectors/courtlistener/test_live.py`
# checks every entry against the live source.
RESOURCES: dict[str, ResourceProfile] = {
    "clusters": ResourceProfile(
        entity_key=["id"],
        cursor_column=CURSOR_COLUMN,
        bulk_file_prefix="opinion-clusters",
        rationale="API endpoint 'clusters'; bulk file 'opinion-clusters'.",
    ),
    "citation-map": ResourceProfile(
        entity_key=["citing_opinion_id", "cited_opinion_id"],
        cursor_column=None,
        api_endpoint="opinions-cited",
        rationale=(
            "The opinions-cited through table: bulk file 'citation-map', API "
            "endpoint 'opinions-cited'. Its 'depth' payload column keeps it from "
            "matching the pure link-table shape, but the citing/cited pair is "
            "still the identity — the surrogate id is a dump artifact."
        ),
    ),
}

# A column named like a foreign key. Django names them "<related>_id".
_FK_SUFFIX = "_id"


def _foreign_keys(columns: list[str]) -> list[str]:
    return [c for c in columns if c.endswith(_FK_SUFFIX) and c != SURROGATE_KEY]


def looks_like_link_table(columns: list[str]) -> bool:
    """Whether ``columns`` has the shape of a Django many-to-many through table.

    The signature is narrow on purpose: a surrogate ``id``, exactly two foreign
    keys, no other columns, and no timestamps. Anything with its own payload or
    history is an entity table that happens to hold foreign keys, and keying it
    on those would collapse distinct rows together.
    """
    if SURROGATE_KEY not in columns or CURSOR_COLUMN in columns:
        return False
    fks = _foreign_keys(columns)
    return len(fks) == 2 and len(columns) == 3


def registry_lookup(resource: str) -> ResourceProfile | None:
    """The registry entry for ``resource``, under any of its names.

    Callers legitimately hold whichever name they met first: ``endpoints()``
    yields API names, ``bulk_datasets()`` yields bulk prefixes, specs carry the
    canonical one. Resolving all three keeps ``clusters``,
    ``opinion-clusters``, ``citation-map`` and ``opinions-cited`` from
    producing different answers for the same table.
    """
    if resource in RESOURCES:
        return RESOURCES[resource]
    for profile in RESOURCES.values():
        if resource in (profile.bulk_file_prefix, profile.api_endpoint):
            return profile
    return None


def bulk_prefix_for(resource: str) -> str:
    """The bulk-file prefix for ``resource``.

    The API and bulk names are separate namespaces — ``clusters`` is published
    as ``opinion-clusters`` — so anything turning a resource name into a bucket
    lookup has to come through here, or it asks S3 for a file that doesn't
    exist and concludes the data isn't published.
    """
    profile = RESOURCES.get(resource)
    if profile is not None and profile.bulk_file_prefix:
        return profile.bulk_file_prefix
    return resource


def api_endpoint_for(resource: str) -> str:
    """The API endpoint name for ``resource``.

    The mirror of :func:`bulk_prefix_for`: the citation map is published as the
    bulk file ``citation-map`` but served by the API endpoint
    ``opinions-cited``, so a request built from the bulk name 404s.
    """
    profile = RESOURCES.get(resource)
    if profile is not None and profile.api_endpoint:
        return profile.api_endpoint
    return resource


def profile_from_columns(resource: str, columns: list[str]) -> ResourceProfile:
    """Derive how ``resource`` should be collected from its actual columns.

    A registry entry for ``resource`` wins, since it encodes knowledge the
    column list can't carry. Otherwise the shape decides: a link table keys on
    its foreign-key pair, anything with an ``id`` keys on that, and a resource
    with neither gets ``entity_key=None`` for the caller to resolve.
    """
    registered = registry_lookup(resource)
    if registered is not None:
        return registered

    cursor = CURSOR_COLUMN if CURSOR_COLUMN in columns else None

    if looks_like_link_table(columns):
        return ResourceProfile(
            entity_key=_foreign_keys(columns),
            cursor_column=cursor,
            rationale=(
                "Link-table shape (surrogate id + exactly two foreign keys, no "
                "timestamps): the foreign-key pair is the identity, and the "
                "surrogate id would renumber on an upstream rebuild."
            ),
        )

    if SURROGATE_KEY in columns:
        return ResourceProfile(
            entity_key=[SURROGATE_KEY],
            cursor_column=cursor,
            rationale=(
                "Entity table: 'id' is the upstream primary key."
                + ("" if cursor else " No date_modified, so no incremental reads.")
            ),
        )

    return ResourceProfile(
        entity_key=None,
        cursor_column=cursor,
        rationale=(
            "No 'id' column, so the entity key can't be derived — pass entity_key "
            "explicitly, or leave it None to append without versioning."
        ),
    )


@dataclass
class ProfileSuggestion:
    """A derived profile plus the columns it was derived from."""

    resource: str
    profile: ResourceProfile
    columns: list[str] = field(default_factory=list)

    def spec_kwargs(self) -> dict:
        """The spec fields this suggestion implies, ready to splat or paste."""
        kwargs: dict = {
            "entity_key": self.profile.entity_key,
            "cursor_column": self.profile.cursor_column,
        }
        if self.profile.api_endpoint:
            kwargs["api_endpoint"] = self.profile.api_endpoint
        if self.profile.bulk_file_prefix:
            kwargs["bulk_file_prefix"] = self.profile.bulk_file_prefix
        return kwargs

    def __str__(self) -> str:
        return (
            f"{self.resource}: entity_key={self.profile.entity_key!r}, "
            f"cursor_column={self.profile.cursor_column!r}\n  {self.profile.rationale}"
        )
