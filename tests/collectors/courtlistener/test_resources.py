"""Deriving entity keys and cursors from a CourtListener resource's columns."""

from __future__ import annotations

import pytest

from datadongle.collectors.courtlistener.resources import (
    RESOURCES,
    ProfileSuggestion,
    looks_like_link_table,
    profile_from_columns,
)

# A dockets-shaped entity table.
ENTITY_COLUMNS = ["id", "date_created", "date_modified", "court_id", "case_name"]

# search_opinioncluster_panel: Django's auto-generated through table.
LINK_COLUMNS = ["id", "opinioncluster_id", "person_id"]


# --------------------------------------------------------------- entity tables


def test_entity_table_keys_on_the_upstream_primary_key():
    profile = profile_from_columns("dockets", ENTITY_COLUMNS)

    assert profile.entity_key == ["id"]
    assert profile.cursor_column == "date_modified"
    assert profile.is_incremental


def test_entity_table_holding_foreign_keys_still_keys_on_id():
    """Foreign keys don't make it a link table — it has its own payload."""
    profile = profile_from_columns("opinions", ENTITY_COLUMNS)
    assert profile.entity_key == ["id"]


def test_reference_table_without_timestamps_keys_on_id_but_has_no_cursor():
    profile = profile_from_columns("people-db-races", ["id", "race"])

    assert profile.entity_key == ["id"]
    assert profile.cursor_column is None
    assert not profile.is_incremental
    assert "No date_modified" in profile.rationale


# ----------------------------------------------------------------- link tables


def test_link_table_keys_on_its_foreign_key_pair_not_the_surrogate():
    """Keying on `id` would re-version the whole table if upstream renumbers."""
    profile = profile_from_columns("search_opinioncluster_panel", LINK_COLUMNS)

    assert profile.entity_key == ["opinioncluster_id", "person_id"]
    assert profile.cursor_column is None
    assert "renumber" in profile.rationale


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        (LINK_COLUMNS, True),
        # A payload column means it carries its own data, not just an edge.
        (["id", "citing_opinion_id", "cited_opinion_id", "depth"], False),
        # Timestamps mean it has history of its own.
        (["id", "a_id", "b_id", "date_modified"], False),
        # One foreign key is an entity table with a parent.
        (["id", "docket_id"], False),
        # Three is not a pair.
        (["id", "a_id", "b_id", "c_id"], False),
        # No surrogate at all.
        (["a_id", "b_id"], False),
    ],
)
def test_link_table_shape_is_recognised_narrowly(columns, expected):
    assert looks_like_link_table(columns) is expected


# -------------------------------------------------------------------- registry


def test_registry_entry_wins_over_shape_derivation():
    """citation-map has a payload column, so only the registry knows its key."""
    columns = ["id", "citing_opinion_id", "cited_opinion_id", "depth"]
    profile = profile_from_columns("citation-map", columns)

    assert profile.entity_key == ["citing_opinion_id", "cited_opinion_id"]
    assert profile.cursor_column is None


def test_clusters_records_the_endpoint_prefix_mismatch():
    assert RESOURCES["clusters"].bulk_file_prefix == "opinion-clusters"
    assert RESOURCES["clusters"].entity_key == ["id"]


@pytest.mark.parametrize("resource", sorted(RESOURCES))
def test_every_registry_entry_explains_itself(resource):
    """A registry entry overrides derivation, so it has to say why."""
    assert RESOURCES[resource].rationale


# ----------------------------------------------------------- undecidable cases


def test_a_resource_with_no_id_defers_to_the_caller():
    profile = profile_from_columns("mystery", ["volume", "reporter", "page"])

    assert profile.entity_key is None
    assert "pass entity_key explicitly" in profile.rationale


# ------------------------------------------------------------------ suggestion


def test_suggestion_renders_spec_kwargs():
    suggestion = ProfileSuggestion(
        resource="clusters",
        profile=RESOURCES["clusters"],
        columns=ENTITY_COLUMNS,
    )
    assert suggestion.spec_kwargs() == {
        "entity_key": ["id"],
        "cursor_column": "date_modified",
        "bulk_file_prefix": "opinion-clusters",
    }


def test_suggestion_omits_unset_overrides():
    suggestion = ProfileSuggestion(
        resource="dockets",
        profile=profile_from_columns("dockets", ENTITY_COLUMNS),
        columns=ENTITY_COLUMNS,
    )
    assert suggestion.spec_kwargs() == {"entity_key": ["id"], "cursor_column": "date_modified"}


def test_suggestion_str_is_reviewable():
    text = str(
        ProfileSuggestion(
            resource="dockets",
            profile=profile_from_columns("dockets", ENTITY_COLUMNS),
            columns=ENTITY_COLUMNS,
        )
    )
    assert "dockets" in text
    assert "entity_key=['id']" in text
