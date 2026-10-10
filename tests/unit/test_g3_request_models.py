"""What the create and rename forms accept (sde_curation/models.py), before any route runs. The
routes answer a refused value with 422. Replaces input checks of the old
tests/integration/test_api_collections.py and test_collection_rename.py (P4,
TEST-STRATEGY-2026-10-09.md)."""

import pytest
from pydantic import ValidationError

from sde_curation.models import CollectionCreate, Division, DivisionUpdate, NameUpdate

SEED = "https://x.org"
LONGEST_NAME = 200


@pytest.mark.parametrize("bad_id", ["..", ".", "a/b"], ids=["parent directory", "this directory", "a path"])
def test_a_collection_id_that_names_a_directory_or_a_path_is_refused(bad_id):
    """The id names the collection's folder: ".." would write its YAML into the parent folder."""
    with pytest.raises(ValidationError):
        CollectionCreate(seed_url=SEED, name="x", collection_id=bad_id)


@pytest.mark.parametrize("good_id", ["a.org", "a..b", "_"], ids=["a host", "dots inside", "one non-dot character"])
def test_a_collection_id_with_one_character_that_is_not_a_dot_is_kept(good_id):
    assert CollectionCreate(seed_url=SEED, name="x", collection_id=good_id).collection_id == good_id


def test_a_new_name_is_stored_without_its_surrounding_spaces():
    assert NameUpdate(name="  NASA Applied Sciences ").name == "NASA Applied Sciences"


@pytest.mark.parametrize("bad", ["   ", "", "x" * (LONGEST_NAME + 1)], ids=["blank", "empty", "one too long"])
def test_a_blank_or_too_long_name_is_refused(bad):
    with pytest.raises(ValidationError):
        NameUpdate(name=bad)


def test_the_longest_name_is_accepted():
    assert len(NameUpdate(name="x" * LONGEST_NAME).name) == LONGEST_NAME


@pytest.mark.parametrize("division", list(Division), ids=[d.value for d in Division])
def test_a_curator_can_move_the_collection_to_any_division_general_included(division):
    """No division is off limits when editing the collection; General means "not assigned"."""
    assert DivisionUpdate(division=division.value).division is division
