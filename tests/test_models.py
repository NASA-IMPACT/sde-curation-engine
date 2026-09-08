import pytest
from pydantic import ValidationError

from sde_curation.models import (
    Collection,
    CollectionCreate,
    Division,
    PatternCreate,
    PatternType,
    Status,
    check_transition,
    collection_id_from_seed,
)


def test_seed_normalised_and_id_derived():
    c = CollectionCreate(seed_url="www.aurorasaurus.org/path", name="Aurorasaurus")
    assert c.seed_url == "https://www.aurorasaurus.org/path"
    assert c.collection_id == "aurorasaurus.org"


@pytest.mark.parametrize("bad", ["", "ftp://x.org", "mailto:a@b", "https://"])
def test_bad_seed_rejected(bad):
    with pytest.raises(ValidationError):
        CollectionCreate(seed_url=bad, name="x")


def test_collection_id_matches_crawler_rule():
    assert collection_id_from_seed("https://science.nasa.gov/") == "science.nasa.gov"


def test_illegal_transition_rejected():
    with pytest.raises(ValueError):
        check_transition(Status.BACKLOG, Status.LIVE)
    check_transition(Status.BACKLOG, Status.SCRAPED)
    check_transition(Status.CONFIG_GENERATED, Status.CURATING)  # validation failure back-edge


def test_field_pattern_requires_valid_value():
    with pytest.raises(ValidationError):
        PatternCreate(type=PatternType.DIVISION, match="*/helio/*")
    with pytest.raises(ValidationError):
        PatternCreate(type=PatternType.DIVISION, match="*/helio/*", value="Nope")
    p = PatternCreate(type=PatternType.DIVISION, match="*/helio/*", value=Division.HELIOPHYSICS)
    assert p.value == "Heliophysics"
    PatternCreate(type=PatternType.EXCLUDE, match="*/legacy/*")


def test_collection_defaults():
    c = Collection(
        collection_id="x", name="x", seed_url="https://x.org", division=Division.GENERAL,
        connector="crawler2", max_pages=10,
    )
    assert c.status is Status.BACKLOG and c.needs_recuration is False
