"""General is the placeholder a collection carries until a curator assigns a division. It is a
choice for the collection's own division and nowhere else — not in the per-URL cells, not as a
metadata rule value, not in the model's answer schema — and the guard that matters is that it can
never be promoted into the curated set."""
import re

from sde_curation.models import (
    CURATION_DIVISIONS,
    CollectionCreate,
    Division,
    MetadataSuggestion,
    PatternCreate,
)


def test_general_is_accepted_on_the_way_in_but_is_not_a_curation_division():
    assert Division.GENERAL not in CURATION_DIVISIONS and len(CURATION_DIVISIONS) == 5
    # nothing refuses it on the way in: it is the default, and a value any write path still takes
    assert CollectionCreate(seed_url="https://x.org", name="X").division is Division.GENERAL
    assert CollectionCreate(seed_url="https://x.org", name="X", division="General").division is Division.GENERAL
    PatternCreate(type="division", match="*", value="General")
    # the model is never given it: a suggestion of General could only produce a row that cannot be
    # promoted
    assert "General" not in str(MetadataSuggestion.model_json_schema())


# every <select> a URL's own division is set through: the cells in the URL tables and under
# Curate › Metadata. The filter selects above the table are not one of them.
CELL_SELECT = re.compile(r'<select class="cell\b.*?</select>', re.DOTALL)
