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
from tests.conftest import wait_job


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


async def setup(c, division=None, n=6):
    body = {"seed_url": "https://ex.org", "name": "Ex", "max_pages": n}
    if division:
        body["division"] = division
    await c.post("/api/collections", json=body)
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")


async def test_general_is_a_choice_for_the_collection_and_is_posted_as_a_division(crawler_client):
    """The add-collection form and Overview › Details both offer it. The option carries its own
    value: the label reads "General — not assigned", and without a value the browser posts the
    label, which is not a division."""
    c = crawler_client
    await setup(c)
    for where in ("/", "/collections/ex.org?tab=overview"):
        page = (await c.get(where)).text
        sel = re.search(r'<select[^>]*name="division".*?</select>', page, re.DOTALL)
        assert sel, where
        opts = re.findall(r"<option[^>]*>", sel.group(0))
        assert any('value="General"' in o for o in opts), where
        assert all("value=" in o for o in opts), where  # a label is never what gets posted
    # and the form really takes it, with the label the option shows
    r = await c.post("/collections", data={"seed_url": "https://two.org", "name": "Two",
                                           "division": "General", "max_pages": 3})
    assert r.status_code in (200, 303), r.text
    assert (await c.get("/api/collections/two.org")).json()["division"] == "General"


async def test_general_is_not_a_choice_during_curation(crawler_client):
    """A URL is curated into one of the five: the per-URL cells and the metadata rule values leave
    the placeholder out, and so does the model."""
    c = crawler_client
    await setup(c)
    # suggestions pending, so the review table under Curate › Metadata has rows to show
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    for where in ("/collections/ex.org?tab=delta", "/collections/ex.org?tab=curate"):
        page = (await c.get(where)).text
        cells = CELL_SELECT.findall(page)
        assert cells, where
        assert any("Astrophysics" in cell for cell in cells), where  # the division cells are there
        assert not any("General" in cell for cell in cells), where
    # nor the add-a-rule-by-hand value list under Curate, which offers the same five
    curate = (await c.get("/collections/ex.org?tab=curate")).text
    values = re.search(r'<datalist id="values">.*?</datalist>', curate, re.DOTALL)
    assert values and "General" not in values.group(0)
    assert "Planetary Science" in values.group(0)
    # the guard is promote, not the write paths: every one of them still takes it
    for path, body in [
        ("/api/collections/ex.org/division", {"division": "General"}),
        ("/api/collections/ex.org/patterns", {"type": "division", "match": "*/p2", "value": "General"}),
        ("/api/collections/ex.org/urls", {"url": "https://ex.org/p3", "type": "division", "value": "General"}),
    ]:
        r = await c.post(path, json=body)
        assert r.status_code in (200, 201), (path, r.status_code, r.text)
    # and a row left on it shows it rather than a blank cell, though it cannot be picked again
    cell = next(x for x in CELL_SELECT.findall((await c.get("/collections/ex.org?tab=delta")).text)
                if "General" in x)
    assert "<option selected>General</option>" in cell


async def test_general_can_never_be_promoted(crawler_client):
    """The only guard: a delta URL whose effective division is General is refused by promote, the
    same as a blank one, and the row says so until a real division is set."""
    c = crawler_client
    await setup(c, "Heliophysics")
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept"})
    # a curator puts one row back on the placeholder
    r = await c.post("/api/collections/ex.org/urls",
                     json={"url": "https://ex.org/p2", "type": "division", "value": "General"})
    assert r.status_code in (200, 201)
    rows = {d["url"]: d for d in (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]}
    assert rows["https://ex.org/p2"]["division"] == "General"
    assert await c.app.state.db.incomplete_counts("ex.org") == {
        "urls": 1, "title": 0, "division": 1, "general": 1, "document_type": 0, "duplicate": 0}

    r = await c.post("/api/collections/ex.org/promote")
    assert r.status_code == 409 and "General placeholder" in r.text
    page = (await c.get("/collections/ex.org?tab=delta&missing=true")).text
    assert "not promotable" in page and "https://ex.org/p2" in page
    assert "still on <b>General</b>" in (await c.get("/collections/ex.org?tab=curate")).text

    # a real division clears it
    await c.post("/api/collections/ex.org/urls",
                 json={"url": "https://ex.org/p2", "type": "division", "value": "Earth Science"})
    assert await c.app.state.db.incomplete_counts("ex.org") == {
        "urls": 0, "title": 0, "division": 0, "general": 0, "document_type": 0, "duplicate": 0}
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200
    curated = (await c.get("/api/collections/ex.org/curated?limit=100")).json()["items"]
    assert all(r["division"] != "General" for r in curated)


async def test_a_collection_left_on_general_still_promotes_via_the_ai(crawler_client):
    """The collection's own division staying General blocks nothing: the AI gives each page a real
    division, and only the accepted per-URL values reach the curated set."""
    c = crawler_client
    await setup(c)
    assert (await c.get("/api/collections/ex.org")).json()["division"] == "General"
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept"})
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200
    curated = (await c.get("/api/collections/ex.org/curated?limit=100")).json()["items"]
    assert curated and all(r["division"] and r["division"] != "General" for r in curated)
