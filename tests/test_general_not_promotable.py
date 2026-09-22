"""General is a placeholder, not a division: it is available everywhere a division can be set, and
the single guard is that it can never be promoted into the curated set."""

from sde_curation.models import (
    CURATION_DIVISIONS,
    CollectionCreate,
    Division,
    MetadataSuggestion,
    PatternCreate,
)
from tests.conftest import wait_job


def test_general_is_a_value_everywhere_but_not_a_curation_division():
    assert Division.GENERAL not in CURATION_DIVISIONS and len(CURATION_DIVISIONS) == 5
    # nothing refuses it on the way in: it is the default, and a legitimate value for any rule
    assert CollectionCreate(seed_url="https://x.org", name="X").division is Division.GENERAL
    assert CollectionCreate(seed_url="https://x.org", name="X", division="General").division is Division.GENERAL
    PatternCreate(type="division", match="*", value="General")
    # the one place it is absent: the model's answer schema — a suggestion of General could only
    # ever produce a row that cannot be promoted
    assert "General" not in str(MetadataSuggestion.model_json_schema())


async def setup(c, division=None, n=6):
    body = {"seed_url": "https://ex.org", "name": "Ex", "max_pages": n}
    if division:
        body["division"] = division
    await c.post("/api/collections", json=body)
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")


async def test_general_is_offered_wherever_a_division_can_be_set(crawler_client):
    c = crawler_client
    await setup(c)
    # the add-collection form, the collection's own division, and the per-URL cells all list it
    for where in ("/", "/collections/ex.org?tab=overview", "/collections/ex.org?tab=delta"):
        assert ">General" in (await c.get(where)).text, where
    # as does the add-a-rule-by-hand value list under Curate
    assert 'value="General">' in (await c.get("/collections/ex.org?tab=curate")).text
    # and every write path takes it
    for path, body in [
        ("/api/collections/ex.org/division", {"division": "General"}),
        ("/api/collections/ex.org/patterns", {"type": "division", "match": "*/p2", "value": "General"}),
        ("/api/collections/ex.org/urls", {"url": "https://ex.org/p3", "type": "division", "value": "General"}),
    ]:
        r = await c.post(path, json=body)
        assert r.status_code in (200, 201), (path, r.status_code, r.text)


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
