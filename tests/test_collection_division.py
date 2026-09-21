"""The collection's division: General until a curator assigns one, editable at any time, and — once
assigned — applied to every URL and never asked of the model."""

from tests.conftest import wait_job


async def setup(c, division=None, n=6):
    body = {"seed_url": "https://ex.org", "name": "Ex", "max_pages": n}
    if division is not None:
        body["division"] = division
    r = await c.post("/api/collections", json=body)
    assert r.status_code == 201, r.text
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")
    return r.json()


async def deltas(c):
    return {d["url"]: d for d in (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]}


async def test_division_defaults_to_general_and_the_ai_is_asked(crawler_client):
    c = crawler_client
    assert (await setup(c))["division"] == "General"  # nothing given → the placeholder
    # the create form posts General by default, and explicitly choosing it is the same thing
    assert (await c.post("/collections", data={"seed_url": "https://two.org", "name": "Two", "division": "General"})
            ).status_code in (303, 200)
    assert (await c.get("/api/collections/two.org")).json()["division"] == "General"
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    rows = await deltas(c)
    assert all(d["division_ai"] and d["division_ai_conf"] for d in rows.values())
    assert all(d["division"] is None for d in rows.values())  # a suggestion, never an effective value
    assert all(not d["division_skipped"] for d in rows.values())


async def test_a_division_given_at_creation_fills_every_url_and_is_never_suggested(crawler_client):
    c = crawler_client
    assert (await setup(c, "Heliophysics"))["division"] == "Heliophysics"
    rows = await deltas(c)
    assert rows and all(d["division"] == "Heliophysics" for d in rows.values())
    assert all(d["edited_by"] == "sme" for d in rows.values())  # the division is the curator's
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    rows = await deltas(c)
    assert all(d["division_ai"] is None and d["division_ai_conf"] is None for d in rows.values())
    assert all(d["title_ai"] and d["document_type_ai"] for d in rows.values())
    # nothing to decide for the division, and the workspace says why
    assert (await c.post("/api/collections/ex.org/ai/bulk",
                         json={"decision": "accept", "field": "division"})).status_code == 409
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "The division is yours" in page
    assert 'class="ptype title"' in page and 'class="ptype division"' not in page
    # and the metadata is complete without it: promote is not blocked on a blank division
    assert (await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept"})).status_code == 200
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200


async def test_editing_the_division_applies_it_to_urls_already_curated(crawler_client):
    c = crawler_client
    await setup(c, "Heliophysics")
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept"})
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200
    curated = (await c.get("/api/collections/ex.org/curated?limit=100")).json()["items"]
    assert curated and all(r["division"] == "Heliophysics" for r in curated)

    r = await c.post("/api/collections/ex.org/division", json={"division": "Earth Science"})
    assert r.status_code == 200 and r.json()["division"] == "Earth Science"
    # every curated row is behind the new division: one modified delta each, ready to promote
    rows = await deltas(c)
    assert len(rows) == len(curated) and all(d["kind"] == "modified" for d in rows.values())
    assert all(d["division"] == "Earth Science" for d in rows.values())
    assert (await c.get("/api/collections/ex.org")).json()["status"] == "curating"
    assert "Earth Science" in (await c.get("/collections/ex.org?tab=overview")).text
    assert "division" in (await c.get("/collections/ex.org?tab=activity")).text
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200
    curated = (await c.get("/api/collections/ex.org/curated?limit=100")).json()["items"]
    assert all(r["division"] == "Earth Science" for r in curated)


async def test_a_curator_can_move_between_any_two_divisions(crawler_client):
    """No division is off limits when editing the collection: the curator picks any of them."""
    c = crawler_client
    await setup(c)
    for d in ("Astrophysics", "Biological and Physical Sciences", "Earth Science",
              "Heliophysics", "Planetary Science", "General"):
        r = await c.post("/api/collections/ex.org/division", json={"division": d})
        assert r.status_code == 200 and r.json()["division"] == d, (d, r.text)
        rows = await deltas(c)
        assert all(x["division"] == (d if d != "General" else None) for x in rows.values()), d


async def test_setting_a_division_clears_suggestions_an_earlier_run_left(crawler_client):
    """A collection classified before a division was assigned carries division suggestions.
    Assigning one makes them moot: there is nothing to review, and accept-all must not be able to
    write one over the division the curator just chose."""
    c = crawler_client
    await setup(c)
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    assert all(d["division_ai"] for d in (await deltas(c)).values())
    await c.post("/api/collections/ex.org/division", json={"division": "Heliophysics"})
    rows = await deltas(c)
    assert all(d["division_ai"] is None and d["division"] == "Heliophysics" for d in rows.values())
    assert (await c.post("/api/collections/ex.org/ai/bulk",
                         json={"decision": "accept", "field": "division"})).status_code == 409
    assert 'class="ptype division"' not in (await c.get("/collections/ex.org?tab=curate")).text
    # the titles and doc types that run produced are untouched
    assert all(d["title_ai"] and d["document_type_ai"] for d in rows.values())


async def test_going_back_to_general_hands_the_division_back_to_the_ai(crawler_client):
    c = crawler_client
    await setup(c, "Heliophysics")
    r = await c.post("/api/collections/ex.org/division", json={"division": "General"})
    assert r.status_code == 200 and r.json()["division"] == "General"
    assert all(d["division"] is None for d in (await deltas(c)).values())
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    assert all(d["division_ai"] for d in (await deltas(c)).values())


async def test_going_back_to_general_after_classifying_asks_for_divisions_alone(crawler_client):
    """Rows classified while a division was assigned were never asked for one. Putting the
    collection back to General leaves them owing a division, so Suggest metadata offers exactly
    those rows — without re-classifying every field of every page, which "redo all" would cost."""
    c = crawler_client
    await setup(c, "Heliophysics")
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    rows = await deltas(c)
    assert all(d["division_skipped"] and not d["division_ai"] for d in rows.values())
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "Suggest metadata <small>(0 URL" in page  # everything is classified

    await c.post("/api/collections/ex.org/division", json={"division": "General"})
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert f"Suggest metadata <small>({len(rows)} URL" in page and "redo all" not in page
    titles = {u: d["title_ai"] for u, d in (await deltas(c)).items()}
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    after = await deltas(c)
    assert all(d["division_ai"] and not d["division_skipped"] for d in after.values())
    assert {u: d["title_ai"] for u, d in after.items()} == titles  # the titles came back the same


async def test_a_division_rule_still_overrides_the_collection_division(crawler_client):
    c = crawler_client
    await setup(c, "Heliophysics")
    await c.post("/api/collections/ex.org/urls",
                 json={"url": "https://ex.org/p2", "type": "division", "value": "Planetary Science"})
    rows = await deltas(c)
    assert rows["https://ex.org/p2"]["division"] == "Planetary Science"
    assert rows["https://ex.org/p3"]["division"] == "Heliophysics"
    # changing the collection division is the newer decision and reaches the exception too
    await c.post("/api/collections/ex.org/division", json={"division": "Earth Science"})
    rows = await deltas(c)
    assert rows["https://ex.org/p2"]["division"] == "Planetary Science"  # the rule still decides it
    assert rows["https://ex.org/p3"]["division"] == "Earth Science"
