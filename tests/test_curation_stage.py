"""Curation sub-stage (scope → metadata) under the `curating` status, and the homepage facets."""

from tests.conftest import wait_job


async def setup(c, n=10):
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": n})
    await c.post("/api/collections/ex.org/scrape")
    await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")


async def coll(c, cid="ex.org"):
    return (await c.get(f"/api/collections/{cid}")).json()


async def test_stage_lifecycle(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    assert (await coll(c))["curation_stage"] is None
    # stages only exist while curating
    assert (await c.post("/api/collections/ex.org/stage", json={"stage": "metadata"})).status_code == 409
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")
    assert (await coll(c))["curation_stage"] == "scope"
    # metadata is gated on pending pattern suggestions
    await c.post("/api/collections/ex.org/suggest/patterns"); await wait_job(c, "ex.org")
    r = await c.post("/api/collections/ex.org/stage", json={"stage": "metadata"})
    assert r.status_code == 409 and "pending" in r.text
    for s in (await c.get("/api/collections/ex.org/suggestions")).json():
        await c.post(f"/api/collections/ex.org/suggestions/{s['id']}/reject")
    assert (await c.post("/api/collections/ex.org/stage", json={"stage": "metadata"})).status_code == 200
    assert (await coll(c))["curation_stage"] == "metadata"
    assert (await c.post("/api/collections/ex.org/stage", json={"stage": "scope"})).status_code == 200
    assert (await c.post("/api/collections/ex.org/stage", json={"stage": "bogus"})).status_code == 422
    # starting metadata suggestions moves to the metadata stage
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    assert (await coll(c))["curation_stage"] == "metadata"
    # visible in header / dashboard / yaml
    assert "② Metadata" in (await c.get("/collections/ex.org/header")).text
    assert "② Metadata" in (await c.get("/collections/ex.org/row")).text
    assert "curation_stage: metadata" in (c.app.state.settings.collections_dir / "ex.org" / "collection.yaml").read_text()
    # a recompute while curating keeps the stage; promote clears it
    await c.post("/api/collections/ex.org/recompute")
    assert (await coll(c))["curation_stage"] == "metadata"
    await c.post("/api/collections/ex.org/promote")
    cc = await coll(c)
    assert (cc["status"], cc["curation_stage"]) == ("curated", None)
    # re-entering curating starts over at scope; re-scrape clears it again
    await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p3"})
    assert (await coll(c))["curation_stage"] == "scope"
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    cc = await coll(c)
    assert (cc["status"], cc["curation_stage"]) == ("scraped", None)
    # manual override into curating also gets a stage
    await c.post("/api/collections/ex.org/status", json={"status": "curating", "force": True})
    assert (await coll(c))["curation_stage"] == "scope"


async def test_last_scraped_at_recorded(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    assert (await coll(c))["last_scraped_at"] is None
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    assert (await coll(c))["last_scraped_at"] is not None
    assert "Last crawl" in (await c.get("/collections/ex.org/step/scraped")).text


async def test_dashboard_stage_and_flag_facets(crawler_client):
    c = crawler_client
    await setup(c)  # ex.org: curating / scope
    await c.post("/api/collections", json={"seed_url": "https://b.org", "name": "Bee", "max_pages": 10})
    await c.post("/api/collections/b.org/scrape"); await wait_job(c, "b.org")
    await c.post("/api/collections/b.org/recompute")
    await c.post("/api/collections/b.org/stage", json={"stage": "metadata"})
    await c.post("/api/collections", json={"seed_url": "https://c.org", "name": "Cee", "max_pages": 10})
    await c.app.state.db.set_flag("c.org", True)

    home = (await c.get("/")).text
    assert 'name="stage" value="scope"' in home and 'name="flag" value="needs_recuration"' in home
    assert '<span id="f-stage-scope" class="cnt">1</span>' in home
    assert '<span id="f-stage-metadata" class="cnt">1</span>' in home
    assert '<span id="f-flag-needs_recuration" class="cnt">1</span>' in home

    rows = (await c.get("/rows?stage=scope")).text
    assert "Ex" in rows and "Bee" not in rows and "Cee" not in rows
    rows = (await c.get("/rows?stage=metadata")).text
    assert "Bee" in rows and "Ex" not in rows
    rows = (await c.get("/rows?status=backlog&stage=metadata")).text  # OR within the status facet
    assert "Bee" in rows and "Cee" in rows and "Ex" not in rows
    rows = (await c.get("/rows?flag=needs_recuration")).text  # AND with the rest
    assert "Cee" in rows and "Ex" not in rows and "Bee" not in rows
    assert "0 of 3" in (await c.get("/rows?flag=needs_recuration&stage=scope")).text
