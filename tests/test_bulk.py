"""Bulk accept / reject of pattern suggestions (per type) and AI metadata suggestions (per field)."""

from tests.conftest import wait_job


async def setup(c, n=10):
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": n})
    await c.post("/api/collections/ex.org/scrape")
    await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")


async def test_suggestions_bulk_by_type_and_all(crawler_client):
    c = crawler_client
    await setup(c)
    assert (await c.post("/api/collections/ex.org/suggestions/bulk", json={"decision": "accept"})).status_code == 409
    await c.post("/api/collections/ex.org/suggest/patterns"); await wait_job(c, "ex.org")
    sugs = (await c.get("/api/collections/ex.org/suggestions")).json()
    assert len(sugs) == 1 and sugs[0]["type"] == "exclude"  # the fake excludes the last URL of the batch
    audit_before = len((await c.get("/collections/ex.org?tab=activity")).text.split("suggestion.bulk_"))
    # accept excludes → one pattern, one recompute
    r = await c.post("/api/collections/ex.org/suggestions/bulk", json={"decision": "accept", "type": "exclude"})
    assert r.status_code == 200 and r.json()["decided"] == 1
    pats = (await c.get("/api/collections/ex.org/patterns")).json()
    assert [p["type"] for p in pats] == ["exclude"]
    assert (await c.get("/api/collections/ex.org/deltas?q=p9")).json()["items"][0]["excluded"] is True
    left = (await c.get("/api/collections/ex.org/suggestions")).json()
    assert left == []
    assert (await c.post("/api/collections/ex.org/suggestions/bulk", json={"decision": "accept", "type": "exclude"})).status_code == 409
    # reject everything else → nothing more applied, none pending
    if left:
        r = await c.post("/api/collections/ex.org/suggestions/bulk", json={"decision": "reject"})
        assert r.status_code == 200 and r.json()["decided"] == len(left)
    assert (await c.get("/api/collections/ex.org/suggestions")).json() == []
    assert len((await c.get("/api/collections/ex.org/patterns")).json()) == 1
    rejected = (await c.get("/api/collections/ex.org/suggestions?state=rejected")).json()
    assert len(rejected) == len(left) and all(s["decided_by"] == "anonymous" for s in rejected)
    assert len((await c.get("/collections/ex.org?tab=activity")).text.split("suggestion.bulk_")) > audit_before
    assert (await c.post("/api/collections/ex.org/suggestions/bulk", json={"decision": "maybe"})).status_code == 422


async def test_bulk_accept_skips_duplicates(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post("/api/collections/ex.org/suggest/patterns"); await wait_job(c, "ex.org")
    # the same rule already exists by hand → accept-all must not fail, and must not duplicate it
    await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "https://ex.org/p9"})
    r = await c.post("/api/collections/ex.org/suggestions/bulk", json={"decision": "accept"})
    assert r.status_code == 200
    pats = (await c.get("/api/collections/ex.org/patterns")).json()
    assert len([p for p in pats if p["type"] == "exclude"]) == 1
    assert (await c.get("/api/collections/ex.org/suggestions")).json() == []


async def test_ai_bulk_accept_and_reject(crawler_client):
    c = crawler_client
    await setup(c)
    assert (await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept", "field": "title"})).status_code == 409
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    items = (await c.get("/api/collections/ex.org/deltas?limit=100")).json()["items"]
    with_title = [d for d in items if d["title_ai"]]
    with_doc = [d for d in items if d["document_type_ai"]]
    assert len(with_title) == 8 and with_doc
    # one URL already has a hand-set exact title: bulk accept replaces it, not duplicates it
    await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "title", "value": "Manual"})
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept", "field": "title"})
    assert r.status_code == 200 and r.json()["decided"] == 8
    items = (await c.get("/api/collections/ex.org/deltas?limit=100")).json()["items"]
    assert all(d["title_ai"] is None for d in items)
    assert all(d["title"] == d["scraped_title"] for d in items if d["kind"] != "deleted")  # fake AI titles = scraped titles
    pats = (await c.get("/api/collections/ex.org/patterns")).json()
    titles = [p for p in pats if p["type"] == "title"]
    assert len(titles) == 8 and all(p["created_by"] == "anonymous" for p in titles)
    # doc types: reject all → cleared, nothing applied
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "reject", "field": "document_type"})
    assert r.status_code == 200 and r.json()["decided"] == len(with_doc)
    items = (await c.get("/api/collections/ex.org/deltas?limit=100")).json()["items"]
    assert all(d["document_type_ai"] is None and d["document_type"] is None for d in items)
    assert len((await c.get("/api/collections/ex.org/patterns")).json()) == 8
    assert (await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept", "field": "bogus"})).status_code == 422
    # the workspace shows the bulk bar only while something is pending
    assert "accept all" not in (await c.get("/collections/ex.org?tab=curate")).text


async def test_curate_workspace_counts_and_gate(crawler_client):
    c = crawler_client
    await setup(c)
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "Suggest exclusions" in page and "(all 8 URLs · 1 call)" in page
    assert "Suggest metadata" in page and "(8 URLs)" in page and "Tip: run" in page
    await c.post("/api/collections/ex.org/suggest/patterns"); await wait_job(c, "ex.org")
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "Decide the" in page and "pending suggestion" in page  # metadata button gated with a reason
    r = await c.post("/api/collections/ex.org/suggest/metadata")
    assert r.status_code == 409 and "pending" in r.text
    await c.post("/api/collections/ex.org/suggestions/bulk", json={"decision": "reject"})
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "Continue to metadata" in page and "Decide the" not in page
    assert (await c.get("/collections/ex.org/rules")).status_code == 200


async def test_pattern_batch_setting(tmp_path):
    from httpx import ASGITransport, AsyncClient

    from tests.conftest import _crawler_app

    app = _crawler_app(tmp_path, llm_pattern_batch_urls=50)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        c.app = app
        await setup(c, n=70)  # 56 docs → 2 calls of ≤ 50
        assert "(all 56 URLs · 2 calls)" in (await c.get("/collections/ex.org?tab=curate")).text
        await c.post("/api/collections/ex.org/suggest/patterns")
        job = await wait_job(c, "ex.org", timeout=30)
        p = job["progress"]
        assert p["calls"] == 2 and p["done"] == 2 and p["urls"] == 56 and p["failed"] == 0
        sugs = (await c.get("/api/collections/ex.org/suggestions")).json()
        assert len(sugs) == 2 and {s["type"] for s in sugs} == {"exclude"}  # one exact exclude per batch
        assert "56 URLs in 2 calls" in (await c.get("/collections/ex.org?tab=curate")).text
