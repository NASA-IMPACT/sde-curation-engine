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
    assert (await c.get("/api/collections/ex.org/delta?q=p9")).json()["total"] == 0  # the rule decides it: no delta
    assert (await c.get("/api/collections/ex.org/dump?q=p9")).json()["items"][0]["excluded"] is True
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
    items = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    with_title = [d for d in items if d["title_ai"]]
    with_doc = [d for d in items if d["document_type_ai"]]
    assert len(with_title) == 8 and with_doc
    # one URL has a title the SME set by hand after the metadata was generated: accept all leaves
    # that row alone (it would overwrite the rule), and says so; the row keeps its suggestion
    await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "title", "value": "Manual"})
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept", "field": "title"})
    assert r.status_code == 200 and r.json()["decided"] == 7
    items = {d["url"]: d for d in (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]}
    assert items["https://ex.org/p2"]["title_ai"] and items["https://ex.org/p2"]["title"] == "Manual"
    assert all(d["title_ai"] is None for u, d in items.items() if u != "https://ex.org/p2")
    assert all(d["title"] == d["scraped_title"] for u, d in items.items()
               if d["kind"] != "deleted" and u != "https://ex.org/p2")  # fake AI titles = scraped titles
    pats = (await c.get("/api/collections/ex.org/patterns")).json()
    titles = [p for p in pats if p["type"] == "title"]
    assert len(titles) == 8 and all(p["created_by"] == "anonymous" for p in titles)
    assert [p["source"] for p in titles if p["match"] == "https://ex.org/p2"] == ["sme"]  # the rule stands
    # the row's own ✓ is never disabled: it still takes the AI's answer for that row
    r = await c.post("/api/collections/ex.org/ai/bulk",
                     json={"decision": "accept", "url": "https://ex.org/p2", "field": "title"})
    assert r.status_code == 200
    items = {d["url"]: d for d in (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]}
    assert items["https://ex.org/p2"]["title_ai"] is None
    assert items["https://ex.org/p2"]["title"] == items["https://ex.org/p2"]["scraped_title"]
    # doc types: reject all → cleared, nothing applied
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "reject", "field": "document_type"})
    assert r.status_code == 200 and r.json()["decided"] == len(with_doc)
    items = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    assert all(d["document_type_ai"] is None and d["document_type"] is None for d in items)
    assert len((await c.get("/api/collections/ex.org/patterns")).json()) == 8
    assert (await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept", "field": "bogus"})).status_code == 422
    # every row got a division suggestion too (a guess at low confidence when nothing says)
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "reject", "field": "division"})
    assert r.status_code == 200 and r.json()["decided"] == 8
    # the workspace shows the bulk bar only while something is pending
    assert "accept all" not in (await c.get("/collections/ex.org?tab=curate")).text


async def test_sme_rules_added_after_metadata_are_kept_out_of_accept_all(crawler_client):
    """A glob rule the SME writes once the suggestions are in is applied at once, and the rows it
    decides drop out of the accept-all buttons — accepting them in bulk would write a newer
    exact-URL rule over the rule just typed. Each row's own ✓ still accepts, and says so."""
    c = crawler_client
    await setup(c)
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    r = await c.post("/api/collections/ex.org/patterns",
                     json={"type": "division", "match": "*/p1", "value": "Earth Science"})
    assert r.status_code == 201
    items = {d["url"]: d for d in (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]}
    assert items["https://ex.org/p1"]["division"] == "Earth Science"  # the rule is applied
    assert items["https://ex.org/p1"]["division_ai"]  # …and the suggestion is still there to review

    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "not in Accept all" in page and "1 on your own rules" in page
    assert 'hx-confirm="Apply 7 AI divisions' in page  # 8 pending, 1 held back

    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept", "field": "division"})
    assert r.status_code == 200 and r.json()["decided"] == 7
    items = {d["url"]: d for d in (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]}
    assert items["https://ex.org/p1"]["division_ai"] and items["https://ex.org/p1"]["division"] == "Earth Science"
    assert all(d["division_ai"] is None for u, d in items.items() if u != "https://ex.org/p1")
    # accept-all has nothing left to do and explains why; the row's own ✓ still works
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept", "field": "division"})
    assert r.status_code == 409 and "your own rules decide" in r.text
    r = await c.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p1", "field": "division"})
    assert r.status_code == 200
    items = {d["url"]: d for d in (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]}
    assert items["https://ex.org/p1"]["division_ai"] is None
    # a reject decides exactly what it says, held back or not
    await c.post("/api/collections/ex.org/patterns",
                 json={"type": "title", "match": "*/p*", "value": "Everything"})
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "reject", "field": "title"})
    assert r.status_code == 200 and r.json()["decided"] == 8


async def test_curate_workspace_counts_and_gate(crawler_client):
    c = crawler_client
    await setup(c)
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "Suggest exclusions" in page and "(8 delta URLs · 1 call)" in page
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
        assert "(56 delta URLs · 2 calls)" in (await c.get("/collections/ex.org?tab=curate")).text
        await c.post("/api/collections/ex.org/suggest/patterns")
        job = await wait_job(c, "ex.org", timeout=30)
        p = job["progress"]
        assert p["calls"] == 2 and p["done"] == 2 and p["urls"] == 56 and p["candidates"] == 56 and p["failed"] == 0
        sugs = (await c.get("/api/collections/ex.org/suggestions")).json()
        assert len(sugs) == 2 and {s["type"] for s in sugs} == {"exclude"}  # one exact exclude per batch
        assert "56 URLs in 2 calls" in (await c.get("/collections/ex.org?tab=curate")).text


async def test_curate_lists_expand_to_their_own_paginated_page(crawler_client):
    c = crawler_client
    await setup(c)
    db = c.app.state.db
    await db.add_pattern_suggestions("ex.org", [{"type": "exclude", "match": f"*/s{i:02d}*", "matches": 0} for i in range(60)])
    await db.set_delta_ai("ex.org", [{"url": f"https://ex.org/p{i}", "title": f"AI {i}"} for i in (1, 2, 3)])
    # collapsed: both lists in place, capped, with Expand; promote is on the page
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert page.count("⤢ Expand</a>") == 3  # two headers + the "showing the first 50 of 60" note
    assert "Showing the first 50 of 60 suggestions" in page and page.count("/suggestions/") >= 50 * 2
    assert 'id="promote"' in page and "⤡ Collapse" not in page
    # expanded exclusions: only that card, no scroll box, paginated, the other cards gone
    page = (await c.get("/collections/ex.org?tab=curate&focus=exclusions&per=25&page=2")).text
    assert 'id="exclusions"' in page and 'id="metadata"' not in page and 'id="promote"' not in page
    assert "⤡ Collapse" in page and 'href="/collections/ex.org?tab=curate#exclusions"' in page
    assert "26–50 of 60" in page and "page 2 / 3" in page and 'class="sugg-page"' in page
    assert "*/s25*" in page and "*/s24*" not in page and "*/s50*" not in page
    assert "focus=exclusions&per=25&page=3" in page
    # past the end (rows were decided meanwhile): the last page
    page = (await c.get("/collections/ex.org?tab=curate&focus=exclusions&per=25&page=9")).text
    assert "51–60 of 60" in page and "page 3 / 3" in page
    # expanded metadata
    page = (await c.get("/collections/ex.org?tab=curate&focus=metadata")).text
    assert 'id="metadata"' in page and 'id="exclusions"' not in page and "1–3 of 3" in page and "AI: AI 2" in page
    # an unknown focus is the whole page
    assert 'id="promote"' in (await c.get("/collections/ex.org?tab=curate&focus=bogus")).text
