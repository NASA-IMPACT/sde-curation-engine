"""2026-09-10 review round: Accept / Edit / Reject, AI-vs-SME provenance, delta-scoped exclusion
suggestions, per-URL edits on every table, re-curation reason, removal warning, rule scope."""

from sde_curation.models import DumpUrl
from tests.conftest import wait_job


async def setup(c, cid="ex.org", n=10):
    await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": n})
    await c.post(f"/api/collections/{cid}/scrape"); await wait_job(c, cid)
    await c.post(f"/api/collections/{cid}/recompute")


async def coll(c, cid="ex.org"):
    return (await c.get(f"/api/collections/{cid}")).json()


async def patterns(c, cid="ex.org"):
    return (await c.get(f"/api/collections/{cid}/patterns")).json()


async def delta(c, url, cid="ex.org"):
    return next(d for d in (await c.get(f"/api/collections/{cid}/deltas?limit=100")).json()["items"] if d["url"] == url)


# ── 1. accept / edit / reject ──────────────────────────────────────────


async def test_suggestion_edit_then_accept(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post("/api/collections/ex.org/suggest/patterns"); await wait_job(c, "ex.org")
    sug = (await c.get("/api/collections/ex.org/suggestions")).json()[0]  # the fake excludes the batch's last URL
    sid, match = sug["id"], sug["match"]
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert ">Accept<" in page and ">Edit<" in page and ">Reject<" in page
    # blank glob → 422, nothing decided
    assert (await c.post(f"/api/collections/ex.org/suggestions/{sid}/accept", json={"match": "   "})).status_code == 422
    assert (await c.get("/api/collections/ex.org/suggestions")).json()[0]["state"] == "pending"
    # edited glob → rule with the edited match, tagged llm_edited; suggestion records what was applied
    r = await c.post(f"/api/collections/ex.org/suggestions/{sid}/accept", json={"match": "https://ex.org/p*9"})
    assert r.status_code == 200 and r.json() == {"id": sid, "state": "accepted", "accepted_as": "https://ex.org/p*9"}
    pats = await patterns(c)
    assert [(p["match"], p["source"]) for p in pats] == [("https://ex.org/p*9", "llm_edited")]
    assert match not in {p["match"] for p in pats}
    decided = (await c.get("/api/collections/ex.org/suggestions?state=accepted")).json()
    assert decided[0]["accepted_as"] == "https://ex.org/p*9" and decided[0]["match"] == match
    assert (await c.post(f"/api/collections/ex.org/suggestions/{sid}/accept")).status_code == 409  # already decided
    # a second suggestion edited to an existing rule → 409, still pending
    await c.post("/api/collections/ex.org/suggest/patterns"); await wait_job(c, "ex.org")
    pend = (await c.get("/api/collections/ex.org/suggestions")).json()
    if pend:
        r = await c.post(f"/api/collections/ex.org/suggestions/{pend[0]['id']}/accept", json={"match": "https://ex.org/p*9"})
        assert r.status_code == 409 and pend[0]["id"] in {s["id"] for s in (await c.get("/api/collections/ex.org/suggestions")).json()}
    audit = (await c.get("/api/collections/ex.org/audit")).json()
    assert any(a["action"] == "suggestion.accept_edited" for a in audit)


async def test_plain_accept_keeps_source_llm_and_global(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post("/api/collections/ex.org/suggest/patterns"); await wait_job(c, "ex.org")
    sugs = (await c.get("/api/collections/ex.org/suggestions")).json()
    await c.post("/api/collections/ex.org/suggestions/bulk", json={"decision": "accept"})
    srcs = {p["match"]: p["source"] for p in await patterns(c)}
    for s in sugs:
        assert srcs[s["match"]] == ("global" if s["source"] == "global" else "llm")


async def test_ai_edit_then_accept(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    d = await delta(c, "https://ex.org/p2")
    assert d["title_ai"] == "Page 2"
    # same value as suggested → llm; a different value → llm_edited; both clear the badge
    r = await c.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p2", "field": "title", "value": "Page 2"})
    assert r.status_code == 200 and "value" not in r.json()
    r = await c.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p3", "field": "title", "value": "Three, by hand"})
    assert r.status_code == 200 and r.json()["value"] == "Three, by hand"
    d2, d3 = await delta(c, "https://ex.org/p2"), await delta(c, "https://ex.org/p3")
    assert d2["title"] == "Page 2" and d2["title_ai"] is None and d2["edited_by"] == "ai"
    assert d3["title"] == "Three, by hand" and d3["title_ai"] is None and d3["edited_by"] == "sme"
    srcs = {p["match"]: p["source"] for p in await patterns(c) if p["type"] == "title"}
    assert srcs == {"https://ex.org/p2": "llm", "https://ex.org/p3": "llm_edited"}
    # an invalid edited enum value → 422 and the badge stays
    r = await c.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p2", "field": "document_type", "value": "Nope"})
    assert r.status_code == 422 and (await delta(c, "https://ex.org/p2"))["document_type_ai"] == "Documentation"
    page = (await c.get("/collections/ex.org?tab=urls&set=delta&q=p2")).text
    assert "pickAi(" in page and "✎" in page
    actions = [a["action"] for a in (await c.get("/api/collections/ex.org/audit")).json()]
    assert "ai.accept_edited" in actions and "ai.accept" in actions


# ── 2. edited-by column and rule sources ──────────────────────────────


async def test_edited_by_survives_promote_and_filters(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept", "field": "title"})   # AI on every row
    await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "reject", "field": "division"})
    await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "reject", "field": "document_type"})
    await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "division", "value": "Earth Science"})  # SME on p2
    await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p3", "type": "exclude"})  # SME exclude on p3
    d2, d3, d4 = [await delta(c, f"https://ex.org/p{i}") for i in (2, 3, 4)]
    assert d2["edited_by"] == "mixed" and d3["edited_by"] == "mixed" and d4["edited_by"] == "ai"
    page = (await c.get("/collections/ex.org?tab=urls&set=delta&q=p2")).text
    assert 'class="edited e-mixed"' in page and "AI + SME" in page
    assert 'division */p2' not in page and "division https://ex.org/p2 → Earth Science (by anonymous)" in page  # SME tooltip: unchanged format
    assert "title https://ex.org/p4 → Page 4 (by anonymous) · AI" in (await c.get("/collections/ex.org?tab=urls&set=delta&q=p4")).text
    assert 'title="exclude https://ex.org/p3 (by anonymous)"' in (await c.get("/collections/ex.org?tab=urls&set=delta&q=p3")).text
    # filter + csv on deltas
    assert (await c.get("/api/collections/ex.org/delta?limit=100")).json()["total"] == 8
    only_ai = (await c.get("/collections/ex.org?tab=urls&set=delta&edited=ai")).text
    assert "https://ex.org/p4" in only_ai and "https://ex.org/p2" not in only_ai
    csv = (await c.get("/collections/ex.org/urls/delta?format=csv&edited=mixed")).text.splitlines()
    assert "edited_by" in csv[0] and len(csv) == 3
    # rules table: source column + counts
    rules = (await c.get("/collections/ex.org/rules")).text
    assert "AI 8" in rules and "SME 2" in rules and 'e-src-llm"' in rules and 'e-src-sme"' in rules and "this collection only" in rules
    # promote: edited_by lands on the curated rows and the tooltips (effects) survive
    await c.post("/api/collections/ex.org/promote")
    cur = {r["url"]: r for r in (await c.get("/api/collections/ex.org/curated?limit=100")).json()["items"]}
    assert cur["https://ex.org/p2"]["edited_by"] == "mixed" and cur["https://ex.org/p4"]["edited_by"] == "ai"
    page = (await c.get("/collections/ex.org?tab=urls&set=curated&q=p2")).text
    assert "AI + SME" in page and "division https://ex.org/p2 → Earth Science (by anonymous)" in page
    only_mixed = (await c.get("/collections/ex.org?tab=urls&set=curated&edited=mixed")).text
    assert "https://ex.org/p2" in only_mixed and "https://ex.org/p4" not in only_mixed
    csv = (await c.get("/collections/ex.org/urls/curated?format=csv&edited=ai")).text.splitlines()
    assert "edited_by" in csv[0] and len(csv) == 7
    # rows promoted without a value (or re-attributed rules) are fixed up by the next recompute, no delta needed
    db = c.app.state.db
    await db.execute("UPDATE curated_urls SET edited_by=NULL WHERE collection_id='ex.org'")
    r = await c.post("/api/collections/ex.org/recompute")
    assert r.json()["modified"] == 0 and (await coll(c))["status"] == "curated"
    cur = {r["url"]: r for r in (await c.get("/api/collections/ex.org/curated?limit=100")).json()["items"]}
    assert cur["https://ex.org/p2"]["edited_by"] == "mixed" and cur["https://ex.org/p4"]["edited_by"] == "ai"
    # a re-crawl that drops p4: the tombstone keeps its edited_by
    await db.replace_dump("ex.org", [DumpUrl(collection_id="ex.org", url=f"https://ex.org/p{i}", scraped_title=f"Page {i}")
                                    for i in (0, 1, 2, 3, 5, 6, 7)])
    await c.post("/api/collections/ex.org/recompute")
    assert (await delta(c, "https://ex.org/p4"))["kind"] == "deleted" and (await delta(c, "https://ex.org/p4"))["edited_by"] == "ai"


# ── 3. suggest exclusions only for delta URLs ────────────────────────


async def test_patterns_only_for_pending_after_promote(crawler_client):
    from sde_curation.llm.fake import FakeProvider

    c = crawler_client
    await setup(c)
    fake = FakeProvider()
    c.app.state.jobs._llm = fake
    await c.post("/api/collections/ex.org/promote")
    # nothing pending → the button is disabled with a reason and the API refuses
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "(0 delta URLs · 0 calls)" in page and "Start curating first" in page
    assert (await c.post("/api/collections/ex.org/suggest/patterns")).status_code == 409
    # re-crawl with two new URLs; excluded pending rows are not candidates either
    db = c.app.state.db
    dump = await db.load_dump("ex.org")
    dump += [DumpUrl(collection_id="ex.org", url=f"https://ex.org/new{i}", scraped_title=f"New {i}") for i in (1, 2)]
    await db.replace_dump("ex.org", dump)
    await c.post("/api/collections/ex.org/recompute")
    await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "https://ex.org/new2"})
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "(1 delta URL · 1 call)" in page and "Match counts are over all 10 dump URLs" in page
    n_calls = len(fake.calls)
    await c.post("/api/collections/ex.org/suggest/patterns"); job = await wait_job(c, "ex.org")
    p = job["progress"]
    assert p["urls"] == 10 and p["candidates"] == 1 and p["calls"] == 1
    sent = fake.calls[n_calls:]
    assert sent and all("new1" in str(call) and "/p2" not in str(call) for call in sent)


# ── 4 + 5. removal warning; per-URL edits on curated and crawl tables ───


async def test_removal_warning_and_edits_on_every_table(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post("/api/collections/ex.org/promote")
    for st in ("config_generated", "live"):
        await c.post("/api/collections/ex.org/status", json={"status": st})
    # curated table: toggle exclude on a promoted row → pending modified change, collection back to curating
    page = (await c.get("/collections/ex.org?tab=urls&set=curated")).text
    assert "✗ exclude" in page and "Edited by</th>" in page and "read-only" not in page.lower()
    r = await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p6", "type": "exclude"})
    assert r.status_code == 200 and r.json()["modified"] == 1 and r.json()["excluded"] == 1
    d = await delta(c, "https://ex.org/p6")
    assert d["kind"] == "modified" and d["excluded"] is True and d["edited_by"] == "sme"
    assert (await coll(c))["status"] == "curating"
    page = (await c.get("/collections/ex.org?tab=urls&set=curated&q=p6")).text
    assert "delta URL ↗" in page and 'title="exclude https://ex.org/p6 (by anonymous)"' in page
    # crawl table: the toggle is there, reflects the pending state, and works
    page = (await c.get("/collections/ex.org?tab=urls&set=dump&q=p6")).text
    assert ">excluded<" in page and "✓ include" in page and "delta URL ↗" in page
    r = await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p7", "type": "exclude"})
    assert r.status_code == 200 and r.json()["modified"] == 2
    # promote, then a crawl that lost most of the set → warning banner
    await c.post("/api/collections/ex.org/promote")
    db = c.app.state.db
    await db.replace_dump("ex.org", [DumpUrl(collection_id="ex.org", url=f"https://ex.org/p{i}", scraped_title=f"Page {i}") for i in (0, 1)])
    await c.post("/api/collections/ex.org/recompute")
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "7 of 8 curated URLs are gone from this dump (88%)" in page and "re-scrape instead of promoting" in page
    assert "7 removed ↗" in page
    # below the ratio (1 of 8) → no banner
    await db.replace_dump("ex.org", [DumpUrl(collection_id="ex.org", url=f"https://ex.org/p{i}", scraped_title=f"Page {i}") for i in range(1, 8)])
    await c.post("/api/collections/ex.org/recompute")
    assert "gone from this dump" not in (await c.get("/collections/ex.org?tab=curate")).text


# ── 6. re-curation reason ──────────────────────────────────────────────


async def test_recuration_reason(crawler_client):
    c = crawler_client
    await setup(c)
    assert (await coll(c))["recuration_reason"] is None
    await c.post("/api/collections/ex.org/promote")
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    col = await coll(c)
    assert col["needs_recuration"] is True and col["recuration_reason"].startswith("re-crawled on ")
    assert "after 8 URLs were promoted" in col["recuration_reason"]
    assert col["recuration_reason"] in (await c.get("/collections/ex.org/header")).text
    assert col["recuration_reason"] in (await c.get("/rows")).text
    await c.post("/api/collections/ex.org/recompute")  # identical crawl → flag and reason cleared
    col = await coll(c)
    assert col["needs_recuration"] is False and col["recuration_reason"] is None


async def test_recuration_reason_on_validation_failure(index_client):
    from tests.conftest import prepare

    c = index_client
    c.app.state.settings.validation_delay_s = 0.1
    await prepare(c, "half.org")
    await c.post("/api/collections/half.org/index?target=test")
    await wait_job(c, "half.org", timeout=40)
    col = await coll(c, "half.org")
    assert col["needs_recuration"] is True and col["recuration_reason"].startswith("test-index validation failed")
    # a recompute with nothing pending does not clear it: the index, not the curation, is what failed
    await c.post("/api/collections/half.org/recompute")
    col = await coll(c, "half.org")
    assert col["status"] == "curated" and col["needs_recuration"] is True and "validation failed" in col["recuration_reason"]


# ── 7. rules never cross collections ───────────────────────────────────


async def test_rules_are_scoped_to_their_collection(crawler_client):
    c = crawler_client
    await setup(c, "a.org")
    await setup(c, "b.org")
    before_b = (await c.get("/api/collections/b.org/delta?limit=100")).json()
    # a suggestion accepted in A, a hand rule in A, a per-URL edit in A
    await c.post("/api/collections/a.org/suggest/patterns"); await wait_job(c, "a.org")
    await c.post("/api/collections/a.org/suggestions/bulk", json={"decision": "accept"})
    await c.post("/api/collections/a.org/patterns", json={"type": "division", "match": "*", "value": "Earth Science"})
    await c.post("/api/collections/a.org/urls", json={"url": "https://a.org/p2", "type": "title", "value": "A two"})
    assert len(await patterns(c, "a.org")) >= 3 and await patterns(c, "b.org") == []
    await c.post("/api/collections/b.org/recompute")
    after_b = (await c.get("/api/collections/b.org/delta?limit=100")).json()
    assert after_b == before_b  # nothing in B moved
    assert all(d["division"] is None and d["edited_by"] is None for d in after_b["items"])
    # a glob that would match B's URLs still does nothing there
    await c.post("/api/collections/a.org/patterns", json={"type": "exclude", "match": "https://b.org/*"})
    await c.post("/api/collections/b.org/recompute")
    assert (await c.get("/api/collections/b.org/delta?excluded=true")).json()["total"] == 0
    # deleting A cascades only A's rules
    await c.delete("/api/collections/a.org")
    assert (await c.get("/api/collections/b.org")).status_code == 200
    await c.post("/api/collections/b.org/patterns", json={"type": "exclude", "match": "*/p1"})
    assert len(await patterns(c, "b.org")) == 1
    assert "this collection only" in (await c.get("/collections/b.org?tab=curate")).text
    assert "nothing you accept here touches another collection" in (await c.get("/manual")).text


# ── 8. workers default ─────────────────────────────────────────────────


def test_llm_workers_default_is_16():
    from sde_curation.config import Settings

    assert Settings.model_fields["llm_workers"].default == 16
