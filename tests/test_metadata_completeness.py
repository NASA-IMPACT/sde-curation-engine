"""Nothing reaches the curated set blank: Suggest metadata answers every field (guesses at low
confidence), rows left without a field are asked again, promote refuses a delta URL without a
division, a document type or any title at all (a page with no title rule keeps its scraped title,
which is what the export indexes it under), and the metadata review filters by confidence and field."""

from sde_curation.engine.export import export_lines
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import METADATA_SYSTEM
from sde_curation.models import CuratedUrl, DumpUrl
from tests.conftest import wait_job

CID = "ex.org"
API = f"/api/collections/{CID}"


async def setup(c, n=10):
    await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": "Ex", "max_pages": n})
    await c.post(f"{API}/scrape"); await wait_job(c, CID)
    await c.post(f"{API}/recompute")  # 8 delta URLs: p1..p9 minus p5


def url(i):
    return f"https://{CID}/p{i}"


async def delta(c, i):
    return (await c.get(f"{API}/delta", params={"q": url(i)})).json()["items"][0]


# ── Suggest metadata answers every field ───────────────────────────────────


def test_the_prompt_never_allows_a_blank_field():
    assert "No field is ever null or empty" in METADATA_SYSTEM
    assert "use null" not in METADATA_SYSTEM and "prefer a null value" not in METADATA_SYSTEM
    assert "Null only if" not in METADATA_SYSTEM


async def test_every_url_gets_a_title_division_and_type_even_as_a_guess(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post(f"{API}/suggest/metadata")
    assert (await wait_job(c, CID))["progress"]["classified"] == 8
    items = (await c.get(f"{API}/delta?limit=100")).json()["items"]
    assert len(items) == 8
    for d in items:
        assert d["title_ai"] and d["division_ai"] and d["document_type_ai"], d
        assert d["title_ai_conf"] and d["division_ai_conf"] and d["document_type_ai_conf"], d
    # nothing on the page says which division: a guess, flagged low for the SME
    assert {d["division_ai_conf"] for d in items} == {"low"}


async def test_an_empty_title_is_a_failed_call_and_is_asked_again(crawler_client):
    c = crawler_client
    await setup(c)

    class BlankTitle(FakeProvider):
        async def complete(self, **kw):
            done = await super().complete(**kw)
            if url(3) in kw["user"]:
                done.parsed.title = "  "
            return done

    c.app.state.jobs._llm = BlankTitle()
    await c.post(f"{API}/suggest/metadata")
    job = await wait_job(c, CID)
    assert job["state"] == "succeeded" and job["progress"]["classified"] == 7 and job["progress"]["failed"] == 1
    d = await delta(c, 3)
    assert d["title_ai"] is None and "empty title" in d["ai_error"]
    c.app.state.jobs._llm = FakeProvider()
    await c.post(f"{API}/suggest/metadata")
    job = await wait_job(c, CID)
    assert job["progress"]["total"] == 1 and (await delta(c, 3))["title_ai"] == "Page 3"


async def test_rows_whose_answer_left_a_field_blank_are_asked_again(crawler_client):
    """Answers from before every field was required: a value missing with its confidence recorded
    and no rule setting it is asked again; a value the SME dismissed (no confidence) is not."""
    c = crawler_client
    db = c.app.state.db
    await setup(c)
    await c.post(f"{API}/suggest/metadata"); await wait_job(c, CID)
    assert await db.count_deltas_for_llm(CID) == 0
    await db.execute("UPDATE delta_urls SET division_ai=NULL WHERE collection_id=%s AND url=%s", (CID, url(2)))
    await db.execute("UPDATE delta_urls SET title_ai=NULL WHERE collection_id=%s AND url=%s", (CID, url(4)))
    assert await db.count_deltas_for_llm(CID) == 2
    await c.post(f"{API}/ai/reject", json={"url": url(6), "field": "division"})  # dismissed by the SME
    assert await db.count_deltas_for_llm(CID) == 2
    # a rule that sets the field answers it: no call needed
    await c.post(f"{API}/urls", json={"url": url(2), "type": "division", "value": "Earth Science"})
    assert await db.count_deltas_for_llm(CID) == 1


# ── promote refuses blanks ───────────────────────────────────────────────────


async def test_promote_refuses_blank_metadata_and_says_where(crawler_client):
    c = crawler_client
    db = c.app.state.db
    await setup(c)
    # the crawl titled every page, so no row is without a title: the two fields with no fallback are
    assert await db.incomplete_counts(CID) == {"urls": 8, "title": 0, "division": 8, "general": 0, "document_type": 8, "duplicate": 0}
    r = await c.post(f"{API}/promote")
    assert r.status_code == 409
    assert r.json()["detail"] == ("8 delta URLs cannot be promoted yet (8 without a division,"
                                  " 8 without a document type): accept the AI suggestions or set the values by hand first")
    k = (await c.get(API)).json()
    assert (k["status"], k["curated_count"], k["delta_count"]) == ("curating", 0, 8)
    # the Curate page and the Delta URLs table say so and point at the rows
    curate = (await c.get(f"/collections/{CID}?tab=curate")).text
    assert "⛔ 8 delta URLs cannot be promoted yet" in curate and "8 without a division · 8 without a document type" in curate
    assert ('disabled title="8 delta URLs still lack a title, division or document type,'
            ' or share a title with another page"') in curate
    table = (await c.get(f"/collections/{CID}?tab=delta")).text
    assert "⛔ 8 cannot be promoted yet" in table
    # suggestions pending are not values yet: still refused until accepted
    await c.post(f"{API}/suggest/metadata"); await wait_job(c, CID)
    assert (await c.post(f"{API}/promote")).status_code == 409
    # accept titles and types; one division is dismissed and one is accepted by hand-edit → one row left
    for field in ("title", "document_type"):
        await c.post(f"{API}/ai/bulk", json={"decision": "accept", "field": field})
    await c.post(f"{API}/ai/reject", json={"url": url(2), "field": "division"})
    await c.post(f"{API}/ai/bulk", json={"decision": "accept", "field": "division"})
    assert await db.incomplete_counts(CID) == {"urls": 1, "title": 0, "division": 1, "general": 0, "document_type": 0, "duplicate": 0}
    assert [r.url for r in (await db.list_deltas(CID, incomplete=True))[0]] == [url(2)]
    page = (await c.get(f"/collections/{CID}?tab=delta&missing=true")).text
    assert url(2) in page and url(3) not in page
    csv = (await c.get(f"/collections/{CID}/urls/delta?format=csv&missing=true")).text.splitlines()
    assert len(csv) == 2 and url(2) in csv[1]
    r = await c.post(f"{API}/promote")
    assert r.status_code == 409 and "1 delta URL cannot be promoted yet (1 without a division)" in r.text
    # a selection without the blank row goes through; with it, it is refused whole
    assert (await c.post(f"{API}/promote/urls", json={"urls": [url(1), url(2)]})).status_code == 409
    assert (await c.get(API)).json()["curated_count"] == 0
    assert (await c.post(f"{API}/promote/urls", json={"urls": [url(1)]})).json()["left"] == 7
    await c.post(f"{API}/urls", json={"url": url(2), "type": "division", "value": "Earth Science"})
    r = await c.post(f"{API}/promote")
    assert r.status_code == 200 and r.json() == {"curated": 8, "status": "curated"}
    assert all(r["title"] and r["division"] and r["document_type"]
               for r in (await c.get(f"{API}/curated?limit=100")).json()["items"])
    assert "cannot be promoted yet" not in (await c.get(f"/collections/{CID}?tab=curate")).text


async def test_only_a_page_the_crawl_left_untitled_counts_as_blank(crawler_client):
    """A page the crawl titled is never blank: with no title rule the export indexes it under the
    scraped title, so promote takes it and the cell shows that title rather than an empty dash —
    filling the other fields by hand is enough. Only a page with no title at all is refused."""
    c = crawler_client
    db = c.app.state.db
    await setup(c)
    await db.replace_dump(CID, [
        DumpUrl(collection_id=CID, url=url(1), scraped_title="Page 1", full_text="x"),
        DumpUrl(collection_id=CID, url=url(2), scraped_title=None, full_text="x"),
    ])
    await c.post(f"{API}/recompute")
    # the curator sets the two fields that have no fallback by hand, taking no AI suggestion
    for i in (1, 2):
        for field, value in (("division", "Earth Science"), ("document_type", "Documentation")):
            r = await c.post(f"{API}/urls", json={"url": url(i), "type": field, "value": value})
            assert r.status_code in (200, 201), r.text
    assert await db.incomplete_counts(CID) == {"urls": 1, "title": 1, "division": 0, "general": 0, "document_type": 0, "duplicate": 0}
    assert [d.url for d in (await db.list_deltas(CID, incomplete=True))[0]] == [url(2)]
    # the titled row reads as titled, not as an empty cell
    page = (await c.get(f"/collections/{CID}?tab=delta")).text
    assert "Page 1" in page and ">scraped<" in page
    r = await c.post(f"{API}/promote")
    assert r.status_code == 409 and "1 delta URL cannot be promoted yet (1 without a title)" in r.text
    # it promotes on its own and reaches the index under the scraped title
    assert (await c.post(f"{API}/promote/urls", json={"urls": [url(1)]})).json()["promoted"] == 1
    assert [x.title for x in export_lines(await db.load_curated(CID))] == ["Page 1"]
    # the untitled page needs a title of its own, and then the queue is through
    await c.post(f"{API}/urls", json={"url": url(2), "type": "title", "value": "Page 2 by hand"})
    assert await db.incomplete_counts(CID) == {"urls": 0, "title": 0, "division": 0, "general": 0, "document_type": 0, "duplicate": 0}
    assert (await c.post(f"{API}/promote")).status_code == 200
    assert sorted(x.title for x in export_lines(await db.load_curated(CID))) == ["Page 1", "Page 2 by hand"]


async def test_removals_and_excluded_rows_are_never_blocked(crawler_client):
    """A tombstone carries no metadata to the index, and an excluded row is never indexed: rows
    promoted before the rule (blank) can still be removed or kept out."""
    c = crawler_client
    db = c.app.state.db
    await setup(c)
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=url(i), scraped_title=f"Page {i}") for i in (1, 2)])
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(1), scraped_title="Page 1 (new)", full_text="x")])
    await c.post(f"{API}/patterns", json={"type": "exclude", "match": url(1)})
    assert await db.incomplete_counts(CID) == {"urls": 0, "title": 0, "division": 0, "general": 0, "document_type": 0, "duplicate": 0}
    kinds = [d["kind"] for d in (await c.get(f"{API}/delta")).json()["items"]]
    assert kinds == ["deleted"]
    r = await c.post(f"{API}/promote")
    # page 2's tombstone is promoted away; page 1 stays as a curated row but the exclude rule keeps
    # it out of the index, so "curated" (the indexed count) is 0 over one remaining row
    assert r.status_code == 200 and r.json()["curated"] == 0
    k = (await c.get(API)).json()
    assert (k["curated_count"], k["curated_rows"]) == (0, 1)


# ── review by confidence ─────────────────────────────────────────────────────


async def test_metadata_review_filters_by_confidence_and_field(crawler_client):
    c = crawler_client
    await setup(c)

    class Mixed(FakeProvider):  # p2 and p3 mention a division in their text: medium, the rest low
        async def complete(self, **kw):
            if url(2) in kw["user"] or url(3) in kw["user"]:
                kw = {**kw, "user": kw["user"] + " aurora"}
            return await super().complete(**kw)

    c.app.state.jobs._llm = Mixed()
    await c.post(f"{API}/suggest/metadata"); await wait_job(c, CID)
    base = f"/collections/{CID}?tab=curate"
    page = (await c.get(base)).text
    # titles are high on every row, types low on every row, divisions medium on p2/p3 and low elsewhere
    assert "all 24" in page and ">high 8<" in page and ">medium 2<" in page and ">low 14<" in page
    assert "accept these" not in page  # no filter, no filtered bulk
    medium = (await c.get(f"{base}&conf=medium")).text
    assert url(2) in medium and url(3) in medium and url(4) not in medium
    assert "2 URLs · 2 medium-confidence suggestions" in medium and "✓ accept these 2" in medium
    assert '"conf": "medium"' in medium and "clear filter" in medium
    low_div = (await c.get(f"{base}&conf=low&field=division")).text
    assert url(4) in low_div and url(2) not in low_div and "6 URLs · 6 low-confidence division suggestions" in low_div
    # paging and expand keep the filter
    assert "focus=metadata&conf=low&field=division" in low_div
    assert "No medium-confidence title suggestions left to review" in (await c.get(f"{base}&conf=medium&field=title")).text
    # accept exactly what the filter names: the medium divisions; every other suggestion stays
    r = await c.post(f"{API}/ai/bulk", json={"decision": "accept", "conf": "medium", "field": "division"})
    assert r.status_code == 200 and r.json()["decided"] == 2
    d2, d4 = await delta(c, 2), await delta(c, 4)
    assert d2["division"] == "Heliophysics" and d2["division_ai"] is None and d2["title_ai"] and d2["document_type_ai"]
    assert d4["division"] is None and d4["division_ai"] == "Astrophysics"
    # reject the low ones of every field: the high titles are untouched
    r = await c.post(f"{API}/ai/bulk", json={"decision": "reject", "conf": "low"})
    assert r.status_code == 200 and r.json()["decided"] == 14
    items = (await c.get(f"{API}/delta?limit=100")).json()["items"]
    assert all(d["title_ai"] for d in items) and not any(d["division_ai"] or d["document_type_ai"] for d in items)
    assert (await c.post(f"{API}/ai/bulk", json={"decision": "accept", "conf": "low"})).status_code == 409
    assert (await c.post(f"{API}/ai/bulk", json={"decision": "accept", "conf": "certain"})).status_code == 422
