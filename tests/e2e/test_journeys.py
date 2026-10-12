"""End-to-end journeys: the main user workflows, each one long, through the API the way a curator
drives it, with the real crawler and indexer subprocesses, S3 (moto server), an in-memory prod index
and the fake LLM. Each step checks state, counts and that every page renders; every assertion names
its step, so a failure points at the step that broke. (TEST-STRATEGY-2026-10-09.md section 3.)

Journeys do not walk into known bugs: those are caught by the expected-failure tests at the unit and
integration levels, so a journey stays a regression guard for everything else.
"""

import asyncio
import json

import pytest
import yaml
from httpx import ASGITransport, AsyncClient

import sde_curation.jobs as jobs_mod
from sde_curation.backends.publish import to_web_document
from sde_curation.llm.fake import FakeProvider
from tests.support.crawler import write_variant
from tests.support.flows import login, prepare, wait_job
from tests.support.journeys import (
    CRAWL_DOCUMENTS,
    CRAWL_PAGES,
    collection,
    counts_match_tables,
    every_page_renders,
    index_to_live,
    job_succeeds,
    to_live,
    url,
    wire_prod,
)

CID = "ex.org"
KEY = "ex_org"  # the index key: the collection name, slugified


async def test_j1_a_new_collection_from_crawl_to_live(index_client, monkeypatch):
    c = index_client
    c.app.state.settings.validation_delay_s = 0.1

    step = "create"
    r = await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": CRAWL_PAGES,
                                               "division": "Heliophysics"})
    assert r.status_code == 201, f"[{step}] {r.text}"
    col = await collection(c, CID)
    assert (col["status"], col["created_by"]) == ("backlog", "anonymous"), f"[{step}] {col}"
    assert (await c.get("/manual")).status_code == 200, f"[{step}] the handbook did not render"
    await every_page_renders(c, CID, step)

    step = "crawl"
    assert (await c.post(f"/api/collections/{CID}/scrape")).status_code == 202, f"[{step}] start"
    await job_succeeds(c, CID, step, timeout=30)
    col = await collection(c, CID)
    assert (col["status"], col["dump_count"]) == ("scraped", CRAWL_DOCUMENTS), f"[{step}] {col}"
    actors = [h["actor"] for h in (await c.get(f"/api/collections/{CID}/history")).json()]
    assert actors == ["anonymous", "system"], f"[{step}] created by the visitor, moved to scraped by the job: {actors}"
    await every_page_renders(c, CID, step)

    step = "Start curating"
    r = await c.post(f"/api/collections/{CID}/recompute")
    assert r.status_code == 200, f"[{step}] {r.text}"
    col = await collection(c, CID)
    assert (col["status"], col["delta_count"]) == ("curating", CRAWL_DOCUMENTS), f"[{step}] {col}"
    await every_page_renders(c, CID, step)

    step = "exclude rule"
    r = await c.post(f"/api/collections/{CID}/patterns", json={"type": "exclude", "match": "*/p8"})
    assert r.status_code == 201, f"[{step}] {r.text}"
    assert (await collection(c, CID))["excluded_count"] == 1, f"[{step}] excluded count"

    step = "per-URL title"
    r = await c.post(f"/api/collections/{CID}/urls", json={"url": url(CID, 2), "type": "title", "value": "Hand made"})
    assert r.status_code == 200, f"[{step}] {r.text}"
    rows = {d["url"]: d for d in (await c.get(f"/api/collections/{CID}/delta?limit=100")).json()["items"]}
    assert rows[url(CID, 2)]["title"] == "Hand made", f"[{step}] title not applied"

    step = "exclude then include one page"
    for kind, excluded in (("exclude", 2), ("include", 1)):
        r = await c.post(f"/api/collections/{CID}/urls", json={"url": url(CID, 3), "type": kind})
        assert r.status_code == 200, f"[{step}] {kind}: {r.text}"
        assert (await collection(c, CID))["excluded_count"] == excluded, f"[{step}] after {kind}"
    await every_page_renders(c, CID, step)

    step = "Suggest patterns and accept all"
    assert (await c.post(f"/api/collections/{CID}/suggest/patterns")).status_code == 202, f"[{step}] start"
    await job_succeeds(c, CID, step)
    pending = (await c.get(f"/api/collections/{CID}/suggestions")).json()
    assert pending, f"[{step}] no suggestions"
    r = await c.post(f"/api/collections/{CID}/suggestions/bulk", json={"decision": "accept"})
    assert r.status_code == 200, f"[{step}] {r.text}"
    excluded_after_patterns = (await collection(c, CID))["excluded_count"]
    assert excluded_after_patterns > 1, f"[{step}] the accepted suggestion excluded nothing"

    step = "Suggest metadata"
    r = await c.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept"})
    assert r.status_code == 409, f"[{step}] accept-all with nothing suggested answered {r.status_code}"
    assert (await c.post(f"/api/collections/{CID}/suggest/metadata")).status_code == 202, f"[{step}] start"
    await job_succeeds(c, CID, step)

    step = "accept one row's suggestions"
    rows = (await c.get(f"/api/collections/{CID}/delta?limit=100")).json()["items"]
    first = next(d for d in rows if d.get("title_ai"))
    u, fields = first["url"], [f for f in ("title_ai", "division_ai", "document_type_ai") if first.get(f)]
    rules_before = {p["id"] for p in (await c.get(f"/api/collections/{CID}/patterns")).json()}
    r = await c.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept", "url": u})
    assert r.status_code == 200 and (r.json()["decided"], r.json()["url"]) == (len(fields), u), f"[{step}] {r.text}"
    row = next(d for d in (await c.get(f"/api/collections/{CID}/delta?limit=100")).json()["items"] if d["url"] == u)
    assert not any(row.get(f) for f in fields), f"[{step}] suggestions left on the row: {row}"
    new_rules = [p for p in (await c.get(f"/api/collections/{CID}/patterns")).json() if p["id"] not in rules_before]
    assert [(p["match"], p["source"]) for p in new_rules] == [(u, "llm")] * len(fields), f"[{step}] {new_rules}"
    r = await c.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "reject", "url": u})
    assert r.status_code == 409, f"[{step}] rejecting a decided row answered {r.status_code}"
    audit = (await c.get(f"/api/collections/{CID}/audit")).json()
    assert any(a["action"] == "ai.bulk_accept" and f"({u})" in (a["detail"] or "") for a in audit), f"[{step}] audit"

    step = "accept all"
    r = await c.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept"})
    assert r.status_code == 200, f"[{step}] {r.text}"
    r = await c.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept"})
    assert r.status_code == 409, f"[{step}] a second accept-all found suggestions left: {r.text}"
    await every_page_renders(c, CID, step)

    step = "a duplicate title blocks promote until Regenerate titles tells the pages apart"
    rows = [d for d in (await c.get(f"/api/collections/{CID}/delta?limit=100")).json()["items"]
            if not d["excluded"] and not d.get("title_ai")]  # not page 2: Accept all held its AI title back
    one, two = next((a, b) for i, a in enumerate(rows) for b in rows[i + 1:] if a["document_type"] == b["document_type"])
    r = await c.post(f"/api/collections/{CID}/urls", json={"url": two["url"], "type": "title", "value": one["title"]})
    assert r.status_code == 200, f"[{step}] {r.text}"
    r = await c.post(f"/api/collections/{CID}/promote")
    assert r.status_code == 409 and "sharing a title and document type with another page" in r.text, \
        f"[{step}] promote answered {r.status_code}: {r.text}"
    r = await c.post(f"/api/collections/{CID}/suggest/titles")
    assert r.status_code == 202, f"[{step}] start: {r.text}"
    titles_job = await job_succeeds(c, CID, step)
    assert titles_job["progress"]["still_duplicate"] == 0, f"[{step}] {titles_job['progress']}"
    r = await c.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept", "field": "title"})
    assert r.status_code == 200, f"[{step}] {r.text}"
    await every_page_renders(c, CID, step)

    step = "promote"
    r = await c.post(f"/api/collections/{CID}/promote")
    assert r.status_code == 200, f"[{step}] {r.text}"
    col = await collection(c, CID)
    included = CRAWL_DOCUMENTS - excluded_after_patterns
    assert (col["status"], col["delta_count"], col["curated_count"]) == ("curated", 0, included), f"[{step}] {col}"
    await every_page_renders(c, CID, step)

    step = "index to test"
    r = await c.post(f"/api/collections/{CID}/index?target=test")
    assert r.status_code == 202, f"[{step}] {r.text}"
    test_job = await job_succeeds(c, CID, step)
    assert test_job["progress"]["validation_ok"] is True, f"[{step}] validation"
    assert (await collection(c, CID))["status"] == "config_generated", f"[{step}] status"
    test_run = (await c.get(f"/api/collections/{CID}/index_runs")).json()[0]["run_id"]
    await every_page_renders(c, CID, step)

    step = "index to prod"
    prod = wire_prod(c, monkeypatch, test_run, KEY)
    r = await c.post(f"/api/collections/{CID}/index?target=prod")
    assert r.status_code == 202, f"[{step}] {r.text}"
    prod_job = await job_succeeds(c, CID, step)
    assert prod_job["kind"] == "index_prod" and prod_job["progress"]["validation_ok"] is True, f"[{step}] {prod_job}"
    assert len(prod.store) == included, f"[{step}] prod holds {len(prod.store)} documents, expected {included}"
    assert (await collection(c, CID))["status"] == "live", f"[{step}] status"
    notified = [n["new_status"] for n in c.app.state.notifier.sent]
    assert {"config_generated", "live"} <= set(notified), f"[{step}] status changes not notified: {notified}"
    await every_page_renders(c, CID, step)
    assert (await wait_job(c, CID))["kind"] == "index_prod"


async def test_j2_a_live_collection_crawled_again_after_the_site_changed(index_client, monkeypatch):
    """The site changes: page 3 gets a new title, page 6 new text, page 4 disappears. The re-crawl
    shows exactly those as delta URLs; Re-curate everything queues the whole collection; a partial
    then a full promote take the changes in; re-indexing removes page 4 from prod."""
    c = index_client
    prod = await to_live(c, monkeypatch, CID, KEY)
    live = await collection(c, CID)
    in_prod = len(prod.store)
    assert in_prod == live["curated_count"] == CRAWL_DOCUMENTS, "[live] prod holds the curated set"

    step = "re-crawl"
    write_variant(c.app.state.settings.crawler_root, retitle={3: "Page three, renamed"}, retext=[6], drop=[4])
    assert (await c.post(f"/api/collections/{CID}/scrape")).status_code == 202, f"[{step}] start"
    await job_succeeds(c, CID, step, timeout=30)
    assert (await collection(c, CID))["dump_count"] == CRAWL_DOCUMENTS - 1, f"[{step}] page 4 is gone"
    await every_page_renders(c, CID, step)

    step = "Start curating"
    assert (await c.post(f"/api/collections/{CID}/recompute")).status_code == 200, f"[{step}]"
    rows = {d["url"]: d for d in (await c.get(f"/api/collections/{CID}/delta?limit=100")).json()["items"]}
    kinds = {u: d["kind"] for u, d in rows.items()}
    assert kinds == {url(CID, 3): "modified", url(CID, 6): "modified", url(CID, 4): "deleted"}, f"[{step}] {kinds}"
    assert rows[url(CID, 6)]["content_changed"] is True, f"[{step}] page 6's new text"
    # the crawl reads page 3's new title; the title the curator accepted (a per-page rule) still wins
    assert (rows[url(CID, 3)]["scraped_title"], rows[url(CID, 3)]["title"]) == ("Page three, renamed", "Page 3"), \
        f"[{step}] page 3: {rows[url(CID, 3)]}"
    assert (await collection(c, CID))["status"] == "curating", f"[{step}] status"
    await every_page_renders(c, CID, step)

    step = "Re-curate everything"
    assert (await c.post(f"/api/collections/{CID}/recompute?all=true")).status_code == 200, f"[{step}]"
    col = await collection(c, CID)
    assert col["review_round"] is True and col["delta_count"] == CRAWL_DOCUMENTS, f"[{step}] {col}"
    await every_page_renders(c, CID, step)

    step = "partial promote"
    r = await c.post(f"/api/collections/{CID}/promote/urls", json={"urls": [url(CID, 3)]})
    assert r.status_code == 200, f"[{step}] {r.text}"
    col = await collection(c, CID)
    assert col["delta_count"] == CRAWL_DOCUMENTS - 1 and col["review_round"] is True, f"[{step}] {col}"

    step = "full promote"
    r = await c.post(f"/api/collections/{CID}/promote")
    assert r.status_code == 200, f"[{step}] {r.text}"
    col = await collection(c, CID)
    assert (col["status"], col["delta_count"], col["review_round"]) == ("curated", 0, False), f"[{step}] {col}"
    assert col["curated_count"] == CRAWL_DOCUMENTS - 1, f"[{step}] page 4 left the curated set"
    await every_page_renders(c, CID, step)

    step = "re-index"
    await index_to_live(c, monkeypatch, CID, KEY, step, prod)
    assert len(prod.store) == CRAWL_DOCUMENTS - 1, f"[{step}] prod still holds {len(prod.store)} documents"
    assert "Page 4" not in {d.get("title") for d in prod.store.values()}, f"[{step}] page 4 is still in prod"
    await every_page_renders(c, CID, step)
    assert (await wait_job(c, CID))["state"] == "succeeded"


async def _job_fails(c, cid: str, step: str, *, timeout: float = 40) -> dict:
    job = await wait_job(c, cid, timeout=timeout)
    assert job["state"] == "failed", f"[{step}] the job ended {job['state']}, expected failed"
    return job


@pytest.mark.parametrize("what", ["crawl", "Suggest metadata"])
async def test_j4_a_curator_cancels_a_running_job(index_client, monkeypatch, what):
    """A cancelled job ends failed, attributed to the curator; nothing is left half-done; the
    collection takes edits again."""
    c = index_client
    step = f"cancel {what}"
    if what == "crawl":
        cid = "slow.org"
        r = await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 400})
        assert r.status_code == 201, f"[{step}] create"
        assert (await c.post(f"/api/collections/{cid}/scrape")).status_code == 202, f"[{step}] start"
        await asyncio.sleep(0.5)  # the crawler is writing its log: 400 pages take about 8 s
        assert (await c.post(f"/api/collections/{cid}/scrape")).status_code == 409, f"[{step}] a second crawl started"
        assert (await c.delete(f"/api/collections/{cid}")).status_code == 409, \
            f"[{step}] the collection was deleted under its crawl"
        panel = (await c.get("/jobs/panel")).text
        assert f">{cid}<" in panel and f"/api/collections/{cid}/jobs/cancel" in panel, f"[{step}] jobs panel"
        assert "All jobs" not in panel, f"[{step}] the panel fragment carries its heading"
        hx = {"HX-Request": "true", "HX-Target": "jobs-panel"}
        assert "All jobs" in (await c.get("/jobs/panel", headers=hx)).text, f"[{step}] the htmx panel has no heading"
    else:
        cid = CID
        await prepare(c, cid)  # a curated collection
        assert (await c.post(f"/api/collections/{cid}/recompute?all=true")).status_code == 200
        held = asyncio.Event()
        ask = jobs_mod.suggest_metadata_one

        async def slow_after_two(llm, d, **kw):
            if held.is_set() or d["url"].endswith(("/p4", "/p6", "/p7", "/p8", "/p9")):
                held.set()
                await asyncio.Event().wait()
            return await ask(llm, d, **kw)

        monkeypatch.setattr(jobs_mod, "suggest_metadata_one", slow_after_two)
        assert (await c.post(f"/api/collections/{cid}/suggest/metadata?all=true")).status_code == 202, f"[{step}]"
        await asyncio.wait_for(held.wait(), 10)
    before = await collection(c, cid)

    hx = {"HX-Request": "true", "HX-Target": "jobs-panel"}
    r = await c.post(f"/api/collections/{cid}/jobs/cancel", headers=hx)
    assert (r.status_code, r.headers.get("HX-Refresh")) == (200, "true"), f"[{step}] {r.status_code} {r.text}"
    job = await _job_fails(c, cid, step)
    assert job["error"] == "cancelled by anonymous", f"[{step}] {job['error']}"
    assert "No jobs running" in (await c.get("/jobs/panel")).text, f"[{step}] the panel still lists the job"
    newest = (await c.get(f"/api/collections/{cid}/audit")).json()[0]
    assert (newest["action"], newest["actor"]) == ("job.cancel", "anonymous"), f"[{step}] audit: {newest}"
    after = await collection(c, cid)
    assert (after["status"], after["dump_count"]) == (before["status"], before["dump_count"]), f"[{step}] {after}"
    r = await c.post(f"/api/collections/{cid}/patterns", json={"type": "exclude", "match": "*/nothing*"})
    assert r.status_code == 201, f"[{step}] the collection still refuses edits: {r.text}"
    await every_page_renders(c, cid, step)


@pytest.mark.parametrize("what", ["crawler crash", "indexer failure", "validation never passes",
                                  "prod deletion refused", "every LLM call fails"])
async def test_j5_a_failure_is_reported_and_nothing_is_half_done(index_client, monkeypatch, what):
    c = index_client
    c.app.state.settings.validation_delay_s = 0.1
    step = what
    if what == "crawler crash":
        cid = "crash.org"
        await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 13})
        await c.post(f"/api/collections/{cid}/scrape")
        job = await _job_fails(c, cid, step, timeout=30)
        assert "boom" in job["error"], f"[{step}] {job['error']}"
        col = await collection(c, cid)
        assert (col["status"], col["dump_count"]) == ("backlog", 0), f"[{step}] {col}"
        home = (await c.get("/")).text
        assert "j-failed" in home and "Scrape</button>" in home, f"[{step}] the dashboard does not offer Scrape again"
        panel = (await c.get("/jobs/panel")).text
        assert f">{cid}<" not in panel and "No jobs running" in panel, f"[{step}] the jobs panel still lists it"
        jobs_page = (await c.get("/jobs")).text
        assert all(t in jobs_page for t in ("Recent", f">{cid}<", "failed")), f"[{step}] /jobs does not list the failure"
    elif what == "indexer failure":
        cid = "fail.org"
        await prepare(c, cid)
        await c.post(f"/api/collections/{cid}/index?target=test")
        await _job_fails(c, cid, step)
        assert (await collection(c, cid))["status"] == "curated", f"[{step}] the status moved on"
    elif what == "validation never passes":
        cid = "half.org"
        await prepare(c, cid)
        await c.post(f"/api/collections/{cid}/index?target=test")
        await wait_job(c, cid, timeout=40)
        assert (await collection(c, cid))["status"] == "curated", f"[{step}] the status moved on"
        assert "needs re-indexing" in (await c.get(f"/collections/{cid}/header")).text, f"[{step}] no warning"
    elif what == "prod deletion refused":
        cid = CID
        await prepare(c, cid)
        await c.post(f"/api/collections/{cid}/index?target=test")
        await job_succeeds(c, cid, step)
        test_run = (await c.get(f"/api/collections/{cid}/index_runs")).json()[0]["run_id"]
        prod = wire_prod(c, monkeypatch, test_run, KEY)
        manifest = json.loads(c.s3.get_object(Bucket="cosmos-idx", Key=f"curated_collections/{KEY}/{test_run}/manifest.json")["Body"].read())
        for i in range(100):  # prod holds far more of this collection than the curated set
            prod.add(to_web_document({"url": f"https://{cid}/stale{i}", "title": f"S{i}"}, manifest))
        before = dict(prod.store)
        await c.post(f"/api/collections/{cid}/index?target=prod")
        job = await _job_fails(c, cid, step)
        assert "deletion" in job["error"], f"[{step}] {job['error']}"
        assert prod.store == before, f"[{step}] the refused publish wrote to prod"
    else:
        cid = CID
        await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": CRAWL_PAGES,
                                                   "division": "Heliophysics"})
        await c.post(f"/api/collections/{cid}/scrape")
        await job_succeeds(c, cid, step, timeout=30)
        await c.post(f"/api/collections/{cid}/recompute")
        c.app.state.jobs._llm = lambda: FakeProvider(canned={"not": "a metadata answer"})
        await c.post(f"/api/collections/{cid}/suggest/metadata")
        job = await _job_fails(c, cid, step)
        assert "failed" in job["error"], f"[{step}] {job['error']}"
        rows = (await c.get(f"/api/collections/{cid}/delta?limit=100")).json()["items"]
        assert not any(d.get("title_ai") for d in rows), f"[{step}] AI values were written"
    await every_page_renders(c, cid, step)


async def test_j6_several_curators_work_at_once(index_client):
    """Three collections: one crawls, one runs Suggest metadata, one runs Suggest patterns while a
    curator edits a fourth, and three open tabs keep loading pages. No request fails with a server
    error, health stays up, and afterwards every count the pages show equals the tables."""
    c = index_client
    cids = ["a.org", "b.org", "c.org", "d.org"]
    for cid in cids:
        await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": CRAWL_PAGES,
                                               "division": "Heliophysics"})
    for cid in cids[1:]:
        await c.post(f"/api/collections/{cid}/scrape")
    for cid in cids[1:]:
        await job_succeeds(c, cid, "setup: crawl", timeout=30)
        assert (await c.post(f"/api/collections/{cid}/recompute")).status_code == 200

    statuses: list[tuple[str, int]] = []
    stop = asyncio.Event()
    paths = ["/", "/jobs/panel", "/health"] + [p for cid in cids for p in (
        f"/collections/{cid}", f"/collections/{cid}/tab-body?tab=curate", f"/collections/{cid}/header")]
    tab_count = 3

    async def tab(k: int) -> None:
        i = k
        while not stop.is_set():
            path = paths[i % len(paths)]
            statuses.append((path, (await c.get(path)).status_code))
            i += 1
            await asyncio.sleep(0.01)

    tabs = [asyncio.create_task(tab(k)) for k in range(tab_count)]
    step = "work at once"
    assert (await c.post(f"/api/collections/{cids[0]}/scrape")).status_code == 202, f"[{step}] crawl"
    assert (await c.post(f"/api/collections/{cids[1]}/suggest/metadata")).status_code == 202, f"[{step}] metadata"
    assert (await c.post(f"/api/collections/{cids[2]}/suggest/patterns")).status_code == 202, f"[{step}] patterns"
    for page in (2, 3):
        r = await c.post(f"/api/collections/{cids[3]}/urls", json={"url": url(cids[3], page), "type": "exclude"})
        assert r.status_code == 200, f"[{step}] edit: {r.text}"
    for cid in cids[:3]:
        await job_succeeds(c, cid, step, timeout=40)
    while len(statuses) < tab_count * len(paths):  # every tab has loaded every page at least once
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.gather(*tabs)

    errors = [(path, code) for path, code in statuses if code >= 500]
    assert not errors, f"[{step}] server errors while curators worked: {errors[:5]}"
    for cid in cids:
        await every_page_renders(c, cid, step)
        await counts_match_tables(c, cid, step)


async def test_j7_accounts_and_roles(authed_crawler_client):
    """Login on: an admin adds a curator; the curator curates but cannot manage users; a promotion
    to admin and a demotion take effect; a disabled account is signed out at once."""
    admin = authed_crawler_client
    app = admin.app
    step = "anonymous"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as anon:
        assert (await anon.get("/api/collections")).status_code == 401, f"[{step}] the API answered without login"

    step = "admin adds a curator"
    r = await admin.post("/users", data={"username": "carol", "password": "carolpass1", "role": "curator"})
    assert r.status_code == 303, f"[{step}] {r.status_code}"
    carol_id = next(u.id for u in await app.state.db.list_users() if u.username == "carol")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as carol:
        await login(carol, "carol", "carolpass1")

        step = "the curator curates"
        r = await carol.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID,
                                                       "max_pages": CRAWL_PAGES, "division": "Heliophysics"})
        assert r.status_code == 201, f"[{step}] create: {r.text}"
        await carol.post(f"/api/collections/{CID}/scrape")
        await job_succeeds(carol, CID, step, timeout=30)
        assert (await carol.post(f"/api/collections/{CID}/recompute")).status_code == 200, f"[{step}] recompute"
        col = await collection(carol, CID)
        assert (col["created_by"], col["curated_by"]) == ("carol", "carol"), f"[{step}] attribution: {col}"
        history = {h["new_status"]: h["actor"] for h in (await carol.get(f"/api/collections/{CID}/history")).json()}
        assert history == {"backlog": "carol", "scraped": "system", "curating": "carol"}, f"[{step}] {history}"
        audit = (await carol.get(f"/api/collections/{CID}/audit")).json()
        assert [(a["action"], a["actor"]) for a in audit[:3]] == [
            ("recompute", "carol"), ("scrape.start", "carol"), ("collection.create", "carol")], f"[{step}] {audit[:3]}"
        meta = yaml.safe_load((app.state.settings.collections_dir / CID / "collection.yaml").read_text())
        assert meta["created_by"] == "carol" and meta["history"][-1]["actor"] == "carol", f"[{step}] collection.yaml"

        step = "the curator's rule"
        r = await carol.post(f"/api/collections/{CID}/patterns", json={"type": "exclude", "match": "*/p9"})
        assert r.status_code == 201 and r.json()["pattern"]["created_by"] == "carol", f"[{step}] {r.text}"
        rules = yaml.safe_load((app.state.settings.collections_dir / CID / "patterns.yaml").read_text())
        assert "carol" in json.dumps(rules, default=str), f"[{step}] patterns.yaml does not name the curator"
        assert "(by carol)" in (await carol.get(f"/collections/{CID}?tab=dump")).text, f"[{step}] rule tooltip"
        r = await carol.delete(f"/api/collections/{CID}/patterns/{r.json()['pattern']['id']}")
        assert r.status_code == 200, f"[{step}] delete: {r.text}"
        newest = (await carol.get(f"/api/collections/{CID}/audit")).json()[0]
        assert (newest["action"], newest["actor"]) == ("pattern.delete", "carol") and "exclude */p9" in newest["detail"], \
            f"[{step}] audit: {newest}"

        step = "the curator filter"
        assert f"/collections/{CID}" in (await admin.get("/?curator=carol")).text, f"[{step}] carol's filter"
        assert f"/collections/{CID}" not in (await admin.get("/?curator=admin")).text, f"[{step}] admin's filter"
        assert (await admin.post(f"/api/collections/{CID}/recompute?all=true")).status_code == 200, f"[{step}]"
        assert (await collection(admin, CID))["curated_by"] == "admin", f"[{step}] Re-curate everything by admin"
        assert f"/collections/{CID}" in (await admin.get("/?curator=admin")).text, f"[{step}] admin's filter after"
        assert f"/collections/{CID}" not in (await admin.get("/?curator=carol")).text, f"[{step}] carol's filter after"

        step = "the curator cannot manage users"
        assert (await carol.get("/users")).status_code == 403, f"[{step}] /users"
        r = await carol.post("/users", data={"username": "mallory", "password": "mallorypass"})
        assert r.status_code == 403, f"[{step}] add user"

        step = "promotion and demotion"
        assert (await admin.post(f"/users/{carol_id}/role", data={"role": "admin"})).status_code == 303
        assert (await carol.get("/users")).status_code == 200, f"[{step}] promoted, but /users is refused"
        assert (await admin.post(f"/users/{carol_id}/role", data={"role": "curator"})).status_code == 303
        assert (await carol.get("/users")).status_code == 403, f"[{step}] demoted, but /users still answers"

        step = "a disabled account"
        assert (await admin.post(f"/users/{carol_id}/active", data={"active": 0})).status_code == 303
        assert (await carol.get("/api/collections")).status_code == 401, f"[{step}] still signed in"

