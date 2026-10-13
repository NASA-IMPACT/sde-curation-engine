"""Adding or deleting a rule on a big collection runs as a job (settings.bulk_job_min_urls).

Both end in a full recompute: 16-51 s on 100K pages, inside the request until 2026-10-13 (dev Run B:
51 s, close to the 60 s CloudFront gives a request). As a job the request answers at once, the
curator sees progress, and no other edit is taken on the collection until it ends (ensure_idle), as
for every job. A duplicate rule or an unknown one is still refused in the request, before any job.
"""

from __future__ import annotations

import asyncio

from sde_curation.curation import CurationService
from tests.support.flows import wait_job

CID = "ex.org"
API = f"/api/collections/{CID}"
GLOB = {"type": "exclude", "match": "*/p3*"}


async def _crawled(c, *, big: bool) -> None:
    await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10})
    await c.post(f"{API}/scrape")
    assert (await wait_job(c, CID))["state"] == "succeeded"
    assert (await c.post(f"{API}/recompute")).status_code == 200
    dump = (await c.app.state.db.get_collection(CID)).dump_count
    c.app.state.settings.bulk_job_min_urls = dump if big else dump + 1


async def _excluded(c) -> bool:
    return any("/p3" in d["url"] for d in await c.app.state.db.fetch(
        "SELECT url FROM delta_urls WHERE collection_id=%s AND excluded", (CID,))) or not any(
        "/p3" in d["url"] for d in await c.app.state.db.fetch("SELECT url FROM delta_urls WHERE collection_id=%s", (CID,)))


async def _rule_id(c) -> int:
    return next(p["id"] for p in (await c.get(f"{API}/patterns")).json() if p["match"] == GLOB["match"])


async def test_on_a_big_collection_adding_a_rule_is_a_job(crawler_client):
    c = crawler_client
    await _crawled(c, big=True)

    r = await c.post(f"{API}/patterns", json=GLOB)

    assert r.status_code == 202, r.text
    assert r.json()["kind"] == "rule_add"
    job = await wait_job(c, CID)
    assert job["state"] == "succeeded", job
    assert any(p["match"] == GLOB["match"] for p in (await c.get(f"{API}/patterns")).json())
    assert await _excluded(c)


async def test_on_a_big_collection_deleting_a_rule_is_a_job(crawler_client):
    c = crawler_client
    await _crawled(c, big=False)
    assert (await c.post(f"{API}/patterns", json=GLOB)).status_code == 201
    pid = await _rule_id(c)
    c.app.state.settings.bulk_job_min_urls = 0
    stored_before = (await c.app.state.db.get_collection(CID)).excluded_count

    r = await c.delete(f"{API}/patterns/{pid}")

    assert r.status_code == 202, r.text
    assert r.json()["kind"] == "rule_delete"
    assert (await wait_job(c, CID))["state"] == "succeeded"
    assert all(p["id"] != pid for p in (await c.get(f"{API}/patterns")).json())
    after = await c.app.state.db.get_collection(CID)
    assert after.excluded_count == await c.app.state.db.count_excluded_by_rules(CID) < stored_before


async def test_while_the_rule_job_runs_every_other_edit_is_refused(crawler_client, monkeypatch):
    c = crawler_client
    await _crawled(c, big=True)
    held, release = asyncio.Event(), asyncio.Event()
    recompute = CurationService._recompute

    async def holding(self, *a, **k):
        held.set()
        await release.wait()
        return await recompute(self, *a, **k)

    monkeypatch.setattr(CurationService, "_recompute", holding)
    add = asyncio.create_task(c.post(f"{API}/patterns", json=GLOB))
    try:
        await asyncio.wait_for(held.wait(), 10)
        assert (await asyncio.wait_for(add, 5)).status_code == 202  # answered while the recompute is held
        edits = [await asyncio.wait_for(e, 5) for e in (
            c.post(f"{API}/patterns", json={"type": "exclude", "match": "*/p4*"}),
            c.post(f"{API}/urls", json={"url": f"https://{CID}/p5", "type": "title", "value": "Five"}),
            c.delete(f"{API}/patterns/{await _rule_id(c)}"))]
    finally:
        release.set()

    assert [r.status_code for r in edits] == [409, 409, 409]
    assert (await wait_job(c, CID))["state"] == "succeeded"


async def test_a_duplicate_or_unknown_rule_is_refused_in_the_request_without_a_job(crawler_client):
    c = crawler_client
    await _crawled(c, big=False)
    assert (await c.post(f"{API}/patterns", json=GLOB)).status_code == 201
    c.app.state.settings.bulk_job_min_urls = 0
    jobs_before = len((await c.get(f"{API}/jobs")).json())

    dup = await c.post(f"{API}/patterns", json=GLOB)
    unknown = await c.delete(f"{API}/patterns/999999")

    assert (dup.status_code, unknown.status_code) == (409, 404)
    assert len((await c.get(f"{API}/jobs")).json()) == jobs_before


async def test_on_a_small_collection_both_stay_in_the_request(crawler_client):
    c = crawler_client
    await _crawled(c, big=False)

    added = await c.post(f"{API}/patterns", json=GLOB)
    deleted = await c.delete(f"{API}/patterns/{await _rule_id(c)}")

    assert (added.status_code, deleted.status_code) == (201, 200)
