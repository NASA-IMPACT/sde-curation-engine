"""Page counts are stored between changes (db._stored, collection_stats) and are always exact.

Every write that changes what a page counts bumps the collection's version when it commits; the next
page view counts once and stores the result for that version. These tests walk one collection through
every kind of write a curator or a job makes and check, after each, that every count a page was given
equals the same count taken straight from the tables.
"""

from __future__ import annotations

import json

import pytest

from sde_curation.db import db_scope
from tests.support.flows import classify, wait_job

CID = "ex.org"
PAGES = (f"/collections/{CID}?tab=curate", f"/collections/{CID}?tab=overview",
         f"/collections/{CID}?tab=curate&focus=metadata", f"/collections/{CID}?tab=delta")


async def assert_stats_match(c) -> int:
    """Render the pages (they read and fill the stored counts), then recount every stored count from
    the tables. Returns how many stored counts were checked."""
    db = c.app.state.db
    for path in PAGES:
        assert (await c.get(path)).status_code == 200, path
    row = (await db.fetch("SELECT version, computed_version, counts FROM collection_stats WHERE collection_id=%s",
                          (CID,)))
    assert row, "no counts stored"
    row = row[0]
    assert row["computed_version"] == row["version"], "the stored counts are for an older version"
    token = db_scope.set("work")  # count straight from the tables, as a job would
    try:
        for key, stored in row["counts"].items():
            name, rest = key[:key.index("[")], key[key.index("["):]
            args, kwargs = json.loads(rest)
            fresh = await getattr(db, name)(CID, *args, **dict(kwargs))
            assert json.loads(json.dumps(fresh)) == stored, (key, stored, fresh)
    finally:
        db_scope.reset(token)
    return len(row["counts"])


async def _rows(c) -> list[dict]:
    return (await c.get(f"/api/collections/{CID}/delta?limit=100")).json()["items"]


async def test_stored_counts_stay_exact_through_every_kind_of_write(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10})
    await c.post(f"/api/collections/{CID}/scrape")
    assert (await wait_job(c, CID))["state"] == "succeeded"
    assert (await c.post(f"/api/collections/{CID}/recompute")).status_code == 200
    assert await assert_stats_match(c) >= 8  # scrape + Start curating

    r = await c.post(f"/api/collections/{CID}/patterns", json={"type": "title", "match": "*/p3", "value": "Three"})
    assert r.status_code == 201
    await assert_stats_match(c)  # rule added
    assert (await c.delete(f"/api/collections/{CID}/patterns/{r.json()['pattern']['id']}")).status_code == 200
    await assert_stats_match(c)  # rule deleted
    url = (await _rows(c))[0]["url"]
    assert (await c.post(f"/api/collections/{CID}/urls", json={"url": url, "type": "title", "value": "Hand"})).status_code == 200
    await assert_stats_match(c)  # per-URL edit
    assert (await c.post(f"/api/collections/{CID}/urls", json={"url": url, "type": "exclude"})).status_code == 200
    await assert_stats_match(c)  # exclude toggle
    assert (await c.post(f"/api/collections/{CID}/urls", json={"url": url, "type": "include"})).status_code == 200

    assert (await c.post(f"/api/collections/{CID}/suggest/patterns")).status_code == 202
    assert (await wait_job(c, CID))["state"] == "succeeded"
    await assert_stats_match(c)  # Suggest patterns (a job's writes)
    assert (await c.post(f"/api/collections/{CID}/suggestions/bulk", json={"decision": "accept"})).status_code == 200
    await assert_stats_match(c)  # accept-all suggestions

    assert (await c.post(f"/api/collections/{CID}/suggest/metadata")).status_code == 202
    assert (await wait_job(c, CID))["state"] == "succeeded"
    await assert_stats_match(c)  # Suggest metadata
    pending = [d for d in await _rows(c) if d["title_ai"]]
    assert len(pending) >= 3
    r = await c.post(f"/api/collections/{CID}/ai/accept", json={"url": pending[0]["url"], "field": "title"})
    assert r.status_code == 200, r.text
    await assert_stats_match(c)  # accept one AI value
    r = await c.post(f"/api/collections/{CID}/ai/reject", json={"url": pending[1]["url"], "field": "title"})
    assert r.status_code == 200, r.text
    await assert_stats_match(c)  # reject one
    assert (await c.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept"})).status_code == 200
    await assert_stats_match(c)  # accept-all AI

    r = await c.post(f"/api/collections/{CID}/suggest/titles")
    if r.status_code == 202:  # only when titles are shared
        await wait_job(c, CID)
        await assert_stats_match(c)  # Regenerate titles

    some = sorted(d["url"] for d in await _rows(c) if d["kind"] != "deleted" and not d["excluded"])[:2]
    assert (await c.post(f"/api/collections/{CID}/promote/urls", json={"urls": some})).status_code == 200
    await assert_stats_match(c)  # partial promote
    assert (await c.post(f"/api/collections/{CID}/promote")).status_code == 200
    await assert_stats_match(c)  # full promote
    assert (await c.post(f"/api/collections/{CID}/recompute?all=true")).status_code == 200
    await assert_stats_match(c)  # Re-curate everything
    assert (await c.post(f"/api/collections/{CID}/scrape")).status_code == 202
    assert (await wait_job(c, CID))["state"] == "succeeded"
    await assert_stats_match(c)  # re-crawl


async def test_a_page_seen_again_without_a_change_counts_nothing(crawler_client):
    c = crawler_client
    db = c.app.state.db
    await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10})
    await c.post(f"/api/collections/{CID}/scrape")
    await wait_job(c, CID)
    await c.post(f"/api/collections/{CID}/recompute")
    await classify(c)

    assert (await c.get(f"/collections/{CID}?tab=curate")).status_code == 200
    before = db.stats_computed
    for _ in range(3):
        assert (await c.get(f"/collections/{CID}?tab=curate")).status_code == 200
    assert db.stats_computed == before  # served from collection_stats

    url = (await _rows(c))[0]["url"]
    await c.post(f"/api/collections/{CID}/urls", json={"url": url, "type": "title", "value": "Changed"})
    assert (await c.get(f"/collections/{CID}?tab=curate")).status_code == 200
    assert db.stats_computed > before  # a write: counted again


# ── Known bugs from REVIEW-SINCE-DEV-MERGE-2026-10-08.md (expected failures until fixed) ──────────


@pytest.mark.xfail(strict=True, reason="M9: a write that commits but fails to mark the change leaves stale page counts")
async def test_a_write_whose_change_mark_fails_does_not_leave_stale_counts(crawler_client, monkeypatch):
    """A write and its change mark must succeed or fail together. Here the mark (a second
    transaction today) fails once, the way a pool timeout makes it fail, during the first recompute
    after a crawl; every count the pages show afterwards must still equal the tables."""
    import contextvars

    from psycopg_pool import PoolTimeout

    from sde_curation.db import Database

    c = crawler_client
    await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10})
    await c.post(f"/api/collections/{CID}/scrape")
    assert (await wait_job(c, CID))["state"] == "succeeded"
    await assert_stats_match(c)  # counts stored for the crawl

    inside = contextvars.ContextVar("inside_replace_deltas", default=False)
    armed = {"on": True}
    changed, replace_deltas = Database.changed, Database.replace_deltas

    async def failing_changed(self, cid, *a, **k):
        if armed["on"] and inside.get():
            armed["on"] = False
            self.touch(cid)
            raise PoolTimeout("couldn't get a connection after 5.00 sec")
        return await changed(self, cid, *a, **k)

    async def marked_replace_deltas(self, *a, **k):
        token = inside.set(True)
        try:
            return await replace_deltas(self, *a, **k)
        finally:
            inside.reset(token)

    monkeypatch.setattr(Database, "changed", failing_changed)
    monkeypatch.setattr(Database, "replace_deltas", marked_replace_deltas)
    await c.post(f"/api/collections/{CID}/recompute")
    assert not armed["on"]  # the mark failed once
    await assert_stats_match(c)


@pytest.mark.xfail(strict=True, reason="M10: a page view during a ✓ makes the excluded count go negative")
async def test_the_excluded_count_stays_exact_when_a_page_view_lands_during_an_include(crawler_client, monkeypatch):
    """✗ excludes a page; ✓ brings it back. The ✓ deletes the exclude rule (the stored excluded count
    becomes unknown) and then recomputes the page. A page view in between counts and stores the
    excluded count; the ✓ must not subtract the page a second time."""
    import asyncio

    from sde_curation.db import Database

    c = crawler_client
    db = c.app.state.db
    await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10})
    await c.post(f"/api/collections/{CID}/scrape")
    assert (await wait_job(c, CID))["state"] == "succeeded"
    assert (await c.post(f"/api/collections/{CID}/recompute")).status_code == 200
    url = sorted(d["url"] for d in await _rows(c))[2]
    assert (await c.post(f"/api/collections/{CID}/urls", json={"url": url, "type": "exclude"})).status_code == 200
    assert (await db.get_collection(CID)).excluded_count == 1

    paused, release = asyncio.Event(), asyncio.Event()
    delete_pattern = Database.delete_pattern

    async def pausing_delete_pattern(self, cid, pid):
        ok = await delete_pattern(self, cid, pid)
        paused.set()
        await release.wait()
        return ok

    monkeypatch.setattr(Database, "delete_pattern", pausing_delete_pattern)
    include = asyncio.create_task(c.post(f"/api/collections/{CID}/urls", json={"url": url, "type": "include"}))
    await asyncio.wait_for(paused.wait(), 10)
    for path in PAGES:  # another curator's tab
        assert (await c.get(path)).status_code == 200
    release.set()
    r = await include
    assert r.status_code == 200, r.text
    fresh = await db.count_excluded_by_rules(CID)
    assert (await db.get_collection(CID)).excluded_count == fresh == 0
    assert r.json().get("excluded") == fresh
