"""While a job runs on a collection, its open tabs share one rendering of the tab body.

Every open tab refreshes the tab body every 10 s while a job runs (#job-watch). On a 100K collection a
render runs the duplicate-title scans and AI-review counts, 11-12 s each under load: in the dev stress
run of 2026-10-12 (run A) twelve tabs on two such collections held the database at 100 % CPU. A
render made during the job is reused for TAB_BODY_SHARE_S by every viewer of the same page, filters
and role: the content is never older than the tab's own refresh interval. Outside a job nothing is
shared, so a curator's own edit always shows at once.
"""

from __future__ import annotations

from sde_curation.models import JobKind, JobRun, JobState
from tests.support.flows import wait_job

CID = "ex.org"
TAB = f"/collections/{CID}/tab-body?step=curating&tab=curate"


async def _curating(c) -> None:
    await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10})
    await c.post(f"/api/collections/{CID}/scrape")
    await wait_job(c, CID)
    await c.post(f"/api/collections/{CID}/recompute")


async def _running(c) -> JobRun:
    return await c.app.state.db.insert_job(JobRun(collection_id=CID, kind=JobKind.LLM_METADATA, state=JobState.RUNNING))


async def test_while_a_job_runs_a_second_tab_gets_the_same_render(crawler_client):
    c = crawler_client
    await _curating(c)
    await _running(c)

    first, second = await c.get(TAB), await c.get(TAB)

    assert (first.status_code, second.status_code) == (200, 200)
    assert (first.headers["x-tab-body"], second.headers["x-tab-body"]) == ("rendered", "shared")
    assert second.text == first.text


async def test_without_a_job_every_request_is_rendered(crawler_client):
    c = crawler_client
    await _curating(c)

    first, second = await c.get(TAB), await c.get(TAB)

    assert (first.headers["x-tab-body"], second.headers["x-tab-body"]) == ("rendered", "rendered")


async def test_when_the_job_ends_the_next_request_is_rendered_again(crawler_client):
    c = crawler_client
    await _curating(c)
    job = await _running(c)
    await c.get(TAB)

    await c.app.state.db.finish_job(job, JobState.SUCCEEDED)
    after = await c.get(TAB)

    assert after.headers["x-tab-body"] == "rendered"


async def test_a_new_job_does_not_reuse_the_render_of_the_one_before(crawler_client):
    c = crawler_client
    await _curating(c)
    job = await _running(c)
    await c.get(TAB)
    await c.app.state.db.finish_job(job, JobState.SUCCEEDED)
    await _running(c)

    assert (await c.get(TAB)).headers["x-tab-body"] == "rendered"


async def test_other_filters_get_their_own_render(crawler_client):
    c = crawler_client
    await _curating(c)
    await _running(c)
    await c.get(TAB)

    other = await c.get(TAB + "&kind=new")

    assert other.headers["x-tab-body"] == "rendered"


async def _slow_get_collection(monkeypatch, delay: float = 0.3) -> None:
    """Every render reads the collection first: make it take a while, as a render on a busy database does."""
    import asyncio

    from sde_curation.db import Database

    get_collection = Database.get_collection

    async def slow(self, *a, **k):
        await asyncio.sleep(delay)
        return await get_collection(self, *a, **k)

    monkeypatch.setattr(Database, "get_collection", slow)


async def test_tabs_that_ask_while_the_render_runs_wait_for_it_and_share_it(crawler_client, monkeypatch):
    """Dev, R3 (2026-10-13): five tabs asked within the same second, and each started its own render
    because the first had not finished: 0 of 196 refreshes were shared. A render already running
    for the same page is awaited instead."""
    import asyncio

    c = crawler_client
    await _curating(c)
    await _running(c)
    await _slow_get_collection(monkeypatch)

    answers = await asyncio.gather(*(c.get(TAB) for _ in range(5)))

    assert [r.status_code for r in answers] == [200] * 5
    assert sorted(r.headers["x-tab-body"] for r in answers) == ["rendered"] + ["shared"] * 4
    assert len({r.text for r in answers}) == 1


async def test_when_the_render_they_wait_for_fails_the_next_request_renders_again(crawler_client, monkeypatch):
    import asyncio

    import psycopg

    from sde_curation.db import Database

    c = crawler_client
    await _curating(c)
    await _running(c)
    get_collection, calls = Database.get_collection, []

    async def busy_once(self, *a, **k):
        calls.append(1)
        await asyncio.sleep(0.3)
        if len(calls) == 1:
            raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")
        return await get_collection(self, *a, **k)

    monkeypatch.setattr(Database, "get_collection", busy_once)

    answers = await asyncio.gather(*(c.get(TAB) for _ in range(3)))
    after = await c.get(TAB)

    assert [r.status_code for r in answers] == [503] * 3
    assert (after.status_code, after.headers["x-tab-body"]) == (200, "rendered")


async def test_without_a_job_tabs_asking_together_each_get_their_own_render(crawler_client, monkeypatch):
    import asyncio

    c = crawler_client
    await _curating(c)
    await _slow_get_collection(monkeypatch)

    answers = await asyncio.gather(*(c.get(TAB) for _ in range(3)))

    assert [r.headers["x-tab-body"] for r in answers] == ["rendered"] * 3
