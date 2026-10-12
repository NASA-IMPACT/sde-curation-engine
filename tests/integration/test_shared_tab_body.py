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
