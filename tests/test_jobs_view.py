"""Global Jobs view: running / queued jobs across collections, recent failures, cancel."""

import asyncio

from tests.conftest import wait_job


async def test_jobs_panel_and_page(crawler_client):
    c = crawler_client
    home = (await c.get("/")).text
    assert 'id="jobs-panel"' in home and "No jobs running" in home
    assert (await c.get("/jobs")).status_code == 200
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 40})
    await c.post("/api/collections", json={"seed_url": "https://b.org", "name": "Bee", "max_pages": 13})  # 13 = crash
    await c.post("/api/collections/ex.org/scrape")
    await asyncio.sleep(0.1)
    panel = (await c.get("/jobs/panel")).text
    assert "scrape" in panel and ">Ex<" in panel and "/api/collections/ex.org/jobs/cancel" in panel
    assert "All jobs" not in panel  # the link only shows on the compact dashboard strip
    strip = (await c.get("/jobs/panel", headers={"HX-Request": "true", "HX-Target": "jobs-panel"})).text
    assert "All jobs" in strip
    # a queued crawl (crawler-host inbox) is labelled as such
    job = (await c.get("/api/collections/ex.org/jobs")).json()[0]
    await c.app.state.db.update_job(_with_progress(job))
    panel = (await c.get("/jobs/panel")).text
    assert "queued" in panel and "behind 2 crawls" in panel
    await wait_job(c, "ex.org")
    # a failed job shows up as the last failure, on the dashboard and on /jobs
    await c.post("/api/collections/b.org/scrape"); await wait_job(c, "b.org")
    home = (await c.get("/")).text
    assert "last failure" in home and ">Bee<" in home
    page = (await c.get("/jobs")).text
    assert "Recent" in page and ">Bee<" in page and ">Ex<" in page and "failed" in page
    assert "Jobs" in (await c.get("/")).text.split('role="menu"')[1]


def _with_progress(job: dict):
    from sde_curation.models import JobRun

    j = JobRun(**job)
    j.progress = {**j.progress, "queued": True, "queue_ahead": 2}
    return j


async def test_cancel_from_jobs_panel(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 40})
    await c.post("/api/collections/ex.org/scrape")
    await asyncio.sleep(0.1)
    r = await c.post("/api/collections/ex.org/jobs/cancel", headers={"HX-Request": "true", "HX-Target": "jobs-panel"})
    assert r.status_code == 200 and r.headers.get("HX-Refresh") == "true"
    job = await wait_job(c, "ex.org")
    assert job["state"] == "failed" and "cancelled" in job["error"]
    assert "No jobs running" in (await c.get("/jobs/panel")).text


async def test_llm_progress_tag_shows_on_every_surface(crawler_client):
    """One macro renders the in-flight LLM state: header chip, curate stepper, dashboard, jobs strip."""
    from sde_curation.models import JobRun

    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 40})
    await c.post("/api/collections/ex.org/scrape")
    await asyncio.sleep(0.1)
    job = JobRun(**(await c.get("/api/collections/ex.org/jobs")).json()[0])
    job.progress = {"llm": "metadata", "done": 3, "total": 8, "inflight": 2, "failed": 1}
    await c.app.state.db.update_job(job)
    expect = "LLM calls in progress · 3/8 URLs · 2 in flight · 1 failed"
    for url in ("/jobs/panel", "/collections/ex.org/header", "/", "/jobs"):
        assert expect in (await c.get(url)).text, url
    job.progress = {"llm": "patterns", "done": 1, "total": 4, "inflight": 3}
    await c.app.state.db.update_job(job)
    assert "LLM calls in progress · 1/4 calls · 3 in flight" in (await c.get("/jobs/panel")).text
    await wait_job(c, "ex.org", timeout=30)
