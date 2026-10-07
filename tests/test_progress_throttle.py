"""A running job's progress is written and announced at most once per PROGRESS_EVERY_S.

The LLM pool reports progress every second; each report was one UPDATE of job_runs and one event to
every open tab. The browser already shows at most one refresh per element every 3 s, so publishing
less often changes nothing curators see, as long as the last value always goes out and a job's end is
announced at once.
"""

from __future__ import annotations

import asyncio

import pytest

from sde_curation import jobs as jobs_mod
from sde_curation.models import JobKind, JobRun, JobState
from tests.conftest import seed_dump

EVERY = 0.3


@pytest.fixture
def fast_interval(monkeypatch):
    monkeypatch.setattr(jobs_mod, "PROGRESS_EVERY_S", EVERY)


async def _job(c) -> tuple[object, JobRun, list[dict]]:
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "ex.org", "max_pages": 10})
    await seed_dump(c, "ex.org")
    c_ = await c.app.state.db.get_collection("ex.org")
    job = await c.app.state.db.insert_job(JobRun(collection_id="ex.org", kind=JobKind.LLM_METADATA,
                                                 state=JobState.RUNNING))
    seen: list[dict] = []
    c.app.state.bus.listeners.append(
        lambda event, data: seen.append(data["job"]) if data.get("job", {}).get("id") == job.id else None)
    return c_, job, seen


async def test_many_updates_in_one_interval_go_out_as_two_with_the_last_value(client, fast_interval):
    c = client
    coll, job, seen = await _job(c)
    progress = c.app.state.jobs._progress_cb(coll, job)
    for i in range(1, 11):
        await progress({"done": i})
        await asyncio.sleep(EVERY / 20)
    assert len(seen) == 1 and seen[0]["progress"]["done"] == 1  # the first goes out at once
    await asyncio.sleep(EVERY * 1.5)
    assert len(seen) == 2 and seen[-1]["progress"]["done"] == 10  # the rest as one, with the last value
    assert (await c.app.state.db.get_job(job.id)).progress["done"] == 10  # and it is in the table


async def test_a_job_that_ends_is_announced_at_once_and_nothing_follows(client, fast_interval):
    c = client
    coll, job, seen = await _job(c)
    jobs = c.app.state.jobs
    progress = jobs._progress_cb(coll, job)
    await progress({"done": 1})
    await progress({"done": 2})  # held back: inside the interval
    await c.app.state.db.finish_job(job, JobState.SUCCEEDED)
    jobs._emit(coll, job)
    assert seen[-1]["state"] == "succeeded" and seen[-1]["progress"]["done"] == 2
    n = len(seen)
    await asyncio.sleep(EVERY * 1.5)
    assert len(seen) == n  # the held-back update does not come after the end
    assert (await c.app.state.db.get_job(job.id)).state is JobState.SUCCEEDED


async def test_a_phase_change_goes_out_at_once(client, fast_interval):
    c = client
    coll, job, seen = await _job(c)
    progress = c.app.state.jobs._progress_cb(coll, job)
    await progress({"done": 1})
    await progress({"phase": "indexing", "external_ref": "arn:task/1"})
    assert len(seen) == 2 and seen[-1]["progress"]["phase"] == "indexing"
    assert (await c.app.state.db.get_job(job.id)).progress["phase"] == "indexing"
