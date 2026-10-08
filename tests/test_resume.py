"""Resuming jobs after an engine restart (JobManager resume registry) without hurting the engine.

Bernard's condition: resuming must never cause a shutdown, a restart loop, or a memory or connection
outage. These tests use stand-in job kinds, registered as resumable for the test, so they check the
mechanism itself: resumes start after the engine serves, staggered; at most `resume_concurrency` heavy
phases run at once; restarts are counted before a job runs again and stop at `resume_max_restarts`.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.db import Database
from sde_curation.jobs import JobManager
from sde_curation.models import JobKind, JobRun, JobState
from sde_curation.web.app import create_app

KINDS = [JobKind.LLM_PATTERNS, JobKind.LLM_TITLES, JobKind.VALIDATE, JobKind.VALIDATE_PROD,
         JobKind.BULK_ACCEPT, JobKind.RECOMPUTE]
FAST = {"resume_start_delay_s": 0.3, "resume_stagger_s": 0.2, "resume_concurrency": 2}


class Recorder:
    def __init__(self):
        self.starts: list[float] = []
        self.heavy_now = 0
        self.heavy_peak = 0
        self.pool_peak = 0
        self.serving_at: float | None = None
        self.release = asyncio.Event()


def stand_in_resumers(rec: Recorder, *, heavy_s: float = 0.4, block: bool = False):
    """_resumers for the test: every KINDS kind resumes into a job that runs one heavy phase holding
    a database connection (like an export), then succeeds — or, with `block`, never ends."""

    def resumers(self: JobManager):
        async def run(c, job):
            rec.starts.append(time.monotonic())
            if block:
                await rec.release.wait()  # until the engine shuts down under it
            async with self.heavy_phase(job):
                rec.heavy_now += 1
                rec.heavy_peak = max(rec.heavy_peak, rec.heavy_now)
                try:
                    async with self.db.pool.connection():
                        await asyncio.sleep(heavy_s)
                finally:
                    rec.heavy_now -= 1
            await self.db.finish_job(job, JobState.SUCCEEDED)
            self._emit(c, job)

        return {k: run for k in KINDS}

    return resumers


async def _left_running(tmp_path, n: int) -> list[int]:
    """n collections, each with a job a previous engine left 'running'."""
    app = create_app(Settings(data_dir=tmp_path / "seed", llm_provider="fake"))
    ids = []
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        for i in range(n):
            cid = f"r{i}.org"
            assert (await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 5})).status_code == 201
            job = await app.state.db.insert_job(JobRun(collection_id=cid, kind=KINDS[i % len(KINDS)],
                                                       state=JobState.RUNNING, progress={"done": 7}))
            ids.append(job.id)
    return ids


async def test_a_restart_storm_resumes_everything_later_staggered_and_bounded(tmp_path, monkeypatch):
    ids = await _left_running(tmp_path, 6)
    rec = Recorder()
    # each heavy phase outlasts several staggered starts: without the limit five would overlap
    monkeypatch.setattr(JobManager, "_resumers", stand_in_resumers(rec, heavy_s=1.0))
    app = create_app(Settings(data_dir=tmp_path / "engine", llm_provider="fake", **FAST))
    t0 = time.monotonic()
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        rec.serving_at = time.monotonic()
        db = app.state.db
        assert rec.starts == [], "a resume ran as part of startup"
        # the collections are busy (locked for curators) while their jobs wait to resume
        assert (await c.post("/api/collections/r0.org/recompute")).status_code == 409
        health_ok = True
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            health_ok &= (await c.get("/health")).status_code == 200
            stats = db.pool.get_stats()
            rec.pool_peak = max(rec.pool_peak, stats["pool_size"] - stats["pool_available"])
            jobs = [await db.get_job(i) for i in ids]
            if all(j.state is JobState.SUCCEEDED for j in jobs):
                break
            await asyncio.sleep(0.02)
        jobs = [await db.get_job(i) for i in ids]
    assert all(j.state is JobState.SUCCEEDED for j in jobs), [(j.kind, j.state, j.error) for j in jobs]
    assert all(j.progress["restarts"] == 1 and j.progress["done"] == 7 for j in jobs)  # counted, nothing lost
    assert health_ok
    assert rec.serving_at - t0 < FAST["resume_start_delay_s"]  # serving before any resume
    assert min(rec.starts) - t0 >= FAST["resume_start_delay_s"]
    gaps = [b - a for a, b in zip(rec.starts, rec.starts[1:], strict=False)]
    assert all(g >= FAST["resume_stagger_s"] * 0.9 for g in gaps), gaps
    assert rec.heavy_peak <= FAST["resume_concurrency"]
    assert rec.pool_peak <= FAST["resume_concurrency"] + 2, rec.pool_peak


async def test_a_job_that_never_survives_its_resume_stops_after_the_limit(tmp_path, monkeypatch):
    """The engine goes down under the job four times. It is resumed three times (the restart counted
    each time before it ran), then fails with the limit message; the engine serves throughout."""
    (job_id,) = await _left_running(tmp_path, 1)
    rec = Recorder()
    monkeypatch.setattr(JobManager, "_resumers", stand_in_resumers(rec, block=True))
    settings = Settings(data_dir=tmp_path / "engine", llm_provider="fake",
                        resume_start_delay_s=0, resume_stagger_s=0, resume_max_restarts=3)
    for restart in range(1, 5):
        app = create_app(settings)
        async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            assert (await c.get("/health")).status_code == 200
            for _ in range(100):
                job = await app.state.db.get_job(job_id)
                if len(rec.starts) == restart or job.state is JobState.FAILED:
                    break
                await asyncio.sleep(0.02)
        # leaving the block shuts the engine down under the running job
    db = await Database(settings.resolved_database_url).connect()  # the engine's own pool is closed
    try:
        job = await db.get_job(job_id)
    finally:
        await db.close()
    assert len(rec.starts) == 3
    assert job.state is JobState.FAILED and job.error == "stopped resuming after 3 engine restarts"


async def test_a_job_cancelled_while_it_waits_to_resume_stays_cancelled(tmp_path, monkeypatch):
    (job_id,) = await _left_running(tmp_path, 1)
    rec = Recorder()
    monkeypatch.setattr(JobManager, "_resumers", stand_in_resumers(rec))
    app = create_app(Settings(data_dir=tmp_path / "engine", llm_provider="fake",
                              resume_start_delay_s=0.5, resume_stagger_s=0))
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t"):
        job = await app.state.jobs.cancel("r0.org", actor="alice")
        assert job.state is JobState.FAILED and job.error == "cancelled by alice"
        await asyncio.sleep(0.8)
        assert rec.starts == []
        assert (await app.state.db.get_job(job_id)).state is JobState.FAILED


@pytest.mark.parametrize("kind", [JobKind.INDEX_PROD, JobKind.LLM_PATTERNS])
async def test_a_kind_that_is_not_resumable_fails_on_restart_as_before(tmp_path, kind, monkeypatch):
    monkeypatch.setattr(JobManager, "_resumers", lambda self: {})  # nothing registered as resumable
    app = create_app(Settings(data_dir=tmp_path / "seed", llm_provider="fake"))
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/api/collections", json={"seed_url": "https://x.org", "name": "x.org", "max_pages": 5})
        job = await app.state.db.insert_job(JobRun(collection_id="x.org", kind=kind, state=JobState.RUNNING))
    app = create_app(Settings(data_dir=tmp_path / "engine", llm_provider="fake"))
    async with app.router.lifespan_context(app):
        got = await app.state.db.get_job(job.id)
    assert got.state is JobState.FAILED and got.error == "engine restarted while job was running"
