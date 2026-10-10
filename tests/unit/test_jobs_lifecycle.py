"""JobManager lifecycle on the in-memory FakeDatabase: what an engine restart resumes or fails, the
resume queue and its heavy-phase limit, cancels, bulk curation jobs and progress throttling."""

import asyncio

import pytest

import sde_curation.jobs as jobs_mod
from sde_curation.models import Collection, ConnectorType, JobKind, JobRun, JobState
from tests.support.engine import (
    CID,
    Calls,
    Scraper,
    collection,
    finished,
    make_engine,
    until,
)
from tests.support.fake_db import FakeDatabase

RESUME_MAX_RESTARTS = 3  # Settings.resume_max_restarts and scrape_resume_after_restart default


async def _running(db, kind: JobKind, *, restarts: int = 0, **fields) -> JobRun:
    """A job the previous engine left 'running' after `restarts` earlier restarts."""
    return await db.insert_job(JobRun(collection_id=CID, kind=kind, state=JobState.RUNNING,
                                      progress={"restarts": restarts, **fields.pop("progress", {})}, **fields))


# ── what an engine restart does with a job left running ──────────────────────────────────────────


@pytest.mark.parametrize(("kind", "settings", "restarts", "error"), [
    (JobKind.VALIDATE, {}, RESUME_MAX_RESTARTS - 1, None),  # the 3rd restart still resumes
    (JobKind.VALIDATE, {}, RESUME_MAX_RESTARTS, "stopped resuming after 3 engine restarts"),  # the 4th fails
    (JobKind.LLM_METADATA, {"llm_resume_after_restart": 1}, 1, "stopped resuming after 1 engine restarts"),
    (JobKind.SCRAPE, {}, RESUME_MAX_RESTARTS, "engine restarted while job was running"),
])
async def test_a_restart_resumes_a_job_until_its_restart_limit_then_fails_it(tmp_path, kind, settings, restarts, error):
    db = FakeDatabase()
    await collection(db, titles=["A"])
    job = await _running(db, kind, restarts=restarts)
    engine = make_engine(db, tmp_path, **settings)

    await engine.recover()  # resumes are queued, not started

    after = await db.get_job(job.id)
    if error is None:
        assert (after.state, after.progress["restarts"]) == (JobState.RUNNING, restarts + 1)
        assert engine.active_for(CID).id == job.id  # the collection stays busy until it resumes
    else:
        assert (after.state, after.error) == (JobState.FAILED, error)
        assert engine.active_for(CID) is None


async def test_a_cancel_while_waiting_to_resume_fails_the_job(tmp_path):
    db = FakeDatabase()
    await collection(db, titles=["A"])
    job = await _running(db, JobKind.VALIDATE)
    engine = make_engine(db, tmp_path, resume_start_delay_s=60)
    await engine.recover()

    cancelled = await engine.cancel(CID, actor="alice")
    engine.start_resumes()
    await asyncio.sleep(0.05)

    assert (cancelled.id, cancelled.state, cancelled.error) == (job.id, JobState.FAILED, "cancelled by alice")
    assert engine.active_for(CID) is None
    await engine.shutdown()


@pytest.mark.parametrize(("resumed_before", "restarted"), [(0, True), (3, False)])
async def test_a_metadata_run_an_older_engine_failed_at_shutdown_restarts_as_a_new_job(tmp_path, resumed_before, restarted):
    """Engines before the resume registry failed Suggest metadata at shutdown; the next one asks the
    pages still without an answer in a new job, at most `llm_resume_after_restart` (3) times."""
    db = FakeDatabase()
    c = await collection(db, titles=["A", "B"])
    old = await db.insert_job(JobRun(collection_id=CID, kind=JobKind.LLM_METADATA, state=JobState.FAILED,
                                     error="cancelled by shutdown", progress={"resumed": resumed_before}))
    engine = make_engine(db, tmp_path)

    await engine.recover()
    new = engine.active_for(c.collection_id)

    if restarted:
        assert new.kind == JobKind.LLM_METADATA and new.id != old.id
        done = await finished(db, new.id)
        assert (done.state, done.progress["resumed"], done.progress["resumed_from"]) == (JobState.SUCCEEDED, 1, old.id)
    else:
        assert new is None
    await engine.shutdown()


# ── the resume queue: heavy phases of resumed jobs are limited ───────────────────────────────────


class Concurrency:
    """A bulk change that counts how many run at once."""

    def __init__(self):
        self.now = self.peak = self.runs = 0

    def work(self, kind, c, request, actor):
        async def run():
            self.now += 1
            self.runs += 1
            self.peak = max(self.peak, self.now)
            await asyncio.sleep(0.05)
            self.now -= 1
            return {"changed": 1}
        return run


@pytest.mark.parametrize("resume_concurrency", [1, 2])
async def test_resumed_jobs_run_their_heavy_phase_at_most_resume_concurrency_at_a_time(tmp_path, resume_concurrency):
    db = FakeDatabase()
    jobs = []
    for cid in ("a.org", "b.org", "c.org"):
        await db.insert_collection(Collection(collection_id=cid, name=cid, seed_url=f"https://{cid}",
                                              connector=ConnectorType.CRAWLER, max_pages=10))
        jobs.append(await db.insert_job(JobRun(collection_id=cid, kind=JobKind.RECOMPUTE, state=JobState.RUNNING,
                                               progress={"curation": "recompute", "request": {}})))
    probe = Concurrency()
    engine = make_engine(db, tmp_path, resume_concurrency=resume_concurrency)
    engine.curation_work = probe.work

    await engine.recover()
    engine.start_resumes()
    done = [await finished(db, j.id) for j in jobs]

    assert [j.state for j in done] == [JobState.SUCCEEDED] * len(jobs)
    assert (probe.runs, probe.peak) == (len(jobs), resume_concurrency)


# ── bulk curation jobs ───────────────────────────────────────────────────────────────────────────


async def test_a_bulk_curation_job_records_the_counts_its_change_returns(tmp_path):
    db = FakeDatabase()
    c = await collection(db, titles=["A"])
    engine = make_engine(db, tmp_path)

    async def accept_all():
        return {"accepted": 7, "field": "title", "urls": ["not", "a", "count"]}

    job = await engine.start_curation(c, JobKind.BULK_ACCEPT, accept_all, "accept all titles")
    done = await finished(db, job.id)

    assert done.state is JobState.SUCCEEDED
    assert done.progress["result"] == {"accepted": 7, "field": "title"}  # scalars only on the job row


class Refused(Exception):
    detail = "a duplicate title blocks this"  # what an HTTPException carries


@pytest.mark.parametrize(("error", "message"), [
    (Refused(), "a duplicate title blocks this"),
    (RuntimeError("boom"), "RuntimeError: boom"),
])
async def test_a_bulk_curation_job_that_raises_fails_with_its_reason(tmp_path, error, message):
    db = FakeDatabase()
    c = await collection(db, titles=["A"])
    engine = make_engine(db, tmp_path)

    async def change():
        raise error

    done = await finished(db, (await engine.start_curation(c, JobKind.RECOMPUTE, change, "recompute")).id)

    assert (done.state, done.error) == (JobState.FAILED, message)


@pytest.mark.parametrize(("request_stored", "state"), [(True, JobState.SUCCEEDED), (False, JobState.FAILED)])
async def test_a_bulk_curation_job_runs_again_after_a_restart_only_from_its_stored_request(tmp_path, request_stored, state):
    """#34: the next engine rebuilds the change from the stored request; without one it cannot."""
    db = FakeDatabase()
    c = await collection(db, titles=["A"])
    first = make_engine(db, tmp_path)
    started = asyncio.Event()

    async def held():
        started.set()
        await asyncio.Event().wait()

    job = await first.start_curation(c, JobKind.BULK_SUGGESTIONS, held, "apply suggestions", actor="alice",
                                     request={"field": "title"} if request_stored else None)
    await started.wait()
    rebuilt: list[tuple] = []
    probe = Concurrency()

    def work(kind, c, request, actor):
        rebuilt.append((kind, request, actor))
        return probe.work(kind, c, request, actor)

    await first.shutdown()
    second = make_engine(db, tmp_path)
    second.curation_work = work
    await second.recover()
    second.start_resumes()
    done = await finished(db, job.id)

    assert done.state is state
    assert rebuilt == ([(JobKind.BULK_SUGGESTIONS, {"field": "title"}, "alice")] if request_stored else [])
    if not request_stored:
        assert done.error == "engine restarted while job was running"


# ── cancel ───────────────────────────────────────────────────────────────────────────────────────


async def _held_scrape(engine, db, c, tmp_path):
    scraper = Scraper(tmp_path, [], hold=True)
    engine.scraper = scraper
    job = await engine.start_scrape(c)
    await scraper.running.wait()
    return job


async def _held_curation(engine, db, c, tmp_path):
    started = asyncio.Event()

    async def held():
        started.set()
        await asyncio.Event().wait()

    job = await engine.start_curation(c, JobKind.RECOMPUTE, held, "recompute", request={})
    await started.wait()
    return job


async def _held_metadata(engine, db, c, tmp_path, monkeypatch):
    calls = Calls(jobs_mod.suggest_metadata_one, hold_after=0)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", calls)
    job = await engine.start_llm_metadata(c)
    await calls.held.wait()
    return job


@pytest.mark.parametrize("start_held", [_held_scrape, _held_curation, _held_metadata])
async def test_a_cancel_fails_the_running_job_naming_who_cancelled_it(tmp_path, monkeypatch, start_held):
    db = FakeDatabase()
    c = await collection(db, titles=["A"])
    engine = make_engine(db, tmp_path)
    extra = (monkeypatch,) if start_held is _held_metadata else ()
    job = await start_held(engine, db, c, tmp_path, *extra)

    cancelled = await engine.cancel(CID, actor="alice")

    assert (cancelled.id, cancelled.state, cancelled.error) == (job.id, JobState.FAILED, "cancelled by alice")
    assert engine.active_for(CID) is None


async def test_a_second_job_on_a_busy_collection_is_refused(tmp_path):
    db = FakeDatabase()
    c = await collection(db, titles=["A"])
    engine = make_engine(db, tmp_path)
    await _held_curation(engine, db, c, tmp_path)

    with pytest.raises(jobs_mod.JobConflict, match="a job is already running for example.org"):
        await engine.start_llm_patterns(c)
    await engine.shutdown()


# ── progress throttling ──────────────────────────────────────────────────────────────────────────


async def _scrape_reporting(tmp_path, monkeypatch, progress, *, every_s: float):
    monkeypatch.setattr(jobs_mod, "PROGRESS_EVERY_S", every_s)
    db = FakeDatabase()
    c = await collection(db, titles=["A"])
    scraper = Scraper(tmp_path, [], progress=progress, hold=True)
    engine = make_engine(db, tmp_path, scraper=scraper)
    job = await engine.start_scrape(c)
    await scraper.running.wait()
    return db, engine, job


async def test_progress_inside_the_throttle_interval_is_held_then_published_when_it_ends(tmp_path, monkeypatch):
    interval = 0.1
    db, engine, job = await _scrape_reporting(tmp_path, monkeypatch, [{"docs": 1}, {"docs": 2}], every_s=interval)

    held_back = (await db.get_job(job.id)).progress["docs"]
    published = await until(lambda: _docs_when(db, job.id, 2))
    await engine.shutdown()

    assert (held_back, published) == (1, 2)


async def _docs_when(db, job_id, n):
    return (await db.get_job(job_id)).progress.get("docs") == n and n


async def test_an_urgent_progress_key_is_published_at_once_inside_the_interval(tmp_path, monkeypatch):
    """A restart depends on the crawler's pid: it must reach the job row without waiting 3 s."""
    db, engine, job = await _scrape_reporting(tmp_path, monkeypatch, [{"docs": 1}, {"docs": 2, "pid": 4242}],
                                              every_s=60)

    row = await db.get_job(job.id)
    await engine.shutdown()

    assert (row.progress["docs"], row.progress["pid"], row.external_ref) == (2, 4242, "4242")
