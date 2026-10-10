"""JobManager decisions on the in-memory FakeDatabase: restarts, resumes and cancels.

An engine restart is two JobManagers on one FakeDatabase: the first is shut down in the middle of a
job (a deploy), the second recovers and resumes it. Backends are fakes: an indexer that records what
it started and stopped, and wrappers around the model calls that count and can hold a call.
"""

import asyncio

import sde_curation.jobs as jobs_mod
from sde_curation.jobs import JobManager
from sde_curation.models import (
    Collection,
    ConnectorType,
    CuratedUrl,
    Division,
    DocumentType,
    IndexRun,
    JobKind,
    JobRun,
    JobState,
)
from tests.support.engine import (
    BUCKET,
    CID,
    Calls,
    Indexer,
    collection,
    finished,
    make_engine,
    promoted,
    restart,
)
from tests.support.fake_db import FakeDatabase

# ── Bugs from REVIEW-SINCE-DEV-MERGE-2026-10-08.md: each test failed before its fix ──────────────────


async def test_a_restart_during_the_dispatch_starts_no_second_indexer_task(tmp_path, aws):
    import boto3

    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
    db = FakeDatabase()
    c = await collection(db, titles=["A", "B", "C"])
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=f"https://{CID}/p{i:03d}", title=t,
                                              division=Division.HELIOPHYSICS, document_type=DocumentType.DOCUMENTATION)
                                   for i, t in enumerate(["A", "B", "C"], 1)])
    indexer = Indexer(slow_first_answer=True)
    first = make_engine(db, tmp_path, indexer=indexer)
    _, run = await first.start_index(await db.get_collection(c.collection_id), "test")
    await asyncio.wait_for(indexer.dispatching.wait(), 10)  # the task started; its answer has not come back

    second = await restart(first, db, tmp_path, indexer=indexer)
    await asyncio.sleep(0.3)
    await second.shutdown()

    assert indexer.started == [run.run_id]  # one task for the run: the resume adopts it


async def test_a_cancel_during_the_dispatch_stops_the_task_it_started(tmp_path, aws):
    import boto3

    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
    db = FakeDatabase()
    c = await promoted(db, titles=["A", "B"])
    indexer = Indexer(slow_first_answer=True)
    engine = make_engine(db, tmp_path, indexer=indexer)
    await engine.start_index(c, "test")
    await asyncio.wait_for(indexer.dispatching.wait(), 10)

    cancelling = asyncio.create_task(engine.cancel(CID, actor="alice"))
    await asyncio.sleep(0.05)
    indexer.answer.set()  # RunTask answers after the cancel
    job = await asyncio.wait_for(cancelling, 10)

    assert (job.state, job.error) == (JobState.FAILED, "cancelled by alice")
    assert indexer.stopped == ["task/1"]


async def test_cancelling_an_index_job_waiting_to_resume_stops_its_task(tmp_path):
    db = FakeDatabase()
    c = await collection(db, titles=["A"])
    run = await db.insert_index_run(IndexRun(run_id="r-1", collection_id=c.collection_id, target="test",
                                             external_ref="task/1"))
    await db.insert_job(JobRun(collection_id=CID, kind=JobKind.INDEX_TEST, state=JobState.RUNNING,
                               run_id=run.run_id, external_ref="task/1", progress={"phase": "indexing"}))
    indexer = Indexer()
    engine = make_engine(db, tmp_path, indexer=indexer, resume_start_delay_s=60)  # still waiting to resume
    await engine.recover()

    job = await engine.cancel(CID, actor="alice")

    assert job.state is JobState.FAILED and job.error == "cancelled by alice"
    assert indexer.stopped == ["task/1"]
    assert (await db.get_index_run("r-1")).state == "failed"


async def test_an_index_job_past_its_restart_limit_stops_its_task(tmp_path):
    db = FakeDatabase()
    await collection(db, titles=["A"])
    await db.insert_index_run(IndexRun(run_id="r-1", collection_id=CID, target="test", external_ref="task/1"))
    job = await db.insert_job(JobRun(collection_id=CID, kind=JobKind.INDEX_TEST, state=JobState.RUNNING, run_id="r-1",
                                     external_ref="task/1", progress={"phase": "indexing", "restarts": 3}))
    indexer = Indexer()
    engine = make_engine(db, tmp_path, indexer=indexer, resume_max_restarts=3)

    await engine.recover()

    assert (await db.get_job(job.id)).error == "stopped resuming after 3 engine restarts"
    assert indexer.stopped == ["task/1"]
    assert (await db.get_index_run("r-1")).state == "failed"


async def test_one_failing_resume_does_not_strand_the_others(tmp_path, monkeypatch):
    from psycopg_pool import PoolTimeout

    db = FakeDatabase()
    for i in range(3):
        await db.insert_collection(Collection(collection_id=f"c{i}.org", name=f"c{i}", seed_url=f"https://c{i}.org",
                                              connector=ConnectorType.CRAWLER, max_pages=10))
        await db.insert_job(JobRun(collection_id=f"c{i}.org", kind=JobKind.VALIDATE, state=JobState.RUNNING))

    def resumers(self):
        async def resume(c, job):
            await self.db.finish_job(job, JobState.SUCCEEDED)
        return {JobKind.VALIDATE: resume}

    monkeypatch.setattr(JobManager, "_resumers", resumers)
    engine = make_engine(db, tmp_path)
    await engine.recover()
    get_collection, failed = db.get_collection, {"once": False}

    async def busy_once(cid):
        if not failed["once"]:
            failed["once"] = True
            raise PoolTimeout("couldn't get a connection after 30.00 sec")
        return await get_collection(cid)

    db.get_collection = busy_once
    engine.start_resumes()
    await asyncio.sleep(0.3)

    states = [(await db.get_job(i)).state for i in (1, 2, 3)]
    assert engine._pending_resume == {}  # no collection stays locked by a resume that never runs
    assert JobState.RUNNING not in states and states.count(JobState.SUCCEEDED) == 2
    await engine.shutdown()


async def test_a_resumed_redo_all_metadata_run_asks_every_page(tmp_path, monkeypatch):
    db = FakeDatabase()
    titles = [f"Page {i}" for i in range(1, 9)]
    c = await collection(db, titles=titles)
    urls = [d.url for d in await db.load_deltas(CID)]
    await db.set_delta_ai(CID, [{"url": u, "title": "old", "title_conf": "high", "division": "Heliophysics",
                                 "division_conf": "high", "document_type": "Documentation",
                                 "document_type_conf": "high", "model": "an older run"} for u in urls])
    first_calls = Calls(jobs_mod.suggest_metadata_one, hold_after=3)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", first_calls)
    first = make_engine(db, tmp_path)
    job = await first.start_llm_metadata(c, only_missing=False)  # redo every page
    await asyncio.wait_for(first_calls.held.wait(), 10)

    second_calls = Calls(first_calls.fn)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", second_calls)
    second = await restart(first, db, tmp_path)
    await finished(db, job.id)
    await second.shutdown()

    assert set(first_calls.asked[:3]) | set(second_calls.asked) == set(urls)


async def test_a_page_that_fails_is_counted_once_across_a_restart(tmp_path, monkeypatch):
    db = FakeDatabase()
    c = await collection(db, titles=[f"Page {i}" for i in range(1, 9)])
    urls = [d.url for d in await db.load_deltas(CID)]
    bad = {urls[1]}  # fails every time it is asked, before the restart
    first_calls = Calls(jobs_mod.suggest_metadata_one, hold_after=4, fail=bad)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", first_calls)
    first = make_engine(db, tmp_path)
    job = await first.start_llm_metadata(c)
    await asyncio.wait_for(first_calls.held.wait(), 10)

    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", Calls(first_calls.fn, fail=bad))
    second = await restart(first, db, tmp_path)
    done = await finished(db, job.id)
    await second.shutdown()

    p = done.progress
    assert (p["failed"], p["done"] + p["failed"]) == (1, len(urls))


async def test_answers_being_saved_at_shutdown_are_not_asked_again(tmp_path, monkeypatch):
    db = FakeDatabase()
    c = await collection(db, titles=[f"Page {i}" for i in range(1, 31)])  # more than one batch of answers
    calls = Calls(jobs_mod.suggest_metadata_one)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", calls)
    set_delta_ai, saving = db.set_delta_ai, asyncio.Event()

    async def slow_first_save(cid, items, **kw):
        if not saving.is_set():
            saving.set()
            await asyncio.Event().wait()  # the engine goes down during this write
        return await set_delta_ai(cid, items, **kw)

    db.set_delta_ai = slow_first_save
    first = make_engine(db, tmp_path)
    job = await first.start_llm_metadata(c)
    await asyncio.wait_for(saving.wait(), 10)

    second = await restart(first, db, tmp_path)
    await finished(db, job.id)
    await second.shutdown()

    twice = sorted({u for u in calls.asked if calls.asked.count(u) > 1})
    assert twice == []


async def test_a_restart_during_the_duplicate_title_pass_still_tells_every_group_apart(tmp_path, monkeypatch):
    db = FakeDatabase()
    c = await collection(db, titles=["Alpha", "Alpha", "Beta", "Beta", "Solo"])
    original = jobs_mod.suggest_distinct_titles
    held = asyncio.Event()

    async def hold_first(*a, **k):
        held.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", hold_first)
    first = make_engine(db, tmp_path, llm_dedupe_titles=True)
    job = await first.start_llm_metadata(c)
    await asyncio.wait_for(held.wait(), 10)

    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", original)
    second = await restart(first, db, tmp_path, llm_dedupe_titles=True)
    await finished(db, job.id)
    await second.shutdown()

    assert (await db.duplicate_title_counts(CID))["delta_urls"] == 0


async def test_a_prod_publish_resumed_twice_counts_every_document_once(tmp_path, monkeypatch):
    """The publisher writes 2 of 7 documents and the engine restarts; the resumed attempt is
    interrupted before it reports anything; the third attempt finds those 2 already in prod and
    writes the other 5. The run's report must say 7 documents published, the size of the export."""
    db = FakeDatabase()
    c = await collection(db, titles=["A"])
    test_run = await db.insert_index_run(IndexRun(run_id="t-1", collection_id=CID, target="test", exported=7))
    test_run.state, test_run.validation = "succeeded", {"count_matches": True, "title_match_rate": 1.0}
    await db.update_index_run(test_run)
    attempts: list[int] = []
    reported = asyncio.Event()
    second_started = asyncio.Event()

    class Publisher:
        async def run(self, key, run_id, source_run_id, progress, *, allow_high_deletion=False):
            attempts.append(len(attempts) + 1)
            if len(attempts) == 1:
                await progress({"indexed": 2})
                reported.set()
                await asyncio.Event().wait()
            if len(attempts) == 2:
                second_started.set()
                await asyncio.Event().wait()
            return {"run_id": run_id, "collection_key": key, "target": "prod", "state": "succeeded",
                    "documents_in_export": 7, "indexed": 5, "changed": 5, "unchanged": 2}

    async def no_validation(self, c, job, run, progress, note=None):
        return None

    monkeypatch.setattr(JobManager, "_validate_prod", no_validation)
    publisher = Publisher()
    kw = {"publisher": lambda: publisher, "opensearch_endpoint_prod": "https://prod.example.aoss.amazonaws.com"}
    first = make_engine(db, tmp_path, **kw)
    job, run = await first.start_index(await db.get_collection(c.collection_id), "prod")
    await asyncio.wait_for(reported.wait(), 10)
    second = await restart(first, db, tmp_path, **kw)
    await asyncio.wait_for(second_started.wait(), 10)
    third = await restart(second, db, tmp_path, **kw)
    await finished(db, job.id)
    await third.shutdown()

    assert attempts == [1, 2, 3]
    assert (await db.get_index_run(run.run_id)).status["indexed"] == 7