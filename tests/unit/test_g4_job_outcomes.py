"""What a finished job leaves behind, on the in-memory FakeDatabase: a crawl whose documents file is
broken keeps the earlier crawl; the validation gate's history note and the legacy re-curation flag;
and when a running job's progress reaches the job row and the open pages. Replaces checks of the old
tests/e2e/test_scale.py, tests/e2e/test_validate.py and tests/integration/test_progress_throttle.py
(TEST-STRATEGY-2026-10-09.md, P4)."""

import asyncio

import pytest

import sde_curation.jobs as jobs_mod
from sde_curation.models import JobKind, JobRun, JobState, Status
from tests.support.engine import (
    BUCKET,
    CID,
    Scraper,
    StatusIndexer,
    collection,
    finished,
    make_engine,
    promoted,
    report,
)
from tests.support.fake_db import FakeDatabase

PAGES = 3
QUICK_GATE = {"validation_delay_s": 0, "validation_poll_interval_s": 0.01, "validation_timeout_s": 0}
OLD_RULE_REASON = "test-index validation failed (second_pass): 3/7 indexed"  # what the pre-2026-09-17 rule wrote


# ── a crawl whose documents file is broken ───────────────────────────────────────────────────────


class BrokenDocuments(Scraper):
    """A crawler that finishes but leaves a documents file the ingest cannot read."""

    def __init__(self, tmp_path, text: str):
        super().__init__(tmp_path, [])
        self.text = text

    async def _result(self, method, on_progress):
        result = await super()._result(method, on_progress)  # its documents source is documents.json
        (self.dir / "documents.json").write_text(self.text)
        return result


@pytest.mark.parametrize(("text", "message"), [
    ('{"url": "https://example.org/x"}', "not a JSON array"),
    ('[{"url": "https://example.org/x", "title": ', "not valid JSON"),
], ids=["an object, not a list", "cut off"])
async def test_a_crawl_with_a_broken_documents_file_fails_and_keeps_the_earlier_crawl(tmp_path, text, message):
    db = FakeDatabase()
    c = await collection(db, titles=["Kept 1", "Kept 2"])
    engine = make_engine(db, tmp_path, scraper=BrokenDocuments(tmp_path, text))

    job = await engine.start_scrape(c)
    done = await finished(db, job.id)
    await engine.shutdown()

    assert done.state is JobState.FAILED and message in done.error
    assert sorted(d.scraped_title for d in await db.load_dump(CID)) == ["Kept 1", "Kept 2"]


# ── the validation gate: what it writes on the collection ────────────────────────────────────────


@pytest.fixture
def s3(aws):
    import boto3

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    return client


async def _index_to_test(db, tmp_path, monkeypatch, visible: int) -> None:
    async def index_read(settings, *, collection_key, run_id, target, expected_titles):
        return report(PAGES, visible)

    monkeypatch.setattr(jobs_mod, "validate_direct", index_read)
    engine = make_engine(db, tmp_path, indexer=StatusIndexer(), **QUICK_GATE)
    job, _ = await engine.start_index(await db.get_collection(CID), "test", actor="alice")
    await finished(db, job.id)
    await engine.shutdown()


async def test_a_failed_validation_says_so_in_the_history_and_raises_no_recuration_flag(tmp_path, monkeypatch, s3):
    """A failed index is not a curation problem: "needs re-indexing", never "needs re-curation"."""
    db = FakeDatabase()
    await promoted(db, titles=["A", "B", "C"])

    await _index_to_test(db, tmp_path, monkeypatch, visible=PAGES - 1)

    c = await db.get_collection(CID)
    last = [h for h in db._status_history if h["collection_id"] == CID][-1]  # no history read on the fake
    assert (c.status, c.needs_recuration) == (Status.CURATED, False)
    assert last["note"].startswith(f"validation FAILED (direct): {PAGES - 1}/{PAGES} indexed")
    assert last["note"].endswith("needs re-indexing")


async def test_a_passing_validation_clears_a_recuration_flag_an_old_failed_validation_raised(tmp_path, monkeypatch, s3):
    db = FakeDatabase()
    await promoted(db, titles=["A", "B", "C"])
    await db.set_flag(CID, True, OLD_RULE_REASON)

    await _index_to_test(db, tmp_path, monkeypatch, visible=PAGES)

    c = await db.get_collection(CID)
    assert (c.status, c.needs_recuration, c.recuration_reason) == (Status.CONFIG_GENERATED, False, None)


# ── progress: written and announced at most once per PROGRESS_EVERY_S ────────────────────────────

EVERY_S = 0.2


async def _watched_job(tmp_path, monkeypatch):
    """A running Suggest metadata job and every job event the open pages receive."""
    monkeypatch.setattr(jobs_mod, "PROGRESS_EVERY_S", EVERY_S)
    db = FakeDatabase()
    c = await collection(db, titles=["A"])
    engine = make_engine(db, tmp_path)
    job = await db.insert_job(JobRun(collection_id=CID, kind=JobKind.LLM_METADATA, state=JobState.RUNNING))
    seen: list[dict] = []
    engine.bus.listeners.append(lambda event, data: seen.append(data["job"]) if "job" in data else None)
    return db, engine, c, job, seen


async def test_a_job_that_ends_is_announced_at_once_and_no_held_back_progress_follows_it(tmp_path, monkeypatch):
    db, engine, c, job, seen = await _watched_job(tmp_path, monkeypatch)
    progress = engine._progress_cb(c, job)
    await progress({"done": 1})
    await progress({"done": 2})  # inside the interval: held back

    await db.finish_job(job, JobState.SUCCEEDED)
    engine._emit(c, job)
    announced = len(seen)
    await asyncio.sleep(EVERY_S * 1.5)

    assert (seen[-1]["state"], seen[-1]["progress"]["done"]) == (JobState.SUCCEEDED, 2)
    assert len(seen) == announced
    assert (await db.get_job(job.id)).state is JobState.SUCCEEDED


async def test_a_phase_change_is_written_and_announced_at_once_inside_the_interval(tmp_path, monkeypatch):
    """A new phase changes what the pages show (indexing, validating): it never waits."""
    db, engine, c, job, seen = await _watched_job(tmp_path, monkeypatch)
    progress = engine._progress_cb(c, job)
    await progress({"done": 1})

    await progress({"phase": "indexing"})

    assert [s["progress"].get("phase") for s in seen] == [None, "indexing"]
    assert (await db.get_job(job.id)).progress["phase"] == "indexing"
    await engine.shutdown()
