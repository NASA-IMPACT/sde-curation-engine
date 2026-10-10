"""The scrape job on the in-memory FakeDatabase: crawl (or reuse a crawl), ingest it as the dump,
mark the collection scraped; fail on a crawler error; carry on after an engine restart."""

from datetime import timedelta

import pytest

from sde_curation.backends.scrape import ScrapeError
from sde_curation.engine.text import content_hash
from sde_curation.models import JobKind, JobRun, JobState, Status, utcnow
from tests.support.engine import CID, Scraper, collection, finished, make_engine, promoted, restart
from tests.support.fake_db import FakeDatabase

CRAWL = [
    {"url": f"https://{CID}/a", "title": "A", "full_text": "alpha\x00text"},  # the PDF extractor's NUL
    {"url": f"https://{CID}/b", "title": "B", "full_text": "beta"},
    {"title": "a record without a URL is read but not stored"},
]
STORED_PAGES = 2
CRAWL_FAILURES = [{"url": f"https://{CID}/gone", "reason": "http_404", "status": 404}]


async def _scrape(tmp_path, scraper, *, reuse=False, db=None):
    db = db or FakeDatabase()
    c = await db.get_collection(CID) or await collection(db, titles=["Old page"])
    engine = make_engine(db, tmp_path, scraper=scraper)
    job = await engine.start_scrape(c, actor="alice", reuse=reuse)
    return db, await finished(db, job.id)


@pytest.mark.parametrize(("reuse", "method"), [(False, "run"), (True, "fetch_existing")])
async def test_a_scrape_replaces_the_dump_and_marks_the_collection_scraped(tmp_path, reuse, method):
    scraper = Scraper(tmp_path, CRAWL, failures=CRAWL_FAILURES)

    db, job = await _scrape(tmp_path, scraper, reuse=reuse)

    assert scraper.calls == [method]
    assert job.state is JobState.SUCCEEDED
    assert (job.progress["docs"], job.progress["failures"], job.external_ref) == (STORED_PAGES, 1, "crawl-1")
    assert await db.dump_content_hashes(CID) == {  # the NUL is stripped: Postgres text cannot hold it
        f"https://{CID}/a": content_hash("alphatext"), f"https://{CID}/b": content_hash("beta")}
    assert await db.load_dump_failures(CID) == {f"https://{CID}/gone": "http_404"}
    assert await db.load_deltas(CID) == []  # deltas against the old dump are meaningless
    assert (await db.get_collection(CID)).status is Status.SCRAPED


async def test_a_recrawl_after_a_promote_flags_the_collection_for_recuration(tmp_path):
    db = FakeDatabase()
    await promoted(db, titles=["Old page"])

    _, job = await _scrape(tmp_path, Scraper(tmp_path, CRAWL), db=db)

    c = await db.get_collection(CID)
    assert job.state is JobState.SUCCEEDED
    assert c.needs_recuration is True and "after 1 URLs were promoted" in c.recuration_reason


@pytest.mark.parametrize(("error", "message"), [
    (ScrapeError("crawler exited 2"), "crawler exited 2"),
    (RuntimeError("boom"), "RuntimeError: boom"),  # an unexpected crash still ends the job
])
async def test_a_crawler_error_fails_the_scrape_with_its_message(tmp_path, error, message):
    db, job = await _scrape(tmp_path, Scraper(tmp_path, CRAWL, fail=error))

    assert (job.state, job.error) == (JobState.FAILED, message)
    assert (await db.get_collection(CID)).status is not Status.SCRAPED


async def test_a_deploy_leaves_the_scrape_running_and_the_next_engine_follows_the_same_crawl(tmp_path):
    """The crawl runs on the crawler host and outlives the engine: never a new job or a second crawl."""
    db = FakeDatabase()
    c = await collection(db, titles=["Old page"])
    first_scraper = Scraper(tmp_path, CRAWL, hold=True)
    first = make_engine(db, tmp_path, scraper=first_scraper)
    job = await first.start_scrape(c)
    await first_scraper.running.wait()

    second_scraper = Scraper(tmp_path, CRAWL)
    second = await restart(first, db, tmp_path, scraper=second_scraper)
    done = await finished(db, job.id)
    await second.shutdown()

    assert (first_scraper.calls, second_scraper.calls) == (["run"], ["resume"])
    assert second_scraper.since == job.started_at
    assert (done.state, done.progress["restarts"], done.progress["docs"]) == (JobState.SUCCEEDED, 1, STORED_PAGES)


@pytest.mark.parametrize(("ended_ago", "reopened"), [
    (timedelta(hours=23), True),
    (timedelta(hours=25), False),  # an old crawl nobody is waiting for stays failed
])
async def test_a_scrape_an_older_engine_failed_at_shutdown_is_reopened_only_within_a_day(tmp_path, ended_ago, reopened):
    """Engines before 2026-09-25 recorded a scrape as 'cancelled by shutdown' while the crawl carried on."""
    db = FakeDatabase()
    await collection(db, titles=["Old page"])
    job = await db.insert_job(JobRun(collection_id=CID, kind=JobKind.SCRAPE, state=JobState.FAILED,
                                     error="cancelled by shutdown", finished_at=utcnow() - ended_ago))
    scraper = Scraper(tmp_path, CRAWL)
    engine = make_engine(db, tmp_path, scraper=scraper)

    await engine.recover()
    done = await finished(db, job.id)
    await engine.shutdown()

    assert scraper.calls == (["resume"] if reopened else [])
    assert done.state is (JobState.SUCCEEDED if reopened else JobState.FAILED)


@pytest.mark.parametrize(("summary", "progress", "expected"), [
    ({"documents_scraped": 8}, {"docs": 5}, 8),  # the crawl's own count wins
    ({}, {"docs": 5}, 5),  # no summary: what the job watched in the crawler log
    ({"documents_scraped": 7.0}, {}, 7),  # a count written as a float
    ({}, {}, None),  # nothing known: the status just counts up
    ({"documents_scraped": 0}, {"docs": 0}, None),
    ({"documents_scraped": "8"}, {}, None),  # not a number
])
def test_the_ingest_knows_how_many_pages_it_is_about_to_read(summary, progress, expected):
    """The crawl status reads "x of y" pages while the ingest streams; y comes from here
    (jobs._expected_docs, the `ingest_total` progress field)."""
    from sde_curation.jobs import _expected_docs

    assert _expected_docs(summary, progress) == expected
