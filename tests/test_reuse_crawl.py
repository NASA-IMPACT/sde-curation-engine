"""Load an existing crawl instead of re-running the crawler."""

import asyncio
import json

import pytest

from sde_curation.models import Collection, Division
from tests.conftest import wait_job
from tests.test_scrape_backend import aws, ssm_env  # noqa: F401 - pytest fixtures

COLL = Collection(collection_id="ex.org", name="Ex", seed_url="https://ex.org", division=Division.GENERAL,
                  connector="crawler2", max_pages=10)


async def test_reuse_local_crawl_output(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    r = await c.get("/api/collections/ex.org/crawl/existing")
    assert r.json() == {"exists": False}
    assert "Load existing crawl" not in (await c.get("/collections/ex.org/step/backlog")).text
    # nothing to load yet → the job fails cleanly
    await c.post("/api/collections/ex.org/scrape?reuse=true")
    job = await wait_job(c, "ex.org")
    assert job["state"] == "failed" and "no existing crawl" in job["error"]

    # a real crawl leaves output behind; forget the collection and register it again
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    docs = c.app.state.settings.crawler_root / "output" / "collections" / "https_ex.org.json"
    first_bytes = docs.read_bytes()
    await c.app.state.db.delete_collection("ex.org")
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    ex = (await c.get("/api/collections/ex.org/crawl/existing")).json()
    assert ex["exists"] and not ex["already_loaded"] and ex["where"].endswith("https_ex.org.json")
    panel = (await c.get("/collections/ex.org/step/backlog")).text
    assert "Load existing crawl (from" in panel and "scrape?reuse=true" in panel
    assert "or load existing" in (await c.get("/collections/ex.org/header")).text

    r = await c.post("/api/collections/ex.org/scrape?reuse=true")
    assert r.status_code == 202
    job = await wait_job(c, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["reused"] is True and job["progress"]["docs"] == 8
    assert docs.read_bytes() == first_bytes  # the crawler did not run again
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["status"] == "scraped" and col["dump_count"] == 8 and col["last_scraped_at"]
    ex = (await c.get("/api/collections/ex.org/crawl/existing")).json()
    assert ex["exists"] and ex["already_loaded"]
    assert "Load existing crawl" not in (await c.get("/collections/ex.org/step/scraped")).text
    assert "loaded from existing crawl" in (await c.get("/collections/ex.org/step/scraped")).text
    hist = (await c.get("/collections/ex.org?tab=activity")).text
    assert "loaded existing crawl from" in hist and "scrape.reuse" in hist


async def test_ssm_existing_and_fetch(ssm_env, tmp_path):  # noqa: F811
    _host, make, upload = ssm_env
    s = make()
    assert await s.existing(COLL) is None
    upload([{"url": "https://ex.org/a", "title": "A", "full_text": "t"}])
    ex = await s.existing(COLL)
    assert ex and ex.where == "s3://crawl-bkt/scraped_collections/https_ex.org.json" and ex.size
    seen = []

    async def cb(p):
        seen.append(dict(p))

    res = await s.fetch_existing(COLL, cb)
    assert seen == [{"reused": True}] and res.crawled_at == ex.modified and res.external_ref == "reused"
    assert json.loads(res.documents_path.read_text())[0]["url"] == "https://ex.org/a"
    assert res.summary["documents_scraped"] == 1 and res.failures_path is None  # no failures log uploaded
    assert s.ssm.commands == []  # no SSM command was issued


async def test_ssm_reads_from_the_configured_bucket_folder(ssm_env, tmp_path):  # noqa: F811
    """CRAWLER_S3_PREFIX: the prototype crawler writes under a folder, not at the bucket root."""
    import boto3

    _host, make, _upload = ssm_env
    s = make(crawler_s3_prefix="/sde-curation-engine-prototype/")
    assert s._crawl_keys(COLL)["docs"] == "sde-curation-engine-prototype/scraped_collections/https_ex.org.json"
    assert await s.existing(COLL) is None  # nothing under the folder yet
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket="crawl-bkt", Key="sde-curation-engine-prototype/scraped_collections/https_ex.org.json",
                  Body=json.dumps([{"url": "https://ex.org/a", "title": "A", "full_text": "t"}]))
    s3.put_object(Bucket="crawl-bkt", Key="sde-curation-engine-prototype/failure_logs/https_ex.org_failures_summary.json",
                  Body=json.dumps({"documents_scraped": 1}))
    s3.put_object(Bucket="crawl-bkt", Key="sde-curation-engine-prototype/failure_logs/https_ex.org_failures.jsonl",
                  Body='{"url": "https://ex.org/b", "reason": "http_403", "status": 403}\n')
    ex = await s.existing(COLL)
    assert ex and ex.where == "s3://crawl-bkt/sde-curation-engine-prototype/scraped_collections/https_ex.org.json"

    async def cb(p):
        pass

    res = await s.fetch_existing(COLL, cb)
    assert json.loads(res.documents_path.read_text())[0]["url"] == "https://ex.org/a"
    assert res.summary == {"documents_scraped": 1}
    assert [f["url"] for f in res.failures()] == ["https://ex.org/b"]  # the failures log rides along


def test_crawl_complete_means_summary_is_at_least_as_new_as_documents():
    """run.py uploads documents, then failures, then the summary — and the summary only exists once
    the crawl is over. Crawler v2 rewrites the documents object every 100 pages as a checkpoint."""
    from datetime import UTC, datetime

    from sde_curation.backends.scrape import crawl_complete

    t = datetime(2026, 9, 14, 20, 0, tzinfo=UTC)
    assert crawl_complete(t, t)  # final upload: both land in the same second
    assert crawl_complete(t, t.replace(minute=1))
    assert not crawl_complete(t.replace(minute=1), t)  # checkpoint newer than the last summary
    assert not crawl_complete(t, None)  # first crawl of the collection still running


async def test_ssm_checkpoint_of_a_running_crawl_is_not_loadable(ssm_env):  # noqa: F811
    """pds.nasa.gov: the collection was added while the crawler was mid-run; "load existing crawl"
    ingested the 1800-page checkpoint, then the 1900-page one. The engine must recognise a
    documents object with no (or an older) failure summary as unfinished and refuse it."""
    import boto3

    from sde_curation.backends.scrape import ScrapeError

    _host, make, upload = ssm_env
    s = make()
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket="crawl-bkt", Key="scraped_collections/https_ex.org.json",
        Body=json.dumps([{"url": "https://ex.org/a", "title": "A", "full_text": "t"}]),
    )
    ex = await s.existing(COLL)
    assert ex and not ex.complete

    async def cb(p):
        raise AssertionError("must not start ingesting a checkpoint")

    with pytest.raises(ScrapeError, match="checkpoint .* has not finished"):
        await s.fetch_existing(COLL, cb)

    upload()  # the crawl finishes: documents re-uploaded and the summary written after them
    ex = await s.existing(COLL)
    assert ex and ex.complete
    res = await s.fetch_existing(COLL, lambda p: asyncio.sleep(0))
    assert json.loads(res.documents_path.read_text())[0]["url"] == "https://ex.org/a"


async def test_workbench_shows_a_running_crawl_but_does_not_offer_to_load_it(crawler_client, monkeypatch):
    from datetime import UTC, datetime

    from sde_curation.backends.scrape import ExistingCrawl

    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    ckpt = ExistingCrawl(modified=datetime(2026, 9, 14, 20, 28, tzinfo=UTC), where="s3://b/scraped_collections/https_ex.org.json",
                         size=5715488, complete=False)

    async def existing(_collection):
        return ckpt

    monkeypatch.setattr(c.app.state.jobs.scraper, "existing", existing)
    ex = (await c.get("/api/collections/ex.org/crawl/existing")).json()
    assert ex["exists"] and not ex["complete"] and not ex["loadable"] and not ex["already_loaded"]
    panel = (await c.get("/collections/ex.org/step/backlog")).text
    assert "crawl in progress" in panel and "scrape?reuse=true" not in panel
    # and Scrape is greyed out: dropping another job would only crawl the site a second time
    scrape = panel.split('hx-post="/api/collections/ex.org/scrape"')[0].rsplit("<button", 1)[1]
    assert "disabled" in scrape and "already running on the crawler host" in scrape
    header = (await c.get("/collections/ex.org/header")).text
    assert "or load existing" not in header
    assert 'hx-post="/api/collections/ex.org/scrape"' not in header and "crawl in progress on the host" in header

    ckpt.complete = True  # the summary landed: the crawl is over
    c.app.state.existing_cache.clear()
    panel = (await c.get("/collections/ex.org/step/backlog")).text
    scrape = panel.split('hx-post="/api/collections/ex.org/scrape"')[0].rsplit("<button", 1)[1]
    assert "disabled" not in scrape and "scrape?reuse=true" in panel
    assert 'hx-post="/api/collections/ex.org/scrape"' in (await c.get("/collections/ex.org/header")).text
