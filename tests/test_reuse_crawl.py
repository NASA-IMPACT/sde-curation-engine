"""Load an existing crawl instead of re-running the crawler."""

import json

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
    docs = c.app.state.settings.crawler_root / "output" / "collections" / "ex.org.json"
    first_bytes = docs.read_bytes()
    await c.delete("/api/collections/ex.org")
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    ex = (await c.get("/api/collections/ex.org/crawl/existing")).json()
    assert ex["exists"] and not ex["already_loaded"] and ex["where"].endswith("ex.org.json")
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
    assert ex and ex.where == "s3://crawl-bkt/scraped_collections/ex.org.json" and ex.size
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
    assert s._docs_key("ex.org") == "sde-curation-engine-prototype/scraped_collections/ex.org.json"
    assert await s.existing(COLL) is None  # nothing under the folder yet
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket="crawl-bkt", Key="sde-curation-engine-prototype/scraped_collections/ex.org.json",
                  Body=json.dumps([{"url": "https://ex.org/a", "title": "A", "full_text": "t"}]))
    s3.put_object(Bucket="crawl-bkt", Key="sde-curation-engine-prototype/failure_logs/ex.org_failures_summary.json",
                  Body=json.dumps({"documents_scraped": 1}))
    s3.put_object(Bucket="crawl-bkt", Key="sde-curation-engine-prototype/failure_logs/ex.org_failures.jsonl",
                  Body='{"url": "https://ex.org/b", "reason": "http_403", "status": 403}\n')
    ex = await s.existing(COLL)
    assert ex and ex.where == "s3://crawl-bkt/sde-curation-engine-prototype/scraped_collections/ex.org.json"

    async def cb(p):
        pass

    res = await s.fetch_existing(COLL, cb)
    assert json.loads(res.documents_path.read_text())[0]["url"] == "https://ex.org/a"
    assert res.summary == {"documents_scraped": 1}
    assert [f["url"] for f in res.failures()] == ["https://ex.org/b"]  # the failures log rides along
