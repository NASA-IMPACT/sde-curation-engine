"""POST /scrape end-to-end through the JobManager with the fake crawler."""

from sde_curation.engine.text import content_hash
from tests.conftest import classify, wait_job


async def test_scrape_success_ingests_dump_and_sets_status(crawler_client):
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    r = await crawler_client.post("/api/collections/ex.org/scrape")
    assert r.status_code == 202 and r.json()["state"] == "running"
    # second start while running → 409, and the collection cannot be deleted under a running job
    assert (await crawler_client.post("/api/collections/ex.org/scrape")).status_code == 409
    assert (await crawler_client.delete("/api/collections/ex.org")).status_code == 409

    job = await wait_job(crawler_client, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["docs"] == 8, job
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "scraped" and c["dump_count"] == 8
    dump = (await crawler_client.get("/api/collections/ex.org/dump?limit=3")).json()
    assert dump["total"] == 8 and len(dump["items"]) == 3 and "full_text" not in dump["items"][0]
    assert (await crawler_client.get("/api/collections/ex.org/dump?q=p7")).json()["total"] == 1
    hist = (await crawler_client.get("/api/collections/ex.org/history")).json()
    assert hist[-1]["new_status"] == "scraped" and "8 documents" in hist[-1]["note"]


async def test_scrape_failure_is_visible(crawler_client):
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 13})
    await crawler_client.post("/api/collections/ex.org/scrape")
    job = await wait_job(crawler_client, "ex.org")
    assert job["state"] == "failed" and "boom" in job["error"]
    assert (await crawler_client.get("/api/collections/ex.org")).json()["status"] == "backlog"
    page = await crawler_client.get("/")
    assert "j-failed" in page.text and "Scrape</button>" in page.text  # failed → back to the Scrape action


async def test_rescrape_of_live_collection_flags_recuration(crawler_client):
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 5})
    await crawler_client.post("/api/collections/ex.org/scrape")
    await wait_job(crawler_client, "ex.org")
    await crawler_client.post("/api/collections/ex.org/recompute")
    await classify(crawler_client)
    assert (await crawler_client.post("/api/collections/ex.org/promote")).status_code == 200
    for s in ("config_generated", "live"):
        assert (await crawler_client.post("/api/collections/ex.org/status", json={"status": s})).status_code == 200
    await crawler_client.post("/api/collections/ex.org/scrape")
    await wait_job(crawler_client, "ex.org")
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "scraped" and c["needs_recuration"] is True and c["delta_count"] == 0


async def test_ingest_strips_nul_bytes(crawler_client):
    """PDF text extraction can emit NUL, which Postgres text rejects: the whole load used to fail."""
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 5})
    jobs = crawler_client.app.state.jobs
    docs = [{"url": "https://ex.org/a.pdf", "title": "A\x00", "full_text": "x\x00y", "content_type": "application/pdf"}]
    failures = [{"url": "https://ex.org/b", "reason": "fail", "detail": "bad\x00byte"}]
    assert await jobs.ingest_dump("ex.org", docs, failures) == 1
    dump = await jobs.db.load_dump("ex.org")
    assert dump[0].scraped_title == "A" and dump[0].content_hash == content_hash("xy")


async def test_ingest_keeps_one_spelling_per_page(crawler_client):
    """A site that links to the same page as http/https, with/without a trailing slash, with a
    #fragment, or under a path that redirects to it gets it crawled once per spelling; curators
    must see it once, under the preferred spelling. A URL alone on its page is not touched."""
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 5})
    jobs = crawler_client.app.state.jobs
    docs = [
        {"url": "http://ex.org/a", "title": "A (http)", "full_text": "a"},
        {"url": "https://ex.org/a", "title": "A", "full_text": "a"},
        {"url": "https://ex.org/map/", "title": "Map (slash)", "full_text": "m"},
        {"url": "https://ex.org/map", "title": "Map", "full_text": "m"},
        {"url": "https://ex.org/faq#top", "title": "FAQ", "full_text": "f"},
        {"url": "https://ex.org/faq", "title": "FAQ", "full_text": "f"},
        {"url": "https://ex.org/maps", "final_url": "https://ex.org/map", "title": "Map", "full_text": "m"},  # redirect
        {"url": "https://www.ex.org/map/", "title": "Map (www)", "full_text": "m"},
        {"url": "http://ex.org/only-http", "title": "B", "full_text": "b"},
        {"url": "https://ex.org/only-slash/", "title": "C", "full_text": "c"},
    ]
    assert await jobs.ingest_dump("ex.org", docs) == 5
    dump = (await crawler_client.get("/api/collections/ex.org/dump?limit=10")).json()
    assert sorted(i["url"] for i in dump["items"]) == [
        "http://ex.org/only-http", "https://ex.org/a", "https://ex.org/faq", "https://ex.org/map", "https://ex.org/only-slash/",
    ]
