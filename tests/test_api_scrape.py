"""POST /scrape end-to-end through the JobManager with the fake crawler."""

from tests.conftest import wait_job


async def test_scrape_success_ingests_dump_and_sets_status(crawler_client):
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    r = await crawler_client.post("/api/collections/ex.org/scrape")
    assert r.status_code == 202 and r.json()["state"] == "running"
    # second start while running → 409
    assert (await crawler_client.post("/api/collections/ex.org/scrape")).status_code == 409

    job = await wait_job(crawler_client, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["docs"] == 8, job
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "scraped" and c["dump_count"] == 8
    dump = (await crawler_client.get("/api/collections/ex.org/dump?limit=3")).json()
    assert len(dump) == 3 and "full_text" not in dump[0] and dump[0]["scraped_title"].startswith("Page")
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
    await crawler_client.post("/api/collections/ex.org/promote")
    for s in ("config_generated", "live"):
        assert (await crawler_client.post("/api/collections/ex.org/status", json={"status": s})).status_code == 200
    await crawler_client.post("/api/collections/ex.org/scrape")
    await wait_job(crawler_client, "ex.org")
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "scraped" and c["needs_recuration"] is True and c["delta_count"] == 0
