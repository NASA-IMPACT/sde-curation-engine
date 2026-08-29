"""POST /scrape end-to-end through the JobManager with the fake crawler."""

import asyncio
import sys
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.web.app import create_app
from tests.test_scrape_backend import FAKE_RUN_PY


@pytest.fixture
async def client(tmp_path):
    root = tmp_path / "crawler"
    root.mkdir()
    (root / "run.py").write_text(FAKE_RUN_PY)
    settings = Settings(
        data_dir=tmp_path / "data", crawler_root=root, crawler_python=Path(sys.executable),
        scrape_poll_interval_s=0.05, llm_provider="fake",
    )
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        c.app = app
        yield c


async def wait_job(client, cid, timeout=10):
    for _ in range(int(timeout / 0.1)):
        jobs = (await client.get(f"/api/collections/{cid}/jobs")).json()
        if jobs and jobs[0]["state"] in ("succeeded", "failed"):
            return jobs[0]
        await asyncio.sleep(0.1)
    raise AssertionError("job did not finish")


async def test_scrape_success_ingests_dump_and_sets_status(client):
    await client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    r = await client.post("/api/collections/ex.org/scrape")
    assert r.status_code == 202 and r.json()["state"] == "running"
    # second start while running → 409
    assert (await client.post("/api/collections/ex.org/scrape")).status_code == 409

    job = await wait_job(client, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["docs"] == 8, job
    c = (await client.get("/api/collections/ex.org")).json()
    assert c["status"] == "scraped" and c["dump_count"] == 8
    dump = (await client.get("/api/collections/ex.org/dump?limit=3")).json()
    assert len(dump) == 3 and "full_text" not in dump[0] and dump[0]["scraped_title"].startswith("Page")
    hist = (await client.get("/api/collections/ex.org/history")).json()
    assert hist[-1]["new_status"] == "scraped" and "8 documents" in hist[-1]["note"]


async def test_scrape_failure_is_visible(client):
    await client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 13})
    await client.post("/api/collections/ex.org/scrape")
    job = await wait_job(client, "ex.org")
    assert job["state"] == "failed" and "boom" in job["error"]
    assert (await client.get("/api/collections/ex.org")).json()["status"] == "backlog"
    page = await client.get("/")
    assert "j-failed" in page.text and "Scrape</button>" in page.text


async def test_rescrape_of_live_collection_flags_recuration(client):
    await client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 5})
    for s in ("scraped", "curating", "curated", "config_generated", "live"):
        assert (await client.post("/api/collections/ex.org/status", json={"status": s})).status_code == 200
    await client.post("/api/collections/ex.org/scrape")
    await wait_job(client, "ex.org")
    c = (await client.get("/api/collections/ex.org")).json()
    assert c["status"] == "scraped" and c["needs_recuration"] is True
