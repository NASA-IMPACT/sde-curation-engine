"""Pages and work on separate PostgreSQL pools (test crash, 2026-10-06): page requests read on their
own pool under a statement timeout and answer 503 instead of piling up; actions and jobs keep their
own pool without one; /health stays up whatever the database does. Moved down from the old
tests/e2e/test_busy_database.py: nothing here needs a subprocess (TEST-STRATEGY-2026-10-09.md, P4)."""

import asyncio

import psycopg
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.db import db_scope
from sde_curation.models import DumpUrl
from sde_curation.web.app import create_app
from tests.support.flows import wait_job

CID = "ex.org"
API = f"/api/collections/{CID}"
READ_TIMEOUT_S = 0.5  # a page gives up after this
LOCK_HELD_S = 1.5  # longer than a page waits; a job waits it out


async def _app(tmp_path, **settings):
    return create_app(Settings(data_dir=tmp_path / "data", llm_provider="fake", **settings))


async def _started(c) -> None:
    r = await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10,
                                               "division": "Heliophysics"})
    assert r.status_code == 201, r.text
    await c.app.state.db.replace_dump(CID, [DumpUrl(collection_id=CID, url=f"https://{CID}/p{i}",
                                                    scraped_title=f"Page {i}") for i in range(1, 9)])


async def test_a_slow_page_answers_503_while_a_job_waits_as_long_as_it_needs(tmp_path, database_url):
    app = await _app(tmp_path, db_read_statement_timeout_s=READ_TIMEOUT_S, bulk_job_min_urls=0)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        c.app = app
        await _started(c)
        with psycopg.connect(database_url) as blocker:
            blocker.execute("LOCK TABLE delta_urls IN ACCESS EXCLUSIVE MODE")
            started = await c.post(f"{API}/recompute")  # a job: bulk_job_min_urls=0
            page = await c.get(f"{API}/delta")
            await asyncio.sleep(LOCK_HELD_S - READ_TIMEOUT_S)
        job = await wait_job(c, CID)
        after = await c.get(f"{API}/delta")

    assert started.status_code == 202
    assert (page.status_code, page.headers["retry-after"]) == (503, "5") and "busy" in page.json()["detail"]
    assert job["state"] == "succeeded"
    assert after.status_code == 200


async def test_busy_jobs_do_not_starve_pages_and_busy_pages_do_not_starve_actions(tmp_path):
    app = await _app(tmp_path, db_pool_size=2, db_read_pool_size=1, health_db_timeout_s=0.3)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        c.app = app
        db = app.state.db
        await _started(c)
        async with db.pool.connection(), db.pool.connection():  # every work connection taken
            page = await asyncio.wait_for(c.get(API), timeout=5)
        token = db_scope.set("read")
        async with db.read_pool.connection():  # every read connection taken
            db_scope.reset(token)
            action = await asyncio.wait_for(c.post(f"{API}/patterns", json={"type": "exclude", "match": "*/p1"}),
                                            timeout=5)
            health = await asyncio.wait_for(c.get("/health"), timeout=1)
            health_db = await c.get("/health/db")
        recovered = await c.get("/health/db")

    assert page.status_code == 200
    assert action.status_code == 201
    assert (health.status_code, health.json()) == (200, {"ok": True, "sse_clients": 0})
    assert (health_db.status_code, health_db.json()["db"]) == (503, "error: timed out")
    assert health_db.json()["pools"]["read"]["pool_available"] == 0
    assert (recovered.status_code, recovered.json()["db"]) == (200, "ok")
