"""Many curators on many collections at once: what keeps a busy database from taking the engine down.

Test, 2026-10-06: after an exclude accept on a 100k collection, the stepper and the tabs of every
open browser re-ran a 7 s count on every job event; 16 copies of it held every pooled connection,
/health (which needed one) timed out, and the ALB replaced a healthy engine. The fixes under test:
the count is stored by the recompute; page requests read on their own pool under a statement timeout
(503 instead of a pile-up) while actions and jobs keep theirs without one; identical page reads run
once at a time and are shared; /health is liveness only and /health/db reports the database.
"""

import asyncio
import sys
from pathlib import Path

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.db import Database, SingleFlight, db_scope, work_context
from sde_curation.web.app import create_app

from .conftest import FAKE_RUN_PY, wait_job


async def _client(tmp_path, **extra):
    root = tmp_path / "crawler"
    root.mkdir(exist_ok=True)
    (root / "run.py").write_text(FAKE_RUN_PY)
    app = create_app(Settings(
        data_dir=tmp_path / "data", crawler_root=root, crawler_python=Path(sys.executable),
        scrape_poll_interval_s=0.05, llm_provider="fake", llm_retry_delay_s=0, **extra,
    ))
    return app


async def _crawled(c, cid="ex.org"):
    """A collection with a crawl (8 pages: p1–p4, p6–p9) and its delta URLs computed."""
    r = await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 10,
                                               "division": "Heliophysics"})
    assert r.status_code == 201, r.text
    await c.post(f"/api/collections/{cid}/scrape")
    assert (await wait_job(c, cid))["state"] == "succeeded"
    r = await c.post(f"/api/collections/{cid}/recompute")
    assert r.status_code in (200, 202), r.text
    if r.status_code == 202:  # ran as a job (bulk_job_min_urls)
        assert (await wait_job(c, cid))["state"] == "succeeded"


async def _stored(c, cid="ex.org") -> int | None:
    return (await c.get(f"/api/collections/{cid}")).json()["excluded_count"]


# ── the excluded count is stored, not counted per view ─────────────────


async def test_the_excluded_count_follows_every_rule_change_and_matches_the_tables(crawler_client):
    c = crawler_client
    db: Database = c.app.state.db
    await _crawled(c)
    assert await _stored(c) == 0 == await db.count_excluded_by_rules("ex.org")

    r = await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p*"})
    assert r.status_code == 201, r.text
    assert await _stored(c) == 8 == await db.count_excluded_by_rules("ex.org")

    r = await c.post("/api/collections/ex.org/patterns", json={"type": "include", "match": "*/p2"})
    include_id = r.json()["pattern"]["id"]
    assert await _stored(c) == 7 == await db.count_excluded_by_rules("ex.org")  # the include wins for p2

    r = await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p3", "type": "include"})
    assert r.status_code == 200, r.text
    assert await _stored(c) == 6 == await db.count_excluded_by_rules("ex.org")  # a per-URL include too

    r = await c.delete(f"/api/collections/ex.org/patterns/{include_id}")
    assert r.status_code == 200, r.text
    assert await _stored(c) == 7 == await db.count_excluded_by_rules("ex.org")

    # the overview shows the stored number
    page = (await c.get("/collections/ex.org/tab/overview")).text
    assert "7 dump URLs excluded by rules" in page

    # a re-crawl drops the rule effects with the old delta URLs; the recompute brings them back
    await c.post("/api/collections/ex.org/scrape")
    assert (await wait_job(c, "ex.org"))["state"] == "succeeded"
    assert await _stored(c) == 0 == await db.count_excluded_by_rules("ex.org")
    await c.post("/api/collections/ex.org/recompute")
    assert await _stored(c) == 7 == await db.count_excluded_by_rules("ex.org")


async def test_an_unknown_count_is_counted_once_for_every_browser_and_stored(crawler_client, monkeypatch):
    """A collection the V11 migration left without a count (NULL): twenty tabs open its overview at
    once; the join runs once, every tab shows the same number, and it is stored for the next view."""
    c = crawler_client
    db: Database = c.app.state.db
    await _crawled(c)
    await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p*"})
    await db.execute("UPDATE collections SET excluded_count=NULL WHERE collection_id='ex.org'")  # as migrated

    runs = 0
    count = Database.count_excluded_by_rules

    async def counting(self, collection_id):
        nonlocal runs
        runs += 1
        await asyncio.sleep(0.2)  # slow enough that the tabs overlap, as the 7 s count did
        return await count(self, collection_id)

    monkeypatch.setattr(Database, "count_excluded_by_rules", counting)
    pages = await asyncio.gather(*(c.get("/collections/ex.org/tab/overview") for _ in range(20)))
    assert all(p.status_code == 200 and "8 dump URLs excluded by rules" in p.text for p in pages)
    assert runs == 1
    assert await _stored(c) == 8
    await c.get("/collections/ex.org/tab/overview")
    assert runs == 1  # stored: not counted again


# ── page requests vs. work: separate pools, a timeout only on pages ────


async def test_a_slow_page_answers_503_while_a_job_waits_as_long_as_it_needs(tmp_path, database_url):
    """A table lock held for 1.5 s: a page that needs the table gives up after the read timeout
    (0.5 s) with a 503 + Retry-After; a recompute job started meanwhile waits and succeeds."""
    app = await _client(tmp_path, db_read_statement_timeout_s=0.5, bulk_job_min_urls=0)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await _crawled(c)
        with psycopg.connect(database_url) as blocker:
            blocker.execute("LOCK TABLE delta_urls IN ACCESS EXCLUSIVE MODE")
            r = await c.post("/api/collections/ex.org/recompute")  # a job: bulk_job_min_urls=0
            assert r.status_code == 202, r.text
            page = await c.get("/api/collections/ex.org/delta")
            assert page.status_code == 503
            assert page.headers["retry-after"] == "5" and "busy" in page.json()["detail"]
            await asyncio.sleep(1.0)
        job = await wait_job(c, "ex.org")
        assert job["state"] == "succeeded", job
        assert (await c.get("/api/collections/ex.org/delta")).status_code == 200


async def test_a_job_started_from_a_page_request_works_outside_its_timeout():
    """Whatever starts a background task from a request hands it the work scope."""
    token = db_scope.set("read")
    try:
        assert work_context().run(db_scope.get) == "work"
        assert db_scope.get() == "read"
    finally:
        db_scope.reset(token)


async def test_busy_jobs_do_not_starve_pages_and_busy_pages_do_not_starve_actions(tmp_path):
    # work pool of 2: an ingest holds one for its COPY and borrows a second to record the job
    app = await _client(tmp_path, db_pool_size=2, db_read_pool_size=1, health_db_timeout_s=0.3)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        db: Database = app.state.db
        await _crawled(c)
        # every work connection taken (jobs holding them): pages still load
        async with db.pool.connection(), db.pool.connection():
            r = await asyncio.wait_for(c.get("/api/collections/ex.org"), timeout=5)
            assert r.status_code == 200
        # every read connection taken (pages piling up): an action still goes through, /health
        # stays up, and /health/db says the database side is busy
        token = db_scope.set("read")
        try:
            async with db.read_pool.connection():
                db_scope.reset(token)
                token = None
                r = await asyncio.wait_for(c.post("/api/collections/ex.org/patterns",
                                                  json={"type": "exclude", "match": "*/p1"}), timeout=5)
                assert r.status_code == 201, r.text
                h = await asyncio.wait_for(c.get("/health"), timeout=1)
                assert h.status_code == 200 and h.json() == {"ok": True, "sse_clients": 0}
                h = await c.get("/health/db")
                assert h.status_code == 503 and h.json()["db"] == "error: timed out"
                assert h.json()["pools"]["read"]["pool_available"] == 0
        finally:
            if token is not None:
                db_scope.reset(token)
        h = await c.get("/health/db")
        assert h.status_code == 200 and h.json()["db"] == "ok"


async def test_health_needs_no_database(tmp_path):
    """The ALB check: an engine whose database is gone is still alive (and says so); the database
    state is /health/db's to report."""
    app = await _client(tmp_path, health_db_timeout_s=0.3)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await app.state.db.close()
        h = await c.get("/health")
        assert h.status_code == 200 and h.json()["ok"] is True
        h = await c.get("/health/db")
        assert h.status_code == 503 and h.json()["ok"] is False


# ── SingleFlight ───────────────────────────────────────────────────────


async def test_one_run_shared_by_everyone_who_asks_while_it_runs():
    sf, gen, runs = SingleFlight(), {"g": 0}, []

    async def count():
        runs.append(1)
        await asyncio.sleep(0.05)
        return len(runs)

    got = await asyncio.gather(*(sf.run("k", lambda: gen["g"], count) for _ in range(10)))
    assert got == [1] * 10 and len(runs) == 1 and sf.in_flight == 0
    assert await sf.run("k", lambda: gen["g"], count) == 2  # done: the next ask runs again


async def test_after_a_change_nobody_is_handed_the_old_result():
    """A change while a run is in progress: those who asked before it share the old run; those who
    ask after it get a run that started after the change — one, shared, not one each."""
    sf, gen, started = SingleFlight(), {"g": 0}, []

    async def count():
        g = gen["g"]  # what the run sees: the tables as they were when it began
        started.append(g)
        await asyncio.sleep(0.05)
        return g

    before = [asyncio.ensure_future(sf.run("k", lambda: gen["g"], count)) for _ in range(3)]
    await asyncio.sleep(0.01)
    gen["g"] += 1  # a curator's edit lands mid-run
    after = [asyncio.ensure_future(sf.run("k", lambda: gen["g"], count)) for _ in range(3)]
    assert await asyncio.gather(*before) == [0, 0, 0]
    assert await asyncio.gather(*after) == [1, 1, 1]
    assert started == [0, 1]  # two runs in all, never two at the same time


async def test_a_caller_that_gives_up_does_not_cancel_the_run_for_the_others():
    sf = SingleFlight()

    async def count():
        await asyncio.sleep(0.05)
        return 42

    quitter = asyncio.ensure_future(sf.run("k", lambda: 0, count))
    stayer = asyncio.ensure_future(sf.run("k", lambda: 0, count))
    await asyncio.sleep(0.01)
    quitter.cancel()
    assert await stayer == 42


async def test_a_failed_run_fails_everyone_waiting_on_it_and_the_next_ask_runs_again():
    sf, n = SingleFlight(), {"runs": 0}

    async def boom():
        n["runs"] += 1
        await asyncio.sleep(0.01)
        raise RuntimeError("statement timeout")

    got = await asyncio.gather(*(sf.run("k", lambda: 0, boom) for _ in range(3)), return_exceptions=True)
    assert all(isinstance(e, RuntimeError) for e in got) and n["runs"] == 1
    with pytest.raises(RuntimeError):
        await sf.run("k", lambda: 0, boom)
    assert n["runs"] == 2


async def test_coalesced_reads_hand_each_page_its_own_copy(crawler_client):
    """A page adjusts what it is handed (step_context deletes 'total'): sharing must not leak that."""
    c = crawler_client
    db: Database = c.app.state.db
    await _crawled(c)
    token = db_scope.set("read")
    try:
        a, b = await asyncio.gather(db.count_deltas_by_kind("ex.org"), db.count_deltas_by_kind("ex.org"))
    finally:
        db_scope.reset(token)
    assert a == b and a is not b
    del a["total"]
    assert "total" in b


async def test_pages_answer_503_when_no_read_connection_frees_up_even_before_the_route(tmp_path):
    """Signed in: the session lookup is the first query of every page, outside the routes. With the
    read pool exhausted it answers 503 (not a 500 stack trace), and pages work again once it frees."""
    from .conftest import SECURED, login

    app = await _client(tmp_path, db_read_pool_size=1, db_read_wait_s=0.3, **SECURED)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await login(c, "admin", "s3cret")
        token = db_scope.set("read")
        try:
            async with app.state.db.read_pool.connection():
                db_scope.reset(token)
                token = None
                r = await c.get("/api/collections")
                assert r.status_code == 503 and r.headers["retry-after"] == "5"
                assert "no free database connection" in r.json()["detail"]
        finally:
            if token is not None:
                db_scope.reset(token)
        assert (await c.get("/api/collections")).status_code == 200


async def test_progress_events_do_not_end_sharing_but_a_jobs_writes_do(crawler_client):
    """A running job announces progress about once a second. Those events change nothing a page
    counts, so they no longer mark the collection changed (which ended the sharing of slow counts
    between open tabs every second). The job's own writes do, when they commit."""
    c = crawler_client
    db, bus = c.app.state.db, c.app.state.bus
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "ex.org", "max_pages": 10})
    await c.post("/api/collections/ex.org/scrape")
    await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")

    gen = db._gens.get("ex.org", 0)
    bus.publish("collection", {"collection_id": "ex.org", "status": "curating",
                               "job": {"id": 9, "kind": "llm_metadata", "state": "running", "progress": {"done": 3}}})
    assert db._gens.get("ex.org", 0) == gen

    before = (await c.get("/collections/ex.org?tab=curate")).text
    url = (await c.get("/api/collections/ex.org/delta?limit=1")).json()["items"][0]["url"]
    await db.set_delta_ai("ex.org", [{"url": url, "title": "From the model", "title_conf": "high",
                                      "division": None, "document_type": None, "model": "fake"}])
    assert db._gens.get("ex.org", 0) > gen
    after = (await c.get("/collections/ex.org?tab=curate")).text
    assert "From the model" not in before and "From the model" in after

    gen = db._gens["ex.org"]
    bus.publish("collection", {"collection_id": "ex.org", "status": "curating",
                               "job": {"id": 9, "kind": "llm_metadata", "state": "succeeded", "progress": {}}})
    assert db._gens["ex.org"] > gen  # a finished job still marks it
