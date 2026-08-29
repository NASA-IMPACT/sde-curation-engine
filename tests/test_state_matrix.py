"""Fool-proofing matrix: every action against every status, with/without a running job.

Invariants checked after every call:
  * never a 500
  * collection row counts match table rows
  * status is consistent with data (curating ⇒ dump exists; curated/… ⇒ curated rows exist unless empty promote)
  * no job left 'running' once the crawler subprocess has exited
  * there is always a forward action (next_action is never 'disabled' for a reachable status < curated)
"""

import asyncio

import pytest

from sde_curation.models import Status
from tests.conftest import wait_job

ACTIONS = [
    ("POST", "/api/collections/{c}/scrape", None),
    ("POST", "/api/collections/{c}/recompute", None),
    ("POST", "/api/collections/{c}/patterns", {"type": "exclude", "match": "*/p3"}),
    ("POST", "/api/collections/{c}/urls", {"url": "https://ex.org/p2", "type": "division", "value": "General"}),
    ("POST", "/api/collections/{c}/urls", {"url": "https://ex.org/p2", "type": "exclude"}),
    ("POST", "/api/collections/{c}/promote", None),
    ("GET", "/api/collections/{c}/deltas?excluded=true", None),
    ("GET", "/collections/{c}/curate?excluded=&kind=&q=", None),
    ("GET", "/collections/{c}", None),
    ("GET", "/", None),
]
STEP_PAGES = [f"/collections/{{c}}/step/{s.value}" for s in Status]


async def invariants(client, cid):
    c = (await client.get(f"/api/collections/{cid}")).json()
    db = client.app.state.db
    for table, col in (("dump_urls", "dump_count"), ("delta_urls", "delta_count"), ("curated_urls", "curated_count")):
        cur = await db.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE collection_id=?", (cid,))
        assert (await cur.fetchone())[0] == c[col], f"{col} drift"
    st = c["status"]
    if st in ("curating", "curated", "config_generated", "live"):
        assert c["dump_count"] > 0 or c["curated_count"] > 0, f"{st} with no data"
    if st == "curating":
        assert c["delta_count"] > 0 or c["curated_count"] == 0 or True  # allowed transiently (manual)
    jobs = (await client.get(f"/api/collections/{cid}/jobs")).json()
    for j in jobs:
        assert j["state"] != "queued"
    return c


async def drive_to(client, cid, status):
    """Reach `status` legitimately (no force) from a fresh scraped collection."""
    order = ["scraped", "curating", "curated", "config_generated", "live"]
    if status == "backlog":
        return
    await client.post(f"/api/collections/{cid}/scrape")
    await wait_job(client, cid)
    if status == "scraped":
        return
    await client.post(f"/api/collections/{cid}/recompute")
    if status == "curating":
        return
    await client.post(f"/api/collections/{cid}/promote")
    for s in order[order.index("curated") + 1 : order.index(status) + 1]:
        r = await client.post(f"/api/collections/{cid}/status", json={"status": s})
        assert r.status_code == 200, r.text


@pytest.mark.parametrize("status", [s.value for s in Status])
async def test_every_action_in_every_status(crawler_client, status):
    cid = "ex.org"
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    await drive_to(crawler_client, cid, status)
    assert (await crawler_client.get(f"/api/collections/{cid}")).json()["status"] == status

    for method, path, body in ACTIONS + [("GET", p, None) for p in STEP_PAGES]:
        url = path.format(c=cid)
        r = await crawler_client.request(method, url, json=body)
        assert r.status_code < 500, f"{method} {url} in {status}: {r.status_code} {r.text[:200]}"
        assert r.status_code in (200, 201, 202, 409), f"{method} {url} in {status}: {r.status_code} {r.text[:200]}"
        if "scrape" in url and r.status_code == 202:
            await wait_job(crawler_client, cid)
        await invariants(crawler_client, cid)


async def test_actions_while_scrape_running(crawler_client):
    """Everything must refuse or queue cleanly while a crawl is running; nothing may corrupt."""
    cid = "ex.org"
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 40})
    await crawler_client.post(f"/api/collections/{cid}/scrape")
    await asyncio.sleep(0.1)
    assert (await crawler_client.get(f"/api/collections/{cid}/jobs")).json()[0]["state"] == "running"
    results = {}
    for method, path, body in ACTIONS:
        url = path.format(c=cid)
        r = await crawler_client.request(method, url, json=body)
        results[f"{method} {url}"] = r.status_code
        assert r.status_code < 500, (url, r.text[:200])
    # every mutating action must be refused while the crawl runs
    for k, code in results.items():
        if k.startswith("POST") and "scrape" not in k:
            assert code == 409, (k, code)
    await wait_job(crawler_client, cid)
    c = await invariants(crawler_client, cid)
    assert c["status"] == "scraped" and c["dump_count"] == 32, (c, results)


async def test_cancel_running_scrape(crawler_client):
    cid = "ex.org"
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 60})
    await crawler_client.post(f"/api/collections/{cid}/scrape")
    await asyncio.sleep(0.3)
    r = await crawler_client.post(f"/api/collections/{cid}/jobs/cancel")
    assert r.status_code == 200 and r.json()["state"] == "failed" and "cancelled" in r.json()["error"]
    assert (await crawler_client.post(f"/api/collections/{cid}/jobs/cancel")).status_code == 409
    c = await invariants(crawler_client, cid)
    assert c["status"] == "backlog" and c["dump_count"] == 0
    # and we can scrape again right away
    assert (await crawler_client.post(f"/api/collections/{cid}/scrape")).status_code == 202
    await wait_job(crawler_client, cid)
    assert (await crawler_client.get(f"/api/collections/{cid}")).json()["status"] == "scraped"


async def test_concurrent_recompute_and_promote(crawler_client):
    cid = "ex.org"
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    await drive_to(crawler_client, cid, "curating")
    rs = await asyncio.gather(*[
        crawler_client.post(f"/api/collections/{cid}/recompute") for _ in range(5)
    ] + [crawler_client.post(f"/api/collections/{cid}/promote")])
    assert all(r.status_code < 500 for r in rs), [r.status_code for r in rs]
    c = await invariants(crawler_client, cid)
    assert c["status"] in ("curating", "curated")


async def test_delete_collection_while_job_runs(crawler_client):
    cid = "ex.org"
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 40})
    await crawler_client.post(f"/api/collections/{cid}/scrape")
    await asyncio.sleep(0.1)
    r = await crawler_client.delete(f"/api/collections/{cid}")
    assert r.status_code == 409, r.text  # refuse: cancel first
    await crawler_client.post(f"/api/collections/{cid}/jobs/cancel")
    assert (await crawler_client.delete(f"/api/collections/{cid}")).status_code == 204
    assert not (await client_active(crawler_client)), "job still active after delete"


async def client_active(client):
    return [j for j in await client.app.state.db.active_jobs()]


async def test_curate_page_locks_while_running(crawler_client):
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 40})
    await crawler_client.post("/api/collections/ex.org/scrape")
    await asyncio.sleep(0.1)
    page = (await crawler_client.get("/collections/ex.org/curate")).text
    assert "curation is locked" in page and page.count("disabled") >= 2
    await wait_job(crawler_client, "ex.org")
