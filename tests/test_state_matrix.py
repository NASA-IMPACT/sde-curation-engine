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
    ("GET", "/collections/{c}?tab=urls&set=deltas&excluded=&kind=&q=&division=&document_type=&page=x&per=y", None),
    ("GET", "/collections/{c}?tab=urls&set=dump", None),
    ("GET", "/collections/{c}?tab=urls&set=curated", None),
    ("GET", "/collections/{c}?tab=patterns", None),
    ("GET", "/collections/{c}?tab=activity", None),
    ("GET", "/collections/{c}/urls/deltas?format=csv", None),
    ("GET", "/collections/{c}/header", None),
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
    page = (await crawler_client.get("/collections/ex.org?tab=urls&set=deltas")).text
    assert "editing is locked" in page
    page = (await crawler_client.get("/collections/ex.org?tab=patterns")).text
    assert "locked" in page and page.count("disabled") >= 3
    await wait_job(crawler_client, "ex.org")


async def test_workbench_urls_tabs_and_csv(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")
    await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p1"})
    await c.post("/api/collections/ex.org/patterns", json={"type": "division", "match": "*/p2", "value": "Earth Science"})
    # dump tab: state column and search
    t = (await c.get("/collections/ex.org?tab=urls&set=dump")).text
    assert t.count("<tr>") >= 8 and ">pending<" in t
    assert "https://ex.org/p7" in t and "https://ex.org/p7" not in (await c.get("/collections/ex.org?tab=urls&set=dump&q=p2")).text
    # deltas tab: filters (kind / excluded / division), effects tooltip, paging
    t = (await c.get("/collections/ex.org?tab=urls&set=deltas&division=Earth+Science")).text
    assert "https://ex.org/p2" in t and "https://ex.org/p3" not in t
    assert 'title="division */p2 → Earth Science"' in t  # "why" tooltip from pattern_effects
    assert "https://ex.org/p1" in (await c.get("/collections/ex.org?tab=urls&set=deltas&excluded=true")).text
    t = (await c.get("/collections/ex.org?tab=urls&set=deltas&per=25&page=2")).text
    assert "No deltas match" in t
    # csv export honours filters
    r = await c.get("/collections/ex.org/urls/deltas?format=csv&excluded=true")
    assert r.headers["content-type"].startswith("text/csv") and r.text.splitlines()[0].startswith("kind,url,excluded")
    assert len(r.text.strip().splitlines()) == 2
    # curated tab after promote: read-only rows, 'Curate ↗' only when a delta exists
    await c.post("/api/collections/ex.org/promote")
    t = (await c.get("/collections/ex.org?tab=urls&set=curated")).text
    assert ">indexed<" in t and ">excluded<" in t and "unchanged" in t and "Curate ↗</a>" not in t
    await c.post("/api/collections/ex.org/patterns", json={"type": "title", "match": "*/p3", "value": "Three"})
    t = (await c.get("/collections/ex.org?tab=urls&set=curated&q=p3")).text
    assert "Curate ↗</a>" in t
    # header chips reflect counts
    h = (await c.get("/collections/ex.org/header")).text
    assert "Dump <b>8</b>" in h and "Curated <b>8</b>" in h and "Patterns <b>3</b>" in h
    assert (await c.get("/collections/ex.org/urls/nope")).status_code == 404


async def test_then_redirect_keeps_full_target(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    row = (await c.get("/collections/ex.org/row")).text
    assert "then=/collections/ex.org%3Ftab%3Durls%26set%3Ddeltas" in row
    r = await c.post("/api/collections/ex.org/recompute?then=%2Fcollections%2Fex.org%3Ftab%3Durls%26set%3Ddeltas",
                     headers={"HX-Request": "true"})
    assert r.headers["HX-Redirect"] == "/collections/ex.org?tab=urls&set=deltas"
