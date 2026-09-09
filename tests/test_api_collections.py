import asyncio
import json
import re

import yaml

from sde_curation.events import sse_format
from tests.conftest import seed_dump


async def test_create_list_get(client, settings):
    r = await client.post("/api/collections", json={"seed_url": "science.nasa.gov", "name": "Sci"})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["collection_id"] == "science.nasa.gov" and body["status"] == "backlog"
    # git-trackable collection.yaml written
    y = yaml.safe_load((settings.collections_dir / "science.nasa.gov" / "collection.yaml").read_text())
    assert y["seed_url"] == "https://science.nasa.gov" and y["status"] == "backlog"

    assert (await client.post("/api/collections", json={"seed_url": "science.nasa.gov", "name": "dup"})).status_code == 409
    assert (await client.post("/api/collections", json={"seed_url": "ftp://x", "name": "bad"})).status_code == 422

    assert [c["collection_id"] for c in (await client.get("/api/collections")).json()] == ["science.nasa.gov"]
    assert (await client.get("/api/collections/nope")).status_code == 404


async def test_status_change_and_history(client):
    await client.post("/api/collections", json={"seed_url": "https://a.org", "name": "A"})
    r = await client.post("/api/collections/a.org/status", json={"status": "live"})
    assert r.status_code == 409  # illegal transition
    r = await client.post("/api/collections/a.org/status", json={"status": "scraped"})
    assert r.status_code == 409 and "scrape first" in r.text  # data rule: no dump yet
    await seed_dump(client, "a.org")
    r = await client.post("/api/collections/a.org/status", json={"status": "scraped", "note": "manual"})
    assert r.status_code == 200 and r.json()["status"] == "scraped"
    hist = (await client.get("/api/collections/a.org/history")).json()
    assert [h["new_status"] for h in hist] == ["backlog", "scraped"] and hist[1]["note"] == "manual"
    # htmx request from a dashboard row gets the row partial back; elsewhere a refresh header
    r = await client.post("/api/collections/a.org/status", json={"status": "curating"},
                          headers={"HX-Request": "true", "HX-Target": "row-a_org"})
    assert r.status_code == 200 and 'id="row-a_org"' in r.text and "curating" in r.text
    r = await client.post("/api/collections/a.org/status", json={"status": "curated"}, headers={"HX-Request": "true"})
    assert r.status_code == 409 and "nothing has been promoted" in r.text
    r = await client.post("/api/collections/a.org/status?then=/x", json={"status": "scraped"}, headers={"HX-Request": "true"})
    assert r.status_code == 200 and r.headers["HX-Redirect"] == "/x"
    r = await client.post("/api/collections/a.org/status", json={"status": "curating"}, headers={"HX-Request": "true"})
    assert r.headers["HX-Refresh"] == "true"


async def test_pages_render(client):
    await client.post("/api/collections", json={"seed_url": "https://a.org", "name": "Alpha"})
    home = await client.get("/")
    assert home.status_code == 200 and "Alpha" in home.text and 'sse-connect="/events"' in home.text
    page = await client.get("/collections/a.org")
    assert page.status_code == 200 and "pipeline" in page.text and 'role="tab"' in page.text
    for tab in ("overview", "urls", "patterns", "activity"):
        r = await client.get(f"/collections/a.org?tab={tab}")
        assert r.status_code == 200 and f'data-tab="{tab}"' in r.text, tab
    assert "Status history" in (await client.get("/collections/a.org?tab=activity")).text
    assert (await client.get("/collections/a.org?tab=bogus")).status_code == 200  # falls back to overview
    assert (await client.get("/collections/nope")).status_code == 404
    assert (await client.get("/static/htmx.min.js")).status_code == 200


async def test_sse_receives_status_event(app, client):
    await client.post("/api/collections", json={"seed_url": "https://a.org", "name": "A"})
    await seed_dump(client, "a.org")
    bus = app.state.bus
    got = []

    async def listen():
        async for msg in bus.subscribe():
            got.append(sse_format(msg))
            if len(got) == 1:
                break

    task = asyncio.create_task(listen())
    await asyncio.sleep(0)  # let the subscriber register
    await client.post("/api/collections/a.org/status", json={"status": "scraped"})
    await asyncio.wait_for(task, 2)
    assert got[0]["event"] == "collection"
    assert json.loads(got[0]["data"]) == {
        "collection_id": "a.org", "status": "scraped",
        "updated_at": json.loads(got[0]["data"])["updated_at"],
    }


async def test_delete(client):
    await client.post("/api/collections", json={"seed_url": "https://a.org", "name": "A"})
    assert (await client.delete("/api/collections/a.org")).status_code == 204
    assert (await client.get("/api/collections/a.org")).status_code == 404


async def test_dashboard_filters(client):
    await client.post("/api/collections", json={"seed_url": "https://a.org", "name": "Alpha", "division": "Earth Science"})
    await client.post("/api/collections", json={"seed_url": "https://b.org", "name": "Beta", "division": "Heliophysics"})
    home = await client.get("/")
    assert 'id="filters"' in home.text and "Alpha" in home.text and "Beta" in home.text
    assert 'id="f-division-Earth_Science" class="cnt">1<' in home.text
    r = await client.get("/", params={"division": "Earth Science"})
    assert "Alpha" in r.text and 'href="/collections/b.org"' not in r.text and ">1 of 2<" in r.text
    r = await client.get("/", params=[("division", "Earth Science"), ("division", "Heliophysics"), ("status", "backlog")])
    assert "Alpha" in r.text and "Beta" in r.text
    r = await client.get("/", params={"status": "live"})
    assert "No collections match" in r.text
    r = await client.get("/rows", params={"q": "beta"})
    assert 'id="collections"' in r.text and "Beta" in r.text and 'href="/collections/a.org"' not in r.text
    assert 'id="f-shown" hx-swap-oob="true">1 of 2<' in r.text
    # No login → actor "anonymous"; rows that predate provenance (created_by NULL) show as "Unassigned".
    assert 'value="anonymous"' in home.text and "Unassigned" not in home.text
    await client.app.state.db.conn.execute("UPDATE collections SET created_by=NULL WHERE collection_id='b.org'")
    r = await client.get("/", params={"curator": "anonymous"})
    assert "Alpha" in r.text and 'href="/collections/b.org"' not in r.text and "Unassigned" in r.text
    r = await client.get("/", params={"curator": "__none__"})
    assert "Beta" in r.text and 'href="/collections/a.org"' not in r.text
    r = await client.get("/", params={"curator": "someone"})
    assert "No collections match" in r.text


async def test_static_assets_are_hash_versioned(client):
    home = await client.get("/")
    m = re.search(r'href="(/static/app\.css\?v=([0-9a-f]{10}))"', home.text)
    assert m, home.text[:800]
    r = await client.get(m.group(1))
    assert r.status_code == 200 and r.headers["cache-control"] == "public, max-age=31536000, immutable"
    r = await client.get("/static/app.css")
    assert r.status_code == 200 and r.headers["cache-control"] == "no-cache"
    r = await client.get("/static/app.css?v=stale00000")
    assert r.status_code == 200 and r.headers["cache-control"] == "no-cache"
