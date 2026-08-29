import asyncio
import json

import yaml

from sde_curation.events import sse_format


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
    r = await client.post("/api/collections/a.org/status", json={"status": "scraped", "note": "manual"})
    assert r.status_code == 200 and r.json()["status"] == "scraped"
    hist = (await client.get("/api/collections/a.org/history")).json()
    assert [h["new_status"] for h in hist] == ["backlog", "scraped"] and hist[1]["note"] == "manual"
    # htmx request gets the row partial back
    r = await client.post("/api/collections/a.org/status", json={"status": "curating"}, headers={"HX-Request": "true"})
    assert r.status_code == 200 and 'id="row-a_org"' in r.text and "curating" in r.text


async def test_pages_render(client):
    await client.post("/api/collections", json={"seed_url": "https://a.org", "name": "Alpha"})
    home = await client.get("/")
    assert home.status_code == 200 and "Alpha" in home.text and 'sse-connect="/events"' in home.text
    page = await client.get("/collections/a.org")
    assert page.status_code == 200 and "Status history" in page.text
    assert (await client.get("/collections/nope")).status_code == 404
    assert (await client.get("/static/htmx.min.js")).status_code == 200


async def test_sse_receives_status_event(app, client):
    await client.post("/api/collections", json={"seed_url": "https://a.org", "name": "A"})
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
