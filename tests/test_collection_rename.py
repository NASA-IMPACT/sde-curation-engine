"""Renaming a collection after it was created: allowed only until the first index run, so the index
key and name always match the name the collection was indexed under. The id and URL stay, and title
rules that render {collection} pick up the new name."""

from tests.conftest import prepare, wait_job


async def test_rename_keeps_the_id_and_moves_an_unpinned_index_key(crawler_client):
    c = crawler_client
    r = await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 6})
    assert r.status_code == 201, r.text
    assert (await c.post("/api/collections/ex.org/name", json={"name": "   "})).status_code == 422
    assert (await c.post("/api/collections/ex.org/name", json={"name": "x" * 201})).status_code == 422

    r = await c.post("/api/collections/ex.org/name", json={"name": "  NASA Applied Sciences "})
    assert r.status_code == 200, r.text
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["collection_id"] == "ex.org" and col["name"] == "NASA Applied Sciences"
    assert col["index_key"] is None  # nothing indexed yet: the key still follows the name
    page = (await c.get("/collections/ex.org?tab=overview")).text
    assert "<code>nasa_applied_sciences</code>" in page and 'value="NASA Applied Sciences"' in page
    assert "The index key will follow the new name." in page
    audit = (await c.get("/api/collections/ex.org/audit")).json()
    assert any(a["action"] == "collection.name" and "Ex → NASA Applied Sciences" in (a["detail"] or "") for a in audit)
    assert "NASA Applied Sciences" in (await c.get("/")).text


async def test_title_rules_using_collection_follow_the_rename(crawler_client):
    c = crawler_client
    await prepare(c)  # name "ex.org", promoted
    r = await c.post("/api/collections/ex.org/patterns", json={"type": "title", "match": "*", "value": "{title} | {collection}"})
    assert r.status_code in (200, 201), r.text
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200
    curated = (await c.get("/api/collections/ex.org/curated?limit=100")).json()["items"]
    included = [x for x in curated if not x.get("excluded")]
    assert included and all(x["title"].endswith(" | ex.org") for x in included)

    assert (await c.post("/api/collections/ex.org/name", json={"name": "Example Site"})).status_code == 200
    rows = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    assert len(rows) == len(included) and all(d["kind"] == "modified" for d in rows)
    assert all(d["title"].endswith(" | Example Site") for d in rows)
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200
    curated = (await c.get("/api/collections/ex.org/curated?limit=100")).json()["items"]
    assert all(x["title"].endswith(" | Example Site") for x in curated if not x.get("excluded"))


async def test_rename_without_a_collection_title_rule_leaves_curated_rows_alone(crawler_client):
    c = crawler_client
    await prepare(c)
    assert (await c.post("/api/collections/ex.org/name", json={"name": "Example Site"})).status_code == 200
    assert (await c.get("/api/collections/ex.org/delta?limit=100")).json()["total"] == 0


async def test_rename_is_refused_once_the_collection_has_been_indexed(index_client):
    c = index_client
    await prepare(c)  # name "ex.org" → key "ex_org"
    assert 'id="cname"' in (await c.get("/collections/ex.org?tab=overview")).text
    r = await c.post("/api/collections/ex.org/index?target=test")
    assert r.status_code == 202, r.text
    assert (await wait_job(c, "ex.org", timeout=30))["state"] == "succeeded"

    r = await c.post("/api/collections/ex.org/name", json={"name": "Example Site"})
    assert r.status_code == 409 and "indexed" in r.text
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["name"] == "ex.org" and col["index_key"] == "ex_org" and col["index_name"] == "ex.org"
    page = (await c.get("/collections/ex.org?tab=overview")).text
    assert 'id="cname"' not in page and "locked — indexed" in page
    assert not any(a["action"] == "collection.name" for a in (await c.get("/api/collections/ex.org/audit")).json())


async def test_a_failed_index_run_locks_the_name_too(index_client, monkeypatch):
    c = index_client
    await prepare(c)
    jobs = c.app.state.jobs

    async def boom(*a, **kw):
        raise RuntimeError("indexer down")
    monkeypatch.setattr(jobs, "_validate", boom)
    r = await c.post("/api/collections/ex.org/index?target=test")
    assert r.status_code == 202, r.text
    assert (await wait_job(c, "ex.org", timeout=30))["state"] == "failed"
    assert (await c.post("/api/collections/ex.org/name", json={"name": "Example Site"})).status_code == 409


async def test_rename_keeps_an_index_key_set_by_hand(crawler_client):
    c = crawler_client
    r = await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 6})
    assert r.status_code == 201, r.text
    r = await c.post("/api/collections/ex.org/index-key", json={"index_key": "legacy_ex", "index_name": "Legacy Ex"})
    assert r.status_code == 200, r.text
    page = (await c.get("/collections/ex.org?tab=overview")).text
    assert "set by hand (legacy_ex) and stays as it is" in page and "will follow the new name" not in page

    assert (await c.post("/api/collections/ex.org/name", json={"name": "NASA Applied Sciences"})).status_code == 200
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["name"] == "NASA Applied Sciences"
    assert col["index_key"] == "legacy_ex" and col["index_name"] == "Legacy Ex"
    page = (await c.get("/collections/ex.org?tab=overview")).text
    assert "<code>legacy_ex</code>" in page and "nasa_applied_sciences" not in page
