"""Menu → History: the global ledger of every action, which outlives the collections it names."""

COLL = {"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10}


async def test_history_keeps_actions_on_deleted_collections(authed_client):
    c = authed_client
    assert 'href="/history"' in (await c.get("/")).text  # in the hamburger menu
    await c.post("/api/collections", json=COLL)
    await c.post("/api/collections/ex.org/status", json={"status": "backlog", "note": "noop", "force": True})
    assert (await c.delete("/api/collections/ex.org")).status_code == 204
    assert (await c.get("/api/collections/ex.org")).status_code == 404

    rows = (await c.get("/api/audit")).json()["entries"]
    assert [r["action"] for r in rows] == ["collection.delete", "status.set", "collection.create"]
    assert all(r["collection_id"] == "ex.org" for r in rows) and all(r["actor"] == "admin" for r in rows)

    page = (await c.get("/history")).text
    assert "(deleted)" in page and "ex.org" in page
    assert 'href="/collections/ex.org' not in page  # no link to a collection that is gone

    # a live collection links to its own activity tab
    await c.post("/api/collections", json={"seed_url": "https://live.org", "name": "Live", "max_pages": 5})
    page = (await c.get("/history")).text
    assert 'href="/collections/live.org?tab=activity"' in page and ">Live</a>" in page


async def test_history_filter_and_paging(authed_client):
    c = authed_client
    for i in range(3):
        await c.post("/api/collections", json={"seed_url": f"https://s{i}.org", "name": f"S{i}", "max_pages": 5})
    r = (await c.get("/api/audit", params={"q": "s1.org"})).json()
    assert [e["collection_id"] for e in r["entries"]] == ["s1.org"] and r["next_before"] is None
    page = (await c.get("/history", params={"q": "collection.create"})).text
    assert page.count("collection.create</code>") == 3 and "status.set</code>" not in page

    first = (await c.get("/api/audit", params={"limit": 2})).json()
    assert len(first["entries"]) == 2 and first["next_before"] == first["entries"][-1]["id"]
    rest = (await c.get("/api/audit", params={"limit": 2, "before": first["next_before"]})).json()
    assert [e["collection_id"] for e in rest["entries"]] == ["s0.org"] and rest["next_before"] is None
    assert "Older →" in (await c.get("/history", params={"limit": 2})).text

