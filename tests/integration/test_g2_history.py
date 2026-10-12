"""Menu › History: the global ledger of every action (audit_log). It outlives the collections it
names (no foreign key, on purpose), filters by text, pages by row id in time order and by offset
in any other order. The ledger is SQL (Database.list_audit), so it is checked on the real database."""

LIVE = {"seed_url": "https://live.org", "name": "Live", "max_pages": 5}
GONE = {"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10}
CREATED = ["s0.org", "s1.org", "s2.org"]  # created in this order


async def test_the_ledger_keeps_actions_on_a_deleted_collection_and_links_only_live_ones(authed_client):
    c = authed_client
    await c.post("/api/collections", json=GONE)
    await c.post("/api/collections/ex.org/status", json={"status": "backlog", "note": "noop", "force": True})
    assert (await c.delete("/api/collections/ex.org")).status_code == 204
    await c.post("/api/collections", json=LIVE)

    entries = (await c.get("/api/audit", params={"q": "ex.org"})).json()["entries"]
    assert [(e["action"], e["collection_id"], e["actor"]) for e in entries] == [
        ("collection.delete", "ex.org", "admin"), ("status.set", "ex.org", "admin"),
        ("collection.create", "ex.org", "admin")]
    page = (await c.get("/history")).text
    assert "(deleted)" in page and 'href="/collections/ex.org' not in page, "a link to a collection that is gone"
    assert 'href="/collections/live.org?tab=activity"' in page and ">Live</a>" in page


async def test_the_ledger_filters_and_pages_by_id_in_time_order_and_by_offset_in_any_other(authed_client):
    c = authed_client
    for cid in CREATED:
        await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 5})

    async def audit(**params) -> dict:
        return (await c.get("/api/audit", params={"q": "collection.create", **params})).json()

    r = (await c.get("/api/audit", params={"q": "s1.org"})).json()
    assert [e["collection_id"] for e in r["entries"]] == ["s1.org"] and r["next_before"] is None
    # newest first, paged by row id
    first = await audit(limit=2)
    assert [e["collection_id"] for e in first["entries"]] == ["s2.org", "s1.org"]
    assert first["next_before"] == first["entries"][-1]["id"] and first["next_offset"] is None
    rest = await audit(limit=2, before=first["next_before"])
    assert [e["collection_id"] for e in rest["entries"]] == ["s0.org"] and rest["next_before"] is None
    # another column, both ways, paged by offset
    assert [e["collection_id"] for e in (await audit(sort="collection", dir="asc"))["entries"]] == CREATED
    assert [e["collection_id"] for e in (await audit(sort="collection", dir="desc"))["entries"]] == CREATED[::-1]
    first = await audit(sort="collection", dir="asc", limit=2)
    assert (len(first["entries"]), first["next_before"], first["next_offset"]) == (2, None, 2)
    rest = await audit(sort="collection", dir="asc", limit=2, offset=2)
    assert [e["collection_id"] for e in rest["entries"]] == ["s2.org"] and rest["next_offset"] is None
    assert (await c.get("/api/audit", params={"sort": "bogus"})).status_code == 200

    page = (await c.get("/history", params={"q": "collection.create"})).text
    assert page.count("collection.create</code>") == len(CREATED)
    assert "Older →" in (await c.get("/history", params={"limit": 2})).text
    page = (await c.get("/history", params={"sort": "action", "dir": "asc", "limit": 2})).text
    assert 'aria-sort="ascending"' in page and "More →" in page and "offset=2&sort=action&dir=asc" in page
