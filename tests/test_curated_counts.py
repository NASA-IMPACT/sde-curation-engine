"""The curated count is the indexed subset, and changing it tells the curator to re-index.

The Curated URLs list holds every approved row, included and excluded; `curated_count` counts only
the rows that reach the index and `curated_rows` the whole set. An exclude rule applies in place
(rules decide exclusions — they are never delta URLs), so the count drops at once; the way back in
is a delta URL and the count moves when it is promoted. Either way, once the collection has been
indexed, the "needs re-indexing" chip goes up until a test index run covers the change.
"""

from tests.conftest import classify, prepare, wait_job

API = "/api/collections/ex.org"


async def setup(client, n=10):
    await client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": n})
    await client.post(f"{API}/scrape")
    await wait_job(client, "ex.org")
    await client.post(f"{API}/recompute")
    await classify(client)
    assert (await client.post(f"{API}/promote")).status_code == 200


async def counts(client) -> tuple[int, int, int, str]:
    c = (await client.get(API)).json()
    return c["curated_count"], c["curated_rows"], c["delta_count"], c["status"]


async def test_exclude_drops_the_curated_count_and_keeps_the_row_listed(crawler_client):
    c = crawler_client
    await setup(c)
    assert await counts(c) == (8, 8, 0, "curated")

    # the ✗ toggle on a curated row: a rule, applied in place — no delta to promote
    r = await c.post(f"{API}/urls", json={"url": "https://ex.org/p4", "type": "exclude"})
    assert r.status_code == 200
    assert await counts(c) == (7, 8, 0, "curated")

    # the row is still in the curated set, listed with both filters, flagged excluded
    all_rows = (await c.get(f"{API}/curated")).json()
    assert all_rows["total"] == 8
    assert [r["url"] for r in all_rows["items"] if r["excluded"]] == ["https://ex.org/p4"]
    assert (await c.get(f"{API}/curated?excluded=false")).json()["total"] == 7
    page = (await c.get("/collections/ex.org?tab=curated")).text
    assert "https://ex.org/p4" in page and ">Curated URLs <span class=\"count ok \">7</span>" in page

    # ✓ include is the way back: a delta URL to review, and the count only moves on promote
    assert (await c.post(f"{API}/urls", json={"url": "https://ex.org/p4", "type": "include"})).status_code == 200
    assert await counts(c) == (7, 8, 1, "curating")
    assert (await c.post(f"{API}/promote")).json() == {"curated": 8, "status": "curated"}
    assert await counts(c) == (8, 8, 0, "curated")


async def test_excluding_every_row_leaves_the_set_promoted(crawler_client):
    """curated_count 0 means "nothing reaches the index", not "nothing was promoted": the status
    invariants and the empty states read curated_rows."""
    c = crawler_client
    await setup(c)
    assert (await c.post(f"{API}/patterns", json={"type": "exclude", "match": "*"})).status_code == 201
    assert await counts(c) == (0, 8, 0, "curated")
    assert (await c.post(f"{API}/status", json={"status": "config_generated"})).status_code == 200
    assert "Nothing promoted yet" not in (await c.get("/collections/ex.org?tab=curated")).text


async def test_needs_reindexing_once_the_curated_set_moves_past_the_index(index_client):
    c = index_client
    await prepare(c)  # 7 curated URLs (*/p1 excluded before the first promote)
    assert await counts(c) == (7, 7, 0, "curated")

    async def chip() -> bool:
        return ">⚠ needs re-indexing<" in (await c.get("/collections/ex.org/header")).text

    assert not await chip()  # never indexed: there is nothing to re-index
    assert (await c.post(f"{API}/index?target=test")).status_code == 202
    assert (await wait_job(c, "ex.org", timeout=30))["state"] == "succeeded"
    assert not await chip()

    # an exclude rule takes a curated row out in place: the index still holds it
    assert (await c.post(f"{API}/urls", json={"url": "https://ex.org/p4", "type": "exclude"})).status_code == 200
    assert await counts(c) == (6, 7, 0, "curated")
    assert await chip()
    header = (await c.get("/collections/ex.org/header")).text
    assert "The curated URLs changed after the last test index run" in header
    assert ">⚠ needs re-indexing<" in (await c.get("/?flag=needs_reindexing")).text

    # re-indexing covers the change and the chip comes down
    assert (await c.post(f"{API}/index?target=test")).status_code == 202
    assert (await wait_job(c, "ex.org", timeout=30))["state"] == "succeeded"
    assert not await chip()

    # and a promote raises it again
    assert (await c.post(f"{API}/urls", json={"url": "https://ex.org/p4", "type": "include"})).status_code == 200
    assert not await chip()  # still only a delta URL: the curated set has not moved yet
    assert (await c.post(f"{API}/promote")).status_code == 200
    assert await counts(c) == (7, 7, 0, "curated")
    assert await chip()


async def test_a_promote_that_moves_nothing_leaves_the_index_up_to_date(index_client):
    """The "mark curated" shortcut (promote with an empty queue) must not fake a change."""
    c = index_client
    await prepare(c)
    assert (await c.post(f"{API}/index?target=test")).status_code == 202
    assert (await wait_job(c, "ex.org", timeout=30))["state"] == "succeeded"
    await c.post(f"{API}/status", json={"status": "curating"})
    assert (await c.post(f"{API}/promote")).status_code == 200
    assert ">⚠ needs re-indexing<" not in (await c.get("/collections/ex.org/header")).text
