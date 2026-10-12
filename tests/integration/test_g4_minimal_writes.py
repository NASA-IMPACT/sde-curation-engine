"""A change rewrites only the rows it changes (2026-09-18 scale audit): replace_deltas and
replace_curated make the table equal to the new state by writing the difference, so an edit on a
100K-URL collection does not rewrite 100K rows. PostgreSQL's xmin (the transaction that last wrote a
row) shows which rows were written. Replaces two tests of the old tests/integration/test_scale.py
(TEST-STRATEGY-2026-10-09.md, P4)."""

from sde_curation.models import DumpUrl

CID = "ex.org"
API = f"/api/collections/{CID}"
PAGES = 8


def url(i: int) -> str:
    return f"https://{CID}/p{i}"


async def started(c) -> None:
    r = await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10,
                                               "division": "Heliophysics"})
    assert r.status_code == 201, r.text
    await c.app.state.db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(i), scraped_title=f"Page {i}",
                                                    full_text=f"text {i}") for i in range(1, PAGES + 1)])
    assert (await c.post(f"{API}/recompute")).status_code == 200


async def versions(c, table: str) -> dict[str, str]:
    rows = await c.app.state.db.fetch(f"SELECT url, xmin::text AS v FROM {table} WHERE collection_id=%s", (CID,))
    return {r["url"]: r["v"] for r in rows}


async def effects(c) -> list[tuple[str, str]]:
    rows = await c.app.state.db.fetch(
        "SELECT url, field FROM pattern_effects WHERE collection_id=%s ORDER BY url, field", (CID,))
    return [(r["url"], r["field"]) for r in rows]


async def test_an_edit_rewrites_only_the_delta_row_it_changes(client):
    c = client
    await started(c)
    before = await versions(c, "delta_urls")

    await c.post(f"{API}/urls", json={"url": url(2), "type": "title", "value": "Two"})

    after = await versions(c, "delta_urls")
    assert len(before) == PAGES
    assert {u for u in before if before[u] != after[u]} == {url(2)}


async def test_an_exclude_rule_removes_its_row_and_deleting_it_brings_the_row_back_untouched_rows_kept(client):
    """The rule's effect rows follow too: the title edit keeps its effect throughout."""
    c = client
    await started(c)
    await c.post(f"{API}/urls", json={"url": url(2), "type": "title", "value": "Two"})
    edited = await versions(c, "delta_urls")

    r = await c.post(f"{API}/patterns", json={"type": "exclude", "match": "*/p3"})
    while_excluded, effects_while = await versions(c, "delta_urls"), await effects(c)
    await c.delete(f"{API}/patterns/{r.json()['pattern']['id']}")

    back = await versions(c, "delta_urls")
    assert url(3) not in while_excluded and effects_while == [(url(2), "title"), (url(3), "excluded")]
    assert url(3) in back and back[url(2)] == edited[url(2)]
    assert await effects(c) == [(url(2), "title")]


async def test_a_promote_rewrites_only_the_curated_row_that_changed(client):
    c = client
    await started(c)
    await c.post(f"{API}/patterns", json={"type": "document_type", "match": "*", "value": "Documentation"})
    assert (await c.post(f"{API}/promote")).status_code == 200
    before = await versions(c, "curated_urls")

    await c.post(f"{API}/urls", json={"url": url(2), "type": "title", "value": "Two"})
    assert (await c.post(f"{API}/promote")).status_code == 200

    after = await versions(c, "curated_urls")
    assert set(after) == set(before) and len(before) == PAGES
    assert {u for u in before if before[u] != after[u]} == {url(2)}
