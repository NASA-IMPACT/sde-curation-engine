"""Column sorting on the URL tables and the History ledger; the metadata review table under
Curate › Metadata with accept-all / per-row decisions."""

import re

from tests.conftest import wait_job


async def setup(c, n=10):
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": n})
    await c.post("/api/collections/ex.org/scrape")
    await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")


def urls_in_order(page: str) -> list[str]:
    return re.findall(r'<td class="url"><a href="(https://ex\.org/p\d+)"', page)


async def test_url_tables_sort_by_column(crawler_client):
    c = crawler_client
    await setup(c)
    # default: kind, url (p5 and p10 failed to crawl, so p1–p4 and p6–p9)
    page = (await c.get("/collections/ex.org?tab=delta")).text
    assert urls_in_order(page)[:3] == ["https://ex.org/p1", "https://ex.org/p2", "https://ex.org/p3"]
    assert 'class="sorted"' not in page and "⇅" in page  # every header offers a sort, none is active
    # url descending
    page = (await c.get("/collections/ex.org?tab=delta&sort=url&dir=desc")).text
    assert urls_in_order(page)[:2] == ["https://ex.org/p9", "https://ex.org/p8"]
    assert 'aria-sort="descending"' in page and "▼" in page
    assert 'name="sort" value="url"' in page  # the filter form keeps the sort
    # the header link flips the direction of the sorted column and resets to page 1
    assert "sort=url&dir=asc&per=50&page=1" in page
    # title ("Page N") descending
    page = (await c.get("/collections/ex.org?tab=delta&sort=title&dir=desc")).text
    assert urls_in_order(page)[0] == "https://ex.org/p9"
    # excluded rows (a rule) sort last ascending, first descending
    await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "https://ex.org/p3", "value": None})
    page = (await c.get("/collections/ex.org?tab=delta&sort=excluded&dir=desc")).text
    assert urls_in_order(page)[0] == "https://ex.org/p3"
    # an unknown key falls back to the default order, never to the SQL
    page = (await c.get("/collections/ex.org?tab=delta&sort=drop%20table&dir=desc")).text
    assert urls_in_order(page)[0] == "https://ex.org/p1" and 'class="sorted"' not in page
    # dump and curated tables sort too
    page = (await c.get("/collections/ex.org?tab=dump&sort=url&dir=desc")).text
    assert urls_in_order(page)[0] == "https://ex.org/p9"
    await c.post("/api/collections/ex.org/promote")
    page = (await c.get("/collections/ex.org?tab=curated&sort=url&dir=desc")).text
    assert urls_in_order(page)[0] == "https://ex.org/p9"
    # the CSV follows the same order
    csv = (await c.get("/collections/ex.org/urls/curated?format=csv&sort=url&dir=desc")).text
    assert csv.splitlines()[1].startswith("https://ex.org/p9,")


async def test_history_sorts_and_pages_by_offset(authed_client):
    c = authed_client
    for i in range(3):
        await c.post("/api/collections", json={"seed_url": f"https://s{i}.org", "name": f"S{i}", "max_pages": 5})
    page = (await c.get("/history")).text  # newest first = the At column, descending
    assert 'title="sort by at, ascending">At<span class="arrow">▼</span>' in page and page.count("⇅") == 3
    r = (await c.get("/api/audit", params={"sort": "collection", "dir": "asc", "q": "collection.create"})).json()
    assert [e["collection_id"] for e in r["entries"]] == ["s0.org", "s1.org", "s2.org"]
    r = (await c.get("/api/audit", params={"sort": "collection", "dir": "desc", "q": "collection.create"})).json()
    assert [e["collection_id"] for e in r["entries"]] == ["s2.org", "s1.org", "s0.org"]
    # sorted by a column other than time the ledger pages by offset, not by row id
    first = (await c.get("/api/audit", params={"sort": "action", "dir": "asc", "limit": 2, "q": "collection.create"})).json()
    assert len(first["entries"]) == 2 and first["next_before"] is None and first["next_offset"] == 2
    rest = (await c.get("/api/audit", params={"sort": "action", "dir": "asc", "limit": 2, "offset": 2,
                                              "q": "collection.create"})).json()
    assert len(rest["entries"]) == 1 and rest["next_offset"] is None
    page = (await c.get("/history", params={"sort": "action", "dir": "asc", "limit": 2})).text
    assert 'aria-sort="ascending"' in page and "More →" in page and "offset=2&sort=action&dir=asc" in page
    # default order still pages by id
    page = (await c.get("/history", params={"limit": 2})).text
    assert "Older →" in page and "before=" in page
    # an unknown sort key is ignored
    assert (await c.get("/api/audit", params={"sort": "bogus"})).status_code == 200


async def test_metadata_review_table_accept_all_and_row(crawler_client):
    c = crawler_client
    await setup(c)
    assert (await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept"})).status_code == 409
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert 'class="urls ai-review"' in page and "sugg-scroll" in page
    assert page.count("✓ row") == 8  # one review row per URL with a suggestion
    assert "Accept all (" in page and "Reject all" in page
    assert 'hx-vals=\'{"decision": "accept"}\'' in page
    items = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    total = sum(1 for d in items for f in ("title_ai", "division_ai", "document_type_ai") if d[f])
    n_p2 = sum(1 for f in ("title_ai", "division_ai", "document_type_ai") if next(d for d in items if d["url"] == "https://ex.org/p2")[f])
    # one row: every field on that URL, nothing else
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept", "url": "https://ex.org/p2"})
    assert r.status_code == 200 and r.json()["decided"] == n_p2 and r.json()["url"] == "https://ex.org/p2"
    items = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    p2 = next(d for d in items if d["url"] == "https://ex.org/p2")
    assert not any(p2[f] for f in ("title_ai", "division_ai", "document_type_ai")) and p2["title"] == "Page 2"
    assert sum(1 for d in items for f in ("title_ai", "division_ai", "document_type_ai") if d[f]) == total - n_p2
    pats = (await c.get("/api/collections/ex.org/patterns")).json()
    assert len(pats) == n_p2 and all(p["match"] == "https://ex.org/p2" and p["source"] == "llm" for p in pats)
    # a row with nothing left to decide is a conflict, not a silent no-op
    assert (await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "reject", "url": "https://ex.org/p2"})).status_code == 409
    # everything else: one call, every field
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept"})
    assert r.status_code == 200 and r.json()["decided"] == total - n_p2 and r.json()["field"] is None
    items = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    assert not any(d[f] for d in items for f in ("title_ai", "division_ai", "document_type_ai"))
    assert len((await c.get("/api/collections/ex.org/patterns")).json()) == total
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "ai-review" not in page and "Accept all (" not in page
    audit = (await c.get("/api/collections/ex.org/audit")).json()
    assert any(a["action"] == "ai.bulk_accept" and "(https://ex.org/p2)" in a["detail"] for a in audit)
