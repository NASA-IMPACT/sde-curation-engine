"""Column sorting on the dashboard, the URL tables and the History ledger; the metadata review table under
Curate › Metadata with accept-all / per-row decisions."""

import re

from tests.conftest import classify, wait_job


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
    page = (await c.get("/collections/ex.org?tab=dump&sort=excluded&dir=desc")).text
    assert urls_in_order(page)[0] == "https://ex.org/p3"
    # an unknown key falls back to the default order, never to the SQL
    page = (await c.get("/collections/ex.org?tab=delta&sort=drop%20table&dir=desc")).text
    assert urls_in_order(page)[0] == "https://ex.org/p1" and 'class="sorted"' not in page
    # dump and curated tables sort too
    page = (await c.get("/collections/ex.org?tab=dump&sort=url&dir=desc")).text
    assert urls_in_order(page)[0] == "https://ex.org/p9"
    await classify(c)
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200
    page = (await c.get("/collections/ex.org?tab=curated&sort=url&dir=desc")).text
    assert urls_in_order(page)[0] == "https://ex.org/p9"
    # the CSV follows the same order
    csv = (await c.get("/collections/ex.org/urls/curated?format=csv&sort=url&dir=desc")).text
    assert csv.splitlines()[1].startswith("https://ex.org/p9,")


async def test_urls_are_ordered_base_first(client):
    """A URL table is read like a site: the seed, then the pages under it, one level at a time.
    Plain lexicographic order on the whole URL does not do that — it puts …/data/aerosol/access
    above …/data/ozone because "a" < "o" — so the order is host, path depth, then alphabetical."""
    from sde_curation.models import DumpUrl

    cid = "ex.org"
    await client.post("/api/collections",
                      json={"seed_url": f"https://{cid}", "name": "Ex", "max_pages": 50, "division": "Earth Science"})
    paths = ["/data/aerosol/access", "/", "/data/ozone", "/images/gallery/aurora", "/data",
             "/missions", "/data/ozone/2024/monthly", "/images/gallery"]
    await client.app.state.db.replace_dump(
        cid, [DumpUrl(collection_id=cid, url=f"https://{cid}{p}", scraped_title=p.strip("/") or "Home")
              for p in paths])
    assert (await client.post(f"/api/collections/{cid}/recompute")).status_code == 200

    def paths_in_order(page: str) -> list[str]:
        return [p or "/" for p in re.findall(r'<td class="url"><a href="https://ex\.org([^"]*)"', page)]

    base_first = ["/", "/data", "/missions", "/data/ozone", "/images/gallery",
                  "/data/aerosol/access", "/images/gallery/aurora", "/data/ozone/2024/monthly"]
    for tab in ("delta", "dump"):
        assert paths_in_order((await client.get(f"/collections/{cid}?tab={tab}")).text) == base_first, tab
        assert paths_in_order((await client.get(f"/collections/{cid}?tab={tab}&sort=url")).text) == base_first, tab
    # descending mirrors it: the deepest page first
    page = (await client.get(f"/collections/{cid}?tab=delta&sort=url&dir=desc")).text
    assert paths_in_order(page) == list(reversed(base_first))
    # the curated set and the CSV follow the same order
    await classify(c=client, cid=cid)
    assert (await client.post(f"/api/collections/{cid}/promote")).status_code == 200
    assert paths_in_order((await client.get(f"/collections/{cid}?tab=curated")).text) == base_first
    csv = (await client.get(f"/collections/{cid}/urls/curated?format=csv")).text
    assert [line.split(",")[0].removeprefix(f"https://{cid}") or "/" for line in csv.splitlines()[1:]] == base_first


def collections_in_order(page: str) -> list[str]:
    return re.findall(r'<td class="name"><a href="/collections/([^"]+)"', page)


async def test_dashboard_sorts_by_column_and_numbers_rows(crawler_client):
    c = crawler_client
    for host, name, n in (("b.org", "Bee", 8), ("a.org", "ant", 3), ("c.org", "Cat", 5)):
        await c.post("/api/collections", json={"seed_url": f"https://{host}", "name": name, "max_pages": n})
    for host in ("b.org", "c.org"):  # a.org stays in the backlog with no job at all
        await c.post(f"/api/collections/{host}/scrape")
        await wait_job(c, host)

    page = (await c.get("/rows")).text  # unsorted: newest first, every column offering a sort
    assert collections_in_order(page) == ["c.org", "a.org", "b.org"]
    assert page.count("⇅") == 7 and "aria-sort" not in page
    assert page.count('<td class="sn"></td>') == 3  # numbered by a CSS counter, one cell per row

    async def order(sort, direction):
        return collections_in_order((await c.get("/rows", params={"sort": sort, "dir": direction})).text)

    assert await order("name", "asc") == ["a.org", "b.org", "c.org"]  # case-insensitive
    assert await order("name", "desc") == ["c.org", "b.org", "a.org"]
    assert await order("dump", "asc") == ["a.org", "c.org", "b.org"]
    assert await order("dump", "desc") == ["b.org", "c.org", "a.org"]
    assert await order("status", "asc") == ["a.org", "c.org", "b.org"]  # pipeline order, ties newest first
    assert (await order("job", "asc"))[-1] == "a.org" and (await order("job", "desc"))[-1] == "a.org"  # no job: last
    assert (await order("updated", "desc"))[-1] == "a.org"
    assert await order("bogus", "asc") == ["c.org", "a.org", "b.org"]  # an unknown key is ignored

    # the sorted header shows its direction and flips on the next click; the sort rides in the
    # filter form so filtering, SSE refreshes and a reload keep it
    page = (await c.get("/", params={"sort": "dump", "dir": "desc", "q": ".org"})).text
    assert collections_in_order(page) == ["b.org", "c.org", "a.org"]
    assert 'aria-sort="descending"' in page and "dashSort('dump', 'asc')" in page
    assert '<input type="hidden" name="sort" value="dump"><input type="hidden" name="dir" value="desc">' in page


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
    # Nothing left to decide, and the table is still there with every row in its place, marked
    # decided: a list a curator works down must not renumber or empty itself as rows are finished.
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert 'class="urls ai-review"' in page and "✓ row" not in page
    assert page.count("✓ decided") == 8 and "Accept all (" not in page  # the bulk bar goes with the suggestions
    assert "Hide decided 8" in page
    # the toggle narrows the table to what is left, and offers the decided rows back
    page = (await c.get("/collections/ex.org?tab=curate&decided=hide")).text
    assert "Nothing left to decide" in page and "show the 8 decided rows" in page
    assert "Show decided 8" in page
    audit = (await c.get("/api/collections/ex.org/audit")).json()
    assert any(a["action"] == "ai.bulk_accept" and "(https://ex.org/p2)" in a["detail"] for a in audit)


# ── the list must not move under the curator ───────────────────────────
# Reported by the curators: accepting a title and a division sometimes reordered the metadata list
# they were working down. Two causes, both here: a sorted column sorted by the stored value (every
# undecided row NULL, NULLS LAST, so the first row decided leapt to the top), and the review table
# dropping a row the moment its last suggestion was decided (renumbering every row below it).

CID = "ex.org"
PATHS = ["/about", "/data/aerosol", "/data/ozone", "/earth/climate", "/helio/sun",
         "/images/gallery/aurora", "/missions/mars", "/software/tools"]


async def classified(client, paths=PATHS):
    """A collection whose delta URLs all carry AI suggestions (fake provider)."""
    from sde_curation.models import DumpUrl

    await client.post("/api/collections", json={"seed_url": f"https://{CID}", "name": "Ex", "max_pages": 50})
    await client.app.state.db.replace_dump(
        CID, [DumpUrl(collection_id=CID, url=f"https://{CID}{p}", scraped_title=p.rsplit("/", 1)[-1],
                      full_text=f"body of {p}") for p in paths])
    assert (await client.post(f"/api/collections/{CID}/recompute")).status_code == 200
    assert (await client.post(f"/api/collections/{CID}/suggest/metadata")).status_code in (200, 202)
    await wait_job(client, CID)


async def pending_for(client, field: str) -> set[str]:
    items = (await client.get(f"/api/collections/{CID}/delta", params={"per": 50})).json()["items"]
    return {d["url"] for d in items if d[f"{field}_ai"]}


def urls_in_order_any(page: str) -> list[str]:
    """Every URL of the page's table, in the order it lists them (any path, not just /pN)."""
    return re.findall(r'<td class="url"><a href="(https://ex\.org[^"]*)"', page)


def review_table(page: str) -> list[tuple[str, str]]:
    """(row number, URL) of the metadata review table, in the order the page lists them."""
    table = page.split('class="urls ai-review"')[1].split("</table>")[0]
    return re.findall(r'<td class="num muted">(\d+)</td>\s*<td class="url"><a href="(https://ex\.org[^"]*)"', table)


async def test_accepting_a_suggestion_never_moves_the_row_in_a_sorted_table(client):
    """A column curation writes sorts by the value the row is shown with — its pending suggestion
    counted as accepted — so ✓ writes the value the row was already sorted under and the row stays
    under the curator's eye. It used to jump to the top of the list."""
    await classified(client)
    for col, field in [("division", "division"), ("document_type", "document_type"),
                       ("title", "title"), ("edited_by", "division")]:
        q = f"/collections/{CID}?tab=delta&sort={col}&dir=asc&per=50"
        before = urls_in_order_any((await client.get(q)).text)
        assert len(before) == len(PATHS)
        left = await pending_for(client, field)
        target = next(u for u in before[1:-1] if u in left)  # mid-list: a move shows either way
        r = await client.post(f"/api/collections/{CID}/ai/accept", json={"url": target, "field": field})
        assert r.status_code == 200, r.text
        assert urls_in_order_any((await client.get(q)).text) == before, f"sorted by {col}, accepted {field}"


async def test_the_metadata_review_list_keeps_a_decided_row_in_its_place(client):
    """Deciding the last suggestion on a row used to remove it, pulling every row below it up one —
    the row the curator was about to click moved out from under the pointer. The row stays, in its
    place and with its number, marked decided; "Hide decided" is how you drop them."""
    await classified(client)
    page = (await client.get(f"/collections/{CID}?tab=curate")).text
    before = review_table(page)
    assert [n for n, _ in before] == [str(i) for i in range(1, len(before) + 1)]
    target = before[1][1]  # the second row: anything dropping out of the list renumbers the rest

    # accept its fields one at a time — including the last one, which used to empty its place
    for field in ("title", "division", "document_type"):
        if target not in await pending_for(client, field):
            continue
        assert (await client.post(f"/api/collections/{CID}/ai/accept",
                                  json={"url": target, "field": field})).status_code == 200
        assert review_table((await client.get(f"/collections/{CID}?tab=curate")).text) == before

    assert not await pending_for(client, "title") & {target}
    page = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert "✓ decided" in page and "Hide decided 1" in page
    # and the curator who wants only what is left says so
    hidden = review_table((await client.get(f"/collections/{CID}?tab=curate&decided=hide")).text)
    assert target not in [u for _, u in hidden] and len(hidden) == len(before) - 1


async def test_the_review_counts_agree_with_the_rows_they_describe(client):
    """The table's total, its pager and the Hide decided count come from count_delta_ai, the rows
    from list_delta_ai — two SQL paths (a `dup` CTE and the same subquery inlined). They must agree
    on every filter, or the pager runs past the end of the list or stops short of it."""
    from sde_curation.db import AI_FIELDS

    await classified(client, PATHS + [f"/archive/{y}/report" for y in range(2010, 2026)])
    db = client.app.state.db
    # a spread of states: one row fully decided, one half decided, one dismissed, one untouched
    await client.post(f"/api/collections/{CID}/ai/bulk",
                      json={"decision": "accept", "url": f"https://{CID}/about"})
    await client.post(f"/api/collections/{CID}/ai/accept",
                      json={"url": f"https://{CID}/data/aerosol", "field": "title"})
    await client.post(f"/api/collections/{CID}/ai/bulk",
                      json={"decision": "reject", "url": f"https://{CID}/earth/climate"})
    # and a collision, so the with_dups arm of the query has something to carry
    await client.post(f"/api/collections/{CID}/patterns",
                      json={"type": "title", "match": f"https://{CID}/helio/sun", "value": "Tools"})
    await client.post(f"/api/collections/{CID}/patterns",
                      json={"type": "title", "match": f"https://{CID}/software/tools", "value": "Tools"})

    combos = [{"with_dups": True}, {"dups_only": True}]
    combos += [{"field": f} for f in AI_FIELDS]
    combos += [{"conf": c} for c in ("high", "medium", "low")]
    combos += [{"field": f, "conf": c} for f in AI_FIELDS for c in ("high", "low")]
    for kw in combos:
        whole, left = await db.count_delta_ai(CID, **kw)
        rows_whole, total_whole = await db.list_delta_ai(CID, limit=500, **kw, undecided_only=False)
        rows_left, total_left = await db.list_delta_ai(CID, limit=500, **kw, undecided_only=True)
        assert (whole, left) == (total_whole, total_left), kw          # the two SQL paths agree
        assert (len(rows_whole), len(rows_left)) == (whole, left), kw  # and the rows match the count
        # "decided" really means no suggestion left, and hiding them drops exactly those
        decided = [r.url for r in rows_whole if not (r.title_ai or r.division_ai or r.document_type_ai)]
        if kw.get("with_dups"):  # only this arm keeps decided rows; the filters are pending-only
            assert whole - left == len(decided), kw
            assert {r.url for r in rows_left} == {r.url for r in rows_whole} - set(decided), kw
        else:
            assert whole == left, kw

    # paging the expanded list visits every row of the round exactly once, numbered without gaps
    # (?per is clamped to at least 10, so the collection has to be bigger than one page)
    whole, _ = await db.count_delta_ai(CID, with_dups=True)
    assert whole > 10, "the paging check needs more than one page"
    seen: list[str] = []
    for page in range(1, -(-whole // 10) + 1):
        rows = review_table((await client.get(
            f"/collections/{CID}?tab=curate&focus=metadata&per=10&page={page}")).text)
        assert [n for n, _ in rows] == [str(len(seen) + i + 1) for i in range(len(rows))], page
        seen += [u for _, u in rows]
    assert len(seen) == len(set(seen)) == whole
