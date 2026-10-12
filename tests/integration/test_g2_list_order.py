"""The order the URL tables, the metadata review table and the dashboard list their rows in.

The order is SQL (db.url_order, DUMP/DELTA/CURATED_SORTS, the review table's WHERE) or the dashboard's
own sort, so it needs the real database. What matters to a curator: a table reads like a site (the
seed, then the pages under it, one level at a time), a sorted column sorts by the value the row
shows, the list never moves under the pointer while they decide suggestions, and the table's
counts, pager and rows agree.
"""

import re

import pytest

from sde_curation.db import AI_FIELDS
from sde_curation.models import (
    CuratedUrl,
    Division,
    DocumentType,
    DumpUrl,
    JobKind,
    JobRun,
    JobState,
    Status,
)

CID = "ex.org"
# path -> scraped title; the titles sort Aerosol, Aurora, Data, Gallery, Home, Missions, Monthly, Ozone
SITE = {"/data/aerosol/access": "Aerosol", "/": "Home", "/data/ozone": "Ozone", "/images/gallery/aurora": "Aurora",
        "/data": "Data", "/missions": "Missions", "/data/ozone/2024/monthly": "Monthly", "/images/gallery": "Gallery"}
# host, then path depth, then alphabetical: plain text order would put /data/aerosol/access above /data/ozone
BASE_FIRST = ["/", "/data", "/missions", "/data/ozone", "/images/gallery", "/data/aerosol/access",
              "/images/gallery/aurora", "/data/ozone/2024/monthly"]
BY_TITLE = sorted(SITE, key=SITE.get)
EXCLUDED = "/missions"
OLD_HASH = "0" * 64


def url(path: str) -> str:
    return f"https://{CID}{path}"


def path_of(u: str) -> str:
    return u.removeprefix(f"https://{CID}") or "/"


def paths_on_page(html: str) -> list[str]:
    return [p or "/" for p in re.findall(rf'<td class="url"><a href="https://{re.escape(CID)}([^"]*)"', html)]


async def site(client) -> None:
    """The site in the curated set, crawled again with new text (every page queued for review), and
    one page kept out by an exclude rule."""
    db = client.app.state.db
    await client.post("/api/collections", json={"seed_url": url(""), "name": "Ex", "max_pages": 50,
                                                "division": "Earth Science"})
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(p), scraped_title=t, full_text=f"body {t}")
                                for p, t in SITE.items()])
    # promoted before the site's text changed: every page comes back as a modified delta
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=url(p), scraped_title=t, content_hash=OLD_HASH,
                                              division=Division.EARTH_SCIENCE, document_type=DocumentType.DATA)
                                   for p, t in SITE.items()])
    assert (await client.post(f"/api/collections/{CID}/recompute")).status_code == 200
    r = await client.post(f"/api/collections/{CID}/patterns", json={"type": "exclude", "match": url(EXCLUDED)})
    assert r.status_code == 201, r.text


ORDERS = [  # (table, ?sort=, descending, expected paths)
    ("dump", None, False, BASE_FIRST),
    ("dump", "url", False, BASE_FIRST),
    ("dump", "url", True, BASE_FIRST[::-1]),
    ("dump", "scraped_title", True, BY_TITLE[::-1]),
    ("dump", "excluded", False, [p for p in BASE_FIRST if p != EXCLUDED] + [EXCLUDED]),
    ("dump", "excluded", True, [EXCLUDED] + [p for p in BASE_FIRST if p != EXCLUDED]),
    ("dump", "drop table dump_urls", True, BASE_FIRST),
    ("delta", None, False, [p for p in BASE_FIRST if p != EXCLUDED]),
    ("delta", "url", True, [p for p in BASE_FIRST[::-1] if p != EXCLUDED]),
    ("delta", "title", True, [p for p in BY_TITLE[::-1] if p != EXCLUDED]),
    ("delta", "nonsense", True, [p for p in BASE_FIRST if p != EXCLUDED]),
    ("curated", None, False, BASE_FIRST),
    ("curated", "url", True, BASE_FIRST[::-1]),
    ("curated", "title", False, BY_TITLE),
]


async def test_each_url_table_lists_its_rows_in_the_order_asked_for(client):
    """One table for the three URL tables: the default and url orders read base first, a column sorts
    both ways, excluded rows sort last ascending and first descending, an unknown key is the default."""
    await site(client)
    db = client.app.state.db
    lists = {"dump": db.list_dump, "delta": db.list_deltas, "curated": db.list_curated}
    for table, sort, desc, expected in ORDERS:
        rows, _ = await lists[table](CID, limit=100, sort=sort, desc=desc)
        got = [path_of(r["url"] if isinstance(r, dict) else r.url) for r in rows]
        assert got == expected, (table, sort, desc)


async def test_a_sorted_page_shows_its_sort_keeps_it_in_the_filters_and_its_csv_follows_it(client):
    await site(client)
    page = (await client.get(f"/collections/{CID}?tab=delta&sort=url&dir=desc")).text
    assert paths_on_page(page) == [p for p in BASE_FIRST[::-1] if p != EXCLUDED]
    assert 'aria-sort="descending"' in page and "▼" in page
    assert 'name="sort" value="url"' in page, "the filter form drops the sort"
    assert "sort=url&dir=asc&per=50&page=1" in page, "the header does not flip the direction back to page 1"
    for table in ("curated", "delta"):
        csv = (await client.get(f"/collections/{CID}/urls/{table}?format=csv&sort=url&dir=desc")).text
        header, first = csv.splitlines()[:2]
        assert path_of(first.split(",")[header.split(",").index("url")]) == "/data/ozone/2024/monthly", table


# ── the metadata review: the list must not move under the curator ────────


async def suggested(client, extra: int = 0) -> list[str]:
    """Every delta URL (the site plus `extra` archive pages) carries AI suggestions for all three
    fields, with a spread of confidences and two divisions. Returns the URLs, base first."""
    db = client.app.state.db
    await client.post("/api/collections", json={"seed_url": url(""), "name": "Ex", "max_pages": 100})
    paths = list(SITE) + [f"/archive/{2000 + y}/report" for y in range(extra)]
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(p), scraped_title=SITE.get(p) or f"Report {p}",
                                        full_text=f"body of {p}") for p in paths])
    assert (await client.post(f"/api/collections/{CID}/recompute")).status_code == 200
    confs = ("high", "medium", "low")
    await db.set_delta_ai(CID, [
        {"url": url(p), "title": f"AI {i:02d}", "title_conf": confs[i % 3],
         "division": (Division.HELIOPHYSICS if i % 2 else Division.ASTROPHYSICS).value, "division_conf": confs[(i + 1) % 3],
         "document_type": DocumentType.DATA.value, "document_type_conf": confs[(i + 2) % 3], "model": "fake"}
        for i, p in enumerate(paths)])
    rows, _ = await db.list_dump(CID, limit=1000)
    return [r["url"] for r in rows]


@pytest.mark.parametrize(("column", "field"), [("division", "division"), ("document_type", "document_type"),
                                               ("title", "title"), ("edited_by", "division")])
async def test_accepting_a_suggestion_leaves_the_row_where_it_was_in_a_sorted_table(client, column, field):
    """A sorted column sorts by the value the row shows, its pending suggestion counted as accepted,
    so ✓ writes the value the row was already sorted under. It used to jump to the top of the list
    (every undecided row was NULL, NULLS LAST)."""
    urls = await suggested(client)
    q = f"/collections/{CID}?tab=delta&sort={column}&dir=asc&per=50"
    before = paths_on_page((await client.get(q)).text)
    target = url(before[len(before) // 2])  # mid-list: a move up or down shows

    r = await client.post(f"/api/collections/{CID}/ai/accept", json={"url": target, "field": field})

    assert r.status_code == 200, r.text
    assert len(before) == len(urls)
    assert paths_on_page((await client.get(q)).text) == before


def review_table(page: str) -> list[tuple[str, str]]:
    """(row number, path) of the metadata review table, in page order."""
    table = page.split('class="urls ai-review"')[1].split("</table>")[0]
    return [(n, path_of(u)) for n, u in re.findall(
        r'<td class="num muted">(\d+)</td>\s*<td class="url"><a href="(https://[^"]*)"', table)]


async def test_a_decided_row_keeps_its_place_and_number_until_the_curator_hides_decided_rows(client):
    """Deciding the last suggestion on a row used to drop it and renumber every row below it, so the
    row about to be clicked moved out from under the pointer."""
    await suggested(client)
    before = review_table((await client.get(f"/collections/{CID}?tab=curate")).text)
    target = before[1][1]  # the second row: a row dropping out would renumber the rest

    for field in AI_FIELDS:
        r = await client.post(f"/api/collections/{CID}/ai/accept", json={"url": url(target), "field": field})
        assert r.status_code == 200, r.text
        assert review_table((await client.get(f"/collections/{CID}?tab=curate")).text) == before, field

    page = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert "✓ decided" in page and "Hide decided 1" in page
    hidden = review_table((await client.get(f"/collections/{CID}?tab=curate&decided=hide")).text)
    assert [p for _, p in hidden] == [p for _, p in before if p != target]
    assert "Show decided 1" in (await client.get(f"/collections/{CID}?tab=curate&decided=hide")).text


async def test_the_review_counts_and_pager_agree_with_the_rows_they_describe(client):
    """The total, the pager and the Hide-decided count come from count_delta_ai; the rows from
    list_delta_ai: two SQL paths (a `dup` CTE, the same subquery inlined). They must agree under
    every filter, or the pager runs past the end of the list or stops short of it."""
    await suggested(client, extra=16)
    db = client.app.state.db
    # one row fully decided, one half decided, one dismissed, and two pages given the same title
    await client.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept", "url": url("/")})
    await client.post(f"/api/collections/{CID}/ai/accept", json={"url": url("/data"), "field": "title"})
    await client.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "reject", "url": url("/missions")})
    for p in ("/data/ozone", "/images/gallery"):
        await client.post(f"/api/collections/{CID}/patterns", json={"type": "title", "match": url(p), "value": "Same"})

    combos = [{"with_dups": True}, {"dups_only": True}, *({"field": f} for f in AI_FIELDS),
              *({"conf": c} for c in ("high", "medium", "low")),
              *({"field": f, "conf": c} for f in AI_FIELDS for c in ("high", "low"))]
    for kw in combos:
        whole, left = await db.count_delta_ai(CID, **kw)
        rows_whole, total_whole = await db.list_delta_ai(CID, limit=500, **kw, undecided_only=False)
        rows_left, total_left = await db.list_delta_ai(CID, limit=500, **kw, undecided_only=True)
        assert (whole, left) == (total_whole, total_left) == (len(rows_whole), len(rows_left)), kw
        decided = {r.url for r in rows_whole if not (r.title_ai or r.division_ai or r.document_type_ai)}
        if kw.get("with_dups"):  # only this arm keeps decided rows; the filters list pending rows only
            assert {r.url for r in rows_left} == {r.url for r in rows_whole} - decided and decided, kw
        else:
            assert whole == left, kw

    # paging the review table visits every row of the round once, numbered without gaps
    whole, _ = await db.count_delta_ai(CID, with_dups=True)
    per = 10  # ?per is clamped to at least 10
    assert whole > per, "the paging check needs more than one page"
    seen: list[tuple[str, str]] = []
    for n in range(1, -(-whole // per) + 1):
        seen += review_table((await client.get(f"/collections/{CID}?tab=curate&focus=metadata&per={per}&page={n}")).text)
    assert [int(n) for n, _ in seen] == list(range(1, whole + 1))
    assert len({p for _, p in seen}) == whole


# ── the dashboard ────────────────────────────────────────────────────────


def collections_in_order(page: str) -> list[str]:
    return re.findall(r'<td class="name"><a href="/collections/([^"]+)"', page)


async def test_the_dashboard_sorts_by_a_column_and_puts_collections_without_a_job_last(client):
    """Unsorted is newest first, and newest first stays the tiebreak; a collection that never ran a
    job goes last in both directions; an unknown key is ignored."""
    db = client.app.state.db
    for host, name, pages in (("b.org", "Bee", 8), ("a.org", "ant", 3), ("c.org", "Cat", 5)):  # created in this order
        await client.post("/api/collections", json={"seed_url": f"https://{host}", "name": name, "max_pages": 50})
        await db.replace_dump(host, [DumpUrl(collection_id=host, url=f"https://{host}/p{i}") for i in range(pages)])
    for host in ("b.org", "c.org"):  # a.org stays in the backlog, with no job at all
        await db.set_status(host, Status.SCRAPED, "crawled", force=True)
        await db.insert_job(JobRun(collection_id=host, kind=JobKind.SCRAPE, state=JobState.SUCCEEDED))

    async def order(sort: str, direction: str) -> list[str]:
        return collections_in_order((await client.get("/rows", params={"sort": sort, "dir": direction})).text)

    assert collections_in_order((await client.get("/rows")).text) == ["c.org", "a.org", "b.org"]
    assert await order("name", "asc") == ["a.org", "b.org", "c.org"], "case-insensitive"
    assert await order("name", "desc") == ["c.org", "b.org", "a.org"]
    assert await order("dump", "asc") == ["a.org", "c.org", "b.org"]
    assert await order("dump", "desc") == ["b.org", "c.org", "a.org"]
    assert await order("status", "asc") == ["a.org", "c.org", "b.org"], "pipeline order, ties newest first"
    assert (await order("job", "asc"))[-1] == (await order("job", "desc"))[-1] == "a.org"
    assert await order("bogus", "asc") == ["c.org", "a.org", "b.org"]

    page = (await client.get("/", params={"sort": "dump", "dir": "desc", "q": ".org"})).text
    assert collections_in_order(page) == ["b.org", "c.org", "a.org"]
    assert 'aria-sort="descending"' in page and "dashSort('dump', 'asc')" in page
    assert '<input type="hidden" name="sort" value="dump"><input type="hidden" name="dir" value="desc">' in page
