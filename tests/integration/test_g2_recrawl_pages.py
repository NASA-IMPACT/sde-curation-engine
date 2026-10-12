"""What the pages say after a curated collection is crawled again: renamed pages (a new spelling of
the same page), pages the crawl proves gone, pages it could not fetch (kept, flagged), pages whose
text changed, and a crawl that stopped at its page cap. The diff itself is unit-tested
(tests/unit/test_url_identity.py, test_diff.py, test_curation_service.py); this checks the filters
the tables and CSVs use (SQL) and the words the curator reads."""

from sde_curation.engine.text import content_hash
from sde_curation.models import (
    CuratedUrl,
    Division,
    DocumentType,
    DumpFailure,
    DumpUrl,
    Status,
    utcnow,
)

CID = "ex.org"
TEXT, NEW_TEXT = "text " * 5, "brand new body"
OLD_P1 = "http://www.ex.org/p1"  # curated under an old spelling; the crawl now finds https://ex.org/p1/
PAGES = (1, 2, 3, 4, 6, 7, 8, 9)
CURATED_CSV_COLUMNS = ["url", "excluded", "scraped_title", "title", "division", "document_type", "text_len",
                       "edited_by", "crawl_failure"]


def u(i: int) -> str:
    return f"https://{CID}/p{i}"


async def curated(client, pages=PAGES) -> None:
    """A curated collection: every page promoted with its text, p1 under an older spelling."""
    db = client.app.state.db
    await client.post("/api/collections", json={"seed_url": f"https://{CID}", "name": "Ex", "max_pages": 10})
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=u(i), scraped_title=f"Page {i}", full_text=TEXT)
                                for i in pages])
    await db.replace_curated(CID, [
        CuratedUrl(collection_id=CID, url=OLD_P1 if i == 1 else u(i), scraped_title=f"Page {i}",
                   division=Division.EARTH_SCIENCE, document_type=DocumentType.DATA, content_hash=content_hash(TEXT))
        for i in pages])
    await db.set_status(CID, Status.CURATED, "promoted", force=True)


async def test_a_recrawl_shows_renamed_removed_kept_and_changed_pages_in_the_tables_and_csvs(client):
    db = client.app.state.db
    await curated(client)
    # the re-crawl: p1 and p2 under new spellings, p3 blocked (403), p4 gone (404), p6 never met in
    # a complete crawl, p8's text changed, p7 and p9 unchanged
    await db.replace_dump(CID, [
        DumpUrl(collection_id=CID, url=f"{u(1)}/", scraped_title="Page 1", full_text=TEXT),
        DumpUrl(collection_id=CID, url=f"{u(2)}#top", scraped_title="Page 2", full_text=TEXT),
        DumpUrl(collection_id=CID, url=u(7), scraped_title="Page 7", full_text=TEXT),
        DumpUrl(collection_id=CID, url=u(8), scraped_title="Page 8", full_text=NEW_TEXT),
        DumpUrl(collection_id=CID, url=u(9), scraped_title="Page 9", full_text=TEXT),
    ], [DumpFailure(collection_id=CID, url=u(3), reason="http_403", status=403),
        DumpFailure(collection_id=CID, url=u(4), reason="http_404", status=404)])

    r = await client.post(f"/api/collections/{CID}/recompute")
    assert r.json() == {"new": 0, "modified": 3, "deleted": 2, "excluded": 0, "content_changed": 1, "renamed": 2,
                        "kept": 1}

    step = "counts"
    overview = (await client.get(f"/collections/{CID}?step=curating&tab=overview")).text
    assert "(0 new, 3 modified incl. 2 renamed, 2 removed; 1 curated kept" in overview, f"[{step}] overview"
    curate = (await client.get(f"/collections/{CID}?tab=curate")).text
    for link in (">2 renamed</a>", ">1 text changed</a>", ">2 removed ↗</a>", ">1 kept ↗</a>"):
        assert link in curate, f"[{step}] curate tab: {link}"
    assert "could not be fetched by the last crawl" in curate, f"[{step}] no crawl warning"

    step = "renamed"
    renamed = (await client.get(f"/api/collections/{CID}/delta?renamed=true")).json()["items"]
    assert {d["url"]: d["renamed_from"] for d in renamed} == {f"{u(1)}/": OLD_P1, f"{u(2)}#top": u(2)}, step
    assert (await client.get(f"/api/collections/{CID}/delta", params={"q": "www.ex.org/p1"})).json()["total"] == 1, \
        f"[{step}] the search does not find a renamed row by its old spelling"
    page = (await client.get(f"/collections/{CID}?tab=delta&renamed=true")).text
    assert ">renamed<" in page and f"was {OLD_P1}" in page and u(7) not in page, step

    step = "removed"
    removed = (await client.get(f"/api/collections/{CID}/delta?kind=deleted")).json()["items"]
    assert {d["url"]: d["crawl_failure"] for d in removed} == {u(4): "http_404", u(6): None}, step
    page = (await client.get(f"/collections/{CID}?tab=delta&kind=deleted")).text
    assert "HTTP 404 not found" in page and "never seen by the crawl" in page, step

    step = "kept"
    page = (await client.get(f"/collections/{CID}?tab=curated&unreachable=true")).text
    assert u(3) in page and "kept · HTTP 403 forbidden" in page and u(7) not in page, step
    csv = (await client.get(f"/collections/{CID}/urls/curated?format=csv&unreachable=true")).text.splitlines()
    assert csv[0].split(",") == CURATED_CSV_COLUMNS and len(csv) == 2 and csv[1].endswith("http_403"), step
    # the listings carry the size of the approved text, never the text itself
    kept = (await client.get(f"/api/collections/{CID}/curated", params={"q": "/p3"})).json()["items"]
    assert [(r["url"], r["text_len"], r.get("full_text")) for r in kept] == [(u(3), len(TEXT), None)], step

    step = "text changed"
    changed = (await client.get(f"/api/collections/{CID}/delta?content_changed=true")).json()
    assert [(d["url"], d["kind"], d["content_changed"]) for d in changed["items"]] == [(u(8), "modified", True)], step
    page = (await client.get(f"/collections/{CID}?tab=delta&changed=true")).text
    assert "text changed" in page and u(8) in page and u(7) not in page, step
    csv = (await client.get(f"/collections/{CID}/urls/delta?format=csv&changed=true")).text.splitlines()
    assert "content_changed" in csv[0].split(",") and len(csv) == 2 and u(8) in csv[1], step


async def test_a_capped_crawl_says_it_stopped_at_its_cap_and_keeps_the_pages_it_never_reached(client):
    cap = 3
    db = client.app.state.db
    await curated(client, pages=(1, 2, 3))
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=u(i), scraped_title=f"Page {i}", full_text=TEXT)
                                for i in (1, 2)])
    await db.execute("UPDATE collections SET max_pages=%s WHERE collection_id=%s", (cap, CID))
    await db.set_last_scraped(CID, utcnow(), capped=True)

    r = await client.post(f"/api/collections/{CID}/recompute")

    assert (r.json()["deleted"], r.json()["kept"]) == (0, 1)
    page = (await client.get(f"/collections/{CID}?tab=curated&unreachable=true")).text
    assert u(3) in page and "not visited: the crawl stopped at its page cap" in page
    assert f"stopped at its page cap ({cap} pages)" in (await client.get(f"/collections/{CID}?tab=curate")).text
