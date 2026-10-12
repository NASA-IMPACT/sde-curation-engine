"""Loading a crawl into the dump (JobManager.ingest_dump and the scrape job) on the in-memory
FakeDatabase: which pages are stored, what the job reports while and after it loads them, and the
size limit on each chunk read. Replaces the ingest checks of the old
tests/integration/test_api_scrape.py (P4, TEST-STRATEGY-2026-10-09.md)."""

import pytest

from sde_curation import jobs as jobs_mod
from sde_curation.engine.text import content_hash
from sde_curation.models import Status
from tests.support.engine import CID, Scraper, collection, finished, make_engine
from tests.support.fake_db import FakeDatabase

DOTTED = f"https://{CID}/simbad/../guide/otypes.htx"  # simbad.cds.unistra.fr links both spellings
PLAIN = f"https://{CID}/guide/otypes.htx"
CHUNK_BYTES = 250  # a small chunk limit, so the pages below cross it several times
TEXT_SIZES = [100, 300, 10, 10, 10, 240, 5, 1000, 1]  # chunks: [100,300] [10,10,10,240] [5,1000] [1]
SPELLINGS_OF_ONE_PAGE = 2  # the fake crawl below reaches every page under two links


async def engine_with_collection(tmp_path, scraper=None):
    db = FakeDatabase()
    await collection(db, titles=[])
    return db, make_engine(db, tmp_path, scraper=scraper)


async def test_ingest_keeps_one_spelling_of_each_page(tmp_path):
    """http/https, a trailing slash, a #fragment, www. and a redirect are one page to a curator; a
    URL alone on its page is not touched."""
    db, engine = await engine_with_collection(tmp_path)
    docs = [
        {"url": f"http://{CID}/a", "title": "A (http)", "full_text": "a"},
        {"url": f"https://{CID}/a", "title": "A", "full_text": "a"},
        {"url": f"https://{CID}/map/", "title": "Map (slash)", "full_text": "m"},
        {"url": f"https://{CID}/map", "title": "Map", "full_text": "m"},
        {"url": f"https://{CID}/faq#top", "title": "FAQ", "full_text": "f"},
        {"url": f"https://{CID}/faq", "title": "FAQ", "full_text": "f"},
        {"url": f"https://{CID}/maps", "final_url": f"https://{CID}/map", "title": "Map", "full_text": "m"},
        {"url": f"https://www.{CID}/map/", "title": "Map (www)", "full_text": "m"},
        {"url": f"http://{CID}/only-http", "title": "B", "full_text": "b"},
        {"url": f"https://{CID}/only-slash/", "title": "C", "full_text": "c"},
    ]

    stored = await engine.ingest_dump(CID, docs)

    assert stored == 5
    assert sorted(await db.dump_urls(CID)) == [
        f"http://{CID}/only-http", f"https://{CID}/a", f"https://{CID}/faq", f"https://{CID}/map",
        f"https://{CID}/only-slash/"]


async def test_dot_segment_urls_are_ingested_as_pages_of_their_own(tmp_path):
    """/simbad/../guide/x and /guide/x are two URLs the curator decides separately (WAF 2026-09-28)."""
    db, engine = await engine_with_collection(tmp_path)
    docs = [{"url": DOTTED, "title": "Otypes", "full_text": "o"}, {"url": PLAIN, "title": "Otypes", "full_text": "o"},
            {"url": f"https://{CID}/simbad/./../tools/manage?x=1", "title": "Manage", "full_text": "m"}]

    stored = await engine.ingest_dump(CID, docs)

    assert (stored, sorted(await db.dump_urls(CID))) == (3, sorted(d["url"] for d in docs))


async def test_ingest_reports_the_pages_read_then_the_write_out_then_what_it_dropped(tmp_path):
    """On a multi-GB crawl the load takes minutes after the crawler went quiet: the job says how far
    it got instead of sitting on the crawler's last figure."""
    _, engine = await engine_with_collection(tmp_path)
    seen: list[dict] = []

    async def on_progress(p):
        seen.append(p)

    docs = [{"url": f"https://{CID}/p{i}", "title": f"Page {i}", "full_text": f"t{i}"} for i in range(3)]
    await engine.ingest_dump(CID, docs, on_progress=on_progress)

    assert seen == [{"phase": "ingest", "ingested": 3}, {"phase": "ingest_store", "ingested": 3},
                    {"duplicates_dropped": 0}]


async def test_a_chunk_closes_at_its_text_size_and_no_page_is_dropped_or_read_twice(tmp_path, monkeypatch):
    """A run of 1 MB pages once made 500 MB chunks: a chunk now also closes at a text size. The page
    that crosses the limit ends its chunk; none is lost or repeated at the boundary."""
    monkeypatch.setattr(jobs_mod, "_INGEST_BATCH_BYTES", CHUNK_BYTES)
    db, engine = await engine_with_collection(tmp_path)
    docs = [{"url": f"https://{CID}/p{i}", "title": f"P{i}", "full_text": f"{i}" * n} for i, n in enumerate(TEXT_SIZES)]

    stored = await engine.ingest_dump(CID, docs)

    assert stored == len(TEXT_SIZES)
    assert await db.dump_content_hashes(CID) == {d["url"]: content_hash(d["full_text"]) for d in docs}


async def test_a_scrape_says_how_many_links_it_read_and_how_many_duplicates_it_dropped(tmp_path):
    """18 read → 9 stored looks like lost pages unless the job and the status history say why."""
    pages = 9
    crawl = [{"url": f"{scheme}://{CID}/p{i}", "title": f"P{i}", "full_text": f"t{i}"}
             for i in range(pages) for scheme in ("http", "https")]
    db, engine = await engine_with_collection(tmp_path, Scraper(tmp_path, crawl))

    job = await finished(db, (await engine.start_scrape(await db.get_collection(CID), actor="alice")).id)

    read = pages * SPELLINGS_OF_ONE_PAGE
    assert (job.progress["ingested"], job.progress["docs"], job.progress["duplicates_dropped"]) == (
        read, pages, read - pages)
    assert (await db.get_collection(CID)).dump_count == pages
    assert db._status_history[-1]["note"] == f"scrape ok: {pages} documents ({read} read, {read - pages} duplicate links dropped)"


@pytest.mark.parametrize("dropped", [False, True], ids=["nothing dropped", "duplicates dropped"])
async def test_a_scrape_records_when_it_crawled_and_marks_the_collection_scraped(tmp_path, dropped):
    crawl = [{"url": f"https://{CID}/p1", "title": "P1", "full_text": "t"}]
    if dropped:
        crawl.append({"url": f"http://{CID}/p1", "title": "P1", "full_text": "t"})
    db, engine = await engine_with_collection(tmp_path, Scraper(tmp_path, crawl))
    assert (await db.get_collection(CID)).last_scraped_at is None

    await finished(db, (await engine.start_scrape(await db.get_collection(CID), actor="alice")).id)

    c = await db.get_collection(CID)
    assert (c.status, c.last_scraped_at is not None) == (Status.SCRAPED, True)
    assert db._status_history[-1]["note"].startswith("scrape ok: 1 documents") is True
    assert ("duplicate links dropped" in db._status_history[-1]["note"]) is dropped


async def test_ingest_strips_nul_bytes_from_every_text_field(tmp_path):
    """PDF text extraction can emit NUL, which PostgreSQL text rejects: one NUL failed the whole load."""
    db, engine = await engine_with_collection(tmp_path)
    docs = [{"url": f"https://{CID}/a.pdf", "title": "A\x00", "full_text": "x\x00y", "content_type": "application/pdf\x00"}]
    failures = [{"url": f"https://{CID}/b", "reason": "fail", "detail": "bad\x00byte"}]

    assert await engine.ingest_dump(CID, docs, failures) == 1

    [page] = await db.load_dump(CID)
    assert (page.scraped_title, page.content_type, page.content_hash) == ("A", "application/pdf", content_hash("xy"))
    assert await db.load_dump_failures(CID) == {f"https://{CID}/b": "fail"}
