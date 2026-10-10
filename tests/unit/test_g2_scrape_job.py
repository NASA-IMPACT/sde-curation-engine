"""What a finished scrape job records about the crawl beyond its pages: whether it stopped at the
page cap (a capped crawl's absences are not evidence a page is gone, see the curation service's
"capped crawl: kept as not visited"), who started it, and that the status move it makes is the
system's, not the starter's."""

import pytest

from sde_curation.models import SYSTEM_ACTOR, Collection, ConnectorType, Status
from tests.support.engine import CID, Scraper, finished, make_engine
from tests.support.fake_db import FakeDatabase

CRAWLED = 3  # documents in the fake crawl
CRAWL = [{"url": f"https://{CID}/p{i}", "title": f"P{i}", "full_text": f"page {i}"} for i in range(CRAWLED)]
STARTER = "alice"


async def scrape(tmp_path, *, max_pages: int, db: FakeDatabase | None = None):
    db = db or FakeDatabase()
    c = await db.insert_collection(Collection(collection_id=CID, name=CID, seed_url=f"https://{CID}",
                                              connector=ConnectorType.CRAWLER, max_pages=max_pages))
    engine = make_engine(db, tmp_path, scraper=Scraper(tmp_path, CRAWL))
    job = await engine.start_scrape(c, actor=STARTER)
    return db, await finished(db, job.id)


@pytest.mark.parametrize(("max_pages", "capped"), [(CRAWLED, True), (CRAWLED + 1, False)],
                         ids=["reached the cap", "one page under the cap"])
async def test_a_scrape_that_reaches_its_page_cap_is_recorded_as_capped(tmp_path, max_pages, capped):
    db, job = await scrape(tmp_path, max_pages=max_pages)

    assert (job.progress["docs"], job.progress["capped"]) == (CRAWLED, capped)
    assert (await db.get_collection(CID)).last_crawl_capped is capped


async def test_a_scrape_records_its_starter_and_moves_the_status_as_the_system(tmp_path):
    """The curator who pressed Scrape started the job; the move to scraped happens later, inside the
    job, so the history, collection.yaml and the notification name the system."""
    db = FakeDatabase()
    moves: list[tuple[str, str]] = []

    async def hook(cid, old, new, note, actor):
        moves.append((str(new), actor))

    db.on_status_change = hook
    _, job = await scrape(tmp_path, max_pages=100, db=db)

    assert job.started_by == STARTER
    assert moves == [(Status.SCRAPED.value, SYSTEM_ACTOR)]
