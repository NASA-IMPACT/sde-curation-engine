"""CurationService decisions, on the in-memory FakeDatabase (tests/support/fake_db.py): which
recompute a change takes, what it queues, and when a review round ends."""

import pytest

from sde_curation.curation import CurationService
from sde_curation.models import (
    Collection,
    ConnectorType,
    CuratedUrl,
    Division,
    DocumentType,
    DumpUrl,
    PatternCreate,
    PatternType,
)
from tests.support.fake_db import FakeDatabase

CID = "example.org"
PAGES = 8  # a crawl of eight pages
URLS = [f"https://{CID}/p{i}" for i in range(1, PAGES + 1)]


async def crawled() -> tuple[FakeDatabase, Collection]:
    """A collection right after a crawl is ingested: the dump is loaded and the delta set is
    cleared, as the scrape job does (jobs.py), and nobody has pressed Start curating yet."""
    db = FakeDatabase()
    await db.insert_collection(Collection(collection_id=CID, name=CID, seed_url=f"https://{CID}",
                                          connector=ConnectorType.CRAWLER, max_pages=100,
                                          division=Division.HELIOPHYSICS))
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=u, scraped_title=f"Page {i}", full_text=u)
                                for i, u in enumerate(URLS, 1)])
    await db.replace_deltas(CID, [], [])
    return db, await db.get_collection(CID)


def recorded(service: CurationService) -> list[str]:
    """Which recompute each change takes: 'full' (the whole collection) or 'scoped' (one page)."""
    calls: list[str] = []
    full, scoped = service._recompute, service._recompute_keys

    async def full_spy(*a, **k):
        calls.append("full")
        return await full(*a, **k)

    async def scoped_spy(*a, **k):
        ds = await scoped(*a, **k)
        calls.append("scoped" if ds is not None else "scoped-refused")
        return ds

    service._recompute, service._recompute_keys = full_spy, scoped_spy
    return calls


# ── Known bugs from REVIEW-SINCE-DEV-MERGE-2026-10-08.md (expected failures until fixed) ──────────


@pytest.mark.xfail(strict=True, reason="H1: an edit after a crawl and before Start curating queues only that page")
async def test_an_edit_before_start_curating_queues_the_whole_crawl():
    """The per-page recompute is only right once a full recompute has built the delta set for the
    current crawl. Before that, an edit must queue every page of the crawl, as Start curating does."""
    db, c = await crawled()
    service = CurationService(db)

    await service.replace_exact_pattern(c, PatternCreate(type=PatternType.TITLE, match=URLS[2], value="By hand"),
                                        old_id=None)

    assert sorted(d.url for d in await db.load_deltas(CID)) == sorted(URLS)


@pytest.mark.xfail(strict=True, reason="L4: a ✓ that removes a per-page exclude rule takes the full recompute")
async def test_including_a_page_again_recomputes_only_that_page():
    db, c = await crawled()
    service = CurationService(db)
    await service.recompute(c)  # Start curating
    await service.set_excluded(c, URLS[2], True)
    calls = recorded(service)

    await service.set_excluded(c, URLS[2], False)

    assert calls == ["scoped"]


@pytest.mark.xfail(strict=True, reason="L7: a review round stays open after its queue empties without a full promote")
async def test_a_review_round_ends_when_its_queue_is_empty():
    """Re-curate everything queues every curated page and opens a review round. When the queue
    empties (here: all but one page promoted, the last one excluded), the round is over."""
    db, _ = await crawled()
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=u, scraped_title=f"Page {i}", title=f"Page {i}",
                                              division=Division.HELIOPHYSICS,
                                              document_type=DocumentType.DOCUMENTATION)
                                   for i, u in enumerate(URLS, 1)])
    service = CurationService(db)
    await db.set_review_round(CID, True)
    await service.recompute(await db.get_collection(CID), review_all=True)
    assert len(await db.load_deltas(CID)) == PAGES
    await service.promote_urls(await db.get_collection(CID), URLS[:-1])

    await service.set_excluded(await db.get_collection(CID), URLS[-1], True)

    assert await db.load_deltas(CID) == []
    assert (await db.get_collection(CID)).review_round is False
