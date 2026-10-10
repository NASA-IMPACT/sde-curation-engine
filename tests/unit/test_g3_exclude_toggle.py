"""The row's ✗ exclude / ✓ include toggle (CurationService.set_excluded on the in-memory
FakeDatabase) in two cases the main toggle table (test_curation_service.py) does not hold: a page an
include glob keeps in, and a URL with dot segments. Replaces checks of the old
tests/integration/test_api_curation.py and test_api_scrape.py (P4, TEST-STRATEGY-2026-10-09.md)."""

from sde_curation.curation import CurationService
from sde_curation.models import Collection, ConnectorType, Division, DumpUrl, PatternType
from tests.support.fake_db import FakeDatabase
from tests.unit.test_curation import CID, URLS
from tests.unit.test_curation_service import build, fresh, queued, rule, rules_of

EVERY_PAGE = f"https://{CID}/*"
P_PAGES = f"https://{CID}/p*"  # every crawled page: p1..p8
P3 = URLS[2]
DOTTED = f"https://{CID}/simbad/../guide/otypes.htx"
PLAIN = f"https://{CID}/guide/otypes.htx"


async def test_a_page_an_include_glob_keeps_in_can_still_be_excluded_on_its_own_and_brought_back():
    """An include glob does not lock its pages in: ✗ takes one out with an exact exclude, and ✓
    puts it back without leaving a rule behind."""
    db, service = await build()
    await rule(db, PatternType.EXCLUDE, EVERY_PAGE)
    await rule(db, PatternType.INCLUDE, P_PAGES)
    await service.recompute(await fresh(db))
    globs = {("exclude", EVERY_PAGE, None), ("include", P_PAGES, None)}

    await service.set_excluded(await fresh(db), P3, True)
    excluded = (P3 in await queued(db), (await fresh(db)).excluded_count, await rules_of(db, PatternType.EXCLUDE,
                                                                                          PatternType.INCLUDE))
    await service.set_excluded(await fresh(db), P3, False)

    assert excluded == (False, 1, globs | {("exclude", P3, None)})
    assert (P3 in await queued(db), (await fresh(db)).excluded_count) == (True, 0)
    assert await rules_of(db, PatternType.EXCLUDE, PatternType.INCLUDE) == globs


async def test_excluding_the_dot_segment_spelling_of_a_page_leaves_the_plain_spelling_queued():
    """Dot segments are part of the URL: an exact rule on one spelling matches that URL alone."""
    db = FakeDatabase()
    await db.insert_collection(Collection(collection_id=CID, name=CID, seed_url=f"https://{CID}",
                                          connector=ConnectorType.CRAWLER, max_pages=10,
                                          division=Division.HELIOPHYSICS))
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=u, scraped_title="Otypes", full_text="o")
                                for u in (DOTTED, PLAIN)])
    service = CurationService(db)
    await service.recompute(await fresh(db))

    await service.set_excluded(await fresh(db), DOTTED, True)

    assert list(await queued(db)) == [PLAIN]
    stats = {s["match"]: s["matches"] for s in await service.pattern_stats(await fresh(db))}
    assert stats == {DOTTED: 1}
