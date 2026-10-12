"""Who edited a page: a rule of the review round (2026-09-10) that the old
tests/integration/test_review_round.py checked through the API (TEST-STRATEGY-2026-10-09.md, P4)."""

import pytest

from sde_curation.curation import CurationService
from sde_curation.models import (
    Collection,
    ConnectorType,
    CuratedUrl,
    DeltaKind,
    Division,
    DocumentType,
    DumpFailure,
    DumpUrl,
    EditedBy,
    RuleSource,
    edited_by_of,
)
from tests.support.fake_db import FakeDatabase

A = "a.org"


@pytest.mark.parametrize(("sources", "edited_by"), [
    ([], None),
    ([RuleSource.LLM], EditedBy.AI),
    ([RuleSource.GLOBAL], EditedBy.AI),  # the global exclude list is the AI's suggestion
    ([RuleSource.SME], EditedBy.SME),
    ([RuleSource.LLM_EDITED], EditedBy.SME),  # an AI value the curator changed is the curator's
    ([RuleSource.LLM, RuleSource.LLM, RuleSource.SME], EditedBy.MIXED),
], ids=["no rule", "AI", "global list", "SME", "AI edited", "AI and SME"])
def test_who_edited_a_page_follows_the_sources_of_the_rules_that_set_it(sources, edited_by):
    assert edited_by_of([str(s) for s in sources]) is edited_by


async def _crawled(db: FakeDatabase, cid: str, pages: int = 3) -> Collection:
    await db.insert_collection(Collection(collection_id=cid, name=cid, seed_url=f"https://{cid}",
                                          connector=ConnectorType.CRAWLER, max_pages=100,
                                          division=Division.HELIOPHYSICS))
    await db.replace_dump(cid, [DumpUrl(collection_id=cid, url=f"https://{cid}/p{i}", scraped_title=f"Page {i}",
                                        full_text=f"page {i}") for i in range(1, pages + 1)])
    return await db.get_collection(cid)


async def test_a_page_a_recrawl_proves_gone_is_queued_for_removal_with_who_edited_it():
    """The tombstone of a curated page keeps its "edited by", so the review shows whose page goes."""
    db = FakeDatabase()
    c = await _crawled(db, A)
    gone = f"https://{A}/p2"
    await db.replace_curated(A, [CuratedUrl(collection_id=A, url=f"https://{A}/p{i}", scraped_title=f"Page {i}",
                                            title=f"Page {i}", division=Division.HELIOPHYSICS,
                                            document_type=DocumentType.DOCUMENTATION, full_text=f"page {i}",
                                            edited_by=EditedBy.AI) for i in (1, 2, 3)])
    await db.replace_dump(A, [DumpUrl(collection_id=A, url=f"https://{A}/p{i}", scraped_title=f"Page {i}",
                                      full_text=f"page {i}") for i in (1, 3)],
                          [DumpFailure(collection_id=A, url=gone, reason="http_404")])

    await CurationService(db).recompute(c)

    assert [(d.url, d.kind, d.edited_by) for d in await db.load_deltas(A)] == [(gone, DeltaKind.DELETED, EditedBy.AI)]

