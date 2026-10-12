"""A title glob typed by hand after the AI titles were accepted (CurationService on the in-memory
FakeDatabase): it is the newest rule, so it decides every page it matches, the accepted AI titles
it beats decide nothing (the Rules tab reads them superseded), and a page with an SME title and AI
division and type is "mixed". Replaces the old tests/integration/test_promote_selection.py
test_hand_typed_rule_takes_effect_over_accepted_ai_titles (P4, TEST-STRATEGY-2026-10-09.md)."""

from sde_curation.models import DeltaKind, Division, DocumentType, EditedBy, PatternType, RuleSource
from tests.unit.test_curation import URLS
from tests.unit.test_curation_service import build, fresh, rule

HAND_TITLE = "Hand: {title}"
EVERY_PAGE = "https://example.org/p*"


async def test_a_hand_typed_title_glob_beats_the_accepted_ai_titles_and_reopens_every_page():
    db, service = await build(division=Division.GENERAL, doc_rule=False)
    for u in URLS:  # accept-all: one exact AI rule per page and field
        await rule(db, PatternType.TITLE, u, f"AI {u[-2:]}", RuleSource.LLM)
        await rule(db, PatternType.DIVISION, u, Division.EARTH_SCIENCE, RuleSource.LLM)
        await rule(db, PatternType.DOCUMENT_TYPE, u, DocumentType.DATA, RuleSource.LLM)
    await service.recompute(await fresh(db))
    await service.promote(await fresh(db))

    hand = await rule(db, PatternType.TITLE, EVERY_PAGE, HAND_TITLE)
    await service.recompute(await fresh(db))

    rows = {d.url: d for d in await db.load_deltas(hand.collection_id)}
    assert {u: (d.kind, d.title, d.edited_by) for u, d in rows.items()} == {
        u: (DeltaKind.MODIFIED, f"Hand: Page {i}", EditedBy.MIXED) for i, u in enumerate(URLS, 1)}
    stats = {s["id"]: s["in_effect"] for s in await service.pattern_stats(await fresh(db))}
    ai_titles = [p.id for p in await db.list_patterns(hand.collection_id, types=[PatternType.TITLE])
                 if p.source is RuleSource.LLM]
    assert (stats[hand.id], {stats[i] for i in ai_titles}) == (len(URLS), {0})
