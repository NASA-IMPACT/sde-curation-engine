"""CurationService (sde_curation/curation.py) on the in-memory FakeDatabase: what each curator action
leaves in the delta set, the rules, the curated set and the collection row. The known-bug tests
(H1, L4, L7) live in tests/unit/test_curation.py; this file reuses its helpers."""

from datetime import UTC, datetime

import pytest

from sde_curation.curation import CurationService, IncompleteMetadata
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
    Pattern,
    PatternCreate,
    PatternType,
    RuleSource,
    Status,
)
from tests.support.fake_db import FakeDatabase
from tests.unit.test_curation import CID, PAGES, URLS, recorded

EXCLUDE, INCLUDE = PatternType.EXCLUDE, PatternType.INCLUDE
TITLE, DIVISION, DOC_TYPE = PatternType.TITLE, PatternType.DIVISION, PatternType.DOCUMENT_TYPE
ALL_PAGES = f"https://{CID}/*"  # a glob over every page of the crawl
P1_GLOB = f"https://{CID}/p1*"  # among p1..p8, a glob matching p1 only
P3_GLOB = f"https://{CID}/p3*"  # among p1..p8, a glob matching p3 only
P3 = URLS[2]
P3_OTHER_SPELLING = f"http://www.{CID}/p3/"  # the same page as P3 by canonical key
NOT_CRAWLED = f"https://{CID}/missing"  # a page the crawl never returned
DOC = DocumentType.DOCUMENTATION


# ── builders ─────────────────────────────────────────────────────────────


async def build(pages: int = PAGES, *, division: Division = Division.HELIOPHYSICS, titled: bool = True,
                doc_rule: bool = True) -> tuple[FakeDatabase, CurationService]:
    """A crawled collection of `pages` pages (p1..pN), nobody has pressed Start curating yet.
    `titled`: the crawl found a title on each page; `doc_rule`: a glob rule gives every page a
    document type, so with the collection's division every page is complete."""
    db = FakeDatabase()
    await db.insert_collection(Collection(collection_id=CID, name=CID, seed_url=f"https://{CID}",
                                          connector=ConnectorType.CRAWLER, max_pages=100, division=division))
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=u, scraped_title=f"Page {i}" if titled else None,
                                        full_text=u) for i, u in enumerate(URLS[:pages], 1)])
    await db.replace_deltas(CID, [], [])
    if doc_rule:
        await rule(db, DOC_TYPE, ALL_PAGES, DOC)
    return db, CurationService(db)


async def rule(db: FakeDatabase, type_: PatternType, match: str, value: str | None = None,
               source: RuleSource = RuleSource.SME) -> Pattern:
    return await db.insert_pattern(Pattern(collection_id=CID, type=type_, match=match, value=value, source=source))


async def fresh(db: FakeDatabase) -> Collection:
    return await db.get_collection(CID)


async def started(**kw) -> tuple[FakeDatabase, CurationService]:
    """build(), then Start curating (a full recompute)."""
    db, service = await build(**kw)
    await service.recompute(await fresh(db))
    return db, service


async def curated(**kw) -> tuple[FakeDatabase, CurationService]:
    """started(), then the whole queue promoted: every page is curated and nothing is queued."""
    db, service = await started(**kw)
    await service.promote(await fresh(db))
    return db, service


def unkey(db: FakeDatabase, url: str = URLS[-1]) -> None:
    """Make one dump row look like a row stored before schema V18, without its canonical key: the
    collection is then not `keyed`, and the service must take its unkeyed paths."""
    db._dump[CID][url]["canonical_key"] = None


async def queued(db: FakeDatabase) -> dict[str, DeltaKind]:
    return {d.url: d.kind for d in await db.load_deltas(CID)}


async def rules_of(db: FakeDatabase, *types: PatternType) -> set[tuple[str, str, str | None]]:
    return {(str(p.type), p.match, p.value) for p in await db.list_patterns(CID, types=list(types) or None)}


async def snapshot(db: FakeDatabase) -> dict:
    """Everything a recompute writes, with rules named by (type, match, value) instead of their
    ids, so two databases built the same way compare equal."""
    c = await fresh(db)
    effects = await db.effect_counts(CID)
    return {
        "deltas": sorted((d.model_dump() for d in await db.load_deltas(CID)), key=lambda d: d["url"]),
        "curated": sorted((x.model_dump() for x in await db.load_curated(CID)), key=lambda x: x["url"]),
        "in_effect": {(str(p.type), p.match, p.value): effects.get(p.id, 0) for p in await db.list_patterns(CID)},
        "counts": (c.delta_count, c.excluded_count, c.curated_count),
    }


# ── recompute ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("pages", [0, 1, PAGES], ids=["empty collection", "one page", "eight pages"])
async def test_start_curating_queues_every_crawled_page_as_new(pages):
    db, service = await build(pages)

    ds = await service.recompute(await fresh(db))

    assert await queued(db) == dict.fromkeys(URLS[:pages], DeltaKind.NEW)
    assert ds.counts["new"] == pages
    assert ((await fresh(db)).delta_count, (await fresh(db)).excluded_count) == (pages, 0)


@pytest.mark.parametrize(("review_all", "expected"), [(False, {}), (True, dict.fromkeys(URLS, DeltaKind.MODIFIED))],
                         ids=["plain recompute", "re-curate everything"])
async def test_a_recompute_after_promote_queues_unchanged_pages_only_when_reviewing_all(review_all, expected):
    db, service = await curated()

    await service.recompute(await fresh(db), review_all=review_all)

    assert await queued(db) == expected


async def test_an_open_review_round_keeps_its_queue_but_not_the_pages_promoted_out_of_it():
    """Re-curate everything queues unchanged pages; a later recompute (any rule change) must not
    drop them from the round, nor bring back the pages already promoted out of it."""
    db, service = await curated()
    await db.set_review_round(CID, True)
    await service.recompute(await fresh(db), review_all=True)
    await service.promote_urls(await fresh(db), URLS[:2])

    await service.recompute(await fresh(db))

    assert await queued(db) == dict.fromkeys(URLS[2:], DeltaKind.MODIFIED)


async def _recrawl_without_p2(db, reason: str | None = None, *, capped: bool = False) -> None:
    failures = [DumpFailure(collection_id=CID, url=URLS[1], reason=reason)] if reason else []
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=u, scraped_title=f"Page {i}", full_text=u)
                                for i, u in enumerate(URLS, 1) if u != URLS[1]], failures)
    if capped:
        await db.set_last_scraped(CID, datetime(2026, 10, 9, tzinfo=UTC), capped=True)


@pytest.mark.parametrize(("change", "p2_delta", "p2_crawl_failure", "p2_excluded"), [
    (lambda db: _recrawl_without_p2(db, "http_403"), None, "http_403", False),
    (lambda db: _recrawl_without_p2(db, capped=True), None, "not_visited", False),
    (lambda db: _recrawl_without_p2(db, "http_404"), DeltaKind.DELETED, None, False),
    (lambda db: rule(db, EXCLUDE, URLS[1]), None, None, True),
], ids=["unreachable: kept and flagged", "capped crawl: kept as not visited", "gone: tombstone",
        "newly excluded: flagged in place"])
async def test_a_recompute_flags_curated_rows_in_place_or_queues_a_tombstone(change, p2_delta, p2_crawl_failure,
                                                                             p2_excluded):
    """A curated page the crawl missed stays curated unless the crawl proves it gone; a curated page
    an exclude rule now keeps out is flagged, never queued (excludes are rules, not deltas)."""
    db, service = await curated()
    await change(db)

    await service.recompute(await fresh(db))

    assert await queued(db) == ({URLS[1]: p2_delta} if p2_delta else {})
    p2 = next(x for x in await db.load_curated(CID) if x.url == URLS[1])
    assert (p2.crawl_failure, p2.excluded) == (p2_crawl_failure, p2_excluded)
    assert (await fresh(db)).curated_count == PAGES - p2_excluded


async def test_a_recompute_writes_back_who_edited_an_unchanged_curated_page():
    """A curated row promoted before its "edited by" was known takes it from the rules that decide
    it now, without being queued (the collection's division counts as the SME's)."""
    db, service = await build()
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=u, scraped_title=f"Page {i}", title=f"Page {i}",
                                              division=Division.HELIOPHYSICS, document_type=DOC)
                                   for i, u in enumerate(URLS, 1)])

    await service.recompute(await fresh(db))

    assert await queued(db) == {}
    assert {x.edited_by for x in await db.load_curated(CID)} == {EditedBy.SME}


# ── which recompute a per-URL change takes ───────────────────────────────


async def _edit_title(service, c, match=P3):
    await service.replace_exact_pattern(c, PatternCreate(type=TITLE, match=match, value="By hand"), old_id=None)


async def _recrawl(db):
    """A re-crawl forgets how many pages the rules keep out (excluded_count back to unknown)."""
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=u, scraped_title=f"Page {i}", full_text=u)
                                for i, u in enumerate(URLS, 1)])


async def _noop(db):
    pass


async def _unkey(db):
    unkey(db)


@pytest.mark.parametrize(("before", "action", "expected"), [
    (_noop, lambda s, c: _edit_title(s, c), ["scoped"]),
    (_noop, lambda s, c: s.replace_exact_patterns(c, [PatternCreate(type=TITLE, match=P3, value="T"),
                                                      PatternCreate(type=DIVISION, match=P3, value="Astrophysics")]),
     ["scoped"]),
    (_noop, lambda s, c: s.recompute_page(c, P3), ["scoped"]),
    (_noop, lambda s, c: _edit_title(s, c, NOT_CRAWLED), ["scoped"]),
    (_recrawl, lambda s, c: _edit_title(s, c), ["scoped-refused", "full"]),
    (_unkey, lambda s, c: _edit_title(s, c), ["scoped-refused", "full"]),
    (_unkey, lambda s, c: s.recompute_page(c, P3), ["scoped-refused", "full"]),
    (_noop, lambda s, c: _edit_title(s, c, P3_GLOB), ["full"]),
    (_noop, lambda s, c: s.replace_exact_patterns(c, [PatternCreate(type=TITLE, match=u, value="T") for u in URLS[:2]]),
     ["full"]),
], ids=["one page's field", "one page's fields in bulk", "no-op edit of one page", "an exact rule for a page not crawled",
        "excluded count unknown after a re-crawl", "rows without a stored key", "no-op edit, rows without a stored key",
        "a glob rule", "two pages in bulk"])
async def test_a_per_url_change_recomputes_one_page_only_when_the_collection_allows_it(before, action, expected):
    """The scoped recompute needs a known excluded count and a canonical key on every row; it
    applies only to changes that touch one page. Whichever runs, a full recompute after it must
    find nothing left to change."""
    db, service = await started()
    await before(db)
    calls = recorded(service)

    await action(service, await fresh(db))

    assert calls == expected
    after = await snapshot(db)
    await CurationService(db).recompute(await fresh(db))
    assert await snapshot(db) == after


async def _nothing(db, service):
    pass


async def _round_open(db, service):
    """A review round over a curated collection, as Re-curate everything leaves it."""
    await service.promote(await fresh(db))
    await db.set_review_round(CID, True)
    await service.recompute(await fresh(db), review_all=True)


async def _p3_glob_excluded(db, service):
    await rule(db, EXCLUDE, P3_GLOB)
    await service.recompute(await fresh(db))


@pytest.mark.parametrize(("arrange", "edit"), [
    (_nothing, lambda s, c: _edit_title(s, c)),
    (_nothing, lambda s, c: s.set_excluded(c, P3, True)),
    (_p3_glob_excluded, lambda s, c: s.set_excluded(c, P3, False)),
    (_nothing, lambda s, c: s.replace_exact_patterns(c, [
        PatternCreate(type=TITLE, match=P3, value="T"), PatternCreate(type=DIVISION, match=P3, value="Astrophysics"),
        PatternCreate(type=DOC_TYPE, match=P3, value="Data")])),
    (_round_open, lambda s, c: _edit_title(s, c)),
    (_round_open, lambda s, c: s.set_excluded(c, P3, True)),
], ids=["title of a new page", "exclude a page", "include a page a glob excludes", "every field of a page",
        "title of a page in a review round", "exclude a curated page in a review round"])
async def test_the_scoped_recompute_leaves_what_a_full_recompute_of_the_same_rules_leaves(arrange, edit):
    """The scoped recompute is a shortcut, so its result must equal a full recompute: the same
    collection built twice, the change made through the service on one, the same final rules
    written straight into the other and recomputed in full."""
    db, service = await started()
    await arrange(db, service)
    twin, twin_service = await started()
    await arrange(twin, twin_service)
    calls = recorded(service)

    await edit(service, await fresh(db))

    assert calls == ["scoped"]
    final = {(p.type, p.match, p.value, p.source) for p in await db.list_patterns(CID)}
    for p in await twin.list_patterns(CID):
        if (p.type, p.match, p.value, p.source) not in final:
            await twin.delete_pattern(CID, p.id)
    have = {(p.type, p.match, p.value, p.source) for p in await twin.list_patterns(CID)}
    for t, m, v, src in sorted(final - have):
        await rule(twin, t, m, v, src)
    await twin_service.recompute(await fresh(twin))
    assert await snapshot(db) == await snapshot(twin)


# ── adding and replacing rules ───────────────────────────────────────────


async def test_a_bulk_accept_skips_rules_that_already_exist_and_recomputes_once():
    db, service = await started()
    await rule(db, EXCLUDE, P1_GLOB, source=RuleSource.LLM)
    calls = recorded(service)

    n, ds = await service.add_patterns(await fresh(db), [
        (PatternCreate(type=EXCLUDE, match=P1_GLOB), RuleSource.SME),
        (PatternCreate(type=EXCLUDE, match=P3_GLOB), RuleSource.LLM_EDITED),
    ])

    assert (n, calls) == (1, ["full"])
    assert {(p.match, p.source) for p in await db.list_patterns(CID, types=[EXCLUDE])} == {
        (P1_GLOB, RuleSource.LLM), (P3_GLOB, RuleSource.LLM_EDITED)}
    assert sorted(await queued(db)) == sorted(set(URLS) - {URLS[0], P3})
    assert ds.excluded == 2


@pytest.mark.parametrize("bulk", [False, True], ids=["one edit", "bulk edit"])
@pytest.mark.parametrize("keyed", [True, False], ids=["keyed", "rows without a stored key"])
async def test_a_per_url_edit_replaces_the_rule_for_that_field_under_every_spelling_of_the_page(bulk, keyed):
    """An exact rule matches its page by canonical key, so a rule left under another spelling would
    still compete with the new one: one page keeps one per-URL rule per field."""
    db, service = await started()
    old = await rule(db, TITLE, P3, "Old, same spelling")
    await rule(db, TITLE, P3_OTHER_SPELLING, "Old, other spelling")
    await rule(db, DIVISION, P3_OTHER_SPELLING, Division.ASTROPHYSICS)
    if not keyed:
        unkey(db)
    body = PatternCreate(type=TITLE, match=P3, value="New")

    if bulk:
        await service.replace_exact_patterns(await fresh(db), [body])
    else:
        await service.replace_exact_pattern(await fresh(db), body, old_id=old.id)

    assert await rules_of(db, TITLE, DIVISION) == {("title", P3, "New"),
                                                   ("division", P3_OTHER_SPELLING, "Astrophysics")}
    assert next(d.title for d in await db.load_deltas(CID) if d.url == P3) == "New"


async def test_an_exact_rule_for_a_page_not_crawled_is_kept_but_queues_nothing():
    db, service = await started()

    await _edit_title(service, await fresh(db), NOT_CRAWLED)

    assert ("title", NOT_CRAWLED, "By hand") in await rules_of(db, TITLE)
    assert sorted(await queued(db)) == sorted(URLS)
    stats = {s["match"]: (s["matches"], s["in_effect"]) for s in await service.pattern_stats(await fresh(db))}
    assert stats[NOT_CRAWLED] == (0, 0)


# ── the ✗ / ✓ toggle ─────────────────────────────────────────────────────


@pytest.mark.parametrize(("existing", "excluded", "rules_after", "p3_queued"), [
    ([], True, {("exclude", P3)}, False),
    ([(EXCLUDE, P3)], True, {("exclude", P3)}, False),
    ([(EXCLUDE, P3_GLOB)], True, {("exclude", P3_GLOB)}, False),
    ([], False, set(), True),
    ([(EXCLUDE, P3)], False, set(), True),
    ([(EXCLUDE, P3_OTHER_SPELLING)], False, set(), True),
    ([(EXCLUDE, P3_GLOB)], False, {("exclude", P3_GLOB), ("include", P3)}, True),
    ([(EXCLUDE, P3_GLOB), (INCLUDE, P3)], True, {("exclude", P3_GLOB)}, False),
    ([(INCLUDE, P3)], True, {("exclude", P3)}, False),
], ids=["exclude a page no rule touches: exact exclude added",
        "exclude again: still one rule",
        "exclude a page a glob already excludes: no rule added",
        "include a page no rule touches: no rule left behind",
        "include: the stale exact exclude goes",
        "include: the stale exclude under another spelling goes",
        "include a page a glob excludes: exact include added",
        "exclude: the stale include goes and the glob does the rest",
        "exclude: the stale include goes and an exclude is added"])
async def test_the_exclude_toggle_leaves_the_fewest_rules_that_give_the_wanted_state(existing, excluded,
                                                                                    rules_after, p3_queued):
    db, service = await build()
    for t, m in existing:
        await rule(db, t, m)
    await service.recompute(await fresh(db))

    await service.set_excluded(await fresh(db), P3, excluded)

    assert {(t, m) for t, m, _ in await rules_of(db, EXCLUDE, INCLUDE)} == rules_after
    assert (P3 in await queued(db)) is p3_queued
    assert (await fresh(db)).excluded_count == (0 if p3_queued else 1)


# ── deleting a rule ──────────────────────────────────────────────────────


async def test_deleting_an_exclude_rule_queues_its_pages_again():
    db, service = await build()
    p = await rule(db, EXCLUDE, P1_GLOB)
    await service.recompute(await fresh(db))

    ds = await service.delete_pattern(await fresh(db), p.id)

    assert ds.excluded == 0
    assert await queued(db) == dict.fromkeys(URLS, DeltaKind.NEW)


async def test_deleting_a_rule_that_does_not_exist_returns_none_and_changes_nothing():
    db, service = await started()
    before = await snapshot(db)
    calls = recorded(service)

    assert await service.delete_pattern(await fresh(db), 999) is None
    assert (calls, await snapshot(db)) == ([], before)


# ── the Rules tab ────────────────────────────────────────────────────────


async def _rules_only(db, service):
    """The rules are in, nobody has pressed Start curating: no delta, no curated row."""


async def _queued(db, service):
    await service.recompute(await fresh(db))


async def _promoted(db, service):
    await service.recompute(await fresh(db))
    await service.promote(await fresh(db))


@pytest.mark.parametrize(("state", "set_", "doc_matches", "in_effect"), [
    (_rules_only, "dump", PAGES, (0, 0, 0)),
    (_queued, "delta", PAGES - 1, (1, 1, PAGES)),
    (_promoted, "curated", PAGES - 1, (1, 1, PAGES)),
], ids=["before curating: the dump", "while queued: the delta URLs", "after promote: the curated URLs"])
@pytest.mark.parametrize("keyed", [True, False], ids=["counted in SQL", "counted in Python (rows without a key)"])
async def test_rule_stats_count_the_rows_the_rules_table_links_to(state, set_, doc_matches, in_effect, keyed):
    """`matches` counts the URLs of the set the Rules tab shows now (exclude rules: always the dump,
    as excluded pages are in no other set); `in_effect` counts the URLs each rule decides (the
    document type glob decides the excluded page's field too). The SQL
    count (keyed rows) and the Python fallback must agree."""
    db, service = await build(doc_rule=False)
    excl, title, doc = (await rule(db, EXCLUDE, P1_GLOB), await rule(db, TITLE, P3, "By hand"),
                        await rule(db, DOC_TYPE, ALL_PAGES, DOC))
    await state(db, service)
    if not keyed:
        unkey(db)

    stats = {s["id"]: s for s in await service.pattern_stats(await fresh(db))}

    assert [(stats[p.id]["set"], stats[p.id]["matches"]) for p in (excl, title, doc)] == [
        ("dump", 1), (set_, 1), (set_, doc_matches)]
    assert tuple(stats[p.id]["in_effect"] for p in (excl, title, doc)) == in_effect


async def test_rule_stats_page_the_per_url_rules_and_keep_every_glob_rule():
    db, service = await build()
    first, second = await rule(db, TITLE, URLS[0], "One"), await rule(db, TITLE, URLS[1], "Two")
    await service.recompute(await fresh(db))

    page = await service.pattern_stats(await fresh(db), exact_limit=1, exact_offset=1)

    assert [(s["match"], s["matches"], s["in_effect"]) for s in page] == [(ALL_PAGES, PAGES, PAGES), (URLS[1], 1, 1)]
    assert first.id not in {s["id"] for s in page} and second.id in {s["id"] for s in page}


async def test_the_rules_table_page_carries_the_counts_and_the_filtered_total():
    db, service = await build()
    await rule(db, TITLE, URLS[0], "One")
    await service.recompute(await fresh(db))

    rows, total = await service.rules_page(await fresh(db), type_="title")

    assert total == 1
    assert [(s["match"], s["matches"], s["in_effect"], s["set"]) for s in rows] == [(URLS[0], 1, 1, "delta")]


# ── promote ──────────────────────────────────────────────────────────────


async def _blank_title_one_page():
    return await build(1, titled=False)


async def _no_division():
    return await build(division=Division.GENERAL)


async def _general_placeholder():
    db, service = await build()
    await rule(db, DIVISION, URLS[0], Division.GENERAL)
    return db, service


async def _no_document_type():
    return await build(doc_rule=False)


async def _shared_title():
    db, service = await build()
    for u in URLS[:2]:
        await rule(db, TITLE, u, "Same")
    return db, service


async def _blank_and_shared():
    db, service = await build(3, titled=False)
    for u in URLS[:2]:
        await rule(db, TITLE, u, "Same")
    return db, service


ACCEPT = "accept the AI suggestions or set the values by hand first"


@pytest.mark.parametrize(("arrange", "message"), [
    (_blank_title_one_page, f"1 delta URL cannot be promoted yet (1 without a title): {ACCEPT}"),
    (_no_division, f"{PAGES} delta URLs cannot be promoted yet ({PAGES} without a division): {ACCEPT}"),
    (_general_placeholder, ("1 delta URL cannot be promoted yet (1 without a division (of those, 1 still on the"
                            f" General placeholder)): {ACCEPT}")),
    (_no_document_type, f"{PAGES} delta URLs cannot be promoted yet ({PAGES} without a document type): {ACCEPT}"),
    (_shared_title, ("2 delta URLs cannot be promoted yet (2 sharing a title and document type with another page):"
                     " run Regenerate duplicate titles, or give them a title of their own by hand")),
    (_blank_and_shared, ("3 delta URLs cannot be promoted yet (1 without a title, 2 sharing a title and document type"
                         f" with another page): {ACCEPT}; Regenerate duplicate titles gives the shared ones a title of"
                         " their own")),
], ids=["blank title, one page", "no division", "General placeholder", "no document type",
        "duplicate title + document type", "blank and duplicate"])
async def test_promote_refuses_incomplete_metadata_and_moves_nothing(arrange, message):
    db, service = await arrange()
    await service.recompute(await fresh(db))
    before = await snapshot(db)

    with pytest.raises(IncompleteMetadata) as refused:
        await service.promote(await fresh(db))

    assert str(refused.value) == message
    assert await snapshot(db) == before


async def test_promote_moves_the_queue_into_the_curated_set_and_closes_the_round():
    db, service = await started()
    await db.set_flag(CID, True, "re-crawled")
    await db.set_review_round(CID, True)

    n = await service.promote(await fresh(db), actor="sme1")

    c = await fresh(db)
    assert (n, c.curated_count, c.delta_count, await queued(db)) == (PAGES, PAGES, 0, {})
    assert sorted((x.url, x.scraped_title, x.division, x.document_type) for x in await db.load_curated(CID)) == [
        (u, f"Page {i}", Division.HELIOPHYSICS, DOC) for i, u in enumerate(URLS, 1)]
    assert (c.status, c.needs_recuration, c.review_round) == (Status.CURATED, False, False)
    assert list((await db.effect_counts(CID)).values()) == [PAGES]  # the Curated table can still say why


async def test_promote_with_an_empty_queue_marks_curated_without_touching_the_curated_set():
    """The "mark curated" shortcut moves nothing, so the index is as up to date as it was."""
    db, service = await curated()
    changed_at = (await fresh(db)).curated_changed_at

    n = await service.promote(await fresh(db))

    assert (n, (await fresh(db)).curated_changed_at, (await fresh(db)).status) == (PAGES, changed_at, Status.CURATED)


async def test_promoting_picked_pages_checks_only_those_pages():
    db, service = await build(doc_rule=False)
    await rule(db, DOC_TYPE, P3, DOC)
    await service.recompute(await fresh(db))

    n, left = await service.promote_urls(await fresh(db), [P3])

    assert (n, [x.url for x in await db.load_curated(CID)]) == (1, [P3])
    assert sorted(d.url for d in left.deltas) == sorted(set(URLS) - {P3})
    with pytest.raises(IncompleteMetadata, match=r"^1 delta URL cannot be promoted yet \(1 without a document type\)"):
        await service.promote_urls(await fresh(db), [URLS[0]])


async def test_a_page_with_every_field_set_by_per_url_rules_can_be_promoted():
    """Boundary: no collection division, no glob rule, no scraped title; the page's own rules decide all."""
    db, service = await build(division=Division.GENERAL, titled=False, doc_rule=False)
    await service.replace_exact_patterns(await fresh(db), [
        PatternCreate(type=TITLE, match=P3, value="Own title"),
        PatternCreate(type=DIVISION, match=P3, value=Division.EARTH_SCIENCE),
        PatternCreate(type=DOC_TYPE, match=P3, value=DocumentType.DATA)])

    await service.promote_urls(await fresh(db), [P3])

    assert [(x.url, x.title, x.division, x.document_type, x.edited_by) for x in await db.load_curated(CID)] == [
        (P3, "Own title", Division.EARTH_SCIENCE, DocumentType.DATA, EditedBy.SME)]


async def test_promoting_picked_pages_moves_renames_and_drops_tombstones_and_leaves_the_rest_queued():
    """A curated page under an old spelling comes back as one renamed delta; a curated page a
    complete crawl never met is a tombstone. Promoting them moves the row and removes the other;
    the page not picked stays queued, and the review round stays open while anything is left."""
    old_spelling, gone = f"http://www.{CID}/p1/", f"https://{CID}/gone"
    db, service = await build(3)
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=u, scraped_title=t, title=t,
                                              division=Division.HELIOPHYSICS, document_type=DOC)
                                   for u, t in ((old_spelling, "Page 1"), (URLS[1], "Page 2"), (gone, "Gone"))])
    await db.set_review_round(CID, True)
    await service.recompute(await fresh(db))
    assert await queued(db) == {URLS[0]: DeltaKind.MODIFIED, URLS[2]: DeltaKind.NEW, gone: DeltaKind.DELETED}

    n, left = await service.promote_urls(await fresh(db), [URLS[0], gone])

    assert n == 2
    assert sorted(x.url for x in await db.load_curated(CID)) == sorted([URLS[0], URLS[1]])
    assert ([d.url for d in left.deltas], await queued(db)) == ([URLS[2]], {URLS[2]: DeltaKind.NEW})
    assert (await fresh(db)).review_round is True


async def test_promoting_the_last_queued_pages_closes_the_review_round():
    db, service = await started()
    await db.set_flag(CID, True, "re-crawled")
    await db.set_review_round(CID, True)

    _, left = await service.promote_urls(await fresh(db), URLS)

    c = await fresh(db)
    assert (left.deltas, c.delta_count, c.curated_count) == ([], 0, PAGES)
    assert (c.needs_recuration, c.review_round) == (False, False)


async def test_promoting_pages_that_are_not_queued_changes_nothing():
    db, service = await started()

    n, left = await service.promote_urls(await fresh(db), [NOT_CRAWLED])

    assert (n, len(left.deltas), await db.load_curated(CID)) == (0, PAGES, [])

