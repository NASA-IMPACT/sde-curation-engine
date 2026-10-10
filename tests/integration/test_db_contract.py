"""Contract: tests/support/fake_db.FakeDatabase behaves like sde_curation.db.Database for every method
it implements (the Database methods sde_curation/jobs.py and sde_curation/curation.py call).

Every scenario runs twice, through the `store` fixture: once against the real Database on the
test PostgreSQL and once against the fake. Each run asserts the behaviour the unit tests rely on,
and `same` records what the run observed (returned values and later reads, models compared field
by field, timestamps compared only as "set", unordered SQL results sorted) so the second run of a
scenario must observe exactly what the first one did."""

from __future__ import annotations

import dataclasses
import os
import re
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import psycopg
import pytest
from pydantic import BaseModel

from sde_curation.db import ConflictError, Database
from sde_curation.engine.patterns import glob_to_like
from sde_curation.engine.text import content_hash
from sde_curation.engine.urls import canonical_key
from sde_curation.models import (
    Collection,
    Confidence,
    ConnectorType,
    CuratedUrl,
    CurationStage,
    DeltaKind,
    DeltaUrl,
    Division,
    DocumentType,
    DumpFailure,
    DumpUrl,
    IndexRun,
    JobKind,
    JobRun,
    JobState,
    Pattern,
    PatternType,
    RuleSource,
    Status,
)
from tests.support.fake_db import IMPLEMENTED, FakeDatabase

CID = "example.com"
T0 = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
A, B, C, D = (f"https://example.com/docs/{x}" for x in "abcd")


# ── the two stores ─────────────────────────────────────────────────────


@pytest.fixture(params=["postgres", "fake"])
async def store(request):
    if request.param == "postgres":
        db = await Database(os.environ["DATABASE_URL"]).connect()
        try:
            yield db
        finally:
            await db.close()
    else:
        yield FakeDatabase()


def _norm(v: Any) -> Any:
    if isinstance(v, BaseModel):
        return _norm(v.model_dump())
    if dataclasses.is_dataclass(v) and not isinstance(v, type):
        return _norm(dataclasses.asdict(v))
    if isinstance(v, datetime):
        return "<set>"
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, dict):
        return {k: _norm(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_norm(x) for x in v]
    if isinstance(v, (set, frozenset)):
        return sorted((_norm(x) for x in v), key=repr)
    return v


def unordered(xs: Any) -> list[Any]:
    """A result the SQL returns in no defined order, in a comparable one."""
    return sorted((_norm(x) for x in xs), key=repr)


_SEEN: dict[str, dict[str, list[tuple[str, Any]]]] = {}


class Same:
    """What one run of a scenario observed; done() compares it with the other store's run."""

    def __init__(self, scenario: str, kind: str):
        self.scenario, self.kind, self.seen = scenario, kind, []

    def __call__(self, label: str, value: Any) -> Any:
        self.seen.append((label, _norm(value)))
        return value

    def done(self) -> None:
        runs = _SEEN.setdefault(self.scenario, {})
        runs[self.kind] = self.seen
        other = runs.get("fake" if self.kind == "postgres" else "postgres")
        if other is None:
            return
        for (la, a), (lb, b) in zip(other, self.seen, strict=False):
            assert la == lb, f"the scenario recorded {lb!r} where the other store recorded {la!r}"
            assert b == a, f"{la}: the fake and PostgreSQL disagree"
        assert len(other) == len(self.seen)


@pytest.fixture
def same(request, store) -> Same:
    return Same(request.node.originalname, "fake" if isinstance(store, FakeDatabase) else "postgres")


# ── reading what no implemented method returns ─────────────────────────


def _fake(db) -> bool:
    return isinstance(db, FakeDatabase)


async def history(db, cid: str = CID) -> list[tuple]:
    if _fake(db):
        rows = [r for r in db._status_history if r["collection_id"] == cid]
    else:
        rows = await db.fetch("SELECT * FROM status_history WHERE collection_id=%s ORDER BY id", (cid,))
    return [(r["id"], r["old_status"], r["new_status"], r["note"], r["actor"], r["at"] is not None) for r in rows]


async def audit_rows(db) -> list[tuple]:
    rows = db._audit if _fake(db) else await db.fetch("SELECT * FROM audit_log ORDER BY id")
    return [(r["id"], r["actor"], r["action"], r["collection_id"], r["detail"], r["at"] is not None) for r in rows]


async def page_texts(db, cid: str = CID) -> dict[str, str]:
    if _fake(db):
        return {h: t for (c, h), t in db._page_text.items() if c == cid}
    rows = await db.fetch("SELECT content_hash, full_text FROM page_text WHERE collection_id=%s", (cid,))
    return {r["content_hash"]: r["full_text"] for r in rows}


async def effects(db, cid: str = CID) -> list[tuple]:
    if _fake(db):
        return sorted(k for k, c in db._effects.items() if c == cid)
    rows = await db.fetch("SELECT pattern_id, url, field FROM pattern_effects WHERE collection_id=%s", (cid,))
    return sorted((r["pattern_id"], r["url"], r["field"]) for r in rows)


async def suggestions(db, cid: str = CID) -> list[dict]:
    rows = (sorted(db._suggestions.values(), key=lambda r: r["id"]) if _fake(db)
            else await db.fetch("SELECT * FROM pattern_suggestions WHERE collection_id=%s ORDER BY id", (cid,)))
    keep = ("id", "type", "match", "value", "rationale", "matches", "state", "source", "decided_by", "accepted_as")
    return [{k: r[k] for k in keep} | {"created_at": r["created_at"] is not None} for r in rows]


# ── builders ───────────────────────────────────────────────────────────


def coll(cid: str = CID, **kw: Any) -> Collection:
    return Collection(collection_id=cid, name="Example", seed_url="https://example.com/",
                      connector=ConnectorType.CRAWLER, max_pages=100, created_by="alice", **kw)


def dump(url: str, text: str | None = None, cid: str = CID, **kw: Any) -> DumpUrl:
    return DumpUrl(collection_id=cid, url=url, full_text=text, scraped_title=kw.pop("title", None), **kw)


def delta(url: str, kind: DeltaKind = DeltaKind.NEW, cid: str = CID, **kw: Any) -> DeltaUrl:
    return DeltaUrl(collection_id=cid, url=url, kind=kind, **kw)


def cur(url: str, cid: str = CID, **kw: Any) -> CuratedUrl:
    return CuratedUrl(collection_id=cid, url=url, **kw)


def pat(type_: PatternType, match: str, value: str | None = None, cid: str = CID, **kw: Any) -> Pattern:
    return Pattern(collection_id=cid, type=type_, match=match, value=value, **kw)


async def seeded(db, cid: str = CID, **kw: Any) -> Collection:
    return await db.insert_collection(coll(cid, created_at=T0, updated_at=T0, **kw))


# ── scenarios ──────────────────────────────────────────────────────────


async def test_collection_row_status_and_flags(store, same):
    assert same("missing", await store.get_collection(CID)) is None
    given = coll(created_at=T0, updated_at=T0, excluded_count=5, curated_rows=9, review_round=True)
    assert await store.insert_collection(given) is given
    c = same("inserted", await store.get_collection(CID))
    # the columns INSERT leaves out take their defaults, whatever the model carried
    assert (c.excluded_count, c.curated_rows, c.review_round, c.created_at) == (None, 0, False, T0)

    calls: list[tuple] = []

    async def hook(*args):
        calls.append(args)

    store.on_status_change = hook
    s = same("scraped", await store.set_status(CID, Status.SCRAPED, note="crawl", actor="sys"))
    assert (s.status, s.curation_stage) == (Status.SCRAPED, None) and s.updated_at > T0
    s = same("curating", await store.set_status(CID, Status.CURATING))
    assert s.curation_stage is CurationStage.EXCLUSIONS
    s = same("curating again", await store.set_status(CID, Status.CURATING, actor="bob"))
    assert s.curation_stage is CurationStage.EXCLUSIONS
    with pytest.raises(ValueError, match="illegal status transition"):
        await store.set_status(CID, Status.LIVE)
    with pytest.raises(KeyError):
        await store.set_status("nope", Status.SCRAPED)

    async def broken(*_a):
        raise RuntimeError("notification down")

    store.on_status_change = broken
    s = same("forced live", await store.set_status(CID, Status.LIVE, note="forced", force=True))
    assert (s.status, s.curation_stage) == (Status.LIVE, None)
    store.on_status_change = None
    same("hook calls", calls)
    assert [(old, new) for _cid, old, new, _n, _a in calls] == [
        (Status.BACKLOG, Status.SCRAPED), (Status.SCRAPED, Status.CURATING), (Status.CURATING, Status.CURATING)]
    assert same("history", await history(store))[-1][1:5] == ("curating", "live", "forced", None)

    await store.set_flag(CID, True, "re-crawl")
    assert same("flag up", await store.get_collection(CID)).recuration_reason == "re-crawl"
    await store.set_flag(CID, True, "")
    c = same("flag up, no reason", await store.get_collection(CID))
    assert (c.needs_recuration, c.recuration_reason) == (True, None)
    await store.set_flag(CID, False, "ignored")
    c = same("flag down", await store.get_collection(CID))
    assert (c.needs_recuration, c.recuration_reason) == (False, None)

    await store.set_review_round(CID, True)
    await store.set_last_scraped(CID, T0, capped=True)
    await store.set_index_key(CID, "example_key", None)
    c = same("after setters", await store.get_collection(CID))
    assert (c.review_round, c.last_scraped_at, c.last_crawl_capped, c.index_key, c.index_name) == (
        True, T0, True, "example_key", None)
    await store.set_last_scraped(CID, T0)
    await store.set_review_round(CID, False)
    await store.set_index_key(CID, "k2", "Name 2")
    c = same("setters again", await store.get_collection(CID))
    assert (c.review_round, c.last_crawl_capped, c.index_key, c.index_name) == (False, False, "k2", "Name 2")
    # writes to a collection that does not exist change nothing and raise nothing
    await store.set_flag("nope", True, "x")
    await store.set_review_round("nope", True)

    await store.audit("system", "index.key", CID, "example_key")
    await store.audit("bob", "login")
    assert same("audit", await audit_rows(store)) == [
        (1, "system", "index.key", CID, "example_key", True), (2, "bob", "login", None, None, True)]
    with pytest.raises(psycopg.errors.UniqueViolation):
        await store.insert_collection(coll())
    same.done()


async def test_replace_dump_reads_and_page_text(store, same):
    await seeded(store)
    await store.replace_deltas(CID, [], [], excluded_count=7)
    await store.set_review_round(CID, True)

    async def crawl():
        yield dump(A, "Alpha", title="A", content_type="text/html", depth=0)
        yield dump(B, "Alpha", title="B", depth=1)  # same text: one page_text blob
        yield dump(C, None, title="C")  # an empty page: no hash
        yield dump(A, "Ignored", title="A again")  # the first spelling of a URL wins
        yield dump(D, "Delta text", content_hash="h-explicit")

    failures = [DumpFailure(collection_id=CID, url="https://example.com/x", reason="http_404", status=404),
                DumpFailure(collection_id=CID, url="https://example.com/x", reason="http_403"),
                DumpFailure(collection_id=CID, url="https://example.com/y", reason="timeout", detail="slow")]
    assert same("n", await store.replace_dump(CID, crawl(), failures)) == 4
    c = await store.get_collection(CID)
    assert (c.dump_count, c.excluded_count, c.review_round) == (4, None, False)
    rows = same("load_dump", unordered(await store.load_dump(CID)))
    assert {r["url"]: r["content_hash"] for r in rows} == {
        A: content_hash("Alpha"), B: content_hash("Alpha"), C: None, D: "h-explicit"}
    assert all(r["full_text"] is None and r["final_url"] is None for r in rows)
    same("load_dump keys", unordered(await store.load_dump(CID, keys=[canonical_key(A), "nothing"])))
    assert same("load_dump no keys", await store.load_dump(CID, keys=[])) == []
    assert sorted(same("dump_urls", unordered(await store.dump_urls(CID)))) == [A, B, C, D]
    same("set_urls dump", unordered(await store.set_urls(CID, "dump")))
    same("hashes", await store.dump_content_hashes(CID))
    assert same("failures", await store.load_dump_failures(CID)) == {
        "https://example.com/x": "http_404", "https://example.com/y": "timeout"}
    assert same("texts", await page_texts(store)) == {content_hash("Alpha"): "Alpha", "h-explicit": "Delta text"}

    # a new crawl: the blob only the old crawl held goes; failures are replaced too
    assert same("n2", await store.replace_dump(CID, [dump(C, None), dump(D, "Delta text", content_hash="h-explicit"),
                                                     dump("https://example.com/e", "Echo")])) == 3
    assert same("texts 2", await page_texts(store)) == {"h-explicit": "Delta text", content_hash("Echo"): "Echo"}
    assert same("failures 2", await store.load_dump_failures(CID)) == {}

    # dedupe_spellings keeps one row per page (the https, shorter spelling)
    n = await store.replace_dump(CID, [dump("http://www.example.com/p/", "P"), dump("https://example.com/p", "P"),
                                       dump("https://example.com/q", "Q")], dedupe_spellings=True)
    assert same("deduped", (n, unordered(await store.dump_urls(CID)))) == (
        2, ["https://example.com/p", "https://example.com/q"])
    assert same("empty", await store.replace_dump(CID, [])) == 0
    assert await store.load_dump(CID) == [] and await page_texts(store) == {}
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        await store.replace_dump("nope", [dump(A, "x", cid="nope")])
    same.done()


async def test_replace_deltas_twice_keeps_ai_columns_and_effects(store, same):
    await seeded(store)
    p1 = (await store.insert_pattern(pat(PatternType.EXCLUDE, "https://example.com/x*"))).id
    p2 = (await store.insert_pattern(pat(PatternType.TITLE, C, "T {title}"))).id
    first = [
        delta(A, title_ai="AI A", title_ai_conf=Confidence.HIGH, ai_model="m1", ai_failures=1, edited_by="ai"),
        delta(B, DeltaKind.MODIFIED, content_changed=True, renamed_from="http://example.com/docs/b"),
    ]
    await store.replace_deltas(CID, first, [(p1, "https://example.com/x1", "excluded"), (p2, A, "title")])
    c = await store.get_collection(CID)
    assert (c.delta_count, c.excluded_count) == (2, None)  # new effects without a count: unknown
    same("first", unordered(await store.load_deltas(CID)))
    same("effects 1", await effects(store))
    assert same("kinds 1", await store.count_deltas_by_kind(CID)) == {
        "total": 2, "new": 1, "modified": 1, "deleted": 0, "content_changed": 1, "renamed": 1}

    second = [
        delta(A, title="Curated A", division=Division.HELIOPHYSICS, title_ai="NOT WRITTEN", ai_model="m2"),
        delta(C, DeltaKind.DELETED, crawl_failure="http_404", title_ai="AI C", ai_model="m2", division_skipped=True),
    ]
    await store.replace_deltas(CID, second, [(p2, C, "title"), (p2, C, "title")], excluded_count=3)
    rows = {d.url: d for d in await store.load_deltas(CID)}
    same("second", unordered(rows.values()))
    # a row that stays a delta keeps its AI columns; a new row takes them; B is gone
    assert (rows[A].title, rows[A].title_ai, rows[A].ai_model, rows[A].ai_failures) == ("Curated A", "AI A", "m1", 1)
    assert (rows[C].title_ai, rows[C].division_skipped, set(rows)) == ("AI C", True, {A, C})
    assert same("effects 2", await effects(store)) == [(p2, C, "title")]
    c = await store.get_collection(CID)
    assert (c.delta_count, c.excluded_count) == (2, 3)
    same("loaded subset", unordered(await store.load_deltas(CID, urls=[A, "https://example.com/none"])))
    assert await store.load_deltas(CID, urls=[]) == []

    await store.replace_deltas(CID, [delta(A)], [])  # no effects at all: nothing is excluded
    assert same("effects 3", await effects(store)) == []
    assert (await store.get_collection(CID)).excluded_count == 0
    await store.replace_deltas(CID, [delta(A), delta(B)], [(p1, A, "excluded")], keep_effects=True, excluded_count=99)
    c = same("keep effects", await store.get_collection(CID))
    assert (c.delta_count, c.excluded_count, await effects(store)) == (2, 0, [])
    await store.replace_deltas(CID, [], [])
    assert same("emptied", (await store.load_deltas(CID), await store.count_deltas_by_kind(CID))) == (
        [], {"total": 0, "new": 0, "modified": 0, "deleted": 0, "content_changed": 0, "renamed": 0})
    assert (await store.get_collection(CID)).delta_count == 0
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        await store.replace_deltas(CID, [delta(A)], [(9999, A, "title")])
    same.done()


async def test_replace_deltas_scoped_and_excluded_counts(store, same):
    await seeded(store)
    await store.replace_dump(CID, [dump(A, "a"), dump(B, "b"), dump(C, "c")])
    ex = (await store.insert_pattern(pat(PatternType.EXCLUDE, "*/docs/*"))).id
    ti = (await store.insert_pattern(pat(PatternType.TITLE, "*", "T"))).id
    await store.replace_deltas(CID, [delta(A, title_ai="keep me"), delta(B), delta(C)],
                               [(ex, A, "excluded"), (ti, B, "title"), (ti, C, "title")])
    assert (await store.get_collection(CID)).excluded_count is None
    await store.replace_deltas_scoped(CID, [A, B], [delta(A, title="new A", title_ai="ignored"), delta(D)],
                                      [(ex, A, "excluded"), (ex, B, "excluded")], excluded_change=2)
    rows = {d.url: d for d in await store.load_deltas(CID)}
    same("scoped", unordered(rows.values()))
    # B (in scope, not given) went; C (out of scope) stayed; D was written though out of scope
    assert set(rows) == {A, C, D} and (rows[A].title, rows[A].title_ai) == ("new A", "keep me")
    assert same("scoped effects", await effects(store)) == sorted(
        [(ex, A, "excluded"), (ex, B, "excluded"), (ti, C, "title")])
    c = await store.get_collection(CID)
    assert (c.delta_count, c.excluded_count) == (3, None)  # an unknown count stays unknown

    await store.replace_deltas(CID, [delta(A), delta(C)], [(ex, A, "excluded"), (ex, B, "excluded")],
                               excluded_count=4)
    await store.replace_deltas_scoped(CID, [B], [], [], excluded_change=-1)
    c = same("stored count moved", await store.get_collection(CID))
    assert (c.delta_count, c.excluded_count) == (2, 3)
    assert same("effects after", await effects(store)) == [(ex, A, "excluded")]
    assert same("among", await store.excluded_among(CID, [A, B, C, "https://example.com/z"])) == 1
    assert same("among none", await store.excluded_among(CID, [])) == 0
    # an include that overrides is the recorded effect: not counted as excluded
    inc = (await store.insert_pattern(pat(PatternType.INCLUDE, C))).id
    await store.replace_deltas_scoped(CID, [C], [delta(C)], [(inc, C, "excluded")], excluded_change=0)
    assert same("among include", await store.excluded_among(CID, [A, C])) == 1
    same.done()


async def test_patterns_ids_conflicts_filters_and_deletes(store, same):
    await seeded(store)
    await seeded(store, "other.org")
    inc = await store.insert_pattern(pat(PatternType.INCLUDE, "https://www.example.com/a/", created_by="bob"))
    with pytest.raises(ConflictError):
        await store.insert_pattern(pat(PatternType.INCLUDE, "https://www.example.com/a/"))
    glob = await store.insert_pattern(pat(PatternType.EXCLUDE, "https://example.com/docs/*", created_by="Carol"))
    rows = [
        pat(PatternType.TITLE, "http://example.com/a", "Title A", source=RuleSource.LLM, created_at=T0),
        pat(PatternType.EXCLUDE, "https://example.com/docs/*"),  # already there
        pat(PatternType.TITLE, "http://example.com/a", "Other"),  # twice in one batch
        pat(PatternType.DIVISION, "*/a_b*", "Earth Science", source=RuleSource.LLM_EDITED),
        pat(PatternType.DOCUMENT_TYPE, "https://example.com/z", "Data", cid="other.org"),
    ]
    assert same("bulk", await store.insert_patterns(rows)) == 3
    assert same("bulk none", await store.insert_patterns([])) == 0
    every = same("all", await store.list_patterns(CID))
    ids = [p.id for p in every]
    # ids in insertion order; a refused insert uses one up, as an identity column does
    assert (inc.id, glob.id, ids) == (1, 3, [1, 3, 4, 7])
    assert every[2].created_at == T0 and every[2].source is RuleSource.LLM
    same("types", await store.list_patterns(CID, types=[PatternType.TITLE, "division"]))
    same("exact", await store.list_patterns(CID, exact=True))
    same("globs", await store.list_patterns(CID, exact=False))
    same("paged", await store.list_patterns(CID, limit=2, offset=1))
    same("rules", await store.load_rules(CID))
    same("rules by key", await store.load_rules(CID, keys=[canonical_key("https://example.com/a")]))
    same("rules no key", await store.load_rules(CID, keys=[]))
    assert same("exact matches", unordered(await store.exact_pattern_matches(CID, ["include", "title"]))) == [
        [1, "include", "https://www.example.com/a/"], [4, "title", "http://example.com/a"]]
    assert sorted(same("exact ids", unordered(await store.exact_rule_ids(
        CID, ["include", "title"], [canonical_key("HTTPS://EXAMPLE.COM/a#frag")])))) == [1, 4]
    assert same("keyed", await store.keyed(CID)) is True

    async def page(**kw):
        found, total = await store.rules_page(CID, **kw)
        return same(f"rules_page {kw}", ([p.id for p in found], total))

    assert await page() == ([3, 7, 1, 4], 4)  # globs first, then oldest first
    assert await page(q="DOCS") == ([3], 1)
    assert await page(q="a_b") == ([7], 1)  # ILIKE: `_` is any one character
    assert await page(q="title") == ([4], 1)  # the value is searched too
    await page(type_="title")
    await page(source="llm_edited")
    await page(scope="url")
    await page(scope="glob")
    await page(sort="match")
    await page(sort="match", desc=True)
    await page(sort="by")
    await page(sort="value", desc=True)
    await page(sort="type", limit=2, offset=1)
    await page(sort="nonsense")

    await store.replace_deltas(CID, [delta(A)], [(1, A, "excluded"), (4, A, "title"), (3, B, "excluded")],
                               excluded_count=5)
    assert same("effect counts", await store.effect_counts(CID)) == {1: 1, 3: 1, 4: 1}
    assert same("effect counts ids", await store.effect_counts(CID, [3, 7])) == {3: 1}
    assert await store.effect_counts(CID, []) == {}
    assert same("del exact title", await store.delete_exact_patterns(CID, "title", ["http://example.com/a", "x"])) == 1
    assert (await store.get_collection(CID)).excluded_count == 5  # a title rule leaves the count alone
    assert await store.delete_exact_patterns(CID, "include", []) == 0
    assert same("del exact include", await store.delete_exact_patterns(CID, PatternType.INCLUDE,
                                                                     ["https://www.example.com/a/"])) == 1
    assert (await store.get_collection(CID)).excluded_count is None  # an include rule went: unknown
    assert same("effects after deletes", await effects(store)) == [(3, B, "excluded")]  # ON DELETE CASCADE
    other = (await store.list_patterns("other.org"))[0].id
    assert same("del other's", await store.delete_pattern(CID, other)) is False
    assert same("del missing", await store.delete_pattern(CID, 999)) is False
    await store.replace_deltas(CID, [], [], excluded_count=2)
    assert same("del one", await store.delete_pattern(CID, 7)) is True
    assert (await store.get_collection(CID)).excluded_count == 2  # a division rule
    assert same("del many", await store.delete_patterns(CID, [3, other, 999])) == 1
    assert (await store.get_collection(CID)).excluded_count is None
    assert await store.delete_patterns(CID, []) == 0
    assert same("left", (await store.list_patterns(CID), await store.list_patterns("other.org")))[0] == []
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        await store.insert_pattern(pat(PatternType.EXCLUDE, "*", cid="nope"))
    same.done()


async def test_rule_match_counts_over_each_set(store, same):
    await seeded(store)
    await store.replace_dump(CID, [dump(A, "a"), dump("https://www.example.com/docs/a/", "a2"),
                                   dump("https://example.com/docs/b_1", "b"), dump("https://example.com/docs/bx1", "x"),
                                   dump("https://example.com/img/50%off", "i")])
    await store.replace_curated(CID, [cur(A), cur("https://example.com/old"), cur("https://example.com/docs/b_1")])
    await store.replace_deltas(CID, [delta(A), delta("https://example.com/old", DeltaKind.DELETED),
                                     delta("https://example.com/docs/bx1")], [])
    globs = [(1, glob_to_like("https://example.com/docs/*")), (2, glob_to_like("*b_1")),
             (3, glob_to_like("*50%off")), (4, glob_to_like("https://EXAMPLE.com/*")), (5, "%")]
    exact = [(10, canonical_key(A)), (11, canonical_key("https://example.com/old")), (12, "nothing/at/all"),
             (13, canonical_key("https://example.com/docs/b_1"))]
    for set_ in ("dump", "delta", "curated"):
        same(f"counts {set_}", await store.rule_match_counts(CID, set_, globs=globs, exact=exact))
    assert await store.rule_match_counts(CID, "dump", globs=globs, exact=[]) == {1: 3, 2: 1, 3: 1, 4: 0, 5: 5}
    assert await store.rule_match_counts(CID, "delta", globs=[], exact=exact) == {10: 1, 11: 1, 12: 0, 13: 0}
    assert await store.rule_match_counts(CID, "curated", globs=[], exact=[]) == {}
    same.done()


async def test_curated_replace_reads_and_flags(store, same):
    await seeded(store)
    await store.replace_dump(CID, [dump(A, "Alpha"), dump(B, "Bravo")])
    rows = [cur(A, title="A", content_hash=content_hash("Alpha"), division=Division.ASTROPHYSICS),
            cur(B, excluded=True, content_hash=content_hash("Bravo")),
            cur(C, full_text="Charlie", document_type=DocumentType.DATA, edited_by="sme")]
    assert same("included", await store.replace_curated(CID, rows)) == 2
    c = await store.get_collection(CID)
    assert (c.curated_count, c.curated_rows) == (2, 3) and c.curated_changed_at is not None
    loaded = same("loaded", unordered(await store.load_curated(CID)))
    assert all(r["full_text"] is None and r["text_len"] is None for r in loaded)
    with_text = {r.url: r.full_text for r in await store.load_curated(CID, with_text=True)}
    assert same("with text", with_text) == {A: "Alpha", B: "Bravo", C: "Charlie"}
    same("by key", unordered(await store.load_curated(CID, keys=[canonical_key("http://example.com/docs/c/")])))
    chunks = [[r.url for r in chunk] async for chunk in store.iter_curated_for_export(CID, chunk=1)]
    assert same("export", chunks) == [[A], [C]]  # URL order, excluded rows left out
    full = [r async for chunk in store.iter_curated_for_export(CID) for r in chunk]
    assert same("export rows", full)[1].full_text == "Charlie"

    await store.set_curated_edited_by(CID, [(A, "sme"), ("https://example.com/none", "ai")])
    await store.set_curated_edited_by(CID, [])
    await store.set_curated_crawl_failure(CID, [(C, "http_403"), (A, None)])
    await store.set_curated_crawl_failure(CID, [])
    assert same("unreachable", await store.count_curated_unreachable(CID)) == 1
    before = (await store.get_collection(CID)).curated_changed_at
    await store.set_curated_excluded(CID, [(B, False), (A, True)])
    await store.set_curated_excluded(CID, [])
    c = same("after excluded", await store.get_collection(CID))
    assert (c.curated_count, c.curated_rows) == (2, 3) and c.curated_changed_at > before
    same("rows after flags", unordered(await store.load_curated(CID)))

    # a new crawl without A: its text stays while a curated row holds its hash
    await store.replace_dump(CID, [dump(B, "Bravo")])
    assert set(same("texts", await page_texts(store)).values()) == {"Alpha", "Bravo", "Charlie"}
    stamp = (await store.get_collection(CID)).curated_changed_at
    assert same("replaced", await store.replace_curated(CID, [cur(B, content_hash=content_hash("Bravo"))],
                                                        changed=False)) == 1
    c = await store.get_collection(CID)
    assert (c.curated_count, c.curated_rows, c.curated_changed_at) == (1, 1, stamp)
    assert set(same("texts after", await page_texts(store)).values()) == {"Bravo"}
    assert same("emptied", await store.replace_curated(CID, [])) == 0
    assert await store.load_curated(CID) == [] and await store.count_curated_unreachable(CID) == 0
    assert [c async for c in store.iter_curated_for_export(CID)] == []
    same.done()


async def test_delete_deltas_and_effects(store, same):
    await seeded(store)
    p = (await store.insert_pattern(pat(PatternType.TITLE, "*", "T"))).id
    await store.replace_deltas(CID, [delta(A), delta(B), delta(C)], [(p, A, "title"), (p, B, "title")])
    assert same("deleted", await store.delete_deltas(CID, [A, A, "https://example.com/none"])) == 1
    assert (await store.get_collection(CID)).delta_count == 2
    assert await store.delete_deltas(CID, []) == 0
    assert same("effects kept", await effects(store)) == [(p, A, "title"), (p, B, "title")]
    await store.delete_effects(CID, [A, "https://example.com/none"])
    await store.delete_effects(CID, [])
    assert same("effects", await effects(store)) == [(p, B, "title")]
    same("left", unordered(await store.load_deltas(CID)))
    same.done()


async def test_llm_queries_and_ai_writes(store, same):
    await seeded(store)
    urls = {x: f"https://example.com/p/{x}" for x in "abcdefghij"}
    u = urls.__getitem__
    await store.replace_dump(CID, [dump(u(x), f"text of {x}", title=f"T{x}") for x in "abcdefghi"])
    full = {"title_ai": "AI", "division_ai": Division.HELIOPHYSICS, "document_type_ai": DocumentType.DATA,
            "title_ai_conf": Confidence.HIGH, "division_ai_conf": Confidence.LOW,
            "document_type_ai_conf": Confidence.MEDIUM}
    await store.replace_deltas(CID, [
        delta(u("a"), scraped_title="Ta"),                                     # never classified
        delta(u("b"), **full),                                                 # answered
        delta(u("c"), **full, ai_error="boom", ai_failures=2),                 # the last call failed
        delta(u("d"), **{**full, "title_ai": None}),                           # dismissed title, none set
        delta(u("e"), **{**full, "division_ai": None, "division_ai_conf": None}, division_skipped=True),
        delta(u("f"), **full, content_changed=True, ai_content_hash="old"),   # text changed since
        delta(u("g"), **full, content_changed=True, ai_content_hash=content_hash("text of g")),
        delta(u("h"), DeltaKind.DELETED),                                      # removals never
        delta(u("i"), excluded=True),                                          # excluded never
        delta(u("j"), **full, content_changed=True, ai_content_hash="x"),     # no dump row: NULL hash
    ], [])
    assert same("count missing", await store.count_deltas_for_llm(CID)) == 5
    assert same("count all", await store.count_deltas_for_llm(CID, only_missing=False)) == 8
    got = [r async for r in store.iter_deltas_for_llm(CID, chunk=2)]
    assert [r["url"] for r in same("iter missing", got)] == [u(x) for x in "acdef"]
    assert got[0] == {"url": u("a"), "title": "Ta", "text": "text of a", "content_hash": content_hash("text of a")}
    every = same("iter all", [r async for r in store.iter_deltas_for_llm(CID, only_missing=False, chunk=3)])
    assert every[-1] == {"url": u("j"), "title": None, "text": None, "content_hash": None}
    assert [x for x, _ in same("pending", await store.pending_urls_for_patterns(CID))] == [
        u(x) for x in "abcdefgj"]
    docs = same("docs", await store.docs_for_llm(CID, [u("j"), u("a"), u("h"), "https://example.com/none"]))
    assert [d["url"] for d in docs] == [u("a"), u("h"), u("j")]
    assert await store.docs_for_llm(CID, []) == []
    assert same("sizes", await store.text_sizes(CID, [u("a"), u("j"), "https://example.com/none"])) == {
        u("a"): 9, u("j"): 0}
    assert await store.text_sizes(CID, []) == {}

    assert same("set ai", await store.set_delta_ai(CID, [
        {"url": u("c"), "title": "New C", "division": Division.EARTH_SCIENCE, "document_type": "Images",
         "title_conf": "high", "division_conf": Confidence.MEDIUM, "model": "m", "content_hash": "h"},
        {"url": u("a"), "title": "New A", "model": "m", "division_skipped": 1},
        {"url": "https://example.com/none", "title": "x"},
    ])) == 3
    assert await store.set_delta_ai(CID, []) == 0
    assert same("errors", await store.set_delta_ai_errors(CID, [(u("b"), "x" * 1500), (u("b"), "again"),
                                                               ("https://example.com/none", "y")])) == 3
    assert await store.set_delta_ai_errors(CID, []) == 0
    assert same("titles", await store.set_delta_ai_titles(CID, [
        {"url": u("d"), "title": "D2", "title_conf": "low", "model": "m2", "before": "Shared"},
        {"url": u("d"), "title": "D3", "before": "Other"},
        {"url": u("e"), "title": "E2", "title_conf": Confidence.HIGH},
    ])) == 3
    assert await store.set_delta_ai_titles(CID, []) == 0
    loaded = await store.load_deltas(CID)
    same("rows", unordered(loaded))
    rows = {d.url: d for d in loaded}
    assert (rows[u("c")].ai_error, rows[u("c")].ai_failures, rows[u("c")].title_ai) == (None, 0, "New C")
    assert (rows[u("a")].division_skipped, rows[u("a")].division_ai) == (True, None)
    assert (rows[u("b")].ai_error, rows[u("b")].ai_failures, rows[u("b")].title_ai) == ("again", 2, "AI")
    assert (rows[u("d")].title_ai, rows[u("d")].title_ai_conf, rows[u("d")].title_ai_before, rows[u("d")].ai_model) \
        == ("D3", None, "Shared", None)
    same("count after", await store.count_deltas_for_llm(CID))
    same.done()


async def test_duplicate_titles_and_incomplete_counts(store, same):
    await seeded(store)
    u = {x: f"https://example.com/t/{x}" for x in "abcdefghiwxyz"}.__getitem__
    data, img = DocumentType.DATA, DocumentType.IMAGES
    es = Division.EARTH_SCIENCE
    await store.replace_curated(CID, [
        cur(u("x"), title="same   title", document_type=data, division=es),   # shares with a and b
        cur(u("z"), title="Same Title", document_type=data),                  # a delta renames it: not counted
        cur(u("y"), scraped_title="Other", document_type=img),
        cur(u("w"), title="Same Title", document_type=data, excluded=True),   # excluded: never counted
        cur(u("a"), title="Old A", document_type=data),                        # a delta row stands in for it
    ])
    await store.replace_deltas(CID, [
        delta(u("a"), title="  Same  Title ", document_type=data, division=es),
        delta(u("b"), scraped_title="same title", document_type=data, division=es),
        delta(u("c"), title="Unique C", title_ai="Same Title", title_ai_before="Before", document_type=data,
              division=Division.GENERAL),
        delta(u("d")),
        delta(u("e"), DeltaKind.DELETED, title="Same Title", document_type=data),
        delta(u("f"), excluded=True, title="Same Title", document_type=data),
        delta(u("g"), renamed_from=u("z"), title="G", document_type=img, division=Division.HELIOPHYSICS),
        delta(u("h"), title="\tTab", scraped_title="Same Title", document_type=data, division=es),
        delta(u("i"), title="   ", scraped_title="Other", document_type=img, division=es),
    ], [])
    keys = same("title keys", await store.title_keys(CID))
    assert keys[u("a")] == keys[u("b")] == keys[u("x")] == keys[u("c")] == "same title\x1fData"
    assert u("z") not in keys and u("w") not in keys and u("d") not in keys and keys[u("h")] == " tab\x1fData"
    assert same("dup counts", await store.duplicate_title_counts(CID)) == {"urls": 6, "titles": 2, "delta_urls": 4}
    groups = same("dup groups", await store.duplicate_title_groups(CID))
    assert [[m["url"] for m in g["members"]] for g in groups] == [[u("i"), u("y")], [u("a"), u("b"), u("c"), u("x")]]
    assert groups[1]["members"][2] == {"url": u("c"), "delta": True, "pending_ai": True, "before": "Before"}
    # promote's view: c's AI title is undecided, so only a and b collide (with x)
    assert same("incomplete", await store.incomplete_counts(CID)) == {
        "urls": 5, "title": 1, "division": 2, "general": 1, "document_type": 1, "duplicate": 3}
    assert same("incomplete some", await store.incomplete_counts(CID, [u("d"), u("a"), "https://example.com/none"])) \
        == {"urls": 2, "title": 1, "division": 1, "general": 0, "document_type": 1, "duplicate": 1}
    assert await store.incomplete_counts(CID, []) == dict.fromkeys(
        ("urls", "title", "division", "general", "document_type", "duplicate"), 0)
    assert same("none", (await store.title_keys("nope"), await store.duplicate_title_counts("nope"),
                         await store.duplicate_title_groups("nope"))) == ({}, {"urls": 0, "titles": 0, "delta_urls": 0},
                                                                            [])
    same.done()


async def test_pattern_suggestions(store, same):
    await seeded(store)
    rows = [{"type": "exclude", "match": "*/tag/*", "rationale": "tags", "matches": 3},
            {"type": PatternType.EXCLUDE, "match": "*/tag/*", "rationale": "again"},
            {"type": "exclude", "match": "*.pdf", "source": "global", "value": None}]
    assert same("added", await store.add_pattern_suggestions(CID, rows)) == 2
    assert await store.add_pattern_suggestions(CID, []) == 0
    assert same("added again", await store.add_pattern_suggestions(CID, rows[:1])) == 0
    assert same("pending", await store.count_pending_pattern_suggestions(CID)) == 2
    assert [r["id"] for r in same("rows", await suggestions(store))] == [1, 3]
    await store.clear_pending_pattern_suggestions(CID)
    assert await store.count_pending_pattern_suggestions(CID) == 0
    assert same("re-added", await store.add_pattern_suggestions(CID, rows[2:])) == 1
    same("rows after", await suggestions(store))
    same.done()


async def test_job_lifecycle(store, same):
    for cid in ("c1", "c2", "c3"):
        await seeded(store, cid)
    j = JobRun(collection_id="c1", kind=JobKind.SCRAPE, started_at=T0, started_by="bob",
               progress={"phase": "crawl", "batches": (1, 2)})
    assert await store.insert_job(j) is j and j.id == 1
    got = same("inserted", await store.get_job(1))
    assert (got.state, got.progress, got.started_at, got.finished_at) == (
        JobState.QUEUED, {"phase": "crawl", "batches": [1, 2]}, T0, None)
    j.state, j.run_id, j.external_ref, j.progress = JobState.RUNNING, "r1", "pid:1", {"done": 3}
    j.started_by = "not written"
    await store.update_job(j)
    got = same("updated", await store.get_job(1))
    assert (got.state, got.run_id, got.external_ref, got.progress, got.started_by) == (
        JobState.RUNNING, "r1", "pid:1", {"done": 3}, "bob")
    assert [x.id for x in same("active", await store.active_jobs())] == [1]
    await store.finish_job(j, JobState.SUCCEEDED)
    got = same("finished", await store.get_job(1))
    assert (got.state, got.error, j.finished_at is not None, got.finished_at is not None) == (
        JobState.SUCCEEDED, None, True, True)
    assert await store.get_job(999) is None

    a = await store.insert_job(JobRun(collection_id="c1", kind=JobKind.LLM_METADATA))
    await store.finish_job(a, JobState.FAILED, error="cancelled by shutdown")
    await store.insert_job(JobRun(collection_id="c1", kind=JobKind.RECOMPUTE, state=JobState.SUCCEEDED))
    c2 = await store.insert_job(JobRun(collection_id="c2", kind=JobKind.INDEX_TEST, state=JobState.RUNNING))
    await store.finish_job(c2, JobState.FAILED, error="cancelled by shutdown")
    await store.insert_job(JobRun(collection_id="c3", kind=JobKind.VALIDATE))
    await store.insert_job(JobRun(collection_id="c3", kind=JobKind.SCRAPE, state=JobState.RUNNING))
    ended = same("ended by shutdown", await store.jobs_ended_by_shutdown())
    assert [(x.id, x.collection_id) for x in ended] == [(c2.id, "c2")]
    assert [x.id for x in same("active 2", await store.active_jobs())] == [5, 6]
    # an update of a job that is not there changes nothing
    await store.update_job(JobRun(id=999, collection_id="c1", kind=JobKind.SCRAPE, state=JobState.FAILED))
    assert await store.get_job(999) is None
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        await store.insert_job(JobRun(collection_id="nope", kind=JobKind.SCRAPE))
    same.done()


async def test_index_runs(store, same):
    await seeded(store)
    r1 = IndexRun(run_id="r1", collection_id=CID, target="test", started_at=T0, status={},
                  validation={"count_matches": True, "title_match_rate": 1.0}, started_by="bob")
    assert await store.insert_index_run(r1) is r1
    assert same("last run id", (await store.get_collection(CID)).last_run_id) == "r1"
    got = same("r1", await store.get_index_run("r1"))
    assert (got.status, got.validation["count_matches"], got.started_at) == (None, True, T0)  # {} is stored as NULL
    await store.insert_index_run(IndexRun(run_id="r2", collection_id=CID, target="prod",
                                          started_at=T0 + timedelta(seconds=1)))
    await store.insert_index_run(IndexRun(run_id="r3", collection_id=CID, target="test", state="succeeded",
                                          started_at=T0 + timedelta(seconds=2)))
    assert same("last", (await store.last_index_run(CID))).run_id == "r3"
    assert same("last prod", await store.last_index_run(CID, "prod")).run_id == "r2"
    assert same("last empty target", await store.last_index_run(CID, "")).run_id == "r3"
    assert same("last none", await store.last_index_run(CID, "other")) is None
    assert await store.last_index_run("nope") is None

    r1.state, r1.exported, r1.status, r1.error = "succeeded", 5, {"pair": (1, 2)}, "late"
    r1.finished_at, r1.validation, r1.validated_by, r1.external_ref = T0, None, "direct", "task/1"
    r1.started_by, r1.target = "not written", "prod"
    await store.update_index_run(r1)
    got = same("r1 updated", await store.get_index_run("r1"))
    assert (got.state, got.exported, got.status, got.validation, got.finished_at, got.started_by, got.target) == (
        "succeeded", 5, {"pair": [1, 2]}, None, T0, "bob", "test")
    await store.update_index_run(IndexRun(run_id="ghost", collection_id=CID, target="test"))
    assert await store.get_index_run("ghost") is None

    await store.insert_index_run(IndexRun(run_id="r4", collection_id=CID, target="test",
                                          started_at=T0 - timedelta(days=1)))
    assert same("closed", await store.close_orphan_index_runs(keep=["r2"])) == 1
    got = same("r4", await store.get_index_run("r4"))
    assert (got.state, got.error, got.finished_at is not None) == ("failed", "engine restarted", True)
    assert (await store.get_index_run("r2")).state == "running"
    assert same("closed none", await store.close_orphan_index_runs(keep=[])) == 1  # r2 now
    with pytest.raises(psycopg.errors.UniqueViolation):
        await store.insert_index_run(IndexRun(run_id="r1", collection_id=CID, target="test"))
    same.done()


async def test_empty_collection_reads_and_no_op_writes(store, same):
    await seeded(store)
    await store.recycle_connections()
    same("reads", (
        await store.load_dump(CID), await store.dump_urls(CID), await store.set_urls(CID, "delta"),
        await store.set_urls(CID, "curated"), await store.load_dump_failures(CID),
        await store.dump_content_hashes(CID), await store.load_deltas(CID), await store.load_curated(CID),
        await store.load_rules(CID), await store.list_patterns(CID), await store.rules_page(CID),
        await store.effect_counts(CID), await store.count_deltas_by_kind(CID), await store.keyed(CID),
        await store.count_deltas_for_llm(CID), await store.pending_urls_for_patterns(CID),
        await store.count_pending_pattern_suggestions(CID), await store.incomplete_counts(CID),
        await store.count_curated_unreachable(CID), await store.exact_pattern_matches(CID, ["title"]),
        await store.exact_rule_ids(CID, ["title"], ["example.com/a"]), await store.active_jobs(),
        await store.jobs_ended_by_shutdown(), await store.close_orphan_index_runs(keep=[]),
    ))
    assert await store.set_delta_ai(CID, [{"url": A, "title": "x"}]) == 1  # the count asked, not the rows hit
    await store.set_curated_excluded(CID, [(A, True)])  # recounts even when no row matched
    c = same("recounted", await store.get_collection(CID))
    assert (c.curated_count, c.curated_rows) == (0, 0) and c.curated_changed_at is not None
    assert await store.keyed(CID) is True
    same.done()


# ── coverage of the fake ───────────────────────────────────────────────


def test_every_implemented_method_is_exercised_here():
    """Each method the fake implements is the set jobs.py and curation.py call (plus the
    insert_collection a test needs to start from), and this file calls every one of them."""
    root = Path(__file__).resolve().parents[2] / "sde_curation"
    used = set(re.findall(r"self\.db\.([a-z_]+)", (root / "jobs.py").read_text() + (root / "curation.py").read_text()))
    assert set(IMPLEMENTED) == used | {"insert_collection"}
    source = Path(__file__).read_text()
    assert [m for m in IMPLEMENTED if not re.search(rf"\.{m}\(", source)] == []
