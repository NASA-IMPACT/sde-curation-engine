"""Suggest patterns and Suggest metadata on the in-memory FakeDatabase with the fake model: what
the jobs save, count and ask again. These replace the API-level flows of the old
tests/integration/test_llm.py, tests/integration/test_review_round.py (exclusions only for delta
URLs), tests/e2e/test_llm.py and tests/e2e/test_llm_shared_limit.py (TEST-STRATEGY-2026-10-09.md, P4)."""

import asyncio

import pytest

import sde_curation.jobs as jobs_mod
from sde_curation.curation import CurationService
from sde_curation.engine.text import content_hash
from sde_curation.llm.base import LLMRetryable
from sde_curation.llm.fake import FakeProvider
from sde_curation.models import (
    Collection,
    ConnectorType,
    CuratedUrl,
    Division,
    DocumentType,
    DumpUrl,
    JobState,
    Pattern,
    PatternType,
)
from sde_curation.web.app import llm_prompts
from tests.support.engine import CID, Calls, collection, finished, make_engine, restart
from tests.support.fake_db import FakeDatabase

# The fake model's usage per call (sde_curation/llm/fake.py): every answer reports these.
FAKE_TOKENS_OUT = 32
FAKE_TOKENS_REASONING = 8
PAGES = 8


def url(i: int) -> str:
    """The URL support.engine.collection gives page `i` (1-based)."""
    return f"https://{CID}/p{i:03d}"


async def _run(db, tmp_path, start, *, engine=None, **settings):
    engine = engine or make_engine(db, tmp_path, **settings)
    job = await start(engine, await db.get_collection(CID))
    done = await finished(db, job.id)
    await engine.shutdown()
    return done


def _metadata(e, c, **kw):
    return e.start_llm_metadata(c, **kw)


class Slow(FakeProvider):
    """The fake model, `delay` seconds per call (a model call takes time; other calls overlap it)."""

    def __init__(self, delay: float):
        super().__init__()
        self.delay = delay

    async def complete(self, **kw):
        await asyncio.sleep(self.delay)
        return await super().complete(**kw)


# ── Suggest patterns ─────────────────────────────────────────────────────────────────────────────


async def test_suggest_patterns_saves_the_global_list_hits_first_counted_over_every_spelling(tmp_path):
    """The global exclude list (sde_curation/data/global_excludes.yaml) is applied before the model:
    its globs that match come first, tagged "global", with match counts over every spelling of the
    URLs (http and https, trailing slash). The model's own globs follow."""
    db = FakeDatabase()
    await db.insert_collection(Collection(collection_id=CID, name=CID, seed_url=f"https://{CID}",
                                          connector=ConnectorType.CRAWLER, max_pages=100,
                                          division=Division.HELIOPHYSICS))
    urls = [f"https://{CID}/login", f"http://{CID}/login/", f"https://{CID}/tag/sun", f"https://{CID}/science/a"]
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=u, scraped_title="x") for u in urls])
    await CurationService(db).recompute(await db.get_collection(CID))

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_patterns(c))

    rows = [r for r in db._suggestions.values() if r["state"] == "pending"]  # no list method on the fake
    global_rows = [(s["match"], s["matches"]) for s in rows if s["source"] == "global"]
    assert done.state is JobState.SUCCEEDED
    assert sorted(global_rows) == [("*/login*", 2), ("*/tag/*", 1)]
    assert (done.progress["global"], done.progress["urls"], done.progress["unique"]) == (2, 4, 3)


async def test_suggest_patterns_shows_the_model_only_the_included_delta_urls(tmp_path):
    """After a promote only what changed is reviewed: the model sees the new pages a re-crawl
    found, not the curated ones and not a new page an exclude rule already keeps out. Match
    counts still run over the whole crawl."""
    db = FakeDatabase()
    await collection(db, titles=["A", "B"])
    promoted = ((1, "A"), (2, "B"))
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=url(i), scraped_title=t, title=t,
                                              division=Division.HELIOPHYSICS, document_type=DocumentType.DOCUMENTATION,
                                              full_text=f"page {i}", content_hash=content_hash(f"page {i}"))
                                   for i, t in promoted])
    new = [f"https://{CID}/new1", f"https://{CID}/new2"]
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(i), scraped_title=t, full_text=f"page {i}")
                                for i, t in promoted]
                          + [DumpUrl(collection_id=CID, url=u, scraped_title="New", full_text=u) for u in new])
    await db.insert_pattern(Pattern(collection_id=CID, type=PatternType.EXCLUDE, match=new[1]))
    await CurationService(db).recompute(await db.get_collection(CID))
    model = FakeProvider()
    engine = make_engine(db, tmp_path)
    engine._llm = model

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_patterns(c), engine=engine)

    asked = " ".join(call["user"] for call in model.calls)
    assert (done.progress["urls"], done.progress["candidates"], done.progress["calls"]) == (4, 1, 1)
    assert new[0] in asked and new[1] not in asked and url(1) not in asked


# ── Suggest metadata: what an answer writes ──────────────────────────────────────────────────────


async def test_suggest_metadata_saves_suggestions_beside_the_values_and_leaves_the_values_alone(tmp_path):
    """The model's answer goes into the *_ai columns with a confidence each, the model's name and
    the hash of the text it read; the page's own title and document type stay unset until the
    curator accepts."""
    db = FakeDatabase()
    await collection(db, titles=["Page 2"])

    await _run(db, tmp_path, _metadata)

    d = (await db.load_deltas(CID))[0]
    assert (d.title_ai, d.title_ai_conf, d.document_type_ai, d.document_type_ai_conf) == (
        "Page 2", "high", DocumentType.DOCUMENTATION, "low")
    assert (d.ai_model, d.ai_content_hash) == ("fake", content_hash("page 1"))
    assert (d.title, d.document_type) == (None, None)


async def test_suggest_metadata_adds_up_the_tokens_of_every_answer(tmp_path):
    """The job shows its token use next to tokens in / out; reasoning is part of out, and no page
    text is written to the prompt cache."""
    db = FakeDatabase()
    await collection(db, titles=[f"Page {i}" for i in range(1, PAGES + 1)])

    done = await _run(db, tmp_path, _metadata)

    p = done.progress
    assert (p["tokens_out"], p["tokens_reasoning"], p["tokens_cache_write"]) == (
        FAKE_TOKENS_OUT * PAGES, FAKE_TOKENS_REASONING * PAGES, 0)
    assert p["tokens_in"] > 0


# ── Suggest metadata: what is asked again ────────────────────────────────────────────────────────


async def test_the_next_suggest_metadata_asks_only_the_page_that_failed_and_clears_its_error(tmp_path, monkeypatch):
    db = FakeDatabase()
    await collection(db, titles=[f"Page {i}" for i in range(1, PAGES + 1)])
    bad, model = url(3), jobs_mod.suggest_metadata_one
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", Calls(model, fail={bad}))
    await _run(db, tmp_path, _metadata)
    retry = Calls(model)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", retry)

    done = await _run(db, tmp_path, _metadata)

    row = next(d for d in await db.load_deltas(CID) if d.url == bad)
    assert retry.asked == [bad]
    assert (done.progress["total"], done.progress["classified"]) == (1, 1)
    assert (row.ai_error, row.ai_failures, row.title_ai) == (None, 0, "Page 3")


async def test_a_page_the_provider_turned_away_once_is_asked_again_in_the_retry_pass(tmp_path, monkeypatch):
    """A 429 is retryable: the page is asked again at the end of the run, not failed."""
    db = FakeDatabase()
    await collection(db, titles=[f"Page {i}" for i in range(1, PAGES + 1)])
    busy_once, asked = {url(3), url(5)}, []
    original = jobs_mod.suggest_metadata_one

    async def model(llm, d, **kw):
        asked.append(d["url"])
        if d["url"] in busy_once and asked.count(d["url"]) == 1:
            raise LLMRetryable("429 too many requests")
        return await original(llm, d, **kw)

    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", model)

    done = await _run(db, tmp_path, _metadata)

    assert (done.state, done.progress["classified"], done.progress["failed"]) == (JobState.SUCCEEDED, PAGES, 0)
    assert sorted(u for u in set(asked) if asked.count(u) == 2) == sorted(busy_once)
    assert all(d.ai_error is None and d.title_ai for d in await db.load_deltas(CID))


async def test_a_page_whose_text_changed_after_its_answer_is_asked_again(tmp_path):
    """An answer records the hash of the text it was given. A re-crawl that changes the text makes
    the page a candidate again; the new answer, read from the new text, makes it done."""
    db = FakeDatabase()
    await collection(db, titles=["A", "B"])
    await _run(db, tmp_path, _metadata)
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=d.url, title=d.title_ai,
                                              division=Division.HELIOPHYSICS, document_type=d.document_type_ai,
                                              full_text=f"page {i}", content_hash=content_hash(f"page {i}"))
                                   for i, d in enumerate(await db.load_deltas(CID), 1)])
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(1), scraped_title="A", full_text="new aurora text"),
                                DumpUrl(collection_id=CID, url=url(2), scraped_title="B", full_text="page 2")])
    await CurationService(db).recompute(await db.get_collection(CID))
    asked_before = await db.count_deltas_for_llm(CID)

    done = await _run(db, tmp_path, _metadata)

    row = next(d for d in await db.load_deltas(CID) if d.url == url(1))
    assert (asked_before, done.progress["classified"]) == (1, 1)
    assert row.ai_content_hash == content_hash("new aurora text")
    assert await db.count_deltas_for_llm(CID) == 0


# ── Suggest metadata: cancel, restart, overlapping saves ─────────────────────────────────────────


async def test_a_cancelled_suggest_metadata_keeps_the_answers_it_saved_and_the_next_run_asks_the_rest(tmp_path, monkeypatch):
    db = FakeDatabase()
    c = await collection(db, titles=[f"Page {i}" for i in range(1, PAGES + 1)])
    answered_first = 3
    calls = Calls(jobs_mod.suggest_metadata_one, hold_after=answered_first)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", calls)
    engine = make_engine(db, tmp_path)
    job = await engine.start_llm_metadata(c)
    await asyncio.wait_for(calls.held.wait(), 5)

    cancelled = await engine.cancel(CID, actor="alice")
    await engine.shutdown()
    kept = {d.url for d in await db.load_deltas(CID) if d.title_ai}
    again = Calls(calls.fn)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", again)
    rest = await _run(db, tmp_path, _metadata)

    assert (cancelled.id, cancelled.state, cancelled.error) == (job.id, JobState.FAILED, "cancelled by alice")
    assert len(kept) == answered_first
    assert sorted(again.asked) == sorted({url(i) for i in range(1, PAGES + 1)} - kept)
    assert rest.progress["classified"] == PAGES - answered_first


async def test_a_suggest_metadata_resumed_after_a_restart_is_the_same_job_and_asks_only_unanswered_pages(tmp_path, monkeypatch):
    """#35: a deploy restarts the engine under a long Suggest metadata. The next engine carries on
    as the SAME job with the pages still without an answer; only the call in flight when the
    engine went down is asked again, and the counters cover the whole run."""
    db = FakeDatabase()
    c = await collection(db, titles=[f"Page {i}" for i in range(1, PAGES + 1)])
    before = Calls(jobs_mod.suggest_metadata_one, hold_after=3)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", before)
    first = make_engine(db, tmp_path)
    job = await first.start_llm_metadata(c)
    await asyncio.wait_for(before.held.wait(), 5)

    after = Calls(before.fn)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", after)
    second = await restart(first, db, tmp_path)
    done = await finished(db, job.id)
    await second.shutdown()

    p = done.progress
    assert (done.state, p["restarts"]) == (JobState.SUCCEEDED, 1)
    assert (p["total"], p["done"], p["classified"]) == (PAGES, PAGES, PAGES)
    assert set(before.asked) & set(after.asked) == {before.asked[-1]}  # the call in flight at the restart
    assert all(d.title_ai for d in await db.load_deltas(CID))


async def test_a_suggest_metadata_the_curator_cancelled_is_not_resumed_by_the_next_engine(tmp_path, monkeypatch):
    db = FakeDatabase()
    c = await collection(db, titles=["A", "B", "C"])
    calls = Calls(jobs_mod.suggest_metadata_one, hold_after=1)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", calls)
    first = make_engine(db, tmp_path)
    job = await first.start_llm_metadata(c)
    await asyncio.wait_for(calls.held.wait(), 5)
    await first.cancel(CID, actor="alice")

    second = await restart(first, db, tmp_path)
    await asyncio.sleep(0.05)
    after = await db.get_job(job.id)
    await second.shutdown()

    assert (after.state, after.error) == (JobState.FAILED, "cancelled by alice")
    assert second.active_for(CID) is None


async def test_the_classified_count_equals_the_answers_written_when_saves_overlap(tmp_path, monkeypatch):
    """Every answer is saved on its own (AI_FLUSH_ROWS=1) by 8 workers, and a save waits on the
    database: the written count must still add up (a `written += await save()` once lost most)."""
    db = FakeDatabase()
    await collection(db, titles=[f"Page {i}" for i in range(1, 4 * PAGES + 1)])
    monkeypatch.setattr(jobs_mod, "AI_FLUSH_ROWS", 1)
    save = db.set_delta_ai

    async def a_save_that_waits_on_the_database(cid, rows):
        await asyncio.sleep(0.005)
        return await save(cid, rows)

    db.set_delta_ai = a_save_that_waits_on_the_database
    engine = make_engine(db, tmp_path, llm_workers=8)
    engine._llm = Slow(0.002)

    done = await _run(db, tmp_path, _metadata, engine=engine)

    assert done.progress["classified"] == done.progress["done"] == 4 * PAGES
    assert sum(1 for d in await db.load_deltas(CID) if d.title_ai) == 4 * PAGES


# ── the shared limit on model calls (#24) ────────────────────────────────────────────────────────

PER_JOB_LIMIT = 4  # llm_workers
SHARED_LIMIT = 6  # llm_workers_total: under three jobs' 3 × 4


class InFlight:
    """Wraps the model call: the most calls out at once, across every job of the engine."""

    def __init__(self, fn):
        self.fn, self.now, self.peak = fn, 0, 0

    async def __call__(self, *args, **kwargs):
        self.now += 1
        self.peak = max(self.peak, self.now)
        try:
            await asyncio.sleep(0.01)
            return await self.fn(*args, **kwargs)
        finally:
            self.now -= 1


@pytest.mark.parametrize(("collections", "peak"), [(3, SHARED_LIMIT), (1, PER_JOB_LIMIT)],
                         ids=["three jobs share the engine's limit", "one job alone runs its own limit"])
async def test_metadata_jobs_share_one_limit_on_model_calls_in_flight(tmp_path, monkeypatch, collections, peak):
    db = FakeDatabase()
    cids = [f"c{i}.org" for i in range(collections)]
    for cid in cids:
        await db.insert_collection(Collection(collection_id=cid, name=cid, seed_url=f"https://{cid}",
                                              connector=ConnectorType.CRAWLER, max_pages=100,
                                              division=Division.HELIOPHYSICS))
        await db.replace_dump(cid, [DumpUrl(collection_id=cid, url=f"https://{cid}/p{i}", scraped_title=f"P{i}",
                                            full_text=f"page {i}") for i in range(2 * PAGES)])
        await CurationService(db).recompute(await db.get_collection(cid))
    probe = InFlight(jobs_mod.suggest_metadata_one)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", probe)
    engine = make_engine(db, tmp_path, llm_workers=PER_JOB_LIMIT, llm_workers_total=SHARED_LIMIT)

    jobs = [await engine.start_llm_metadata(await db.get_collection(cid)) for cid in cids]
    done = [await finished(db, j.id) for j in jobs]
    await engine.shutdown()

    assert [j.progress["done"] for j in done] == [2 * PAGES] * collections
    assert probe.peak == peak


# ── the prompts the curator can read ─────────────────────────────────────────────────────────────


def test_the_prompts_shown_to_curators_ask_for_exclusions_and_confidences_over_the_full_text():
    """/api/llm/prompts and "Show the prompt" show what the jobs send."""
    from sde_curation.config import Settings

    prompts = llm_prompts(Settings(llm_provider="fake"))

    assert "exclude" in prompts["patterns"]["system"]
    assert "confidence" in prompts["metadata"]["system"]
    assert "FULL page text; cut from the end only when" in prompts["metadata"]["user"]

