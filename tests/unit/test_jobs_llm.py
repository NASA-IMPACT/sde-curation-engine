"""The LLM jobs on the in-memory FakeDatabase with the fake model: Suggest patterns, Suggest metadata
and Regenerate titles — what they save, what they refuse, and how they carry on after a restart."""

import asyncio

import pytest

import sde_curation.jobs as jobs_mod
from sde_curation.llm.base import LLMError
from sde_curation.models import Collection, ConnectorType, DumpUrl, JobKind, JobRun, JobState
from tests.support.engine import CID, Calls, collection, finished, make_engine, restart
from tests.support.fake_db import FakeDatabase

BATCH_URLS = 50  # the smallest Suggest-patterns batch Settings allows
BATCHES = 3
PAGES = BATCH_URLS * BATCHES


async def _run(db, tmp_path, start, **settings) -> JobRun:
    engine = make_engine(db, tmp_path, **settings)
    job = await start(engine, await db.get_collection(CID))
    done = await finished(db, job.id)
    await engine.shutdown()
    return done


# ── Suggest patterns ─────────────────────────────────────────────────────────────────────────────


async def test_suggest_patterns_asks_once_per_batch_and_saves_each_batchs_suggestion(tmp_path):
    """The fake model excludes the last URL of every batch it is shown: one suggestion per batch."""
    db = FakeDatabase()
    await collection(db, titles=[f"Page {i}" for i in range(1, PAGES + 1)])

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_patterns(c), llm_pattern_batch_urls=BATCH_URLS)

    assert done.state is JobState.SUCCEEDED
    assert (done.progress["calls"], done.progress["done"], done.progress["failed"]) == (BATCHES, BATCHES, 0)
    assert done.progress["suggestions"] == await db.count_pending_pattern_suggestions(CID) == BATCHES


async def _crawled_not_curated(db):
    await db.insert_collection(Collection(collection_id=CID, name=CID, seed_url=f"https://{CID}",
                                          connector=ConnectorType.CRAWLER, max_pages=10))
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=f"https://{CID}/a", full_text="a")])


async def _empty(db):
    await db.insert_collection(Collection(collection_id=CID, name=CID, seed_url=f"https://{CID}",
                                          connector=ConnectorType.CRAWLER, max_pages=10))


@pytest.mark.parametrize(("arrange", "error"), [
    (_empty, "no crawl dump to sample — scrape first"),
    (_crawled_not_curated, "no delta URLs to look at — Start curating first"),
])
async def test_suggest_patterns_refuses_a_collection_with_nothing_to_look_at(tmp_path, arrange, error):
    db = FakeDatabase()
    await arrange(db)

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_patterns(c))

    assert done.state is JobState.FAILED and done.error.startswith(error)


async def test_a_resumed_suggest_patterns_asks_only_the_batches_not_answered_yet(tmp_path, monkeypatch):
    db = FakeDatabase()
    c = await collection(db, titles=[f"Page {i}" for i in range(1, PAGES + 1)])
    original = jobs_mod.suggest_patterns_batch
    first_asked, second_asked, held = [], [], asyncio.Event()

    async def first_model(llm, c, chunk, *, batch_no, **kw):
        first_asked.append(batch_no)
        if batch_no > 1:  # batch 1 is answered and saved; the engine goes down during batch 2
            held.set()
            await asyncio.Event().wait()
        return await original(llm, c, chunk, batch_no=batch_no, **kw)

    async def second_model(llm, c, chunk, *, batch_no, **kw):
        second_asked.append(batch_no)
        return await original(llm, c, chunk, batch_no=batch_no, **kw)

    monkeypatch.setattr(jobs_mod, "suggest_patterns_batch", first_model)
    first = make_engine(db, tmp_path, llm_pattern_batch_urls=BATCH_URLS)
    job = await first.start_llm_patterns(c)
    await asyncio.wait_for(held.wait(), 10)

    monkeypatch.setattr(jobs_mod, "suggest_patterns_batch", second_model)
    second = await restart(first, db, tmp_path, llm_pattern_batch_urls=BATCH_URLS)
    done = await finished(db, job.id)
    await second.shutdown()

    assert (first_asked, sorted(second_asked)) == ([1, 2], [2, 3])
    assert (done.state, done.progress["done"]) == (JobState.SUCCEEDED, BATCHES)
    assert await db.count_pending_pattern_suggestions(CID) == BATCHES


async def test_a_resumed_suggest_patterns_fails_when_the_delta_urls_changed_meanwhile(tmp_path):
    """Its checkpoint numbers batches; with a different set of URLs those numbers mean other URLs."""
    db = FakeDatabase()
    await collection(db, titles=["A", "B"])
    job = await db.insert_job(JobRun(collection_id=CID, kind=JobKind.LLM_PATTERNS, state=JobState.RUNNING,
                                     progress={"calls": BATCHES, "done_batches": "0"}))
    engine = make_engine(db, tmp_path)

    await engine.recover()
    engine.start_resumes()
    done = await finished(db, job.id)

    assert (done.state, done.error) == (
        JobState.FAILED, "the delta URLs changed while it was interrupted; run Suggest patterns again")


# ── Suggest metadata ─────────────────────────────────────────────────────────────────────────────


async def test_suggest_metadata_on_an_empty_collection_refuses_with_nothing_to_classify(tmp_path):
    db = FakeDatabase()
    await collection(db, titles=[])

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_metadata(c))

    assert done.state is JobState.FAILED and done.error.startswith("no delta URLs to classify")


async def test_a_page_the_model_cannot_read_is_recorded_on_its_row_and_the_job_still_succeeds(tmp_path, monkeypatch):
    db = FakeDatabase()
    await collection(db, titles=["A", "B", "C"])
    bad = f"https://{CID}/p002"
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", Calls(jobs_mod.suggest_metadata_one, fail={bad}))

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_metadata(c))

    errors = {d.url: d.ai_error for d in await db.load_deltas(CID)}
    assert done.state is JobState.SUCCEEDED
    assert (done.progress["done"], done.progress["failed"], done.progress["classified"]) == (2, 1, 2)
    assert errors[bad] == "LLMError: the model could not read this page"
    assert [u for u, e in errors.items() if e] == [bad]


async def test_suggest_metadata_keeps_its_answers_when_telling_duplicate_titles_apart_fails(tmp_path, monkeypatch):
    """LLM_DEDUPE_TITLES: the duplicate pass is extra; its failure is reported, not the job's."""
    db = FakeDatabase()
    await collection(db, titles=["Alpha", "Alpha", "Solo"])

    async def model_down(*a, **k):
        raise LLMError("model unavailable")

    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", model_down)

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_metadata(c), llm_dedupe_titles=True)

    assert done.state is JobState.SUCCEEDED and done.progress["classified"] == 3
    assert done.progress["titles_error"].startswith("all 1 calls failed: model unavailable")


# ── Regenerate titles ────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("model_repeats_for", "retitled", "disambiguated"), [
    (set(), 4, 0),  # the model tells both groups apart
    ({"Alpha"}, 2, 2),  # a group the model only gives its shared title back is told apart by its URLs
])
async def test_regenerate_titles_leaves_no_delta_url_sharing_a_title(tmp_path, monkeypatch, model_repeats_for, retitled, disambiguated):
    db = FakeDatabase()
    await collection(db, titles=["Alpha", "Alpha", "Beta", "Beta", "Solo"])
    original = jobs_mod.suggest_distinct_titles

    async def model(llm, docs, *, shared_title, **kw):
        row = await original(llm, docs, shared_title=shared_title, **kw)
        if shared_title in model_repeats_for:
            row["titles"] = {u: {**t, "title": shared_title} for u, t in row["titles"].items()}
        return row

    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", model)

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_titles(c))

    assert done.state is JobState.SUCCEEDED
    assert (done.progress["retitled"], done.progress["disambiguated"]) == (retitled, disambiguated)
    assert (await db.duplicate_title_counts(CID))["delta_urls"] == 0


async def test_a_group_the_model_keeps_failing_on_is_told_apart_by_its_urls(tmp_path, monkeypatch):
    """_retitle_duplicates promises no duplicate survives: what the model cannot do, the URLs do. On
    the second pass only the failed group is left, run_pool raises "all 1 calls failed", and the
    job fails with that group still duplicated."""
    db = FakeDatabase()
    await collection(db, titles=["Alpha", "Alpha", "Beta", "Beta"])
    original = jobs_mod.suggest_distinct_titles

    async def model(llm, docs, *, shared_title, **kw):
        if shared_title == "Alpha":
            raise LLMError("the model gave up on this group")
        return await original(llm, docs, shared_title=shared_title, **kw)

    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", model)

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_titles(c))

    assert done.state is JobState.SUCCEEDED
    assert (await db.duplicate_title_counts(CID))["delta_urls"] == 0


async def test_regenerate_titles_refuses_when_no_title_is_shared(tmp_path):
    db = FakeDatabase()
    await collection(db, titles=["Alpha", "Beta"])

    done = await _run(db, tmp_path, lambda e, c: e.start_llm_titles(c))

    assert (done.state, done.error) == (JobState.FAILED, "no delta URL shares its title and document type with another page")


async def test_a_resumed_regenerate_titles_still_leaves_no_duplicates(tmp_path, monkeypatch):
    """#30: titles written before the restart are kept; the groups left are planned again."""
    db = FakeDatabase()
    c = await collection(db, titles=["Alpha", "Alpha", "Beta", "Beta"])
    original, held = jobs_mod.suggest_distinct_titles, asyncio.Event()

    async def hold_beta(llm, docs, *, shared_title, **kw):
        if shared_title == "Beta":
            held.set()
            await asyncio.Event().wait()
        return await original(llm, docs, shared_title=shared_title, **kw)

    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", hold_beta)
    first = make_engine(db, tmp_path)
    job = await first.start_llm_titles(c)
    await asyncio.wait_for(held.wait(), 10)

    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", original)
    second = await restart(first, db, tmp_path)
    done = await finished(db, job.id)
    await second.shutdown()

    assert (done.state, done.progress["restarts"]) == (JobState.SUCCEEDED, 1)
    assert (await db.duplicate_title_counts(CID))["delta_urls"] == 0


async def test_suggest_metadata_reports_its_total_before_the_first_answer(tmp_path, monkeypatch):
    """A long Suggest-metadata run shows "0 of N" from the start, and the progress a restart resumes
    from has every counter in place before the first answer comes back."""
    db = FakeDatabase()
    await collection(db, titles=["A", "B", "C"])
    calls = Calls(jobs_mod.suggest_metadata_one, hold_after=0)  # every call waits: no answer yet
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", calls)
    engine = make_engine(db, tmp_path)

    job = await engine.start_llm_metadata(await db.get_collection(CID))
    await asyncio.wait_for(calls.held.wait(), 5)

    p = (await db.get_job(job.id)).progress
    assert {k: p.get(k) for k in ("llm", "total", "done", "failed", "tokens_in", "tokens_out")} == {
        "llm": "metadata", "total": 3, "done": 0, "failed": 0, "tokens_in": 0, "tokens_out": 0}
    await engine.shutdown()
