"""The duplicate-title pass on the in-memory FakeDatabase with the fake model: which pages go to the
model, in which calls, with which question, and what is written back. Replaces the job-level parts of
the old tests/integration/test_duplicate_titles.py; the SQL that finds the groups is checked by
tests/integration/test_db_contract.py and the pages by tests/integration/test_g1_duplicate_titles.py."""

import json

import pytest

import sde_curation.jobs as jobs_mod
from sde_curation.config import Settings
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import URL_DISAMBIGUATED, suggest_distinct_titles
from sde_curation.models import Confidence, DistinctTitle, DistinctTitles, JobState
from tests.support.engine import CID, collection, finished, make_engine
from tests.support.fake_db import FakeDatabase

SHARED = "Same"  # what the fake model makes of "Same - Ex" (it strips the site suffix)
SCRAPED_SHARED = f"{SHARED} - Ex"
SOLO = "Unique - Ex"
SETTINGS = Settings(llm_provider="fake", data_dir="/tmp/unused")


def page(i: int) -> str:
    """The URL `tests.support.engine.collection` gives the i-th title (1-based)."""
    return f"https://{CID}/p{i:03d}"


class Recorder:
    """Wraps tasks.suggest_distinct_titles as jobs.py calls it: records each group call, and can
    replace the model's answers (`answer(shared_title, urls) -> {url: title}`)."""

    def __init__(self, answer=None):
        self.calls: list[dict] = []
        self.answer = answer
        self.original = jobs_mod.suggest_distinct_titles

    async def __call__(self, llm, docs, *, shared_title, **kw):
        urls = [d["url"] for d in docs]
        self.calls.append({"urls": urls, "shared_title": shared_title,
                           "settled": [s["url"] for s in kw.get("settled") or []],
                           "previous": set(kw.get("previous") or ())})
        row = await self.original(llm, docs, shared_title=shared_title, **kw)
        if self.answer is not None:
            row["titles"] = {u: {"title": t, "title_conf": Confidence.MEDIUM}
                             for u, t in self.answer(shared_title, urls).items()}
        return row


async def run(db, tmp_path, start, **settings):
    engine = make_engine(db, tmp_path, **settings)
    job = await start(engine, await db.get_collection(CID))
    done = await finished(db, job.id)
    await engine.shutdown()
    return done


async def rows(db) -> dict:
    return {d.url: d for d in await db.load_deltas(CID)}


# ── Suggest metadata: the pass after it ─────────────────────────────────────────────────────────


async def test_suggest_metadata_leaves_shared_titles_as_generated_by_default(tmp_path, monkeypatch):
    """LLM_DEDUPE_TITLES is off by default: the duplicates are flagged for the curator, and nothing
    is sent back to the model on its own (each regenerate is a cost the curator chooses)."""
    db = FakeDatabase()
    await collection(db, titles=[SCRAPED_SHARED, SCRAPED_SHARED, SOLO])
    recorder = Recorder()
    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", recorder)

    done = await run(db, tmp_path, lambda e, c: e.start_llm_metadata(c))

    assert done.state is JobState.SUCCEEDED and "titles_total" not in done.progress
    assert recorder.calls == []
    assert [r.title_ai for r in (await rows(db)).values()] == [SHARED, SHARED, "Unique"]


async def test_suggest_metadata_with_dedupe_on_retitles_only_the_pages_that_share_a_title(tmp_path):
    """Opted in, Suggest metadata ends with the duplicate pass: the two pages sharing "Same" get a
    title of their own, keep the title they shared as `title_ai_before`, and nothing else on the
    row is redone; the page with a title of its own is left alone."""
    db = FakeDatabase()
    await collection(db, titles=[SCRAPED_SHARED, SCRAPED_SHARED, SOLO])

    done = await run(db, tmp_path, lambda e, c: e.start_llm_metadata(c), llm_dedupe_titles=True)

    got = await rows(db)
    p = done.progress
    assert (p["classified"], p["titles_total"], p["retitled"], p["still_duplicate"]) == (3, 2, 2, 0)
    assert [(got[page(i)].title_ai, got[page(i)].title_ai_before) for i in (1, 2, 3)] == [
        (f"{SHARED} — P001", SHARED), (f"{SHARED} — P002", SHARED), ("Unique", None)]
    assert got[page(1)].document_type_ai == got[page(3)].document_type_ai  # only the title is redone


# ── Regenerate duplicate titles ─────────────────────────────────────────────────────────────────


async def test_a_group_goes_to_the_model_in_one_call_with_only_its_own_pages(tmp_path, monkeypatch):
    """The model tells pages apart from EACH OTHER, so the whole group is one call; a page with a
    title of its own is never sent."""
    db = FakeDatabase()
    await collection(db, titles=[SHARED, SHARED, SOLO])
    recorder = Recorder()
    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", recorder)

    await run(db, tmp_path, lambda e, c: e.start_llm_titles(c))

    assert [c["urls"] for c in recorder.calls] == [[page(1), page(2)]]


async def test_regenerated_titles_are_suggestions_and_never_applied(tmp_path):
    """Nothing reaches the effective title until the SME accepts: the new title is `title_ai`, and
    the title the pages shared is kept beside it."""
    db = FakeDatabase()
    await collection(db, titles=[SHARED, SHARED])
    titles_before = {u: r.title for u, r in (await rows(db)).items()}

    done = await run(db, tmp_path, lambda e, c: e.start_llm_titles(c))

    got = await rows(db)
    assert done.progress["retitled"] == 2
    assert {u: r.title for u, r in got.items()} == titles_before
    assert [(got[page(i)].title_ai, got[page(i)].title_ai_before) for i in (1, 2)] == [
        (f"{SHARED} — P001", SHARED), (f"{SHARED} — P002", SHARED)]


async def test_pages_with_a_pending_ai_title_are_asked_first_and_the_rest_go_round_again(tmp_path, monkeypatch):
    """In a group, the pages with a pending AI title are asked first and the others are named as
    settled; whatever still shares a title after that pass goes round again (p3 has left the group
    by then), so one click ends at zero."""
    db = FakeDatabase()
    await collection(db, titles=[SHARED, SHARED, SHARED])
    await db.set_delta_ai_titles(CID, [{"url": page(3), "title": SHARED, "title_conf": "low"}])
    recorder = Recorder()
    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", recorder)

    done = await run(db, tmp_path, lambda e, c: e.start_llm_titles(c))

    assert [(c["urls"], c["settled"]) for c in recorder.calls] == [
        ([page(3)], [page(1), page(2)]), ([page(1), page(2)], [])]
    assert (done.progress["titles_total"], done.progress["title_pass"], done.progress["still_duplicate"]) == (1, 2, 0)


async def test_a_group_asked_again_is_asked_from_the_title_it_first_shared(tmp_path, monkeypatch):
    """Two pages put back on one regenerated title: the group is asked about the title the pages
    originally shared, never about the last answer (that is what grew "X — A — A — …" tails), and
    the answer that put them here goes along as one not to offer again."""
    db = FakeDatabase()
    regenerated = f"{SHARED} — Access"
    await collection(db, titles=[SHARED, SHARED])
    await db.set_delta_ai_titles(CID, [{"url": page(i), "title": regenerated, "before": SHARED} for i in (1, 2)])
    recorder = Recorder()
    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", recorder)

    await run(db, tmp_path, lambda e, c: e.start_llm_titles(c))

    assert (recorder.calls[0]["shared_title"], recorder.calls[0]["previous"]) == (SHARED, {regenerated})


async def test_a_title_the_urls_had_to_resolve_is_marked_as_such(tmp_path, monkeypatch):
    """When the model only hands the shared title back, the URLs tell the pages apart; those rows
    carry the rule's name as their model, at low confidence, so the SME knows which to check."""
    db = FakeDatabase()
    await collection(db, titles=[SHARED, SHARED])
    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", Recorder(lambda shared, urls: dict.fromkeys(urls, shared)))

    done = await run(db, tmp_path, lambda e, c: e.start_llm_titles(c))

    got = await rows(db)
    assert (done.progress["disambiguated"], done.progress["still_duplicate"]) == (2, 0)
    assert {(r.ai_model, r.title_ai_conf) for r in got.values()} == {(URL_DISAMBIGUATED, Confidence.LOW)}


GROUPS, GROUP_SIZE = 4, 4


async def test_groups_side_by_side_never_take_the_same_title_and_count_every_page_once(tmp_path, monkeypatch):
    """Groups run against each other in the pool. A model that gives every group the same answers
    must not hand one title to two groups (the claim on a title), and every page that moved is in
    exactly one of the two counters."""
    db = FakeDatabase()
    await collection(db, titles=[f"Section {g}" for g in range(GROUPS) for _ in range(GROUP_SIZE)])
    same_answers = Recorder(lambda shared, urls: {u: f"Common {i}" for i, u in enumerate(urls)})
    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", same_answers)

    done = await run(db, tmp_path, lambda e, c: e.start_llm_titles(c), llm_workers=GROUPS)

    got = (await rows(db)).values()
    moved = [r for r in got if r.title_ai_before]
    assert done.progress["retitled"] + done.progress["disambiguated"] == len(moved) == GROUPS * GROUP_SIZE
    assert len({r.title_ai for r in got}) == GROUPS * GROUP_SIZE


# ── the group call itself (tasks.suggest_distinct_titles) ───────────────────────────────────────


def _header(call: dict) -> dict:
    return json.loads(call["user"].split("\n", 1)[1].split("\n\n", 1)[0])


DOCS = [{"url": "https://ex.org/x/alpha", "title": SCRAPED_SHARED, "text": "body of alpha"},
        {"url": "https://ex.org/x/beta", "title": SCRAPED_SHARED, "text": "body of beta"}]


async def test_the_group_call_tells_the_model_what_differs_in_each_url():
    """The differing part of each URL is worked out for the model, not left for it to spot."""
    fake = FakeProvider()

    await suggest_distinct_titles(fake, DOCS, shared_title=SHARED, sharing=2, settings=SETTINGS)

    header = _header(fake.calls[-1])
    assert (header["shared_title"], header["pages_sharing_it"], header["pages_to_retitle"]) == (SHARED, 2, 2)
    assert header["url_differs_at"] == {DOCS[0]["url"]: ["alpha"], DOCS[1]["url"]: ["beta"]}


async def test_the_group_call_names_the_answers_that_already_failed():
    fake = FakeProvider()

    await suggest_distinct_titles(fake, DOCS, shared_title=SHARED, sharing=2, previous=["One Title"],
                                  settings=SETTINGS)

    assert _header(fake.calls[-1])["previous_titles"] == ["One Title"]


@pytest.mark.parametrize("taken", [SHARED, "one title", "SETTLED"], ids=["shared", "previous", "settled"])
async def test_an_answer_that_repeats_a_title_already_spoken_for_is_dropped(taken):
    """A title the group shared, one an earlier pass gave, or a settled page's title would collide
    again: it is dropped (case and spacing do not make it new), and the page falls through to the
    next pass or to the URLs."""
    answer = DistinctTitles(items=[DistinctTitle(url=DOCS[0]["url"], title=taken, title_confidence=Confidence.HIGH),
                                   DistinctTitle(url=DOCS[1]["url"], title="Beta", title_confidence=Confidence.HIGH)])
    fake = FakeProvider(canned=answer.model_dump(mode="json"))

    row = await suggest_distinct_titles(fake, DOCS, shared_title=SHARED, sharing=3, previous=["One Title"],
                                        settled=[{"url": "https://ex.org/x/gamma", "title": "Settled"}],
                                        settings=SETTINGS)

    assert row["titles"] == {DOCS[1]["url"]: {"title": "Beta", "title_conf": Confidence.HIGH}}
