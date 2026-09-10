"""Phase 4: provider contract, sanity filters, jobs, accept/reject — all offline (fake provider)."""

import asyncio

import pytest

from sde_curation.config import Settings
from sde_curation.llm.base import LLMError, LLMRetryable, make_llm
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import (
    suggest_metadata,
    suggest_metadata_one,
    suggest_patterns_batch,
)
from sde_curation.models import Collection, Division, MetadataSuggestion, PatternSuggestions
from tests.conftest import wait_job

COLL = Collection(collection_id="ex.org", name="Ex", seed_url="https://ex.org", division=Division.GENERAL,
                  connector="crawler2", max_pages=10)


# ── pure task layer ───────────────────────────────────────────────────


async def test_suggest_patterns_keeps_only_globs_matching_the_batch_it_saw():
    canned = {"suggestions": [
        {"type": "exclude", "match": "*/privacy*", "rationale": "chrome"},
        {"type": "exclude", "match": "*/privacy*", "rationale": "dup"},
        {"type": "exclude", "match": "*/nothing-like-this*", "rationale": "hallucinated"},
        {"type": "exclude", "match": "*", "rationale": "too broad"},
        {"type": "exclude", "match": "*/login*", "rationale": "in the dump, but not in this batch"},
    ]}
    fake = FakeProvider(canned)
    kept, done = await suggest_patterns_batch(
        fake, COLL, [{"url": "https://ex.org/privacy"}, {"url": "https://ex.org/a"}],
        examples=["*/feed*"], batch_no=2, batches=3,
    )
    assert [(k.type, k.match) for k in kept] == [("exclude", "*/privacy*")]
    assert done.model == "fake" and done.tokens_in > 0
    assert "*/feed*" in fake.calls[0]["user"] and "2 of 3" in fake.calls[0]["user"]


async def test_suggest_patterns_only_accepts_exclude_globs():
    for bad in (
        {"type": "division", "match": "*", "value": "Heliophysics", "rationale": "x"},
        {"type": "title", "match": "*", "value": "{title}", "rationale": "x"},
        {"type": "include", "match": "*/keep*", "rationale": "x"},
    ):
        with pytest.raises(LLMError, match="did not match PatternSuggestions"):
            await suggest_patterns_batch(FakeProvider({"suggestions": [bad]}), COLL, [{"url": "https://ex.org/a"}])


SETTINGS = Settings(llm_provider="fake", data_dir="/tmp/x")


async def test_suggest_metadata_is_one_call_per_document_with_full_text_and_confidence():
    fake = FakeProvider()
    docs = [{"url": f"https://ex.org/p{i}", "title": f"Aurora page {i} – Ex", "text": "some data " * 500}
            for i in range(45)]
    seen = []
    rows = await suggest_metadata(fake, docs, settings=SETTINGS,
                                  on_progress=lambda p: (seen.append(p), asyncio.sleep(0))[1])
    assert len(fake.calls) == 45 and seen[-1] == {"done": 45, "total": 45}
    assert "some data " * 500 in fake.calls[0]["user"]  # the whole text, not a 1200-char slice
    assert len(rows) == 45 and rows[0]["title"] == "Aurora page 0" and rows[0]["division"] == "Heliophysics"
    assert rows[0]["title_conf"] == "high" and rows[0]["division_conf"] == "high"  # "aurora" in the title
    assert rows[0]["document_type"] == "Data" and rows[0]["document_type_conf"] == "medium"  # "data" only in text
    assert rows[0]["model"] == "fake" and rows[0]["tokens_in"] > 0


async def test_suggest_metadata_one_sends_the_whole_page_and_records_the_model():
    fake = FakeProvider()
    huge = "".join(f"paragraph {i} " for i in range(100_000))  # ~1.3M chars: still sent whole
    row = await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "A", "text": huge,
                                            "content_hash": "h"}, settings=SETTINGS)
    assert fake.calls[-1]["model"] is None and row["model"] == "fake"  # the provider's default model
    assert fake.calls[-1]["user"].endswith(huge) and "paragraph 99999" in fake.calls[-1]["user"]
    assert row["content_hash"] == "h" and "truncated" not in row and "large_model" not in row


async def test_schemas_reject_bad_enums():
    from pydantic import ValidationError

    ok = {"title_confidence": "high", "division_confidence": "low", "document_type_confidence": "low"}
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({"division": "Kitchen", **ok})
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({"title": "T"})  # confidence is required per field
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({"title": "T", **ok, "title_confidence": "certain"})
    with pytest.raises(ValidationError):
        PatternSuggestions.model_validate({"suggestions": [{"type": "title", "match": "*", "rationale": "no value"}]})
    PatternSuggestions.model_validate({"suggestions": []})


def test_registry_and_missing_key(settings):
    assert make_llm(settings).name == "fake"
    with pytest.raises(LLMError, match="OPENAI_API_KEY"):
        make_llm(settings.model_copy(update={"llm_provider": "openai", "openai_api_key": None}))


# ── through the app (fake crawler + fake LLM) ─────────────────────────


async def setup(client, n=10):
    if (await client.get("/api/collections/ex.org")).status_code == 404:
        await client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": n})
    await client.post("/api/collections/ex.org/scrape")
    await wait_job(client, "ex.org")
    await client.post("/api/collections/ex.org/recompute")


async def test_pattern_suggestions_flow(crawler_client):
    c = crawler_client
    assert (await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})).status_code == 201
    assert (await c.post("/api/collections/ex.org/suggest/patterns")).status_code == 409  # no dump
    await setup(c)
    r = await c.post("/api/collections/ex.org/suggest/patterns")
    assert r.status_code == 202
    job = await wait_job(c, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["suggestions"] >= 1, job
    assert job["progress"]["urls"] == 8 and job["progress"]["calls"] == 1 and job["progress"]["llm"] == "patterns"
    sugs = (await c.get("/api/collections/ex.org/suggestions")).json()
    assert sugs and all(s["state"] == "pending" and s["type"] == "exclude" and s["source"] == "llm" for s in sugs)
    ex = next(s for s in sugs if s["match"] == "https://ex.org/p9")  # the fake excludes the last URL of a batch
    # nothing applied yet
    assert (await c.get("/api/collections/ex.org/patterns")).json() == []
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert f"{len(sugs)} suggestion" in page and "Accept" in page and "accept all" in page
    assert "8 URLs in 1 call" in page
    # accept → real exclude rule, recomputed: p9 is out of scope, so it can never reach the index
    r = await c.post(f"/api/collections/ex.org/suggestions/{ex['id']}/accept")
    assert r.status_code == 200
    pats = (await c.get("/api/collections/ex.org/patterns")).json()
    assert len(pats) == 1 and pats[0]["type"] == "exclude" and pats[0]["matches"] == 1
    d = (await c.get("/api/collections/ex.org/deltas?q=p9")).json()["items"][0]
    assert d["excluded"] is True
    assert (await c.post(f"/api/collections/ex.org/suggestions/{ex['id']}/accept")).status_code == 409  # already decided
    # reject leaves nothing behind
    other = next((s for s in sugs if s["id"] != ex["id"]), None)
    if other:
        assert (await c.post(f"/api/collections/ex.org/suggestions/{other['id']}/reject")).status_code == 200
        assert len((await c.get("/api/collections/ex.org/patterns")).json()) == 1
    assert (await c.post("/api/collections/ex.org/suggestions/9999/accept")).status_code == 404
    assert (await c.post(f"/api/collections/ex.org/suggestions/{ex['id']}/maybe")).status_code == 422


async def test_global_excludes_prepass_and_batching(crawler_client):
    c = crawler_client
    db = c.app.state.db
    await setup(c)
    from sde_curation.models import DumpUrl
    dump = await db.load_dump("ex.org")
    extra = [DumpUrl(collection_id="ex.org", url=u, scraped_title="x") for u in (
        "https://ex.org/login", "http://ex.org/login/", "https://ex.org/tag/sun", "https://ex.org/science/a",
    )]
    await db.replace_dump("ex.org", dump + extra)
    c.app.state.settings.llm_pattern_batch_urls = 50  # small batches on a small dump
    c.app.state.jobs.s.llm_pattern_batch_urls = 50
    await c.post("/api/collections/ex.org/recompute")
    await c.post("/api/collections/ex.org/suggest/patterns")
    job = await wait_job(c, "ex.org")
    p = job["progress"]
    assert job["state"] == "succeeded", job
    assert p["urls"] == 12 and p["unique"] == 11 and p["calls"] == 1 and p["global"] == 2
    sugs = (await c.get("/api/collections/ex.org/suggestions")).json()
    by = {s["match"]: s for s in sugs}
    # global list hits come first, with match counts over the whole dump (http + https variants)
    assert [s["source"] for s in sugs[:2]] == ["global", "global"]
    assert by["*/login*"]["source"] == "global" and by["*/login*"]["matches"] == 2
    assert by["*/tag/*"]["source"] == "global" and by["*/tag/*"]["matches"] == 1
    assert "*/privacy*" not in by  # in the list, matches nothing here
    # the model's own rows: it also proposed */login* and */tag* (chrome segments) — the global row wins
    assert all(s["type"] == "exclude" for s in sugs)
    assert sum(1 for s in sugs if s["source"] == "llm") >= 1
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert "src-global" in page and "from the global list" in page


async def test_metadata_suggestions_flow(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 409  # no deltas
    await setup(c)
    assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 202
    job = await wait_job(c, "ex.org")
    p = job["progress"]
    assert job["state"] == "succeeded" and p["classified"] == 8 and p["done"] == 8 and p["failed"] == 0, job
    assert p["llm"] == "metadata" and p["tokens_in"] > 0 and p["inflight"] == 0
    d = (await c.get("/api/collections/ex.org/deltas?q=p2")).json()["items"][0]
    assert d["title_ai"] == "Page 2" and d["document_type_ai"] == "Documentation"
    assert d["title_ai_conf"] == "high" and d["document_type_ai_conf"] == "low" and d["ai_model"] == "fake"
    assert len(d["ai_content_hash"]) == 64
    assert d["title"] is None and d["document_type"] is None  # effective fields untouched
    page = (await c.get("/collections/ex.org?tab=urls&set=deltas")).text
    assert "AI: Page 2" in page and 'class="ai conf-high"' in page and 'class="ai conf-low"' in page
    assert (await c.get("/api/collections/ex.org/deltas?q=p2")).json()["total"] == 1
    low = (await c.get("/collections/ex.org?tab=urls&set=deltas&ai=low")).text
    assert "AI: Documentation" in low
    curate = (await c.get("/collections/ex.org?tab=curate")).text
    assert "conf-high" in curate and "8 classified" in curate and "tokens in" in curate
    # accept title → exact-URL pattern; ml cleared
    r = await c.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p2", "field": "title"})
    assert r.status_code == 200
    d = (await c.get("/api/collections/ex.org/deltas?q=p2")).json()["items"][0]
    assert d["title"] == "Page 2" and d["title_ai"] is None
    # reject doc type → cleared, effective untouched
    await c.post("/api/collections/ex.org/ai/reject", json={"url": "https://ex.org/p2", "field": "document_type"})
    d = (await c.get("/api/collections/ex.org/deltas?q=p2")).json()["items"][0]
    assert d["document_type_ai"] is None and d["document_type"] is None
    assert (await c.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p2", "field": "division"})).status_code == 409  # none
    assert (await c.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p2", "field": "bogus"})).status_code == 422
    # second run: URLs still missing suggestions only → p2 (cleared) is the only candidate again
    r = await c.post("/api/collections/ex.org/suggest/metadata")
    assert r.status_code == 202
    job = await wait_job(c, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["classified"] == 1
    # now nothing is left → 409 up front, no failed job
    await c.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p2", "field": "title"})
    r = await c.post("/api/collections/ex.org/suggest/metadata")
    assert r.status_code == 409 and "nothing to classify" in r.text
    assert (await c.post("/api/collections/ex.org/suggest/metadata?all=true")).status_code == 202
    await wait_job(c, "ex.org")


async def test_malformed_llm_output_fails_job_and_writes_nothing(crawler_client, monkeypatch):
    c = crawler_client
    await setup(c)
    c.app.state.jobs._llm = FakeProvider(canned={"division": "Kitchen", "title_confidence": "high",
                                                 "division_confidence": "high", "document_type_confidence": "high"})
    await c.post("/api/collections/ex.org/suggest/metadata")
    job = await wait_job(c, "ex.org")
    assert job["state"] == "failed" and "did not match" in job["error"] and job["progress"]["failed"] == 8
    d = (await c.get("/api/collections/ex.org/deltas?q=p2")).json()["items"][0]
    assert d["division_ai"] is None


async def test_llm_job_locks_collection(crawler_client):
    c = crawler_client
    await setup(c)

    class Slow(FakeProvider):
        async def complete(self, **kw):
            await asyncio.sleep(1.0)
            return await super().complete(**kw)

    c.app.state.jobs._llm = Slow()
    await c.post("/api/collections/ex.org/suggest/metadata")
    await asyncio.sleep(0.1)
    assert (await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p3"})).status_code == 409
    assert (await c.post("/api/collections/ex.org/suggest/patterns")).status_code == 409
    r = await c.post("/api/collections/ex.org/jobs/cancel")
    assert r.status_code == 200 and r.json()["state"] == "failed"


async def test_metadata_partial_failure_succeeds_and_resumes(crawler_client):
    c = crawler_client
    await setup(c)

    class Flaky(FakeProvider):
        async def complete(self, **kw):
            if "ex.org/p3" in kw["user"]:
                raise LLMRetryable("429 too many requests")
            return await super().complete(**kw)

    c.app.state.jobs._llm = Flaky()
    await c.post("/api/collections/ex.org/suggest/metadata")
    job = await wait_job(c, "ex.org")
    p = job["progress"]
    assert job["state"] == "succeeded" and p["classified"] == 7 and p["failed"] == 1 and "429" in p["last_error"]
    assert "7 classified · 1 failed" in (await c.get("/collections/ex.org?tab=curate")).text
    d = (await c.get("/api/collections/ex.org/deltas?q=p3")).json()["items"][0]
    assert d["title_ai"] is None
    # the next run only picks up the one that failed
    c.app.state.jobs._llm = FakeProvider()
    assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 202
    job = await wait_job(c, "ex.org")
    assert job["progress"]["classified"] == 1 and job["progress"]["total"] == 1


async def test_metadata_cancel_keeps_finished_rows(crawler_client):
    c = crawler_client
    await setup(c, n=40)  # 32 docs
    c.app.state.jobs.s.llm_workers = 2

    class Slow(FakeProvider):
        async def complete(self, **kw):
            await asyncio.sleep(0.08)
            return await super().complete(**kw)

    c.app.state.jobs._llm = Slow()
    await c.post("/api/collections/ex.org/suggest/metadata")
    await asyncio.sleep(0.5)
    r = await c.post("/api/collections/ex.org/jobs/cancel")
    assert r.status_code == 200 and r.json()["state"] == "failed" and "cancelled" in r.json()["error"]
    items = (await c.get("/api/collections/ex.org/deltas?limit=100")).json()["items"]
    done = [d for d in items if d["title_ai"]]
    assert 2 <= len(done) < 32
    c.app.state.jobs._llm = FakeProvider()
    await c.post("/api/collections/ex.org/suggest/metadata")
    job = await wait_job(c, "ex.org", timeout=30)
    assert job["progress"]["total"] == 32 - len(done) and job["progress"]["classified"] == 32 - len(done)


async def test_content_changed_rows_are_reclassified(crawler_client):
    from sde_curation.models import DumpUrl

    c = crawler_client
    db = c.app.state.db
    await setup(c)
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 409  # all classified
    await c.post("/api/collections/ex.org/promote")
    dump = await db.load_dump("ex.org")
    rows = [DumpUrl(collection_id="ex.org", url=d.url, scraped_title=d.scraped_title,
                    full_text="new aurora text" if d.url.endswith("/p2") else "text " * 5) for d in dump]
    await db.replace_dump("ex.org", rows)
    r = await c.post("/api/collections/ex.org/recompute")
    assert r.json()["content_changed"] == 1
    assert await db.count_deltas_for_llm("ex.org") == 1
    await c.post("/api/collections/ex.org/suggest/metadata")
    job = await wait_job(c, "ex.org")
    assert job["progress"]["classified"] == 1
    d = (await c.get("/api/collections/ex.org/deltas?q=p2")).json()["items"][0]
    assert d["division_ai"] == "Heliophysics" and d["division_ai_conf"] == "medium"  # from the new text
    assert d["ai_content_hash"] == next(x.content_hash for x in await db.load_dump("ex.org") if x.url.endswith("/p2"))
    assert await db.count_deltas_for_llm("ex.org") == 0  # classified against the current text: done


async def test_metadata_classified_count_survives_concurrent_flushes(crawler_client, monkeypatch):
    """Every answer flushes on its own (AI_FLUSH_ROWS=1) while 8 workers run: the written count
    must still equal the number of answers (a `+= await` here once lost most of them)."""
    from sde_curation import jobs as jobs_mod

    monkeypatch.setattr(jobs_mod, "AI_FLUSH_ROWS", 1)
    c = crawler_client
    await setup(c, n=40)  # 32 docs

    class Slow(FakeProvider):
        async def complete(self, **kw):
            await asyncio.sleep(0.01)
            return await super().complete(**kw)

    c.app.state.jobs._llm = Slow()
    await c.post("/api/collections/ex.org/suggest/metadata")
    job = await wait_job(c, "ex.org", timeout=30)
    assert job["progress"]["classified"] == 32 == job["progress"]["done"]
    items = (await c.get("/api/collections/ex.org/deltas?limit=100")).json()["items"]
    assert sum(1 for d in items if d["title_ai"]) == 32


async def test_prompts_are_visible(crawler_client):
    c = crawler_client
    r = await c.get("/api/llm/prompts")
    assert r.status_code == 200 and "exclude" in r.json()["patterns"]["system"]
    assert "confidence" in r.json()["metadata"]["system"] and "FULL page text, never cut" in r.json()["metadata"]["user"]
    await setup(c)
    page = (await c.get("/collections/ex.org?tab=curate")).text
    assert page.count("Show the prompt") == 2 and "search result" in page


async def test_openai_provider_sends_temperature_only_when_configured():
    """gpt-5 / o-series reject any temperature but the default (400 unsupported_value): the
    request must omit it unless LLM_TEMPERATURE is set explicitly."""
    from types import SimpleNamespace

    from sde_curation.llm.openai import OpenAIProvider

    class Stub:
        def __init__(self):
            self.calls = []
            self.chat = SimpleNamespace(completions=SimpleNamespace(parse=self.parse))

        async def parse(self, **kw):
            self.calls.append(kw)
            answer = MetadataSuggestion(title="T", title_confidence="high", division_confidence="low",
                                        document_type_confidence="low")
            msg = SimpleNamespace(parsed=answer, refusal=None, content=None)
            return SimpleNamespace(choices=[SimpleNamespace(message=msg)], model=kw["model"], usage=None)

    async def run(**over):
        p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai", **over))
        p.client = Stub()
        await p.complete(system="s", user="u", schema=MetadataSuggestion)
        return p.client.calls[0]

    assert "temperature" not in await run()
    assert (await run(llm_temperature=0))["temperature"] == 0
