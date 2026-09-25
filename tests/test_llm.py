"""Phase 4: provider contract, sanity filters, jobs, accept/reject — all offline (fake provider)."""

import asyncio

import pytest

from sde_curation.config import Settings
from sde_curation.llm.base import LLMError, LLMRetryable, make_llm
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import (
    count_tokens,
    fixed_tokens,
    share_budget,
    suggest_distinct_titles,
    suggest_metadata,
    suggest_metadata_one,
    suggest_patterns_batch,
)
from sde_curation.models import (
    Collection,
    DistinctTitles,
    Division,
    MetadataSuggestion,
    PatternSuggestions,
)
from tests.conftest import wait_job

# division defaults to General = "not assigned", so the model is asked for one per page
COLL = Collection(collection_id="ex.org", name="Ex", seed_url="https://ex.org",
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


async def test_suggest_patterns_drops_globs_the_applied_global_list_already_covers():
    canned = {"suggestions": [
        {"type": "exclude", "match": "*/login*", "rationale": "covered by */login*"},
        {"type": "exclude", "match": "*/account/*", "rationale": "only partly covered"},
    ]}
    batch = [{"url": u} for u in ("https://ex.org/login", "https://ex.org/account/login", "https://ex.org/account/me")]
    kept, _ = await suggest_patterns_batch(FakeProvider(canned), COLL, batch, examples=["*/login*"])
    assert [k.match for k in kept] == ["*/account/*"]


async def test_suggest_metadata_sends_the_collection_context():
    """A division the curator set goes along as context and is never asked for: the schema has no
    division field, the prompt says so, and the row carries no division suggestion. Without one,
    the model is asked for a division as before."""
    fake = FakeProvider()
    coll = COLL.model_copy(update={"name": "PDS", "division": Division.PLANETARY})
    row = await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "Proposers - PDS", "text": "t"},
                                     settings=SETTINGS, collection=coll)
    user = fake.calls[-1]["user"]
    assert '"collection": "PDS"' in user and '"collection_division": "Planetary Science"' in user
    assert "collection_document_type" not in user  # not set on the collection
    assert row["title"] == "Proposers"  # the model's title as written: no prefix added
    assert fake.calls[-1]["schema"] == "MetadataSuggestionNoDivision"
    assert "division" not in row and "division_conf" not in row
    assert "division — not asked for." in fake.calls[-1]["system"]
    # no division on the collection: the model decides one per page, as before
    row = await suggest_metadata_one(fake, {"url": "https://ex.org/a", "text": "t"}, settings=SETTINGS, collection=COLL)
    assert "collection_division" not in fake.calls[-1]["user"]
    assert fake.calls[-1]["schema"] == "MetadataSuggestion" and row["division"]


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


def _prompt_tokens(call, schema=MetadataSuggestion):
    """What the API would count for a fake call: system + user + response schema + framing."""
    return fixed_tokens(call["system"], schema) + count_tokens(call["user"])


async def test_suggest_metadata_one_sends_a_page_that_fits_whole_and_records_the_model():
    fake = FakeProvider()
    page = "".join(f"paragraph {i} " for i in range(30_000))  # ~390K chars, ~60K tokens: fits
    row = await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "A", "text": page,
                                            "content_hash": "h"}, settings=SETTINGS)
    assert fake.calls[-1]["model"] is None and row["model"] == "fake"  # the provider's default model
    assert fake.calls[-1]["user"].endswith(page) and '"text_cut"' not in fake.calls[-1]["user"]
    assert row["content_hash"] == "h" and "truncated" not in row and "large_model" not in row


async def test_suggest_metadata_one_cuts_a_page_to_fit_the_input_limit():
    """gpt-5-nano refuses a prompt over 272K tokens; a bigger page (ascl.net's listing URLs are
    ~780K) is cut from the end so the WHOLE prompt fits llm_max_input_tokens, and the model is told."""
    fake = FakeProvider()
    huge = "".join(f"paragraph {i} " for i in range(150_000))  # ~2.3M chars, ~450K tokens
    await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "A", "text": huge,
                                      "content_hash": "h"}, settings=SETTINGS)
    call = fake.calls[-1]
    assert f'"text_chars": {len(huge)}, "text_cut": true' in call["user"]
    assert "Text:\nparagraph 0 paragraph 1 " in call["user"] and "paragraph 149999" not in call["user"]
    assert 269_000 < _prompt_tokens(call) <= SETTINGS.llm_max_input_tokens  # uses the room, stays under it
    await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "A", "text": huge, "content_hash": "h"},
                               settings=Settings(llm_provider="fake", data_dir="/tmp/x", llm_max_input_tokens=50_000))
    assert _prompt_tokens(fake.calls[-1]) <= 50_000


def test_share_budget_cuts_only_the_biggest_pages():
    assert share_budget([10, 20, 30], 100) == [10, 20, 30]  # all fit
    assert share_budget([10, 500, 900], 410) == [10, 200, 200]  # the small one whole, the rest even
    assert share_budget([10, 100, 900], 410) == [10, 100, 300]
    assert sum(share_budget([7, 1_000, 3_000, 2], 999)) <= 999


async def test_suggest_distinct_titles_cuts_the_longest_pages_to_fit():
    fake = FakeProvider()
    small = "Volume 12 of the calibrated images. " * 50
    huge = "".join(f"row {i} " for i in range(250_000))  # ~500K tokens
    docs = [{"url": "https://ex.org/v12", "title": "Vol", "text": small},
            {"url": "https://ex.org/all", "title": "Vol", "text": huge}]
    await suggest_distinct_titles(fake, docs, shared_title="Vol", sharing=2, settings=SETTINGS)
    call = fake.calls[-1]
    assert _prompt_tokens(call, DistinctTitles) <= SETTINGS.llm_max_input_tokens
    assert small in call["user"] and "row 249999" not in call["user"]  # the small page whole
    assert f'"text_chars": {len(small)}}}' in call["user"] and f'"text_chars": {len(huge)}, "text_cut": true' in call["user"]


async def test_a_prompt_the_provider_still_refuses_as_too_long_is_cut_and_asked_again():
    """The local count is the GPT-5 tokenizer's; if the provider still says too long, the call is
    re-sent once, shorter by what its error message counted over the limit."""
    from sde_curation.llm.base import LLMInputTooLong

    class Strict(FakeProvider):
        async def complete(self, *, system, user, schema, model=None):
            if not self.calls:
                self.calls.append({"user": user})
                raise LLMInputTooLong("too long", got=300_000, limit=272_000)
            return await super().complete(system=system, user=user, schema=schema, model=model)

    fake = Strict()
    huge = "".join(f"paragraph {i} " for i in range(150_000))
    row = await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "A", "text": huge,
                                            "content_hash": "h"}, settings=SETTINGS)
    first, second = count_tokens(fake.calls[0]["user"]), count_tokens(fake.calls[1]["user"])
    assert first - second >= 28_000 and row["title"]  # cut by the 28K over (+2%), and answered


async def test_schemas_reject_bad_enums():
    from pydantic import ValidationError

    ok = {"title_confidence": "high", "division_confidence": "low", "document_type_confidence": "low"}
    full = {"title": "T", "division": "Earth Science", "document_type": "Data", **ok}
    MetadataSuggestion.model_validate(full)
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({**full, "division": "Kitchen"})
    # "General" is not a division a page can be curated into, so it is not an answer the model
    # can give: it is absent from the schema the provider is handed, not merely rejected after
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({**full, "division": "General"})
    assert "General" not in str(MetadataSuggestion.model_json_schema())
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({"title": "T", "division": "Earth Science", "document_type": "Data"})  # confidence per field
    # every page gets every field: none may be missing or null
    for field in ("title", "division", "document_type"):
        with pytest.raises(ValidationError):
            MetadataSuggestion.model_validate({k: v for k, v in full.items() if k != field})
        with pytest.raises(ValidationError):
            MetadataSuggestion.model_validate({**full, field: None})
        schema = MetadataSuggestion.model_json_schema()
        assert field in schema["required"] and "null" not in str(schema["properties"][field])
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({**full, "title_confidence": "certain"})
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
    assert (await c.get("/api/collections/ex.org/delta?q=p9")).json()["total"] == 0  # decided by the rule, not a delta
    d = (await c.get("/api/collections/ex.org/dump?q=p9")).json()["items"][0]
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
        "https://ex.org/zz",
    )]
    await db.replace_dump("ex.org", dump + extra)
    c.app.state.settings.llm_pattern_batch_urls = 50  # small batches on a small dump
    c.app.state.jobs.s.llm_pattern_batch_urls = 50
    await c.post("/api/collections/ex.org/recompute")
    await c.post("/api/collections/ex.org/suggest/patterns")
    job = await wait_job(c, "ex.org")
    p = job["progress"]
    assert job["state"] == "succeeded", job
    assert p["urls"] == 13 and p["unique"] == 12 and p["calls"] == 1 and p["global"] == 2
    sugs = (await c.get("/api/collections/ex.org/suggestions")).json()
    by = {s["match"]: s for s in sugs}
    # global list hits come first, with match counts over the whole dump (http + https variants)
    assert [s["source"] for s in sugs[:2]] == ["global", "global"]
    assert by["*/login*"]["source"] == "global" and by["*/login*"]["matches"] == 2
    assert by["*/tag/*"]["source"] == "global" and by["*/tag/*"]["matches"] == 1
    assert "*/privacy*" not in by  # in the list, matches nothing here
    # the model's own rows: it also proposed */login* and */tag* (chrome segments) — the global row
    # wins, and a model glob whose URLs the global list already covers is dropped
    assert all(s["type"] == "exclude" for s in sugs)
    assert [s["match"] for s in sugs if s["source"] == "llm"] == ["https://ex.org/zz"]
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
    assert p["tokens_cache_write"] == 0  # counted per job, next to tokens in / out
    assert p["tokens_reasoning"] == 8 * 8 and p["tokens_out"] == 32 * 8  # reasoning is part of out
    jobs_page = (await c.get("/jobs")).text
    assert f"{p['tokens_out']:,} out ({p['tokens_reasoning']:,} reasoning)" in jobs_page
    d = (await c.get("/api/collections/ex.org/delta?q=p2")).json()["items"][0]
    assert d["title_ai"] == "Page 2" and d["document_type_ai"] == "Documentation"
    assert d["title_ai_conf"] == "high" and d["document_type_ai_conf"] == "low" and d["ai_model"] == "fake"
    assert len(d["ai_content_hash"]) == 64
    assert d["title"] is None and d["document_type"] is None  # effective fields untouched
    page = (await c.get("/collections/ex.org?tab=delta")).text
    assert "AI: Page 2" in page and 'class="ai conf-high"' in page and 'class="ai conf-low"' in page
    assert (await c.get("/api/collections/ex.org/delta?q=p2")).json()["total"] == 1
    low = (await c.get("/collections/ex.org?tab=delta&ai=low")).text
    assert "AI: Documentation" in low
    curate = (await c.get("/collections/ex.org?tab=curate")).text
    assert "conf-high" in curate and "8 classified" in curate and "tokens in" in curate
    # accept title → exact-URL pattern; ml cleared
    r = await c.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p2", "field": "title"})
    assert r.status_code == 200
    d = (await c.get("/api/collections/ex.org/delta?q=p2")).json()["items"][0]
    assert d["title"] == "Page 2" and d["title_ai"] is None
    # reject doc type → cleared, effective untouched
    await c.post("/api/collections/ex.org/ai/reject", json={"url": "https://ex.org/p2", "field": "document_type"})
    d = (await c.get("/api/collections/ex.org/delta?q=p2")).json()["items"][0]
    assert d["document_type_ai"] is None and d["document_type"] is None
    assert d["division_ai"] == "Astrophysics" and d["division_ai_conf"] == "low"  # every field is answered, guesses too
    await c.post("/api/collections/ex.org/ai/reject", json={"url": "https://ex.org/p2", "field": "division"})
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
    d = (await c.get("/api/collections/ex.org/delta?q=p2")).json()["items"][0]
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
    assert p["retrying"] == 0
    curate = (await c.get("/collections/ex.org?tab=curate")).text
    assert "7 classified · 1 failed" in curate and "1 URL failed</a> to classify" in curate
    d = (await c.get("/api/collections/ex.org/delta?q=p3")).json()["items"][0]
    assert d["title_ai"] is None and d["ai_failures"] == 1 and d["ai_error"] == "LLMRetryable: 429 too many requests"
    failed = (await c.get("/collections/ex.org?tab=delta&ai=failed")).text
    assert "ex.org/p3" in failed and "ex.org/p2" not in failed and "AI metadata failed" in failed
    # the next run only picks up the one that failed, and a success clears the error
    c.app.state.jobs._llm = FakeProvider()
    assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 202
    job = await wait_job(c, "ex.org")
    assert job["progress"]["classified"] == 1 and job["progress"]["total"] == 1
    d = (await c.get("/api/collections/ex.org/delta?q=p3")).json()["items"][0]
    assert d["title_ai"] and d["ai_error"] is None and d["ai_failures"] == 0


async def test_metadata_retry_pass_recovers_rate_limited_calls(crawler_client):
    c = crawler_client
    await setup(c)
    seen: dict[str, int] = {}

    class OnceBusy(FakeProvider):
        async def complete(self, **kw):
            url = kw["user"].split('"url": "', 1)[1].split('"', 1)[0]
            seen[url] = seen.get(url, 0) + 1
            if url.endswith(("/p3", "/p5")) and seen[url] == 1:
                raise LLMRetryable("429 too many requests")
            return await super().complete(**kw)

    c.app.state.jobs._llm = OnceBusy()
    await c.post("/api/collections/ex.org/suggest/metadata")
    job = await wait_job(c, "ex.org")
    p = job["progress"]
    assert job["state"] == "succeeded" and p["classified"] == 8 and p["failed"] == 0 and p["retrying"] == 0
    assert seen["https://ex.org/p3"] == 2 and seen["https://ex.org/p2"] == 1
    items = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    assert all(d["document_type_ai"] and d["ai_error"] is None for d in items if not d["excluded"])


async def test_rows_without_a_document_type_are_asked_again(crawler_client):
    c = crawler_client
    db = c.app.state.db
    await setup(c)
    await c.post("/api/collections/ex.org/suggest/metadata"); await wait_job(c, "ex.org")
    assert await db.count_deltas_for_llm("ex.org") == 0
    # an answer from before document_type was required: a title, no type, but a confidence for it
    await db.set_delta_ai("ex.org", [{"url": "https://ex.org/p2", "title": "Page 2", "title_conf": "high",
                                      "division": None, "division_conf": "low", "document_type": None,
                                      "document_type_conf": "low", "model": "old"}])
    assert await db.count_deltas_for_llm("ex.org") == 1
    # a type the SME dismissed is not re-asked
    await c.post("/api/collections/ex.org/ai/reject", json={"url": "https://ex.org/p4", "field": "document_type"})
    assert await db.count_deltas_for_llm("ex.org") == 1
    await c.post("/api/collections/ex.org/suggest/metadata")
    job = await wait_job(c, "ex.org")
    assert job["progress"]["classified"] == 1
    d = (await c.get("/api/collections/ex.org/delta?q=p2")).json()["items"][0]
    assert d["document_type_ai"] == "Documentation" and d["ai_model"] == "fake"
    assert await db.count_deltas_for_llm("ex.org") == 0


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
    items = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
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
    await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept"})
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200
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
    d = (await c.get("/api/collections/ex.org/delta?q=p2")).json()["items"][0]
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
    items = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    assert sum(1 for d in items if d["title_ai"]) == 32


async def test_prompts_are_visible(crawler_client):
    c = crawler_client
    r = await c.get("/api/llm/prompts")
    assert r.status_code == 200 and "exclude" in r.json()["patterns"]["system"]
    assert "confidence" in r.json()["metadata"]["system"] and "FULL page text; cut from the end only when" in r.json()["metadata"]["user"]
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
            answer = MetadataSuggestion(title="T", title_confidence="high", division="Earth Science", division_confidence="low", document_type="Data",
                                        document_type_confidence="low")
            msg = SimpleNamespace(parsed=answer, refusal=None, content=None)
            return SimpleNamespace(choices=[SimpleNamespace(message=msg)], model=kw["model"], usage=self.usage)

        usage = None

    async def run(**over):
        p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai", **over))
        p.client = Stub()
        await p.complete(system="s", user="u", schema=MetadataSuggestion)
        return p.client.calls[0]

    assert "temperature" not in await run()
    assert (await run(llm_temperature=0))["temperature"] == 0

    # reasoning effort and service tier go only when set; the output budget always goes
    call = await run()
    assert "reasoning_effort" not in call and "service_tier" not in call
    assert call["max_completion_tokens"] == 4_000
    call = await run(llm_reasoning_effort="low", llm_service_tier="flex", llm_max_completion_tokens=900)
    assert (call["reasoning_effort"], call["service_tier"], call["max_completion_tokens"]) == ("low", "flex", 900)

    # page text is unique per call: only the system prompt may be written to the prompt cache
    call = await run()
    assert call["prompt_cache_options"] == {"mode": "explicit"}
    sys_msg, user_msg = call["messages"]
    assert sys_msg["content"] == [{"type": "text", "text": "s", "prompt_cache_breakpoint": {"mode": "explicit"}}]
    assert user_msg == {"role": "user", "content": "u"}
    call = await run(openai_prompt_cache="provider_default")
    assert "prompt_cache_options" not in call and call["messages"][0]["content"] == "s"


async def test_openai_provider_records_cache_writes_and_alarms_when_page_text_is_written(caplog):
    """Cache writes are billed at 1.25× input on gpt-5.6+: they are counted per call, and a call that
    wrote more than its system prompt (page text in the cache again) logs an error."""
    from types import SimpleNamespace

    from sde_curation.llm.openai import OpenAIProvider

    system = "x" * 8_000  # ~2k tokens, like the real system prompts
    p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai"))

    async def answer(written):
        usage = SimpleNamespace(prompt_tokens=50_000, completion_tokens=10, prompt_tokens_details=SimpleNamespace(
            cached_tokens=0, cache_write_tokens=written))
        msg = SimpleNamespace(parsed=MetadataSuggestion(
            title="T", title_confidence="high", division="Earth Science", division_confidence="low",
            document_type="Data", document_type_confidence="low"), refusal=None, content=None)

        async def parse(**kw):
            return SimpleNamespace(choices=[SimpleNamespace(message=msg)], model=kw["model"], usage=usage)
        p.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse)))
        return await p.complete(system=system, user="u", schema=MetadataSuggestion)

    caplog.clear()
    assert (await answer(1_848)).tokens_cache_write == 1_848  # the system prompt: expected
    assert not [r for r in caplog.records if r.levelname == "ERROR"]
    assert (await answer(48_000)).tokens_cache_write == 48_000  # the page too: alarm
    assert any("wrote 48000 prompt tokens to the cache" in r.getMessage() for r in caplog.records)


async def test_metadata_job_resumes_after_an_engine_restart(tmp_path):
    """A deploy restarts the engine under a Suggest-metadata job that runs for hours on a big
    collection. The shutdown cancels it; the next start carries on with the URLs still missing
    (answers are saved as they arrive, so nothing is asked twice). A curator's own cancel is final."""
    import sys
    from pathlib import Path

    from httpx import ASGITransport, AsyncClient

    from sde_curation.web.app import create_app
    from tests.conftest import FAKE_RUN_PY

    root = tmp_path / "crawler"; root.mkdir(); (root / "run.py").write_text(FAKE_RUN_PY)

    def engine():
        return create_app(Settings(data_dir=tmp_path / "data", crawler_root=root, crawler_python=Path(sys.executable),
                                   scrape_poll_interval_s=0.05, llm_provider="fake", llm_retry_delay_s=0, llm_workers=2))

    class Slow(FakeProvider):
        async def complete(self, **kw):
            await asyncio.sleep(0.08)
            return await super().complete(**kw)

    app = engine()
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await setup(c, n=40)  # 32 docs
        app.state.jobs._llm = Slow()
        assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 202
        await asyncio.sleep(0.5)
    # ← the engine went down with the job running

    app = engine()
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        jobs = (await c.get("/api/collections/ex.org/jobs")).json()
        assert [j["kind"] for j in jobs[:2]] == ["llm_metadata", "llm_metadata"]
        assert jobs[1]["state"] == "failed" and jobs[1]["error"] == "cancelled by shutdown"
        job = await wait_job(c, "ex.org", timeout=30)
        done_before = 32 - job["progress"]["total"]
        assert job["state"] == "succeeded" and job["progress"]["resumed"] == 1 and 2 <= done_before < 32
        items = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
        assert all(d["title_ai"] for d in items)
        # a job the curator cancelled stays cancelled across a restart
        app.state.jobs._llm = Slow()
        await c.post("/api/collections/ex.org/suggest/metadata?all=true")
        await asyncio.sleep(0.3)
        assert (await c.post("/api/collections/ex.org/jobs/cancel")).status_code == 200

    app = engine()
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await asyncio.sleep(0.2)
        jobs = (await c.get("/api/collections/ex.org/jobs")).json()
        assert jobs[0]["state"] == "failed" and "cancelled by" in jobs[0]["error"] and len(jobs) == 4  # scrape + 3


async def test_openai_provider_asks_again_when_the_output_budget_runs_out(caplog):
    """A reasoning model can spend the whole `max_completion_tokens` thinking and never write the
    JSON (the SDK raises LengthFinishReasonError, seen live on gpt-5.6-luna 2026-09-25). The call is
    asked once more with 4× the budget; both calls are billed, so both count. If that is cut too,
    the page fails with an LLMError (recorded on its row) instead of an uncaught SDK error."""
    from types import SimpleNamespace

    from openai import LengthFinishReasonError

    from sde_curation.llm.base import LLMError
    from sde_curation.llm.openai import OpenAIProvider

    def usage(out, reasoning):
        return SimpleNamespace(prompt_tokens=3_000, completion_tokens=out,
                               prompt_tokens_details=SimpleNamespace(cached_tokens=1_873, cache_write_tokens=0),
                               completion_tokens_details=SimpleNamespace(reasoning_tokens=reasoning))

    def provider(cut_calls):
        budgets = []

        async def parse(**kw):
            budgets.append(kw["max_completion_tokens"])
            if len(budgets) <= cut_calls:
                raise LengthFinishReasonError(completion=SimpleNamespace(usage=usage(kw["max_completion_tokens"],
                                                                                     kw["max_completion_tokens"])))
            msg = SimpleNamespace(parsed=MetadataSuggestion(
                title="T", title_confidence="high", division="Earth Science", division_confidence="low",
                document_type="Data", document_type_confidence="low"), refusal=None, content=None)
            return SimpleNamespace(choices=[SimpleNamespace(message=msg)], model=kw["model"], usage=usage(90, 40))

        p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai", llm_max_completion_tokens=500))
        p.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse)))
        return p, budgets

    p, budgets = provider(cut_calls=1)
    done = await p.complete(system="s", user="u", schema=MetadataSuggestion)
    assert done.parsed.title == "T" and budgets == [500, 2_000]
    assert (done.tokens_in, done.tokens_out, done.tokens_reasoning, done.tokens_cached) == (6_000, 590, 540, 3_746)
    assert any("output cut at 500 of 500 tokens" in r.getMessage() for r in caplog.records)

    p, budgets = provider(cut_calls=2)
    with pytest.raises(LLMError, match="did not fit in 2,000 output tokens"):
        await p.complete(system="s", user="u", schema=MetadataSuggestion)
    assert budgets == [500, 2_000]


@pytest.mark.parametrize("param", ["prompt_cache_options", "prompt_cache_breakpoint"])
async def test_openai_provider_drops_cache_options_on_a_model_that_rejects_them(param):
    """Models before gpt-5.6 answer 400 to the cache options (they have no cache-write charge):
    the call is re-sent without them, and that model is never sent them again. gpt-5-nano names
    `prompt_cache_breakpoint` in its 400 (live, 2026-09-25)."""
    from types import SimpleNamespace

    import httpx
    from openai import BadRequestError

    from sde_curation.llm.openai import OpenAIProvider

    calls = []

    async def parse(**kw):
        calls.append(kw)
        if "prompt_cache_options" in kw:
            raise BadRequestError(f"{param} is not supported on this model",
                                  response=httpx.Response(400, request=httpx.Request("POST", "https://x")),
                                  body={"message": "not supported", "param": param})
        msg = SimpleNamespace(parsed=MetadataSuggestion(
            title="T", title_confidence="high", division="Earth Science", division_confidence="low",
            document_type="Data", document_type_confidence="low"), refusal=None, content=None)
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)], model=kw["model"], usage=None)

    p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai", openai_model="gpt-5-nano"))
    p.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse)))
    assert (await p.complete(system="s", user="u", schema=MetadataSuggestion)).parsed.title == "T"
    assert ["prompt_cache_options" in c for c in calls] == [True, False]
    assert calls[1]["messages"][0] == {"role": "system", "content": "s"}
    await p.complete(system="s", user="u", schema=MetadataSuggestion)
    assert len(calls) == 3 and "prompt_cache_options" not in calls[2]  # remembered: one call, no 400


async def test_openai_provider_reports_a_too_long_prompt_with_its_token_counts():
    from types import SimpleNamespace

    import httpx
    from openai import BadRequestError

    from sde_curation.llm.base import LLMInputTooLong
    from sde_curation.llm.openai import OpenAIProvider

    async def parse(**kw):
        raise BadRequestError(
            "Error code: 400 - Input tokens exceed the configured limit of 272000 tokens. Your messages"
            " resulted in 300010 tokens. Please reduce the length of the messages.",
            response=httpx.Response(400, request=httpx.Request("POST", "https://x")),
            body={"message": "…", "param": "messages", "code": "context_length_exceeded"})

    p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai", openai_prompt_cache="provider_default"))
    p.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse)))
    with pytest.raises(LLMInputTooLong) as e:
        await p.complete(system="s", user="u", schema=MetadataSuggestion)
    assert (e.value.limit, e.value.got) == (272_000, 300_010)


async def test_metadata_job_classifies_a_page_too_long_for_the_model(tmp_path):
    """End to end: a crawl with one ~360K-token page (max_pages=14, see FAKE_RUN_PY). Every page is
    classified, none fails, and the long one went to the model cut to fit."""
    from httpx import ASGITransport, AsyncClient

    from tests.conftest import _crawler_app
    app = _crawler_app(tmp_path)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await setup(c, n=14)
        fake = app.state.jobs._llm = FakeProvider()
        assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 202
        job = await wait_job(c, "ex.org")
        assert job["state"] == "succeeded" and job["progress"]["failed"] == 0, job
        assert job["progress"]["classified"] == job["progress"]["total"] == 12
        long = [call for call in fake.calls if '"url": "https://ex.org/p1"' in call["user"]]
        assert len(long) == 1 and '"text_cut": true' in long[0]["user"]
        assert _prompt_tokens(long[0]) <= app.state.jobs.s.llm_max_input_tokens
        assert all('"text_cut"' not in call["user"] for call in fake.calls if call not in long)
