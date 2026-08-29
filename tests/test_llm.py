"""Phase 4: provider contract, sanity filters, jobs, accept/reject — all offline (fake provider)."""

import asyncio

import pytest

from sde_curation.llm.base import LLMError, make_llm
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import suggest_metadata, suggest_patterns
from sde_curation.models import Collection, Division, MetadataSuggestions, PatternSuggestions
from tests.conftest import wait_job

COLL = Collection(collection_id="ex.org", name="Ex", seed_url="https://ex.org", division=Division.GENERAL,
                  connector="crawler2", max_pages=10)


# ── pure task layer ───────────────────────────────────────────────────


async def test_suggest_patterns_drops_nonmatching_and_dedups():
    canned = {"suggestions": [
        {"type": "exclude", "match": "*/privacy*", "rationale": "chrome"},
        {"type": "exclude", "match": "*/privacy*", "rationale": "dup"},
        {"type": "exclude", "match": "*/nothing-like-this*", "rationale": "hallucinated"},
        {"type": "exclude", "match": "*", "rationale": "too broad"},
        {"type": "division", "match": "https://ex.org/*", "value": "Heliophysics", "rationale": "site is helio"},
    ]}
    kept = await suggest_patterns(FakeProvider(canned), COLL, [{"url": "https://ex.org/privacy"}],
                                  ["https://ex.org/privacy", "https://ex.org/a"])
    assert [(k.type, k.match) for k in kept] == [("exclude", "*/privacy*"), ("division", "https://ex.org/*")]


async def test_suggest_patterns_invalid_value_is_llm_error():
    canned = {"suggestions": [{"type": "division", "match": "*", "value": "Nope", "rationale": "x"}]}
    with pytest.raises(LLMError, match="did not match PatternSuggestions"):
        await suggest_patterns(FakeProvider(canned), COLL, [], ["https://ex.org/a"])


async def test_suggest_metadata_batches_and_filters_unknown_urls():
    fake = FakeProvider()
    docs = [{"url": f"https://ex.org/p{i}", "title": f"Aurora page {i} – Ex", "text": "helio data"} for i in range(45)]
    seen = []
    rows = await suggest_metadata(fake, docs, batch_size=20, on_progress=lambda p: (seen.append(p), asyncio.sleep(0))[1])
    assert len(fake.calls) == 3 and [s["done"] for s in seen] == [20, 40, 45]
    assert len(rows) == 45 and rows[0]["title"] == "Aurora page 0" and rows[0]["division"] == "Heliophysics"
    assert rows[0]["document_type"] == "Data"
    # a suggestion for a url we did not send is dropped
    canned = {"items": [{"url": "https://ex.org/p0", "title": "T"}, {"url": "https://evil.example/x", "title": "E"}]}
    rows = await suggest_metadata(FakeProvider(canned), docs[:1])
    assert [r["url"] for r in rows] == ["https://ex.org/p0"]


async def test_schemas_reject_bad_enums():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        MetadataSuggestions.model_validate({"items": [{"url": "u", "division": "Kitchen"}]})
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
    sugs = (await c.get("/api/collections/ex.org/suggestions")).json()
    assert sugs and all(s["state"] == "pending" for s in sugs)
    title = next(s for s in sugs if s["type"] == "title")
    # nothing applied yet
    assert (await c.get("/api/collections/ex.org/patterns")).json() == []
    page = (await c.get("/collections/ex.org?tab=patterns")).text
    assert "Suggested patterns" in page and "Accept" in page
    # accept → real pattern, recomputed
    r = await c.post(f"/api/collections/ex.org/suggestions/{title['id']}/accept")
    assert r.status_code == 200
    pats = (await c.get("/api/collections/ex.org/patterns")).json()
    assert len(pats) == 1 and pats[0]["type"] == "title" and pats[0]["matches"] == 8
    d = (await c.get("/api/collections/ex.org/deltas?q=p2")).json()["items"][0]
    assert d["title"] == "Page 2 | Ex"
    assert (await c.post(f"/api/collections/ex.org/suggestions/{title['id']}/accept")).status_code == 409  # already decided
    # reject leaves nothing behind
    other = next(s for s in sugs if s["id"] != title["id"]) if len(sugs) > 1 else None
    if other:
        assert (await c.post(f"/api/collections/ex.org/suggestions/{other['id']}/reject")).status_code == 200
        assert len((await c.get("/api/collections/ex.org/patterns")).json()) == 1
    assert (await c.post("/api/collections/ex.org/suggestions/9999/accept")).status_code == 404
    assert (await c.post(f"/api/collections/ex.org/suggestions/{title['id']}/maybe")).status_code == 422


async def test_metadata_suggestions_flow(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 409  # no deltas
    await setup(c)
    assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 202
    job = await wait_job(c, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["classified"] == 8, job
    d = (await c.get("/api/collections/ex.org/deltas?q=p2")).json()["items"][0]
    assert d["title_ai"] == "Page 2" and d["document_type_ai"] == "Documentation"
    assert d["title"] is None and d["document_type"] is None  # effective fields untouched
    page = (await c.get("/collections/ex.org?tab=urls&set=deltas")).text
    assert "AI: Page 2" in page
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
    c.app.state.jobs._llm = FakeProvider(canned={"items": [{"url": "https://ex.org/p2", "division": "Kitchen"}]})
    await c.post("/api/collections/ex.org/suggest/metadata")
    job = await wait_job(c, "ex.org")
    assert job["state"] == "failed" and "did not match" in job["error"]
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
