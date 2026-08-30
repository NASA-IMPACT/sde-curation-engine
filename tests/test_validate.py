"""Phase 6: direct validation, 403 → second-pass fallback, gate → prod/live, notifications."""

import asyncio

import pytest

from sde_curation.backends.validate import NoIndexAccess, compare, validate_direct, web_id
from sde_curation.config import Settings
from sde_curation.models import IndexRun
from sde_curation.notify import Notifier
from tests.conftest import prepare, wait_job


def test_compare_mirrors_indexer_report():
    exp = {web_id("k", "https://x/a"): "A", web_id("k", "https://x/b"): "B", web_id("k", "https://x/c"): "C"}
    idx = {web_id("k", "https://x/a"): "A", web_id("k", "https://x/b"): "B!", web_id("k", "https://x/z"): "Z"}
    r = compare("k", "r", exp, idx)
    assert r["expected_count"] == 3 and r["indexed_count"] == 3 and r["count_matches"] is True
    assert r["titles_missing_in_index"] == ["C"] and r["titles_only_in_index"] == ["Z"]
    assert r["titles_mismatched"][0]["exported"] == "B" and r["title_match_rate"] == round(1 / 3, 6)
    run = IndexRun(run_id="r", collection_id="k", target="test", validation=r)
    assert run.validation_passes(0.99) is False and run.validation_passes(0.3) is True


async def test_validate_direct_uses_client_and_maps_403():
    class Client:
        def search(self, index, body):
            assert index == "sde-web-subset" and body["query"]["bool"]["filter"][0]["term"]["collection_key"] == "k"
            return {"hits": {"hits": [{"_source": {"id": web_id("k", "https://x/a"), "title": "A"}, "sort": [1]}]}}

    s = Settings(opensearch_endpoint_test="https://e.example", llm_provider="fake")
    r = await validate_direct(s, collection_key="k", run_id="r", target="test", expected_titles={"https://x/a": "A"}, client=Client())
    assert r["count_matches"] and r["title_match_rate"] == 1.0

    from opensearchpy.exceptions import AuthorizationException

    class Denied:
        def search(self, index, body):
            raise AuthorizationException(403, "security_exception", "Bad Authorization")

    with pytest.raises(NoIndexAccess, match="no AOSS data access"):
        await validate_direct(s, collection_key="k", run_id="r", target="test", expected_titles={}, client=Denied())
    with pytest.raises(NoIndexAccess, match="OPENSEARCH_ENDPOINT_TEST"):
        await validate_direct(Settings(llm_provider="fake"), collection_key="k", run_id="r", target="test", expected_titles={})


async def test_notifier_posts_and_never_raises():
    calls = []

    async def post(url, payload):
        calls.append((url, payload))
        raise RuntimeError("slack down")

    n = Notifier("https://hook", post=post, base_url="https://engine")
    await n.status_changed("ex.org", "curated", "live", "prod run r1")
    assert calls[0][0] == "https://hook" and "*live*" in calls[0][1]["text"] and "/collections/ex.org" in calls[0][1]["text"]
    assert n.sent[0]["new_status"] == "live"
    assert (await Notifier(None).status_changed("x", None, "backlog", None)) is None


# ── through the app (fake indexer writes 0/N first pass, N/N second pass) ──


async def test_gate_falls_back_to_second_pass_then_prod(index_client, monkeypatch):
    c = index_client
    c.app.state.settings.validation_delay_s = 0.1
    await prepare(c)
    notes = c.app.state.notifier.sent
    r = await c.post("/api/collections/ex.org/index?target=test")
    assert r.status_code == 202
    job = await wait_job(c, "ex.org", timeout=40)
    assert job["state"] == "succeeded", job
    # no endpoint configured → direct validation unavailable → second pass produced a fresh validation
    assert job["progress"]["fallback"] == "second_pass" and job["progress"]["validated_by"] == "second_pass"
    assert job["progress"]["validation_ok"] is True
    runs = (await c.get("/api/collections/ex.org/index_runs")).json()
    assert len(runs) == 1 and runs[0]["validated_by"] == "second_pass" and runs[0]["validation"]["count_matches"] is True
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["status"] == "config_generated" and col["needs_recuration"] is False
    # header/dashboard now offer prod
    assert "Index to prod" in (await c.get("/collections/ex.org/header")).text
    page = (await c.get("/collections/ex.org?tab=overview&step=live")).text
    assert "Index to prod" in page and "via second_pass" in (await c.get("/collections/ex.org?tab=overview&step=config_generated")).text
    # prod
    r = await c.post("/api/collections/ex.org/index?target=prod")
    assert r.status_code == 202, r.text
    job = await wait_job(c, "ex.org", timeout=40)
    assert job["state"] == "succeeded" and job["kind"] == "index_prod"
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["status"] == "live"
    runs = (await c.get("/api/collections/ex.org/index_runs")).json()
    assert runs[0]["target"] == "prod" and runs[0]["state"] == "succeeded"
    # notifications fired for each transition
    assert [n["new_status"] for n in notes][-2:] == ["config_generated", "live"] or "live" in [n["new_status"] for n in notes]
    assert "Live ✓" in (await c.get("/collections/ex.org/header")).text


async def test_gate_failure_sends_back_to_curating(index_client, monkeypatch):
    c = index_client
    c.app.state.settings.validation_delay_s = 0.1
    await prepare(c, "half.org")  # fake indexer's second pass reports half the docs for half.org
    await c.post("/api/collections/half.org/index?target=test")
    job = await wait_job(c, "half.org", timeout=40)
    assert job["state"] == "succeeded" and job["progress"]["validation_ok"] is False
    col = (await c.get("/api/collections/half.org")).json()
    assert col["status"] == "curating" and col["needs_recuration"] is True
    hist = (await c.get("/api/collections/half.org/history")).json()
    assert "validation FAILED" in hist[-1]["note"]
    assert (await c.post("/api/collections/half.org/index?target=prod")).status_code == 409


async def test_revalidate_direct_when_access_exists(index_client, monkeypatch):
    c = index_client
    c.app.state.settings.validation_delay_s = 0.1
    await prepare(c)
    await c.post("/api/collections/ex.org/index?target=test"); await wait_job(c, "ex.org", timeout=40)
    # now simulate AOSS access: patch validate_direct to return a passing report without a second pass
    import sde_curation.jobs as jobs_mod

    async def fake_direct(settings, *, collection_key, run_id, target, expected_titles, client=None):
        return compare(collection_key, run_id, {web_id(collection_key, u): t for u, t in expected_titles.items()},
                       {web_id(collection_key, u): t for u, t in expected_titles.items()})

    monkeypatch.setattr(jobs_mod, "validate_direct", fake_direct)
    r = await c.post("/api/collections/ex.org/index/revalidate")
    assert r.status_code == 202
    job = await wait_job(c, "ex.org", timeout=20)
    assert job["state"] == "succeeded" and job["kind"] == "validate" and job["progress"]["validated_by"] == "direct"
    assert "fallback" not in job["progress"]
    runs = (await c.get("/api/collections/ex.org/index_runs")).json()
    assert runs[0]["validated_by"] == "direct" and runs[0]["validation"]["title_match_rate"] == 1.0
    assert (await c.post("/api/collections/nope/index/revalidate")).status_code == 404
    await asyncio.sleep(0)
