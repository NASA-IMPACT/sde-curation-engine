"""Phase 6: direct validation, 403 → second-pass fallback, gate → prod/live, notifications."""

import asyncio
import json

import pytest

from sde_curation.backends.publish import ProdPublisher, to_web_document
from sde_curation.backends.s3 import S3
from sde_curation.backends.validate import NoIndexAccess, compare, validate_direct, web_id
from sde_curation.config import Settings
from sde_curation.models import IndexRun
from sde_curation.notify import Notifier
from tests.conftest import prepare, wait_job
from tests.fake_aoss import FakeAoss


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
    # prod: publishes the test run's vectors (S3 vectorized/) into the prod index — no indexer task
    settings = c.app.state.settings
    assert (await c.post("/api/collections/ex.org/index?target=prod")).status_code == 409  # no prod endpoint
    settings.opensearch_endpoint_prod = "https://prod.example.aoss.amazonaws.com"
    test_run = runs[0]["run_id"]
    prefix = f"curated_collections/ex.org/{test_run}"
    manifest = json.loads(c.s3.get_object(Bucket="cosmos-idx", Key=f"{prefix}/manifest.json")["Body"].read())
    lines = [json.loads(x) for x in c.s3.get_object(Bucket="cosmos-idx", Key=f"{prefix}/documents.jsonl")["Body"].read().splitlines()]
    c.s3.put_object(Bucket="cosmos-idx", Key=f"vectorized/ex.org/{test_run}/batch_0001.jsonl", Body="\n".join(
        json.dumps({**to_web_document(ln, manifest), "vectorized_title": [1], "vectorized_full_text": []}) for ln in lines).encode())
    prod = FakeAoss()
    c.app.state.jobs._publisher = lambda: ProdPublisher(settings, s3=S3("cosmos-idx", client=c.s3), prod=prod)
    import sde_curation.jobs as jobs_mod

    calls = []

    async def prod_direct(settings, *, collection_key, run_id, target, expected_titles, client=None):
        assert target == "prod"
        calls.append(1)  # AOSS lag: the first two checks see nothing yet
        hits = prod.search("sde-web", {"size": 10_000})["hits"]["hits"] if len(calls) > 2 else []
        indexed = {h["_source"]["id"]: h["_source"]["title"] or "" for h in hits}
        return compare(collection_key, run_id, {web_id(collection_key, u): t for u, t in expected_titles.items()}, indexed)

    monkeypatch.setattr(jobs_mod, "validate_direct", prod_direct)
    r = await c.post("/api/collections/ex.org/index?target=prod")
    assert r.status_code == 202, r.text
    job = await wait_job(c, "ex.org", timeout=40)
    assert job["state"] == "succeeded" and job["kind"] == "index_prod", job
    assert job["progress"]["status"]["from_vectorized"] == len(lines) and job["progress"]["validation_ok"] is True
    assert job["progress"]["validation_attempt"] == 3 and job["progress"]["indexed_so_far"] == len(lines)
    assert len(prod.store) == len(lines)
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["status"] == "live" and col["needs_recuration"] is False
    runs = (await c.get("/api/collections/ex.org/index_runs")).json()
    assert runs[0]["target"] == "prod" and runs[0]["state"] == "succeeded" and runs[0]["external_ref"] == f"publish:{test_run}"
    live = (await c.get("/collections/ex.org?tab=overview&step=live")).text
    assert f"from test run {test_run}" in live and f"{len(lines)} from S3 vectors" in live
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


async def test_direct_validation_polls_until_index_is_consistent(index_client, monkeypatch):
    """AOSS makes a bulk upsert searchable some time after the indexer succeeds: a short count on the
    first check must be re-checked, not failed."""
    c = index_client
    s = c.app.state.settings
    s.validation_delay_s, s.validation_poll_interval_s, s.validation_timeout_s = 0.05, 0.05, 5.0
    await prepare(c)
    import sde_curation.jobs as jobs_mod

    calls = []

    async def lagging_direct(settings, *, collection_key, run_id, target, expected_titles, client=None):
        calls.append(1)
        exp = {web_id(collection_key, u): t for u, t in expected_titles.items()}
        visible = dict(list(exp.items())[: len(exp) // 2]) if len(calls) < 3 else exp  # 2 short reads, then all
        return compare(collection_key, run_id, exp, visible)

    monkeypatch.setattr(jobs_mod, "validate_direct", lagging_direct)
    await c.post("/api/collections/ex.org/index?target=test")
    job = await wait_job(c, "ex.org", timeout=40)
    assert job["state"] == "succeeded", job
    assert len(calls) == 3 and job["progress"]["validation_attempt"] == 3
    assert job["progress"]["validated_by"] == "direct" and job["progress"]["validation_ok"] is True and "fallback" not in job["progress"]
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["status"] == "config_generated" and col["needs_recuration"] is False
    hist = (await c.get("/api/collections/ex.org/history")).json()
    assert "validation FAILED" not in "".join(h["note"] for h in hist)


async def test_direct_validation_fails_only_after_timeout(index_client, monkeypatch):
    c = index_client
    s = c.app.state.settings
    s.validation_delay_s, s.validation_poll_interval_s, s.validation_timeout_s = 0.05, 0.05, 0.3
    await prepare(c)
    import sde_curation.jobs as jobs_mod

    calls = []

    async def never_consistent(settings, *, collection_key, run_id, target, expected_titles, client=None):
        calls.append(1)
        exp = {web_id(collection_key, u): t for u, t in expected_titles.items()}
        return compare(collection_key, run_id, exp, dict(list(exp.items())[: len(exp) // 2]))

    monkeypatch.setattr(jobs_mod, "validate_direct", never_consistent)
    await c.post("/api/collections/ex.org/index?target=test")
    job = await wait_job(c, "ex.org", timeout=40)
    assert job["state"] == "succeeded" and job["progress"]["validation_ok"] is False
    assert len(calls) >= 3  # kept re-checking through the window before giving up
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["status"] == "curating" and col["needs_recuration"] is True
    hist = (await c.get("/api/collections/ex.org/history")).json()
    assert "validation FAILED (direct)" in hist[-1]["note"]
