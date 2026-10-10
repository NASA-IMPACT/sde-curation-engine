"""Helpers for the end-to-end journeys (tests/e2e/test_journeys.py): the curator's steps through the
API, a check that every page of a collection renders, and the prod index wiring (the test run's
vectors in S3, an in-memory prod index, direct validation against it).

The fake crawler (tests/support/crawler.py) crawls p1..pN under the seed; every fifth page fails
(p5 with a 404, later ones with a 403). So a crawl of CRAWL_PAGES gives CRAWL_DOCUMENTS documents.
"""

from __future__ import annotations

import json

from sde_curation.backends.publish import ProdPublisher, to_web_document
from sde_curation.backends.s3 import S3
from sde_curation.backends.validate import compare, web_id
from tests.support.fake_aoss import FakeAoss
from tests.support.flows import wait_job

CRAWL_PAGES = 10
CRAWL_DOCUMENTS = 8  # p5 and p10 fail
TABS = ("overview", "dump", "delta", "curated", "rules", "curate", "activity")
PROD_ENDPOINT = "https://prod.example.aoss.amazonaws.com"


def url(cid: str, page: int) -> str:
    return f"https://{cid}/p{page}"


async def collection(c, cid: str) -> dict:
    return (await c.get(f"/api/collections/{cid}")).json()


async def every_page_renders(c, cid: str, step: str) -> None:
    """Each tab of the collection page, its tab body, the header and the stepper answer 200."""
    for tab in TABS:
        r = await c.get(f"/collections/{cid}?tab={tab}")
        assert r.status_code == 200, f"[{step}] the {tab} tab answered {r.status_code}"
        r = await c.get(f"/collections/{cid}/tab-body?tab={tab}")
        assert r.status_code == 200, f"[{step}] the {tab} tab body answered {r.status_code}"
    for part in ("header", "pipeline"):
        r = await c.get(f"/collections/{cid}/{part}")
        assert r.status_code == 200, f"[{step}] the {part} answered {r.status_code}"
    assert (await c.get("/")).status_code == 200, f"[{step}] the dashboard did not render"
    await counts_match_tables(c, cid, step)


async def counts_match_tables(c, cid: str, step: str) -> None:
    """Every count the pages were given (stored between changes in collection_stats) equals the same
    count taken straight from the tables."""
    from sde_curation.db import db_scope

    db = c.app.state.db
    rows = await db.fetch("SELECT version, computed_version, counts FROM collection_stats WHERE collection_id=%s",
                          (cid,))
    if not rows or rows[0]["computed_version"] != rows[0]["version"]:
        return  # nothing stored for the current version: the pages counted from the tables
    token = db_scope.set("work")  # count straight from the tables, as a job would
    try:
        for key, stored in rows[0]["counts"].items():
            name, rest = key[:key.index("[")], key[key.index("["):]
            args, kwargs = json.loads(rest)
            fresh = json.loads(json.dumps(await getattr(db, name)(cid, *args, **dict(kwargs))))
            assert fresh == stored, f"[{step}] the pages show {name} = {stored}, the tables say {fresh}"
    finally:
        db_scope.reset(token)


async def job_succeeds(c, cid: str, step: str, *, timeout: float = 40) -> dict:
    job = await wait_job(c, cid, timeout=timeout)
    assert job["state"] == "succeeded", f"[{step}] the job ended {job['state']}: {job.get('error')}"
    return job


def wire_prod(c, monkeypatch, test_run: str, key: str, prod: FakeAoss | None = None) -> FakeAoss:
    """Index to prod against an in-memory prod index: put the test run's vectors where the publisher
    reads them, give the engine a publisher writing into `FakeAoss`, and validate against it."""
    import sde_curation.jobs as jobs_mod

    settings = c.app.state.settings
    settings.opensearch_endpoint_prod = PROD_ENDPOINT
    prefix = f"curated_collections/{key}/{test_run}"
    manifest = json.loads(c.s3.get_object(Bucket="cosmos-idx", Key=f"{prefix}/manifest.json")["Body"].read())
    lines = [json.loads(x) for x in
             c.s3.get_object(Bucket="cosmos-idx", Key=f"{prefix}/documents.jsonl")["Body"].read().splitlines()]
    c.s3.put_object(Bucket="cosmos-idx", Key=f"vectorized/{key}/{test_run}/batch_0001.jsonl", Body="\n".join(
        json.dumps({**to_web_document(ln, manifest), "vectorized_title": [1], "vectorized_full_text": []})
        for ln in lines).encode())
    prod = prod if prod is not None else FakeAoss()
    c.app.state.jobs._publisher = lambda: ProdPublisher(settings, s3=S3("cosmos-idx", client=c.s3), prod=prod)

    validate_test = getattr(jobs_mod.validate_direct, "validates_test_with", jobs_mod.validate_direct)

    async def prod_direct(settings, *, collection_key, run_id, target, expected_titles, client=None):
        if target != "prod":  # a test run is validated the way it always is
            return await validate_test(settings, collection_key=collection_key, run_id=run_id, target=target,
                                       expected_titles=expected_titles, client=client)
        hits = prod.search("sde-web", {"size": 10_000})["hits"]["hits"]
        indexed = {h["_source"]["id"]: h["_source"]["title"] or "" for h in hits}
        return compare(collection_key, run_id, {web_id(collection_key, u): t for u, t in expected_titles.items()},
                       indexed)

    prod_direct.validates_test_with = validate_test
    monkeypatch.setattr(jobs_mod, "validate_direct", prod_direct)
    return prod


async def index_to_live(c, monkeypatch, cid: str, key: str, step: str, prod: FakeAoss | None = None) -> FakeAoss:
    """Index to test (validated), then to prod (validated): the collection is live."""
    r = await c.post(f"/api/collections/{cid}/index?target=test")
    assert r.status_code == 202, f"[{step}: index to test] {r.text}"
    await job_succeeds(c, cid, f"{step}: index to test")
    test_run = (await c.get(f"/api/collections/{cid}/index_runs")).json()[0]["run_id"]
    prod = wire_prod(c, monkeypatch, test_run, key, prod)
    r = await c.post(f"/api/collections/{cid}/index?target=prod")
    assert r.status_code == 202, f"[{step}: index to prod] {r.text}"
    await job_succeeds(c, cid, f"{step}: index to prod")
    assert (await collection(c, cid))["status"] == "live", f"[{step}] not live"
    return prod


async def to_live(c, monkeypatch, cid: str, key: str, *, pages: int = CRAWL_PAGES) -> FakeAoss:
    """The shortest path to a live collection: create, crawl, Start curating, Suggest metadata and
    accept all, promote, index to test and to prod."""
    c.app.state.settings.validation_delay_s = 0.1
    r = await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": pages,
                                               "division": "Heliophysics"})
    assert r.status_code == 201, f"[to live: create] {r.text}"
    await c.post(f"/api/collections/{cid}/scrape")
    await job_succeeds(c, cid, "to live: crawl", timeout=30)
    assert (await c.post(f"/api/collections/{cid}/recompute")).status_code == 200, "[to live: Start curating]"
    await c.post(f"/api/collections/{cid}/suggest/metadata")
    await job_succeeds(c, cid, "to live: Suggest metadata")
    assert (await c.post(f"/api/collections/{cid}/ai/bulk", json={"decision": "accept"})).status_code == 200
    r = await c.post(f"/api/collections/{cid}/promote")
    assert r.status_code == 200, f"[to live: promote] {r.text}"
    return await index_to_live(c, monkeypatch, cid, key, "to live")

