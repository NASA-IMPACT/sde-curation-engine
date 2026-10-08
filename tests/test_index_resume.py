"""Index-to-test runs carry on across an engine restart (#22, #33), and a cancel stops the indexer.

The engine is shut down and started again in the middle of a run, the way a deploy or a crash does
it. The fake indexer waits for a "go" object in S3 before it writes status.json, so a test controls
when it finishes, and it logs every dispatch, so a test can check the indexer was started once.
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.backends.publish import ProdPublisher, to_web_document
from sde_curation.backends.s3 import S3
from sde_curation.backends.validate import compare, web_id
from sde_curation.config import Settings
from sde_curation.db import Database
from sde_curation.models import IndexRun, JobKind, JobRun, JobState
from sde_curation.web.app import create_app
from tests.conftest import FAKE_INDEXER, FAKE_RUN_PY, prepare, wait_job
from tests.fake_aoss import FakeAoss

# FAKE_INDEXER, gated: the first pass waits for index_runs/<key>/<run>/.go before status.json, and
# every invocation appends "main" or "second" to $DISPATCH_LOG
GATED_INDEXER = FAKE_INDEXER.replace(
    "time.sleep(0.2)\n",
    "time.sleep(0.2)\n"
    "open(os.environ['DISPATCH_LOG'], 'a').write(('second' if (target == 'test' and second) else 'main') + '\\n')\n"
    "if target == 'test' and not second:\n"
    "    for _ in range(600):\n"
    "        try:\n"
    "            s3.head_object(Bucket=bucket, Key=f'index_runs/{key}/{run}/.go'); break\n"
    "        except Exception:\n"
    "            time.sleep(0.05)\n",
)
assert GATED_INDEXER != FAKE_INDEXER


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A moto S3 server, the fake crawler, the gated indexer, and a factory for engines on them."""
    import boto3
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0); server.start()
    endpoint = f"http://127.0.0.1:{server._server.socket.getsockname()[1]}"
    for k, v in {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_DEFAULT_REGION": "us-east-1",
                 "MOTO_ENDPOINT": endpoint, "AWS_ENDPOINT_URL": endpoint,
                 "DISPATCH_LOG": str(tmp_path / "dispatches.log")}.items():
        monkeypatch.setenv(k, v)
    s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=endpoint)
    s3.create_bucket(Bucket="cosmos-idx")
    croot = tmp_path / "crawler"; croot.mkdir(); (croot / "run.py").write_text(FAKE_RUN_PY)
    iroot = tmp_path / "indexer"; iroot.mkdir(); (iroot / "api_scraper.py").write_text(GATED_INDEXER)
    base = {
        "data_dir": tmp_path / "data", "crawler_root": croot, "crawler_python": Path(sys.executable),
        "indexer_root": iroot, "indexer_python": Path(sys.executable), "cosmos_index_bucket": "cosmos-idx",
        "index_poll_interval_s": 0.1, "index_stall_timeout_s": 60, "scrape_poll_interval_s": 0.05,
        "llm_provider": "fake", "validation_delay_s": 0.1, "validation_poll_interval_s": 0.05,
        "validation_timeout_s": 1.0, "resume_start_delay_s": 0, "resume_stagger_s": 0,
    }

    class Env:
        def settings(self, **over):
            return Settings(**{**base, **over})

        def engine(self, **over):
            app = create_app(self.settings(**over))
            return app, AsyncClient(transport=ASGITransport(app=app), base_url="http://t")

        def go(self, run_id: str):
            s3.put_object(Bucket="cosmos-idx", Key=f"index_runs/ex_org/{run_id}/.go", Body=b"1")

        def dispatches(self) -> list[str]:
            p = tmp_path / "dispatches.log"
            return p.read_text().split() if p.exists() else []

        s3_client = s3

    yield Env()
    server.stop()


async def _until(c, pred, timeout=20.0):
    for _ in range(int(timeout / 0.05)):
        job = (await c.get("/api/collections/ex.org/jobs")).json()[0]
        if pred(job):
            return job
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out; job is {job}")


async def _db_state(settings, job_id: int, run_id: str) -> tuple[JobRun, IndexRun]:
    db = await Database(settings.resolved_database_url).connect()
    try:
        return await db.get_job(job_id), await db.get_index_run(run_id)
    finally:
        await db.close()


async def test_a_restart_while_the_indexer_runs_follows_the_same_task_to_the_end(env):
    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        await prepare(c)
        r = await c.post("/api/collections/ex.org/index?target=test")
        assert r.status_code == 202, r.text
        job_id, run_id = r.json()["id"], r.json()["run_id"]
        await _until(c, lambda j: j["progress"].get("phase") == "indexing")
    # the engine is gone; the job and its run are still running, the indexer task too
    job, run = await _db_state(env.settings(), job_id, run_id)
    assert job.state is JobState.RUNNING and run.state == "running" and run.external_ref
    env.go(run_id)  # the indexer finishes while no engine watches it

    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        job = await wait_job(c, "ex.org", timeout=30)
        assert job["id"] == job_id and job["state"] == "succeeded", job
        assert job["progress"]["restarts"] == 1 and job["progress"]["validation"]["count_matches"] is True
        run = (await c.get("/api/collections/ex.org/index_runs")).json()[0]
        assert run["run_id"] == run_id and run["state"] == "succeeded"
        assert (await c.get("/api/collections/ex.org")).json()["status"] == "config_generated"
    assert env.dispatches() == ["main", "second"]  # dispatched once; the second pass is the validation's


async def test_a_restart_during_validation_validates_again(env):
    app, c = env.engine(validation_delay_s=5)
    async with app.router.lifespan_context(app), c:
        await prepare(c)
        r = await c.post("/api/collections/ex.org/index?target=test")
        job_id, run_id = r.json()["id"], r.json()["run_id"]
        env.go(run_id)
        await _until(c, lambda j: j["progress"].get("phase") == "validating")
    job, run = await _db_state(env.settings(), job_id, run_id)
    assert job.state is JobState.RUNNING and run.state == "succeeded"

    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        job = await wait_job(c, "ex.org", timeout=30)
        assert job["id"] == job_id and job["state"] == "succeeded", job
        assert job["progress"]["validation_ok"] is True
    assert env.dispatches() == ["main", "second"]


async def test_a_restart_during_the_export_exports_again_with_the_same_run(env, monkeypatch):
    """#33: no dispatch yet, so the run starts over from the export, under the same run id; the
    files are overwritten and the manifest is still written last."""
    original = Database.iter_curated_for_export
    hold = asyncio.Event()

    async def slow_export(self, collection_id, chunk=500):
        await hold.wait()  # the engine goes down in the middle of the export
        async for rows in original(self, collection_id, chunk):
            yield rows

    monkeypatch.setattr(Database, "iter_curated_for_export", slow_export)
    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        await prepare(c)
        r = await c.post("/api/collections/ex.org/index?target=test")
        job_id, run_id = r.json()["id"], r.json()["run_id"]
        await asyncio.sleep(0.3)
    job, run = await _db_state(env.settings(), job_id, run_id)
    assert job.state is JobState.RUNNING and run.state == "running" and run.external_ref is None
    monkeypatch.setattr(Database, "iter_curated_for_export", original)

    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        await _until(c, lambda j: j["progress"].get("phase") == "indexing")
        env.go(run_id)
        job = await wait_job(c, "ex.org", timeout=30)
        assert job["id"] == job_id and job["state"] == "succeeded", job
        assert job["progress"]["exported"] == 7
    prefix = f"curated_collections/ex_org/{run_id}/"
    keys = sorted(o["Key"] for o in env.s3_client.list_objects_v2(Bucket="cosmos-idx", Prefix=prefix)["Contents"])
    assert keys == [prefix + "documents.jsonl", prefix + "manifest.json"]
    manifest = json.loads(env.s3_client.get_object(Bucket="cosmos-idx", Key=prefix + "manifest.json")["Body"].read())
    docs = env.s3_client.get_object(Bucket="cosmos-idx", Key=prefix + "documents.jsonl")["Body"].read().splitlines()
    assert manifest["document_count"] == len(docs) == 7
    assert env.dispatches() == ["main", "second"]


async def test_a_cancel_stops_the_indexer_and_closes_the_run(env, monkeypatch):
    from sde_curation.backends.index import LocalSubprocessIndexer
    killed = []
    original = LocalSubprocessIndexer.kill

    async def kill(self, d):
        killed.append(d.external_ref)
        await original(self, d)

    monkeypatch.setattr(LocalSubprocessIndexer, "kill", kill)
    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        await prepare(c)
        r = await c.post("/api/collections/ex.org/index?target=test")
        run_id = r.json()["run_id"]
        await _until(c, lambda j: j["progress"].get("phase") == "indexing")
        assert (await c.post("/api/collections/ex.org/jobs/cancel")).status_code == 200
        job = (await c.get("/api/collections/ex.org/jobs")).json()[0]
        run = (await c.get("/api/collections/ex.org/index_runs")).json()[0]
    assert job["state"] == "failed" and job["error"].startswith("cancelled by")
    assert run["run_id"] == run_id and run["state"] == "failed" and run["error"] == job["error"]
    assert len(killed) == 1


async def test_index_runs_left_running_by_a_job_that_does_not_resume_are_closed(env, monkeypatch):
    from sde_curation.jobs import JobManager
    monkeypatch.setattr(JobManager, "_resumers", lambda self: {})  # this job will not resume
    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "ex.org", "max_pages": 5})
        db = app.state.db
        job = await db.insert_job(JobRun(collection_id="ex.org", kind=JobKind.INDEX_PROD, state=JobState.RUNNING,
                                         run_id="r-prod-1"))
        await db.insert_index_run(IndexRun(run_id="r-prod-1", collection_id="ex.org", target="prod"))
    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        got_job = await app.state.db.get_job(job.id)
        got_run = await app.state.db.get_index_run("r-prod-1")
    assert got_job.state is JobState.FAILED
    assert got_run.state == "failed" and got_run.error == "engine restarted" and got_run.finished_at


async def test_a_revalidation_interrupted_by_a_restart_checks_the_same_run_again(env):
    """#32: a validate job carries on as the same job and records one report on the run."""
    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        await prepare(c)
        r = await c.post("/api/collections/ex.org/index?target=test")
        run_id = r.json()["run_id"]
        env.go(run_id)
        assert (await wait_job(c, "ex.org", timeout=30))["state"] == "succeeded"
    app, c = env.engine(validation_delay_s=5)
    async with app.router.lifespan_context(app), c:
        r = await c.post("/api/collections/ex.org/index/revalidate?target=test")
        assert r.status_code == 202, r.text
        job_id = r.json()["id"]
        job = await _until(c, lambda j: j["progress"].get("phase") == "validating")
        assert job["id"] == job_id and job["kind"] == "validate" and job["run_id"] == run_id
    app, c = env.engine()
    async with app.router.lifespan_context(app), c:
        job = await wait_job(c, "ex.org", timeout=30)
        assert job["id"] == job_id and job["state"] == "succeeded", job
        assert job["progress"]["restarts"] == 1 and job["progress"]["validation_ok"] is True
        run = (await c.get("/api/collections/ex.org/index_runs")).json()[0]
        assert run["run_id"] == run_id and run["validation"]["count_matches"] is True
    # the first index (main + its second pass), then the revalidation's own second pass, once
    assert env.dispatches() == ["main", "second", "second"]


# ── Index to prod (#31) ─────────────────────────────────────────────────


async def _validated_test_run(env, c) -> tuple[str, list[dict], dict]:
    """ex.org curated, indexed to test and validated; the run's vectors in S3. Returns
    (test run id, its export lines, its manifest)."""
    await prepare(c)
    r = await c.post("/api/collections/ex.org/index?target=test")
    test_run = r.json()["run_id"]
    env.go(test_run)
    assert (await wait_job(c, "ex.org", timeout=30))["state"] == "succeeded"
    prefix = f"curated_collections/ex_org/{test_run}"
    s3 = env.s3_client
    manifest = json.loads(s3.get_object(Bucket="cosmos-idx", Key=f"{prefix}/manifest.json")["Body"].read())
    lines = [json.loads(x) for x in s3.get_object(Bucket="cosmos-idx", Key=f"{prefix}/documents.jsonl")["Body"].read().splitlines()]
    s3.put_object(Bucket="cosmos-idx", Key=f"vectorized/ex_org/{test_run}/batch_0001.jsonl", Body="\n".join(
        json.dumps({**to_web_document(ln, manifest), "vectorized_title": [1], "vectorized_full_text": []}) for ln in lines).encode())
    return test_run, lines, manifest


def _wire_prod(app, env, prod, monkeypatch):
    import sde_curation.jobs as jobs_mod
    settings = app.state.settings
    app.state.jobs._publisher = lambda: ProdPublisher(settings, s3=S3("cosmos-idx", client=env.s3_client), prod=prod)

    async def prod_direct(settings, *, collection_key, run_id, target, expected_titles, client=None):
        hits = prod.search("sde-web", {"size": 10_000})["hits"]["hits"]
        indexed = {h["_source"]["id"]: h["_source"]["title"] or "" for h in hits}
        return compare(collection_key, run_id, {web_id(collection_key, u): t for u, t in expected_titles.items()}, indexed)

    monkeypatch.setattr(jobs_mod, "validate_direct", prod_direct)


PROD = {"opensearch_endpoint_prod": "https://prod.example.aoss.amazonaws.com", "publish_bulk_docs": 2}


async def test_a_prod_publish_interrupted_halfway_finishes_without_writing_anything_twice(env, monkeypatch):
    prod = FakeAoss()
    app, c = env.engine(**PROD)
    async with app.router.lifespan_context(app), c:
        _, lines, manifest = await _validated_test_run(env, c)
        for ln in lines:  # prod holds an older version of every page, and one page the crawl lost
            prod.add({**to_web_document(ln, manifest), "title": "older title", "version": "older"})
        stale = prod.add(to_web_document({"url": "https://ex.org/gone", "title": "Gone"}, manifest))
        down = threading.Event()
        bulk = prod.bulk

        def bulk_until_the_engine_goes_down(body):
            if len(prod.bulk_calls) >= 1:  # the second request never lands: the engine dies under it
                down.wait(10)
                raise ConnectionError("engine went down")
            return bulk(body)

        prod.bulk = bulk_until_the_engine_goes_down
        _wire_prod(app, env, prod, monkeypatch)
        r = await c.post("/api/collections/ex.org/index?target=prod")
        assert r.status_code == 202, r.text
        job_id, run_id = r.json()["id"], r.json()["run_id"]
        for _ in range(200):
            if len(prod.bulk_calls) >= 1 and (await app.state.db.get_job(job_id)).progress.get("indexed"):
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.2)
    down.set()
    prod.bulk = bulk
    written_first = len(prod.bulk_calls[0])

    app, c = env.engine(**PROD)
    async with app.router.lifespan_context(app), c:
        _wire_prod(app, env, prod, monkeypatch)
        job = await wait_job(c, "ex.org", timeout=30)
        assert job["id"] == job_id and job["state"] == "succeeded", job
        assert job["progress"]["restarts"] == 1  # it was interrupted and resumed, not finished before
        st = job["progress"]["status"]
        assert st["indexed"] == len(lines) and st["deleted"] == 1 and st["attempts"] == 2, st
        run = (await c.get("/api/collections/ex.org/index_runs")).json()[0]
        assert run["run_id"] == run_id and run["state"] == "succeeded"
        assert (await c.get("/api/collections/ex.org")).json()["status"] == "live"
    assert written_first > 0
    # every page once, at the version of the test run; the lost page removed, once
    for ln in lines:
        doc = to_web_document(ln, manifest)
        held = prod.by_id(doc["id"])
        assert len(held) == 1 and held[0]["title"] == doc["title"], (ln["url"], held)
    assert stale not in prod.store
    deletes = [a for call in prod.bulk_calls for a in call if "delete" in a and a["delete"]["_id"] == stale]
    assert len(deletes) == 1


async def test_a_resumed_prod_publish_refuses_a_mass_deletion_as_a_first_run_does(env, monkeypatch):
    prod = FakeAoss()
    app, c = env.engine(**PROD)
    async with app.router.lifespan_context(app), c:
        test_run, lines, manifest = await _validated_test_run(env, c)
        for i in range(len(lines) * 10):  # prod holds far more of this collection than the export
            prod.add(to_web_document({"url": f"https://ex.org/stale{i}", "title": f"S{i}"}, manifest))
        db = app.state.db
        job = await db.insert_job(JobRun(collection_id="ex.org", kind=JobKind.INDEX_PROD, state=JobState.RUNNING,
                                         run_id="r-prod-2", progress={"phase": "preflight"}))
        await db.insert_index_run(IndexRun(run_id="r-prod-2", collection_id="ex.org", target="prod",
                                           external_ref=f"publish:{test_run}"))
    before = dict(prod.store)
    app, c = env.engine(**PROD)
    async with app.router.lifespan_context(app), c:
        _wire_prod(app, env, prod, monkeypatch)
        got = await wait_job(c, "ex.org", timeout=30)
    assert got["id"] == job.id and got["state"] == "failed" and "deletion_threshold_exceeded" in got["error"], got
    assert prod.store == before and not prod.bulk_calls  # refused with nothing written
