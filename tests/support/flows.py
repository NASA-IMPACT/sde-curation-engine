"""Helpers the integration and end-to-end tests share: logging in, waiting for a job, the curator's
usual steps (crawl, Start curating, metadata, promote), and the fake indexer script."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sde_curation.config import Settings
from sde_curation.web.app import create_app
from tests.support.crawler import FAKE_RUN_PY

__all__ = ["FAKE_INDEXER", "FAKE_RUN_PY", "PROGRESS_MAX_BYTES", "SECURED", "_crawler_app", "add_user", "classify",
           "login", "prepare", "seed_dump", "wait_job"]

# Login enabled: APP_PASSWORD seeds the bootstrap "admin" account with that password.
SECURED = {"app_password": "s3cret", "session_secret": "unit-test-secret"}

# A job's progress is its resume checkpoint, written every 3 s and sent to every browser: counters,
# batch numbers as ranges and phase names, never lists of URLs or ids (fixtures.progress_stays_small).
PROGRESS_MAX_BYTES = 4096


async def login(client, username: str, password: str) -> None:
    r = await client.post("/login", data={"username": username, "password": password, "next": "/"})
    assert r.status_code == 303, r.text


async def add_user(client, username: str, password: str, role: str = "curator"):
    from sde_curation.models import Role
    from sde_curation.web import auth

    return await client.app.state.db.create_user(username, auth.hash_password(password), Role(role))


async def seed_dump(client, cid, n=3):
    """Give a collection a fake dump so status rules that need data are satisfied."""
    from sde_curation.models import DumpUrl

    await client.app.state.db.replace_dump(
        cid, [DumpUrl(collection_id=cid, url=f"https://{cid}/p{i}", scraped_title=f"P{i}") for i in range(n)]
    )


async def wait_job(client, cid, timeout=10):
    for _ in range(int(timeout / 0.1)):
        jobs = (await client.get(f"/api/collections/{cid}/jobs")).json()
        if jobs and jobs[0]["state"] in ("succeeded", "failed"):
            return jobs[0]
        await asyncio.sleep(0.1)
    raise AssertionError("job did not finish")


def _crawler_app(tmp_path, **extra):
    """An app whose crawler is the fake crawler (a `run.py` subprocess in the end-to-end tests,
    in-process in the integration tests: see tests/integration/conftest.py)."""
    root = tmp_path / "crawler"
    root.mkdir()
    (root / "run.py").write_text(FAKE_RUN_PY)
    return create_app(Settings(
        data_dir=tmp_path / "data", crawler_root=root, crawler_python=Path(sys.executable),
        scrape_poll_interval_s=0.05, llm_provider="fake", **{"llm_retry_delay_s": 0, **extra},
    ))


async def prepare(c, cid="ex.org", *, create=True):
    if create:
        await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 10,
                                               "division": "Heliophysics"})
    await c.post(f"/api/collections/{cid}/scrape"); await wait_job(c, cid)
    await c.post(f"/api/collections/{cid}/recompute")
    await c.post(f"/api/collections/{cid}/patterns", json={"type": "exclude", "match": "*/p1"})
    await classify(c, cid)
    r = await c.post(f"/api/collections/{cid}/promote")
    assert r.status_code == 200, r.text


async def classify(c, cid="ex.org"):
    """The curator's metadata step: Suggest metadata (fake LLM), then accept every suggestion. Promote
    refuses delta URLs without a title, division or document type, so a flow that promotes runs this first."""
    r = await c.post(f"/api/collections/{cid}/suggest/metadata")
    assert r.status_code == 202, r.text
    job = await wait_job(c, cid)
    assert job["state"] == "succeeded", job
    r = await c.post(f"/api/collections/{cid}/ai/bulk", json={"decision": "accept"})
    assert r.status_code == 200, r.text


FAKE_INDEXER = """
import json, os, sys, time, boto3
args = sys.argv[1:]; key = args[args.index("--collection")+1]; run = args[args.index("--run-id")+1]; target = args[args.index("--target")+1]
bucket = os.environ["COSMOS_INDEX_BUCKET"]
s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=os.environ.get("MOTO_ENDPOINT"))
m = json.loads(s3.get_object(Bucket=bucket, Key=f"curated_collections/{key}/{run}/manifest.json")["Body"].read())
docs = s3.get_object(Bucket=bucket, Key=f"curated_collections/{key}/{run}/documents.jsonl")["Body"].read().decode().splitlines()
fail = key == "fail_org"  # collection "fail.org" → key "fail_org"
status = {"run_id": run, "collection_key": key, "target": target, "index": "sde-web",
          "state": "failed" if fail else "succeeded", "documents_in_export": m["document_count"], "changed": len(docs),
          "indexed": 0 if fail else len(docs), "failed": 0, "deleted": 0, "error": "export_incomplete" if fail else None}
if target == "test" and not fail:
    # mimic AOSS eventual consistency: first pass sees 0 docs, a later pass sees them all
    # (half.org: only half ever show up → validation must fail)
    try:
        s3.head_object(Bucket=bucket, Key=f"index_runs/{key}/{run}/.pass1"); second = True
    except Exception:
        second = False
        s3.put_object(Bucket=bucket, Key=f"index_runs/{key}/{run}/.pass1", Body=b"1")
    n = len(docs); seen = 0 if not second else (n // 2 if key.startswith("half") else n)
    s3.put_object(Bucket=bucket, Key=f"index_runs/{key}/{run}/validation.json", Body=json.dumps(
        {"run_id": run, "collection_key": key, "expected_count": m["document_count"], "indexed_count": seen,
         "count_matches": seen == n, "title_match_rate": round(seen / n, 6) if n else 1.0}))
time.sleep(0.2)
s3.put_object(Bucket=bucket, Key=f"index_runs/{key}/{run}/status.json", Body=json.dumps(status))
sys.exit(0 if not fail else 1)
"""
