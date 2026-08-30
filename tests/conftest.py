import asyncio
import sys
import textwrap
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.web.app import create_app


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(data_dir=tmp_path, llm_provider="fake")


@pytest.fixture
async def app(settings):
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        yield app


@pytest.fixture
async def client(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        c.app = app
        yield c


async def seed_dump(client, cid, n=3):
    """Give a collection a fake dump so status rules that need data are satisfied."""
    from sde_curation.models import DumpUrl

    await client.app.state.db.replace_dump(
        cid, [DumpUrl(collection_id=cid, url=f"https://{cid}/p{i}", scraped_title=f"P{i}") for i in range(n)]
    )


FAKE_RUN_PY = textwrap.dedent(
    """
    import json, sys, time
    from pathlib import Path
    ROOT = Path(__file__).resolve().parent
    job = json.loads(Path(sys.argv[sys.argv.index("--job") + 1]).read_text())
    cid = job["collection_id"]
    log = ROOT / "logs" / "jobs" / f"{cid}.log"; log.parent.mkdir(parents=True, exist_ok=True)
    docs = ROOT / "output" / "collections" / f"{cid}.json"; docs.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as out:
        out.write(f"# job={cid}.json collection_id={cid}\\n# seed={job['seed']}\\n")
        if job.get("max_pages") == 13:  # sentinel: simulate a crash
            out.write("\\n# ERROR: RuntimeError('boom')\\n# exit=1 elapsed_s=0.1\\n"); sys.exit(1)
        n = job["max_pages"]
        for i in range(1, n + 1):
            st = "fail" if i % 5 == 0 else "ok"
            out.write(f"  {i:<5} {st:<10} {0:<6} {job['seed']}/p{i}\\n"); out.flush()
            time.sleep(0.02)
        out.write(f"  ... {n - n // 5} docs / {n // 5} failed  (cap {n})\\n")
        out.write("\\n# exit=0 elapsed_s=0.5\\n")
    docs.write_text(json.dumps([
        {"url": f"{job['seed']}/p{i}", "title": f"Page {i}", "full_text": "text " * 5, "content_type": "text/html", "depth": 0}
        for i in range(1, n + 1) if i % 5
    ]))
    """
)


@pytest.fixture
async def crawler_client(tmp_path):
    """App wired to a fake crawl4ai `run.py` (see FAKE_RUN_PY): max_pages=13 simulates a crash,
    every 5th page fails, the rest succeed."""
    root = tmp_path / "crawler"
    root.mkdir()
    (root / "run.py").write_text(FAKE_RUN_PY)
    settings = Settings(
        data_dir=tmp_path / "data", crawler_root=root, crawler_python=Path(sys.executable),
        scrape_poll_interval_s=0.05, llm_provider="fake",
    )
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        c.app = app
        yield c


async def wait_job(client, cid, timeout=10):
    for _ in range(int(timeout / 0.1)):
        jobs = (await client.get(f"/api/collections/{cid}/jobs")).json()
        if jobs and jobs[0]["state"] in ("succeeded", "failed"):
            return jobs[0]
        await asyncio.sleep(0.1)
    raise AssertionError("job did not finish")


FAKE_INDEXER = """
import json, os, sys, time, boto3
args = sys.argv[1:]; key = args[args.index("--collection")+1]; run = args[args.index("--run-id")+1]; target = args[args.index("--target")+1]
bucket = os.environ["COSMOS_INDEX_BUCKET"]
s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=os.environ.get("MOTO_ENDPOINT"))
m = json.loads(s3.get_object(Bucket=bucket, Key=f"curated_collections/{key}/{run}/manifest.json")["Body"].read())
docs = s3.get_object(Bucket=bucket, Key=f"curated_collections/{key}/{run}/documents.jsonl")["Body"].read().decode().splitlines()
fail = key == "fail.org"
status = {"run_id": run, "collection_key": key, "target": target, "index": "sde-web-subset",
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


@pytest.fixture
async def index_client(tmp_path, monkeypatch):
    """App with fake crawler + fake indexer subprocess + a moto S3 *server* (subprocess needs a real endpoint)."""
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0); server.start()
    port = server._server.socket.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test"); monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1"); monkeypatch.setenv("MOTO_ENDPOINT", endpoint)
    monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)
    import boto3
    boto3.client("s3", region_name="us-east-1", endpoint_url=endpoint).create_bucket(Bucket="cosmos-idx")

    croot = tmp_path / "crawler"; croot.mkdir(); (croot / "run.py").write_text(FAKE_RUN_PY)
    iroot = tmp_path / "indexer"; iroot.mkdir(); (iroot / "api_scraper.py").write_text(FAKE_INDEXER)
    settings = Settings(
        data_dir=tmp_path / "data", crawler_root=croot, crawler_python=Path(sys.executable),
        indexer_root=iroot, indexer_python=Path(sys.executable), cosmos_index_bucket="cosmos-idx",
        index_poll_interval_s=0.1, index_stall_timeout_s=20, scrape_poll_interval_s=0.05, llm_provider="fake",
        validation_delay_s=0.1,
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        c.app = app; c.s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=endpoint)
        yield c
    server.stop()


async def prepare(c, cid="ex.org"):
    await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 10, "division": "Heliophysics"})
    await c.post(f"/api/collections/{cid}/scrape"); await wait_job(c, cid)
    await c.post(f"/api/collections/{cid}/recompute")
    await c.post(f"/api/collections/{cid}/patterns", json={"type": "exclude", "match": "*/p1"})
    await c.post(f"/api/collections/{cid}/promote")
