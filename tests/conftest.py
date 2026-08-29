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
