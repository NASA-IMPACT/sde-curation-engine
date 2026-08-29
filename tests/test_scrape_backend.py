"""Scrape backends against a fake crawler (local) and moto (SSM)."""

import asyncio
import json
import sys
import textwrap
from pathlib import Path

import pytest

from sde_curation.backends.scrape import (
    LocalSubprocessScraper,
    LogProgress,
    ScrapeError,
    SsmRemoteScraper,
    build_job,
)
from sde_curation.config import Settings
from sde_curation.models import Collection, Division

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
def crawler_root(tmp_path) -> Path:
    root = tmp_path / "crawler"
    root.mkdir()
    (root / "run.py").write_text(FAKE_RUN_PY)
    return root


@pytest.fixture
def settings(tmp_path, crawler_root) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        crawler_root=crawler_root,
        crawler_python=Path(sys.executable),
        scrape_poll_interval_s=0.05,
        llm_provider="fake",
    )


def coll(n: int) -> Collection:
    return Collection(
        collection_id="ex.org", name="Ex", seed_url="https://ex.org", division=Division.GENERAL,
        connector="crawler2", max_pages=n,
    )


def test_build_job_matches_crawler_shape():
    assert build_job(coll(7)) == {"seed": "https://ex.org", "collection_id": "ex.org", "max_pages": 7}


def test_log_progress_parser():
    p = LogProgress()
    lines = [
        "# job=x.json collection_id=x",
        "  1     ok         0      https://x/a",
        "  2     fail       1      https://x/b",
        "        http_404",
        "  ... 25 docs / 3 failed  (cap 100)",
        "# ERROR: RuntimeError('boom')",
        "# exit=1 elapsed_s=1.0",
    ]
    for line in lines:
        p.feed(line)
    assert p.snapshot() == {"processed": 2, "docs": 25, "failed": 3}
    assert p.exit_code == 1 and "boom" in p.error


async def test_local_success_with_progress(settings, crawler_root):
    seen = []

    async def cb(p):
        seen.append(dict(p))

    res = await LocalSubprocessScraper(settings).run(coll(10), cb)
    docs = json.loads(res.documents_path.read_text())
    assert len(docs) == 8 and docs[0]["url"] == "https://ex.org/p1"
    assert seen[0]["pid"] and seen[-1] == {"processed": 10, "docs": 8, "failed": 2}
    assert any(0 < s.get("processed", 0) < 10 for s in seen), "no intermediate progress seen"
    # job json written where the crawler expects a path, under our DATA_DIR
    assert (settings.data_dir / "scrape_jobs" / "ex.org.json").is_file()


async def test_local_failure_surfaces_error(settings):
    with pytest.raises(ScrapeError, match="exited 1.*boom"):
        await LocalSubprocessScraper(settings).run(coll(13), lambda p: asyncio.sleep(0))


async def test_local_missing_python(settings):
    s = settings.model_copy(update={"crawler_python": Path("/nonexistent/python")})
    with pytest.raises(ScrapeError, match="CRAWLER_PYTHON"):
        await LocalSubprocessScraper(s).run(coll(1), lambda p: asyncio.sleep(0))


# ── SSM ────────────────────────────────────────────────────────────────


@pytest.fixture
def aws(monkeypatch):
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        yield


async def test_ssm_drops_job_and_waits_for_s3(aws, tmp_path):
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="crawl-bkt")
    ssm = boto3.client("ssm", region_name="us-east-1")
    settings = Settings(
        data_dir=tmp_path, scrape_backend="ssm", crawler_instance_id="i-123",
        crawler_s3_bucket="crawl-bkt", scrape_poll_interval_s=0.05, llm_provider="fake",
    )
    backend = SsmRemoteScraper(settings, ssm=ssm, s3=s3)
    script = backend.remote_script(build_job(coll(5)))
    assert "/opt/sde-crawler/jobs/incoming/ex.org.json" in script and '"max_pages": 5' in script

    async def upload_later():
        await asyncio.sleep(0.2)
        s3.put_object(
            Bucket="crawl-bkt", Key="scraped_collections/ex.org.json",
            Body=json.dumps([{"url": "https://ex.org/a", "title": "A", "full_text": "t"}]),
        )
        s3.put_object(
            Bucket="crawl-bkt", Key="failure_logs/ex.org_failures_summary.json",
            Body=json.dumps({"documents_scraped": 1, "failures_logged": 0}),
        )

    asyncio.create_task(upload_later())
    res = await backend.run(coll(5), lambda p: asyncio.sleep(0))
    assert json.loads(res.documents_path.read_text())[0]["url"] == "https://ex.org/a"
    assert res.summary["documents_scraped"] == 1
    # the first SSM command sent was the job drop
    cmds = ssm.list_commands()["Commands"]
    assert any(c["Comment"] == "sde-curation-engine" for c in cmds)
    assert "jobs/incoming/ex.org.json" in cmds[-1]["Parameters"]["commands"][0]
