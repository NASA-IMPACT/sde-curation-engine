"""Scrape backends against a fake crawler (local) and moto (SSM)."""
import asyncio
import json
import sys
from pathlib import Path

import pytest

from sde_curation.backends.scrape import (
    LocalSubprocessScraper,
    ScrapeError,
    SsmRemoteScraper,
)
from sde_curation.config import Settings
from sde_curation.models import Collection, Division
from tests.support.aws import _docs_text
from tests.support.flows import FAKE_RUN_PY


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


async def test_local_success_with_progress(settings, crawler_root):
    seen = []

    async def cb(p):
        seen.append(dict(p))

    res = await LocalSubprocessScraper(settings).run(coll(10), cb)
    docs = json.loads(_docs_text(res))
    assert len(docs) == 8 and docs[0]["url"] == "https://ex.org/p1"
    assert seen[0]["pid"] and seen[-1] == {"processed": 10, "docs": 8, "failed": 2}
    assert any(0 < s.get("processed", 0) < 10 for s in seen), "no intermediate progress seen"
    # job json written where the crawler expects a path, under our DATA_DIR
    assert (settings.data_dir / "scrape_jobs" / "https_ex.org.json").is_file()


async def test_local_failure_surfaces_error(settings):
    with pytest.raises(ScrapeError, match="exited 1.*boom"):
        await LocalSubprocessScraper(settings).run(coll(13), lambda p: asyncio.sleep(0))


# ── SSM ────────────────────────────────────────────────────────────────


STALE_FAILED_LOG = ["# ERROR: RuntimeError('old run blew up')", "# exit=1 elapsed_s=3.0"]
PAGES = ["  1     ok         0      https://ex.org/a", "  2     ok         1      https://ex.org/b"]
    # no log yet, watcher unknown


        # after poll 6 the log goes silent


# ── resume after an engine restart ───────────────────────────────────────
# A deploy restarts the engine; the crawl on the host carries on (2026-09-25: pds.nasa.gov showed
# "failed" mid-deploy while the crawler kept going). Resuming must never drop a second job file.


def _drops(s: SsmRemoteScraper) -> list:
    return [c for c in s.ssm.commands if "cat >" in c["Parameters"]["commands"][0]]


def _engine(monkeypatch, tmp_path, make):
    """create_app() wired to the fake crawler host; the host outlives every engine built."""
    import sde_curation.web.app as web
    scrapers: list[SsmRemoteScraper] = []

    def backend(settings):
        scrapers.append(make())
        return scrapers[-1]

    monkeypatch.setattr(web, "make_scrape_backend", backend)

    def engine():
        return web.create_app(Settings(data_dir=tmp_path / "data", scrape_backend="ssm", crawler_instance_id="i-123",
                                       crawler_s3_bucket="crawl-bkt", scrape_poll_interval_s=0.01, llm_provider="fake"))
    return engine, scrapers


async def _start_crawl_then_go_down(engine, host, *, old_engine=False):
    """Scrape; the host starts crawling; the engine shuts down (a deploy) mid-crawl."""
    from httpx import ASGITransport, AsyncClient
    host.inbox.clear()
    host.on_poll = lambda n: host.start_crawl(PAGES[:1]) if n == 2 else None
    app = engine()
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 5})
        assert (await c.post("/api/collections/ex.org/scrape")).status_code == 202
        for _ in range(100):
            if host.polls >= 3:
                break
            await asyncio.sleep(0.02)
        if old_engine:  # before 2026-09-25 a shutdown recorded the scrape as failed
            app.state.jobs.shutdown = _old_shutdown(app.state.jobs)
        jobs = (await c.get("/api/collections/ex.org/jobs")).json()
    return jobs[0]["id"]


def _old_shutdown(jm):
    async def shutdown():
        for t in list(jm._tasks.values()):
            t.cancel()
        await asyncio.gather(*jm._tasks.values(), return_exceptions=True)
    return shutdown


def _host_finishes(host, upload):
    """The crawl writes its last lines (touching the log, as the real crawler does) and uploads."""
    def finish(n):
        if n >= 2:
            host.start_crawl(PAGES + ["# s3 documents=...", "# exit=0 elapsed_s=9.0"])
            if n == 2:
                upload()
    host.polls, host.on_poll = 0, finish


async def test_a_deploy_mid_crawl_carries_on_the_same_scrape_job(ssm_env, monkeypatch, tmp_path):
    """End to end: Scrape → the engine goes down mid-crawl (a deploy) → the next engine start
    carries on the SAME job: it never shows failed, follows the crawl to the end and ingests it.
    One job file ever dropped, one job ever listed."""
    from httpx import ASGITransport, AsyncClient

    from tests.support.flows import wait_job
    host, make, upload = ssm_env
    engine, scrapers = _engine(monkeypatch, tmp_path, make)
    job_id = await _start_crawl_then_go_down(engine, host)

    host.polls, host.on_poll = 0, lambda n: None  # the crawl is still going on the host
    app = engine()
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        jobs = (await c.get("/api/collections/ex.org/jobs")).json()
        assert [(j["id"], j["state"]) for j in jobs] == [(job_id, "running")]  # still running, same job
        _host_finishes(host, upload)
        job = await wait_job(c, "ex.org")
        assert job["id"] == job_id and job["state"] == "succeeded" and job["error"] is None
        assert job["progress"]["restarts"] == 1 and job["progress"]["docs"] == 1
        assert len((await c.get("/api/collections/ex.org/jobs")).json()) == 1
        assert sum(len(_drops(s)) for s in scrapers) == 1  # the original Scrape only


async def test_a_scrape_an_older_engine_failed_on_shutdown_is_reopened(ssm_env, monkeypatch, tmp_path):
    """pds.nasa.gov job 86: the engine before this change recorded 'cancelled by shutdown' while
    the crawl carried on. The next start reopens that same job and follows the crawl."""
    from httpx import ASGITransport, AsyncClient

    from tests.support.flows import wait_job
    host, make, upload = ssm_env
    engine, scrapers = _engine(monkeypatch, tmp_path, make)
    job_id = await _start_crawl_then_go_down(engine, host, old_engine=True)

    _host_finishes(host, upload)
    app = engine()
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        job = await wait_job(c, "ex.org")
        assert job["id"] == job_id and job["state"] == "succeeded" and job["error"] is None
        assert len((await c.get("/api/collections/ex.org/jobs")).json()) == 1
        assert sum(len(_drops(s)) for s in scrapers) == 1


async def test_a_scrape_cancelled_by_a_curator_stays_cancelled_across_a_restart(ssm_env, monkeypatch, tmp_path):
    from httpx import ASGITransport, AsyncClient
    host, make, _upload = ssm_env
    engine, _ = _engine(monkeypatch, tmp_path, make)
    host.inbox.clear()
    host.on_poll = lambda n: host.start_crawl(PAGES[:1]) if n == 2 else None
    app = engine()
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 5})
        await c.post("/api/collections/ex.org/scrape")
        await asyncio.sleep(0.1)
        assert (await c.post("/api/collections/ex.org/jobs/cancel")).status_code == 200
    app = engine()
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await asyncio.sleep(0.1)
        jobs = (await c.get("/api/collections/ex.org/jobs")).json()
        assert len(jobs) == 1 and jobs[0]["state"] == "failed" and jobs[0]["error"].startswith("cancelled by ")
        assert jobs[0]["error"] != "cancelled by shutdown"


# ── reading the documents file ────────────────────────────────────────


# Strings that look like structure: quotes, brackets and braces inside text, backslash runs of
# every parity right before a quote, and the control characters a binary file kept as text is
# made of — JSON escapes all of them, so the raw bytes are full of \ and \uXXXX.
TRICKY = [
    {"url": "https://ex.org/a", "title": 'say "hi" {not} [an] ,array', "full_text": "a"},
    {"url": "https://ex.org/b", "title": "ends in a backslash \\", "full_text": "\\\\"},
    {"url": "https://ex.org/c", "title": '\\"', "full_text": '\\\\\\"}]'},
    {"url": "https://ex.org/d", "title": "é ✓ \ufffd", "full_text": "".join(map(chr, range(0x20))) * 3,
     "depth": 2, "tags": [{"k": ["[", "]"]}], "score": 0.5},
    {"url": "https://ex.org/e", "title": "", "full_text": ""},
]
