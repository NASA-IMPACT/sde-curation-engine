"""Scrape backends against a fake crawler (local) and moto (SSM)."""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest

from sde_curation.backends.scrape import (
    LocalSubprocessScraper,
    LogProgress,
    ScrapeError,
    SsmRemoteScraper,
    build_job,
    parse_poll,
)
from sde_curation.config import Settings
from sde_curation.models import Collection, Division


def _docs_text(res) -> str:
    """The crawl a ScrapeResult points at, read back. A source is a file (local backend) or the
    S3 object itself (remote): either way it is opened, not copied to disk."""
    with res.documents.open() as fh:
        return fh.read().decode()
from tests.conftest import FAKE_RUN_PY


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
    assert build_job(coll(7)) == {"seed": "https://ex.org", "collection_id": "https_ex.org", "max_pages": 7}


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


def test_log_progress_parser_reads_crawler_v2_lines():
    """v2.1 renamed the heartbeat and added timeout/skip statuses and checkpoint notes."""
    p = LogProgress()
    for line in [
        "  1     ok         0      https://x/a",
        "  2     timeout    1      https://x/b",
        "  3     skip       1      https://x/c.docx",
        "  4     pdf        1      https://x/d.pdf",
        "  checkpoint  100 docs -> s3://bkt/scraped_collections/x.json",
        "  ... 50 docs / 7 failures logged  (cap 100000)",
    ]:
        p.feed(line)
    assert p.snapshot() == {"processed": 4, "docs": 50, "failed": 7}
    assert p.exit_code is None


def test_feed_tail_does_not_double_count_overlapping_tails():
    p = LogProgress()
    first = ["  1     ok         0      https://x/a", "  2     ok         1      https://x/b"]
    assert p.feed_tail(first) is True
    assert p.feed_tail(first) is False, "re-reading the same tail must be a no-op"
    assert p.feed_tail(first[1:] + ["  3     fail       1      https://x/c"]) is True
    assert p.snapshot() == {"processed": 3, "docs": 2, "failed": 1}


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


async def test_local_missing_python(settings):
    s = settings.model_copy(update={"crawler_python": Path("/nonexistent/python")})
    with pytest.raises(ScrapeError, match="CRAWLER_PYTHON"):
        await LocalSubprocessScraper(s).run(coll(1), lambda p: asyncio.sleep(0))


# ── SSM ────────────────────────────────────────────────────────────────


class FakeHost:
    """The crawler EC2 box as the poll script sees it. Tests mutate the fields, or set
    `on_poll(n)` to change state after the n-th poll."""

    def __init__(self):
        self.inbox: set[str] = {"asdc.json", "espo.json"}  # a batch already running
        self.watcher = 2
        self.log_mtime: int | None = None
        self.tail: list[str] = []
        self.polls = 0
        self.on_poll = lambda n: None

    def execute(self, script: str) -> str:
        if "cat >" in script:  # job drop
            name = script.split("cat > ")[1].split(" ")[0].rsplit("/", 1)[1]
            self.inbox.add(name)
            return ""
        self.polls += 1
        self.on_poll(self.polls)
        cid = script.split("[ -f ")[1].split(" ]")[0].rsplit("/", 1)[1]
        mtime = "" if self.log_mtime is None else str(self.log_mtime)
        return (
            f"@@inbox={1 if cid in self.inbox else 0}\n@@jobs={len(self.inbox)}\n"
            f"@@watcher={self.watcher}\n@@mtime={mtime}\n" + "".join(f"{l}\n" for l in self.tail)
        )

    def start_crawl(self, tail: list[str]) -> None:
        self.log_mtime = int(time.time())
        self.tail = tail


class FakeSsm:
    class exceptions:
        class InvocationDoesNotExist(Exception):
            pass

    def __init__(self, host: FakeHost):
        self.host = host
        self.commands: list[dict] = []
        self.out: dict[str, str] = {}

    def send_command(self, **kw):
        cid = f"cmd-{len(self.commands) + 1}"
        self.commands.append(kw)
        self.out[cid] = self.host.execute(kw["Parameters"]["commands"][0])
        return {"Command": {"CommandId": cid}}

    def get_command_invocation(self, CommandId, InstanceId):
        return {"Status": "Success", "StandardOutputContent": self.out[CommandId]}


@pytest.fixture
def aws(monkeypatch):
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        yield


@pytest.fixture
def ssm_env(aws, tmp_path):
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="crawl-bkt")
    host = FakeHost()

    def make(**overrides) -> SsmRemoteScraper:
        settings = Settings(
            data_dir=tmp_path, scrape_backend="ssm", crawler_instance_id="i-123",
            crawler_s3_bucket="crawl-bkt", scrape_poll_interval_s=0.01, llm_provider="fake",
            **overrides,
        )
        return SsmRemoteScraper(settings, ssm=FakeSsm(host), s3=s3)

    def upload(docs=None):
        s3.put_object(
            Bucket="crawl-bkt", Key="scraped_collections/https_ex.org.json",
            Body=json.dumps(docs or [{"url": "https://ex.org/a", "title": "A", "full_text": "t"}]),
        )
        s3.put_object(
            Bucket="crawl-bkt", Key="failure_logs/https_ex.org_failures_summary.json",
            Body=json.dumps({"documents_scraped": 1, "failures_logged": 0}),
        )

    return host, make, upload


STALE_FAILED_LOG = ["# ERROR: RuntimeError('old run blew up')", "# exit=1 elapsed_s=3.0"]
PAGES = ["  1     ok         0      https://ex.org/a", "  2     ok         1      https://ex.org/b"]


async def progress_recorder():
    seen: list[dict] = []

    async def cb(p):
        seen.append(dict(p))

    return seen, cb


def test_parse_poll_reads_markers_and_tail():
    poll = parse_poll("@@inbox=1\n@@jobs= 3\n@@watcher=2\n@@mtime=1788889220\n  5     ok    1   https://x\n# exit=0\n")
    assert poll.inbox and poll.jobs == 3 and poll.watcher == 2
    assert poll.log_mtime.timestamp() == 1788889220
    assert poll.tail == ["  5     ok    1   https://x", "# exit=0"]
    # no log yet, watcher unknown
    poll = parse_poll("@@inbox=0\n@@jobs=0\n@@watcher=\n@@mtime=\n")
    assert not poll.inbox and poll.watcher is None and poll.log_mtime is None and poll.tail == []


def test_poll_script_targets_inbox_and_job_log(ssm_env):
    _, make, _ = ssm_env
    script = make().poll_script("https_ex.org")
    assert "/opt/sde-crawler/jobs/incoming/https_ex.org.json" in script
    assert "/opt/sde-crawler/logs/jobs/https_ex.org.log" in script and "tail -n 5" in script


async def test_ssm_queued_behind_batch_then_runs(ssm_env):
    """Job sits in the inbox behind a running batch (stale log from a *failed* previous run
    still on disk), then the crawler picks it up, then uploads."""
    host, make, upload = ssm_env
    host.log_mtime = int(time.time()) - 3600
    host.tail = STALE_FAILED_LOG

    def on_poll(n):
        if n == 3:
            host.start_crawl(PAGES)
        if n == 5:
            host.tail = PAGES[1:] + ["# s3 documents=...", "# exit=0 elapsed_s=9.0"]
            upload()

    host.on_poll = on_poll
    backend = make(index_stall_timeout_s=0.05)  # stall clock must not run while queued
    seen, cb = await progress_recorder()
    res = await backend.run(coll(5), cb)

    assert json.loads(_docs_text(res))[0]["url"] == "https://ex.org/a"
    assert res.summary["documents_scraped"] == 1
    assert seen[0]["ssm_command"] == "cmd-2" and seen[0]["queued"] is True  # cmd-1 checked the inbox
    assert {"queued": True, "queue_ahead": 2} in seen, seen
    started = seen.index({"queued": False, "queue_ahead": 0})
    assert all(s.get("queued") for s in seen[:started]), "no crawl progress before the log was fresh"
    assert seen[-1] == {"processed": 2, "docs": 2, "failed": 0}
    drop = backend.ssm.commands[0]
    assert drop["Comment"] == "sde-curation-engine"
    assert "jobs/incoming/https_ex.org.json" in drop["Parameters"]["commands"][0]


async def test_ssm_dead_watcher_fails_fast(ssm_env):
    host, make, _ = ssm_env
    host.watcher = 0
    with pytest.raises(ScrapeError, match="watch_inbox.sh.*not running"):
        await make().run(coll(5), lambda p: asyncio.sleep(0))


async def test_ssm_job_file_vanished(ssm_env):
    host, make, _ = ssm_env
    host.on_poll = lambda n: host.inbox.discard("https_ex.org.json")
    with pytest.raises(ScrapeError, match="vanished from the crawler inbox"):
        await make().run(coll(5), lambda p: asyncio.sleep(0))


async def test_ssm_fresh_failure_is_reported(ssm_env):
    host, make, _ = ssm_env
    host.on_poll = lambda n: host.start_crawl(["# ERROR: RuntimeError('boom')", "# exit=1 elapsed_s=1.0"])
    with pytest.raises(ScrapeError, match="remote crawler failed: RuntimeError\\('boom'\\)"):
        await make().run(coll(5), lambda p: asyncio.sleep(0))


async def test_ssm_stall_clock_starts_with_the_crawl_and_resets_on_activity(ssm_env):
    host, make, _ = ssm_env

    def on_poll(n):
        if n == 1:
            host.start_crawl(PAGES[:1])
        if 2 <= n <= 6:  # a slow but live crawl: a new page every poll
            host.tail = [f"  {n}     ok         1      https://ex.org/p{n}"]
        # after poll 6 the log goes silent

    host.on_poll = on_poll
    with pytest.raises(ScrapeError, match="stalled: no log activity"):
        await make(index_stall_timeout_s=0.05).run(coll(5), lambda p: asyncio.sleep(0))
    assert host.polls > 6, "stall fired while the log was still changing"


async def test_ssm_mid_run_checkpoint_upload_does_not_end_the_crawl(ssm_env):
    """Crawler v2 re-uploads the documents object every N pages. The engine must ignore those
    partial objects and only download after the log says exit=0."""
    host, make, upload = ssm_env
    partial = [{"url": "https://ex.org/a", "title": "A", "full_text": "t"}]
    full = partial + [{"url": "https://ex.org/b", "title": "B", "full_text": "t"}]

    def on_poll(n):
        if n == 1:
            host.start_crawl(PAGES[:1])
        if n == 2:  # checkpoint: object changes while the crawl is still running
            upload(partial)
            host.tail = PAGES[:1] + ["  checkpoint  1 docs -> s3://crawl-bkt/scraped_collections/https_ex.org.json"]
        if n == 4:
            host.tail = PAGES + ["# s3 documents=...", "# exit=0 elapsed_s=9.0"]
            upload(full)

    host.on_poll = on_poll
    res = await make().run(coll(5), lambda p: asyncio.sleep(0))
    assert host.polls >= 4, "returned before the crawler wrote exit=0"
    assert [d["url"] for d in json.loads(_docs_text(res))] == ["https://ex.org/a", "https://ex.org/b"]


async def test_ssm_exit_zero_without_upload(ssm_env):
    host, make, _ = ssm_env
    host.on_poll = lambda n: host.start_crawl(["# s3 skipped (no bucket)", "# exit=0 elapsed_s=1.0"])
    with pytest.raises(ScrapeError, match="uploaded no documents object"):
        await make().run(coll(5), lambda p: asyncio.sleep(0))


async def test_ssm_attaches_to_a_job_already_in_the_inbox(ssm_env):
    """pds.nasa.gov: Scrape was pressed while the host was mid-crawl on the same collection.
    Dropping a second job file would overwrite the one in the inbox and crawl the site again
    after the batch; the engine must follow the running job and ingest its final upload."""
    host, make, upload = ssm_env
    host.inbox.add("https_ex.org.json")  # queued or running on the host already
    seen, cb = await progress_recorder()

    def on_poll(n):
        if n == 2:
            host.start_crawl(PAGES[:1])
        if n == 4:
            host.tail = PAGES + ["# s3 documents=...", "# exit=0 elapsed_s=9.0"]
            upload()

    host.on_poll = on_poll
    s = make()
    res = await s.run(coll(5), cb)
    drops = [c for c in s.ssm.commands if "cat >" in c["Parameters"]["commands"][0]]
    assert drops == [], "dropped a duplicate job file"
    assert seen[0] == {"attached": True, "processed": 0, "docs": 0, "failed": 0, "queued": True}
    assert res.external_ref == "attached" and json.loads(_docs_text(res))[0]["url"] == "https://ex.org/a"


async def test_ssm_drops_the_job_when_the_inbox_has_no_file_for_it(ssm_env):
    host, make, upload = ssm_env

    def on_poll(n):
        if n == 2:
            host.start_crawl(PAGES + ["# exit=0 elapsed_s=1.0"])
            upload()

    host.on_poll = on_poll
    s = make()
    seen, cb = await progress_recorder()
    await s.run(coll(5), cb)
    assert "https_ex.org.json" in host.inbox and "ssm_command" in seen[0] and "attached" not in seen[0]


# ── resume after an engine restart ───────────────────────────────────────
# A deploy restarts the engine; the crawl on the host carries on (2026-09-25: pds.nasa.gov showed
# "failed" mid-deploy while the crawler kept going). Resuming must never drop a second job file.


def _drops(s: SsmRemoteScraper) -> list:
    return [c for c in s.ssm.commands if "cat >" in c["Parameters"]["commands"][0]]


async def test_ssm_resume_follows_a_crawl_still_in_the_inbox(ssm_env):
    from datetime import UTC, datetime
    host, make, upload = ssm_env
    host.inbox.add("https_ex.org.json")
    host.start_crawl(PAGES[:1])  # mid-crawl

    def on_poll(n):
        if n == 3:
            host.tail = PAGES + ["# s3 documents=...", "# exit=0 elapsed_s=9.0"]
            upload()

    host.on_poll = on_poll
    seen, cb = await progress_recorder()
    s = make()
    res = await s.resume(coll(5), datetime(2026, 9, 25, tzinfo=UTC), cb)
    assert _drops(s) == [] and seen[0]["attached"] and seen[0]["resumed"]
    assert json.loads(_docs_text(res))[0]["url"] == "https://ex.org/a"


async def test_ssm_resume_ingests_a_crawl_that_finished_while_the_engine_was_down(ssm_env):
    from datetime import UTC, datetime, timedelta
    _host, make, upload = ssm_env
    started = datetime.now(UTC) - timedelta(minutes=5)
    upload()  # finished: documents + summary uploaded, job file moved to done/
    seen, cb = await progress_recorder()
    s = make()
    res = await s.resume(coll(5), started, cb)
    assert _drops(s) == [] and seen == [{"resumed": True, "finished_while_down": True}]
    assert res.external_ref == "resumed" and json.loads(_docs_text(res))[0]["url"] == "https://ex.org/a"


async def test_ssm_resume_fails_rather_than_recrawl_when_the_crawl_is_gone(ssm_env):
    from datetime import UTC, datetime, timedelta
    _host, make, upload = ssm_env
    upload()  # an OLDER finished crawl …
    s = make()
    with pytest.raises(ScrapeError, match="left no finished upload"):  # … is not this crawl's
        await s.resume(coll(5), datetime.now(UTC) + timedelta(minutes=1), lambda p: asyncio.sleep(0))
    assert _drops(s) == []


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

    from tests.conftest import wait_job
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

    from tests.conftest import wait_job
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


async def test_ssm_resume_ingests_a_crawl_that_finished_just_as_the_engine_came_back(ssm_env):
    """The job file is still in the inbox at the first poll, then gone before the log moves again:
    the crawl finished while the engine was starting. That is 'finished while down', not
    'job file vanished before the crawl started'."""
    from datetime import UTC, datetime, timedelta
    host, make, upload = ssm_env
    host.inbox.add("https_ex.org.json")
    host.log_mtime, host.tail = int(time.time()) - 60, PAGES  # last written a minute ago

    def on_poll(n):
        if n == 2:  # run.py uploaded, wrote exit=0 (before we started), moved the job to done/
            upload()
            host.inbox.discard("https_ex.org.json")

    host.on_poll = on_poll
    seen, cb = await progress_recorder()
    s = make()
    res = await s.resume(coll(5), datetime.now(UTC) - timedelta(minutes=5), cb)
    assert _drops(s) == [] and seen[-1] == {"resumed": True, "finished_while_down": True}
    assert json.loads(_docs_text(res))[0]["url"] == "https://ex.org/a"
