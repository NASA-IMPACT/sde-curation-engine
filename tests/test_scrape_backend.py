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
            Bucket="crawl-bkt", Key="scraped_collections/ex.org.json",
            Body=json.dumps(docs or [{"url": "https://ex.org/a", "title": "A", "full_text": "t"}]),
        )
        s3.put_object(
            Bucket="crawl-bkt", Key="failure_logs/ex.org_failures_summary.json",
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
    script = make().poll_script("ex.org")
    assert "/opt/sde-crawler/jobs/incoming/ex.org.json" in script
    assert "/opt/sde-crawler/logs/jobs/ex.org.log" in script and "tail -n 5" in script


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

    assert json.loads(res.documents_path.read_text())[0]["url"] == "https://ex.org/a"
    assert res.summary["documents_scraped"] == 1
    assert seen[0]["ssm_command"] == "cmd-1" and seen[0]["queued"] is True
    assert {"queued": True, "queue_ahead": 2} in seen, seen
    started = seen.index({"queued": False, "queue_ahead": 0})
    assert all(s.get("queued") for s in seen[:started]), "no crawl progress before the log was fresh"
    assert seen[-1] == {"processed": 2, "docs": 2, "failed": 0}
    drop = backend.ssm.commands[0]
    assert drop["Comment"] == "sde-curation-engine"
    assert "jobs/incoming/ex.org.json" in drop["Parameters"]["commands"][0]


async def test_ssm_dead_watcher_fails_fast(ssm_env):
    host, make, _ = ssm_env
    host.watcher = 0
    with pytest.raises(ScrapeError, match="watch_inbox.sh.*not running"):
        await make().run(coll(5), lambda p: asyncio.sleep(0))


async def test_ssm_job_file_vanished(ssm_env):
    host, make, _ = ssm_env
    host.on_poll = lambda n: host.inbox.discard("ex.org.json")
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


async def test_ssm_exit_zero_without_upload(ssm_env):
    host, make, _ = ssm_env
    host.on_poll = lambda n: host.start_crawl(["# s3 skipped (no bucket)", "# exit=0 elapsed_s=1.0"])
    with pytest.raises(ScrapeError, match="uploaded no documents object"):
        await make().run(coll(5), lambda p: asyncio.sleep(0))
