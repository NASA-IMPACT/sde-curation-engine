"""Fakes for the remote (SSM) crawler backend and the AWS fixtures its tests share: a fake crawler
EC2 host and SSM client, moto's in-memory AWS (`aws`), and `ssm_env` (an S3 bucket plus a factory
for the remote scraper)."""

from __future__ import annotations

import json
import time

import pytest

from sde_curation.backends.scrape import SsmRemoteScraper
from sde_curation.config import Settings


def _docs_text(res) -> str:
    """The crawl a ScrapeResult points at, read back. A source is a file (local backend) or the
    S3 object itself (remote): either way it is opened, not copied to disk."""
    with res.documents.open() as fh:
        return fh.read().decode()


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
