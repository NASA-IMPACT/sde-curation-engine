"""Scrape backends: run the crawl4ai scraper locally (subprocess) or remotely (EC2 via SSM).

Both produce the same thing: a path to the crawler's documents JSON
(array of {url,title,full_text,content_type,seed,host,depth}) plus the failure summary.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from ..config import Settings
from ..models import Collection

ProgressCb = Callable[[dict[str, Any]], Awaitable[None]]

# crawler log lines: "  12    ok         1      https://..."  and  "  ... 25 docs / 3 failed  (cap 100)"
_PAGE_RE = re.compile(r"^\s+(\d+)\s+(ok|pdf|plain|fail|empty|challenge)\s+\d+\s+\S")
_HEARTBEAT_RE = re.compile(r"^\s+\.\.\.\s+(\d+) docs / (\d+) failed")
_EXIT_RE = re.compile(r"^# exit=(\d+)")
_ERROR_RE = re.compile(r"^# ERROR: (.*)")


class ScrapeError(RuntimeError):
    pass


@dataclass
class ScrapeResult:
    documents_path: Path
    summary: dict[str, Any] = field(default_factory=dict)
    external_ref: str | None = None


class ScrapeBackend(Protocol):
    name: str

    async def run(self, collection: Collection, on_progress: ProgressCb) -> ScrapeResult: ...


def build_job(collection: Collection) -> dict[str, Any]:
    """Job JSON in the shape sde_crawler.job.merge_job expects."""
    return {
        "seed": collection.seed_url,
        "collection_id": collection.collection_id,
        "max_pages": collection.max_pages,
    }


class LogProgress:
    """Incremental parser for logs/jobs/<stem>.log. Feed lines; read .snapshot()."""

    def __init__(self) -> None:
        self.processed = 0
        self.ok = 0
        self.failed = 0
        self.exit_code: int | None = None
        self.error: str | None = None

    def feed(self, line: str) -> bool:
        """Return True if the snapshot changed."""
        if m := _PAGE_RE.match(line):
            self.processed = int(m.group(1))
            if m.group(2) in ("ok", "pdf", "plain"):
                self.ok += 1
            else:
                self.failed += 1
            return True
        if m := _HEARTBEAT_RE.match(line):
            self.ok, self.failed = int(m.group(1)), int(m.group(2))
            return True
        if m := _ERROR_RE.match(line):
            self.error = m.group(1)
            return True
        if m := _EXIT_RE.match(line):
            self.exit_code = int(m.group(1))
            return True
        return False

    def snapshot(self) -> dict[str, Any]:
        return {"processed": self.processed, "docs": self.ok, "failed": self.failed}


def parse_documents(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ScrapeError(f"documents file is not a JSON array: {path}")
    return data


# ── local subprocess ───────────────────────────────────────────────────


class LocalSubprocessScraper:
    name = "local"

    def __init__(self, settings: Settings):
        self.s = settings
        self.root = settings.crawler_root
        self.python = settings.resolved_crawler_python

    def _paths(self, collection_id: str) -> dict[str, Path]:
        return {
            "job": self.s.data_dir / "scrape_jobs" / f"{collection_id}.json",
            "log": self.root / "logs" / "jobs" / f"{collection_id}.log",
            "docs": self.root / "output" / "collections" / f"{collection_id}.json",
            "summary": self.root / "logs" / "collections" / f"{collection_id}_failures_summary.json",
        }

    async def run(self, collection: Collection, on_progress: ProgressCb) -> ScrapeResult:
        if not (self.root / "run.py").is_file():
            raise ScrapeError(f"crawler not found: {self.root / 'run.py'} (CRAWLER_ROOT)")
        if not self.python.is_file():
            raise ScrapeError(f"crawler python not found: {self.python} (CRAWLER_PYTHON)")

        p = self._paths(collection.collection_id)
        p["job"].parent.mkdir(parents=True, exist_ok=True)
        p["job"].write_text(json.dumps(build_job(collection), indent=2), encoding="utf-8")
        # run.py truncates the log on start; remove stale outputs so we never ingest an old crawl
        p["docs"].unlink(missing_ok=True)
        p["log"].unlink(missing_ok=True)

        cmd = [str(self.python), "run.py", "--job", str(p["job"])]
        if self.s.crawler_s3_bucket:
            cmd += ["--bucket", self.s.crawler_s3_bucket]
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=self.root, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await on_progress({"pid": proc.pid, "processed": 0, "docs": 0, "failed": 0})

        tail = asyncio.create_task(self._tail(p["log"], on_progress, proc))
        try:
            stdout, stderr = await proc.communicate()
        except asyncio.CancelledError:
            proc.kill()
            await proc.wait()
            raise
        finally:
            tail.cancel()
        progress = LogProgress()
        if p["log"].is_file():
            for line in p["log"].read_text(encoding="utf-8", errors="replace").splitlines():
                progress.feed(line)
        await on_progress(progress.snapshot())

        if proc.returncode != 0:
            detail = progress.error or stderr.decode(errors="replace")[-800:].strip() or stdout.decode(
                errors="replace"
            )[-400:].strip()
            raise ScrapeError(f"crawler exited {proc.returncode}: {detail}")
        if not p["docs"].is_file():
            raise ScrapeError(f"crawler exited 0 but no documents file at {p['docs']}")

        summary: dict[str, Any] = {}
        if p["summary"].is_file():
            summary = json.loads(p["summary"].read_text(encoding="utf-8"))
        return ScrapeResult(documents_path=p["docs"], summary=summary, external_ref=str(proc.pid))

    async def _tail(self, log: Path, on_progress: ProgressCb, proc: asyncio.subprocess.Process) -> None:
        """Poll the crawler's job log and push progress snapshots when they change."""
        progress = LogProgress()
        pos = 0
        interval = min(2.0, self.s.scrape_poll_interval_s)
        while proc.returncode is None:
            await asyncio.sleep(interval)
            if not log.is_file():
                continue
            with log.open("r", encoding="utf-8", errors="replace") as f:
                f.seek(pos)
                chunk = f.read()
                pos = f.tell()
            changed = False
            for line in chunk.splitlines():
                changed |= progress.feed(line)
            if changed:
                await on_progress(progress.snapshot())


# ── remote EC2 via SSM ─────────────────────────────────────────────────


class SsmRemoteScraper:
    """Port of scripts/drop_job.sh: write the job JSON into the EC2 inbox through SSM,
    then wait for the documents object to appear in S3 (uploaded by run.py on success).
    Failure is detected from the tail of the remote job log."""

    name = "ssm"

    def __init__(self, settings: Settings, *, ssm=None, s3=None):
        self.s = settings
        if not settings.crawler_instance_id or not settings.crawler_s3_bucket:
            raise ScrapeError("SSM backend needs CRAWLER_INSTANCE_ID and CRAWLER_S3_BUCKET")
        import boto3

        self.ssm = ssm or boto3.client("ssm", region_name=settings.aws_region)
        self.s3 = s3 or boto3.client("s3", region_name=settings.aws_region)
        self.remote_root = str(Path(settings.crawler_remote_inbox).parent.parent)

    def remote_script(self, job: dict[str, Any]) -> str:
        cid = job["collection_id"]
        inbox = self.s.crawler_remote_inbox
        return (
            "set -euo pipefail\n"
            f"cat > {inbox}/{cid}.json <<'JOB'\n{json.dumps(job)}\nJOB\n"
            f"chown ec2-user:ec2-user {inbox}/{cid}.json\n"
        )

    async def _send(self, script: str) -> str:
        resp = await asyncio.to_thread(
            self.ssm.send_command,
            InstanceIds=[self.s.crawler_instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [script]},
            Comment="sde-curation-engine",
        )
        return resp["Command"]["CommandId"]

    async def _invocation(self, command_id: str) -> tuple[str, str]:
        for _ in range(30):
            try:
                inv = await asyncio.to_thread(
                    self.ssm.get_command_invocation,
                    CommandId=command_id,
                    InstanceId=self.s.crawler_instance_id,
                )
            except self.ssm.exceptions.InvocationDoesNotExist:
                await asyncio.sleep(1)
                continue
            if inv["Status"] in ("Pending", "InProgress", "Delayed"):
                await asyncio.sleep(1)
                continue
            return inv["Status"], inv.get("StandardOutputContent", "")
        return "TimedOut", ""

    async def _head(self, key: str) -> tuple[str, datetime] | None:
        """(ETag, LastModified) or None if absent."""
        try:
            r = await asyncio.to_thread(self.s3.head_object, Bucket=self.s.crawler_s3_bucket, Key=key)
        except self.s3.exceptions.ClientError:
            return None
        return r["ETag"], r["LastModified"]

    async def run(self, collection: Collection, on_progress: ProgressCb) -> ScrapeResult:
        cid = collection.collection_id
        docs_key = f"scraped_collections/{cid}.json"
        summary_key = f"failure_logs/{cid}_failures_summary.json"
        before = await self._head(docs_key)
        # S3 LastModified has 1-second resolution: floor our own timestamp so an upload
        # landing in the same second still counts, and also accept any ETag change.
        submitted = datetime.now(UTC).replace(microsecond=0)

        cmd_id = await self._send(self.remote_script(build_job(collection)))
        status, out = await self._invocation(cmd_id)
        if status != "Success":
            raise ScrapeError(f"SSM job drop {status}: {out[-400:]}")
        await on_progress({"ssm_command": cmd_id, "processed": 0, "docs": 0, "failed": 0})

        log = f"{self.remote_root}/logs/jobs/{cid}.log"
        progress = LogProgress()
        t0 = time.monotonic()
        while True:
            await asyncio.sleep(self.s.scrape_poll_interval_s)
            now = await self._head(docs_key)
            if now is not None and now != before and (before is None or now[1] >= submitted):
                break
            tail_id = await self._send(f"tail -n 5 {log} 2>/dev/null || true")
            _, tail = await self._invocation(tail_id)
            changed = False
            for line in tail.splitlines():
                changed |= progress.feed(line)
            if progress.exit_code == 1:
                raise ScrapeError(f"remote crawler failed: {progress.error or tail[-400:]}")
            if changed:
                await on_progress(progress.snapshot())
            if time.monotonic() - t0 > self.s.index_stall_timeout_s:
                raise ScrapeError("remote crawl timed out waiting for S3 documents object")

        local = self.s.data_dir / "scrapes" / f"{cid}.json"
        local.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(
            self.s3.download_file, self.s.crawler_s3_bucket, docs_key, str(local)
        )
        summary: dict[str, Any] = {}
        try:
            obj = await asyncio.to_thread(
                self.s3.get_object, Bucket=self.s.crawler_s3_bucket, Key=summary_key
            )
            summary = json.loads(obj["Body"].read())
        except self.s3.exceptions.ClientError:
            pass
        return ScrapeResult(documents_path=local, summary=summary, external_ref=cmd_id)


def make_scrape_backend(settings: Settings) -> ScrapeBackend:
    if settings.scrape_backend == "ssm":
        return SsmRemoteScraper(settings)
    return LocalSubprocessScraper(settings)
