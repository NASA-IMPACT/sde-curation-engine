"""Scrape backends: run the crawl4ai scraper locally (subprocess) or remotely (EC2 via SSM).

Both produce the same thing: a `DocumentSource` for the crawler's documents JSON
(array of {url,title,full_text,content_type,seed,host,depth}), the failures JSONL
(one {url,reason,status,detail,…} per URL the crawler could not fetch) and the failure summary.

A source is opened, read once and closed — `ingest_dump` streams it into PostgreSQL and never
holds more than a few hundred pages. The remote backend's source is the S3 object itself, so a
crawl goes S3 → COPY without being written down anywhere on the way: parking it on DATA_DIR meant
a multi-GB copy of every collection's crawl sitting on the shared EFS mount for ever, read twice
and then never again.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, Protocol

import ijson

from ..config import Settings
from ..models import Collection, crawl_file_stem

ProgressCb = Callable[[dict[str, Any]], Awaitable[None]]

# crawler log lines: "  12    ok         1      https://..."  and, every 25 docs,
# "  ... 25 docs / 3 failures logged  (cap 100)" (v1 wrote "3 failed"). Document statuses are
# ok/pdf/plain; every other status (fail, empty, challenge, timeout, skip, ...) is a failure.
_PAGE_RE = re.compile(r"^\s+(\d+)\s+([a-z]+)\s+\d+\s+\S")
_HEARTBEAT_RE = re.compile(r"^\s+\.\.\.\s+(\d+) docs / (\d+) (?:failed|failures logged)")
_EXIT_RE = re.compile(r"^# exit=(\d+)")
_ERROR_RE = re.compile(r"^# ERROR: (.*)")


class ScrapeError(RuntimeError):
    pass


class DocumentSource(Protocol):
    """Somewhere the crawl's documents JSON can be read from as a byte stream."""

    def open(self) -> IO[bytes]:
        """A fresh reader positioned at the start. The caller closes it."""

    def describe(self) -> str:
        """Where this is, for an error message."""


@dataclass(frozen=True)
class FileDocuments:
    """A file on this host — the local crawler's own output, which it owns and we only read."""

    path: Path

    def open(self) -> IO[bytes]:
        return self.path.open("rb")

    def describe(self) -> str:
        return str(self.path)

    def exists(self) -> bool:
        return self.path.is_file()


@dataclass(frozen=True)
class S3Documents:
    """An object in the crawler's bucket, read straight into the ingest.

    `open()` issues a fresh GET, so the object is never copied to disk on the way to PostgreSQL.
    The body is a single long-lived HTTP stream: a crawl is read exactly once (`replace_dump` does
    the duplicate-spelling pass from a temp table, not from a second read of this), and a stream
    that breaks fails the scrape job, which is re-runnable — the crawl stays in S3 either way."""

    s3: Any
    bucket: str
    key: str

    def open(self) -> IO[bytes]:
        return self.s3.get_object(Bucket=self.bucket, Key=self.key)["Body"]

    def describe(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


@dataclass
class ScrapeResult:
    documents: DocumentSource
    summary: dict[str, Any] = field(default_factory=dict)
    external_ref: str | None = None
    crawled_at: datetime | None = None  # when the documents were produced (reused crawls); None = now
    # The crawler's failures JSONL, when it produced one. Unlike the documents this is read whole:
    # it is one short line per URL the crawl could not fetch, and the diff wants all of them at once.
    failures_source: DocumentSource | None = None

    def failures(self) -> list[dict[str, Any]]:
        if self.failures_source is None:
            return []
        if isinstance(self.failures_source, FileDocuments) and not self.failures_source.exists():
            return []
        with self.failures_source.open() as fh:
            return parse_failures(fh.read())

    def capped(self, documents: int, fallback_max_pages: int | None = None) -> bool:
        """Did the crawl stop at its page cap? The summary knows the cap the crawler ran with;
        without a summary (local runs that died before writing one) fall back to the job's."""
        cap = self.summary.get("max_pages") or fallback_max_pages
        return bool(cap) and documents >= int(cap)


@dataclass
class ExistingCrawl:
    """A documents file the crawler already produced for this collection, wherever it lives."""

    modified: datetime
    where: str
    size: int | None = None
    # False when the file is a mid-run checkpoint (crawler v2 rewrites the S3 documents object every
    # N pages / M seconds) rather than the output of a finished crawl: loading it would ingest a
    # truncated collection, so the UI must not offer it and fetch_existing must refuse it.
    complete: bool = True


def crawl_complete(documents_modified: datetime, summary_modified: datetime | None) -> bool:
    """Is the S3 documents object a finished crawl? run.py uploads documents, then the failures log,
    then the failure summary — and the summary only exists once the crawl (retry pass included)
    is over. So the object is final iff the summary is at least as new as it. A newer documents
    object is a checkpoint of a crawl still running (or one that died before finishing); no
    summary at all means the collection's first crawl has not finished yet. Both timestamps have
    1-second resolution, hence >= rather than >."""
    return summary_modified is not None and summary_modified >= documents_modified


class ScrapeBackend(Protocol):
    name: str

    async def run(self, collection: Collection, on_progress: ProgressCb) -> ScrapeResult: ...

    async def existing(self, collection: Collection) -> ExistingCrawl | None: ...

    async def fetch_existing(self, collection: Collection, on_progress: ProgressCb) -> ScrapeResult: ...

    async def resume(self, collection: Collection, since: datetime, on_progress: ProgressCb) -> ScrapeResult: ...


def build_job(collection: Collection) -> dict[str, Any]:
    """Job JSON in the shape sde_crawler.job.merge_job expects. The crawler names its output files
    after collection_id when one is given, so send the seed-derived stem the engine looks them up by."""
    return {
        "seed": collection.seed_url,
        "collection_id": crawl_file_stem(collection.seed_url),
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

    def feed_tail(self, lines: list[str]) -> bool:
        """Feed a `tail -n N` snapshot that overlaps the previous one: page lines carry their
        sequence number, so anything at or below what we already counted is skipped."""
        changed = False
        for line in lines:
            if (m := _PAGE_RE.match(line)) and int(m.group(1)) <= self.processed:
                continue
            changed |= self.feed(line)
        return changed

    def snapshot(self) -> dict[str, Any]:
        return {"processed": self.processed, "docs": self.ok, "failed": self.failed}


def iter_documents(source: DocumentSource) -> Iterator[dict[str, Any]]:
    """The crawl's documents, one at a time. The source is a single JSON array with the full text
    of up to 100k pages; read whole and parsed whole it was held in memory three times over during
    ingest, which is what decided the engine's memory size. Read once, forwards only — an S3 body
    cannot be rewound, so the opening bracket is checked from a buffer rather than by seeking."""
    where = source.describe()
    with source.open() as raw:
        head = raw.read(256)
        if not head.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"["):
            raise ScrapeError(f"documents file is not a JSON array: {where}")
        try:
            yield from ijson.items(_Pushback(raw, head), "item", use_float=True)
        except ijson.JSONError as e:
            raise ScrapeError(f"documents file is not valid JSON: {where} ({e})") from e


class _Pushback:
    """`raw` with `head` put back in front of it. An S3 body is forwards-only — it cannot be
    seeked back to 0 after the opening bracket has been sniffed — and this is the whole of what
    ijson asks of a stream (`read(n)`), so it costs one small object instead of a second GET."""

    def __init__(self, raw: IO[bytes], head: bytes):
        self._raw, self._head = raw, head

    def read(self, size: int = -1) -> bytes:
        if not self._head:
            return self._raw.read(size)
        if size is None or size < 0:
            out, self._head = self._head + self._raw.read(), b""
            return out
        out, self._head = self._head[:size], self._head[size:]
        if len(out) < size:
            out += self._raw.read(size - len(out))
        return out


def parse_failures(data: bytes) -> list[dict[str, Any]]:
    """The crawler's failures JSONL: one object per line; a bad line is skipped, not fatal
    (the file is a log, and losing one record only turns a kept row into a removal)."""
    out: list[dict[str, Any]] = []
    for line in data.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict) and rec.get("url") and rec.get("reason"):
            out.append(rec)
    return out


# ── local subprocess ───────────────────────────────────────────────────


class LocalSubprocessScraper:
    name = "local"

    async def resume(self, collection: Collection, since: datetime, on_progress: ProgressCb) -> ScrapeResult:
        raise ScrapeError("a local crawl cannot be picked up after an engine restart — run Scrape again")

    def __init__(self, settings: Settings):
        self.s = settings
        self.root = settings.crawler_root
        self.python = settings.resolved_crawler_python

    def _paths(self, collection: Collection) -> dict[str, Path]:
        stem = crawl_file_stem(collection.seed_url)
        return {
            # run.py names the job log after the job file
            "job": self.s.data_dir / "scrape_jobs" / f"{stem}.json",
            "log": self.root / "logs" / "jobs" / f"{stem}.log",
            "docs": self.root / "output" / "collections" / f"{stem}.json",
            "failures": self.root / "logs" / "collections" / f"{stem}_failures.jsonl",
            "summary": self.root / "logs" / "collections" / f"{stem}_failures_summary.json",
        }

    async def existing(self, collection: Collection) -> ExistingCrawl | None:
        p = self._paths(collection)["docs"]
        if not p.is_file():
            return None
        st = p.stat()
        return ExistingCrawl(modified=datetime.fromtimestamp(st.st_mtime, UTC), where=str(p), size=st.st_size)

    async def fetch_existing(self, collection: Collection, on_progress: ProgressCb) -> ScrapeResult:
        ex = await self.existing(collection)
        if ex is None:
            raise ScrapeError("no existing crawl output to load — run the crawler")
        await on_progress({"reused": True})
        p = self._paths(collection)
        summary: dict[str, Any] = {}
        if p["summary"].is_file():
            summary = json.loads(p["summary"].read_text(encoding="utf-8"))
        return ScrapeResult(documents=FileDocuments(p["docs"]), summary=summary, external_ref="reused",
                            crawled_at=ex.modified, failures_source=FileDocuments(p["failures"]))

    async def run(self, collection: Collection, on_progress: ProgressCb) -> ScrapeResult:
        if not (self.root / "run.py").is_file():
            raise ScrapeError(f"crawler not found: {self.root / 'run.py'} (CRAWLER_ROOT)")
        if not self.python.is_file():
            raise ScrapeError(f"crawler python not found: {self.python} (CRAWLER_PYTHON)")

        p = self._paths(collection)
        p["job"].parent.mkdir(parents=True, exist_ok=True)
        p["job"].write_text(json.dumps(build_job(collection), indent=2), encoding="utf-8")
        # run.py truncates the log on start; remove stale outputs so we never ingest an old crawl
        p["docs"].unlink(missing_ok=True)
        p["log"].unlink(missing_ok=True)
        p["failures"].unlink(missing_ok=True)

        # never pass --bucket: since crawler v2, run.py deletes the local documents file after
        # a successful S3 upload, and this backend reads that file
        cmd = [str(self.python), "run.py", "--job", str(p["job"])]
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
        return ScrapeResult(documents=FileDocuments(p["docs"]), summary=summary, external_ref=str(proc.pid),
                            failures_source=FileDocuments(p["failures"]))

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


@dataclass
class RemotePoll:
    """One look at the crawler host: is our job still in the inbox, has the crawler touched
    our log since we submitted, and what are the last log lines."""

    inbox: bool = False  # jobs/incoming/<cid>.json still present (queued or running)
    jobs: int = 0  # *.json files in the inbox, ours included
    watcher: int | None = None  # watch_inbox.sh processes; None if unknown
    log_mtime: datetime | None = None
    tail: list[str] = field(default_factory=list)

    def fresh(self, submitted: datetime) -> bool:
        """True once the crawler has written our log after we dropped the job."""
        return self.log_mtime is not None and self.log_mtime >= submitted


def parse_poll(out: str) -> RemotePoll:
    poll = RemotePoll()
    tail: list[str] = []
    for line in out.splitlines():
        if line.startswith("@@inbox="):
            poll.inbox = line[8:].strip() == "1"
        elif line.startswith("@@jobs="):
            poll.jobs = _int(line[7:])
        elif line.startswith("@@watcher="):
            v = line[10:].strip()
            poll.watcher = _int(v) if v else None
        elif line.startswith("@@mtime="):
            v = line[8:].strip()
            poll.log_mtime = datetime.fromtimestamp(int(v), UTC) if v.isdigit() else None
        else:
            tail.append(line)
    poll.tail = tail
    return poll


def _int(v: str) -> int:
    try:
        return int(v.strip())
    except ValueError:
        return 0


class SsmRemoteScraper:
    """Port of scripts/drop_job.sh: write the job JSON into the EC2 inbox through SSM,
    then wait for the job log to end with `# exit=0` and download the documents object.

    The documents object is *not* a completion signal: since crawler v2 run.py re-uploads it
    as a checkpoint every N pages / M seconds, so mid-run it holds a partial array. Only the
    log's exit line says the crawl (including its retry pass) is finished.

    The crawler's watch_inbox.sh runs one run.py at a time under flock, and run.py only
    picks up the inbox files that exist when it starts — so a job dropped while a batch is
    running waits for the whole batch. We therefore track two phases:

    * queued  — our job file is in the inbox and our log has not been touched since we
      submitted (or, when a job for the collection was already in the inbox, since we attached
      to it — we never drop a duplicate). No clock runs: a queue can legitimately be days long. Only a dead watcher
      (or the job file disappearing) fails it; the UI shows how long it has waited.
    * running — the crawler rewrote our log after submission. The stall clock
      (INDEX_STALL_TIMEOUT_S) restarts on every change to the log tail.

    The log is only read once it is fresh, so a previous run's `# exit=1` cannot fail a new job."""

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
        cid = job["collection_id"]  # the crawl file stem (build_job); run.py names the job log after this file
        inbox = self.s.crawler_remote_inbox
        return (
            "set -euo pipefail\n"
            f"cat > {inbox}/{cid}.json <<'JOB'\n{json.dumps(job)}\nJOB\n"
            f"chown ec2-user:ec2-user {inbox}/{cid}.json\n"
        )

    def poll_script(self, cid: str) -> str:
        inbox = self.s.crawler_remote_inbox
        log = f"{self.remote_root}/logs/jobs/{cid}.log"
        return (
            f"[ -f {inbox}/{cid}.json ] && echo '@@inbox=1' || echo '@@inbox=0'\n"
            f"echo \"@@jobs=$(ls {inbox}/*.json 2>/dev/null | wc -l)\"\n"
            "echo \"@@watcher=$(pgrep -fc watch_inbox || true)\"\n"
            f"echo \"@@mtime=$(stat -c %Y {log} 2>/dev/null || true)\"\n"
            f"tail -n 5 {log} 2>/dev/null || true\n"
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

    def _key(self, rel: str) -> str:
        prefix = self.s.crawler_s3_prefix.strip("/")
        return f"{prefix}/{rel}" if prefix else rel

    def _crawl_keys(self, collection: Collection) -> dict[str, str]:
        """S3 keys of the crawler's output for this collection's seed."""
        stem = crawl_file_stem(collection.seed_url)
        return {
            "docs": self._key(f"scraped_collections/{stem}.json"),
            "failures": self._key(f"failure_logs/{stem}_failures.jsonl"),
            "summary": self._key(f"failure_logs/{stem}_failures_summary.json"),
        }

    async def existing(self, collection: Collection) -> ExistingCrawl | None:
        keys = self._crawl_keys(collection)
        key = keys["docs"]
        try:
            r = await asyncio.to_thread(self.s3.head_object, Bucket=self.s.crawler_s3_bucket, Key=key)
        except self.s3.exceptions.ClientError:
            return None
        summary = await self._head(keys["summary"])
        return ExistingCrawl(modified=r["LastModified"], where=f"s3://{self.s.crawler_s3_bucket}/{key}",
                             size=r.get("ContentLength"),
                             complete=crawl_complete(r["LastModified"], summary[1] if summary else None))

    async def fetch_existing(self, collection: Collection, on_progress: ProgressCb) -> ScrapeResult:
        ex = await self.existing(collection)
        if ex is None:
            raise ScrapeError(f"no existing crawl in {ex.where if ex else self.s.crawler_s3_bucket} — run the crawler")
        if not ex.complete:
            raise ScrapeError(
                f"{ex.where} is a checkpoint written at {ex.modified:%Y-%m-%d %H:%M}Z by a crawl that has not "
                "finished (its failure summary is older or missing): the crawler is still working on this "
                "collection, or died mid-run. Wait for it to finish, or run the crawler."
            )
        await on_progress({"reused": True})
        result = await self._resolve(collection)
        result.external_ref, result.crawled_at = "reused", ex.modified
        return result

    async def _resolve(self, collection: Collection) -> ScrapeResult:
        """Point the ingest at the crawl in S3 — nothing is downloaded here.

        The documents object is GBs of JSON per collection. Copying it to DATA_DIR gave every
        collection a permanent second copy on the shared EFS mount that nothing ever read again
        (`existing` heads S3, `fetch_existing` comes back through here), and put an NFS round trip
        between the crawl and PostgreSQL. `ingest_dump` reads this stream exactly once, straight
        into a COPY. Only the two small objects — the failure summary and the failures log — are
        fetched now, because the diff wants all of their contents at once anyway."""
        keys = self._crawl_keys(collection)
        bucket = self.s.crawler_s3_bucket
        summary: dict[str, Any] = {}
        try:
            obj = await asyncio.to_thread(self.s3.get_object, Bucket=bucket, Key=keys["summary"])
            summary = json.loads(obj["Body"].read())
        except self.s3.exceptions.ClientError:
            pass
        # a crawl with no failures uploads no log; never pair this dump with an older crawl's
        failures = S3Documents(self.s3, bucket, keys["failures"])
        if await self._head(keys["failures"]) is None:
            failures = None
        return ScrapeResult(documents=S3Documents(self.s3, bucket, keys["docs"]), summary=summary,
                            failures_source=failures)

    async def _poll(self, cid: str) -> RemotePoll | None:
        status, out = await self._invocation(await self._send(self.poll_script(cid)))
        return parse_poll(out) if status == "Success" else None

    async def resume(self, collection: Collection, since: datetime, on_progress: ProgressCb) -> ScrapeResult:
        """Pick a crawl back up after the engine restarted under it (a deploy cancels the watcher;
        the crawler host never hears of it and carries on). Never submits a new crawl: the job is
        followed if its file is still in the inbox, ingested if it finished while the engine was
        down (a complete upload newer than `since`, when the crawl was started), else it failed."""
        return await self.run(collection, on_progress, resume_since=since)

    async def run(self, collection: Collection, on_progress: ProgressCb, *,
                  resume_since: datetime | None = None) -> ScrapeResult:
        cid = crawl_file_stem(collection.seed_url)  # inbox job file and job log name
        docs_key = self._crawl_keys(collection)["docs"]
        before = await self._head(docs_key)
        # S3 LastModified and the remote log mtime have 1-second resolution: floor our own
        # timestamp so a write landing in the same second still counts.
        submitted = datetime.now(UTC).replace(microsecond=0)

        # A job file for this collection already in the inbox is a crawl queued or running on the
        # host (run.py moves it to jobs/done only when it finishes). Dropping ours would overwrite
        # that file and make the watcher crawl the site a second time after the current batch —
        # so follow the job that is already there instead.
        already = await self._poll(cid)
        while already is None and resume_since is not None:  # SSM hiccup: must not guess on a resume
            await asyncio.sleep(self.s.scrape_poll_interval_s)
            already = await self._poll(cid)
        if already is not None and already.inbox:
            cmd_id = None
            await on_progress({"attached": True, "processed": 0, "docs": 0, "failed": 0, "queued": True,
                               **({"resumed": True} if resume_since is not None else {})})
        elif resume_since is not None:
            # the job file has left the inbox: the crawl ended while the engine was down
            ex = await self.existing(collection)
            if ex is None or not ex.complete or ex.modified < resume_since:
                raise ScrapeError(
                    "the crawl ended while the engine was restarting and left no finished upload"
                    f" newer than {resume_since:%Y-%m-%d %H:%M}Z (it failed on the crawler host) — run Scrape again"
                )
            await on_progress({"resumed": True, "finished_while_down": True})
            result = await self._resolve(collection)
            result.external_ref, result.crawled_at = "resumed", ex.modified
            return result
        else:
            cmd_id = await self._send(self.remote_script(build_job(collection)))
            status, out = await self._invocation(cmd_id)
            if status != "Success":
                raise ScrapeError(f"SSM job drop {status}: {out[-400:]}")
            await on_progress({"ssm_command": cmd_id, "processed": 0, "docs": 0, "failed": 0, "queued": True})

        def uploaded(now: tuple[str, datetime] | None) -> bool:
            return now is not None and now != before and (before is None or now[1] >= submitted)

        progress = LogProgress()
        queued = True
        last_activity = time.monotonic()
        while True:
            await asyncio.sleep(self.s.scrape_poll_interval_s)
            poll = await self._poll(cid)
            if poll is None:  # SSM hiccup: neither evidence of life nor of death
                continue
            if not poll.fresh(submitted):
                if not poll.inbox:
                    raise ScrapeError(
                        "job file vanished from the crawler inbox before the crawl started"
                    )
                if poll.watcher == 0:
                    raise ScrapeError(
                        "crawler inbox watcher (watch_inbox.sh) is not running; job left in the inbox"
                    )
                await on_progress({"queued": True, "queue_ahead": max(poll.jobs - 1, 0)})
                continue
            if queued:  # first sign of the crawler working on our job
                queued = False
                last_activity = time.monotonic()
                await on_progress({"queued": False, "queue_ahead": 0})
            changed = progress.feed_tail(poll.tail)
            if progress.exit_code == 1:
                raise ScrapeError(
                    f"remote crawler failed: {progress.error or ' '.join(poll.tail)[-400:]}"
                )
            if changed:
                last_activity = time.monotonic()
                await on_progress(progress.snapshot())
            if progress.exit_code == 0:
                # run.py's final upload precedes exit=0, so the object is complete or absent
                if uploaded(await self._head(docs_key)):
                    break
                raise ScrapeError(
                    "remote crawler finished but uploaded no documents object "
                    "(is SDE_S3_BUCKET set on the crawler host?)"
                )
            stalled = time.monotonic() - last_activity
            if stalled > self.s.index_stall_timeout_s:
                raise ScrapeError(
                    f"remote crawl stalled: no log activity for {stalled / 3600:.1f}h"
                )

        result = await self._resolve(collection)
        result.external_ref = cmd_id or "attached"
        return result


def make_scrape_backend(settings: Settings) -> ScrapeBackend:
    if settings.scrape_backend == "ssm":
        return SsmRemoteScraper(settings)
    return LocalSubprocessScraper(settings)
