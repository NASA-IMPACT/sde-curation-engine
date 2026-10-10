"""Doubles for unit tests of the job manager (sde_curation/jobs.py) on the in-memory FakeDatabase.

An engine restart is two JobManagers on one FakeDatabase: `restart` shuts the first down in the middle
of a job (a deploy) and starts a second that recovers and resumes it. Backends are fakes: `Indexer`
records what it started and stopped; `Calls` wraps a model call, records each page asked, can hold
calls until the engine goes down under them, and can make some pages fail.
"""

import asyncio
import json
from pathlib import Path
from typing import Any

from sde_curation.backends.index import Dispatch
from sde_curation.backends.scrape import FileDocuments, ScrapeResult
from sde_curation.config import Settings
from sde_curation.curation import CurationService
from sde_curation.engine.export import status_prefix
from sde_curation.events import EventBus
from sde_curation.jobs import JobManager
from sde_curation.llm.base import LLMError
from sde_curation.llm.fake import FakeProvider
from sde_curation.models import (
    Collection,
    ConnectorType,
    CuratedUrl,
    Division,
    DocumentType,
    DumpUrl,
    IndexRun,
    JobRun,
    JobState,
)

CID = "example.org"
BUCKET = "cosmos-idx"


def make_engine(db, tmp_path, *, indexer=None, publisher=None, scraper=None, **over) -> JobManager:
    s = Settings(**{"data_dir": tmp_path, "llm_provider": "fake", "llm_retry_delay_s": 0, "resume_start_delay_s": 0,
                    "resume_stagger_s": 0, "cosmos_index_bucket": BUCKET, "index_poll_interval_s": 0.01,
                    "llm_workers": 1, **over})
    return JobManager(s, db, EventBus(), scraper=scraper, llm=lambda: FakeProvider(), indexer=indexer, publisher=publisher)


async def restart(old: JobManager, db, tmp_path, **kw) -> JobManager:
    """A deploy: the old engine shuts down under its jobs; a new one recovers and resumes them."""
    await old.shutdown()
    new = make_engine(db, tmp_path, **kw)
    await new.recover()
    new.start_resumes()
    return new


async def until(check, timeout: float = 10.0):
    for _ in range(int(timeout / 0.01)):
        if got := await check():
            return got
        await asyncio.sleep(0.01)
    raise AssertionError("timed out")


async def finished(db, job_id: int) -> JobRun:
    async def done():
        j = await db.get_job(job_id)
        return j if j.state in (JobState.SUCCEEDED, JobState.FAILED) else None
    return await until(done)


async def collection(db, *, titles: list[str]) -> Collection:
    """A collection with one dump page per title, recomputed (Start curating)."""
    c = await db.insert_collection(Collection(collection_id=CID, name=CID, seed_url=f"https://{CID}",
                                              connector=ConnectorType.CRAWLER, max_pages=1000,
                                              division=Division.HELIOPHYSICS))
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=f"https://{CID}/p{i:03d}", scraped_title=t,
                                        full_text=f"page {i}") for i, t in enumerate(titles, 1)])
    await CurationService(db).recompute(await db.get_collection(CID))
    return await db.get_collection(c.collection_id)


class Calls:
    """Wraps a model call: records each page asked, can hold every call after the first `hold_after`
    until the engine goes down under it, and can make some pages fail."""

    def __init__(self, fn, *, hold_after: int | None = None, fail: set[str] = frozenset()):
        self.fn, self.hold_after, self.fail = fn, hold_after, fail
        self.asked: list[str] = []
        self.held = asyncio.Event()

    async def __call__(self, llm, d, **kw):
        url = d["url"]
        self.asked.append(url)
        if self.hold_after is not None and len(self.asked) > self.hold_after:
            self.held.set()
            await asyncio.Event().wait()
        if url in self.fail:
            raise LLMError("the model could not read this page")
        return await self.fn(llm, d, **kw)


class Indexer:
    """An ECS indexer: `dispatch` starts a task at once, but its answer can be slow to come back."""

    name = "fake-ecs"

    def __init__(self, *, slow_first_answer: bool = False):
        self.started: list[str] = []
        self.stopped: list[str] = []
        self.slow_first_answer = slow_first_answer
        self.dispatching = asyncio.Event()

    async def dispatch(self, c, run_id, target, *, allow_high_deletion=False) -> Dispatch:
        self.started.append(run_id)  # the task is running from here on
        if self.slow_first_answer and len(self.started) == 1:
            self.dispatching.set()
            await asyncio.Event().wait()
        return Dispatch(f"task/{len(self.started)}", {})

    async def still_running(self, d) -> bool:
        return True

    async def kill(self, d) -> None:
        self.stopped.append(d.external_ref)


async def promoted(db, *, titles: list[str], excluded: bool = False) -> Collection:
    """`collection` with every page promoted (curated), titled, ready to index."""
    await collection(db, titles=titles)
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=f"https://{CID}/p{i:03d}", title=t,
                                              full_text=f"page {i}", excluded=excluded,
                                              division=Division.HELIOPHYSICS,
                                              document_type=DocumentType.DOCUMENTATION)
                                   for i, t in enumerate(titles, 1)])
    return await db.get_collection(CID)


class Scraper:
    """A ScrapeBackend (run / fetch_existing / resume / existing). Each call reports `progress`, then
    raises `fail`, or holds until the engine goes down (`hold`), or returns `docs` written as the
    crawl's documents file plus `failures` as its failures log. `calls` records which method ran."""

    name = "fake-scraper"

    def __init__(self, tmp_path: Path, docs: list[dict[str, Any]], *, failures: list[dict[str, Any]] = (),
                 progress: list[dict[str, Any]] = (), fail: Exception | None = None, hold: bool = False,
                 summary: dict[str, Any] | None = None):
        self.dir, self.docs, self.failures, self.progress = tmp_path, docs, list(failures), list(progress)
        self.fail, self.hold, self.summary = fail, hold, summary or {}
        self.calls: list[str] = []
        self.since = None
        self.running = asyncio.Event()

    async def _result(self, method: str, on_progress) -> ScrapeResult:
        self.calls.append(method)
        for p in self.progress:
            await on_progress(p)
        self.running.set()
        if self.fail is not None:
            raise self.fail
        if self.hold:
            await asyncio.Event().wait()
        docs, fails = self.dir / "documents.json", self.dir / "failures.jsonl"
        docs.write_text(json.dumps(self.docs))
        fails.write_text("".join(json.dumps(f) + "\n" for f in self.failures))
        return ScrapeResult(documents=FileDocuments(docs), summary=self.summary, external_ref="crawl-1",
                            failures_source=FileDocuments(fails))

    async def run(self, c, on_progress) -> ScrapeResult:
        return await self._result("run", on_progress)

    async def fetch_existing(self, c, on_progress) -> ScrapeResult:
        return await self._result("fetch_existing", on_progress)

    async def resume(self, c, since, on_progress) -> ScrapeResult:
        self.since = since
        return await self._result("resume", on_progress)

    async def existing(self, c):
        return None


def report(expected: int, indexed: int, title_match_rate: float = 1.0) -> dict[str, Any]:
    """A validation report as the indexer and validate_direct write it."""
    return {"expected_count": expected, "indexed_count": indexed, "count_matches": expected == indexed,
            "title_match_rate": title_match_rate}


class StatusIndexer(Indexer):
    """An indexer that finishes as soon as it is dispatched: it writes the run's status.json, and
    validation.json for a test run, to the moto bucket, as WEB_COSMOS does at its end."""

    def __init__(self, *, state: str = "succeeded", error: str | None = None,
                 validation: dict[str, Any] | None = None):
        super().__init__()
        self.state, self.error, self.validation = state, error, validation

    async def dispatch(self, c, run_id, target, *, allow_high_deletion=False) -> Dispatch:
        import boto3

        d = await super().dispatch(c, run_id, target, allow_high_deletion=allow_high_deletion)
        s3, prefix = boto3.client("s3", region_name="us-east-1"), status_prefix(c.collection_key, run_id)
        status = {"run_id": run_id, "collection_key": c.collection_key, "target": target, "state": self.state,
                  "error": self.error, "indexed": c.curated_count, "deleted": 0}
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}/status.json", Body=json.dumps(status))
        if self.validation is not None and target == "test":
            s3.put_object(Bucket=BUCKET, Key=f"{prefix}/validation.json", Body=json.dumps(self.validation))
        return d


class Publisher:
    """The prod publisher: records each run and answers with `result` merged over a success."""

    def __init__(self, **result: Any):
        self.result = result
        self.runs: list[tuple[str, str, str]] = []

    async def run(self, key, run_id, source_run_id, progress, *, allow_high_deletion=False) -> dict[str, Any]:
        self.runs.append((key, run_id, source_run_id))
        return {"run_id": run_id, "collection_key": key, "target": "prod", "state": "succeeded",
                "documents_in_export": 1, "indexed": 1, "changed": 1, "unchanged": 0, "deleted": 0, **self.result}


async def validated_test_run(db, c: Collection, *, exported: int = 1, key: str | None = None) -> IndexRun:
    """The latest test run of `c`, succeeded and validated: what Index to prod publishes from."""
    run = await db.insert_index_run(IndexRun(run_id="t-1", collection_id=c.collection_id, target="test",
                                             exported=exported))
    run.state, run.validation = "succeeded", report(exported, exported)
    run.status = {"collection_key": key or c.collection_key}
    await db.update_index_run(run)
    return run
