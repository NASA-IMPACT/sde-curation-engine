"""JobManager: runs long work as asyncio background tasks, one at a time per collection,
records explicit success/failure in job_runs, and publishes SSE events."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .backends.scrape import ScrapeBackend, ScrapeError, parse_documents
from .config import Settings
from .db import Database
from .events import EventBus
from .models import Collection, DumpUrl, JobKind, JobRun, JobState, Status, utcnow

log = logging.getLogger(__name__)


class JobConflict(Exception):
    pass


class JobManager:
    def __init__(self, settings: Settings, db: Database, bus: EventBus, *, scraper: ScrapeBackend):
        self.s = settings
        self.db = db
        self.bus = bus
        self.scraper = scraper
        self._tasks: dict[int, asyncio.Task] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._starting: set[str] = set()  # collections with a job being created (TOCTOU guard)

    # ── infrastructure ─────────────────────────────────────────────────

    def lock(self, collection_id: str) -> asyncio.Lock:
        """One lock per collection, shared by scrape ingest and curation writes."""
        return self._locks.setdefault(collection_id, asyncio.Lock())

    _lock = lock

    def active_for(self, collection_id: str) -> JobRun | None:
        for t in self._tasks.values():
            j: JobRun | None = getattr(t, "job", None)
            if j and j.collection_id == collection_id and not t.done():
                return j
        return None

    def _emit(self, c: Collection | str, job: JobRun | None = None) -> None:
        cid = c if isinstance(c, str) else c.collection_id
        data: dict[str, Any] = {"collection_id": cid}
        if not isinstance(c, str):
            data["status"] = c.status
        if job:
            data["job"] = {"id": job.id, "kind": job.kind, "state": job.state, "progress": job.progress}
        self.bus.publish("collection", data)

    async def _spawn(self, job: JobRun, coro) -> JobRun:
        task = asyncio.create_task(coro, name=f"job-{job.id}")
        task.job = job  # type: ignore[attr-defined]
        self._tasks[job.id] = task
        task.add_done_callback(lambda t: self._tasks.pop(job.id, None))
        return job

    async def cancel(self, collection_id: str) -> JobRun | None:
        """Cancel the running job for a collection; waits until it has recorded 'failed'."""
        for jid, t in list(self._tasks.items()):
            j: JobRun | None = getattr(t, "job", None)
            if j and j.collection_id == collection_id and not t.done():
                t.cancel()
                await asyncio.gather(t, return_exceptions=True)
                return await self.db.get_job(jid)
        return None

    async def shutdown(self) -> None:
        for t in list(self._tasks.values()):
            t.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    async def recover(self) -> None:
        """Startup: jobs left 'running' by a previous process are dead — say so explicitly."""
        for j in await self.db.active_jobs():
            await self.db.finish_job(j, JobState.FAILED, error="engine restarted while job was running")
            self._emit(j.collection_id, j)

    # ── scrape ─────────────────────────────────────────────────────────

    async def start_scrape(self, c: Collection) -> JobRun:
        cid = c.collection_id
        if cid in self._starting or self.active_for(cid) or self.lock(cid).locked():
            raise JobConflict(f"a job is already running for {cid}")
        self._starting.add(cid)
        try:
            job = await self.db.insert_job(
                JobRun(collection_id=cid, kind=JobKind.SCRAPE, state=JobState.RUNNING)
            )
            self._emit(c, job)
            return await self._spawn(job, self._run_scrape(c, job))
        finally:
            self._starting.discard(cid)

    async def _run_scrape(self, c: Collection, job: JobRun) -> None:
        async with self._lock(c.collection_id):
            try:
                async def on_progress(p: dict[str, Any]) -> None:
                    job.progress = {**job.progress, **p}
                    if "pid" in p or "ssm_command" in p:
                        job.external_ref = str(p.get("pid") or p.get("ssm_command"))
                    await self.db.update_job(job)
                    self._emit(c, job)

                result = await self.scraper.run(c, on_progress)
                docs = parse_documents(result.documents_path)
                n = await self.ingest_dump(c.collection_id, docs)
                # deltas computed against the previous dump are now meaningless
                await self.db.replace_deltas(c.collection_id, [], [])
                job.progress = {**job.progress, "docs": n, "summary": _brief(result.summary)}
                job.external_ref = result.external_ref or job.external_ref
                await self.db.finish_job(job, JobState.SUCCEEDED)
                updated = await self.db.set_status(
                    c.collection_id, Status.SCRAPED, note=f"scrape ok: {n} documents", force=True
                )
                if c.curated_count:  # anything already promoted must be re-reviewed
                    await self.db.set_flag(c.collection_id, True)
                    updated.needs_recuration = True
                self._emit(updated, job)
            except asyncio.CancelledError:
                await self.db.finish_job(job, JobState.FAILED, error="cancelled by user or shutdown")
                self._emit(c, job)
                raise
            except (ScrapeError, OSError, ValueError) as e:
                log.error("scrape %s failed: %s", c.collection_id, e)
                await self.db.finish_job(job, JobState.FAILED, error=str(e)[:2000])
                self._emit(c, job)
            except Exception as e:
                log.exception("scrape %s crashed", c.collection_id)
                await self.db.finish_job(job, JobState.FAILED, error=f"{type(e).__name__}: {e}"[:2000])
                self._emit(c, job)

    async def ingest_dump(self, collection_id: str, docs: list[dict[str, Any]]) -> int:
        rows = [
            DumpUrl(
                collection_id=collection_id,
                url=d["url"],
                scraped_title=d.get("title"),
                full_text=d.get("full_text"),
                content_type=d.get("content_type"),
                depth=d.get("depth"),
            )
            for d in docs
            if d.get("url")
        ]
        n = await self.db.replace_dump(collection_id, rows)
        return n


def _brief(summary: dict[str, Any]) -> dict[str, Any]:
    keys = ("documents_scraped", "failures_logged", "failures_by_reason", "robots_fetch_ok")
    return {k: summary[k] for k in keys if k in summary}


def _now_iso() -> str:
    return utcnow().isoformat()
