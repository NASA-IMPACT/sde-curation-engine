"""JobManager: runs long work as asyncio background tasks, one at a time per collection,
records explicit success/failure in job_runs, and publishes SSE events."""

from __future__ import annotations

import asyncio
import ctypes
import logging
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any

from .backends.index import Dispatch, IndexBackend, IndexError_, wait_for_status
from .backends.publish import ProdPublisher
from .backends.s3 import S3
from .backends.scrape import DocumentSource, ProgressCb, ScrapeBackend, ScrapeError, iter_documents
from .backends.validate import NoIndexAccess, validate_direct
from .config import Settings
from .db import Database
from .engine.export import (
    build_manifest,
    export_lines,
    export_prefix,
    mint_run_id,
    status_prefix,
    write_jsonl,
)
from .engine.patterns import match_counts
from .engine.text import content_hash
from .engine.urls import batches, dedupe_variants
from .events import EventBus
from .llm.base import LLMError, LLMProvider
from .llm.global_excludes import global_exclude_hits, load_global_excludes
from .llm.pool import run_pool
from .llm.tasks import (
    TITLE_SIBLINGS,
    URL_DISAMBIGUATED,
    disambiguate,
    norm_title,
    suggest_distinct_titles,
    suggest_metadata_one,
    suggest_patterns_batch,
    title_siblings,
)
from .models import (
    SYSTEM_ACTOR,
    Collection,
    Confidence,
    DumpFailure,
    DumpUrl,
    IndexRun,
    IndexStatus,
    JobKind,
    JobRun,
    JobState,
    Pattern,
    Status,
    report_passes,
    utcnow,
)

log = logging.getLogger(__name__)

# Suggest metadata writes answers to the DB in small chunks (cancel keeps them, commits stay few).
AI_FLUSH_ROWS = 25
AI_FLUSH_SECONDS = 2.0
# How often the crawl ingest reports the pages it has read. Each report is one job-row UPDATE and
# one SSE event, so it is paced by the clock rather than by the read chunks: a small crawl
# is over before the second report, a big one ticks steadily.
_INGEST_PROGRESS_S = 2.0
# One read chunk of the crawl ingest: this many pages, or this much page text, whichever comes
# first. A count alone let a run of 1 MB pages (ascl.net has thousands) make a 500 MB chunk.
# Measured on an ascl.net-shaped crawl (2.8 GB, 45K pages): peak 1.7 GB with the count alone,
# 640 MB at 16 MB, 340 MB at 1 MB, and no lower below that; the ingest got faster, not slower.
_INGEST_BATCH_DOCS = 500
_INGEST_BATCH_BYTES = 1024 * 1024
# Rounds of URL disambiguation after the model has had its passes. One resolves a group; the rest
# are only there in case a title it wrote lands on one a page outside that group already had.
DISAMBIGUATE_ROUNDS = 3


class JobConflict(Exception):
    pass


class JobManager:
    def __init__(
        self, settings: Settings, db: Database, bus: EventBus, *, scraper: ScrapeBackend,
        llm: LLMProvider | Callable[[], LLMProvider] | None = None,
        indexer: IndexBackend | Callable[[], IndexBackend] | None = None,
        publisher: Callable[[], ProdPublisher] | None = None,
    ):
        self.s = settings
        self.db = db
        self.bus = bus
        self.scraper = scraper
        self._llm = llm
        self._indexer = indexer
        self._publisher = publisher
        self._tasks: dict[int, asyncio.Task] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._starting: set[str] = set()  # collections with a job being created (TOCTOU guard)
        self._cancel_actor: dict[int, str] = {}  # job id → who asked for the cancel

    # ── infrastructure ─────────────────────────────────────────────────

    def lock(self, collection_id: str) -> asyncio.Lock:
        """One lock per collection, shared by scrape ingest and curation writes."""
        return self._locks.setdefault(collection_id, asyncio.Lock())

    _lock = lock

    def active_for(self, collection_id: str) -> JobRun | None:
        # A job counts as active until its final state is recorded (finish_job), not until the
        # asyncio task exits: the task still emits events after that, and a caller that saw
        # "succeeded" in the DB must not be told the job is running.
        for t in self._tasks.values():
            j: JobRun | None = getattr(t, "job", None)
            if j and j.collection_id == collection_id and not t.done() \
                    and j.state in (JobState.QUEUED, JobState.RUNNING):
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

    async def cancel(self, collection_id: str, *, actor: str | None = None) -> JobRun | None:
        """Cancel the running job for a collection; waits until it has recorded 'failed'."""
        for jid, t in list(self._tasks.items()):
            j: JobRun | None = getattr(t, "job", None)
            if j and j.collection_id == collection_id and not t.done():
                if actor:
                    self._cancel_actor[jid] = actor
                t.cancel()
                await asyncio.gather(t, return_exceptions=True)
                return await self.db.get_job(jid)
        return None

    async def shutdown(self) -> None:
        for t in list(self._tasks.values()):
            t.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    async def recover(self) -> None:
        """Startup: jobs left 'running' by a previous process are dead — say so explicitly. A
        Suggest-metadata job the restart interrupted (killed under it, or cancelled by the shutdown
        of a deploy) is started again for the URLs still without an answer: what it had already
        been told is in the table, so nothing is asked or paid for twice."""
        interrupted = await self.db.jobs_ended_by_shutdown()
        for j in await self.db.active_jobs():
            await self.db.finish_job(j, JobState.FAILED, error="engine restarted while job was running")
            self._emit(j.collection_id, j)
            interrupted.append(j)
        for j in interrupted:
            resumed = int(j.progress.get("resumed", 0))
            if j.kind != JobKind.LLM_METADATA or resumed >= self.s.llm_resume_after_restart:
                continue
            c = await self.db.get_collection(j.collection_id)
            if not c or not await self.db.count_deltas_for_llm(c.collection_id, only_missing=True):
                continue
            try:
                new = await self.start_llm_metadata(c, only_missing=True, actor=j.started_by)
            except JobConflict:
                continue
            new.progress = {**new.progress, "resumed": resumed + 1, "resumed_from": j.id}
            await self.db.update_job(new)
            log.info("resumed %s for %s as job %s (after job %s)", j.kind, c.collection_id, new.id, j.id)

    # ── scrape ─────────────────────────────────────────────────────────

    async def start_scrape(self, c: Collection, *, actor: str | None = None, reuse: bool = False) -> JobRun:
        """Run the crawler, or with reuse=True load the crawl output that already exists."""
        cid = c.collection_id
        if cid in self._starting or self.active_for(cid) or self.lock(cid).locked():
            raise JobConflict(f"a job is already running for {cid}")
        self._starting.add(cid)
        try:
            job = await self.db.insert_job(
                JobRun(collection_id=cid, kind=JobKind.SCRAPE, state=JobState.RUNNING, started_by=actor,
                       progress={"reused": True} if reuse else {})
            )
            self._emit(c, job)
            return await self._spawn(job, self._run_scrape(c, job, reuse))
        finally:
            self._starting.discard(cid)

    async def _run_scrape(self, c: Collection, job: JobRun, reuse: bool = False) -> None:
        async with self._lock(c.collection_id):
            try:
                async def on_progress(p: dict[str, Any]) -> None:
                    job.progress = {**job.progress, **p}
                    if "pid" in p or "ssm_command" in p:
                        job.external_ref = str(p.get("pid") or p.get("ssm_command"))
                    await self.db.update_job(job)
                    self._emit(c, job)

                if reuse:
                    result = await self.scraper.fetch_existing(c, on_progress)
                else:
                    result = await self.scraper.run(c, on_progress)
                failures = await asyncio.to_thread(result.failures)
                # The crawl still has to be streamed into PostgreSQL, which on a multi-GB
                # collection is minutes of work after the crawler itself has gone quiet. Say so,
                # with the page counts, instead of leaving the last crawl figure on screen.
                await on_progress({"phase": "ingest", "ingested": 0, "failures": len(failures),
                                   "ingest_total": _expected_docs(result.summary, job.progress)})
                n = await self.ingest_dump(c.collection_id, result.documents, failures,
                                           on_progress=on_progress)
                crawled_at = result.crawled_at or utcnow()
                capped = result.capped(n, c.max_pages)
                await self.db.set_last_scraped(c.collection_id, crawled_at, capped=capped)
                # deltas computed against the previous dump are now meaningless
                await self.db.replace_deltas(c.collection_id, [], [])
                job.progress = {**job.progress, "docs": n, "failures": len(failures), "capped": capped,
                                "summary": _brief(result.summary)}
                job.external_ref = result.external_ref or job.external_ref
                note = (f"loaded existing crawl from {crawled_at:%Y-%m-%d %H:%M}Z: {n} documents" if reuse
                        else f"scrape ok: {n} documents")
                if dropped := job.progress.get("duplicates_dropped"):
                    note += (f" ({job.progress.get('ingested', n + dropped):,} read,"
                             f" {dropped:,} duplicate links dropped)")
                # Collection state first, job record last: "succeeded" must mean every effect of
                # the job is already visible to whoever polls the job list.
                updated = await self.db.set_status(
                    c.collection_id, Status.SCRAPED, note=note, force=True, actor=SYSTEM_ACTOR,
                )
                if c.curated_rows:  # anything already promoted must be re-reviewed
                    reason = (f"{'loaded existing crawl' if reuse else 're-crawled'} on {crawled_at:%Y-%m-%d %H:%M}Z"
                              f" ({n} documents) after {c.curated_rows} URLs were promoted — Start curating"
                              " shows what changed")
                    await self.db.set_flag(c.collection_id, True, reason)
                    updated.needs_recuration, updated.recuration_reason = True, reason
                await self.db.finish_job(job, JobState.SUCCEEDED)
                self._emit(updated, job)
            except asyncio.CancelledError:
                await self.db.finish_job(job, JobState.FAILED, error=f"cancelled by {self._cancel_actor.pop(job.id, None) or 'shutdown'}")
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

    # ── LLM assist ─────────────────────────────────────────────────────

    def llm(self) -> LLMProvider:
        if self._llm is None:
            raise LLMError("no LLM provider configured")
        return self._llm() if callable(self._llm) and not hasattr(self._llm, "complete") else self._llm  # type: ignore[return-value]

    async def _start(self, c: Collection, kind: JobKind, coro_factory, *, actor: str | None = None) -> JobRun:
        cid = c.collection_id
        if cid in self._starting or self.active_for(cid) or self.lock(cid).locked():
            raise JobConflict(f"a job is already running for {cid}")
        self._starting.add(cid)
        try:
            job = await self.db.insert_job(
                JobRun(collection_id=cid, kind=kind, state=JobState.RUNNING, started_by=actor)
            )
            self._emit(c, job)
            return await self._spawn(job, coro_factory(job))
        finally:
            self._starting.discard(cid)

    async def start_curation(self, c: Collection, kind: JobKind, work: Callable[[], Awaitable[Any]], what: str,
                             *, actor: str | None = None) -> JobRun:
        """Run a bulk curation change (`work`: the same coroutine the request would have awaited) as
        a job. Other edits on the collection are refused while it runs (ensure_idle), as for any job."""
        return await self._start(c, kind, lambda job: self._run_curation(c, job, work, what), actor=actor)

    async def _run_curation(self, c: Collection, job: JobRun, work: Callable[[], Awaitable[Any]], what: str) -> None:
        # not under the collection lock: the curation service takes it for each of its writes, and an
        # asyncio.Lock is not re-entrant
        try:
            await self._progress_cb(c, job)({"curation": what})
            result = await work()
            if isinstance(result, dict):
                job.progress = {**job.progress, "result": {k: v for k, v in result.items() if isinstance(v, int | str | bool)}}
                await self.db.update_job(job)
            await self.db.finish_job(job, JobState.SUCCEEDED)
            self._emit(await self.db.get_collection(c.collection_id) or c, job)
        except asyncio.CancelledError:
            await self.db.finish_job(job, JobState.FAILED, error=f"cancelled by {self._cancel_actor.pop(job.id, None) or 'shutdown'}")
            self._emit(c, job)
            raise
        except Exception as e:
            log.exception("%s %s failed", job.kind, c.collection_id)
            await self.db.finish_job(job, JobState.FAILED, error=str(getattr(e, "detail", None) or f"{type(e).__name__}: {e}")[:2000])
            self._emit(c, job)

    async def start_llm_patterns(self, c: Collection, *, actor: str | None = None) -> JobRun:
        return await self._start(c, JobKind.LLM_PATTERNS, lambda job: self._run_llm_patterns(c, job), actor=actor)

    async def start_llm_metadata(
        self, c: Collection, *, only_missing: bool = True, actor: str | None = None
    ) -> JobRun:
        return await self._start(
            c, JobKind.LLM_METADATA, lambda job: self._run_llm_metadata(c, job, only_missing), actor=actor
        )

    async def start_llm_titles(self, c: Collection, *, actor: str | None = None) -> JobRun:
        return await self._start(c, JobKind.LLM_TITLES, lambda job: self._run_llm_titles(c, job), actor=actor)

    def _progress_cb(self, c: Collection, job: JobRun):
        """Merge a progress dict into the job, persist it and publish it over SSE."""
        async def on_progress(p: dict[str, Any]) -> None:
            job.progress = {**job.progress, **p}
            await self.db.update_job(job)
            self._emit(c, job)
        return on_progress

    async def _guarded(self, c: Collection, job: JobRun, body) -> None:
        async with self._lock(c.collection_id):
            try:
                await body()
                await self.db.finish_job(job, JobState.SUCCEEDED)
                self._emit(await self.db.get_collection(c.collection_id) or c, job)
            except asyncio.CancelledError:
                await self.db.finish_job(job, JobState.FAILED, error=f"cancelled by {self._cancel_actor.pop(job.id, None) or 'shutdown'}")
                self._emit(c, job)
                raise
            except (LLMError, ScrapeError, IndexError_, OSError, ValueError) as e:
                log.error("%s %s failed: %s", job.kind, c.collection_id, e)
                await self.db.finish_job(job, JobState.FAILED, error=str(e)[:2000])
                self._emit(c, job)
            except Exception as e:
                log.exception("%s %s crashed", job.kind, c.collection_id)
                await self.db.finish_job(job, JobState.FAILED, error=f"{type(e).__name__}: {e}"[:2000])
                self._emit(c, job)

    def _retry(self) -> dict[str, Any]:
        return {"retry_passes": self.s.llm_retry_passes, "retry_delay_s": self.s.llm_retry_delay_s}

    async def _run_llm_patterns(self, c: Collection, job: JobRun) -> None:
        """Exclude-only suggestions over the included delta URLs (on a first pass that is the
        whole crawl; after a promote only what changed): the global exclude list first
        (deterministic), then one model call per batch of URLs, merged by (type, match). Match
        counts are still taken over the whole crawl so the impact of a glob is visible."""
        async def body():
            cid = c.collection_id
            all_urls = await self.db.dump_urls(cid)
            if not all_urls:
                raise LLMError("no crawl dump to sample — scrape first")
            pending = await self.db.pending_urls_for_patterns(cid)
            if not pending:
                raise LLMError("no delta URLs to look at — Start curating first (or every delta URL is already excluded)")
            cand_urls = [u for u, _ in pending]
            titles = dict(pending)
            await self.db.clear_pending_pattern_suggestions(cid)
            gl = global_exclude_hits(load_global_excludes(self.s.global_excludes_path), cand_urls, count_over=all_urls)
            n_global = await self.db.add_pattern_suggestions(cid, gl)
            # every global glob that matched: the model must not repeat them, and a suggestion
            # whose URLs they already cover is dropped (tasks.suggest_patterns_batch)
            examples = [g["match"] for g in sorted(gl, key=lambda g: (-g["matches"], g["match"]))]
            if not examples:  # nothing matched: still show the style
                examples = [g.match for g in load_global_excludes(self.s.global_excludes_path).patterns[:10]]
            unique = dedupe_variants(cand_urls)
            chunks = batches([{"url": u, "scraped_title": titles.get(u)} for u in unique],
                             self.s.llm_pattern_batch_urls)
            progress = self._progress_cb(c, job)
            await progress({"llm": "patterns", "urls": len(all_urls), "candidates": len(cand_urls), "unique": len(unique),
                            "calls": len(chunks), "total": len(chunks), "done": 0, "failed": 0, "global": n_global,
                            "suggestions": n_global, "tokens_in": 0, "tokens_out": 0})
            llm = self.llm()

            async def one(item):
                i, chunk = item
                return await suggest_patterns_batch(llm, c, chunk, examples=examples, batch_no=i + 1,
                                                    batches=len(chunks))

            async def on_result(item, result):
                kept, done = result
                counts = await asyncio.to_thread(
                    match_counts,
                    [Pattern(id=i, collection_id=cid, type=s.type, match=s.match) for i, s in enumerate(kept)],
                    all_urls)
                rows = [{"type": s.type, "match": s.match, "rationale": s.rationale, "matches": counts.get(i, 0)}
                        for i, s in enumerate(kept)]
                added = await self.db.add_pattern_suggestions(cid, rows)
                job.progress["suggestions"] = job.progress.get("suggestions", 0) + added
                job.progress["tokens_in"] = job.progress.get("tokens_in", 0) + done.tokens_in
                job.progress["tokens_out"] = job.progress.get("tokens_out", 0) + done.tokens_out

            await run_pool(list(enumerate(chunks)), one, workers=self.s.llm_workers, on_result=on_result,
                           on_progress=progress, total=len(chunks), **self._retry())
            await progress({"suggestions": await self.db.count_pending_pattern_suggestions(cid)})
        await self._guarded(c, job, body)

    async def _run_llm_metadata(self, c: Collection, job: JobRun, only_missing: bool) -> None:
        """One call per included delta URL with the full page text, LLM_WORKERS at a time.
        Results are written in small chunks as they arrive, so a cancel keeps what finished and
        a re-run (only_missing) resumes with the rest. One bad URL never fails the job: calls the
        provider turned away are retried once at the end, and a URL that still fails gets its
        error recorded on the row (the next Suggest metadata picks it up again)."""
        async def body():
            cid = c.collection_id
            total = await self.db.count_deltas_for_llm(cid, only_missing=only_missing)
            if not total:
                raise LLMError("no delta URLs to classify — Start curating (recompute) first (or all already have suggestions)")
            progress = self._progress_cb(c, job)
            await progress({"llm": "metadata", "total": total, "done": 0, "failed": 0, "inflight": 0,
                            "tokens_in": 0, "tokens_out": 0, "tokens_cached": 0})
            llm = self.llm()
            buf: list[dict[str, Any]] = []
            errs: list[tuple[str, str]] = []
            last_flush = time.monotonic()
            written = 0
            titled: set[str] = set()  # URLs this run gave an AI title

            async def flush() -> None:
                nonlocal last_flush, written
                last_flush = time.monotonic()
                if errs:
                    failed, errs[:] = list(errs), []
                    await self.db.set_delta_ai_errors(cid, failed)
                if buf:
                    rows, buf[:] = list(buf), []
                    n = await self.db.set_delta_ai(cid, rows)
                    written += n  # never `written += await …`: two flushes overlap and one is lost

            async def on_error(doc, e: Exception) -> None:
                errs.append((doc["url"], f"{type(e).__name__}: {e}"))
                if len(errs) >= AI_FLUSH_ROWS or time.monotonic() - last_flush > AI_FLUSH_SECONDS:
                    await flush()

            async def on_result(doc, row):
                _add_tokens(job, row)
                if row["title"]:
                    titled.add(row["url"])
                buf.append(row)
                if len(buf) >= AI_FLUSH_ROWS or time.monotonic() - last_flush > AI_FLUSH_SECONDS:
                    await flush()

            try:
                await run_pool(
                    self.db.iter_deltas_for_llm(cid, only_missing=only_missing),
                    lambda d: suggest_metadata_one(llm, d, settings=self.s, collection=c),
                    workers=self.s.llm_workers, on_result=on_result, on_error=on_error, on_progress=progress,
                    total=total, **self._retry(),
                )
            finally:
                await flush()  # a cancel still keeps every answer that arrived
            await progress({"classified": written, "inflight": 0})
            if self.s.llm_dedupe_titles and titled:
                try:
                    await self._retitle_duplicates(c, job, llm, touching=titled)
                except LLMError as e:  # the classification stands; the duplicates stay flagged
                    log.warning("titles %s: telling duplicate titles apart failed: %s", cid, e)
                    await progress({"titles_error": str(e)[:500]})
        await self._guarded(c, job, body)

    async def _run_llm_titles(self, c: Collection, job: JobRun) -> None:
        """Regenerate duplicate titles, on demand: the same pass Suggest metadata ends with, over every title +
        document type that a delta URL shares with another page (whoever set them: AI, a rule, a curator)."""
        async def body():
            await self._progress_cb(c, job)({"llm": "titles", "tokens_in": 0, "tokens_out": 0, "tokens_cached": 0})
            if not await self._retitle_duplicates(c, job, self.llm()):
                raise LLMError("no delta URL shares its title and document type with another page")
        await self._guarded(c, job, body)

    async def _plan_retitle(self, cid: str, touching: set[str] | None) -> list[dict[str, Any]]:
        """The duplicate-title groups this pass will rewrite, each with the pages to ask about
        (`rewrite`), the pages whose titles are fixed and must not be collided with (`settled`), the
        title the group shared before any earlier regeneration (`origin`) and the answers that
        already failed (`previous`)."""
        plan = []
        for g in await self.db.duplicate_title_groups(cid):
            members = g["members"]
            if touching is not None and not any(m["url"] in touching for m in members):
                continue
            delta = [m for m in members if m["delta"]]
            ask = {m["url"] for m in ([m for m in delta if m["pending_ai"]] or delta)}
            for m in members:
                m["rewrite"] = m["url"] in ask
            # Asked again, a group is asked the SAME question, not a new one about the last answer:
            # from the title the pages originally shared, or every pass builds on the one before it
            # and the title grows a tail ("Data Access — Access — Access — …"). What was already
            # tried goes along as `previous` so the model does not offer it twice.
            origins = {m["before"] for m in members if m["rewrite"] and m["before"]}
            g["origin"] = origins.pop() if len(origins) == 1 else g["title"]
            g["previous"] = {g["title"]} - {g["origin"]}
            g["settled"] = [m for m in members if not m["rewrite"]]
            g["rewrite"] = [m for m in members if m["rewrite"]]
            if g["rewrite"]:
                plan.append(g)
        return plan

    async def _retitle_duplicates(self, c: Collection, job: JobRun, llm: LLMProvider, *,
                                  touching: set[str] | None = None) -> int:
        """Pages of one collection that will be indexed under the same title AND document type are
        told apart, and this pass owns the outcome: when it ends, no delta URL it was allowed to
        touch still shares a title + document type with another page. The curator is never handed a
        button to press again.

        A GROUP goes to the model in one call, with every page's full text, so the model tells the
        pages apart from each other instead of guessing page by page and colliding all over again
        (the old one-call-per-page shape manufactured nearly as many duplicates as it cleared). A
        group whose texts exceed `llm_title_group_chars` is split, and each later call is told the
        titles the earlier ones used. Groups that still collide are re-asked up to
        `llm_title_passes` times, and whatever survives that is disambiguated from the URLs
        (tasks.disambiguate) — unique URLs mean that always resolves.

        Only the title is asked for; the document type is left alone and nothing is applied until
        the SME accepts. Only delta URLs can take a suggestion: in a group the ones with a pending
        AI title are re-asked and the rest keep theirs, and when none has one every delta URL is.
        `touching`: only groups with one of these URLs (the pages Suggest metadata just titled).
        Returns how many pages were sent."""
        cid = c.collection_id
        progress = self._progress_cb(c, job)
        plan = await self._plan_retitle(cid, touching)
        asked = sum(len(g["rewrite"]) for g in plan)
        # `titles_total` counts URLs (what the curator is told); the pool runs over GROUPS, so its
        # own done / failed / inflight are reported as calls.
        await progress({"llm_phase": "titles", "titles_total": asked, "title_groups": len(plan),
                        "title_calls": 0, "title_calls_done": 0, "title_calls_failed": 0,
                        "retitled": 0, "disambiguated": 0})
        if not plan:
            return 0
        retitled = disambiguated = calls = 0

        # Every title + document type the collection already uses. A group told apart within itself
        # still collides if it picks a title another group — or a page that was never in a group —
        # already has, which is how a pass that only looked at one group at a time kept clearing
        # duplicates and making new ones. Claims are added here as they are handed out, so two
        # groups running side by side cannot take the same title.
        claimed = set((await self.db.title_keys(cid)).values())

        def key_of(title: str, document_type: str | None) -> str:
            return f"{norm_title(title)}\x1f{document_type or ''}"

        def claim(title: str, document_type: str | None) -> bool:
            """True if this title is free for this document type, and takes it. A group's own shared
            title stays claimed throughout: one of its pages may end up keeping it."""
            key = key_of(title, document_type)
            if key in claimed:
                return False
            claimed.add(key)
            return True

        async def calls_for(g: dict[str, Any]) -> list[list[dict[str, Any]]]:
            """The group's pages to ask about, packed into calls by how much text they carry: the
            full text goes in uncut, so characters are what bounds a call, not the page count."""
            urls = [m["url"] for m in g["rewrite"]]
            sizes = await self.db.text_sizes(cid, urls)
            out: list[list[dict[str, Any]]] = [[]]
            budget = 0
            for m in g["rewrite"]:
                n = sizes.get(m["url"], 0)
                if out[-1] and budget + n > self.s.llm_title_group_chars:
                    out.append([]); budget = 0
                out[-1].append(m); budget += n
            return [batch for batch in out if batch]

        async def one(g: dict[str, Any]) -> dict[str, Any]:
            """One group: its calls run in order, because a later call has to know the titles the
            earlier ones already used. Groups run against each other in the pool."""
            given: dict[str, dict[str, Any]] = {}
            same_pages: list[list[str]] = []
            made = 0
            fixed = [{"url": m["url"], "title": g["title"]} for m in g["settled"]]
            for batch in await calls_for(g):
                docs = await self.db.docs_for_llm(cid, [m["url"] for m in batch])
                if not docs:
                    continue
                settled = fixed + [{"url": u, "title": t["title"]} for u, t in given.items()]
                if len(settled) > TITLE_SIBLINGS:  # a huge group: name the nearest in URL order
                    settled = title_siblings(settled, docs[0]["url"])
                row = await suggest_distinct_titles(
                    llm, docs, shared_title=g["origin"], document_type=g["document_type"],
                    sharing=len(g["members"]), settled=settled, previous=g["previous"], collection=c)
                given |= {u: {**t, "model": row["model"]} for u, t in row["titles"].items()}
                same_pages += row["same_page_groups"]
                made += 1
                _add_tokens(job, row)
            return {"titles": given, "same_page_groups": same_pages, "calls": made}

        async def on_result(g, row):
            nonlocal retitled, calls
            calls += row["calls"]
            # the model answered for its group; the claim check is what makes the answer safe for
            # the whole collection. A title that is already taken is dropped, and the page falls
            # through to the next pass or to the URLs.
            rows = [{"url": u, **t, "before": g["origin"]} for u, t in row["titles"].items()
                    if claim(t["title"], g["document_type"])]
            if rows:
                # groups run side by side: read the counter, add, write back with no await in
                # between, or five of them clobber each other's totals
                written = await self.db.set_delta_ai_titles(cid, rows)
                retitled += written

        async def on_error(g, e: Exception) -> None:
            log.warning("titles %s: group %r failed: %s", cid, g["title"], e)

        async def pool_progress(p: dict[str, Any]) -> None:
            await progress({f"title_calls_{k}": v for k, v in p.items()})

        for attempt in range(self.s.llm_title_passes + 1):
            if attempt:  # only the groups the last pass could not tell apart go round again
                plan = await self._plan_retitle(cid, touching)
                if not plan:
                    break
                await progress({"title_pass": attempt + 1, "title_groups": len(plan), "title_calls_done": 0})
            await run_pool(plan, one, workers=self.s.llm_workers, on_result=on_result, on_error=on_error,
                           on_progress=pool_progress, total=len(plan), **self._retry())
            await progress({"retitled": retitled, "title_calls": calls})

        # The floor: whatever the model could not tell apart, the URLs do. This is what makes one
        # click enough — the pass never ends leaving the curator a group to send back again. One
        # round settles it, because the claim check is over the whole collection; the second is
        # there only in case a group's pages were themselves re-grouped by what the first wrote.
        for _ in range(DISAMBIGUATE_ROUNDS):
            groups = await self._plan_retitle(cid, touching)
            if not groups:
                break
            for g in groups:
                dt = g["document_type"]
                titles = disambiguate(g["origin"], [m["url"] for m in g["rewrite"]],
                                      taken=[g["title"], g["origin"]],
                                      is_taken=lambda t, dt=dt: key_of(t, dt) in claimed)
                rows = [{"url": u, "title": t, "title_conf": Confidence.LOW, "model": URL_DISAMBIGUATED,
                         "before": g["origin"]} for u, t in titles.items() if claim(t, dt)]
                written = await self.db.set_delta_ai_titles(cid, rows)
                disambiguated += written
        else:
            log.warning("titles %s: %s URLs still share a title after disambiguating", cid,
                        (await self.db.duplicate_title_counts(cid))["delta_urls"])

        still = await self.db.duplicate_title_counts(cid)
        await progress({"retitled": retitled, "disambiguated": disambiguated, "still_duplicate": still["urls"],
                        "title_calls": calls, "title_calls_inflight": 0})
        return asked

    # ── index (export → S3 → WEB_COSMOS → status.json) ─────────────────

    def indexer(self) -> IndexBackend:
        if self._indexer is None:
            raise IndexError_("no index backend configured")
        return self._indexer() if callable(self._indexer) and not hasattr(self._indexer, "complete") and not hasattr(self._indexer, "dispatch") else self._indexer  # type: ignore[return-value]

    def publisher(self) -> ProdPublisher:
        if self._publisher is None:
            raise IndexError_("no prod publisher configured")
        return self._publisher()

    async def start_index(
        self, c: Collection, target: str, *, actor: str | None = None,
        allow_high_deletion: bool = False,
    ) -> tuple[JobRun, IndexRun]:
        if not self.s.cosmos_index_bucket:
            raise IndexError_("COSMOS_INDEX_BUCKET is not set")
        if target == "prod" and not self.s.opensearch_endpoint_prod:
            raise IndexError_("OPENSEARCH_ENDPOINT_PROD is not set — nowhere to publish to")
        run = IndexRun(run_id=mint_run_id(), collection_id=c.collection_id, target=target, started_by=actor)
        if target == "prod":
            job = await self._start(
                c, JobKind.INDEX_PROD,
                lambda job: self._run_publish_prod(c, job, run, allow_high_deletion=allow_high_deletion),
                actor=actor,
            )
        else:
            job = await self._start(
                c, JobKind.INDEX_TEST,
                lambda job: self._run_index(c, job, run, allow_high_deletion=allow_high_deletion),
                actor=actor,
            )
        job.run_id = run.run_id
        await self.db.update_job(job)
        return job, run

    async def _run_index(
        self, c: Collection, job: JobRun, run: IndexRun, *, allow_high_deletion: bool = False
    ) -> None:
        async def body():
            s3 = S3(self.s.cosmos_index_bucket, region=self.s.aws_region)
            await self.db.insert_index_run(run)
            backend = self.indexer()

            async def progress(p: dict[str, Any]) -> None:
                job.progress = {**job.progress, **p}
                await self.db.update_job(job)
                self._emit(c, job)

            # 1. pin the OpenSearch collection this indexes as, then export: stream curated
            #    (non-excluded) rows — with the text they were approved with — to a temp jsonl,
            #    upload, THEN the manifest
            await self._pin_index_key(c, progress)
            # (a server-side cursor, a few hundred rows at a time: the approved text of 100k pages is
            # never in memory at once, and each batch is serialised off the event loop)
            n = 0
            with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as fh:
                tmp = Path(fh.name)
                async for rows in self.db.iter_curated_for_export(c.collection_id):
                    n += await asyncio.to_thread(write_jsonl, export_lines(rows), fh)
            try:
                if n == 0:
                    raise IndexError_("nothing to export: every curated URL is excluded")
                prefix = export_prefix(c.collection_key, run.run_id)
                await s3.upload_file(tmp, f"{prefix}/documents.jsonl", "application/x-ndjson")
                manifest = build_manifest(c, run.run_id, n, run.target)
                await s3.put_json(f"{prefix}/manifest.json", manifest.model_dump(mode="json"))
            finally:
                tmp.unlink(missing_ok=True)
            run.exported = n
            await self.db.update_index_run(run)
            await progress({"exported": n, "export": s3.url(prefix), "phase": "dispatch"})

            # 2. dispatch
            d: Dispatch = await backend.dispatch(
                c, run.run_id, run.target, allow_high_deletion=allow_high_deletion
            )
            run.external_ref = d.external_ref
            job.external_ref = d.external_ref
            await self.db.update_index_run(run)
            await progress({"external_ref": d.external_ref, "phase": "indexing", **{k: v for k, v in d.detail.items() if k != "log"}})

            # 3. wait for status.json (+ validation.json on test)
            try:
                status, validation = await wait_for_status(
                    s3, self.s, c, run.run_id, backend, d, progress, target=run.target
                )
            except asyncio.CancelledError:
                if hasattr(backend, "kill"):
                    await backend.kill(d)
                raise
            run.status = status.model_dump()
            run.validation = validation.model_dump() if validation else None
            run.validated_by = "indexer" if validation else None
            job.progress = {**job.progress, "phase": "done", "status": run.status, "validation": run.validation}
            if status.state != "succeeded":
                run.state, run.error, run.finished_at = "failed", status.error or "indexer reported failure", utcnow()
                await self.db.update_index_run(run)
                raise IndexError_(f"indexer failed: {status.error}{(' — ' + status.error_detail) if status.error_detail else ''}")
            run.state, run.finished_at = "succeeded", utcnow()
            await self.db.update_index_run(run)

            await self.db.set_status(
                c.collection_id, Status.CONFIG_GENERATED, force=True, actor=SYSTEM_ACTOR,
                note=f"test index run {run.run_id}: {status.indexed} indexed, {status.deleted} deleted",
            )
            # 4. validation gate — the indexer's own validation.json is pre-refresh; re-check after a delay
            await self._validate(c, job, run, s3, backend, progress)

        await self._guarded(c, job, body)

    async def _pin_index_key(self, c: Collection, progress) -> None:
        """Record the key this collection is indexed as, so a later rename cannot silently move it to
        a second collection: `collection_key` follows the name (the COSMOS rule) until a run pins it,
        and after that only "Set index key" changes it."""
        key, name = c.collection_key, c.collection_name
        await progress({"index_key": key, "index_name": name})
        if c.index_key:
            return
        await self.db.set_index_key(c.collection_id, key, name)
        await self.db.audit(SYSTEM_ACTOR, "index.key", c.collection_id,
                            f"indexed as '{key}' ({name}), from the collection name")
        c.index_key, c.index_name = key, name
        log.info("%s: indexed as '%s' (%s)", c.collection_id, key, name)

    async def _run_publish_prod(
        self, c: Collection, job: JobRun, run: IndexRun, *, allow_high_deletion: bool = False
    ) -> None:
        """Index to prod: publish the vectors of the latest validated test run straight into the
        production index (backends/publish.py) — no export, no indexer task, no re-vectorizing —
        then run the same validation gate as test against prod. The collection only becomes `live`
        once that passes; a failed or impossible check sends it back to `config_generated`, flagged
        (the documents that were written stay in prod)."""
        async def body():
            source = await self.db.last_index_run(c.collection_id, "test")
            if not source or source.state != "succeeded" or not source.validation_passes(self.s.validation_title_match_threshold):
                raise IndexError_("prod indexing requires a successful, validated test run first")
            tested_as = (source.status or {}).get("collection_key")
            if tested_as and tested_as != c.collection_key:
                raise IndexError_(f"the latest test run was indexed as '{tested_as}' but this collection is now "
                                  f"'{c.collection_key}' — index to test again first")
            publisher = self.publisher()
            run.exported = source.exported
            run.external_ref = job.external_ref = f"publish:{source.run_id}"
            await self.db.insert_index_run(run)

            async def progress(p: dict[str, Any]) -> None:
                job.progress = {**job.progress, **p}
                await self.db.update_job(job)
                self._emit(c, job)

            await progress({"source_test_run": source.run_id, "exported": source.exported})
            st = await publisher.run(c.collection_key, run.run_id, source.run_id, progress,
                                     allow_high_deletion=allow_high_deletion)
            status = IndexStatus.model_validate(st)
            run.status = st
            job.progress = {**job.progress, "phase": "done", "status": st}
            if status.state != "succeeded":
                run.state, run.error, run.finished_at = "failed", status.error or "publish failed", utcnow()
                await self.db.update_index_run(run)
                detail = st.get("error_detail") or (f"{st.get('missing')} documents have no vectors in S3 or the test index, "
                                                    f"e.g. {', '.join(st.get('missing_urls', [])[:3])}" if st.get("missing") else "")
                raise IndexError_(f"publish to prod failed: {status.error}{(' — ' + detail) if detail else ''}")
            run.state, run.finished_at = "succeeded", utcnow()
            await self.db.update_index_run(run)
            await self._validate_prod(
                c, job, run, progress,
                note=(f"prod publish {run.run_id} from test run {source.run_id}: {status.indexed} written "
                      f"({st.get('from_vectorized', 0)} from S3, {st.get('from_test_index', 0)} from the test index), "
                      f"{st.get('unchanged', 0)} unchanged, {status.deleted} removed"),
            )

        await self._guarded(c, job, body)

    async def _validate_prod(self, c: Collection, job: JobRun, run: IndexRun, progress, note: str | None = None) -> None:
        """The test gate, against prod: wait, poll directly until visible or timed out, same pass rule.
        Pass → `live`. Fail → back to `config_generated`. There is no indexer to fall back to, so a
        check that cannot run (no endpoint / no read access) fails the job instead of letting an
        unchecked publish count as live. Prod never touches the needs-re-curation flag: nothing
        curated is wrong when prod lags. The UI's "prod not validated" chip is read off the run."""
        cid = c.collection_id
        await progress({"phase": "validating", "validation_delay_s": int(self.s.validation_delay_s)})
        await asyncio.sleep(self.s.validation_delay_s)
        expected = await self._expected_titles(cid)
        prefix = f"{note}; " if note else ""
        try:
            report = await self._validate_direct_until_visible(c, run, expected, progress)
        except NoIndexAccess as e:
            reason = f"prod validation could not run: {e}"[:500] + " — fix prod read access (aoss:ReadDocument), then Re-validate prod"
            log.warning("%s: %s", cid, reason)
            await self.db.set_status(cid, Status.CONFIG_GENERATED, force=True, actor=SYSTEM_ACTOR,
                                     note=f"{prefix}prod run {run.run_id} NOT validated — {reason}")
            raise IndexError_(reason) from e
        run.validation, run.validated_by = report, "direct"
        await self.db.update_index_run(run)
        ok = run.validation_passes(self.s.validation_title_match_threshold)
        job.progress = {**job.progress, "phase": "done", "validation": report, "validated_by": "direct", "validation_ok": ok}
        summary = f"{report['indexed_count']}/{report['expected_count']} visible, titles {report['title_match_rate']:.1%}"
        if ok:
            await self.db.set_status(cid, Status.LIVE, force=True, actor=SYSTEM_ACTOR,
                                     note=f"{prefix}prod validated (direct): {summary}")
        else:
            await self.db.set_status(cid, Status.CONFIG_GENERATED, force=True, actor=SYSTEM_ACTOR,
                                     note=f"{prefix}prod validation FAILED (direct): {summary} — not live")

    async def _validate(self, c: Collection, job: JobRun, run: IndexRun, s3: S3, backend: IndexBackend, progress) -> None:
        """Wait for the index to refresh, then validate directly (fast), re-checking until the
        documents are visible or the validation timeout passes — OpenSearch Serverless makes a bulk
        upsert searchable some unpredictable time after the indexer reports success. On 403 fall
        back to a second pass of the same export (changed: 0) purely to get a fresh validation.json."""
        await progress({"phase": "validating", "validation_delay_s": int(self.s.validation_delay_s)})
        await asyncio.sleep(self.s.validation_delay_s)
        expected = await self._expected_titles(c.collection_id)
        try:
            report = await self._validate_direct_until_visible(c, run, expected, progress)
            run.validated_by = "direct"
        except NoIndexAccess as e:
            log.warning("direct validation unavailable (%s) — falling back to a second indexer pass", e)
            await progress({"phase": "validating", "fallback": "second_pass", "reason": str(e)[:200]})
            # the first pass left status.json/validation.json behind — remove them or the poller
            # would return the stale (pre-refresh) validation immediately
            prefix = status_prefix(c.collection_key, run.run_id)
            await s3.delete(f"{prefix}/status.json", f"{prefix}/validation.json")
            d = await backend.dispatch(c, run.run_id, run.target)
            status, validation = await wait_for_status(s3, self.s, c, run.run_id, backend, d, progress, target=run.target)
            if status.state != "succeeded" or validation is None:
                raise IndexError_(f"second-pass validation failed: {status.error or 'no validation.json'}")
            report = validation.model_dump()
            run.validated_by = "second_pass"
        run.validation = report
        await self.db.update_index_run(run)
        ok = run.validation_passes(self.s.validation_title_match_threshold)
        job.progress = {**job.progress, "phase": "done", "validation": report, "validated_by": run.validated_by, "validation_ok": ok}
        # A failed index is not a curation problem: it never raises needs-re-curation. The UI's
        # "needs re-indexing" chip is read off the run, so a later pass is the only thing that clears it.
        if ok:
            now = await self.db.get_collection(c.collection_id)
            if now and now.needs_recuration and (now.recuration_reason or "").startswith("test-index validation failed"):
                await self.db.set_flag(c.collection_id, False)  # a flag raised by the old rule
            await self.db.set_status(
                c.collection_id, Status.CONFIG_GENERATED, force=True, actor=SYSTEM_ACTOR,
                note=f"validated ({run.validated_by}): {report['indexed_count']}/{report['expected_count']}, titles {report['title_match_rate']:.1%}",
            )
        else:
            await self.db.set_status(
                c.collection_id, Status.CURATED, force=True, actor=SYSTEM_ACTOR,
                note=f"validation FAILED ({run.validated_by}): {report['indexed_count']}/{report['expected_count']} indexed, titles {report['title_match_rate']:.1%} — needs re-indexing",
            )

    async def _validate_direct_until_visible(self, c: Collection, run: IndexRun, expected: dict[str, str], progress) -> dict[str, Any]:
        """Poll `validate_direct` until the report passes the gate or `validation_timeout_s` elapses;
        returns the last report either way. A short count is only a failure once the index has had
        the whole window to catch up."""
        started, attempt = time.monotonic(), 0
        key = (run.status or {}).get("collection_key") or c.collection_key  # what that run indexed as
        while True:
            attempt += 1
            report = await validate_direct(
                self.s, collection_key=key, run_id=run.run_id, target=run.target, expected_titles=expected
            )
            ok = report_passes(report, self.s.validation_title_match_threshold)
            waited = time.monotonic() - started
            await progress({"phase": "validating", "validation_attempt": attempt, "validation_waiting_s": int(waited),
                            "indexed_so_far": report["indexed_count"], "expected_count": report["expected_count"]})
            if ok or waited + self.s.validation_poll_interval_s > self.s.validation_timeout_s:
                if not ok:
                    log.warning("validation still short after %ds (%d attempts): %d/%d — failing the gate",
                                waited, attempt, report["indexed_count"], report["expected_count"])
                return report
            log.info("index not yet consistent for %s: %d/%d after %ds — re-checking in %ss",
                     c.collection_id, report["indexed_count"], report["expected_count"], waited,
                     self.s.validation_poll_interval_s)
            await asyncio.sleep(self.s.validation_poll_interval_s)

    async def _expected_titles(self, collection_id: str) -> dict[str, str]:
        curated = await self.db.load_curated(collection_id)
        return {r.url: (r.title or r.scraped_title or "").strip() for r in curated if not r.excluded}

    async def start_revalidate(self, c: Collection, run: IndexRun, *, actor: str | None = None) -> JobRun:
        """Manual re-check of an existing test or prod run (no new export / publish)."""
        if run.target == "prod":
            if not self.s.opensearch_endpoint_prod:
                raise IndexError_("OPENSEARCH_ENDPOINT_PROD is not set — nothing to validate against")
            return await self._start(c, JobKind.VALIDATE_PROD, lambda job: self._run_revalidate(c, job, run), actor=actor)
        return await self._start(
            c, JobKind.VALIDATE, lambda job: self._run_revalidate(c, job, run), actor=actor
        )

    async def _run_revalidate(self, c: Collection, job: JobRun, run: IndexRun) -> None:
        async def body():
            s3 = S3(self.s.cosmos_index_bucket, region=self.s.aws_region)

            async def progress(p: dict[str, Any]) -> None:
                job.progress = {**job.progress, **p}
                await self.db.update_job(job)
                self._emit(c, job)

            job.run_id = run.run_id
            await self.db.update_job(job)
            if run.target == "prod":
                await self._validate_prod(c, job, run, progress)
            else:
                await self._validate(c, job, run, s3, self.indexer(), progress)

        await self._guarded(c, job, body)

    async def ingest_dump(
        self, collection_id: str, docs: DocumentSource | list[dict[str, Any]],
        failures: list[dict[str, Any]] | None = None, *, on_progress: ProgressCb | None = None,
    ) -> int:
        """Store the crawl as the collection's dump.

        `docs` is the crawl — a `DocumentSource`, which for a remote scrape is the S3 object
        itself — or documents already in memory. It is read exactly once, forwards, a few hundred
        pages at a time, and every page goes straight into the COPY that `replace_dump` is
        streaming: the crawl is never written to this host's disk and never held whole in memory.

        Which spellings of a page survive is decided there too, from the URL columns of the
        staging table (`engine.urls.duplicate_docs`) — it used to be a second pass over this
        stream, which meant the crawl could not be a stream at all.

        `on_progress` is called every few seconds with the pages read so far. It runs while the
        COPY is open, so it borrows a second pooled connection for the moment it writes the job
        row — brief, and the alternative is a status frozen at the crawler's last figure for as
        long as the ingest takes."""
        def source() -> Iterator[dict[str, Any]]:
            return iter(docs) if isinstance(docs, list) else iter_documents(docs)

        def take(it: Iterator[dict[str, Any]]) -> tuple[int, list[DumpUrl]]:
            """The next chunk of documents as rows (parsing and hashing happen here, off the event
            loop): up to `_INGEST_BATCH_DOCS` of them, cut short once they carry
            `_INGEST_BATCH_BYTES` of text."""
            seen, size, rows = 0, 0, []
            for d in it:
                seen += 1
                if d.get("url"):
                    text = _no_nul(d.get("full_text"))
                    size += len(text or "")
                    rows.append(DumpUrl(
                        collection_id=collection_id, url=_no_nul(d["url"]),
                        final_url=_no_nul(d.get("final_url")), scraped_title=_no_nul(d.get("title")),
                        full_text=text, content_type=_no_nul(d.get("content_type")), depth=d.get("depth"),
                        content_hash=content_hash(text),
                    ))
                if seen >= _INGEST_BATCH_DOCS or size >= _INGEST_BATCH_BYTES:
                    break
            return seen, rows

        linked = 0  # pages read that have a URL: what the duplicate-link pass started from

        async def rows() -> AsyncIterator[DumpUrl]:
            """Counts as it reads: this is the only point that knows how far into the crawl the
            ingest has got. The count is pages taken off the stream, not rows in the table — the
            duplicate-spelling pass runs afterwards, so the job's final figure is a little lower."""
            nonlocal linked
            it = source()
            read, reported = 0, 0.0
            while True:
                seen, chunk = await asyncio.to_thread(take, it)
                linked += len(chunk)
                for r in chunk:
                    yield r
                read += seen
                if not seen:
                    break
                now = time.monotonic()
                if on_progress and now - reported >= _INGEST_PROGRESS_S:
                    reported = now
                    await on_progress({"phase": "ingest", "ingested": read})
            if on_progress:
                # the stream is spent; what is left is the duplicate pass and the two INSERTs
                await on_progress({"phase": "ingest_store", "ingested": read})

        fails = [
            DumpFailure(
                collection_id=collection_id, url=_no_nul(f["url"]), reason=_no_nul(str(f["reason"])),
                status=f["status"] if isinstance(f.get("status"), int) else None,
                detail=(_no_nul(str(f.get("detail") or ""))[:500] or None),
            )
            for f in failures or []
        ]
        n = await self.db.replace_dump(collection_id, rows(), fails, dedupe_spellings=True)
        if on_progress:
            # A crawl that reached a page under several links (http/https, www., a trailing slash)
            # keeps it once: say how many went, or 45,024 read → 22,324 stored looks like loss.
            await on_progress({"duplicates_dropped": linked - n})
        # Hand the ingest's memory back to the OS: the connection's buffers first, then the heap
        # glibc keeps after the pages are freed. Measured on an ascl.net-shaped crawl, the two
        # were ~90% of what stayed resident after the job (live Python objects were ~2%).
        await self.db.recycle_connections()
        await asyncio.to_thread(_trim_heap)
        return n


def _trim_heap() -> None:
    """Return freed heap to the OS (glibc only; a no-op on macOS and other libcs)."""
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except (OSError, AttributeError):
        pass


def _expected_docs(summary: dict[str, Any], progress: dict[str, Any]) -> int | None:
    """How many pages the ingest is about to read, for an "x of y" status: the crawl's own count
    when it wrote a summary (a reused crawl has one and nothing else), else what the job watched
    the crawler log. Either can be absent, and then the status just counts up."""
    n = summary.get("documents_scraped") or progress.get("docs")
    return int(n) if isinstance(n, int | float) and n > 0 else None


def _add_tokens(job: JobRun, row: dict[str, Any]) -> None:
    """Move a call's token usage from its result row onto the job's running totals."""
    p = job.progress
    for k in ("tokens_in", "tokens_out", "tokens_cached"):
        p[k] = p.get(k, 0) + row.pop(k, 0)


def _no_nul(v: Any) -> Any:
    """Postgres text cannot hold NUL; the crawler's PDF text extraction sometimes emits it."""
    return v.replace("\x00", "") if isinstance(v, str) else v


def _brief(summary: dict[str, Any]) -> dict[str, Any]:
    keys = ("documents_scraped", "failures_logged", "failures_by_reason", "robots_fetch_ok")
    return {k: summary[k] for k in keys if k in summary}


def _now_iso() -> str:
    return utcnow().isoformat()
