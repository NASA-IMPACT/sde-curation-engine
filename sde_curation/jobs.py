"""JobManager: runs long work as asyncio background tasks, one at a time per collection,
records explicit success/failure in job_runs, and publishes SSE events."""

from __future__ import annotations

import asyncio
import logging
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .backends.index import Dispatch, IndexBackend, IndexError_, wait_for_status
from .backends.s3 import S3
from .backends.scrape import ScrapeBackend, ScrapeError, parse_documents
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
from .engine.urls import batches, dedupe_variants
from .events import EventBus
from .llm.base import LLMError, LLMProvider
from .llm.global_excludes import global_exclude_hits, load_global_excludes
from .llm.pool import run_pool
from .llm.tasks import suggest_metadata_one, suggest_patterns_batch
from .models import (
    SYSTEM_ACTOR,
    Collection,
    DumpFailure,
    DumpUrl,
    IndexRun,
    JobKind,
    JobRun,
    JobState,
    Pattern,
    Status,
    utcnow,
)

log = logging.getLogger(__name__)

# Suggest metadata writes answers to the DB in small chunks (cancel keeps them, commits stay few).
AI_FLUSH_ROWS = 25
AI_FLUSH_SECONDS = 2.0


class JobConflict(Exception):
    pass


class JobManager:
    def __init__(
        self, settings: Settings, db: Database, bus: EventBus, *, scraper: ScrapeBackend,
        llm: LLMProvider | Callable[[], LLMProvider] | None = None,
        indexer: IndexBackend | Callable[[], IndexBackend] | None = None,
    ):
        self.s = settings
        self.db = db
        self.bus = bus
        self.scraper = scraper
        self._llm = llm
        self._indexer = indexer
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
        """Startup: jobs left 'running' by a previous process are dead — say so explicitly."""
        for j in await self.db.active_jobs():
            await self.db.finish_job(j, JobState.FAILED, error="engine restarted while job was running")
            self._emit(j.collection_id, j)

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
                docs = parse_documents(result.documents_path)
                failures = result.failures()
                n = await self.ingest_dump(c.collection_id, docs, failures)
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
                # Collection state first, job record last: "succeeded" must mean every effect of
                # the job is already visible to whoever polls the job list.
                updated = await self.db.set_status(
                    c.collection_id, Status.SCRAPED, note=note, force=True, actor=SYSTEM_ACTOR,
                )
                if c.curated_count:  # anything already promoted must be re-reviewed
                    reason = (f"{'loaded existing crawl' if reuse else 're-crawled'} on {crawled_at:%Y-%m-%d %H:%M}Z"
                              f" ({n} documents) after {c.curated_count} URLs were promoted — Start curating"
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

    async def start_llm_patterns(self, c: Collection, *, actor: str | None = None) -> JobRun:
        return await self._start(c, JobKind.LLM_PATTERNS, lambda job: self._run_llm_patterns(c, job), actor=actor)

    async def start_llm_metadata(
        self, c: Collection, *, only_missing: bool = True, actor: str | None = None
    ) -> JobRun:
        return await self._start(
            c, JobKind.LLM_METADATA, lambda job: self._run_llm_metadata(c, job, only_missing), actor=actor
        )

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
            examples = [g["match"] for g in sorted(gl, key=lambda g: -g["matches"])[:15]]
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
                counts = match_counts(
                    [Pattern(id=i, collection_id=cid, type=s.type, match=s.match) for i, s in enumerate(kept)],
                    all_urls)
                rows = [{"type": s.type, "match": s.match, "rationale": s.rationale, "matches": counts.get(i, 0)}
                        for i, s in enumerate(kept)]
                added = await self.db.add_pattern_suggestions(cid, rows)
                job.progress["suggestions"] = job.progress.get("suggestions", 0) + added
                job.progress["tokens_in"] = job.progress.get("tokens_in", 0) + done.tokens_in
                job.progress["tokens_out"] = job.progress.get("tokens_out", 0) + done.tokens_out

            await run_pool(list(enumerate(chunks)), one, workers=self.s.llm_workers, on_result=on_result,
                           on_progress=progress, total=len(chunks))
            await progress({"suggestions": await self.db.count_pending_pattern_suggestions(cid)})
        await self._guarded(c, job, body)

    async def _run_llm_metadata(self, c: Collection, job: JobRun, only_missing: bool) -> None:
        """One call per included delta URL with the full page text, LLM_WORKERS at a time.
        Results are written in small chunks as they arrive, so a cancel keeps what finished and
        a re-run (only_missing) resumes with the rest. One bad URL never fails the job."""
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
            last_flush = time.monotonic()
            written = 0

            async def flush() -> None:
                nonlocal last_flush, written
                last_flush = time.monotonic()
                if buf:
                    rows, buf[:] = list(buf), []
                    n = await self.db.set_delta_ai(cid, rows)
                    written += n  # never `written += await …`: two flushes overlap and one is lost

            async def on_result(doc, row):
                p = job.progress
                p["tokens_in"] = p.get("tokens_in", 0) + row.pop("tokens_in", 0)
                p["tokens_out"] = p.get("tokens_out", 0) + row.pop("tokens_out", 0)
                p["tokens_cached"] = p.get("tokens_cached", 0) + row.pop("tokens_cached", 0)
                buf.append(row)
                if len(buf) >= AI_FLUSH_ROWS or time.monotonic() - last_flush > AI_FLUSH_SECONDS:
                    await flush()

            try:
                await run_pool(
                    self.db.iter_deltas_for_llm(cid, only_missing=only_missing),
                    lambda d: suggest_metadata_one(llm, d, settings=self.s),
                    workers=self.s.llm_workers, on_result=on_result, on_progress=progress, total=total,
                )
            finally:
                await flush()  # a cancel still keeps every answer that arrived
            await progress({"classified": written, "inflight": 0})
        await self._guarded(c, job, body)

    # ── index (export → S3 → WEB_COSMOS → status.json) ─────────────────

    def indexer(self) -> IndexBackend:
        if self._indexer is None:
            raise IndexError_("no index backend configured")
        return self._indexer() if callable(self._indexer) and not hasattr(self._indexer, "complete") and not hasattr(self._indexer, "dispatch") else self._indexer  # type: ignore[return-value]

    async def start_index(
        self, c: Collection, target: str, *, actor: str | None = None
    ) -> tuple[JobRun, IndexRun]:
        if not self.s.cosmos_index_bucket:
            raise IndexError_("COSMOS_INDEX_BUCKET is not set")
        run = IndexRun(run_id=mint_run_id(), collection_id=c.collection_id, target=target, started_by=actor)
        kind = JobKind.INDEX_PROD if target == "prod" else JobKind.INDEX_TEST
        job = await self._start(c, kind, lambda job: self._run_index(c, job, run), actor=actor)
        job.run_id = run.run_id
        await self.db.update_job(job)
        return job, run

    async def _run_index(self, c: Collection, job: JobRun, run: IndexRun) -> None:
        async def body():
            s3 = S3(self.s.cosmos_index_bucket, region=self.s.aws_region)
            await self.db.insert_index_run(run)
            backend = self.indexer()

            async def progress(p: dict[str, Any]) -> None:
                job.progress = {**job.progress, **p}
                await self.db.update_job(job)
                self._emit(c, job)

            # 1. export: stream curated (non-excluded) rows — with the text they were approved
            #    with — to a temp jsonl, upload, THEN the manifest
            curated = await self.db.load_curated(c.collection_id, with_text=True)
            with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as fh:
                n = write_jsonl(export_lines(curated), fh)
                tmp = Path(fh.name)
            try:
                if n == 0:
                    raise IndexError_("nothing to export: every curated URL is excluded")
                prefix = export_prefix(c.collection_id, run.run_id)
                await s3.upload_file(tmp, f"{prefix}/documents.jsonl", "application/x-ndjson")
                manifest = build_manifest(c, run.run_id, n, run.target)
                await s3.put_json(f"{prefix}/manifest.json", manifest.model_dump(mode="json"))
            finally:
                tmp.unlink(missing_ok=True)
            run.exported = n
            await self.db.update_index_run(run)
            await progress({"exported": n, "export": s3.url(prefix), "phase": "dispatch"})

            # 2. dispatch
            d: Dispatch = await backend.dispatch(c, run.run_id, run.target)
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

            if run.target == "prod":
                await self.db.set_status(
                    c.collection_id, Status.LIVE, force=True, actor=SYSTEM_ACTOR,
                    note=f"prod index run {run.run_id}: {status.indexed} indexed, {status.deleted} deleted",
                )
                await self.db.set_flag(c.collection_id, False)
                return

            await self.db.set_status(
                c.collection_id, Status.CONFIG_GENERATED, force=True, actor=SYSTEM_ACTOR,
                note=f"test index run {run.run_id}: {status.indexed} indexed, {status.deleted} deleted",
            )
            # 4. validation gate — the indexer's own validation.json is pre-refresh; re-check after a delay
            await self._validate(c, job, run, s3, backend, progress)

        await self._guarded(c, job, body)

    async def _validate(self, c: Collection, job: JobRun, run: IndexRun, s3: S3, backend: IndexBackend, progress) -> None:
        """Wait for the index to refresh, then validate directly (fast); on 403 fall back to a
        second pass of the same export (changed: 0) purely to get a fresh validation.json."""
        await progress({"phase": "validating", "validation_delay_s": int(self.s.validation_delay_s)})
        await asyncio.sleep(self.s.validation_delay_s)
        expected = await self._expected_titles(c.collection_id)
        try:
            report = await validate_direct(
                self.s, collection_key=c.collection_id, run_id=run.run_id, target=run.target, expected_titles=expected
            )
            run.validated_by = "direct"
        except NoIndexAccess as e:
            log.warning("direct validation unavailable (%s) — falling back to a second indexer pass", e)
            await progress({"phase": "validating", "fallback": "second_pass", "reason": str(e)[:200]})
            # the first pass left status.json/validation.json behind — remove them or the poller
            # would return the stale (pre-refresh) validation immediately
            prefix = status_prefix(c.collection_id, run.run_id)
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
        if ok:
            await self.db.set_flag(c.collection_id, False)
            await self.db.set_status(
                c.collection_id, Status.CONFIG_GENERATED, force=True, actor=SYSTEM_ACTOR,
                note=f"validated ({run.validated_by}): {report['indexed_count']}/{report['expected_count']}, titles {report['title_match_rate']:.1%}",
            )
        else:
            reason = (f"test-index validation failed ({run.validated_by}): {report['indexed_count']}/{report['expected_count']}"
                      f" indexed, titles {report['title_match_rate']:.1%} — fix and re-index, or Re-validate")
            await self.db.set_flag(c.collection_id, True, reason)
            await self.db.set_status(
                c.collection_id, Status.CURATING, force=True, actor=SYSTEM_ACTOR,
                note=f"validation FAILED ({run.validated_by}): {report['indexed_count']}/{report['expected_count']} indexed, titles {report['title_match_rate']:.1%} — needs re-curation",
            )

    async def _expected_titles(self, collection_id: str) -> dict[str, str]:
        curated = await self.db.load_curated(collection_id)
        return {r.url: (r.title or r.scraped_title or "").strip() for r in curated if not r.excluded}

    async def start_revalidate(self, c: Collection, run: IndexRun, *, actor: str | None = None) -> JobRun:
        """Manual re-check of an existing test run (no new export)."""
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
            await self._validate(c, job, run, s3, self.indexer(), progress)

        await self._guarded(c, job, body)

    async def ingest_dump(
        self, collection_id: str, docs: list[dict[str, Any]], failures: list[dict[str, Any]] | None = None,
    ) -> int:
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
        fails = [
            DumpFailure(
                collection_id=collection_id, url=f["url"], reason=str(f["reason"]),
                status=f["status"] if isinstance(f.get("status"), int) else None,
                detail=(str(f.get("detail") or "")[:500] or None),
            )
            for f in failures or []
        ]
        n = await self.db.replace_dump(collection_id, rows, fails)
        return n


def _brief(summary: dict[str, Any]) -> dict[str, Any]:
    keys = ("documents_scraped", "failures_logged", "failures_by_reason", "robots_fetch_ok")
    return {k: summary[k] for k in keys if k in summary}


def _now_iso() -> str:
    return utcnow().isoformat()
