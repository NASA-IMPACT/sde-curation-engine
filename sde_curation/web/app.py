"""FastAPI application: JSON API + HTMX pages + SSE."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, model_validator
from sse_starlette.sse import EventSourceResponse

from ..backends.scrape import make_scrape_backend
from ..config import Settings, get_settings
from ..curation import CurationService
from ..db import Database
from ..events import EventBus, sse_format
from ..jobs import JobConflict, JobManager
from ..models import (
    Collection,
    CollectionCreate,
    Division,
    DocumentType,
    PatternCreate,
    PatternType,
    Status,
)
from ..store import remove_collection_files, write_collection_yaml, write_patterns_yaml

_HERE = Path(__file__).parent
templates = Jinja2Templates(directory=_HERE / "templates")


def dom_id(value: str) -> str:
    """Collection ids contain dots (host names); make them safe for CSS id selectors."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", value)


templates.env.filters["dom_id"] = dom_id

PIPELINE = [
    (Status.BACKLOG, "Backlog", "Collection registered"),
    (Status.SCRAPED, "Scraped", "Crawl finished, dump ingested"),
    (Status.CURATING, "Curating", "Deltas computed, patterns being applied"),
    (Status.CURATED, "Curated", "Deltas promoted to the curated set"),
    (Status.CONFIG_GENERATED, "Test index", "Exported and indexed to the test index"),
    (Status.LIVE, "Live", "Validated and indexed to production"),
]
_ORDER = {st: i for i, (st, _, _) in enumerate(PIPELINE)}


def next_action(c: Collection, job) -> dict:
    """The one thing the curator should do next, given where the collection is."""
    cid = c.collection_id
    if job and job.state == "running":
        return {"label": f"{job.kind} running…", "kind": "busy"}
    if c.status is Status.BACKLOG or (c.status is Status.SCRAPED and c.dump_count == 0):
        return {"label": "Scrape", "kind": "post", "url": f"/api/collections/{cid}/scrape",
                "hint": "Run the crawler on the seed URL"}
    if c.status is Status.SCRAPED:
        return {"label": "Start curating", "kind": "post", "url": f"/api/collections/{cid}/recompute",
                "then": f"/collections/{cid}/curate", "hint": "Compute deltas vs. the curated set"}
    if c.status is Status.CURATING:
        return {"label": "Open curation", "kind": "link", "url": f"/collections/{cid}/curate",
                "hint": "Review deltas, add patterns, then promote"}
    if c.status is Status.CURATED:
        return {"label": "Index to test", "kind": "disabled", "hint": "Phase 5 — not built yet"}
    if c.status is Status.CONFIG_GENERATED:
        return {"label": "Validate", "kind": "disabled", "hint": "Phase 6 — not built yet"}
    return {"label": "Live ✓", "kind": "done", "hint": "Re-scrape to start a new cycle"}


def status_invariant_problem(c: Collection, new: Status) -> str | None:
    """Even a forced/manual status change must not contradict the data."""
    if new in (Status.SCRAPED, Status.CURATING) and c.dump_count == 0:
        return f"cannot be '{new}': no crawl dump yet — scrape first"
    if new in (Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE):
        if c.curated_count == 0:
            return f"cannot be '{new}': nothing has been promoted to the curated set"
        if c.delta_count and c.status is not new:
            return f"cannot be '{new}': {c.delta_count} deltas are pending — promote (or discard) them first"
    return None


def pipeline_steps(c: Collection) -> list[dict]:
    cur = _ORDER[c.status]
    return [
        {"status": st, "label": label, "hint": hint, "index": i + 1,
         "state": "done" if i < cur else "current" if i == cur else "todo"}
        for i, (st, label, hint) in enumerate(PIPELINE)
    ]


templates.env.globals.update(next_action=next_action, pipeline_steps=pipeline_steps)


class StatusChange(BaseModel):
    status: Status
    note: str | None = None
    force: bool = False


class UrlEdit(BaseModel):
    """Per-URL curator edit = an exact-URL pattern (the most specific pattern possible)."""

    url: str = Field(min_length=1)
    type: PatternType
    value: str | None = None

    @model_validator(mode="after")
    def _check(self) -> UrlEdit:
        PatternCreate(type=self.type, match=self.url, value=self.value)  # same rules → 422
        return self


def tri_bool(v: str | None) -> bool | None:
    """Query-string tri-state: '' / None → None, 'true'/'1'/'yes' → True, 'false'/'0'/'no' → False."""
    if v is None or v.strip() == "":
        return None
    return v.strip().lower() in ("1", "true", "yes", "on")


def _is_htmx(request: Request) -> bool:
    return request.headers.get("HX-Request") == "true"


def htmx_done(request: Request, payload, *, then: str | None = None):
    """For HTMX callers, navigate server-side (HX-Redirect to `?then=` or the given url,
    else HX-Refresh). Header-driven so it works even if the clicked element was already
    re-rendered by an SSE event. JSON callers just get the payload."""
    if not _is_htmx(request):
        return payload
    target = request.query_params.get("then") or then
    headers = {"HX-Redirect": target} if target else {"HX-Refresh": "true"}
    return JSONResponse(jsonable_encoder(payload), headers=headers)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        db = await Database(settings.resolved_db_path).connect()
        app.state.settings = settings
        app.state.db = db
        app.state.bus = EventBus()
        jobs = JobManager(settings, db, app.state.bus, scraper=make_scrape_backend(settings))
        app.state.jobs = jobs
        app.state.curation = CurationService(db, lock_for=jobs.lock)
        await jobs.recover()
        try:
            yield
        finally:
            await jobs.shutdown()
            await db.close()

    app = FastAPI(title="SDE Curation Engine", version="0.1.0", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=_HERE / "static"), name="static")

    # ── helpers ────────────────────────────────────────────────────────

    def db(request: Request) -> Database:
        return request.app.state.db

    def bus(request: Request) -> EventBus:
        return request.app.state.bus

    async def must_get(request: Request, collection_id: str) -> Collection:
        c = await db(request).get_collection(collection_id)
        if c is None:
            raise HTTPException(404, f"collection {collection_id!r} not found")
        return c

    def ensure_idle(request: Request, c: Collection) -> None:
        """Mutating actions are refused while a job runs on the collection (409)."""
        j = request.app.state.jobs.active_for(c.collection_id)
        if j:
            raise HTTPException(409, f"{j.kind} job #{j.id} is running — wait for it or cancel it")

    async def row_context(request: Request, c: Collection) -> dict:
        return {"c": c, "job": await db(request).latest_job(c.collection_id)}

    def emit_collection(request: Request, c: Collection) -> None:
        bus(request).publish(
            "collection",
            {
                "collection_id": c.collection_id,
                "status": c.status,
                "updated_at": c.updated_at.isoformat(),
            },
        )

    # ── health ─────────────────────────────────────────────────────────

    @app.get("/health")
    async def health(request: Request) -> dict:
        try:
            ok = await db(request).ping()
        except Exception as e:  # noqa: BLE001 - surfaced to the caller, not hidden
            return {"ok": False, "db": f"error: {e}"}
        return {
            "ok": ok,
            "db": "ok" if ok else "error",
            "sse_clients": bus(request).subscriber_count,
        }

    # ── pages ──────────────────────────────────────────────────────────

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        rows = [await row_context(request, c) for c in await db(request).list_collections()]
        return templates.TemplateResponse(
            request, "dashboard.html", {"rows": rows, "statuses": list(Status)}
        )

    @app.get("/rows", response_class=HTMLResponse)
    async def dashboard_rows(request: Request):
        rows = [await row_context(request, c) for c in await db(request).list_collections()]
        return templates.TemplateResponse(request, "partials/rows.html", {"rows": rows})

    @app.get("/collections/{collection_id}", response_class=HTMLResponse)
    async def collection_page(request: Request, collection_id: str, step: str | None = None):
        c = await must_get(request, collection_id)
        sel = Status(step) if step in {s.value for s in Status} else c.status
        ctx = await step_context(request, c, sel)
        ctx["selected"] = sel
        ctx.update(
            history=await db(request).status_history(collection_id),
            jobs=await db(request).list_jobs(collection_id),
            statuses=list(Status),
        )
        return templates.TemplateResponse(request, "collection.html", ctx)

    async def step_context(request: Request, c: Collection, step: Status) -> dict:
        d = db(request)
        jobs = await d.list_jobs(c.collection_id, limit=20)
        _, total = await d.list_deltas(c.collection_id, limit=1)
        counts = {"new": 0, "modified": 0, "deleted": 0, "excluded": 0}
        if total:
            for k in ("new", "modified", "deleted"):
                counts[k] = (await d.list_deltas(c.collection_id, kind=k, limit=1))[1]
            counts["excluded"] = (await d.list_deltas(c.collection_id, excluded=True, limit=1))[1]
        curated = await d.load_curated(c.collection_id) if c.curated_count else []
        stats = {
            "last_scrape": next((j for j in jobs if j.kind == "scrape"), None),
            "delta_counts": counts,
            "pattern_count": len(await d.list_patterns(c.collection_id)),
            "curated_excluded": sum(1 for r in curated if r.excluded),
        }
        return {"c": c, "job": jobs[0] if jobs else None, "step": step,
                "steps": pipeline_steps(c), "stats": stats}

    @app.get("/collections/{collection_id}/pipeline", response_class=HTMLResponse)
    async def collection_pipeline(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        ctx = await step_context(request, c, c.status)
        ctx["selected"] = c.status
        return templates.TemplateResponse(request, "partials/pipeline_inner.html", ctx)

    @app.get("/collections/{collection_id}/step/{step}", response_class=HTMLResponse)
    async def collection_step(request: Request, collection_id: str, step: Status):
        c = await must_get(request, collection_id)
        return templates.TemplateResponse(
            request, "partials/step.html", await step_context(request, c, step)
        )

    @app.get("/collections/{collection_id}/row", response_class=HTMLResponse)
    async def collection_row(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        return templates.TemplateResponse(
            request, "partials/row_cells.html", await row_context(request, c)
        )

    # ── API ────────────────────────────────────────────────────────────

    @app.get("/api/collections", response_model=list[Collection])
    async def api_list(request: Request):
        return await db(request).list_collections()

    @app.post("/api/collections", response_model=Collection, status_code=201)
    async def api_create(request: Request, body: CollectionCreate):
        if await db(request).get_collection(body.collection_id):
            raise HTTPException(409, f"collection {body.collection_id!r} already exists")
        c = Collection(**body.model_dump())
        await db(request).insert_collection(c)
        write_collection_yaml(settings.collections_dir, c)
        bus(request).publish("collection_created", {"collection_id": c.collection_id})
        emit_collection(request, c)
        return c

    @app.post("/collections", include_in_schema=False)
    async def form_create(request: Request):
        form = await request.form()
        data = {k: v for k, v in form.items() if v != ""}
        try:
            body = CollectionCreate(**data)
            await api_create(request, body)
        except (ValueError, HTTPException) as e:
            msg = e.detail if isinstance(e, HTTPException) else "; ".join(
                err["msg"] for err in e.errors()
            ) if hasattr(e, "errors") else str(e)
            rows = [await row_context(request, c) for c in await db(request).list_collections()]
            return templates.TemplateResponse(
                request, "dashboard.html",
                {"rows": rows, "statuses": list(Status), "error": msg, "form": data},
                status_code=422,
            )
        return RedirectResponse("/", status_code=303)

    @app.get("/api/collections/{collection_id}", response_model=Collection)
    async def api_get(request: Request, collection_id: str):
        return await must_get(request, collection_id)

    @app.delete("/api/collections/{collection_id}", response_model=None)
    async def api_delete(request: Request, collection_id: str):
        ensure_idle(request, await must_get(request, collection_id))
        if not await db(request).delete_collection(collection_id):
            raise HTTPException(404, "not found")
        remove_collection_files(settings.collections_dir, collection_id)
        bus(request).publish("collection_deleted", {"collection_id": collection_id})
        if _is_htmx(request):
            return JSONResponse(None, status_code=200, headers={"HX-Redirect": "/"})
        return Response(status_code=204)

    @app.post("/api/collections/{collection_id}/status", response_model=None)
    async def api_set_status(request: Request, collection_id: str, body: StatusChange):
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        problem = status_invariant_problem(c, body.status)
        if problem:
            raise HTTPException(409, problem)
        try:
            c = await db(request).set_status(
                collection_id, body.status, body.note, force=body.force
            )
        except ValueError as e:
            raise HTTPException(409, str(e)) from e
        write_collection_yaml(settings.collections_dir, c)
        emit_collection(request, c)
        if _is_htmx(request) and request.headers.get("HX-Target", "").startswith("row-"):
            return templates.TemplateResponse(
                request, "partials/row.html", await row_context(request, c)
            )
        return htmx_done(request, c)

    @app.get("/api/collections/{collection_id}/history")
    async def api_history(request: Request, collection_id: str):
        await must_get(request, collection_id)
        return await db(request).status_history(collection_id)

    # ── jobs ───────────────────────────────────────────────────────────

    @app.post("/api/collections/{collection_id}/scrape", status_code=202, response_model=None)
    async def api_scrape(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        jobs: JobManager = request.app.state.jobs
        try:
            job = await jobs.start_scrape(c)
        except JobConflict as e:
            raise HTTPException(409, str(e)) from e
        if _is_htmx(request) and request.headers.get("HX-Target", "").startswith("row-"):
            return templates.TemplateResponse(
                request, "partials/row.html", await row_context(request, c)
            )
        return htmx_done(request, job)

    @app.post("/api/collections/{collection_id}/jobs/cancel")
    async def api_cancel_job(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        jobs: JobManager = request.app.state.jobs
        job = await jobs.cancel(c.collection_id)
        if job is None:
            raise HTTPException(409, "no running job")
        if _is_htmx(request) and request.headers.get("HX-Target", "").startswith("row-"):
            return templates.TemplateResponse(
                request, "partials/row.html", await row_context(request, c)
            )
        return htmx_done(request, job)

    @app.get("/api/collections/{collection_id}/jobs")
    async def api_jobs(request: Request, collection_id: str):
        await must_get(request, collection_id)
        return await db(request).list_jobs(collection_id)

    @app.get("/api/collections/{collection_id}/dump")
    async def api_dump(request: Request, collection_id: str, limit: int = 100, offset: int = 0):
        await must_get(request, collection_id)
        rows = await db(request).list_dump(collection_id, limit=min(limit, 1000), offset=offset)
        return [r.model_dump(exclude={"full_text"}) for r in rows]

    # ── curation ───────────────────────────────────────────────────────

    def curation(request: Request) -> CurationService:
        return request.app.state.curation

    async def _after_curation_change(request: Request, c: Collection, ds) -> Collection:
        """A diff/pattern change that produces pending deltas puts the collection in
        'curating'. A recompute with nothing to review never demotes a curated/live
        collection (otherwise it would be stuck: nothing to promote, no way forward)."""
        n = len(ds.deltas)
        pre = c.status in (Status.BACKLOG, Status.SCRAPED)
        if pre and n == 0 and c.curated_count:
            # re-crawl identical to the curated set: nothing to review
            await db(request).set_flag(c.collection_id, False)
            c = await db(request).set_status(
                c.collection_id, Status.CURATED, note="re-crawl matches curated set: no changes",
                force=True,
            )
        elif (pre and n) or (
            n and c.status in (Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE)
        ):
            c = await db(request).set_status(
                c.collection_id, Status.CURATING, note=f"deltas recomputed: {n} pending", force=True
            )
        elif n == 0 and c.status is Status.CURATING and c.curated_count:
            # nothing left to review on an already-promoted set → it is curated
            c = await db(request).set_status(
                c.collection_id, Status.CURATED, note="recomputed: no pending deltas", force=True
            )
        c = await must_get(request, c.collection_id)
        write_patterns_yaml(settings.collections_dir, c.collection_id,
                            await db(request).list_patterns(c.collection_id))
        emit_collection(request, c)
        return c

    @app.post("/api/collections/{collection_id}/recompute")
    async def api_recompute(request: Request, collection_id: str):
        """Calculate deltas (dump vs curated) and apply all patterns. Idempotent."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if c.dump_count == 0:
            raise HTTPException(409, "no dump ingested yet — scrape first")
        ds = await curation(request).recompute(c)
        await _after_curation_change(request, c, ds)
        return htmx_done(request, ds.counts)

    @app.get("/api/collections/{collection_id}/patterns")
    async def api_patterns(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        return await curation(request).pattern_stats(c)

    @app.post("/api/collections/{collection_id}/patterns", status_code=201)
    async def api_add_pattern(request: Request, collection_id: str, body: PatternCreate):
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        try:
            p, ds = await curation(request).add_pattern(c, body)
        except Exception as e:
            if "UNIQUE" in str(e):
                raise HTTPException(409, "pattern already exists") from e
            raise
        await _after_curation_change(request, c, ds)
        return htmx_done(request, {"pattern": p.model_dump(mode="json"), "deltas": ds.counts})

    @app.delete("/api/collections/{collection_id}/patterns/{pattern_id}")
    async def api_delete_pattern(request: Request, collection_id: str, pattern_id: int):
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        ds = await curation(request).delete_pattern(c, pattern_id)
        if ds is None:
            raise HTTPException(404, "pattern not found")
        await _after_curation_change(request, c, ds)
        return htmx_done(request, ds.counts)

    @app.post("/api/collections/{collection_id}/urls")
    async def api_url_edit(request: Request, collection_id: str, body: UrlEdit):
        """Curator edit on one URL: exact-match pattern. Repeating an exclude/include removes it."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        existing = [p for p in await db(request).list_patterns(collection_id)
                    if p.match == body.url and p.type == body.type]
        if existing and body.type in (PatternType.EXCLUDE, PatternType.INCLUDE):
            ds = await curation(request).delete_pattern(c, existing[0].id)
        elif existing and existing[0].value == body.value:
            ds = await curation(request).recompute(c)  # no-op edit
        else:
            ds = await curation(request).replace_exact_pattern(
                c, PatternCreate(type=body.type, match=body.url, value=body.value),
                old_id=existing[0].id if existing else None,
            )
        await _after_curation_change(request, c, ds)
        return htmx_done(request, ds.counts)

    @app.get("/api/collections/{collection_id}/deltas")
    async def api_deltas(
        request: Request, collection_id: str, kind: str | None = None,
        excluded: str | None = None, q: str | None = None, limit: int = 100, offset: int = 0,
    ):
        await must_get(request, collection_id)
        rows, total = await db(request).list_deltas(
            collection_id, kind=kind or None, excluded=tri_bool(excluded), q=q or None,
            limit=max(1, min(limit, 1000)), offset=max(0, offset),
        )
        return {"total": total, "items": rows}

    @app.post("/api/collections/{collection_id}/promote")
    async def api_promote(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if c.status is not Status.CURATING:
            raise HTTPException(409, f"promote requires status 'curating' (is {c.status})")
        n = await curation(request).promote(c)
        c = await must_get(request, collection_id)
        write_collection_yaml(settings.collections_dir, c)
        emit_collection(request, c)
        return htmx_done(request, {"curated": n, "status": c.status})

    @app.get("/collections/{collection_id}/curate", response_class=HTMLResponse)
    async def curate_page(
        request: Request, collection_id: str, kind: str | None = None,
        excluded: str | None = None, q: str | None = None, page: int = 1,
    ):
        c = await must_get(request, collection_id)
        per, page = 50, max(1, page)
        kind, excluded = kind or None, tri_bool(excluded)
        rows, total = await db(request).list_deltas(
            collection_id, kind=kind, excluded=excluded, q=q or None, limit=per, offset=(page - 1) * per
        )
        ctx = {
            "c": c, "job": await db(request).latest_job(collection_id),
            "rows": rows, "total": total, "page": page, "pages": max(1, -(-total // per)),
            "kind": kind, "excluded": excluded, "q": q or "",
            "patterns": await curation(request).pattern_stats(c),
            "divisions": list(Division), "doc_types": list(DocumentType),
            "pattern_types": list(PatternType),
        }
        return templates.TemplateResponse(request, "curate.html", ctx)

    # ── SSE ────────────────────────────────────────────────────────────

    @app.get("/events")
    async def events(request: Request):
        async def gen():
            async for msg in bus(request).subscribe():
                yield sse_format(msg)

        return EventSourceResponse(gen(), ping=15)

    return app


app = create_app()
