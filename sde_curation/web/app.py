"""FastAPI application: JSON API + HTMX pages + SSE."""

from __future__ import annotations

import functools
import hashlib
import logging
import re
import secrets
import sqlite3
import time
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, model_validator
from sse_starlette.sse import EventSourceResponse

from ..backends.index import IndexError_, make_index_backend
from ..backends.scrape import make_scrape_backend
from ..config import Settings, get_settings
from ..curation import CurationService
from ..db import Database
from ..events import EventBus, sse_format
from ..jobs import JobConflict, JobManager
from ..llm.base import LLMError, make_llm
from ..llm.global_excludes import load_global_excludes
from ..llm.tasks import METADATA_SYSTEM, PATTERN_SYSTEM
from ..models import (
    ANONYMOUS_ACTOR,
    Collection,
    CollectionCreate,
    CurationStage,
    Division,
    DocumentType,
    PatternCreate,
    PatternType,
    Role,
    Status,
    User,
    utcnow,
)
from ..notify import Notifier
from ..store import remove_collection_files, write_collection_yaml, write_patterns_yaml
from . import auth

_HERE = Path(__file__).parent
log = logging.getLogger(__name__)
templates = Jinja2Templates(directory=_HERE / "templates")


def dom_id(value: str) -> str:
    """Collection ids contain dots (host names); make them safe for CSS id selectors."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", value)


def since(dt: datetime) -> str:
    """Compact elapsed time since `dt`: 45s, 12m, 3h 10m, 2d 4h."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    s = max(int((utcnow() - dt).total_seconds()), 0)
    if s < 60:
        return f"{s}s"
    m, h, d = s // 60, s // 3600, s // 86400
    if h < 1:
        return f"{m}m"
    if d < 1:
        return f"{h}h {m % 60}m"
    return f"{d}d {h % 24}h"


templates.env.filters["dom_id"] = dom_id
templates.env.filters["since"] = since


@functools.lru_cache(maxsize=32)
def _static_digest(name: str, mtime_ns: int) -> str:
    return hashlib.sha1((_HERE / "static" / name).read_bytes()).hexdigest()[:10]


def static_url(name: str) -> str:
    """/static/<name>?v=<content hash> so browsers drop cached copies after a deploy."""
    path = _HERE / "static" / name
    digest = _static_digest(name, path.stat().st_mtime_ns) if path.exists() else "0"
    return f"/static/{name}?v={digest}"


templates.env.globals["static_url"] = static_url

# (status, label, what to do while this is the current/upcoming step, what it means once done)
PIPELINE = [
    (Status.BACKLOG, "Backlog", "Run the crawler on the seed URL, or load an existing crawl.", "Registered"),
    (Status.SCRAPED, "Scraped", "Click Start curating to compute what changed vs. the curated set.", "Crawl ingested"),
    (Status.CURATING, "Curating", "Settle the exclusions, set metadata, then promote.", "Reviewed and promoted"),
    (Status.CURATED, "Curated", "Index the curated set to the test index.", "Curated set promoted"),
    (Status.CONFIG_GENERATED, "Test index", "Check the validation result, then index to production.", "Indexed to test and validated"),
    (Status.LIVE, "Live", "Live. Re-scrape to start a new cycle.", "Indexed to production"),
]
_ORDER = {st: i for i, (st, _, _, _) in enumerate(PIPELINE)}

# Which pipeline step a job kind belongs to (failure footers, step panels).
STEP_FOR_KIND = {
    "scrape": Status.BACKLOG, "llm_patterns": Status.CURATING, "llm_metadata": Status.CURATING,
    "index_test": Status.CONFIG_GENERATED, "validate": Status.CONFIG_GENERATED, "index_prod": Status.LIVE,
}


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
                "then": f"/collections/{cid}?tab=curate", "hint": "Compute what changed vs. the curated set"}
    if c.status is Status.CURATING:
        return {"label": "Open curation", "kind": "link", "url": f"/collections/{cid}?tab=curate",
                "hint": "Settle the exclusions, set metadata, then promote"}
    if c.status is Status.CURATED:
        return {"label": "Index to test", "kind": "post", "url": f"/api/collections/{cid}/index?target=test",
                "hint": "Export the curated set to S3 and run the WEB_COSMOS indexer against the test index"}
    if c.status is Status.CONFIG_GENERATED:
        if getattr(c, "_validated", None):
            return {"label": "Index to prod", "kind": "post", "url": f"/api/collections/{cid}/index?target=prod",
                    "confirm": "Index this collection into PRODUCTION?", "hint": "Test run validated — publish to the production index"}
        return {"label": "Re-validate", "kind": "post", "url": f"/api/collections/{cid}/index/revalidate",
                "hint": "Check the test index against the curated set (count + titles)"}
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
    steps = []
    for i, (st, label, do, done) in enumerate(PIPELINE):
        state = "done" if i < cur else "current" if i == cur else "todo"
        steps.append({"status": st, "label": label, "do": do, "done": done, "index": i + 1,
                      "state": state, "hint": done if state == "done" else do})
    return steps


STATUS_ICON = {Status.BACKLOG: "○", Status.SCRAPED: "⬇", Status.CURATING: "✎", Status.CURATED: "✓",
               Status.CONFIG_GENERATED: "⚙", Status.LIVE: "●"}
STATUS_LABEL = {st: label for st, label, _, _ in PIPELINE}
NONE_CURATOR = "__none__"  # filter value for collections without provenance


def status_icon(st) -> str:
    return STATUS_ICON.get(Status(st), "")


def status_label(st) -> str:
    return STATUS_LABEL.get(Status(st), str(st))


templates.env.globals.update(
    next_action=next_action, pipeline_steps=pipeline_steps, status_icon=status_icon, status_label=status_label,
    step_for_kind=lambda kind: STEP_FOR_KIND.get(str(kind)),
)


class StatusChange(BaseModel):
    status: Status
    note: str | None = None
    force: bool = False


class StageChange(BaseModel):
    stage: CurationStage


class SuggestionBulk(BaseModel):
    decision: Literal["accept", "reject"]
    type: PatternType | None = None  # None = every pending suggestion


class AiBulk(BaseModel):
    decision: Literal["accept", "reject"]
    field: Literal["title", "division", "document_type"]


class UrlEdit(BaseModel):
    """Per-URL curator edit = an exact-URL pattern (the most specific pattern possible)."""

    url: str = Field(min_length=1)
    type: PatternType
    value: str | None = None

    @model_validator(mode="after")
    def _check(self) -> UrlEdit:
        PatternCreate(type=self.type, match=self.url, value=self.value)  # same rules → 422
        return self


def llm_prompts(settings: Settings) -> dict[str, dict[str, str]]:
    return {
        "patterns": {
            "system": PATTERN_SYSTEM,
            "user": ("Batch:\n{\"collection\": …, \"seed\": …, \"global_excludes_already_applied\": [top 15 global globs"
                     " that matched], \"batch\": \"i of K\", \"urls\": [{\"url\": …, \"title\": scraped title}, …]}"
                     f" — up to {settings.llm_pattern_batch_urls} URLs per call, no page text"),
        },
        "metadata": {
            "system": METADATA_SYSTEM,
            "user": ("Document:\n{\"url\": …, \"scraped_title\": …, \"text_chars\": N}\n\nText:\n"
                     f"<the FULL page text, never cut; every page goes to {settings.openai_model}>"),
        },
    }


def tri_bool(v: str | None) -> bool | None:
    """Query-string tri-state: '' / None → None, 'true'/'1'/'yes' → True, 'false'/'0'/'no' → False."""
    if v is None or v.strip() == "":
        return None
    return v.strip().lower() in ("1", "true", "yes", "on")


class AiDecision(BaseModel):
    url: str
    field: Literal["title", "division", "document_type"]


def _is_htmx(request: Request) -> bool:
    return request.headers.get("HX-Request") == "true"


def _pattern_detail(p) -> str:
    return f"{p.type} {p.match}" + (f" → {p.value}" if p.value else "")


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
        db = await Database(
            settings.resolved_db_path, exclusive=settings.db_locking_mode == "exclusive"
        ).connect()
        app.state.settings = settings
        app.state.db = db
        app.state.notifier = Notifier(settings.notify_webhook_url, base_url=settings.public_base_url)

        async def status_hook(cid: str, old, new, note: str | None, actor: str | None) -> None:
            """Every status-history row: notify on real transitions, and keep collection.yaml
            (record + history with actors) current — including job-driven transitions."""
            if old != new:
                await app.state.notifier.status_changed(cid, old, new, note, actor)
            c = await db.get_collection(cid)
            if c:
                write_collection_yaml(settings.collections_dir, c, await db.status_history(cid))

        db.on_status_change = status_hook
        if settings.app_password and await db.count_users() == 0:
            # first start with login enabled: APP_PASSWORD seeds the bootstrap admin account
            await db.create_user("admin", auth.hash_password(settings.app_password), Role.ADMIN)
        app.state.bus = EventBus()
        jobs = JobManager(
            settings, db, app.state.bus, scraper=make_scrape_backend(settings),
            llm=lambda: make_llm(settings),  # lazy: a missing API key only fails the LLM job
            indexer=lambda: make_index_backend(settings),
        )
        app.state.jobs = jobs
        app.state.existing_cache = {}
        app.state.curation = CurationService(db, lock_for=jobs.lock)
        await jobs.recover()
        try:
            yield
        finally:
            await jobs.shutdown()
            await db.close()

    app = FastAPI(title="SDE Curation Engine", version="0.1.0", lifespan=lifespan)
    templates.env.globals["settings"] = settings
    app.mount("/static", StaticFiles(directory=_HERE / "static"), name="static")

    @app.middleware("http")
    async def static_cache_control(request: Request, call_next):
        """?v=<hash> URLs never change content → cache for a year; bare /static URLs revalidate."""
        response = await call_next(request)
        if request.url.path.startswith("/static/") and response.status_code == 200:
            hashed = request.query_params.get("v") and request.query_params["v"] == static_url(
                request.url.path.removeprefix("/static/")).rsplit("v=", 1)[1]
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable" if hashed else "no-cache"
        return response

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

    # ── identity (set by auth.AuthMiddleware; absent when login is off) ─

    def current_user(request: Request) -> User | None:
        return getattr(request.state, "user", None)

    def actor(request: Request) -> str:
        """Provenance name for whoever is making this request."""
        u = current_user(request)
        return u.username if u else ANONYMOUS_ACTOR

    def is_admin(request: Request) -> bool:
        if not settings.app_password:
            return True
        u = current_user(request)
        return u is not None and u.role is Role.ADMIN

    def require_admin(request: Request) -> None:
        if not is_admin(request):
            raise HTTPException(403, "admin only")

    templates.env.globals.update(current_user=current_user, is_admin=is_admin)

    async def audit(request: Request, action: str, collection_id: str | None = None, detail: str | None = None) -> None:
        await db(request).audit(actor(request), action, collection_id, detail)

    async def with_validation(request: Request, c: Collection) -> Collection:
        if c.status is Status.CONFIG_GENERATED:
            last = await db(request).last_index_run(c.collection_id, "test")
            c._validated = bool(last and last.state == "succeeded" and last.validation_passes(settings.validation_title_match_threshold))
        return c

    async def row_context(request: Request, c: Collection) -> dict:
        return {"c": await with_validation(request, c), "job": await db(request).latest_job(c.collection_id)}

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

    async def dashboard_context(request: Request) -> dict:
        """Collections filtered by the left pane (?status=&division=&curator=&q=), plus
        unfiltered per-option counts so the pane shows the whole distribution."""
        qp = request.query_params
        f = {
            "status": set(qp.getlist("status")), "stage": set(qp.getlist("stage")),
            "flag": set(qp.getlist("flag")), "division": set(qp.getlist("division")),
            "curator": set(qp.getlist("curator")), "q": (qp.get("q") or "").strip(),
        }
        cols = await db(request).list_collections()

        def curator_of(c: Collection) -> str:
            return c.created_by or NONE_CURATOR

        def keep(c: Collection) -> bool:
            # status and its curating sub-stages are one facet: any ticked box admits the row
            if (f["status"] or f["stage"]) and c.status not in f["status"] and not (
                c.status is Status.CURATING and c.curation_stage in f["stage"]
            ):
                return False
            if "needs_recuration" in f["flag"] and not c.needs_recuration:
                return False
            if f["division"] and c.division not in f["division"]:
                return False
            if f["curator"] and curator_of(c) not in f["curator"]:
                return False
            q = f["q"].lower()
            return not q or q in c.name.lower() or q in c.collection_id.lower() or q in c.seed_url.lower()

        counts = {
            "status": Counter(c.status for c in cols),
            "stage": Counter(c.curation_stage for c in cols if c.status is Status.CURATING and c.curation_stage),
            "flag": {"needs_recuration": sum(1 for c in cols if c.needs_recuration)},
            "division": Counter(c.division for c in cols),
            "curator": Counter(curator_of(c) for c in cols),
        }
        curators = sorted((k for k in counts["curator"] if k != NONE_CURATOR), key=str.lower)
        if NONE_CURATOR in counts["curator"]:
            curators.append(NONE_CURATOR)
        shown = [c for c in cols if keep(c)]
        return {
            "rows": [await row_context(request, c) for c in shown],
            "statuses": list(Status), "stages": list(CurationStage), "divisions": list(Division), "curators": curators,
            "counts": counts, "filters": f,
            "active": bool(f["q"] or f["status"] or f["stage"] or f["flag"] or f["division"] or f["curator"]),
            "total": len(cols), "shown": len(shown), "none_curator": NONE_CURATOR,
        }

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        return templates.TemplateResponse(
            request, "dashboard.html", {**await dashboard_context(request), **await jobs_context(request, compact=True)}
        )

    async def jobs_context(request: Request, *, compact: bool = False) -> dict[str, Any]:
        d = db(request)
        return {
            "jobs_active": await d.active_jobs(), "jobs_failed": await d.list_recent_jobs(5, "failed"),
            "names": {c.collection_id: c.name for c in await d.list_collections()}, "compact": compact,
        }

    @app.get("/jobs", response_class=HTMLResponse)
    async def jobs_page(request: Request):
        ctx = await jobs_context(request)
        ctx["recent"] = await db(request).list_recent_jobs(50)
        return templates.TemplateResponse(request, "jobs.html", ctx)

    @app.get("/manual", response_class=HTMLResponse)
    async def manual_page(request: Request):
        """The curator's handbook: workflow walkthrough with screenshots, rule semantics, quirks."""
        return templates.TemplateResponse(request, "manual.html", {})

    @app.get("/jobs/panel", response_class=HTMLResponse)
    async def jobs_panel(request: Request):
        return templates.TemplateResponse(
            request, "partials/jobs_panel.html", await jobs_context(request, compact=request.headers.get("HX-Target") == "jobs-panel")
        )

    @app.get("/rows", response_class=HTMLResponse)
    async def dashboard_rows(request: Request):
        """The collections table (outerHTML swap) + out-of-band filter counts."""
        return templates.TemplateResponse(request, "partials/rows.html", {**await dashboard_context(request), "oob": True})

    TABS = ("overview", "curate", "urls", "activity")
    TAB_ALIASES = {"patterns": "curate"}  # old links / bookmarks

    def norm_tab(tab: str | None) -> str:
        tab = TAB_ALIASES.get(tab or "", tab or "")
        return tab if tab in TABS else "overview"
    SETS = ("dump", "deltas", "curated")

    def list_params(request: Request) -> dict[str, Any]:
        qp = request.query_params
        try:
            page = max(1, int(qp.get("page", 1)))
        except ValueError:
            page = 1
        try:
            per = max(10, min(500, int(qp.get("per", 50))))
        except ValueError:
            per = 50
        return {
            "q": (qp.get("q") or "").strip() or None, "kind": qp.get("kind") or None,
            "excluded": tri_bool(qp.get("excluded")), "division": qp.get("division") or None,
            "document_type": qp.get("document_type") or None, "page": page, "per": per,
            "ai": qp.get("ai") if qp.get("ai") in ("pending", "high", "medium", "low") else None,
            "changed": "true" if qp.get("changed") == "true" else None,
        }

    async def urls_context(request: Request, c: Collection, set_: str) -> dict[str, Any]:
        d, lp = db(request), list_params(request)
        off = (lp["page"] - 1) * lp["per"]
        effects: dict[str, dict[str, str]] = {}
        has_delta: set[str] = set()
        if set_ == "dump":
            rows, total = await d.list_dump(c.collection_id, limit=lp["per"], offset=off, q=lp["q"])
        elif set_ == "curated":
            rows, total = await d.list_curated(
                c.collection_id, limit=lp["per"], offset=off, q=lp["q"], excluded=lp["excluded"]
            )
            has_delta = await d.urls_with_deltas(c.collection_id, [r.url for r in rows])
        else:
            rows, total = await d.list_deltas(
                c.collection_id, kind=lp["kind"], excluded=lp["excluded"], q=lp["q"],
                division=lp["division"], document_type=lp["document_type"], ai_pending=lp["ai"] == "pending",
                ai_conf=lp["ai"] if lp["ai"] in ("high", "medium", "low") else None,
                content_changed=True if lp["changed"] else None, limit=lp["per"], offset=off,
            )
            effects = await d.effects_for(c.collection_id, [r.url for r in rows])
        return {
            **lp, "set": set_, "rows": rows, "total": total, "pages": max(1, -(-total // lp["per"])),
            "effects": effects, "has_delta": has_delta, "divisions": list(Division),
            "doc_types": list(DocumentType), "kinds": ["new", "modified", "deleted"],
        }

    async def tab_context(request: Request, c: Collection, tab: str) -> dict[str, Any]:
        d = db(request)
        job = await d.latest_job(c.collection_id)
        ctx: dict[str, Any] = {"c": c, "job": job, "tab": tab, "statuses": list(Status)}
        if tab == "overview":
            step = request.query_params.get("step")
            sel = Status(step) if step in {s.value for s in Status} else c.status
            ctx.update(await step_context(request, c, sel)); ctx["selected"] = sel
        elif tab == "urls":
            set_ = request.query_params.get("set") or ("deltas" if c.delta_count else "curated" if c.curated_count else "dump")
            if set_ not in SETS:
                set_ = "deltas"
            ctx.update(await urls_context(request, c, set_))
        elif tab == "curate":
            ctx.update(await curate_context(request, c))
        else:
            ctx.update(history=await d.status_history(c.collection_id), jobs=await d.list_jobs(c.collection_id, 50),
                       audit=await d.list_audit(c.collection_id, 100))
        return ctx

    async def curate_context(request: Request, c: Collection) -> dict[str, Any]:
        """The guided workspace: ① exclusions (suggested exclude rules) → ② metadata (AI per URL) → ③ promote."""
        d = db(request)
        cid = c.collection_id
        suggestions = await d.list_pattern_suggestions(cid, "pending")
        ai_counts = await d.delta_ai_counts(cid)
        step = await step_context(request, c, Status.CURATING)
        return {
            "stats": step["stats"],
            "suggestions": suggestions,
            "suggestion_counts": {"total": len(suggestions), "by_type": Counter(s["type"] for s in suggestions)},
            "patterns_ever_run": await d.job_exists(cid, "llm_patterns"),
            "last_patterns_job": await d.latest_job_of_kind(cid, "llm_patterns"),
            "last_metadata_job": await d.latest_job_of_kind(cid, "llm_metadata"),
            "classifiable": await d.count_deltas_for_llm(cid),
            "classifiable_all": await d.count_deltas_for_llm(cid, only_missing=False),
            "ai_counts": ai_counts, "ai_pending_total": ai_counts["title"] + ai_counts["division"] + ai_counts["document_type"],
            "llm_model": settings.openai_model if settings.llm_provider == "openai" else settings.llm_provider,
            "llm_workers": settings.llm_workers,
            "prompts": llm_prompts(settings),
            "pattern_calls": -(-c.dump_count // settings.llm_pattern_batch_urls) if c.dump_count else 0,
            "pattern_batch": settings.llm_pattern_batch_urls,
            "global_excludes": len(load_global_excludes(settings.global_excludes_path).patterns),
            "llm_name": settings.llm_provider, "divisions": list(Division), "doc_types": list(DocumentType),
            "pattern_types": list(PatternType),
        }

    @app.get("/collections/{collection_id}/rules", response_class=HTMLResponse)
    async def collection_rules(request: Request, collection_id: str):
        """The rules (patterns) table, loaded lazily: match counting scans every dump URL."""
        c = await must_get(request, collection_id)
        job = await db(request).latest_job(collection_id)
        return templates.TemplateResponse(request, "partials/rules.html", {
            "c": c, "job": job, "patterns": await curation(request).pattern_stats(c),
        })

    async def header_context(request: Request, c: Collection) -> dict[str, Any]:
        ctx = await step_context(request, c, c.status)
        return {"c": c, "job": ctx["job"], "stats": ctx["stats"]}

    @app.get("/collections/{collection_id}", response_class=HTMLResponse)
    async def collection_page(request: Request, collection_id: str, tab: str = "overview"):
        c = await must_get(request, collection_id)
        tab = norm_tab(tab)
        ctx = await header_context(request, c)
        ctx.update(await tab_context(request, c, tab))
        ctx["selected"] = ctx.get("selected", c.status)
        return templates.TemplateResponse(request, "collection.html", ctx)

    @app.get("/collections/{collection_id}/header", response_class=HTMLResponse)
    async def collection_header(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        return templates.TemplateResponse(request, "partials/header.html", await header_context(request, c))

    @app.get("/collections/{collection_id}/tab/{tab}", response_class=HTMLResponse)
    async def collection_tab(request: Request, collection_id: str, tab: str):
        c = await must_get(request, collection_id)
        tab = norm_tab(tab)
        ctx = await tab_context(request, c, tab)
        ctx["selected"] = ctx.get("selected", c.status)
        return templates.TemplateResponse(request, f"partials/tab_{tab}.html", ctx)

    @app.get("/collections/{collection_id}/urls/{set_}")
    async def collection_urls(request: Request, collection_id: str, set_: str, format: str | None = None):
        c = await must_get(request, collection_id)
        if set_ not in SETS:
            raise HTTPException(404, "unknown set")
        if format == "csv":
            return await urls_csv(request, c, set_)
        ctx = {"c": c, "job": await db(request).latest_job(collection_id), **await urls_context(request, c, set_)}
        return templates.TemplateResponse(request, "partials/urls_table.html", ctx)

    async def urls_csv(request: Request, c: Collection, set_: str):
        import csv
        import io

        d, lp = db(request), list_params(request)
        if set_ == "dump":
            rows, _ = await d.list_dump(c.collection_id, limit=1_000_000, q=lp["q"])
            cols = ["url", "scraped_title", "content_type", "depth", "text_len", "in_curated"]
            data = [[r[k] for k in cols] for r in rows]
        elif set_ == "curated":
            rows, _ = await d.list_curated(c.collection_id, limit=1_000_000, q=lp["q"], excluded=lp["excluded"])
            cols = ["url", "excluded", "scraped_title", "title", "division", "document_type"]
            data = [[getattr(r, k) for k in cols] for r in rows]
        else:
            rows, _ = await d.list_deltas(
                c.collection_id, kind=lp["kind"], excluded=lp["excluded"], q=lp["q"],
                division=lp["division"], document_type=lp["document_type"], ai_pending=lp["ai"] == "pending",
                ai_conf=lp["ai"] if lp["ai"] in ("high", "medium", "low") else None,
                content_changed=True if lp["changed"] else None, limit=1_000_000,
            )
            cols = ["kind", "url", "excluded", "content_changed", "scraped_title", "title", "division",
                    "document_type", "title_ai", "title_ai_conf", "division_ai", "division_ai_conf",
                    "document_type_ai", "document_type_ai_conf", "ai_model"]
            data = [[getattr(r, k) for k in cols] for r in rows]
        buf = io.StringIO()
        w = csv.writer(buf); w.writerow(cols); w.writerows(data)
        return Response(
            buf.getvalue(), media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{c.collection_id}-{set_}.csv"'},
        )

    async def step_context(request: Request, c: Collection, step: Status) -> dict:
        d = db(request)
        jobs = await d.list_jobs(c.collection_id, limit=20)
        _, total = await d.list_deltas(c.collection_id, limit=1)
        counts = {"new": 0, "modified": 0, "deleted": 0, "excluded": 0, "content_changed": 0}
        if total:
            for k in ("new", "modified", "deleted"):
                counts[k] = (await d.list_deltas(c.collection_id, kind=k, limit=1))[1]
            counts["excluded"] = (await d.list_deltas(c.collection_id, excluded=True, limit=1))[1]
            counts["content_changed"] = (await d.list_deltas(c.collection_id, content_changed=True, limit=1))[1]
        curated = await d.load_curated(c.collection_id) if c.curated_count else []
        runs = await d.list_index_runs(c.collection_id, limit=5)
        failed = jobs[0] if jobs and jobs[0].state == "failed" else None
        stats = {
            "last_scrape": await d.latest_job_of_kind(c.collection_id, "scrape"),
            "failed_step": STEP_FOR_KIND.get(str(failed.kind)) if failed else None,
            "index_runs": runs,
            "last_test_run": next((r for r in runs if r.target == "test"), None),
            "last_prod_run": next((r for r in runs if r.target == "prod"), None),
            "exportable": await d.curated_export_count(c.collection_id) if c.curated_count else 0,
            "delta_counts": counts,
            "pattern_count": len(await d.list_patterns(c.collection_id)),
            "curated_excluded": sum(1 for r in curated if r.excluded),
        }
        if step in (Status.BACKLOG, Status.SCRAPED) or c.status in (Status.BACKLOG, Status.SCRAPED):
            stats["existing_crawl"] = await existing_crawl(request, c)
        await with_validation(request, c)
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
        c = Collection(**body.model_dump(), created_by=actor(request))
        await db(request).insert_collection(c)
        write_collection_yaml(settings.collections_dir, c, await db(request).status_history(c.collection_id))
        await audit(request, "collection.create", c.collection_id, f"{c.name} ← {c.seed_url}")
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
            return templates.TemplateResponse(
                request, "dashboard.html",
                {**await dashboard_context(request), **await jobs_context(request, compact=True), "error": msg, "form": data},
                status_code=422,
            )
        return RedirectResponse("/", status_code=303)

    @app.get("/api/collections/{collection_id}", response_model=Collection)
    async def api_get(request: Request, collection_id: str):
        return await must_get(request, collection_id)

    @app.delete("/api/collections/{collection_id}", response_model=None)
    async def api_delete(request: Request, collection_id: str):
        require_admin(request)
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        await audit(request, "collection.delete", collection_id, f"{c.name} ← {c.seed_url} (status {c.status})")
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
                collection_id, body.status, body.note, force=body.force, actor=actor(request)
            )
        except ValueError as e:
            raise HTTPException(409, str(e)) from e
        await audit(request, "status.set", collection_id, f"→ {body.status}" + (f": {body.note}" if body.note else ""))
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

    @app.get("/api/collections/{collection_id}/audit")
    async def api_audit(request: Request, collection_id: str, limit: int = 100):
        """Provenance ledger: who did what to this collection, newest first."""
        await must_get(request, collection_id)
        return await db(request).list_audit(collection_id, max(1, min(limit, 1000)))

    # ── jobs ───────────────────────────────────────────────────────────

    async def existing_crawl(request: Request, c: Collection) -> dict[str, Any] | None:
        """The crawl output already produced for this collection (S3 or local), cached briefly:
        the pipeline and header partials poll every few seconds. Never raises."""
        cache: dict[str, tuple[float, Any]] = request.app.state.existing_cache
        hit = cache.get(c.collection_id)
        if hit and time.monotonic() - hit[0] < 60:
            ex = hit[1]
        else:
            try:
                ex = await request.app.state.jobs.scraper.existing(c)
            except Exception as e:  # noqa: BLE001 - a broken backend must not break the page
                log.warning("existing crawl lookup failed for %s: %s", c.collection_id, e)
                ex = None
            cache[c.collection_id] = (time.monotonic(), ex)
        if ex is None:
            return None
        loaded = c.last_scraped_at is not None and ex.modified <= c.last_scraped_at
        return {"modified": ex.modified, "where": ex.where, "size": ex.size, "already_loaded": loaded}

    @app.get("/api/collections/{collection_id}/crawl/existing")
    async def api_existing_crawl(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        ex = await existing_crawl(request, c)
        return {"exists": ex is not None, **(ex or {})}

    @app.post("/api/collections/{collection_id}/scrape", status_code=202, response_model=None)
    async def api_scrape(request: Request, collection_id: str, reuse: bool = False):
        """Run the crawler; with ?reuse=true ingest the crawl output that already exists instead."""
        c = await must_get(request, collection_id)
        jobs: JobManager = request.app.state.jobs
        try:
            job = await jobs.start_scrape(c, actor=actor(request), reuse=reuse)
        except JobConflict as e:
            raise HTTPException(409, str(e)) from e
        request.app.state.existing_cache.pop(collection_id, None)
        await audit(request, "scrape.reuse" if reuse else "scrape.start", collection_id, f"job #{job.id}")
        if _is_htmx(request) and request.headers.get("HX-Target", "").startswith("row-"):
            return templates.TemplateResponse(
                request, "partials/row.html", await row_context(request, c)
            )
        return htmx_done(request, job)

    @app.post("/api/collections/{collection_id}/jobs/cancel")
    async def api_cancel_job(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        jobs: JobManager = request.app.state.jobs
        job = await jobs.cancel(c.collection_id, actor=actor(request))
        if job is None:
            raise HTTPException(409, "no running job")
        await audit(request, "job.cancel", collection_id, f"{job.kind} job #{job.id}")
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
    async def api_dump(
        request: Request, collection_id: str, limit: int = 100, offset: int = 0, q: str | None = None
    ):
        await must_get(request, collection_id)
        rows, total = await db(request).list_dump(
            collection_id, limit=max(1, min(limit, 1000)), offset=max(0, offset), q=q or None
        )
        return {"total": total, "items": rows}

    @app.get("/api/collections/{collection_id}/curated")
    async def api_curated(
        request: Request, collection_id: str, limit: int = 100, offset: int = 0,
        q: str | None = None, excluded: str | None = None,
    ):
        await must_get(request, collection_id)
        rows, total = await db(request).list_curated(
            collection_id, limit=max(1, min(limit, 1000)), offset=max(0, offset), q=q or None,
            excluded=tri_bool(excluded),
        )
        return {"total": total, "items": rows}

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
                force=True, actor=actor(request),
            )
        elif (pre and n) or (
            n and c.status in (Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE)
        ):
            c = await db(request).set_status(
                c.collection_id, Status.CURATING, note=f"deltas recomputed: {n} pending", force=True,
                actor=actor(request),
            )
        elif n == 0 and c.status is Status.CURATING and c.curated_count:
            # nothing left to review on an already-promoted set → it is curated
            c = await db(request).set_status(
                c.collection_id, Status.CURATED, note="recomputed: no pending deltas", force=True,
                actor=actor(request),
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
        await audit(request, "recompute", collection_id, f"{len(ds.deltas)} pending")
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
            p, ds = await curation(request).add_pattern(c, body, actor=actor(request))
        except Exception as e:
            if "UNIQUE" in str(e):
                raise HTTPException(409, "pattern already exists") from e
            raise
        await _after_curation_change(request, c, ds)
        await audit(request, "pattern.add", collection_id, _pattern_detail(p))
        return htmx_done(request, {"pattern": p.model_dump(mode="json"), "deltas": ds.counts})

    @app.delete("/api/collections/{collection_id}/patterns/{pattern_id}")
    async def api_delete_pattern(request: Request, collection_id: str, pattern_id: int):
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        gone = next((p for p in await db(request).list_patterns(collection_id) if p.id == pattern_id), None)
        ds = await curation(request).delete_pattern(c, pattern_id)
        if ds is None or gone is None:
            raise HTTPException(404, "pattern not found")
        await _after_curation_change(request, c, ds)
        await audit(request, "pattern.delete", collection_id, _pattern_detail(gone))
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
                old_id=existing[0].id if existing else None, actor=actor(request),
            )
        await _after_curation_change(request, c, ds)
        await audit(request, "url.edit", collection_id,
                    f"{body.type} {body.url}" + (f" → {body.value}" if body.value else ""))
        return htmx_done(request, ds.counts)

    @app.get("/api/collections/{collection_id}/deltas")
    async def api_deltas(
        request: Request, collection_id: str, kind: str | None = None,
        excluded: str | None = None, q: str | None = None, content_changed: str | None = None,
        limit: int = 100, offset: int = 0,
    ):
        await must_get(request, collection_id)
        rows, total = await db(request).list_deltas(
            collection_id, kind=kind or None, excluded=tri_bool(excluded), q=q or None,
            content_changed=tri_bool(content_changed), limit=max(1, min(limit, 1000)), offset=max(0, offset),
        )
        return {"total": total, "items": rows}

    @app.post("/api/collections/{collection_id}/stage")
    async def api_set_stage(request: Request, collection_id: str, body: StageChange):
        """Move between the curation stages (exclusions → metadata → back). Metadata is gated on every
        pattern suggestion having been decided."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if c.status is not Status.CURATING:
            raise HTTPException(409, f"stages only apply while curating (status is {c.status})")
        if body.stage is CurationStage.METADATA:
            pending = await db(request).list_pattern_suggestions(collection_id, "pending")
            if pending:
                raise HTTPException(
                    409, f"{len(pending)} pattern suggestion{'s are' if len(pending) != 1 else ' is'} pending"
                         " — accept or reject them first"
                )
        await _set_stage(request, collection_id, body.stage)
        await audit(request, "stage.set", collection_id, f"→ {body.stage}")
        return htmx_done(request, {"stage": body.stage})

    async def _set_stage(request: Request, collection_id: str, stage: CurationStage) -> None:
        await db(request).set_stage(collection_id, stage)
        c = await must_get(request, collection_id)
        write_collection_yaml(settings.collections_dir, c, await db(request).status_history(collection_id))
        emit_collection(request, c)

    @app.post("/api/collections/{collection_id}/promote")
    async def api_promote(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if c.status is not Status.CURATING:
            raise HTTPException(409, f"promote requires status 'curating' (is {c.status})")
        n = await curation(request).promote(c, actor=actor(request))
        c = await must_get(request, collection_id)
        await audit(request, "promote", collection_id, f"{n} curated")
        emit_collection(request, c)
        return htmx_done(request, {"curated": n, "status": c.status})

    @app.get("/collections/{collection_id}/curate", include_in_schema=False)
    async def curate_page(request: Request, collection_id: str):
        """Old curation page → the URLs › Deltas tab of the workbench (filters preserved)."""
        await must_get(request, collection_id)
        qs = str(request.url.query)
        return RedirectResponse(
            f"/collections/{collection_id}?tab=urls&set=deltas" + (f"&{qs}" if qs else ""), status_code=302
        )

    # ── indexing ───────────────────────────────────────────────────────

    @app.post("/api/collections/{collection_id}/index", status_code=202, response_model=None)
    async def api_index(request: Request, collection_id: str, target: Literal["test", "prod"] = "test"):
        """Export curated (non-excluded) URLs to S3 and dispatch the WEB_COSMOS indexer."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if c.status not in (Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE):
            raise HTTPException(409, f"indexing requires a promoted (curated) set — status is '{c.status}'")
        if c.delta_count:
            raise HTTPException(409, f"{c.delta_count} deltas are pending — promote them first")
        if await db(request).curated_export_count(collection_id) == 0:
            raise HTTPException(409, "nothing to export: every curated URL is excluded")
        if target == "prod":
            last = await db(request).last_index_run(collection_id, "test")
            if not last or last.state != "succeeded" or not last.validation_passes(settings.validation_title_match_threshold):
                raise HTTPException(409, "prod indexing requires a successful, validated test run first")
        jobs: JobManager = request.app.state.jobs
        try:
            job, run = await jobs.start_index(c, target, actor=actor(request))
        except (JobConflict, IndexError_) as e:
            raise HTTPException(409, str(e)) from e
        await audit(request, "index.start", collection_id, f"{target} run {run.run_id}")
        return htmx_done(request, {**job.model_dump(mode="json"), "run_id": run.run_id})

    @app.post("/api/collections/{collection_id}/index/revalidate", status_code=202, response_model=None)
    async def api_revalidate(request: Request, collection_id: str):
        """Re-check the latest test run against the index (direct query, or second pass on 403)."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        last = await db(request).last_index_run(collection_id, "test")
        if not last or last.state != "succeeded":
            raise HTTPException(409, "no successful test index run to validate")
        jobs: JobManager = request.app.state.jobs
        try:
            job = await jobs.start_revalidate(c, last, actor=actor(request))
        except (JobConflict, IndexError_) as e:
            raise HTTPException(409, str(e)) from e
        await audit(request, "revalidate", collection_id, f"test run {last.run_id}")
        return htmx_done(request, job)

    @app.get("/api/collections/{collection_id}/index_runs")
    async def api_index_runs(request: Request, collection_id: str):
        await must_get(request, collection_id)
        return await db(request).list_index_runs(collection_id)

    # ── LLM assist ─────────────────────────────────────────────────────

    async def _start_llm(request: Request, collection_id: str, action: str, starter) -> Any:
        c = await must_get(request, collection_id)
        jobs: JobManager = request.app.state.jobs
        try:
            job = await starter(jobs, c, actor(request))
        except JobConflict as e:
            raise HTTPException(409, str(e)) from e
        except LLMError as e:
            raise HTTPException(409, str(e)) from e
        await audit(request, action, collection_id, f"job #{job.id}")
        return htmx_done(request, job)

    @app.post("/api/collections/{collection_id}/suggest/patterns", status_code=202, response_model=None)
    async def api_suggest_patterns(request: Request, collection_id: str):
        """LLM drafts patterns from a sample of crawled URLs → pending suggestions (accept/reject)."""
        c = await must_get(request, collection_id)
        if c.dump_count == 0:
            raise HTTPException(409, "no crawl dump yet — scrape first")
        return await _start_llm(request, collection_id, "suggest.patterns",
                                lambda j, c, who: j.start_llm_patterns(c, actor=who))

    @app.post("/api/collections/{collection_id}/suggest/metadata", status_code=202, response_model=None)
    async def api_suggest_metadata(request: Request, collection_id: str, all: bool = False):
        """LLM suggests title/division/doc type per pending URL → *_ai fields (never the effective values)."""
        c = await must_get(request, collection_id)
        if c.delta_count == 0:
            raise HTTPException(409, "no pending changes — recompute first")
        pending = await db(request).list_pattern_suggestions(collection_id, "pending")
        if pending:  # exclusions first: excluded URLs are never classified, and titles depend on them
            raise HTTPException(
                409, f"{len(pending)} pattern suggestion{'s are' if len(pending) != 1 else ' is'} pending"
                     " — accept or reject them before suggesting metadata",
            )
        if not await db(request).count_deltas_for_llm(collection_id, only_missing=not all):
            raise HTTPException(
                409, "nothing to classify: every pending (non-excluded) URL already has suggestions"
                     " — use ?all=true to redo them",
            )
        resp = await _start_llm(request, collection_id, "suggest.metadata",
                                lambda j, c, who: j.start_llm_metadata(c, only_missing=not all, actor=who))
        if c.status is Status.CURATING and c.curation_stage is not CurationStage.METADATA:
            await _set_stage(request, collection_id, CurationStage.METADATA)
        return resp

    @app.get("/api/llm/prompts")
    async def api_llm_prompts(request: Request):
        """The exact system prompts and the shape of the user message for both LLM jobs."""
        return llm_prompts(settings)

    @app.get("/api/collections/{collection_id}/suggestions")
    async def api_suggestions(request: Request, collection_id: str, state: str | None = "pending"):
        await must_get(request, collection_id)
        return await db(request).list_pattern_suggestions(collection_id, state or None)

    async def _decide_suggestions(request: Request, c: Collection, sugs: list[dict], decision: str) -> int:
        """accept → the suggestions become real patterns (one recompute); reject → kept for the
        record, never applied. Returns how many suggestions were decided."""
        if decision == "accept":
            _, ds = await curation(request).add_patterns(
                c, [PatternCreate(type=s["type"], match=s["match"], value=s.get("value")) for s in sugs],
                actor=actor(request),
            )
            await _after_curation_change(request, c, ds)
        return await db(request).set_pattern_suggestions_state(
            c.collection_id, [s["id"] for s in sugs], "accepted" if decision == "accept" else "rejected",
            actor=actor(request),
        )

    @app.post("/api/collections/{collection_id}/suggestions/bulk")
    async def api_decide_suggestions_bulk(request: Request, collection_id: str, body: SuggestionBulk):
        """Accept or reject every pending pattern suggestion, optionally only one type."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        sugs = [s for s in await db(request).list_pattern_suggestions(collection_id, "pending")
                if body.type is None or s["type"] == body.type]
        if not sugs:
            raise HTTPException(409, f"no pending {body.type or ''} suggestions".replace("  ", " "))
        n = await _decide_suggestions(request, c, sugs, body.decision)
        await audit(request, f"suggestion.bulk_{body.decision}", collection_id, f"{body.type or 'all'} × {n}")
        return htmx_done(request, {"decided": n, "state": body.decision + "ed"})

    @app.post("/api/collections/{collection_id}/suggestions/{sid}/{decision}")
    async def api_decide_suggestion(request: Request, collection_id: str, sid: int, decision: str):
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if decision not in ("accept", "reject"):
            raise HTTPException(422, "decision must be accept or reject")
        sug = await db(request).get_pattern_suggestion(collection_id, sid)
        if sug is None:
            raise HTTPException(404, "suggestion not found")
        if sug["state"] != "pending":
            raise HTTPException(409, f"suggestion already {sug['state']}")
        await _decide_suggestions(request, c, [sug], decision)
        await audit(request, f"suggestion.{decision}", collection_id,
                    f"{sug['type']} {sug['match']}" + (f" → {sug['value']}" if sug.get("value") else ""))
        return htmx_done(request, {"id": sid, "state": decision + "ed"})

    @app.post("/api/collections/{collection_id}/ai/bulk")
    async def api_decide_ai_bulk(request: Request, collection_id: str, body: AiBulk):
        """Accept every AI suggestion for one field as exact-URL rules (one recompute), or drop them all."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        rows = await db(request).deltas_with_ai(collection_id, body.field)
        if not rows:
            raise HTTPException(409, f"no AI {body.field} suggestions to {body.decision}")
        if body.decision == "accept":
            ds = await curation(request).replace_exact_patterns(
                c, [PatternCreate(type=PatternType(body.field), match=url, value=str(v)) for url, v in rows],
                actor=actor(request),
            )
            await _after_curation_change(request, c, ds)
        await db(request).clear_delta_ai_field(collection_id, body.field)
        await audit(request, f"ai.bulk_{body.decision}", collection_id, f"{body.field} × {len(rows)}")
        return htmx_done(request, {"decided": len(rows), "field": body.field, "state": body.decision + "ed"})

    @app.post("/api/collections/{collection_id}/ai/{decision}")
    async def api_decide_ai(request: Request, collection_id: str, decision: str, body: AiDecision):
        """Per-URL AI suggestion: accept → exact-URL pattern with the suggested value; reject → clear it."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if decision not in ("accept", "reject"):
            raise HTTPException(422, "decision must be accept or reject")
        row = await db(request).get_delta(collection_id, body.url)
        if row is None:
            raise HTTPException(404, "URL not in deltas")
        value = getattr(row, f"{body.field}_ai")
        if value is None:
            raise HTTPException(409, "no suggestion for that field")
        if decision == "accept":
            existing = [p for p in await db(request).list_patterns(collection_id)
                        if p.match == body.url and p.type == body.field]
            ds = await curation(request).replace_exact_pattern(
                c, PatternCreate(type=PatternType(body.field), match=body.url, value=str(value)),
                old_id=existing[0].id if existing else None, actor=actor(request),
            )
            await _after_curation_change(request, c, ds)
        await db(request).clear_delta_ai(collection_id, body.url, body.field)
        await audit(request, f"ai.{decision}", collection_id, f"{body.field} {body.url} → {value}")
        return htmx_done(request, {"url": body.url, "field": body.field, "state": decision + "ed"})

    # ── login + accounts (only mounted when APP_PASSWORD is set) ───────

    if settings.app_password:
        # When SESSION_SECRET is unset every restart invalidates all cookies, which is fine.
        session_secret = settings.session_secret or secrets.token_hex(32)
        app.add_middleware(auth.AuthMiddleware, secret=session_secret)
        USERNAME_RE = re.compile(r"^[a-z0-9._-]{2,32}$")
        MIN_PASSWORD = 8

        def set_session_cookie(resp: Response, user: User) -> None:
            resp.set_cookie(
                auth.COOKIE,
                auth.sign(session_secret, user.id, user.session_version, int(time.time()) + settings.session_ttl_s),
                max_age=settings.session_ttl_s, httponly=True, samesite="lax",
                secure=settings.auth_cookie_secure, path="/",
            )

        @app.get("/login", response_class=HTMLResponse)
        async def login_form(request: Request, next: str = "/"):
            if auth.verify(session_secret, request.cookies.get(auth.COOKIE)):
                return RedirectResponse(auth.safe_next(next), status_code=302)
            return templates.TemplateResponse(
                request, "login.html", {"next": auth.safe_next(next), "error": None}
            )

        @app.post("/login", response_class=HTMLResponse)
        async def login_submit(
            request: Request, username: str = Form(...), password: str = Form(...), next: str = Form("/")
        ):
            u = await db(request).get_user_by_username(username.strip())
            if not (u and u.active and auth.verify_password(u.password_hash, password)):
                return templates.TemplateResponse(
                    request, "login.html",
                    {"next": auth.safe_next(next), "error": "Wrong username or password."}, status_code=401,
                )
            resp = RedirectResponse(auth.safe_next(next), status_code=303)
            set_session_cookie(resp, u)
            return resp

        @app.post("/logout")
        async def logout(request: Request):
            resp = RedirectResponse("/login", status_code=303)
            resp.delete_cookie(auth.COOKIE, path="/")
            return resp

        # ── own account ─────────────────────────────────────────────

        def account_view(request: Request, *, error: str | None = None, ok: bool = False, status_code: int = 200):
            return templates.TemplateResponse(
                request, "account.html", {"user": current_user(request), "error": error, "ok": ok},
                status_code=status_code,
            )

        @app.get("/account", response_class=HTMLResponse)
        async def account_page(request: Request, changed: int = 0):
            return account_view(request, ok=bool(changed))

        @app.post("/account/password", response_class=HTMLResponse)
        async def account_password(
            request: Request, current: str = Form(...), new: str = Form(...), confirm: str = Form(...)
        ):
            u = current_user(request)
            assert u is not None  # the middleware only lets authenticated requests through
            if not auth.verify_password(u.password_hash, current):
                return account_view(request, error="Current password is wrong.", status_code=401)
            if new != confirm:
                return account_view(request, error="New passwords do not match.", status_code=422)
            if len(new) < MIN_PASSWORD:
                return account_view(request, error=f"Use at least {MIN_PASSWORD} characters.", status_code=422)
            await db(request).set_password(u.id, auth.hash_password(new))
            await audit(request, "user.password", None, u.username)
            resp = RedirectResponse("/account?changed=1", status_code=303)
            set_session_cookie(resp, await db(request).get_user(u.id))  # new session_version
            return resp

        # ── user administration (admins) ─────────────────────────────

        async def users_view(request: Request, *, error: str | None = None, status_code: int = 200):
            return templates.TemplateResponse(
                request, "users.html",
                {"users": await db(request).list_users(), "roles": list(Role), "error": error},
                status_code=status_code,
            )

        async def target_user(request: Request, user_id: int) -> User:
            u = await db(request).get_user(user_id)
            if u is None:
                raise HTTPException(404, "user not found")
            return u

        async def guard_lockout(request: Request, u: User) -> None:
            """Disabling/demoting: never yourself, never the last active admin."""
            me = current_user(request)
            if me and me.id == u.id:
                raise HTTPException(409, "you cannot disable or demote yourself")
            if u.role is Role.ADMIN and u.active:
                admins = [x for x in await db(request).list_users() if x.role is Role.ADMIN and x.active]
                if len(admins) <= 1:
                    raise HTTPException(409, f"{u.username} is the last active admin")

        @app.get("/users", response_class=HTMLResponse)
        async def users_page(request: Request):
            require_admin(request)
            return await users_view(request)

        @app.post("/users", response_class=HTMLResponse)
        async def users_create(
            request: Request, username: str = Form(...), password: str = Form(...),
            role: Annotated[Role, Form()] = Role.CURATOR,
        ):
            require_admin(request)
            username = username.strip().lower()
            if not USERNAME_RE.match(username):
                return await users_view(request, error="Username: 2–32 characters, a-z 0-9 . _ -", status_code=422)
            if len(password) < MIN_PASSWORD:
                return await users_view(request, error=f"Password: at least {MIN_PASSWORD} characters.", status_code=422)
            try:
                await db(request).create_user(username, auth.hash_password(password), role)
            except sqlite3.IntegrityError:
                return await users_view(request, error=f"User {username!r} already exists.", status_code=409)
            await audit(request, "user.create", None, f"{username} ({role})")
            return RedirectResponse("/users", status_code=303)

        @app.post("/users/{user_id}/active")
        async def users_active(request: Request, user_id: int, active: int = Form(...)):
            require_admin(request)
            u = await target_user(request, user_id)
            if not active:
                await guard_lockout(request, u)
            await db(request).set_active(u.id, bool(active))
            await audit(request, "user.enable" if active else "user.disable", None, u.username)
            return RedirectResponse("/users", status_code=303)

        @app.post("/users/{user_id}/role")
        async def users_role(request: Request, user_id: int, role: Annotated[Role, Form()]):
            require_admin(request)
            u = await target_user(request, user_id)
            if role is not Role.ADMIN:
                await guard_lockout(request, u)
            await db(request).set_role(u.id, role)
            await audit(request, "user.role", None, f"{u.username} → {role}")
            return RedirectResponse("/users", status_code=303)

        @app.post("/users/{user_id}/password", response_class=HTMLResponse)
        async def users_password(request: Request, user_id: int, password: str = Form(...)):
            require_admin(request)
            u = await target_user(request, user_id)
            if len(password) < MIN_PASSWORD:
                return await users_view(request, error=f"Password: at least {MIN_PASSWORD} characters.", status_code=422)
            await db(request).set_password(u.id, auth.hash_password(password))
            await audit(request, "user.password", None, u.username)
            resp = RedirectResponse("/users", status_code=303)
            me = current_user(request)
            if me and me.id == u.id:  # our own session_version just changed: keep this session alive
                set_session_cookie(resp, await db(request).get_user(u.id))
            return resp

    # ── SSE ────────────────────────────────────────────────────────────

    @app.get("/events")
    async def events(request: Request):
        async def gen():
            async for msg in bus(request).subscribe():
                yield sse_format(msg)

        return EventSourceResponse(gen(), ping=15)

    return app


app = create_app()
