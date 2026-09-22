"""FastAPI application: JSON API + HTMX pages + SSE."""

from __future__ import annotations

import functools
import hashlib
import logging
import re
import secrets
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, field_validator, model_validator
from sse_starlette.sse import EventSourceResponse

from ..backends.index import IndexError_, make_index_backend
from ..backends.publish import make_prod_publisher
from ..backends.scrape import make_scrape_backend
from ..config import Settings, get_settings
from ..curation import CurationService, IncompleteMetadata
from ..db import (
    AUDIT_SORTS,
    CURATED_SORTS,
    DELTA_SORTS,
    DUMP_SORTS,
    SOURCE_LABEL,
    ConflictError,
    Database,
)
from ..events import EventBus, sse_format
from ..jobs import JobConflict, JobManager
from ..llm.base import LLMError, make_llm
from ..llm.global_excludes import load_global_excludes
from ..llm.tasks import PATTERN_SYSTEM, TITLE_SIBLINGS, TITLES_SYSTEM, metadata_system
from ..models import (
    ANONYMOUS_ACTOR,
    NOT_VISITED,
    Collection,
    CollectionCreate,
    CurationStage,
    Division,
    DivisionUpdate,
    DocumentType,
    EditedBy,
    IndexKeyUpdate,
    IndexRun,
    JobKind,
    JobRun,
    PatternCreate,
    PatternType,
    Role,
    RuleSource,
    Status,
    User,
    division_assigned,
    utcnow,
)
from ..notify import Notifier
from ..store import PatternsFile, remove_collection_files, write_collection_yaml
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
    "scrape": Status.BACKLOG, "llm_patterns": Status.CURATING, "llm_metadata": Status.CURATING, "llm_titles": Status.CURATING,
    "recompute": Status.CURATING, "bulk_accept": Status.CURATING, "bulk_suggestions": Status.CURATING,
    "index_test": Status.CONFIG_GENERATED, "validate": Status.CONFIG_GENERATED, "index_prod": Status.LIVE,
    "validate_prod": Status.LIVE,
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
                "then": f"/collections/{cid}?tab=dump", "hint": "Compute what changed vs. the curated set, then look through the dump URLs"}
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
        if c.curated_rows == 0:
            return f"cannot be '{new}': nothing has been promoted to the curated set"
        if c.delta_count and c.status is not new:
            return f"cannot be '{new}': {c.delta_count} delta URLs are waiting — promote (or discard) them first"
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


AI_FIELDS = ("title", "division", "document_type")
RULES_PAGE = 200  # per-URL rules the Rules tab shows at a time (the glob rules are always all shown)
CURATE_PREVIEW_ROWS = 50  # rows each Curate list shows in place; ⤢ Expand pages through all of them
CURATE_FOCUS = ("exclusions", "metadata")  # ?focus=: one Curate list on its own page, paginated


class AiBulk(BaseModel):
    """Decide AI metadata suggestions in bulk: every field (default) or one `field`, on every
    delta URL (default) or on one `url`, of any confidence (default) or one `conf` — the whole
    review, one column, one row, or what the review table's filter shows."""

    decision: Literal["accept", "reject"]
    field: Literal["title", "division", "document_type"] | None = None
    url: str | None = None
    conf: Literal["high", "medium", "low"] | None = None  # only suggestions of this confidence


class SuggestionAccept(BaseModel):
    """Optional body for accepting a pattern suggestion: `match` = the glob as edited by the curator."""

    match: str | None = Field(default=None, min_length=1)


class PromoteUrls(BaseModel):
    """Promote only these delta URLs (the ticked rows of one page of the Delta URLs table).
    htmx json-enc sends a single checked box as a string, not a list."""

    urls: list[str] = Field(min_length=1, max_length=5000)

    @field_validator("urls", mode="before")
    @classmethod
    def _listify(cls, v):
        return [v] if isinstance(v, str) else v


class UrlEdit(BaseModel):
    """Per-URL curator edit = an exact-URL pattern; the newest rule for a URL wins."""

    url: str = Field(min_length=1)
    type: PatternType
    value: str | None = None

    @model_validator(mode="after")
    def _check(self) -> UrlEdit:
        PatternCreate(type=self.type, match=self.url, value=self.value)  # same rules → 422
        return self


def llm_prompts(settings: Settings, c: Collection | None = None) -> dict[str, dict[str, str]]:
    """The prompts as the curator sees them under "Show the prompt". A collection whose division
    the curator set is shown the metadata prompt that does not ask for one."""
    ask_division = c is None or not division_assigned(c.division)
    return {
        "patterns": {
            "system": PATTERN_SYSTEM,
            "user": ("Batch:\n{\"collection\": …, \"seed\": …, \"global_excludes_already_applied\": [every global glob"
                     " that matched], \"batch\": \"i of K\", \"urls\": [{\"url\": …, \"title\": scraped title}, …]}"
                     f" — up to {settings.llm_pattern_batch_urls} URLs per call, no page text"),
        },
        "metadata": {
            "system": metadata_system(ask_division),
            "user": ("Document:\n{\"collection\": name, \"collection_seed\": …, \"collection_division\": only when the"
                     " curator set one (then no division is asked for), \"collection_document_type\": only when set,"
                     " \"url\": …, \"scraped_title\": …, \"text_chars\": N}\n\nText:\n"
                     f"<the FULL page text, never cut; every page goes to {settings.openai_model}>"),
        },
        "titles": {
            "system": TITLES_SYSTEM,
            "user": ("Group:\n{\"collection\": name, \"collection_seed\": …, \"shared_title\": the title they share,"
                     " \"document_type\": the type they share too, \"pages_sharing_it\": N,"
                     " \"pages_to_retitle\": how many are in this call, \"url_differs_at\": {url: [the parts of it the"
                     " other URLs do not have], …}, \"settled_titles\": [{\"url\": …, \"title\": a title already taken},"
                     f" … up to {TITLE_SIBLINGS}], \"previous_titles\": [answers that already failed]}}"
                     "\n\nPage 1 of K:\n{\"url\": …, \"scraped_title\": …, \"text_chars\": N}\nText:\n"
                     "<the FULL page text, never cut>\n\nPage 2 of K:\n…"
                     f"\n\n— one call per duplicate group, every page of it together, split at"
                     f" {settings.llm_title_group_chars:,} characters of text"),
        },
    }


def failure_label(reason: str | None) -> str:
    """The crawler's reason code as the curator reads it."""
    if not reason:
        return "never seen by the crawl"
    if reason == NOT_VISITED:
        return "not visited: the crawl stopped at its page cap"
    if reason.startswith("challenge"):
        return "bot-challenge page instead of content"
    return {
        "http_404": "HTTP 404 not found", "http_410": "HTTP 410 gone", "http_403": "HTTP 403 forbidden",
        "http_auth": "authentication required", "http_rate_limit": "rate limited (429/503)",
        "crawl_unsuccessful": "fetch failed (timeout / connection)", "empty_extract": "page had no text",
        "extract_error": "text extraction failed", "download_error": "file download failed",
        "url_timeout": "fetch timed out", "skipped_type": "file type the crawler never fetches (office/science data)",
    }.get(reason, reason.replace("_", " "))


def tri_bool(v: str | None) -> bool | None:
    """Query-string tri-state: '' / None → None, 'true'/'1'/'yes' → True, 'false'/'0'/'no' → False."""
    if v is None or v.strip() == "":
        return None
    return v.strip().lower() in ("1", "true", "yes", "on")


class AiDecision(BaseModel):
    url: str
    field: Literal["title", "division", "document_type"]
    value: str | None = None  # accept only: the value as edited by the curator (None = as suggested)


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
        db = await Database(settings.resolved_database_url, pool_size=settings.db_pool_size).connect()
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
            publisher=lambda: make_prod_publisher(settings),
        )
        app.state.jobs = jobs
        app.state.existing_cache = {}
        app.state.curation = CurationService(db, lock_for=jobs.lock)
        app.state.patterns_file = PatternsFile(db, settings.collections_dir)
        await jobs.recover()
        try:
            yield
        finally:
            await jobs.shutdown()
            await app.state.patterns_file.flush()
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

    async def run_or_job(request: Request, c: Collection, kind: JobKind, what: str, work) -> Any:
        """A bulk curation change: awaited in the request on a small collection, a background job
        on a big one (settings.bulk_job_min_urls) — there it takes longer than the 60 s a request
        has behind CloudFront, and the curator gets progress instead of an error page. `work` is the
        change itself and returns the payload; as a job the answer is 202 + the job."""
        if c.dump_count < settings.bulk_job_min_urls:
            return htmx_done(request, await work())
        try:
            job = await request.app.state.jobs.start_curation(c, kind, work, what, actor=actor(request))
        except JobConflict as e:
            raise HTTPException(409, str(e)) from e
        return JSONResponse(jsonable_encoder(job), status_code=202,
                            headers={"HX-Refresh": "true"} if _is_htmx(request) else None)

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

    CHECKING = {"test": ("index_test", "validate"), "prod": ("index_prod", "validate_prod")}

    def run_passed(run: IndexRun | None) -> bool:
        return bool(run and run.state == "succeeded" and run.validation_passes(settings.validation_title_match_threshold))

    def unvalidated(target: str, run: IndexRun | None, active: JobRun | None) -> bool:
        """Chip rule, read off the latest run of `target`. test ("needs re-indexing"): the run failed,
        never finished, or did not validate. prod ("prod not validated"): the publish succeeded but its
        check failed or never ran. Never while a job of that target (`active`: the collection's
        queued/running job, if any) is still on it."""
        if not run or run_passed(run) or (target == "prod" and run.state != "succeeded"):
            return False
        return not (active and active.state in ("queued", "running") and active.kind in CHECKING[target])

    def index_stale(c: Collection, run: IndexRun | None, active: JobRun | None) -> bool:
        """The other half of "needs re-indexing": the curated set changed after the last test index
        run started (a promote, or an exclude rule applied to curated rows in place), so what is in
        the index is behind. Never before the first test run — there is nothing to re-index — and
        never while a test index / validate job is on it, since that run carries the change."""
        if not run or c.curated_changed_at is None:
            return False
        if active and active.state in ("queued", "running") and active.kind in CHECKING["test"]:
            return False
        return c.curated_changed_at > run.started_at

    def test_chip(c: Collection, run: IndexRun | None, active: JobRun | None) -> bool:
        """The "needs re-indexing" chip: the last test run did not pass, or it is behind the curated set."""
        return unvalidated("test", run, active) or index_stale(c, run, active)

    async def with_validation(request: Request, c: Collection, job: JobRun | None = None,
                              runs: dict[str, IndexRun | None] | None = None) -> Collection:
        """`job`: the collection's latest job; `runs`: {target: latest run}, when the caller already has them."""
        if runs is None:
            runs = {t: await db(request).last_index_run(c.collection_id, t) for t in CHECKING}
        c._validated = run_passed(runs["test"])
        c._test_unvalidated = unvalidated("test", runs["test"], job)
        c._index_stale = index_stale(c, runs["test"], job)
        c._prod_unvalidated = unvalidated("prod", runs["prod"], job)
        return c

    async def row_context(request: Request, c: Collection, runs: dict[str, IndexRun | None] | None = None) -> dict:
        job = await db(request).latest_job(c.collection_id)
        return {"c": await with_validation(request, c, job, runs), "job": job}

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

    STATUS_ORDER = {s: i for i, s in enumerate(Status)}
    STAGE_ORDER = {s: i for i, s in enumerate(CurationStage)}
    DASH_SORTS: dict[str, Callable[[dict], Any]] = {  # dashboard column -> sort key of a row context
        "name": lambda r: r["c"].name.lower(),
        "status": lambda r: (STATUS_ORDER[r["c"].status], STAGE_ORDER.get(r["c"].curation_stage, -1)),  # pipeline order
        "dump": lambda r: r["c"].dump_count,
        "delta": lambda r: r["c"].delta_count,
        "curated": lambda r: r["c"].curated_count,
        "job": lambda r: (r["job"].kind, r["job"].state),  # as the cell reads: "scrape · succeeded"
        "updated": lambda r: r["c"].updated_at,
    }

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
        latest = {t: await db(request).latest_runs(t) for t in CHECKING}
        runs = {c.collection_id: {t: latest[t].get(c.collection_id) for t in CHECKING} for c in cols}
        active = {j.collection_id: j for j in await db(request).active_jobs()}
        chips = {c.collection_id: {"test": test_chip(c, runs[c.collection_id]["test"], active.get(c.collection_id)),
                                   "prod": unvalidated("prod", runs[c.collection_id]["prod"], active.get(c.collection_id))}
                 for c in cols}

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
            if "needs_reindexing" in f["flag"] and not chips[c.collection_id]["test"]:
                return False
            if "prod_not_validated" in f["flag"] and not chips[c.collection_id]["prod"]:
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
            "flag": {"needs_recuration": sum(1 for c in cols if c.needs_recuration),
                     "needs_reindexing": sum(ch["test"] for ch in chips.values()),
                     "prod_not_validated": sum(ch["prod"] for ch in chips.values())},
            "division": Counter(c.division for c in cols),
            "curator": Counter(curator_of(c) for c in cols),
        }
        curators = sorted((k for k in counts["curator"] if k != NONE_CURATOR), key=str.lower)
        if NONE_CURATOR in counts["curator"]:
            curators.append(NONE_CURATOR)
        shown = [c for c in cols if keep(c)]
        rows = [await row_context(request, c, runs[c.collection_id]) for c in shown]
        # Column sort (?sort=&dir=). Unsorted is newest first, and stays the tie-break: sorted() is
        # stable in both directions. Collections that never ran a job go last either way.
        sort = qp.get("sort") if qp.get("sort") in DASH_SORTS else None
        direction = "desc" if qp.get("dir") == "desc" else "asc"
        if sort:
            has_key = [r for r in rows if sort != "job" or r["job"]]
            rows = sorted(has_key, key=DASH_SORTS[sort], reverse=direction == "desc") + [
                r for r in rows if sort == "job" and not r["job"]
            ]
        return {
            "rows": rows, "sort": sort, "dir": direction if sort else None,
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

    async def history_context(request: Request, q: str | None, before: int | None, limit: int,
                              sort: str | None = None, dir: str | None = None, offset: int = 0) -> dict:
        """Newest first by default. Sorted by another column (?sort=actor|collection|action, ?dir=asc|desc)
        the ledger pages by offset instead of by row id."""
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        q = (q or "").strip() or None
        sort = sort if sort in AUDIT_SORTS else "at"
        desc = dir != "asc" if dir in ("asc", "desc") else True
        by_id = sort == "at"
        rows = await db(request).list_audit(None, limit + 1, q=q, before=before if by_id else None,
                                            sort=sort, desc=desc, offset=0 if by_id else offset)
        more = len(rows) > limit
        rows = rows[:limit]
        alive = {c.collection_id: c.name for c in await db(request).list_collections()}
        return {"entries": rows, "names": alive, "q": q or "", "limit": limit, "more": more,
                "sort": sort, "dir": "desc" if desc else "asc", "offset": offset,
                "next_before": rows[-1]["id"] if rows and more and by_id else None,
                "next_offset": offset + limit if more and not by_id else None}

    @app.get("/history", response_class=HTMLResponse)
    async def history_page(request: Request, q: str | None = None, before: int | None = None, limit: int = 200,
                           sort: str | None = None, dir: str | None = None, offset: int = 0):
        """The global ledger: every action by anyone on any collection (or on users), newest first —
        including actions on collections that have since been deleted. Sign-ins are not actions."""
        return templates.TemplateResponse(
            request, "history.html", await history_context(request, q, before, limit, sort, dir, offset))

    @app.get("/api/audit")
    async def api_audit_all(request: Request, q: str | None = None, before: int | None = None, limit: int = 200,
                            sort: str | None = None, dir: str | None = None, offset: int = 0):
        """The same ledger as JSON: rows keep their collection_id after the collection is deleted."""
        ctx = await history_context(request, q, before, limit, sort, dir, offset)
        return {"entries": ctx["entries"], "next_before": ctx["next_before"], "next_offset": ctx["next_offset"]}

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

    # The pipeline stepper (backlog → scraped → curating → curated → test index → live) is always at
    # the top; a collection opens at its current step. The tab row (Overview · Dump URLs · Curate ·
    # Rules · Delta URLs · Curated URLs · Activity) only exists under steps 3 Curating and 4 Curated;
    # every other step shows its panel alone. The three URL sets share one template (tab_urls.html).
    TABS = ("overview", "dump", "curate", "rules", "delta", "curated", "activity")  # tabs.html order
    TABBED_STEPS = (Status.CURATING, Status.CURATED)
    TAB_ALIASES = {"patterns": "curate"}  # old links / bookmarks

    def norm_tab(tab: str | None, set_: str | None = None) -> str:
        tab = TAB_ALIASES.get(tab or "", tab or "")
        if tab == "urls":  # old links: ?tab=dump|delta|curated
            tab = norm_set(set_) or "dump"
        return tab if tab in TABS else "overview"

    def tab_template(tab: str) -> str:
        return "partials/tab_urls.html" if tab in SETS else f"partials/tab_{tab}.html"

    def selected_step(c: Collection, step: str | None, tab: str) -> tuple[Status, str]:
        """Which pipeline step is open, and which tab. A tab other than Overview only exists under
        Curating / Curated, so a link straight to a URL set or Curate pulls the stepper there."""
        sel = Status(step) if step in {s.value for s in Status} else c.status
        if tab != "overview" and sel not in TABBED_STEPS:
            sel = Status.CURATED if _ORDER[c.status] >= _ORDER[Status.CURATED] else Status.CURATING
        return sel, tab
    # The three URL sets, named the same everywhere: dump_urls / delta_urls / curated_urls tables,
    # dump_count / delta_count / curated_count, ?set=dump|delta|curated, and the labels
    # "Dump URLs" / "Delta URLs" / "Curated URLs". ("deltas" is accepted for old links.)
    SETS = ("dump", "delta", "curated")
    SET_ALIASES = {"deltas": "delta"}
    SORTABLE = {"dump": DUMP_SORTS, "delta": DELTA_SORTS, "curated": CURATED_SORTS}  # ?sort= keys per set

    def norm_set(s: str | None) -> str | None:
        s = SET_ALIASES.get(s or "", s or "")
        return s if s in SETS else None

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
            "renamed": "true" if qp.get("renamed") == "true" else None,
            "unreachable": "true" if qp.get("unreachable") == "true" else None,
            "excluded": tri_bool(qp.get("excluded")), "division": qp.get("division") or None,
            "document_type": qp.get("document_type") or None, "page": page, "per": per,
            "ai": qp.get("ai") if qp.get("ai") in ("pending", "failed", "high", "medium", "low", *AI_FIELDS) else None,
            # ?match=<glob or exact URL>: the rows a rule matches (the Rules table links here)
            "match": (qp.get("match") or "").strip() or None,
            "changed": "true" if qp.get("changed") == "true" else None,
            # ?dup=title: rows whose title and document type another page of the collection will also have
            # ?dup=retitled: delta rows whose duplicate AI title was regenerated
            "dup": qp.get("dup") if qp.get("dup") in ("title", "retitled") else None,
            # ?missing=true: delta rows promote refuses (no title, division or document type yet)
            "missing": "true" if qp.get("missing") == "true" else None,
            "edited": qp.get("edited") if qp.get("edited") in {e.value for e in EditedBy} else None,
            # column sort: the key is validated against the set's whitelist in the db layer
            "sort": qp.get("sort") or None, "dir": "desc" if qp.get("dir") == "desc" else "asc",
        }

    async def urls_context(request: Request, c: Collection, set_: str) -> dict[str, Any]:
        d, lp = db(request), list_params(request)
        off = (lp["page"] - 1) * lp["per"]
        has_delta: dict[str, Any] = {}  # curated: url -> its pending delta row, shown in place of the promoted values
        if set_ == "dump":
            rows, total = await d.list_dump(c.collection_id, limit=lp["per"], offset=off, q=lp["q"],
                                            match=lp["match"], sort=lp["sort"], desc=lp["dir"] == "desc",
                                            excluded=lp["excluded"])
        elif set_ == "curated":
            rows, total = await d.list_curated(
                c.collection_id, limit=lp["per"], offset=off, q=lp["q"], excluded=lp["excluded"],
                edited=lp["edited"], unreachable=True if lp["unreachable"] else None, match=lp["match"],
                dup_title=lp["dup"] == "title", sort=lp["sort"], desc=lp["dir"] == "desc",
            )
            has_delta = await d.deltas_for(c.collection_id, [r.url for r in rows])
        else:
            rows, total = await d.list_deltas(
                c.collection_id, kind=lp["kind"], excluded=lp["excluded"], q=lp["q"],
                division=lp["division"], document_type=lp["document_type"], ai_pending=lp["ai"] == "pending", ai_failed=lp["ai"] == "failed",
                ai_conf=lp["ai"] if lp["ai"] in ("high", "medium", "low") else None,
                ai_field=lp["ai"] if lp["ai"] in AI_FIELDS else None,
                content_changed=True if lp["changed"] else None, edited=lp["edited"],
                renamed=True if lp["renamed"] else None, match=lp["match"], dup_title=lp["dup"] == "title",
                retitled=lp["dup"] == "retitled", incomplete=bool(lp["missing"]),
                limit=lp["per"], offset=off, sort=lp["sort"], desc=lp["dir"] == "desc",
            )
        # every row can be edited in place, so every table explains which rule set what
        urls = [r["url"] if isinstance(r, dict) else r.url for r in rows]
        effects = await d.effects_for(c.collection_id, urls)
        dup_titles = await d.duplicate_titles_for(c.collection_id, urls) if set_ != "dump" else {}
        incomplete = await d.incomplete_counts(c.collection_id) if set_ == "delta" else None
        return {
            **lp, "set": set_, "rows": rows, "total": total, "pages": max(1, -(-total // lp["per"])),
            "effects": effects, "dup_titles": dup_titles, "incomplete": incomplete, "has_delta": has_delta,
            "dup_href": f"/collections/{c.collection_id}?tab={set_}&dup=title",
            "human_fields": await d.human_set_fields(c.collection_id, urls) if set_ == "delta" else {},
            "divisions": list(Division),
            "doc_types": list(DocumentType), "kinds": ["new", "modified", "deleted"],
            "edited_values": list(EditedBy), "source_label": SOURCE_LABEL,
            "failure_label": failure_label, "sortable": SORTABLE[set_],
        }

    async def tab_context(request: Request, c: Collection, tab: str, sel: Status) -> dict[str, Any]:
        d = db(request)
        job = await d.latest_job(c.collection_id)
        ctx: dict[str, Any] = {"c": c, "job": job, "tab": tab, "statuses": list(Status),
                               "selected": sel, "tabbed": sel in TABBED_STEPS, "tab_template": tab_template(tab)}
        if tab == "overview":
            ctx.update(await step_context(request, c, sel))
        elif tab in SETS:
            ctx.update(await urls_context(request, c, tab))
        elif tab == "curate":
            ctx.update(await curate_context(request, c))
        elif tab == "rules":
            ctx.update(await rules_context(request, c))
        else:
            ctx.update(history=await d.status_history(c.collection_id), jobs=await d.list_jobs(c.collection_id, 50),
                       audit=await d.list_audit(c.collection_id, 100))
        return ctx

    def removal_warning(c: Collection, dc: dict[str, int]) -> str | None:
        """A crawl that lost a large share of the curated set is more likely a bad crawl than a
        site that shrank; say so before the curator promotes the removals."""
        gone, ratio = dc.get("deleted", 0), settings.promote_removal_warn_ratio
        if c.curated_rows and gone >= 5 and gone >= ratio * c.curated_rows:
            return (f"{gone} of {c.curated_rows} curated URLs are gone from this dump ({gone / c.curated_rows:.0%})."
                    " If the crawl was partial or failed part-way, re-scrape instead of promoting:"
                    " promoting removes them from the curated URLs and the next index run deletes them.")
        return None

    def crawl_warning(c: Collection, dc: dict[str, int]) -> str | None:
        """Curated URLs the last crawl could not vouch for are kept as they are, not removed; say
        so, and why, before the curator wonders where the removals went."""
        kept = dc.get("kept", 0)
        if not kept:
            return None
        if c.last_crawl_capped:
            return (f"The last crawl stopped at its page cap ({c.max_pages:,} pages), so {kept} curated URL"
                    f"{'s' if kept != 1 else ''} it never reached {'are' if kept != 1 else 'is'} kept unchanged"
                    " (not removed). Raise the cap and re-scrape to review them.")
        return (f"{kept} curated URL{'s' if kept != 1 else ''} could not be fetched by the last crawl"
                " (blocked, timed out, challenge page …) and are kept unchanged, not removed. The index keeps"
                " their last approved text; re-scrape later to check them again.")

    async def curate_context(request: Request, c: Collection) -> dict[str, Any]:
        """The guided workspace: ① exclusions (suggested exclude rules) → ② metadata (AI per URL) → ③ promote.
        Each list shows its first CURATE_PREVIEW_ROWS rows in place; ?focus=exclusions|metadata
        expands one of them to its own page, paginated with ?page= / ?per=."""
        d, lp = db(request), list_params(request)
        cid = c.collection_id
        focus = request.query_params.get("focus")
        focus = focus if focus in CURATE_FOCUS else None

        def paging(name: str, total: int) -> dict[str, int]:
            if focus != name:
                return {"page": 1, "pages": 1, "per": CURATE_PREVIEW_ROWS, "total": total,
                        "limit": CURATE_PREVIEW_ROWS, "offset": 0}
            pages = max(1, -(-total // lp["per"]))
            page = min(lp["page"], pages)  # deciding rows shrinks the list: stay on its last page
            return {"page": page, "pages": pages, "per": lp["per"], "total": total,
                    "limit": lp["per"], "offset": (page - 1) * lp["per"]}

        suggestion_counts = await d.pattern_suggestion_counts(cid)
        sugg_paging = paging("exclusions", suggestion_counts["total"])
        suggestions = await d.list_pattern_suggestions(cid, "pending", limit=sugg_paging["limit"],
                                                       offset=sugg_paging["offset"])
        ai_counts = await d.delta_ai_counts(cid)
        # the review table's filter: ?conf=high|medium|low and ?field=title|division|document_type;
        # ?dup=title narrows it to the rows that share a title + document type (the ⚠ badge links
        # here, so the collision is fixed without leaving the step). Those rows are listed whatever
        # their suggestions are — accepting one does not make two pages tell apart — unless a
        # confidence / field filter is on, which is a question about suggestions only.
        qp = request.query_params
        ai_dups_only = lp["dup"] == "title"
        ai_conf = None if ai_dups_only or qp.get("conf") not in ("high", "medium", "low") else qp.get("conf")
        ai_field = None if ai_dups_only or qp.get("field") not in AI_FIELDS else qp.get("field")
        ai_args = {"field": ai_field, "conf": ai_conf, "dups_only": ai_dups_only,
                   "with_dups": not (ai_conf or ai_field)}
        _, ai_total = await d.list_delta_ai(cid, limit=0, **ai_args)
        ai_paging = paging("metadata", ai_total)
        ai_rows, _ = await d.list_delta_ai(cid, limit=ai_paging["limit"], offset=ai_paging["offset"], **ai_args)
        step = await step_context(request, c, Status.CURATING)
        candidates = await d.count_deltas_for_llm(cid, only_missing=False)  # included delta URLs
        # what the accept-all buttons would really apply, and what they hold back: a field an SME
        # rule already decides is left for the row's own ✓ (see api_decide_ai_bulk)
        accept_counts = {f: await d.count_ai_suggestions(cid, field=f, skip_human=True) for f in AI_FIELDS}
        held = {f: ai_counts[f] - accept_counts[f] for f in AI_FIELDS}
        return {
            "stats": step["stats"],
            "focus": focus, "suggestions": suggestions, "suggestion_counts": suggestion_counts,
            "sugg_paging": sugg_paging, "ai_paging": ai_paging,
            "patterns_ever_run": await d.job_exists(cid, "llm_patterns"),
            "last_patterns_job": await d.latest_job_of_kind(cid, "llm_patterns"),
            "last_metadata_job": await d.latest_job_of_kind(cid, "llm_metadata"),
            "classifiable": await d.count_deltas_for_llm(cid),
            "classifiable_all": await d.count_deltas_for_llm(cid, only_missing=False),
            "ai_counts": ai_counts, "ai_pending_total": ai_counts["title"] + ai_counts["division"] + ai_counts["document_type"],
            "ai_accept_counts": accept_counts, "ai_held": held, "ai_held_total": sum(held.values()),
            "ai_accept_total": sum(accept_counts.values()),
            "ai_rows": ai_rows, "ai_conf": ai_conf, "ai_field": ai_field, "ai_dups_only": ai_dups_only,
            # the ⚠ same-title badge stays on this step instead of jumping to the Delta URLs tab
            "dup_href": f"/collections/{cid}?tab=curate{'&focus=metadata' if focus else ''}&dup=title#metadata",
            "ai_filtered": await d.count_ai_suggestions(cid, field=ai_field, conf=ai_conf) if (ai_conf or ai_field) else 0,
            "ai_filtered_accept": await d.count_ai_suggestions(cid, field=ai_field, conf=ai_conf, skip_human=True)
                                  if (ai_conf or ai_field) else 0,
            "incomplete": await d.incomplete_counts(cid),
            "effects": await d.effects_for(cid, [r.url for r in ai_rows]),
            "human_fields": await d.human_set_fields(cid, [r.url for r in ai_rows]),
            "dup_counts": await d.duplicate_title_counts(cid),
            "dup_titles": await d.duplicate_titles_for(cid, [r.url for r in ai_rows]),
            "last_titles_job": await d.latest_job_of_kind(cid, "llm_titles"),
            "dedupe_titles": settings.llm_dedupe_titles,
            "llm_model": settings.openai_model if settings.llm_provider == "openai" else settings.llm_provider,
            "llm_workers": settings.llm_workers,
            "prompts": llm_prompts(settings, c),
            "pattern_candidates": candidates,
            "pattern_calls": -(-candidates // settings.llm_pattern_batch_urls) if candidates else 0,
            "pattern_batch": settings.llm_pattern_batch_urls,
            "removal_warning": removal_warning(c, step["stats"]["delta_counts"]),
            "crawl_warning": crawl_warning(c, step["stats"]["delta_counts"]),
            "global_excludes": len(load_global_excludes(settings.global_excludes_path).patterns),
            "llm_name": settings.llm_provider, "divisions": list(Division), "doc_types": list(DocumentType),
            "pattern_types": list(PatternType),
        }

    async def rules_context(request: Request, c: Collection) -> dict[str, Any]:
        """The rules (patterns) table: every rule with its match count over the set the count
        links to (CurationService.rows_set) and how many URLs it still decides."""
        n = await db(request).pattern_counts(c.collection_id)
        try:
            page = max(1, int(request.query_params.get("rpage", 1)))
        except ValueError:
            page = 1
        pages = max(1, -(-n["exact"] // RULES_PAGE))
        page = min(page, pages)
        patterns = await curation(request).pattern_stats(c, exact_limit=RULES_PAGE, exact_offset=(page - 1) * RULES_PAGE)
        return {
            "patterns": patterns, "source_label": SOURCE_LABEL, "source_counts": n["by_source"],
            "rules_paging": {"page": page, "pages": pages, "per": RULES_PAGE, "total": n["exact"]},
            "rows_set": CurationService.rows_set(c), "divisions": list(Division), "doc_types": list(DocumentType),
        }

    @app.get("/collections/{collection_id}/rules", response_class=HTMLResponse)
    async def collection_rules(request: Request, collection_id: str):
        """The rules table alone (htmx fragment)."""
        c = await must_get(request, collection_id)
        job = await db(request).latest_job(collection_id)
        return templates.TemplateResponse(request, "partials/rules.html",
                                          {"c": c, "job": job, **await rules_context(request, c)})

    async def header_context(request: Request, c: Collection) -> dict[str, Any]:
        ctx = await step_context(request, c, c.status)
        return {"c": c, "job": ctx["job"], "stats": ctx["stats"]}

    @app.get("/collections/{collection_id}", response_class=HTMLResponse)
    async def collection_page(request: Request, collection_id: str, tab: str = "overview", set: str | None = None,
                              step: str | None = None):
        c = await must_get(request, collection_id)
        sel, tab = selected_step(c, step, norm_tab(tab, set))
        ctx = await header_context(request, c)
        ctx.update(await tab_context(request, c, tab, sel))
        return templates.TemplateResponse(request, "collection.html", ctx)

    @app.get("/collections/{collection_id}/header", response_class=HTMLResponse)
    async def collection_header(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        return templates.TemplateResponse(request, "partials/header.html", await header_context(request, c))

    @app.get("/collections/{collection_id}/tab/{tab}", response_class=HTMLResponse)
    async def collection_tab(request: Request, collection_id: str, tab: str):
        c = await must_get(request, collection_id)
        sel, tab = selected_step(c, request.query_params.get("step"), norm_tab(tab, request.query_params.get("set")))
        ctx = await tab_context(request, c, tab, sel)
        return templates.TemplateResponse(request, tab_template(tab), ctx)

    @app.get("/collections/{collection_id}/urls/{set_}")
    async def collection_urls(request: Request, collection_id: str, set_: str, format: str | None = None):
        c = await must_get(request, collection_id)
        set_ = norm_set(set_)  # type: ignore[assignment]
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
            rows, _ = await d.list_dump(c.collection_id, limit=1_000_000, q=lp["q"], match=lp["match"],
                                        sort=lp["sort"], desc=lp["dir"] == "desc", excluded=lp["excluded"])
            cols = ["url", "excluded", "scraped_title", "content_type", "depth", "text_len", "in_curated"]
            data = [[r[k] for k in cols] for r in rows]
        elif set_ == "curated":
            rows, _ = await d.list_curated(c.collection_id, limit=1_000_000, q=lp["q"], excluded=lp["excluded"],
                                           edited=lp["edited"], unreachable=True if lp["unreachable"] else None,
                                           match=lp["match"], dup_title=lp["dup"] == "title",
                                           sort=lp["sort"], desc=lp["dir"] == "desc")
            cols = ["url", "excluded", "scraped_title", "title", "division", "document_type", "text_len", "edited_by",
                    "crawl_failure"]
            data = [[getattr(r, k) for k in cols] for r in rows]
        else:
            rows, _ = await d.list_deltas(
                c.collection_id, kind=lp["kind"], excluded=lp["excluded"], q=lp["q"],
                division=lp["division"], document_type=lp["document_type"], ai_pending=lp["ai"] == "pending", ai_failed=lp["ai"] == "failed",
                ai_conf=lp["ai"] if lp["ai"] in ("high", "medium", "low") else None,
                ai_field=lp["ai"] if lp["ai"] in AI_FIELDS else None,
                content_changed=True if lp["changed"] else None, edited=lp["edited"],
                renamed=True if lp["renamed"] else None, match=lp["match"], dup_title=lp["dup"] == "title",
                retitled=lp["dup"] == "retitled", incomplete=bool(lp["missing"]),
                limit=1_000_000, sort=lp["sort"], desc=lp["dir"] == "desc",
            )
            cols = ["kind", "url", "excluded", "content_changed", "edited_by", "scraped_title", "title", "division",
                    "document_type", "title_ai", "title_ai_conf", "division_ai", "division_ai_conf",
                    "document_type_ai", "document_type_ai_conf", "ai_model", "ai_error", "renamed_from", "crawl_failure",
                    "title_ai_before"]
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
        counts = {"new": 0, "modified": 0, "deleted": 0, "excluded": 0, "content_changed": 0, "renamed": 0,
                  "kept": 0}
        if total:
            for k in ("new", "modified", "deleted"):
                counts[k] = (await d.list_deltas(c.collection_id, kind=k, limit=1))[1]
            counts["content_changed"] = (await d.list_deltas(c.collection_id, content_changed=True, limit=1))[1]
            counts["renamed"] = (await d.list_deltas(c.collection_id, renamed=True, limit=1))[1]
        counts["excluded"] = await d.count_excluded_by_rules(c.collection_id)  # rules, not deltas
        counts["kept"] = await d.count_curated_unreachable(c.collection_id) if c.curated_rows else 0
        runs = await d.list_index_runs(c.collection_id, limit=5)
        failed = jobs[0] if jobs and jobs[0].state == "failed" else None
        # while an index/validate job is still going, the run's stored report is provisional (the
        # indexer's pre-refresh validation.json, or the previous check) — show "validating", not fail
        active = [j for j in jobs if j.state in ("queued", "running")]
        stats = {
            "last_scrape": await d.latest_job_of_kind(c.collection_id, "scrape"),
            "failed_step": STEP_FOR_KIND.get(str(failed.kind)) if failed else None,
            "index_runs": runs,
            "last_test_run": next((r for r in runs if r.target == "test"), None),
            "last_prod_run": next((r for r in runs if r.target == "prod"), None),
            "validating_test": next((j for j in active if j.kind in ("index_test", "validate")), None),
            "validating_prod": next((j for j in active if j.kind in ("index_prod", "validate_prod")), None),
            "exportable": await d.curated_export_count(c.collection_id) if c.curated_rows else 0,
            "delta_counts": counts,
            "pattern_count": await d.count_patterns(c.collection_id),
            "curated_excluded": await d.count_curated_excluded(c.collection_id) if c.curated_count else 0,
        }
        if step in (Status.BACKLOG, Status.SCRAPED) or c.status in (Status.BACKLOG, Status.SCRAPED):
            stats["existing_crawl"] = await existing_crawl(request, c)
        await with_validation(request, c, jobs[0] if jobs else None)
        return {"c": c, "job": jobs[0] if jobs else None, "step": step,
                "steps": pipeline_steps(c), "stats": stats, "divisions": list(Division)}

    @app.get("/collections/{collection_id}/pipeline", response_class=HTMLResponse)
    async def collection_pipeline(request: Request, collection_id: str, step: str | None = None):
        """The stepper alone (refreshed on SSE / polling); `step` keeps the curator's selection lit."""
        c = await must_get(request, collection_id)
        ctx = await step_context(request, c, c.status)
        ctx["selected"] = Status(step) if step in {s.value for s in Status} else c.status
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
            request, "partials/row.html", await row_context(request, c)
        )

    # ── API ────────────────────────────────────────────────────────────

    @app.get("/api/collections", response_model=list[Collection])
    async def api_list(request: Request):
        return await db(request).list_collections()

    @app.post("/api/collections", response_model=Collection, status_code=201)
    async def api_create(request: Request, body: CollectionCreate):
        if existing := await db(request).get_collection(body.collection_id):
            raise HTTPException(
                409,
                f"collection {body.collection_id!r} already exists: {existing.name} — {existing.seed_url}. "
                "That seed is already curated; re-scrape it there, or delete that collection first.",
            )
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
        """Admin only, and permanent: the collection, its crawl, rules, curated URLs, runs and history
        go (ON DELETE CASCADE), along with its YAML files. The audit row naming it stays — the ledger
        keeps rows of deleted collections — and nothing is removed from S3 or any index."""
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
        # loadable: a finished crawl the collection has not ingested yet. A checkpoint of a crawl
        # still running on the host is shown but never offered (it would ingest a truncated dump).
        return {"modified": ex.modified, "where": ex.where, "size": ex.size, "already_loaded": loaded,
                "complete": ex.complete, "loadable": ex.complete and not loaded}

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
        request: Request, collection_id: str, limit: int = 100, offset: int = 0, q: str | None = None,
        excluded: str | None = None,
    ):
        await must_get(request, collection_id)
        rows, total = await db(request).list_dump(
            collection_id, limit=max(1, min(limit, 1000)), offset=max(0, offset), q=q or None,
            excluded=tri_bool(excluded),
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

    async def _after_curation_change(request: Request, c: Collection, ds, *, note: str | None = None) -> Collection:
        """A diff/pattern change that produces delta URLs puts the collection in
        'curating'. A recompute with nothing to review never demotes a curated/live
        collection (otherwise it would be stuck: nothing to promote, no way forward).
        `note`: what happened, for the status history (default: a recompute)."""
        n = len(ds.deltas)
        pre = c.status in (Status.BACKLOG, Status.SCRAPED)
        if pre and n == 0 and c.curated_rows:
            # re-crawl identical to the curated set: nothing to review
            await db(request).set_flag(c.collection_id, False)
            c = await db(request).set_status(
                c.collection_id, Status.CURATED, note="re-crawl matches the curated URLs: no changes",
                force=True, actor=actor(request),
            )
        elif (pre and n) or (
            n and c.status in (Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE)
        ):
            c = await db(request).set_status(
                c.collection_id, Status.CURATING, note=f"delta URLs recomputed: {n}", force=True,
                actor=actor(request),
            )
        elif getattr(ds, "curated_excluded", None) and c.status in (Status.CONFIG_GENERATED, Status.LIVE):
            # an exclude rule took curated URLs out in place (no delta): the index is behind again
            k = len(ds.curated_excluded)
            c = await db(request).set_status(
                c.collection_id, Status.CURATED, force=True, actor=actor(request),
                note=f"{k} curated URL{'s' if k != 1 else ''} excluded by rules: re-index to apply",
            )
        elif n == 0 and c.status is Status.CURATING and c.curated_rows:
            # nothing left to review on an already-promoted set → it is curated
            c = await db(request).set_status(
                c.collection_id, Status.CURATED, note=note or "recomputed: no delta URLs", force=True,
                actor=actor(request),
            )
        c = await must_get(request, c.collection_id)
        await request.app.state.patterns_file.changed(c.collection_id)
        emit_collection(request, c)
        return c

    @app.post("/api/collections/{collection_id}/recompute")
    async def api_recompute(request: Request, collection_id: str):
        """Calculate deltas (dump vs curated) and apply all patterns. Idempotent."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if c.dump_count == 0:
            raise HTTPException(409, "no dump ingested yet — scrape first")
        async def work() -> dict:
            ds = await curation(request).recompute(c)
            await _after_curation_change(request, c, ds)
            await audit(request, "recompute", collection_id, f"{len(ds.deltas)} delta URLs")
            return ds.counts

        return await run_or_job(request, c, JobKind.RECOMPUTE, f"comparing {c.dump_count:,} dump URLs with the curated URLs", work)

    @app.get("/api/collections/{collection_id}/patterns")
    async def api_patterns(request: Request, collection_id: str, exact_limit: int | None = Query(None, ge=0, le=5000),
                           exact_offset: int = Query(0, ge=0)):
        """Every rule with its match / in-effect counts. `exact_limit` + `exact_offset` page the
        per-URL (exact) rules — there can be three per URL; the glob rules always come in full."""
        c = await must_get(request, collection_id)
        return await curation(request).pattern_stats(c, exact_limit=exact_limit, exact_offset=exact_offset)

    @app.post("/api/collections/{collection_id}/patterns", status_code=201)
    async def api_add_pattern(request: Request, collection_id: str, body: PatternCreate):
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        try:
            p, ds = await curation(request).add_pattern(c, body, actor=actor(request))
        except ConflictError as e:
            raise HTTPException(409, "pattern already exists") from e
        await _after_curation_change(request, c, ds)
        await audit(request, "pattern.add", collection_id, _pattern_detail(p))
        return htmx_done(request, {"pattern": p.model_dump(mode="json"), "deltas": ds.counts})

    @app.delete("/api/collections/{collection_id}/patterns/{pattern_id}")
    async def api_delete_pattern(request: Request, collection_id: str, pattern_id: int):
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        gone = await db(request).get_pattern(collection_id, pattern_id)
        ds = await curation(request).delete_pattern(c, pattern_id)
        if ds is None or gone is None:
            raise HTTPException(404, "pattern not found")
        await _after_curation_change(request, c, ds)
        await audit(request, "pattern.delete", collection_id, _pattern_detail(gone))
        return htmx_done(request, ds.counts)

    @app.post("/api/collections/{collection_id}/urls")
    async def api_url_edit(request: Request, collection_id: str, body: UrlEdit):
        """Curator edit on one URL: exact-match pattern. `exclude` / `include` is the state wanted
        for the row (idempotent — see CurationService.set_excluded), a field type carries the value."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if body.type in (PatternType.EXCLUDE, PatternType.INCLUDE):
            ds = await curation(request).set_excluded(c, body.url, body.type is PatternType.EXCLUDE, actor=actor(request))
            await _after_curation_change(request, c, ds)
            await audit(request, "url.edit", collection_id, f"{body.type} {body.url}")
            return htmx_done(request, ds.counts)
        # an exact rule matches every spelling of its page, so find it under any spelling
        existing = await db(request).exact_patterns_for(collection_id, body.url, str(body.type))
        if existing and existing[0].value == body.value:
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

    @app.get("/api/collections/{collection_id}/delta")
    @app.get("/api/collections/{collection_id}/deltas", include_in_schema=False)  # old spelling
    async def api_deltas(
        request: Request, collection_id: str, kind: str | None = None,
        excluded: str | None = None, q: str | None = None, content_changed: str | None = None,
        renamed: str | None = None, limit: int = 100, offset: int = 0,
    ):
        await must_get(request, collection_id)
        rows, total = await db(request).list_deltas(
            collection_id, kind=kind or None, excluded=tri_bool(excluded), q=q or None,
            content_changed=tri_bool(content_changed), renamed=tri_bool(renamed),
            limit=max(1, min(limit, 1000)), offset=max(0, offset),
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
            pending = (await db(request).pattern_suggestion_counts(collection_id))["total"]
            if pending:
                raise HTTPException(
                    409, f"{pending} pattern suggestion{'s are' if pending != 1 else ' is'} pending"
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
        try:
            n = await curation(request).promote(c, actor=actor(request))
        except IncompleteMetadata as e:
            raise HTTPException(409, str(e)) from e
        c = await must_get(request, collection_id)
        await request.app.state.patterns_file.flush(collection_id)  # the approved set's rules are on disk
        await audit(request, "promote", collection_id, f"{n} curated")
        emit_collection(request, c)
        return htmx_done(request, {"curated": n, "status": c.status})

    @app.post("/api/collections/{collection_id}/promote/urls")
    async def api_promote_urls(request: Request, collection_id: str, body: PromoteUrls):
        """Promote the ticked delta URLs only. The rest of the queue stays and the collection stays
        'curating' until nothing is left to review. An AI suggestion still pending on a promoted
        row goes with it (it lives on the delta row) — the table warns before sending."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if c.status is not Status.CURATING:
            raise HTTPException(409, f"promote requires status 'curating' (is {c.status})")
        found = await db(request).urls_with_deltas(collection_id, body.urls)
        stale = [u for u in body.urls if u not in found]
        if stale:
            raise HTTPException(409, f"{len(stale)} of the selected URLs are no longer delta URLs"
                                     f" (e.g. {stale[0]}) — reload the page and pick again")
        try:
            n, ds = await curation(request).promote_urls(c, body.urls)
        except IncompleteMetadata as e:
            raise HTTPException(409, str(e)) from e
        c = await must_get(request, collection_id)  # fresh counts for the status rules
        await _after_curation_change(request, c, ds, note=f"promoted {len(body.urls)} selected delta URLs")
        c = await must_get(request, collection_id)
        await audit(request, "promote.urls", collection_id, f"{len(body.urls)} → {n} curated, {len(ds.deltas)} left")
        return htmx_done(request, {"curated": n, "promoted": len(body.urls), "left": len(ds.deltas),
                                   "status": c.status})

    @app.get("/collections/{collection_id}/curate", include_in_schema=False)
    async def curate_page(request: Request, collection_id: str):
        """Old curation page → the Delta URLs tab of the workbench (filters preserved)."""
        await must_get(request, collection_id)
        qs = str(request.url.query)
        return RedirectResponse(
            f"/collections/{collection_id}?tab=delta" + (f"&{qs}" if qs else ""), status_code=302
        )

    # ── indexing ───────────────────────────────────────────────────────

    @app.post("/api/collections/{collection_id}/index", status_code=202, response_model=None)
    async def api_index(request: Request, collection_id: str, target: Literal["test", "prod"] = "test"):
        """test: export curated (non-excluded) URLs to S3 and dispatch the WEB_COSMOS indexer.
        prod: publish the latest validated test run's vectors to the production index."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if c.status not in (Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE):
            raise HTTPException(409, f"indexing requires a promoted (curated) set — status is '{c.status}'")
        if c.delta_count:
            raise HTTPException(409, f"{c.delta_count} delta URLs are waiting — promote them first")
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
    async def api_revalidate(request: Request, collection_id: str, target: Literal["test", "prod"] = "test"):
        """Re-check the latest run of `target` against its index (direct query; test falls back to a
        second indexer pass on 403, prod has no fallback and fails)."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        last = await db(request).last_index_run(collection_id, target)
        if not last or last.state != "succeeded":
            raise HTTPException(409, f"no successful {target} index run to validate")
        jobs: JobManager = request.app.state.jobs
        try:
            job = await jobs.start_revalidate(c, last, actor=actor(request))
        except (JobConflict, IndexError_) as e:
            raise HTTPException(409, str(e)) from e
        await audit(request, "revalidate", collection_id, f"{target} run {last.run_id}")
        return htmx_done(request, job)

    @app.post("/api/collections/{collection_id}/index-key", response_model=None)
    async def api_set_index_key(request: Request, collection_id: str, body: IndexKeyUpdate):
        """Say by hand which OpenSearch collection this one is indexed as, for a collection whose
        folder does not follow from its current name. The next test run exports under it."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        name = (body.index_name or "").strip() or (c.index_name if body.index_key == c.index_key else None) or c.name
        await db(request).set_index_key(collection_id, body.index_key, name)
        await audit(request, "index.key", collection_id, f"set by hand: '{body.index_key}' ({name})")
        c = await must_get(request, collection_id)
        emit_collection(request, c)
        return htmx_done(request, c)

    @app.post("/api/collections/{collection_id}/division", response_model=None)
    async def api_set_division(request: Request, collection_id: str, body: DivisionUpdate):
        """Change the collection's division after it was created, to any of the five. It is then the
        curator's decision for the whole collection: every URL that no division rule decides takes it
        (a recompute applies it right away, so rows already curated become modified deltas and reach
        the index on the next promote), and Suggest metadata stops asking the model for one. Putting
        it back to General means "not assigned": the model is asked for a division per page again."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if body.division == c.division:
            return htmx_done(request, c)
        old = c.division
        await db(request).set_division(collection_id, body.division)
        if division_assigned(body.division):
            # a division suggestion left by a run from before the division was assigned: the division
            # is the curator's now, so there is nothing to review and nothing accept-all could apply
            await db(request).clear_delta_ai_field(collection_id, "division")
        c = await must_get(request, collection_id)
        if c.delta_count or c.curated_rows:  # apply it to the URLs the collection already has
            ds = await curation(request).recompute(c)
            await _after_curation_change(request, c, ds)
            c = await must_get(request, collection_id)
        write_collection_yaml(settings.collections_dir, c, await db(request).status_history(collection_id))
        await audit(request, "collection.division", collection_id, f"{old} → {body.division}")
        emit_collection(request, c)
        return htmx_done(request, c)

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
        if c.delta_count == 0:
            raise HTTPException(409, "no delta URLs — Start curating first, then suggest exclusions for the delta URLs")
        return await _start_llm(request, collection_id, "suggest.patterns",
                                lambda j, c, who: j.start_llm_patterns(c, actor=who))

    @app.post("/api/collections/{collection_id}/suggest/metadata", status_code=202, response_model=None)
    async def api_suggest_metadata(request: Request, collection_id: str, all: bool = False):
        """LLM suggests title/division/doc type per delta URL → *_ai fields (never the effective values)."""
        c = await must_get(request, collection_id)
        if c.delta_count == 0:
            raise HTTPException(409, "no delta URLs — Start curating (recompute) first")
        pending = await db(request).list_pattern_suggestions(collection_id, "pending")
        if pending:  # exclusions first: excluded URLs are never classified, and titles depend on them
            raise HTTPException(
                409, f"{len(pending)} pattern suggestion{'s are' if len(pending) != 1 else ' is'} pending"
                     " — accept or reject them before suggesting metadata",
            )
        if not await db(request).count_deltas_for_llm(collection_id, only_missing=not all):
            raise HTTPException(
                409, "nothing to classify: every included delta URL already has suggestions"
                     " — use ?all=true to redo them",
            )
        resp = await _start_llm(request, collection_id, "suggest.metadata",
                                lambda j, c, who: j.start_llm_metadata(c, only_missing=not all, actor=who))
        if c.status is Status.CURATING and c.curation_stage is not CurationStage.METADATA:
            await _set_stage(request, collection_id, CurationStage.METADATA)
        return resp

    @app.post("/api/collections/{collection_id}/suggest/titles", status_code=202, response_model=None)
    async def api_suggest_titles(request: Request, collection_id: str):
        """Regenerate duplicate titles: every delta URL whose title and document type another page of the collection
        will also have goes back to the LLM for a title (only) that sets it apart → title_ai suggestions
        (never applied)."""
        await must_get(request, collection_id)
        if not (await db(request).duplicate_title_counts(collection_id))["delta_urls"]:
            raise HTTPException(409, "no delta URL shares its title and document type with another page")
        return await _start_llm(request, collection_id, "suggest.titles",
                                lambda j, c, who: j.start_llm_titles(c, actor=who))

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
                c, [(PatternCreate(type=s["type"], match=s["match"], value=s.get("value")),
                     RuleSource.GLOBAL if s.get("source") == "global" else RuleSource.LLM) for s in sugs],
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
        async def work() -> dict:
            n = await _decide_suggestions(request, c, sugs, body.decision)
            await audit(request, f"suggestion.bulk_{body.decision}", collection_id, f"{body.type or 'all'} × {n}")
            return {"decided": n, "state": body.decision + "ed"}

        if body.decision != "accept":  # a reject applies nothing: no recompute, nothing long
            return htmx_done(request, await work())
        return await run_or_job(request, c, JobKind.BULK_SUGGESTIONS, f"applying {len(sugs)} suggested rules", work)

    @app.post("/api/collections/{collection_id}/suggestions/{sid}/{decision}")
    async def api_decide_suggestion(
        request: Request, collection_id: str, sid: int, decision: str, body: SuggestionAccept | None = None
    ):
        """Accept / reject one pattern suggestion. Accept may carry `{"match": …}`: the glob as the
        curator edited it — the rule is created with that glob (source "AI, edited") and the
        suggestion records what was actually applied."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if decision not in ("accept", "reject"):
            raise HTTPException(422, "decision must be accept or reject")
        sug = await db(request).get_pattern_suggestion(collection_id, sid)
        if sug is None:
            raise HTTPException(404, "suggestion not found")
        if sug["state"] != "pending":
            raise HTTPException(409, f"suggestion already {sug['state']}")
        if body and body.match is not None and not body.match.strip():
            raise HTTPException(422, "the match cannot be blank")
        edited = body.match.strip() if body and body.match and body.match.strip() != sug["match"] else None
        if decision == "accept" and edited:
            try:
                pc = PatternCreate(type=sug["type"], match=edited, value=sug.get("value"))
            except ValueError as e:
                raise HTTPException(422, str(e)) from e
            try:
                _, ds = await curation(request).add_pattern(c, pc, actor=actor(request), source=RuleSource.LLM_EDITED)
            except ConflictError as e:
                raise HTTPException(409, f"a {sug['type']} rule for {edited} already exists") from e
            await _after_curation_change(request, c, ds)
            await db(request).set_pattern_suggestion_state(collection_id, sid, "accepted", actor=actor(request),
                                                           accepted_as=edited)
            await audit(request, "suggestion.accept_edited", collection_id,
                        f"{sug['type']} {sug['match']} → edited to {edited}")
            return htmx_done(request, {"id": sid, "state": "accepted", "accepted_as": edited})
        await _decide_suggestions(request, c, [sug], decision)
        await audit(request, f"suggestion.{decision}", collection_id,
                    f"{sug['type']} {sug['match']}" + (f" → {sug['value']}" if sug.get("value") else ""))
        return htmx_done(request, {"id": sid, "state": decision + "ed"})

    @app.post("/api/collections/{collection_id}/ai/bulk")
    async def api_decide_ai_bulk(request: Request, collection_id: str, body: AiBulk):
        """Accept AI suggestions as exact-URL rules (one recompute), or drop them: every field or
        one `field`, every delta URL or one `url`.

        An accept over many rows (no `url`) passes over the fields an SME rule already decides:
        accepting would write a newer exact-URL rule and silently undo a rule a person added after
        the metadata was generated. Those suggestions stay in the review table, where the row's own
        ✓ accepts them — that button is never disabled, so the curator can still take the AI's
        answer for a row they had ruled on. A reject decides exactly what it says, held back or not."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        d = db(request)
        fields = [body.field] if body.field else list(AI_FIELDS)
        kind = (f"{body.conf}-confidence " if body.conf else "") + (f"AI {body.field} suggestions" if body.field else "AI suggestions")
        whole = body.url is None and body.decision == "accept"  # every URL's suggestions → rules: the long one
        if whole and not await d.count_ai_suggestions(collection_id, field=body.field, conf=body.conf):
            raise HTTPException(409, f"no {kind} to {body.decision}")

        async def work() -> dict:
            return await _decide_ai_bulk(request, c, body, fields, kind)

        if not whole:
            return htmx_done(request, await work())
        return await run_or_job(request, c, JobKind.BULK_ACCEPT, f"accepting the {kind}", work)

    async def _decide_ai_bulk(request: Request, c: Collection, body: AiBulk, fields: list[str], kind: str) -> dict:
        d, collection_id = db(request), c.collection_id
        # accept-all (no url) skips what an SME rule decides; a named row, and any reject, does not
        skip_human = body.decision == "accept" and body.url is None
        per_field = {f: await d.deltas_with_ai(collection_id, f, url=body.url, conf=body.conf,
                                               skip_human=skip_human) for f in fields}
        n = sum(len(v) for v in per_field.values())
        if not n:
            held = skip_human and await d.count_ai_suggestions(collection_id, field=body.field, conf=body.conf)
            raise HTTPException(409, f"no {kind} to {body.decision}" + (f" on {body.url}" if body.url else "")
                                + (f": all {held} left are on fields your own rules decide — accept those row by row"
                                   if held else ""))
        if body.decision == "accept":
            ds = await curation(request).replace_exact_patterns(
                c, [PatternCreate(type=PatternType(f), match=url, value=str(v))
                    for f, rows in per_field.items() for url, v in rows],
                actor=actor(request), source=RuleSource.LLM,
            )
            await _after_curation_change(request, c, ds)
        for f, rows in per_field.items():
            if rows:  # only the rows just decided: the ones passed over keep their suggestion
                await d.clear_delta_ai_field(collection_id, f, url=body.url, conf=body.conf,
                                             urls=[u for u, _ in rows] if skip_human else None)
        detail = " · ".join(f"{f} × {len(rows)}" for f, rows in per_field.items() if rows)
        await audit(request, f"ai.bulk_{body.decision}", collection_id, detail + (f" ({body.url})" if body.url else "")
                    + (f" [{body.conf} confidence]" if body.conf else ""))
        return {"decided": n, "field": body.field, "url": body.url, "state": body.decision + "ed"}

    @app.post("/api/collections/{collection_id}/ai/{decision}")
    async def api_decide_ai(request: Request, collection_id: str, decision: str, body: AiDecision):
        """Per-URL AI suggestion: accept → exact-URL pattern with the suggested value; reject → clear it."""
        c = await must_get(request, collection_id)
        ensure_idle(request, c)
        if decision not in ("accept", "reject"):
            raise HTTPException(422, "decision must be accept or reject")
        row = await db(request).get_delta(collection_id, body.url)
        if row is None:
            raise HTTPException(404, "URL not in the delta URLs")
        value = getattr(row, f"{body.field}_ai")
        if value is None:
            raise HTTPException(409, "no suggestion for that field")
        edited = body.value.strip() if decision == "accept" and body.value and body.value.strip() != str(value) else None
        if decision == "accept":
            try:
                pc = PatternCreate(type=PatternType(body.field), match=body.url, value=edited or str(value))
            except ValueError as e:
                raise HTTPException(422, str(e)) from e
            existing = [p for p in await db(request).exact_patterns_for(collection_id, body.url, body.field)
                        if p.match == body.url]
            ds = await curation(request).replace_exact_pattern(
                c, pc, old_id=existing[0].id if existing else None, actor=actor(request),
                source=RuleSource.LLM_EDITED if edited else RuleSource.LLM,
            )
            await _after_curation_change(request, c, ds)
        await db(request).clear_delta_ai(collection_id, body.url, body.field)
        action = "ai.accept_edited" if edited else f"ai.{decision}"
        await audit(request, action, collection_id,
                    f"{body.field} {body.url} → {value}" + (f" edited to {edited}" if edited else ""))
        return htmx_done(request, {"url": body.url, "field": body.field, "state": decision + "ed",
                                   **({"value": edited} if edited else {})})

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
            except ConflictError:
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
