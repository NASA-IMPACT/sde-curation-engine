"""FastAPI application: JSON API + HTMX pages + SSE."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from ..config import Settings, get_settings
from ..db import Database
from ..events import EventBus, sse_format
from ..models import Collection, CollectionCreate, Status
from ..store import write_collection_yaml

_HERE = Path(__file__).parent
templates = Jinja2Templates(directory=_HERE / "templates")


def dom_id(value: str) -> str:
    """Collection ids contain dots (host names); make them safe for CSS id selectors."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", value)


templates.env.filters["dom_id"] = dom_id


class StatusChange(BaseModel):
    status: Status
    note: str | None = None
    force: bool = False


def _is_htmx(request: Request) -> bool:
    return request.headers.get("HX-Request") == "true"


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        db = await Database(settings.resolved_db_path).connect()
        app.state.settings = settings
        app.state.db = db
        app.state.bus = EventBus()
        try:
            yield
        finally:
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

    @app.get("/collections/{collection_id}", response_class=HTMLResponse)
    async def collection_page(request: Request, collection_id: str):
        c = await must_get(request, collection_id)
        ctx = {
            "c": c,
            "history": await db(request).status_history(collection_id),
            "jobs": await db(request).list_jobs(collection_id),
            "statuses": list(Status),
        }
        return templates.TemplateResponse(request, "collection.html", ctx)

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
        if await db(request).get_collection(body.collection_id):
            raise HTTPException(409, f"collection {body.collection_id!r} already exists")
        c = Collection(**body.model_dump())
        await db(request).insert_collection(c)
        write_collection_yaml(settings.collections_dir, c)
        emit_collection(request, c)
        return c

    @app.post("/collections", include_in_schema=False)
    async def form_create(request: Request):
        form = await request.form()
        data = {k: v for k, v in form.items() if v != ""}
        try:
            body = CollectionCreate(**data)
        except ValueError as e:
            raise HTTPException(422, str(e)) from e
        await api_create(request, body)
        return RedirectResponse("/", status_code=303)

    @app.get("/api/collections/{collection_id}", response_model=Collection)
    async def api_get(request: Request, collection_id: str):
        return await must_get(request, collection_id)

    @app.delete("/api/collections/{collection_id}", status_code=204)
    async def api_delete(request: Request, collection_id: str):
        if not await db(request).delete_collection(collection_id):
            raise HTTPException(404, "not found")
        bus(request).publish("collection_deleted", {"collection_id": collection_id})

    @app.post("/api/collections/{collection_id}/status", response_model=None)
    async def api_set_status(request: Request, collection_id: str, body: StatusChange):
        await must_get(request, collection_id)
        try:
            c = await db(request).set_status(
                collection_id, body.status, body.note, force=body.force
            )
        except ValueError as e:
            raise HTTPException(409, str(e)) from e
        write_collection_yaml(settings.collections_dir, c)
        emit_collection(request, c)
        if _is_htmx(request):
            return templates.TemplateResponse(
                request, "partials/row.html", await row_context(request, c)
            )
        return c

    @app.get("/api/collections/{collection_id}/history")
    async def api_history(request: Request, collection_id: str):
        await must_get(request, collection_id)
        return await db(request).status_history(collection_id)

    # ── SSE ────────────────────────────────────────────────────────────

    @app.get("/events")
    async def events(request: Request):
        async def gen():
            async for msg in bus(request).subscribe():
                yield sse_format(msg)

        return EventSourceResponse(gen(), ping=15)

    return app


app = create_app()
