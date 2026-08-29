"""FastAPI application. Phase 0: lifespan wiring + /health."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from ..config import Settings, get_settings
from ..db import Database


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        db = await Database(settings.resolved_db_path).connect()
        app.state.settings = settings
        app.state.db = db
        try:
            yield
        finally:
            await db.close()

    app = FastAPI(title="SDE Curation Engine", version="0.1.0", lifespan=lifespan)

    @app.get("/health")
    async def health(request: Request) -> dict:
        db: Database = request.app.state.db
        try:
            ok = await db.ping()
        except Exception as e:  # noqa: BLE001 - surfaced to the caller, not hidden
            return {"ok": False, "db": f"error: {e}"}
        return {"ok": ok, "db": "ok" if ok else "error"}

    return app


app = create_app()
