"""Fixtures for the integration and end-to-end tests (loaded for the whole suite by tests/conftest.py;
none is automatic: the unit tests never touch them). tests/integration/conftest.py and
tests/e2e/conftest.py make `fresh_database` automatic for their tests."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.schema import TABLES, migrate_sync
from sde_curation.web.app import create_app
from tests.support.crawler import FAKE_RUN_PY
from tests.support.flows import FAKE_INDEXER, PROGRESS_MAX_BYTES, SECURED, _crawler_app, login

# ── PostgreSQL ─────────────────────────────────────────────────────────
# TEST_DATABASE_URL (CI: a `services: postgres` job container; locally: `make db-up` +
# postgresql://engine:engine@localhost:5432/engine) or, when unset, a throwaway container started
# by testcontainers (needs Docker). The schema is created once; every test starts from empty tables.


@pytest.fixture(scope="session")
def pg_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        yield url
        return
    try:
        try:
            from testcontainers.community.postgres import PostgresContainer
        except ImportError:  # testcontainers < 4.15
            from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:17-alpine", username="engine", password="engine", dbname="engine")
        container.start()
    except Exception as e:  # noqa: BLE001 - one clear message instead of 140 identical failures
        pytest.exit(f"PostgreSQL is required: set TEST_DATABASE_URL or start Docker ({e})", returncode=3)
    try:
        yield (f"postgresql://engine:engine@{container.get_container_host_ip()}"
               f":{container.get_exposed_port(5432)}/engine")
    finally:
        container.stop()


@pytest.fixture(scope="session")
def pg_schema(pg_url) -> str:
    with psycopg.connect(pg_url) as conn:
        migrate_sync(conn)
    return pg_url


@pytest.fixture
def database_url(pg_schema, monkeypatch) -> str:
    """The test sees DATABASE_URL (Settings reads it) and empty tables."""
    with psycopg.connect(pg_schema, autocommit=True) as conn:
        conn.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE")
    monkeypatch.setenv("DATABASE_URL", pg_schema)
    return pg_schema


@pytest.fixture
def progress_stays_small(monkeypatch):
    """T5.0: a job's progress is its resume checkpoint, written every 3 s and sent to every browser.
    It holds counters, batch numbers as ranges and phase names, never lists of URLs or ids. Every
    progress a test writes must stay under PROGRESS_MAX_BYTES as JSON."""
    from sde_curation.db import Database

    seen: list[tuple[int, str, str]] = []

    def wrap(fn):
        async def wrapped(self, j, *args, **kwargs):
            seen.append((len(json.dumps(j.progress, default=str)), str(j.kind), ",".join(sorted(j.progress))))
            return await fn(self, j, *args, **kwargs)
        return wrapped

    for name in ("insert_job", "update_job", "finish_job"):
        monkeypatch.setattr(Database, name, wrap(getattr(Database, name)))
    yield
    big = [s for s in seen if s[0] >= PROGRESS_MAX_BYTES]
    assert not big, f"job progress over {PROGRESS_MAX_BYTES} bytes: {max(big)}"


@pytest.fixture
def fresh_database(database_url, progress_stays_small) -> str:
    """Empty tables, DATABASE_URL set, and the progress-size check."""
    return database_url


# ── apps and clients ───────────────────────────────────────────────────


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(data_dir=tmp_path, llm_provider="fake")


@pytest.fixture
async def app(settings):
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        yield app


@pytest.fixture
async def client(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        c.app = app
        yield c


@pytest.fixture
async def authed_client(tmp_path):
    """Login enabled, signed in as the bootstrap admin."""
    app = create_app(Settings(data_dir=tmp_path, llm_provider="fake", **SECURED))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        c.app = app
        await login(c, "admin", "s3cret")
        yield c


@pytest.fixture
async def crawler_client(tmp_path):
    """App wired to the fake crawler (tests/support/crawler.py): max_pages=13 simulates a crash,
    max_pages=11 crawls every page under a second (http://) link as well, every 5th page fails (p5
    with a 404, the others with a 403), the rest succeed."""
    app = _crawler_app(tmp_path)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        c.app = app
        yield c


@pytest.fixture
async def authed_crawler_client(tmp_path):
    """crawler_client with login enabled, signed in as the bootstrap admin."""
    app = _crawler_app(tmp_path, **SECURED)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        c.app = app
        await login(c, "admin", "s3cret")
        yield c


@pytest.fixture
async def index_client(tmp_path, monkeypatch):
    """App with fake crawler + fake indexer subprocess + a moto S3 *server* (subprocess needs a real endpoint)."""
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0); server.start()
    port = server._server.socket.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test"); monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1"); monkeypatch.setenv("MOTO_ENDPOINT", endpoint)
    monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)
    import boto3
    boto3.client("s3", region_name="us-east-1", endpoint_url=endpoint).create_bucket(Bucket="cosmos-idx")

    croot = tmp_path / "crawler"; croot.mkdir(); (croot / "run.py").write_text(FAKE_RUN_PY)
    iroot = tmp_path / "indexer"; iroot.mkdir(); (iroot / "api_scraper.py").write_text(FAKE_INDEXER)
    settings = Settings(
        data_dir=tmp_path / "data", crawler_root=croot, crawler_python=Path(sys.executable),
        indexer_root=iroot, indexer_python=Path(sys.executable), cosmos_index_bucket="cosmos-idx",
        index_poll_interval_s=0.1, index_stall_timeout_s=20, scrape_poll_interval_s=0.05, llm_provider="fake",
        validation_delay_s=0.1, validation_poll_interval_s=0.05, validation_timeout_s=1.0,
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        c.app = app; c.s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=endpoint)
        yield c
    server.stop()
