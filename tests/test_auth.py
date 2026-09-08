"""Shared-password login: off by default, gates everything but /health, /login, /static."""

import time

import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.web import auth
from sde_curation.web.app import create_app


@pytest.fixture
async def secured(tmp_path):
    settings = Settings(data_dir=tmp_path, llm_provider="fake", app_password="s3cret",
                        session_secret="unit-test-secret")
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c


async def test_auth_disabled_by_default(client):
    assert (await client.get("/")).status_code == 200
    assert (await client.get("/login")).status_code == 404


async def test_health_and_static_stay_open(secured):
    r = await secured.get("/health")
    assert r.status_code == 200 and r.json()["ok"] is True
    assert (await secured.get("/static/app.css")).status_code == 200


async def test_browser_is_redirected_and_json_gets_401(secured):
    r = await secured.get("/", headers={"Accept": "text/html"})
    assert r.status_code == 302 and r.headers["location"] == "/login?next=/"
    r = await secured.get("/collections/x", headers={"HX-Request": "true"})
    assert r.status_code == 401 and r.headers["HX-Redirect"].startswith("/login")
    r = await secured.get("/api/collections")
    assert r.status_code == 401 and r.json() == {"detail": "login required"}
    r = await secured.post("/api/collections", json={"seed_url": "https://x.org", "name": "x"})
    assert r.status_code == 401


async def test_login_roundtrip(secured):
    assert (await secured.get("/login")).status_code == 200
    r = await secured.post("/login", data={"password": "nope", "next": "/"})
    assert r.status_code == 401 and auth.COOKIE not in r.cookies
    r = await secured.post("/login", data={"password": "s3cret", "next": "/api/collections"})
    assert r.status_code == 303 and r.headers["location"] == "/api/collections"
    assert auth.COOKIE in r.cookies
    assert (await secured.get("/api/collections")).status_code == 200
    assert (await secured.get("/", headers={"Accept": "text/html"})).status_code == 200
    r = await secured.post("/logout")
    assert r.status_code == 303
    assert (await secured.get("/api/collections")).status_code == 401


async def test_open_redirect_is_neutralised(secured):
    r = await secured.post("/login", data={"password": "s3cret", "next": "//evil.example/x"})
    assert r.headers["location"] == "/"


async def test_tampered_or_expired_cookie_rejected(secured):
    good = auth.sign("unit-test-secret", int(time.time()) + 60)
    secured.cookies.set(auth.COOKIE, good)
    assert (await secured.get("/api/collections")).status_code == 200
    secured.cookies.set(auth.COOKIE, good[:-1] + ("0" if good[-1] != "0" else "1"))
    assert (await secured.get("/api/collections")).status_code == 401
    secured.cookies.set(auth.COOKIE, auth.sign("unit-test-secret", int(time.time()) - 1))
    assert (await secured.get("/api/collections")).status_code == 401
    secured.cookies.set(auth.COOKIE, auth.sign("other-secret", int(time.time()) + 60))
    assert (await secured.get("/api/collections")).status_code == 401


def test_sign_verify_pure():
    t = auth.sign("k", 1_000)
    assert auth.verify("k", t, now=999) and not auth.verify("k", t, now=1_000)
    assert not auth.verify("k", "garbage") and not auth.verify("k", None)
