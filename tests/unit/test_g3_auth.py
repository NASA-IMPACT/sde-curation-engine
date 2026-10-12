"""The login gate (sde_curation/web/auth.py AuthMiddleware), the sign-in redirect target and the
session user cache, without a database: the middleware wraps a one-route app whose `db` is a stub
that answers `session_user`. Replaces the HTTP checks of the old tests/integration/test_auth.py
that do not need PostgreSQL (P4, TEST-STRATEGY-2026-10-09.md)."""

import time
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from sde_curation.db import Database
from sde_curation.models import Role, User
from sde_curation.web import auth

SECRET = "unit-test-secret"
USER_ID = 1
SESSION_VERSION = 3
AN_HOUR = 3600
SESSION_LOOKUPS = 10  # requests one browser makes inside the cache time (polls, SSE reconnects)


def user(*, active: bool = True, session_version: int = SESSION_VERSION) -> User:
    return User(id=USER_ID, username="bob", password_hash="x", role=Role.CURATOR, active=active,
                session_version=session_version)


def gated_app(stored: User | None) -> Starlette:
    """Every path answers 200 with the signed-in user's name; the gate decides who gets there."""

    async def page(request):
        who = request.scope.get("state", {}).get("user")
        return PlainTextResponse(who.username if who else "nobody")

    async def session_user(user_id):
        return stored if stored and stored.id == user_id else None

    app = Starlette(routes=[Route("/{path:path}", page)], middleware=[Middleware(auth.AuthMiddleware, secret=SECRET)])
    app.state.db = SimpleNamespace(session_user=session_user)
    return app


async def get(stored: User | None, path: str, *, cookie: str | None = None, **headers):
    app = gated_app(stored)
    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url="http://t") as c:
        if cookie:
            c.cookies.set(auth.COOKIE, cookie)
        return await c.get(path, headers=headers)


def session(user_id: int = USER_ID, session_version: int = SESSION_VERSION) -> str:
    return auth.sign(SECRET, user_id, session_version, int(time.time()) + AN_HOUR)


async def test_a_browser_without_a_session_is_sent_to_sign_in_and_back_to_its_page():
    r = await get(user(), "/collections/x", Accept="text/html")

    assert (r.status_code, r.headers["location"]) == (302, "/login?next=/collections/x")


async def test_an_htmx_request_without_a_session_gets_401_and_tells_htmx_where_to_go():
    """A 302 inside an htmx swap would paste the sign-in page into the fragment."""
    r = await get(user(), "/collections/x", **{"HX-Request": "true"})

    assert (r.status_code, r.headers["HX-Redirect"]) == (401, "/login?next=/collections/x")


async def test_an_api_call_without_a_session_gets_a_401_json_answer():
    r = await get(user(), "/api/collections")

    assert (r.status_code, r.json()) == (401, {"detail": "login required"})


@pytest.mark.parametrize(("stored", "cookie"), [
    (None, session()),
    (user(active=False), session()),
    (user(session_version=SESSION_VERSION + 1), session()),
    (user(), session()[:-1] + ("0" if session()[-1] != "0" else "1")),
    (user(), auth.sign(SECRET, USER_ID, SESSION_VERSION, int(time.time()) - 1)),
    (user(), auth.sign("other-secret", USER_ID, SESSION_VERSION, int(time.time()) + AN_HOUR)),
], ids=["user gone", "user disabled", "password changed elsewhere", "tampered cookie", "expired cookie",
        "signed with another secret"])
async def test_a_session_that_no_longer_matches_its_user_is_refused(stored, cookie):
    r = await get(stored, "/api/collections", cookie=cookie)

    assert r.status_code == 401


async def test_a_valid_session_reaches_the_route_as_its_user():
    r = await get(user(), "/api/collections", cookie=session())

    assert (r.status_code, r.text) == (200, "bob")


@pytest.mark.parametrize("path", ["/health", "/login", "/static/app.css"])
async def test_health_sign_in_and_static_files_need_no_session(path):
    """The load balancer's health check and the sign-in page's own stylesheet must load signed out."""
    r = await get(None, path)

    assert (r.status_code, r.text) == (200, "nobody")


@pytest.mark.parametrize(("given", "target"), [
    ("/api/collections", "/api/collections"),
    ("//evil.example/x", "/"),
    ("https://evil.example/x", "/"),
    ("", "/"),
    (None, "/"),
], ids=["a local path", "a protocol-relative URL", "another site", "empty", "missing"])
def test_the_page_after_sign_in_is_only_ever_a_local_path(given, target):
    """`next` comes from the sign-in form: anything but a local path would be an open redirect."""
    assert auth.safe_next(given) == target


async def test_a_session_reads_its_user_row_once_within_the_cache_time():
    """Every poll and SSE reconnect carries the cookie: the user row is read once and reused for
    Database.SESSION_USER_TTL_S, not on every request."""
    db = Database("postgresql://unused")  # no connection: get_user is replaced below
    reads: list[int] = []

    async def get_user(uid):
        reads.append(uid)
        return user()

    db.get_user = get_user

    for _ in range(SESSION_LOOKUPS):
        assert (await db.session_user(USER_ID)).username == "bob"

    assert reads == [USER_ID]
