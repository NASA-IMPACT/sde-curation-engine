"""Login with local accounts: off by default; APP_PASSWORD seeds the bootstrap admin; sessions
die when a user is disabled or changes password; admins manage users, curators cannot."""

import time

import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.web import auth
from sde_curation.web.app import create_app
from tests.conftest import SECURED, add_user, login


@pytest.fixture
async def secured(tmp_path):
    app = create_app(Settings(data_dir=tmp_path, llm_provider="fake", **SECURED))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        c.app = app
        yield c


async def test_auth_disabled_by_default(client):
    assert (await client.get("/")).status_code == 200
    assert (await client.get("/login")).status_code == 404
    assert (await client.get("/users")).status_code == 404


async def test_bootstrap_admin_created_once(secured, tmp_path):
    users = await secured.app.state.db.list_users()
    assert [(u.username, u.role, u.active) for u in users] == [("admin", "admin", True)]
    # a second start on the same database must not add another admin
    app2 = create_app(Settings(data_dir=tmp_path, llm_provider="fake", **SECURED))
    async with app2.router.lifespan_context(app2):
        assert await app2.state.db.count_users() == 1


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
    r = await secured.post("/login", data={"username": "admin", "password": "nope", "next": "/"})
    assert r.status_code == 401 and auth.COOKIE not in r.cookies and "Wrong username" in r.text
    r = await secured.post("/login", data={"username": "nobody", "password": "s3cret", "next": "/"})
    assert r.status_code == 401
    r = await secured.post("/login", data={"username": "Admin", "password": "s3cret", "next": "/api/collections"})
    assert r.status_code == 303 and r.headers["location"] == "/api/collections"  # username is case-insensitive
    assert auth.COOKIE in r.cookies
    assert (await secured.get("/api/collections")).status_code == 200
    page = (await secured.get("/", headers={"Accept": "text/html"}))
    assert page.status_code == 200 and "signed in as" in page.text and "admin" in page.text
    r = await secured.post("/logout")
    assert r.status_code == 303
    assert (await secured.get("/api/collections")).status_code == 401


async def test_open_redirect_is_neutralised(secured):
    r = await secured.post("/login", data={"username": "admin", "password": "s3cret", "next": "//evil.example/x"})
    assert r.headers["location"] == "/"


async def test_tampered_or_expired_cookie_rejected(secured):
    good = auth.sign("unit-test-secret", 1, 1, int(time.time()) + 60)
    secured.cookies.set(auth.COOKIE, good)
    assert (await secured.get("/api/collections")).status_code == 200
    secured.cookies.set(auth.COOKIE, good[:-1] + ("0" if good[-1] != "0" else "1"))
    assert (await secured.get("/api/collections")).status_code == 401
    secured.cookies.set(auth.COOKIE, auth.sign("unit-test-secret", 1, 1, int(time.time()) - 1))
    assert (await secured.get("/api/collections")).status_code == 401
    secured.cookies.set(auth.COOKIE, auth.sign("other-secret", 1, 1, int(time.time()) + 60))
    assert (await secured.get("/api/collections")).status_code == 401
    secured.cookies.set(auth.COOKIE, auth.sign("unit-test-secret", 999, 1, int(time.time()) + 60))  # no such user
    assert (await secured.get("/api/collections")).status_code == 401
    secured.cookies.set(auth.COOKIE, auth.sign("unit-test-secret", 1, 7, int(time.time()) + 60))  # stale session
    assert (await secured.get("/api/collections")).status_code == 401


async def test_disabled_user_session_dies_mid_session(secured):
    bob = await add_user(secured, "bob", "bobpassword")
    bobc = AsyncClient(transport=ASGITransport(app=secured.app), base_url="http://t")
    await login(bobc, "bob", "bobpassword")
    assert (await bobc.get("/api/collections")).status_code == 200
    await login(secured, "admin", "s3cret")
    r = await secured.post(f"/users/{bob.id}/active", data={"active": 0})
    assert r.status_code == 303
    assert (await bobc.get("/api/collections")).status_code == 401
    assert (await bobc.post("/login", data={"username": "bob", "password": "bobpassword"})).status_code == 401
    r = await secured.post(f"/users/{bob.id}/active", data={"active": 1})
    assert r.status_code == 303
    await login(bobc, "bob", "bobpassword")
    assert (await bobc.get("/api/collections")).status_code == 200


async def test_password_change_invalidates_other_sessions(secured):
    await add_user(secured, "bob", "bobpassword")
    c1 = AsyncClient(transport=ASGITransport(app=secured.app), base_url="http://t")
    c2 = AsyncClient(transport=ASGITransport(app=secured.app), base_url="http://t")
    await login(c1, "bob", "bobpassword"); await login(c2, "bob", "bobpassword")
    r = await c1.post("/account/password", data={"current": "wrong", "new": "newpassword1", "confirm": "newpassword1"})
    assert r.status_code == 401
    r = await c1.post("/account/password", data={"current": "bobpassword", "new": "short", "confirm": "short"})
    assert r.status_code == 422
    r = await c1.post("/account/password", data={"current": "bobpassword", "new": "newpassword1", "confirm": "newpassword1"})
    assert r.status_code == 303 and auth.COOKIE in r.cookies  # re-issued for the new session_version
    assert (await c1.get("/api/collections")).status_code == 200  # this session survives
    assert (await c2.get("/api/collections")).status_code == 401  # the other one is out
    await login(c2, "bob", "newpassword1")
    assert (await c2.get("/api/collections")).status_code == 200


async def test_curator_cannot_delete_or_manage_users(secured):
    await add_user(secured, "cur", "curatorpass")
    await login(secured, "cur", "curatorpass")
    await secured.post("/api/collections", json={"seed_url": "https://x.org", "name": "x"})
    assert (await secured.delete("/api/collections/x.org")).status_code == 403
    assert (await secured.get("/users")).status_code == 403
    assert (await secured.post("/users", data={"username": "z", "password": "zzzzzzzzz"})).status_code == 403
    page = (await secured.get("/collections/x.org", headers={"Accept": "text/html"})).text
    assert "Delete collection" not in page and "/users" not in page
    assert (await secured.get("/account")).status_code == 200  # own account is always reachable


async def test_admin_users_crud(secured):
    await login(secured, "admin", "s3cret")
    r = await secured.post("/users", data={"username": "Alice", "password": "alicepass1", "role": "curator"})
    assert r.status_code == 303
    r = await secured.post("/users", data={"username": "alice", "password": "alicepass1"})
    assert r.status_code == 409  # case-insensitive duplicate
    assert (await secured.post("/users", data={"username": "bad name", "password": "alicepass1"})).status_code == 422
    assert (await secured.post("/users", data={"username": "ok", "password": "short"})).status_code == 422
    alice = await secured.app.state.db.get_user_by_username("alice")
    assert alice.role == "curator" and alice.active
    assert (await secured.post(f"/users/{alice.id}/role", data={"role": "admin"})).status_code == 303
    assert (await secured.post(f"/users/{alice.id}/password", data={"password": "newalicepass"})).status_code == 303
    ac = AsyncClient(transport=ASGITransport(app=secured.app), base_url="http://t")
    await login(ac, "alice", "newalicepass")
    assert (await ac.get("/users")).status_code == 200  # now an admin
    page = (await secured.get("/users")).text
    assert "alice" in page and "admin" in page
    audit = await secured.app.state.db.list_audit()
    assert [a["action"] for a in audit][:3] == ["user.password", "user.role", "user.create"]
    assert all(a["actor"] == "admin" for a in audit)


async def test_cannot_disable_self_or_last_admin(secured):
    await login(secured, "admin", "s3cret")
    admin = await secured.app.state.db.get_user_by_username("admin")
    assert (await secured.post(f"/users/{admin.id}/active", data={"active": 0})).status_code == 409
    assert (await secured.post(f"/users/{admin.id}/role", data={"role": "curator"})).status_code == 409
    other = await add_user(secured, "root2", "root2password", role="admin")
    ac = AsyncClient(transport=ASGITransport(app=secured.app), base_url="http://t")
    await login(ac, "root2", "root2password")
    assert (await ac.post(f"/users/{admin.id}/active", data={"active": 0})).status_code == 303  # two admins: fine
    assert (await ac.post(f"/users/{other.id}/active", data={"active": 0})).status_code == 409  # self
    await add_user(secured, "third", "thirdpassword", role="admin")
    third = await secured.app.state.db.get_user_by_username("third")
    assert (await ac.post(f"/users/{third.id}/role", data={"role": "curator"})).status_code == 303
    # root2 is the last active admin now → nobody may demote or disable it
    assert (await ac.post(f"/users/{other.id}/role", data={"role": "curator"})).status_code == 409


def test_sign_verify_pure():
    t = auth.sign("k", 7, 3, 1_000)
    assert auth.verify("k", t, now=999) == (7, 3) and auth.verify("k", t, now=1_000) is None
    assert auth.verify("k", "garbage") is None and auth.verify("k", None) is None
    assert auth.verify("other", t, now=999) is None


def test_hash_verify_password():
    h = auth.hash_password("correct horse")
    assert h.startswith("scrypt$14$8$1$") and h != auth.hash_password("correct horse")  # random salt
    assert auth.verify_password(h, "correct horse") and not auth.verify_password(h, "wrong")
    assert not auth.verify_password("garbage", "x") and not auth.verify_password("", "x")
