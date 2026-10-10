"""Accounts through the real app and PostgreSQL: login off, the bootstrap admin, signing in, a
password change, and the user administration guards that keep admins from locking everyone out.
Who may reach each route is tests/integration/test_route_access.py; the login gate itself is
tests/unit/test_g3_auth.py; a role change and a disabled account are journey J7. These tests
replace the rest of the old tests/integration/test_auth.py and tests/e2e/test_auth.py (P4,
TEST-STRATEGY-2026-10-09.md)."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.web import auth
from sde_curation.web.app import create_app
from tests.support.flows import SECURED, add_user, login

ADMIN_PASSWORD = SECURED["app_password"]


@pytest.fixture
async def secured(tmp_path):
    """Login on; `new()` opens another browser on the same app."""
    app = create_app(Settings(data_dir=tmp_path, llm_provider="fake", **SECURED))
    opened: list[AsyncClient] = []

    def new() -> AsyncClient:
        c = AsyncClient(transport=ASGITransport(app=app), base_url="http://t")
        c.app = app
        opened.append(c)
        return c

    async with app.router.lifespan_context(app):
        try:
            yield new
        finally:
            for c in opened:
                await c.aclose()


async def test_with_login_off_there_is_no_sign_in_and_every_caller_is_an_admin(client):
    await client.post("/api/collections", json={"seed_url": "https://x.org", "name": "x"})
    assert [(await client.get(p)).status_code for p in ("/", "/login", "/users")] == [200, 404, 404]
    assert "Delete collection" in (await client.get("/collections/x.org")).text


async def test_the_bootstrap_admin_is_created_once_across_restarts(secured, tmp_path):
    db = secured().app.state.db
    assert [(u.username, u.role, u.active) for u in await db.list_users()] == [("admin", "admin", True)]
    again = create_app(Settings(data_dir=tmp_path, llm_provider="fake", **SECURED))
    async with again.router.lifespan_context(again):
        assert await again.state.db.count_users() == 1


async def test_signing_in_needs_an_active_account_and_its_password_and_returns_to_a_local_page(secured):
    admin, bob = secured(), secured()
    user = await add_user(admin, "bob", "bobpassword")
    await login(admin, "admin", ADMIN_PASSWORD)

    for username, password in (("bob", "wrong"), ("nobody", "bobpassword")):
        r = await bob.post("/login", data={"username": username, "password": password, "next": "/"})
        assert (r.status_code, auth.COOKIE in r.cookies, "Wrong username" in r.text) == (401, False, True), username
    r = await bob.post("/login", data={"username": "BOB", "password": "bobpassword", "next": "/api/collections"})
    assert (r.status_code, r.headers["location"]) == (303, "/api/collections")  # usernames ignore case
    r = await bob.post("/login", data={"username": "bob", "password": "bobpassword", "next": "//evil.example/x"})
    assert r.headers["location"] == "/"
    page = await bob.get("/", headers={"Accept": "text/html"})
    assert "signed in as" in page.text and "bob" in page.text

    assert (await admin.post(f"/users/{user.id}/active", data={"active": 0})).status_code == 303
    assert (await bob.post("/login", data={"username": "bob", "password": "bobpassword"})).status_code == 401
    assert (await admin.post(f"/users/{user.id}/active", data={"active": 1})).status_code == 303
    await login(bob, "bob", "bobpassword")
    assert (await bob.post("/logout")).status_code == 303
    assert (await bob.get("/api/collections")).status_code == 401


async def test_a_password_change_keeps_this_session_and_signs_out_the_others(secured):
    here, there = secured(), secured()
    await add_user(here, "bob", "bobpassword")
    await login(here, "bob", "bobpassword")
    await login(there, "bob", "bobpassword")
    for form, status in (({"current": "wrong", "new": "newpassword1", "confirm": "newpassword1"}, 401),
                         ({"current": "bobpassword", "new": "newpassword1", "confirm": "different1"}, 422),
                         ({"current": "bobpassword", "new": "short", "confirm": "short"}, 422)):
        assert (await here.post("/account/password", data=form)).status_code == status, form

    r = await here.post("/account/password", data={"current": "bobpassword", "new": "newpassword1",
                                                    "confirm": "newpassword1"})

    assert (r.status_code, auth.COOKIE in r.cookies) == (303, True)  # re-issued for the new session version
    assert [(await c.get("/api/collections")).status_code for c in (here, there)] == [200, 401]
    await login(there, "bob", "newpassword1")


async def test_admins_manage_accounts_but_never_lock_every_admin_out(secured):
    """An admin never disables or demotes their own account, so there is always an active admin
    (the "last active admin" check in guard_lockout is reached only through that same case)."""
    admin, root2, alice = secured(), secured(), secured()
    await login(admin, "admin", ADMIN_PASSWORD)
    users = admin.app.state.db
    steps = [  # (who, path, form, status)
        (admin, "/users", {"username": "Alice", "password": "alicepass1", "role": "curator"}, 303),
        (admin, "/users", {"username": "alice", "password": "alicepass1"}, 409),  # names ignore case
        (admin, "/users", {"username": "bad name", "password": "alicepass1"}, 422),
        (admin, "/users", {"username": "ok", "password": "short"}, 422),
        (admin, "/users/{admin}/role", {"role": "curator"}, 409),  # the only admin
        (admin, "/users/{alice}/password", {"password": "newalicepass"}, 303),
        (admin, "/users", {"username": "root2", "password": "root2password", "role": "admin"}, 303),
        (admin, "/users/{admin}/active", {"active": 0}, 409),  # not yourself, even with another admin
        (admin, "/users/{admin}/role", {"role": "curator"}, 409),
        (root2, "/users/{admin}/active", {"active": 0}, 303),  # two admins: fine
        (root2, "/users/{root2}/active", {"active": 0}, 409),  # not yourself
        (root2, "/users/{alice}/role", {"role": "admin"}, 303),
        (root2, "/users/{alice}/role", {"role": "curator"}, 303),
        (root2, "/users/{root2}/role", {"role": "curator"}, 409),  # root2 is the last active admin
    ]
    for who, path, form, status in steps:
        if who is root2 and auth.COOKIE not in root2.cookies:
            await login(root2, "root2", "root2password")
        ids = {u.username: u.id for u in await users.list_users()}
        r = await who.post(path.format(**ids), data=form)
        assert r.status_code == status, f"{path} {form}: {r.status_code}"

    await login(alice, "alice", "newalicepass")
    assert (await alice.get("/users")).status_code == 403  # the password an admin set works; demoted again
    audit = [(a["action"], a["actor"]) for a in await users.list_audit()]
    assert ("user.create", "admin") in audit and ("user.password", "admin") in audit and ("user.role", "root2") in audit


async def test_a_curator_is_not_offered_the_delete_button_an_admin_sees(secured):
    admin, carol = secured(), secured()
    await login(admin, "admin", ADMIN_PASSWORD)
    await add_user(admin, "carol", "carolpass1")
    await login(carol, "carol", "carolpass1")
    assert (await carol.post("/api/collections", json={"seed_url": "https://x.org", "name": "x"})).status_code == 201

    pages = [(await c.get("/collections/x.org", headers={"Accept": "text/html"})).text for c in (carol, admin)]

    assert ["Delete collection" in p for p in pages] == [False, True]
