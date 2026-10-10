"""I6: who can reach each route (TEST-STRATEGY-2026-10-09.md section 4), as one table.

Every route the app serves with login on is in ACCESS: open to anyone, any signed-in user, or
admins only. A route added without a row fails the first test, so whoever adds it decides who may
reach it. The other tests send a real request to every route as an anonymous visitor, a curator
and an admin. Path parameters name things that do not exist, so no request changes anything.
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.web.app import create_app
from tests.support.flows import SECURED, add_user, login

OPEN, SIGNED_IN, ADMIN = "open", "signed in", "admin"

ACCESS: dict[tuple[str, str], str] = {
    ("GET", "/health"): OPEN,
    ("GET", "/login"): OPEN,
    ("POST", "/login"): OPEN,
    ("POST", "/logout"): SIGNED_IN,
    ("GET", "/account"): SIGNED_IN,
    ("POST", "/account/password"): SIGNED_IN,
    ("GET", "/users"): ADMIN,
    ("POST", "/users"): ADMIN,
    ("POST", "/users/{user_id}/active"): ADMIN,
    ("POST", "/users/{user_id}/role"): ADMIN,
    ("POST", "/users/{user_id}/password"): ADMIN,
    ("GET", "/docs"): SIGNED_IN,
    ("GET", "/docs/oauth2-redirect"): SIGNED_IN,
    ("GET", "/openapi.json"): SIGNED_IN,
    ("GET", "/redoc"): SIGNED_IN,
    ("GET", "/health/db"): SIGNED_IN,
    ("GET", "/events"): SIGNED_IN,
    ("GET", "/"): SIGNED_IN,
    ("GET", "/rows"): SIGNED_IN,
    ("GET", "/jobs"): SIGNED_IN,
    ("GET", "/jobs/panel"): SIGNED_IN,
    ("GET", "/history"): SIGNED_IN,
    ("GET", "/manual"): SIGNED_IN,
    ("GET", "/api/audit"): SIGNED_IN,
    ("GET", "/api/llm/prompts"): SIGNED_IN,
    ("POST", "/collections"): SIGNED_IN,
    ("GET", "/collections/{collection_id}"): SIGNED_IN,
    ("GET", "/collections/{collection_id}/curate"): SIGNED_IN,
    ("GET", "/collections/{collection_id}/header"): SIGNED_IN,
    ("GET", "/collections/{collection_id}/pipeline"): SIGNED_IN,
    ("GET", "/collections/{collection_id}/row"): SIGNED_IN,
    ("GET", "/collections/{collection_id}/rules"): SIGNED_IN,
    ("GET", "/collections/{collection_id}/step/{step}"): SIGNED_IN,
    ("GET", "/collections/{collection_id}/tab-body"): SIGNED_IN,
    ("GET", "/collections/{collection_id}/tab/{tab}"): SIGNED_IN,
    ("GET", "/collections/{collection_id}/urls/{set_}"): SIGNED_IN,
    ("GET", "/api/collections"): SIGNED_IN,
    ("POST", "/api/collections"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}"): SIGNED_IN,
    ("DELETE", "/api/collections/{collection_id}"): ADMIN,
    ("POST", "/api/collections/{collection_id}/ai/bulk"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/ai/{decision}"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/audit"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/crawl/existing"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/curated"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/delta"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/deltas"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/division"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/dump"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/history"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/index"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/index-key"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/index/revalidate"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/index_runs"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/jobs"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/jobs/cancel"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/name"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/patterns"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/patterns"): SIGNED_IN,
    ("DELETE", "/api/collections/{collection_id}/patterns/{pattern_id}"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/promote"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/promote/urls"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/recompute"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/scrape"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/stage"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/status"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/suggest/metadata"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/suggest/patterns"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/suggest/titles"): SIGNED_IN,
    ("GET", "/api/collections/{collection_id}/suggestions"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/suggestions/bulk"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/suggestions/{sid}/{decision}"): SIGNED_IN,
    ("POST", "/api/collections/{collection_id}/urls"): SIGNED_IN,
}

# Values for path parameters: things that do not exist, so a request that gets through changes nothing.
MISSING = {"collection_id": "missing.example.org", "user_id": "999", "pattern_id": "999", "sid": "999",
           "decision": "accept", "step": "curating", "tab": "overview", "set_": "dump"}
# Form fields the admin routes validate before their role check; harmless values.
FORM = {"username": "x", "password": "not-a-real-password", "active": "1", "role": "curator"}
NOT_SENT_AS_CURATOR = {("GET", "/events"), ("POST", "/logout")}  # a stream that never ends; signs out


def routes(app) -> set[tuple[str, str]]:
    return {(m, r.path) for r in app.routes if getattr(r, "methods", None) for m in r.methods if m != "HEAD"}


def concrete(path: str) -> str:
    for name, value in MISSING.items():
        path = path.replace("{" + name + "}", value)
    assert "{" not in path, f"no value for a path parameter in {path}"
    return path


@pytest.fixture
async def secured(tmp_path):
    """Login on; anonymous, curator and admin clients on one app."""
    app = create_app(Settings(data_dir=tmp_path, llm_provider="fake", **SECURED))
    async with app.router.lifespan_context(app):
        clients = {who: AsyncClient(transport=ASGITransport(app=app), base_url="http://t")
                   for who in ("anonymous", "curator", "admin")}
        for c in clients.values():
            c.app = app
        await login(clients["admin"], "admin", "s3cret")
        await add_user(clients["admin"], "carol", "curator-pass-1")
        await login(clients["curator"], "carol", "curator-pass-1")
        try:
            yield app, clients
        finally:
            for c in clients.values():
                await c.aclose()


async def send(c, method: str, path: str):
    kwargs = {"data": FORM} if path.startswith("/users") or path == "/account/password" else {}
    return await c.request(method, concrete(path), **kwargs)


async def test_every_route_has_a_row_in_the_access_table(secured):
    app, _ = secured
    served = routes(app)
    assert served - ACCESS.keys() == set(), "a route with no row: decide who may reach it"
    assert ACCESS.keys() - served == set(), "a row for a route the app no longer serves"


async def test_an_anonymous_visitor_reaches_only_the_open_routes(secured):
    _, clients = secured
    for (method, path), access in ACCESS.items():
        if access == OPEN:
            continue
        r = await send(clients["anonymous"], method, path)
        signed_out = r.status_code == 401 or (r.status_code == 302 and "/login" in r.headers["location"])
        assert signed_out, f"{method} {path} answered an anonymous visitor with {r.status_code}"
    assert (await clients["anonymous"].get("/health")).status_code == 200
    assert (await clients["anonymous"].get("/login")).status_code == 200


async def test_a_curator_is_refused_exactly_the_admin_routes(secured):
    _, clients = secured
    for (method, path), access in ACCESS.items():
        if access == OPEN or (method, path) in NOT_SENT_AS_CURATOR:
            continue
        r = await send(clients["curator"], method, path)
        if access == ADMIN:
            assert r.status_code == 403, f"a curator reached {method} {path} ({r.status_code})"
        else:
            assert r.status_code not in (401, 403), f"a curator was refused {method} {path} ({r.status_code})"


async def test_an_admin_reaches_the_admin_routes(secured):
    _, clients = secured
    for (method, path), access in ACCESS.items():
        if access == ADMIN:
            r = await send(clients["admin"], method, path)
            assert r.status_code not in (401, 403), f"an admin was refused {method} {path} ({r.status_code})"
