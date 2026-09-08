"""Shared-password login with an HMAC-signed session cookie (stdlib only).

Enabled only when `Settings.app_password` is set. Everything except /health, /login and /static
requires a valid `sde_session` cookie. Browser / HTMX callers are redirected to /login; JSON
callers (the /api/* routes, /events) get a 401 so nothing fails silently.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from urllib.parse import quote

from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

COOKIE = "sde_session"
OPEN_PATHS = ("/health", "/login")
OPEN_PREFIXES = ("/static/",)


def sign(secret: str, expires_at: int) -> str:
    """`<expires>.<hex hmac>` — the only state is the expiry, so nothing user-supplied is trusted."""
    msg = str(int(expires_at)).encode()
    mac = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    return f"{msg.decode()}.{mac}"


def verify(secret: str, token: str | None, *, now: float | None = None) -> bool:
    if not token or "." not in token:
        return False
    exp_s, _, mac = token.partition(".")
    if not exp_s.isdigit():
        return False
    expected = hmac.new(secret.encode(), exp_s.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, mac):
        return False
    return int(exp_s) > (now if now is not None else time.time())


def password_ok(expected: str, given: str) -> bool:
    return hmac.compare_digest(expected, given)


def safe_next(value: str | None) -> str:
    """Only same-site absolute paths — never an open redirect."""
    if value and value.startswith("/") and not value.startswith("//"):
        return value
    return "/"


def _wants_html(scope: Scope) -> bool:
    headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
    return headers.get("hx-request") == "true" or "text/html" in headers.get("accept", "")


def _is_open(path: str) -> bool:
    return path in OPEN_PATHS or path.startswith(OPEN_PREFIXES)


class AuthMiddleware:
    """Pure ASGI so it wraps the whole app (StaticFiles included) without touching routes."""

    def __init__(self, app: ASGIApp, *, secret: str):
        self.app = app
        self.secret = secret

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or _is_open(scope["path"]):
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        if verify(self.secret, request.cookies.get(COOKIE)):
            await self.app(scope, receive, send)
            return
        if _wants_html(scope):
            target = "/login?next=" + quote(request.url.path, safe="/")
            if request.headers.get("hx-request") == "true":
                resp: Response = Response(status_code=401, headers={"HX-Redirect": target})
            else:
                resp = RedirectResponse(target, status_code=302)
        else:
            resp = JSONResponse({"detail": "login required"}, status_code=401)
        await resp(scope, receive, send)
