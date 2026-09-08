"""Login with local user accounts and an HMAC-signed session cookie (stdlib only).

Enabled only when `Settings.app_password` is set (it seeds the first `admin` account). Everything
except /health, /login and /static requires a valid `sde_session` cookie that resolves to an
active user. Browser / HTMX callers are redirected to /login; JSON callers (the /api/* routes,
/events) get a 401 so nothing fails silently.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from urllib.parse import quote

from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

COOKIE = "sde_session"
OPEN_PATHS = ("/health", "/login")
OPEN_PREFIXES = ("/static/",)


# ── passwords ──────────────────────────────────────────────────────────


def hash_password(password: str, *, n_log2: int = 14) -> str:
    """scrypt with a random 16-byte salt: `scrypt$<log2 n>$<r>$<p>$<salt hex>$<hash hex>`."""
    salt = os.urandom(16)
    r, p = 8, 1
    digest = hashlib.scrypt(password.encode(), salt=salt, n=2**n_log2, r=r, p=p, dklen=32)
    return f"scrypt${n_log2}${r}${p}${salt.hex()}${digest.hex()}"


def verify_password(stored: str, given: str) -> bool:
    try:
        algo, n_log2, r, p, salt_hex, digest_hex = stored.split("$")
        if algo != "scrypt":
            return False
        digest = hashlib.scrypt(
            given.encode(), salt=bytes.fromhex(salt_hex), n=2 ** int(n_log2), r=int(r), p=int(p), dklen=32
        )
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(digest.hex(), digest_hex)


# ── session tokens ─────────────────────────────────────────────────────


def sign(secret: str, user_id: int, session_version: int, expires_at: int) -> str:
    """`<uid>.<session_version>.<expires>.<hex hmac>` — nothing user-supplied is trusted: the
    middleware re-reads the user row and compares `session_version` on every request."""
    msg = f"{int(user_id)}.{int(session_version)}.{int(expires_at)}"
    mac = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return f"{msg}.{mac}"


def verify(secret: str, token: str | None, *, now: float | None = None) -> tuple[int, int] | None:
    """(user_id, session_version) for a valid, unexpired token; None otherwise."""
    if not token or token.count(".") != 3:
        return None
    uid, sv, exp, mac = token.split(".")
    if not (uid.isdigit() and sv.isdigit() and exp.isdigit()):
        return None
    expected = hmac.new(secret.encode(), f"{uid}.{sv}.{exp}".encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, mac):
        return None
    if int(exp) <= (now if now is not None else time.time()):
        return None
    return int(uid), int(sv)


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
    """Pure ASGI so it wraps the whole app (StaticFiles included) without touching routes.
    Resolves the session to a `User` (one primary-key lookup) and exposes it as
    `request.state.user`; a disabled user or a changed password ends the session at once."""

    def __init__(self, app: ASGIApp, *, secret: str):
        self.app = app
        self.secret = secret

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or _is_open(scope["path"]):
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        claims = verify(self.secret, request.cookies.get(COOKIE))
        user = await scope["app"].state.db.get_user(claims[0]) if claims else None
        if user and user.active and user.session_version == claims[1]:
            scope.setdefault("state", {})["user"] = user
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
