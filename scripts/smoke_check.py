"""Post-deploy smoke check (TEST-STRATEGY-2026-10-09.md section 9.1, P-c): the deployed engine answers
the way a curator's browser needs it to. READ-ONLY: it signs in, reads pages and the API, and never
starts a job, writes anything or calls the LLM.

    python scripts/smoke_check.py https://<cloudfront-domain>        # APP_PASSWORD in the environment

Checks, in order, each with a step name in its failure message:
  1. /health answers ok (liveness)
  2. /login answers (the sign-in page)
  3. with APP_PASSWORD: sign in as admin; /health/db answers ok (database reachable); the dashboard,
     the collections API, and (when there is one) a collection page and its tab body answer 200.
Without APP_PASSWORD only steps 1–2 run, and the script says so. Standard library only.
"""

from __future__ import annotations

import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

TIMEOUT_S = 30


def main(base: str) -> int:
    base = base.rstrip("/")
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    def get(step: str, path: str, *, want_json: bool = False):
        try:
            with opener.open(base + path, timeout=TIMEOUT_S) as r:
                body = r.read()
                if r.status != 200:
                    raise SystemExit(f"FAIL {step}: {path} answered {r.status}")
                return json.loads(body) if want_json else body
        except urllib.error.HTTPError as e:
            raise SystemExit(f"FAIL {step}: {path} answered {e.code}") from e
        except urllib.error.URLError as e:
            raise SystemExit(f"FAIL {step}: {path} unreachable ({e.reason})") from e

    health = get("liveness", "/health", want_json=True)
    if not health.get("ok"):
        raise SystemExit(f"FAIL liveness: /health said {health}")
    print("ok   liveness: /health")
    get("sign-in page", "/login")
    print("ok   sign-in page: /login")

    password = os.environ.get("APP_PASSWORD")
    if not password:
        print("skip signed-in checks: APP_PASSWORD is not set")
        return 0
    data = urllib.parse.urlencode({"username": "admin", "password": password, "next": "/"}).encode()
    req = urllib.request.Request(base + "/login", data=data, method="POST")
    try:
        opener.open(req, timeout=TIMEOUT_S)
    except urllib.error.HTTPError as e:
        if e.code not in (302, 303):
            raise SystemExit(f"FAIL sign in: /login answered {e.code}") from e
    if not any(c.name for c in jar):
        raise SystemExit("FAIL sign in: no session cookie (wrong password?)")
    print("ok   sign in")

    db = get("database", "/health/db", want_json=True)
    if not db.get("ok"):
        raise SystemExit(f"FAIL database: /health/db said {db}")
    print(f"ok   database: /health/db (loop lag max {db.get('loop_lag_ms', {}).get('max')} ms)")
    get("dashboard", "/")
    print("ok   dashboard: /")
    collections = get("collections API", "/api/collections", want_json=True)
    print(f"ok   collections API: {len(collections)} collections")
    if collections:
        cid = urllib.parse.quote(collections[0]["collection_id"])
        get("collection page", f"/collections/{cid}")
        get("tab body", f"/collections/{cid}/tab-body")
        print(f"ok   collection page and tab body: {collections[0]['collection_id']}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    sys.exit(main(sys.argv[1]))
