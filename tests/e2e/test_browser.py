"""Browser journeys (TEST-STRATEGY-2026-10-09.md section 9.1, B1–B3): what only a browser can show.
The engine runs as a real uvicorn process (fake LLM, fake crawler subprocess, the test database);
Chromium drives it through Playwright. Each journey checks the page the curator sees, then the API.

    python -m playwright install chromium     # once; CI does it in the workflow
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg
import pytest

from tests.support.crawler import FAKE_RUN_PY

playwright = pytest.importorskip("playwright.async_api")

REPO = Path(__file__).resolve().parents[2]
ADMIN_PASSWORD = "s3cret-admin"
CURATOR_PASSWORD = "curator-pass-1"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def server(fresh_database, tmp_path):
    """The engine on a free port, login enabled. A clean environment: no .env (the working directory
    is empty), no OpenAI key, no webhook, no AWS."""
    crawler = tmp_path / "crawler"
    crawler.mkdir()
    (crawler / "run.py").write_text(FAKE_RUN_PY)
    port = _free_port()
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path), "PYTHONPATH": str(REPO),
        "DATABASE_URL": fresh_database, "DATA_DIR": str(tmp_path / "data"), "CRAWLER_ROOT": str(crawler),
        "CRAWLER_PYTHON": sys.executable, "SCRAPE_BACKEND": "local", "SCRAPE_POLL_INTERVAL_S": "0.2",
        "LLM_PROVIDER": "fake", "LLM_RETRY_DELAY_S": "0", "LLM_PATTERN_BATCH_URLS": "50", "OPENAI_API_KEY": "", "NOTIFY_WEBHOOK_URL": "",
        "APP_PASSWORD": ADMIN_PASSWORD, "SESSION_SECRET": "browser-test-secret",
    }
    log = (tmp_path / "server.log").open("w")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "sde_curation.web.app:app", "--port", str(port), "--log-level", "warning"],
        cwd=tmp_path, env=env, stdout=log, stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{port}"
    try:
        for _ in range(150):
            try:
                if httpx.get(f"{base}/health", timeout=1, trust_env=False).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if proc.poll() is not None:
                raise AssertionError(f"the engine exited: {(tmp_path / 'server.log').read_text()[-2000:]}")
            time.sleep(0.2)
        else:
            raise AssertionError("the engine did not answer /health")
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()


@pytest.fixture
async def api(server):
    """An API client signed in as the bootstrap admin, for setup and for checks."""
    async with httpx.AsyncClient(base_url=server, timeout=60, trust_env=False) as c:
        r = await c.post("/login", data={"username": "admin", "password": ADMIN_PASSWORD, "next": "/"})
        assert r.status_code == 303, r.text
        yield c


@pytest.fixture
async def browser():
    async with playwright.async_playwright() as pw:
        b = await pw.chromium.launch()
        try:
            yield b
        finally:
            await b.close()


async def sign_in(page, base: str, username: str, password: str) -> None:
    await page.goto(f"{base}/login")
    await page.fill("input[name=username]", username)
    await page.fill("input[name=password]", password)
    async with page.expect_navigation():
        await page.click("button[type=submit]")


async def job_ends(api, cid: str, *, timeout: float = 60) -> dict:
    for _ in range(int(timeout / 0.2)):
        jobs = (await api.get(f"/api/collections/{cid}/jobs")).json()
        if jobs and jobs[0]["state"] in ("succeeded", "failed"):
            assert jobs[0]["state"] == "succeeded", jobs[0].get("error")
            return jobs[0]
        await asyncio.sleep(0.2)
    raise AssertionError("the job did not finish")


async def create(api, cid: str, pages: int) -> None:
    r = await api.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": pages,
                                                 "division": "Heliophysics"})
    assert r.status_code == 201, r.text


async def until(check, what: str, timeout: float = 15):
    for _ in range(int(timeout / 0.1)):
        if await check():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out: {what}")


# ── B1: the stepper while a job runs (the 2026-10-06 bug) ──────────────────────────────────────────


async def test_b1_a_step_opens_while_a_job_runs_and_its_page_is_slow(server, api, browser, fresh_database):
    """A crawl streams progress (header and stepper refresh all along) while the clicked step's page
    is slow (a lock on index_runs, which it reads, as a 7 s count made it on test on 2026-10-06).
    The step must open and its URL must be pushed. Then, with no job, an ordinary click works too."""
    cid = "slow.example.org"
    await create(api, cid, 600)  # 600 pages at 0.02 s each: about 12 s of progress events
    assert (await api.post(f"/api/collections/{cid}/scrape")).status_code == 202
    page = await browser.new_page()
    await sign_in(page, server, "admin", ADMIN_PASSWORD)
    await page.goto(f"{server}/collections/{cid}")
    start = await page.get_attribute("#tab-body", "data-step")
    target = "backlog" if start != "backlog" else "scraped"
    await page.wait_for_timeout(1500)  # the event stream is connected; refreshes are running

    with psycopg.connect(fresh_database) as lock:
        lock.execute("LOCK TABLE index_runs IN ACCESS EXCLUSIVE MODE")
        await page.click(f".pipeline a[href$='step={target}']")
        await page.wait_for_timeout(4000)  # several progress refreshes land while the step waits
        lock.rollback()
    await page.wait_for_function(f"document.querySelector('#tab-body')?.dataset.step === '{target}'",
                                 timeout=15_000)
    assert page.url.endswith(f"step={target}"), f"the URL was not pushed: {page.url}"

    await job_ends(api, cid)
    await page.wait_for_timeout(1000)
    await page.click(".pipeline a[href$='step=backlog']")
    await page.wait_for_function("document.querySelector('#tab-body')?.dataset.step === 'backlog'", timeout=15_000)
    assert page.url.endswith("step=backlog")
    assert await page.locator(".pipeline li.selected a[href$='step=backlog']").count() == 1


# ── B2: accept and reject suggestions in the review tables ─────────────────────────────────────────


async def test_b2_accept_and_reject_suggestions(server, api, browser):
    """Exclusion suggestions: Accept makes a rule, Reject dismisses. AI metadata: ✓ accepts a
    title as a rule, ✕ dismisses one. Each click reloads the page with the decision gone from it."""
    cid = "review.example.org"
    await create(api, cid, 120)  # 96 documents, two batches of Suggest exclusions: at least two suggestions
    await api.post(f"/api/collections/{cid}/scrape")
    await job_ends(api, cid)
    assert (await api.post(f"/api/collections/{cid}/recompute")).status_code == 200
    await api.post(f"/api/collections/{cid}/suggest/patterns")
    await job_ends(api, cid)
    pending = (await api.get(f"/api/collections/{cid}/suggestions")).json()
    assert len(pending) >= 2, pending

    page = await browser.new_page()
    page.on("dialog", lambda d: asyncio.ensure_future(d.accept()))
    await sign_in(page, server, "admin", ADMIN_PASSWORD)
    await page.goto(f"{server}/collections/{cid}?tab=curate")
    rows = page.locator(".suggestions tbody tr")
    assert await rows.count() == len(pending)

    accepted = (await rows.nth(0).locator("td.match code").inner_text()).strip()
    async with page.expect_navigation():
        await rows.nth(0).get_by_role("button", name="Accept").click()
    assert await rows.count() == len(pending) - 1, "the accepted suggestion is still listed"
    rejected = (await rows.nth(0).locator("td.match code").inner_text()).strip()
    async with page.expect_navigation():
        await rows.nth(0).get_by_role("button", name="Reject").click()
    assert await rows.count() == len(pending) - 2, "the rejected suggestion is still listed"

    rules = {p["match"] for p in (await api.get(f"/api/collections/{cid}/patterns")).json()}
    assert accepted in rules and rejected not in rules
    rejected_rows = (await api.get(f"/api/collections/{cid}/suggestions?state=rejected")).json()
    assert [s["match"] for s in rejected_rows] == [rejected]

    await api.post(f"/api/collections/{cid}/suggest/metadata")
    await job_ends(api, cid)

    async def ai_titles() -> int:
        items = (await api.get(f"/api/collections/{cid}/delta?limit=1000")).json()["items"]
        return sum(1 for d in items if d.get("title_ai"))

    suggested = await ai_titles()
    await page.goto(f"{server}/collections/{cid}?tab=delta")
    accept = page.locator("button[title='accept this title']")
    before = await accept.count()
    assert before >= 2, "no AI titles to decide"  # the rows of the first page
    async with page.expect_navigation():
        await accept.first.click()
    assert await accept.count() == before - 1, "the accepted title still has its badge"
    dismiss = page.locator("td:has(button[title='accept this title']) button[title='dismiss']")
    async with page.expect_navigation():
        await dismiss.first.click()
    assert await accept.count() == before - 2, "the dismissed title still has its badge"

    assert await ai_titles() == suggested - 2


# ── B3: sign in and out; a curator cannot reach admin pages ────────────────────────────────────────


async def test_b3_sign_in_and_a_curator_cannot_reach_admin_pages(server, browser):
    page = await browser.new_page()
    await page.goto(f"{server}/")
    assert "/login" in page.url, "an anonymous visitor was not sent to sign in"

    await sign_in(page, server, "admin", "wrong password")
    assert "/login" in page.url, "a wrong password signed in"
    await sign_in(page, server, "admin", ADMIN_PASSWORD)
    assert "/login" not in page.url
    await page.goto(f"{server}/users")
    await page.fill("form[action='/users'] input[name=username]", "carol")
    await page.fill("form[action='/users'] input[name=password]", CURATOR_PASSWORD)
    async with page.expect_navigation():
        await page.click("form[action='/users'] button")
    assert await page.get_by_text("carol").count() >= 1, "the new curator is not listed"
    await page.click("details:has(.menu-panel) > summary")  # the account menu
    async with page.expect_navigation():
        await page.click("form[action='/logout'] button")
    assert "/login" in page.url, "sign out did not return to the sign-in page"
    r = await page.goto(f"{server}/users")
    assert "/login" in page.url, "/users answered after sign out"

    await sign_in(page, server, "carol", CURATOR_PASSWORD)
    assert "/login" not in page.url
    assert await page.locator("a[href='/users']").count() == 0, "a curator sees the Users menu"
    r = await page.goto(f"{server}/users")
    assert r.status == 403, f"a curator reached /users ({r.status})"
