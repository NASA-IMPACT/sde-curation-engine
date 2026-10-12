"""LLM jobs carry on across an engine restart (#29 Suggest patterns, #30 Regenerate titles,
#35 Suggest metadata): what was answered is not asked again, and the result equals an uninterrupted run.

The engine is shut down in the middle of a job and started again. A wrapper around the job's model
call counts what is asked and can hold a call until the engine goes down under it.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

import sde_curation.jobs as jobs_mod
from sde_curation.config import Settings
from sde_curation.web.app import create_app
from tests.support.flows import FAKE_RUN_PY, wait_job

CID = "ex.org"


@pytest.fixture
def engines(tmp_path):
    croot = tmp_path / "crawler"; croot.mkdir(); (croot / "run.py").write_text(FAKE_RUN_PY)
    base = {"data_dir": tmp_path / "data", "crawler_root": croot, "crawler_python": Path(sys.executable),
            "scrape_poll_interval_s": 0.05, "llm_provider": "fake", "llm_retry_delay_s": 0,
            "resume_start_delay_s": 0, "resume_stagger_s": 0, "llm_workers": 1}

    def engine(*, batch: int | None = None, **over):
        settings = Settings(**{**base, **over})
        if batch is not None:  # below the setting's minimum of 50, to get several batches from a small crawl
            settings.llm_pattern_batch_urls = batch
        app = create_app(settings)
        return app, AsyncClient(transport=ASGITransport(app=app), base_url="http://t")

    return engine


class Calls:
    """Counts the wrapped model calls by a key; holds every call after the first `hold_after`."""

    def __init__(self, hold_after: int | None = None):
        self.asked: list = []
        self.hold_after = hold_after
        self.held = asyncio.Event()

    def wrap(self, fn, key):
        async def wrapped(*args, **kwargs):
            self.asked.append(key(*args, **kwargs))
            if self.hold_after is not None and len(self.asked) > self.hold_after:
                self.held.set()
                await asyncio.Event().wait()  # until the engine goes down under it
            return await fn(*args, **kwargs)
        return wrapped


async def _crawled(c, pages: int = 20):
    r = await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": pages})
    assert r.status_code == 201, r.text
    await c.post(f"/api/collections/{CID}/scrape")
    assert (await wait_job(c, CID))["state"] == "succeeded"
    assert (await c.post(f"/api/collections/{CID}/recompute")).status_code == 200


async def _suggestions(c) -> set[tuple[str, str]]:
    body = (await c.get(f"/api/collections/{CID}/suggestions")).json()
    items = body.get("items", body) if isinstance(body, dict) else body
    return {(s["type"], s["match"]) for s in items if s.get("state") == "pending"}


# ── #29 Suggest patterns ────────────────────────────────────────────────


async def test_suggest_patterns_resumes_with_the_batches_not_answered_yet(engines, monkeypatch):
    original = jobs_mod.suggest_patterns_batch
    def batch_no(llm, c, chunk, *, examples, batch_no, batches):
        return batch_no

    app, c = engines(batch=2)
    async with app.router.lifespan_context(app), c:
        await _crawled(c)
        assert (await c.post(f"/api/collections/{CID}/suggest/patterns")).status_code == 202
        whole = await wait_job(c, CID)
        assert whole["state"] == "succeeded" and whole["progress"]["calls"] >= 5
        expected = await _suggestions(c)
        first = Calls(hold_after=2)  # the third batch is in flight when the engine goes down
        monkeypatch.setattr(jobs_mod, "suggest_patterns_batch", first.wrap(original, batch_no))
        r = await c.post(f"/api/collections/{CID}/suggest/patterns")
        assert r.status_code == 202
        job_id = r.json()["id"]
        await asyncio.wait_for(first.held.wait(), 10)
        await asyncio.sleep(0.2)
    second = Calls()
    monkeypatch.setattr(jobs_mod, "suggest_patterns_batch", second.wrap(original, batch_no))
    app, c = engines(batch=2)
    async with app.router.lifespan_context(app), c:
        job = await wait_job(c, CID)
        assert job["id"] == job_id and job["state"] == "succeeded", job
        p = job["progress"]
        assert p["restarts"] == 1 and p["done"] == p["total"] == p["calls"] == whole["progress"]["calls"]
        assert await _suggestions(c) == expected  # the same suggestions as the uninterrupted run
    answered_before = set(first.asked[:2])
    assert not answered_before & set(second.asked)  # finished batches are not asked again
    assert set(first.asked) | set(second.asked) == set(range(1, p["calls"] + 1))
    assert len(second.asked) == p["calls"] - 2  # the rest, the one in flight included, once each


async def test_suggest_patterns_stops_when_its_batches_changed_under_the_restart(engines, monkeypatch):
    original = jobs_mod.suggest_patterns_batch
    first = Calls(hold_after=1)
    app, c = engines(batch=2)
    async with app.router.lifespan_context(app), c:
        await _crawled(c)
        monkeypatch.setattr(jobs_mod, "suggest_patterns_batch", first.wrap(original, lambda *a, **k: k["batch_no"]))
        assert (await c.post(f"/api/collections/{CID}/suggest/patterns")).status_code == 202
        await asyncio.wait_for(first.held.wait(), 10)
    monkeypatch.setattr(jobs_mod, "suggest_patterns_batch", original)
    app, c = engines(batch=3)  # a deploy changed the batch size
    async with app.router.lifespan_context(app), c:
        job = await wait_job(c, CID)
    assert job["state"] == "failed" and "changed while it was interrupted" in job["error"], job


async def test_suggest_patterns_builds_the_same_batches_every_time(engines):
    """A resume finds its batches by number (done_batches), so two builds on the same data must give
    the same batches in the same order — the job's own steps: pending URLs, variants folded, batched."""
    from sde_curation.engine.urls import batches, dedupe_variants

    app, c = engines(batch=2)
    async with app.router.lifespan_context(app), c:
        await _crawled(c)
        db = app.state.db

        async def build():
            pending = await db.pending_urls_for_patterns(CID)
            titles = dict(pending)
            return batches([{"url": u, "scraped_title": titles.get(u)} for u in dedupe_variants([u for u, _ in pending])],
                           app.state.settings.llm_pattern_batch_urls)

        first = await build()
        assert len(first) >= 4 and first == await build()


# ── #30 Regenerate titles ───────────────────────────────────────────────


async def test_regenerate_titles_resumes_without_asking_fixed_groups_again(engines, monkeypatch):
    from sde_curation.models import DumpUrl
    original = jobs_mod.suggest_distinct_titles

    def shared(llm, docs, **kw):
        return kw["shared_title"]

    groups = ["Alpha", "Beta", "Gamma", "Delta"]
    app, c = engines()
    async with app.router.lifespan_context(app), c:
        db = app.state.db
        assert (await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 20})).status_code == 201
        pages = {f"/{g.lower()}{i}": g for g in groups for i in range(2)} | {"/solo": "Solo"}
        await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=f"https://{CID}{p}", scraped_title=t,
                                            full_text=f"body of {p}") for p, t in pages.items()])
        assert (await c.post(f"/api/collections/{CID}/recompute")).status_code == 200
        assert (await db.duplicate_title_counts(CID))["titles"] == len(groups)
        first = Calls(hold_after=2)
        monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", first.wrap(original, shared))
        r = await c.post(f"/api/collections/{CID}/suggest/titles")
        assert r.status_code == 202, r.text
        job_id = r.json()["id"]
        await asyncio.wait_for(first.held.wait(), 10)
        await asyncio.sleep(0.2)
    second = Calls()
    monkeypatch.setattr(jobs_mod, "suggest_distinct_titles", second.wrap(original, shared))
    app, c = engines()
    async with app.router.lifespan_context(app), c:
        job = await wait_job(c, CID)
        assert job["id"] == job_id and job["state"] == "succeeded", job
        p = job["progress"]
        assert p["restarts"] == 1 and p.get("title_pass_index", 0) <= app.state.settings.llm_title_passes
        assert (await app.state.db.duplicate_title_counts(CID))["delta_urls"] == 0  # every group told apart
    fixed_before = set(first.asked[:2])
    assert not fixed_before & set(second.asked)  # groups fixed before the restart are not asked again
    assert set(first.asked) | set(second.asked) == set(groups)
