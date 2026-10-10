"""#34: Recompute, bulk AI accept and bulk suggestions accept carry on across an engine restart, and
the result equals a run that was never interrupted.

Two twin collections are crawled the same way (the fake crawler's pages are relative to the seed).
The first runs the bulk change without a break. On the second, the change is held at one of its
internal steps, the engine goes down under it, and a new engine resumes the same job. Then the
rules, the delta rows, the rule effects, the suggestions and the collection's state of the two
collections must be the same, with the host names taken out.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.curation import CurationService
from sde_curation.db import Database
from sde_curation.web.app import create_app
from tests.support.flows import FAKE_RUN_PY, wait_job

CID, REF = "ex.org", "ref.org"


@pytest.fixture
def engines(tmp_path):
    croot = tmp_path / "crawler"; croot.mkdir(); (croot / "run.py").write_text(FAKE_RUN_PY)
    base = {"data_dir": tmp_path / "data", "crawler_root": croot, "crawler_python": Path(sys.executable),
            "scrape_poll_interval_s": 0.05, "llm_provider": "fake", "llm_retry_delay_s": 0,
            "resume_start_delay_s": 0, "resume_stagger_s": 0, "bulk_job_min_urls": 5}

    def engine():
        settings = Settings(**base)
        settings.llm_pattern_batch_urls = 2  # below the setting's minimum: several suggestions from 8 pages
        app = create_app(settings)
        return app, AsyncClient(transport=ASGITransport(app=app), base_url="http://t")

    return engine


class Hold:
    """Wraps a method: its `nth` call on CID never returns, until the engine goes down under it."""

    def __init__(self, nth: int = 1):
        self.nth, self.seen = nth, 0
        self.reached = asyncio.Event()

    def wrap(self, fn):
        async def wrapped(*args, **kwargs):
            if any(a == CID or getattr(a, "collection_id", None) == CID for a in (*args, *kwargs.values())):
                self.seen += 1
                if self.seen == self.nth:
                    self.reached.set()
                    await asyncio.Event().wait()
            return await fn(*args, **kwargs)
        return wrapped


def _plain(v: Any) -> Any:
    return v.replace(CID, "X").replace(REF, "X") if isinstance(v, str) else v


async def snapshot(db: Database, cid: str) -> dict:
    """Everything a bulk change writes, comparable across the twins (no ids, no host names)."""
    def rows(rs, drop=("collection_id",)):
        return sorted(tuple(sorted((k, _plain(v)) for k, v in r.items() if k not in drop)) for r in rs)

    c = await db.get_collection(cid)
    coll = await db.fetch("SELECT review_round FROM collections WHERE collection_id=%s", (cid,))
    return {
        "collection": (c.status, c.curation_stage, c.needs_recuration, c.dump_count, c.delta_count, coll[0]["review_round"]),
        "rules": rows([{"type": p.type, "match": p.match, "value": p.value, "source": p.source}
                       for p in await db.list_patterns(cid)]),
        # ai_job names the job that wrote the AI columns: an id, different in each twin
        "deltas": rows(await db.fetch("SELECT * FROM delta_urls WHERE collection_id=%s", (cid,)),
                       drop=("collection_id", "ai_job")),
        "effects": rows(await db.fetch(
            "SELECT e.url, e.field, p.type, p.match, p.value FROM pattern_effects e"
            " JOIN patterns p ON p.id=e.pattern_id WHERE e.collection_id=%s", (cid,))),
        "suggestions": rows([{"type": s["type"], "match": s["match"], "state": s["state"]}
                             for s in await db.list_pattern_suggestions(cid)]),
    }


async def _done(c, cid) -> dict:
    job = await wait_job(c, cid, timeout=30)
    assert job["state"] == "succeeded", job
    return job


async def _prepare(c, cid: str, kind: str) -> None:
    """Crawl, and get the collection to where the bulk change applies."""
    r = await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 10,
                                               "division": "Heliophysics"})
    assert r.status_code == 201, r.text
    await c.post(f"/api/collections/{cid}/scrape")
    await _done(c, cid)
    assert (await c.post(f"/api/collections/{cid}/recompute")).status_code == 202
    await _done(c, cid)
    r = await c.post(f"/api/collections/{cid}/patterns", json={"type": "exclude", "match": "*/p9"})  # an SME rule
    assert r.status_code == 201, r.text
    if kind == "bulk_accept":
        assert (await c.post(f"/api/collections/{cid}/suggest/metadata")).status_code == 202
        await _done(c, cid)
    if kind == "bulk_suggestions":
        assert (await c.post(f"/api/collections/{cid}/suggest/patterns")).status_code == 202
        await _done(c, cid)


async def _act(c, cid: str, kind: str) -> int:
    """Start the bulk change as a job (the collection is over bulk_job_min_urls)."""
    if kind == "recompute":
        r = await c.post(f"/api/collections/{cid}/recompute?all=true")
    elif kind == "bulk_accept":
        r = await c.post(f"/api/collections/{cid}/ai/bulk", json={"decision": "accept"})
    else:
        r = await c.post(f"/api/collections/{cid}/suggestions/bulk", json={"decision": "accept"})
    assert r.status_code == 202 and r.json()["kind"] == kind, r.text
    return r.json()["id"]


# (job kind, class, method held, which call of it on CID): each internal step of each change
STEPS = [
    ("recompute", Database, "replace_deltas", 1),  # inside the recompute, before its write
    ("recompute", Database, "set_review_round", 1),  # recomputed, stage set, round not opened yet
    ("recompute", Database, "audit", 1),  # everything done but the audit line and the job's end
    ("bulk_accept", CurationService, "_recompute", 1),  # rules written, no recompute yet
    ("bulk_accept", Database, "clear_delta_ai_field", 1),  # recomputed, no AI value cleared
    ("bulk_accept", Database, "clear_delta_ai_field", 2),  # one field's AI values cleared, not the other
    ("bulk_accept", Database, "audit", 1),  # everything cleared: nothing left to accept on resume
    ("bulk_suggestions", CurationService, "_recompute", 1),  # rules written, no recompute yet
    ("bulk_suggestions", Database, "set_pattern_suggestions_state", 1),  # recomputed, still 'pending'
    ("bulk_suggestions", Database, "audit", 1),  # marked accepted: nothing pending on resume
]


@pytest.mark.parametrize(("kind", "cls", "method", "nth"), STEPS,
                         ids=[f"{k}-{m}-{n}" for k, _, m, n in STEPS])
async def test_a_bulk_change_interrupted_at_any_step_resumes_to_the_same_result(engines, monkeypatch, kind, cls, method, nth):
    original = getattr(cls, method)
    app, c = engines()
    async with app.router.lifespan_context(app), c:
        for cid in (REF, CID):
            await _prepare(c, cid, kind)
        await _act(c, REF, kind)
        whole = await _done(c, REF)
        hold = Hold(nth)
        monkeypatch.setattr(cls, method, hold.wrap(original))
        job_id = await _act(c, CID, kind)
        await asyncio.wait_for(hold.reached.wait(), 15)
        # the request's arguments are on the job row, so a new engine can build the same change
        assert "request" in (await app.state.db.get_job(job_id)).progress
    monkeypatch.setattr(cls, method, original)
    app, c = engines()
    async with app.router.lifespan_context(app), c:
        job = await _done(c, CID)
        assert job["id"] == job_id and job["progress"]["restarts"] == 1, job
        assert job["progress"]["curation"] == whole["progress"]["curation"].replace(REF, CID)
        db = app.state.db
        got, want = await snapshot(db, CID), await snapshot(db, REF)
    assert got == want
    assert want["deltas"] and want["rules"]  # the twins had something to compare


async def test_a_bulk_job_from_an_engine_that_stored_no_arguments_fails_as_before(engines):
    """A bulk job left running by an engine from before #34 has no stored arguments: it cannot be
    rebuilt, and fails with the old message."""
    from sde_curation.models import JobKind, JobRun, JobState

    app, c = engines()
    async with app.router.lifespan_context(app), c:
        await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 5})
        job = await app.state.db.insert_job(JobRun(collection_id=CID, kind=JobKind.BULK_ACCEPT, state=JobState.RUNNING,
                                                   progress={"curation": "accepting the AI suggestions"}))
    app, c = engines()
    async with app.router.lifespan_context(app), c:
        got = await wait_job(c, CID)
    assert got["id"] == job.id and got["state"] == "failed" and got["error"] == "engine restarted while job was running"
