"""#24: one limit on LLM calls in flight across every job in the engine (LLM_WORKERS_TOTAL), on top
of the per-job limit (LLM_WORKERS). Three Suggest-metadata jobs at once stay under the shared
limit; one job alone still runs its full per-job number of calls.
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
from tests.conftest import FAKE_RUN_PY, wait_job


class InFlight:
    """Wraps the model call: counts the calls out at once, each one held a moment."""

    def __init__(self, fn):
        self.fn, self.now, self.peak, self.calls = fn, 0, 0, 0

    async def __call__(self, *args, **kwargs):
        self.now += 1
        self.calls += 1
        self.peak = max(self.peak, self.now)
        try:
            await asyncio.sleep(0.05)
            return await self.fn(*args, **kwargs)
        finally:
            self.now -= 1


@pytest.fixture
def engine(tmp_path, monkeypatch):
    croot = tmp_path / "crawler"; croot.mkdir(); (croot / "run.py").write_text(FAKE_RUN_PY)
    probe = InFlight(jobs_mod.suggest_metadata_one)
    monkeypatch.setattr(jobs_mod, "suggest_metadata_one", probe)
    app = create_app(Settings(data_dir=tmp_path / "data", crawler_root=croot, crawler_python=Path(sys.executable),
                              scrape_poll_interval_s=0.05, llm_provider="fake", llm_retry_delay_s=0,
                              llm_workers=16, llm_workers_total=20))
    return app, AsyncClient(transport=ASGITransport(app=app), base_url="http://t"), probe


async def _crawled(c, cid: str) -> None:
    r = await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 60,
                                               "division": "Heliophysics"})
    assert r.status_code == 201, r.text
    await c.post(f"/api/collections/{cid}/scrape")
    assert (await wait_job(c, cid, timeout=30))["state"] == "succeeded"
    assert (await c.post(f"/api/collections/{cid}/recompute")).status_code == 200


async def test_three_metadata_jobs_share_one_limit_on_calls_in_flight(engine):
    app, c, probe = engine
    cids = ["a.org", "b.org", "c.org"]
    async with app.router.lifespan_context(app), c:
        for cid in cids:
            await _crawled(c, cid)
        for cid in cids:
            assert (await c.post(f"/api/collections/{cid}/suggest/metadata")).status_code == 202
        jobs = [await wait_job(c, cid, timeout=60) for cid in cids]
    assert all(j["state"] == "succeeded" and j["progress"]["done"] == 48 for j in jobs), jobs
    assert probe.calls == 3 * 48
    assert probe.peak == 20  # the shared limit, reached and never passed (3 × 16 would be 48)


async def test_one_metadata_job_alone_still_runs_its_own_limit(engine):
    app, c, probe = engine
    async with app.router.lifespan_context(app), c:
        await _crawled(c, "a.org")
        assert (await c.post("/api/collections/a.org/suggest/metadata")).status_code == 202
        job = await wait_job(c, "a.org", timeout=60)
    assert job["state"] == "succeeded" and job["progress"]["done"] == 48
    assert probe.peak == 16  # LLM_WORKERS, under the shared 20
