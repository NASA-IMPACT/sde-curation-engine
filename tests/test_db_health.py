import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.db import Database
from sde_curation.models import Collection, Division, JobKind, JobRun, JobState, Status
from sde_curation.web.app import create_app


@pytest.fixture
async def db(tmp_path):
    d = await Database(tmp_path / "t.db").connect()
    yield d
    await d.close()


async def test_status_transitions_and_history(db):
    await db.insert_collection(
        Collection(collection_id="c1", name="c1", seed_url="https://c1.org",
                   division=Division.GENERAL, connector="crawler2", max_pages=5)
    )
    await db.set_status("c1", Status.SCRAPED, note="scrape ok")
    with pytest.raises(ValueError):
        await db.set_status("c1", Status.LIVE)
    hist = await db.status_history("c1")
    assert [h.new_status for h in hist] == [Status.BACKLOG, Status.SCRAPED]
    assert (await db.get_collection("c1")).status is Status.SCRAPED


async def test_job_roundtrip(db):
    await db.insert_collection(
        Collection(collection_id="c1", name="c1", seed_url="https://c1.org",
                   division=Division.GENERAL, connector="crawler2", max_pages=5)
    )
    j = await db.insert_job(JobRun(collection_id="c1", kind=JobKind.SCRAPE))
    j.progress = {"docs": 3}
    await db.finish_job(j, JobState.FAILED, error="boom")
    got = await db.get_job(j.id)
    assert got.state is JobState.FAILED and got.error == "boom" and got.progress == {"docs": 3}
    assert await db.active_jobs() == []


async def test_health(tmp_path):
    app = create_app(Settings(data_dir=tmp_path))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        r = await c.get("/health")
    assert r.status_code == 200 and r.json() == {"ok": True, "db": "ok"}
