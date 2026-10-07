"""Writes that land while a recompute is between loading the delta rows and writing them back.

A recompute loads every delta row, works for seconds on a large collection, then writes the rows
back. The AI suggestion columns are not the recompute's: Suggest metadata writes them, and accept /
reject clear them. Whatever one of those writes in that window must survive the recompute's write —
before the fix the recompute put back the values it had loaded (a lost suggestion, or a rejected one
that came back).

The window is opened deterministically: `load_dump_failures` is the recompute's last load before it
computes, so a hook on it runs the other write exactly between the load and the write.
"""

from __future__ import annotations

from tests.conftest import wait_job


async def _collection_with_pending_suggestions(c) -> list[dict]:
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "ex.org", "max_pages": 10})
    await c.post("/api/collections/ex.org/scrape")
    assert (await wait_job(c, "ex.org"))["state"] == "succeeded"
    assert (await c.post("/api/collections/ex.org/recompute")).status_code == 200
    assert (await c.post("/api/collections/ex.org/suggest/metadata")).status_code == 202
    assert (await wait_job(c, "ex.org"))["state"] == "succeeded"
    rows = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    pending = sorted((d for d in rows if d["title_ai"]), key=lambda d: d["url"])
    assert len(pending) >= 2
    return pending


async def _row(c, url: str) -> dict:
    rows = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    return next(d for d in rows if d["url"] == url)


def _hook_once(monkeypatch, db, write):
    """Run `write` once, right after the next recompute has loaded the delta rows."""
    original = db.load_dump_failures
    fired = []

    async def hooked(collection_id):
        if not fired:
            fired.append(True)
            await write()
        return await original(collection_id)

    monkeypatch.setattr(db, "load_dump_failures", hooked)
    return fired


async def test_a_suggestion_written_during_a_recompute_survives_it(crawler_client, monkeypatch):
    c = crawler_client
    db = c.app.state.db
    pending = await _collection_with_pending_suggestions(c)
    other, edited = pending[0]["url"], pending[1]["url"]

    async def metadata_flush():  # what a Suggest-metadata flush writes for one URL
        await db.set_delta_ai("ex.org", [{"url": other, "title": "Newer suggestion", "title_conf": "high",
                                          "division": None, "document_type": None, "model": "fake"}])

    fired = _hook_once(monkeypatch, db, metadata_flush)
    r = await c.post("/api/collections/ex.org/urls", json={"url": edited, "type": "title", "value": "By hand"})
    assert r.status_code == 200, r.text
    assert fired, "the hook did not run: the recompute no longer loads crawl failures last"
    assert (await _row(c, other))["title_ai"] == "Newer suggestion"
    assert (await _row(c, edited))["title"] == "By hand"


async def test_a_suggestion_rejected_during_a_recompute_stays_rejected(crawler_client, monkeypatch):
    c = crawler_client
    db = c.app.state.db
    pending = await _collection_with_pending_suggestions(c)
    rejected, edited = pending[0]["url"], pending[1]["url"]
    assert (await _row(c, rejected))["title_ai"] is not None

    async def reject():  # what ✕ on that row's title writes
        await db.clear_delta_ai("ex.org", rejected, "title")

    fired = _hook_once(monkeypatch, db, reject)
    r = await c.post("/api/collections/ex.org/urls", json={"url": edited, "type": "title", "value": "By hand"})
    assert r.status_code == 200, r.text
    assert fired
    row = await _row(c, rejected)
    assert row["title_ai"] is None and row["title_ai_conf"] is None


async def test_a_new_delta_row_still_carries_its_previous_suggestions(crawler_client):
    """The AI columns are still written when a row is inserted: a page that leaves the queue and
    comes back keeps nothing (as before), and an untouched row keeps its suggestions through any
    number of recomputes."""
    c = crawler_client
    pending = await _collection_with_pending_suggestions(c)
    url = pending[0]["url"]
    before = await _row(c, url)
    for _ in range(2):
        assert (await c.post("/api/collections/ex.org/recompute")).status_code == 200
    after = await _row(c, url)
    keys = ("title_ai", "division_ai", "document_type_ai", "title_ai_conf", "ai_model")
    assert {k: after[k] for k in keys} == {k: before[k] for k in keys}


def test_the_columns_a_recompute_leaves_alone_are_exactly_the_ones_it_carries_forward():
    """engine.diff copies the AI fields from the previous row; Database writes them on insert only.
    The two lists must name the same columns, or a new AI column would be overwritten again."""
    from sde_curation.db import Database
    from sde_curation.engine.diff import _AI_FIELDS

    assert set(_AI_FIELDS) == Database._AI_COLS
    assert Database._AI_COLS <= set(Database._DELTA_COLS)
