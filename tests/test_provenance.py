"""Every mutating action is attributed: per-row columns, the audit ledger, the YAML files, the
tooltips, and the notifications all say who did it; job-driven transitions say `system`."""

import yaml

from tests.conftest import add_user, login, wait_job

COLL = {"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10, "division": "Heliophysics"}


def _yaml(client, name):
    return yaml.safe_load((client.app.state.settings.collections_dir / "ex.org" / name).read_text())


async def test_create_and_status_change_are_attributed(authed_client):
    c = authed_client
    r = await c.post("/api/collections", json=COLL)
    assert r.status_code == 201 and r.json()["created_by"] == "admin"
    hist = (await c.get("/api/collections/ex.org/history")).json()
    assert hist[0]["note"] == "created" and hist[0]["actor"] == "admin"
    y = _yaml(c, "collection.yaml")
    assert y["created_by"] == "admin" and y["history"][0]["actor"] == "admin"
    # manual status change: history row, YAML history, notifier text
    await c.app.state.db.replace_dump("ex.org", [])
    r = await c.post("/api/collections/ex.org/status", json={"status": "backlog", "note": "noop", "force": True})
    assert r.status_code == 200
    hist = (await c.get("/api/collections/ex.org/history")).json()
    assert hist[-1]["actor"] == "admin" and hist[-1]["note"] == "noop"
    assert _yaml(c, "collection.yaml")["history"][-1]["actor"] == "admin"
    audit = (await c.get("/api/collections/ex.org/audit")).json()
    assert [a["action"] for a in audit] == ["status.set", "collection.create"]
    assert all(a["actor"] == "admin" for a in audit)


async def test_curation_actions_are_attributed(authed_crawler_client):
    c = authed_crawler_client
    await add_user(c, "alice", "alicepass1")
    await c.post("/api/collections", json=COLL)
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    # alice does the curation in her own session
    a = type(c)(transport=c._transport, base_url="http://t"); a.app = c.app
    await login(a, "alice", "alicepass1")
    await a.post("/api/collections/ex.org/recompute")
    r = await a.post("/api/collections/ex.org/patterns", json={"type": "division", "match": "*/p2", "value": "Earth Science"})
    assert r.status_code == 201 and r.json()["pattern"]["created_by"] == "alice"
    await a.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p3", "type": "title", "value": "T3"})
    pats = (await a.get("/api/collections/ex.org/patterns")).json()
    assert {p["created_by"] for p in pats} == {"alice"}
    assert {p["created_by"] for p in _yaml(c, "patterns.yaml")} == {"alice"}
    page = (await a.get("/collections/ex.org?tab=urls&set=delta")).text
    assert 'title="division */p2 → Earth Science (by alice)"' in page
    page = (await a.get("/collections/ex.org?tab=patterns")).text
    assert ">alice<" in page
    # AI suggestion accept/reject
    await c.app.state.db.set_delta_ai("ex.org", [{"url": "https://ex.org/p4", "division": "General"}])
    r = await a.post("/api/collections/ex.org/ai/accept", json={"url": "https://ex.org/p4", "field": "division"})
    assert r.status_code == 200
    # promote → status row by alice; deltas recomputed rows by alice
    r = await a.post("/api/collections/ex.org/promote")
    assert r.status_code == 200
    hist = (await a.get("/api/collections/ex.org/history")).json()
    by = {h["new_status"]: h["actor"] for h in hist}
    assert by["scraped"] == "system" and by["curating"] == "alice" and by["curated"] == "alice"
    assert hist[0]["actor"] == "admin"  # created
    actions = [x["action"] for x in (await a.get("/api/collections/ex.org/audit")).json()]
    assert actions == ["promote", "ai.accept", "url.edit", "pattern.add", "recompute", "scrape.start", "collection.create"]
    who = {x["action"]: x["actor"] for x in (await a.get("/api/collections/ex.org/audit")).json()}
    assert who["scrape.start"] == "admin" and who["promote"] == "alice"
    # activity tab shows the By columns and the audit trail
    page = (await a.get("/collections/ex.org?tab=activity")).text
    assert "Audit trail" in page and "pattern.add" in page and ">system<" in page


async def test_jobs_record_starter_and_system_transitions(authed_crawler_client):
    c = authed_crawler_client
    await c.post("/api/collections", json=COLL)
    r = await c.post("/api/collections/ex.org/scrape")
    assert r.status_code == 202 and r.json()["started_by"] == "admin"
    job = await wait_job(c, "ex.org")
    assert job["started_by"] == "admin"
    hist = (await c.get("/api/collections/ex.org/history")).json()
    assert hist[-1]["new_status"] == "scraped" and hist[-1]["actor"] == "system"
    y = _yaml(c, "collection.yaml")
    assert y["history"][-1]["actor"] == "system"  # written from the job, not from a request
    sent = c.app.state.notifier.sent
    assert sent[-1]["actor"] == "system" and sent[-1]["text"].split("\n")[0].endswith("(by system)")
    # cancel is attributed to the canceller
    await c.post("/api/collections/ex.org/scrape")
    r = await c.post("/api/collections/ex.org/jobs/cancel")
    assert r.status_code == 200 and r.json()["error"] == "cancelled by admin"
    actions = [x["action"] for x in (await c.get("/api/collections/ex.org/audit")).json()]
    assert actions[:2] == ["job.cancel", "scrape.start"]


async def test_delete_and_pattern_delete_survive_in_ledger(authed_crawler_client):
    c = authed_crawler_client
    await c.post("/api/collections", json=COLL)
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    await c.post("/api/collections/ex.org/recompute")
    r = await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p1"})
    pid = r.json()["pattern"]["id"]
    assert (await c.delete(f"/api/collections/ex.org/patterns/{pid}")).status_code == 200
    assert (await c.delete("/api/collections/ex.org")).status_code == 204
    rows = await c.app.state.db.list_audit("ex.org")
    actions = [x["action"] for x in rows]
    assert actions[0] == "collection.delete" and "pattern.delete" in actions
    assert rows[0]["actor"] == "admin" and "exclude */p1" in next(x["detail"] for x in rows if x["action"] == "pattern.delete")


async def test_unauthenticated_mode_uses_anonymous_actor(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json=COLL)
    assert (await c.get("/api/collections/ex.org")).json()["created_by"] == "anonymous"
    await c.post("/api/collections/ex.org/scrape"); await wait_job(c, "ex.org")
    hist = (await c.get("/api/collections/ex.org/history")).json()
    assert [h["actor"] for h in hist] == ["anonymous", "system"]


async def test_migration_leaves_old_rows_blank(tmp_path):
    """A database created before provenance existed gains the columns; old rows stay NULL."""
    import sqlite3

    from sde_curation.db import Database

    path = tmp_path / "old.db"
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE collections (collection_id TEXT PRIMARY KEY, name TEXT NOT NULL, seed_url TEXT NOT NULL,
          division TEXT NOT NULL, document_type TEXT, connector TEXT NOT NULL, max_pages INTEGER NOT NULL,
          status TEXT NOT NULL, needs_recuration INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL, dump_count INTEGER NOT NULL DEFAULT 0, delta_count INTEGER NOT NULL DEFAULT 0,
          curated_count INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE status_history (id INTEGER PRIMARY KEY AUTOINCREMENT, collection_id TEXT NOT NULL,
          old_status TEXT, new_status TEXT NOT NULL, note TEXT, at TEXT NOT NULL);
        INSERT INTO collections VALUES ('old.org','Old','https://old.org','General',NULL,'crawler2',5,'backlog',0,
          '2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00',0,0,0);
        INSERT INTO status_history (collection_id,old_status,new_status,note,at) VALUES ('old.org',NULL,'backlog','created','2026-01-01T00:00:00+00:00');
    """)
    con.commit(); con.close()
    db = await Database(path).connect()
    try:
        old = await db.get_collection("old.org")
        assert old is not None and old.created_by is None
        hist = await db.status_history("old.org")
        assert hist[0].actor is None
        await db.set_status("old.org", "scraped", note="later", force=True, actor="bob")
        assert (await db.status_history("old.org"))[-1].actor == "bob"
        assert await db.count_users() == 0
    finally:
        await db.close()
