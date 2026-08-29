"""Curation flow over the API: scrape (fake) → recompute → patterns → url edit → promote."""

import yaml

from tests.conftest import wait_job


async def setup(client, n=10):
    await client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": n})
    await client.post("/api/collections/ex.org/scrape")
    await wait_job(client, "ex.org")


async def test_recompute_requires_dump(crawler_client):
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex"})
    assert (await crawler_client.post("/api/collections/ex.org/recompute")).status_code == 409


async def test_full_flow(crawler_client):
    await setup(crawler_client)  # 8 docs (p1..p10 minus multiples of 5)
    r = await crawler_client.post("/api/collections/ex.org/recompute")
    assert r.status_code == 200 and r.json() == {"new": 8, "modified": 0, "deleted": 0, "excluded": 0}
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "curating" and c["delta_count"] == 8

    # exclude p* (all 8), force-include p2 → 7 excluded
    r = await crawler_client.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "https://ex.org/p*"})
    assert r.status_code == 201 and r.json()["deltas"]["excluded"] == 8
    r = await crawler_client.post("/api/collections/ex.org/patterns", json={"type": "include", "match": "https://ex.org/p2"})
    assert r.json()["deltas"]["excluded"] == 7
    # duplicate pattern → 409; bad value → 422
    assert (await crawler_client.post("/api/collections/ex.org/patterns", json={"type": "include", "match": "https://ex.org/p2"})).status_code == 409
    # narrow the exclude to p1 only (delete + re-add) so the rest of the flow has one exclusion
    ex_id = next(p["id"] for p in (await crawler_client.get("/api/collections/ex.org/patterns")).json() if p["type"] == "exclude")
    assert (await crawler_client.delete(f"/api/collections/ex.org/patterns/{ex_id}")).json()["excluded"] == 0
    await crawler_client.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "https://ex.org/p1"})
    assert (await crawler_client.post("/api/collections/ex.org/patterns", json={"type": "division", "match": "*", "value": "Nope"})).status_code == 422

    # title template + division for all
    await crawler_client.post("/api/collections/ex.org/patterns", json={"type": "title", "match": "*", "value": "{title} | {collection}"})
    await crawler_client.post("/api/collections/ex.org/patterns", json={"type": "division", "match": "*", "value": "Heliophysics"})
    pats = (await crawler_client.get("/api/collections/ex.org/patterns")).json()
    assert {p["type"]: p["matches"] for p in pats} == {"exclude": 1, "include": 1, "title": 8, "division": 8}
    y = yaml.safe_load((crawler_client.app.state.settings.collections_dir / "ex.org" / "patterns.yaml").read_text())
    assert len(y) == 4

    d = (await crawler_client.get("/api/collections/ex.org/deltas?q=p2")).json()
    assert d["total"] == 1 and d["items"][0]["title"] == "Page 2 | Ex" and d["items"][0]["division"] == "Heliophysics"
    assert (await crawler_client.get("/api/collections/ex.org/deltas?excluded=true")).json()["total"] == 1

    # per-URL edit = exact pattern, most specific → wins over "*"
    r = await crawler_client.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "division", "value": "Earth Science"})
    assert r.status_code == 200
    d = (await crawler_client.get("/api/collections/ex.org/deltas?q=p2")).json()["items"][0]
    assert d["division"] == "Earth Science"
    # toggling exclude twice on one URL removes it again
    await crawler_client.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p3", "type": "exclude"})
    assert (await crawler_client.get("/api/collections/ex.org/deltas?excluded=true")).json()["total"] == 2
    await crawler_client.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p3", "type": "exclude"})
    assert (await crawler_client.get("/api/collections/ex.org/deltas?excluded=true")).json()["total"] == 1

    # curate page renders
    page = await crawler_client.get("/collections/ex.org/curate?excluded=true")
    assert page.status_code == 200 and "https://ex.org/p1" in page.text and "Promote" in page.text

    # promote
    r = await crawler_client.post("/api/collections/ex.org/promote")
    assert r.status_code == 200 and r.json() == {"curated": 8, "status": "curated"}
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["curated_count"] == 8 and c["delta_count"] == 0
    assert (await crawler_client.post("/api/collections/ex.org/promote")).status_code == 409  # not curating

    # recompute after promote with nothing changed must NOT demote to curating (dead end otherwise)
    assert (await crawler_client.post("/api/collections/ex.org/recompute")).json()["new"] == 0
    assert (await crawler_client.get("/api/collections/ex.org")).json()["status"] == "curated"
    # a pattern that changes something does reopen curation; deleting it (no deltas left) returns to curated
    r = await crawler_client.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "https://ex.org/p4"})
    assert (await crawler_client.get("/api/collections/ex.org")).json()["status"] == "curating"
    pid = next(p["id"] for p in (await crawler_client.get("/api/collections/ex.org/patterns")).json() if p["match"] == "https://ex.org/p4")
    await crawler_client.delete(f"/api/collections/ex.org/patterns/{pid}")
    assert (await crawler_client.get("/api/collections/ex.org")).json()["status"] == "curated"
    # manual 'curating' with zero deltas: promote acts as "mark curated"
    await crawler_client.post("/api/collections/ex.org/status", json={"status": "curating"})
    r = await crawler_client.post("/api/collections/ex.org/promote")
    assert r.status_code == 200 and r.json() == {"curated": 8, "status": "curated"}

    # delete the division pattern → unapply: p2 keeps its exact pattern, others fall back to curated value
    div_all = next(p["id"] for p in pats if p["type"] == "division")
    r = await crawler_client.delete(f"/api/collections/ex.org/patterns/{div_all}")
    assert r.status_code == 200 and r.json()["modified"] == 0  # curated value == pattern value, so no delta
    d = (await crawler_client.get("/api/collections/ex.org/deltas?q=p2")).json()
    assert d["total"] == 0


async def test_shrunk_dump_after_promote_yields_tombstones(crawler_client):
    await setup(crawler_client, n=10)
    await crawler_client.post("/api/collections/ex.org/recompute")
    await crawler_client.post("/api/collections/ex.org/promote")
    for st in ("config_generated", "live"):
        await crawler_client.post("/api/collections/ex.org/status", json={"status": st})
    # simulate a re-crawl that lost p7..p10 and retitled p1
    db = crawler_client.app.state.db
    from sde_curation.models import DumpUrl
    rows = [DumpUrl(collection_id="ex.org", url=f"https://ex.org/p{i}", scraped_title=("Page 1 (new)" if i == 1 else f"Page {i}"))
            for i in (1, 2, 3, 4, 6)]
    await db.replace_dump("ex.org", rows)
    counts = (await crawler_client.post("/api/collections/ex.org/recompute")).json()
    assert counts == {"new": 0, "modified": 1, "deleted": 3, "excluded": 0}  # p7,p8,p9 gone; p1 retitled
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "curating"
    r = await crawler_client.post("/api/collections/ex.org/promote")
    assert r.json()["curated"] == 5


async def test_identical_recrawl_goes_straight_to_curated(crawler_client):
    await setup(crawler_client, n=10)
    await crawler_client.post("/api/collections/ex.org/recompute")
    await crawler_client.post("/api/collections/ex.org/promote")
    for st in ("config_generated", "live"):
        await crawler_client.post("/api/collections/ex.org/status", json={"status": st})
    await crawler_client.post("/api/collections/ex.org/scrape")
    await wait_job(crawler_client, "ex.org")
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "scraped" and c["needs_recuration"] is True
    assert (await crawler_client.post("/api/collections/ex.org/recompute")).json()["new"] == 0
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "curated" and c["needs_recuration"] is False


async def test_url_edit_validation_and_replace(crawler_client):
    await setup(crawler_client)
    await crawler_client.post("/api/collections/ex.org/recompute")
    r = await crawler_client.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "title"})
    assert r.status_code == 422  # value required → not a 500
    r = await crawler_client.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "division", "value": "Nope"})
    assert r.status_code == 422
    for v in ("Earth Science", "Heliophysics"):
        assert (await crawler_client.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "division", "value": v})).status_code == 200
    pats = [p for p in (await crawler_client.get("/api/collections/ex.org/patterns")).json() if p["type"] == "division"]
    assert len(pats) == 1 and pats[0]["value"] == "Heliophysics"  # replaced, not duplicated
    d = (await crawler_client.get("/api/collections/ex.org/deltas?q=p2")).json()["items"][0]
    assert d["division"] == "Heliophysics"


async def test_recompute_from_backlog_with_dump_moves_forward(crawler_client):
    await setup(crawler_client)
    r = await crawler_client.post("/api/collections/ex.org/status", json={"status": "backlog", "force": True})
    assert r.status_code == 200
    await crawler_client.post("/api/collections/ex.org/recompute")
    assert (await crawler_client.get("/api/collections/ex.org")).json()["status"] == "curating"


async def test_manual_status_cannot_skip_promote(crawler_client):
    await setup(crawler_client)
    await crawler_client.post("/api/collections/ex.org/recompute")
    r = await crawler_client.post("/api/collections/ex.org/status", json={"status": "curated"})
    assert r.status_code == 409 and "nothing has been promoted" in r.text
    await crawler_client.post("/api/collections/ex.org/promote")
    await crawler_client.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p4"})
    r = await crawler_client.post("/api/collections/ex.org/status", json={"status": "live"})
    assert r.status_code == 409 and "pending" in r.text


async def test_delete_removes_files_and_bad_step_param(crawler_client):
    await setup(crawler_client)
    d = crawler_client.app.state.settings.collections_dir / "ex.org"
    assert d.is_dir()
    assert (await crawler_client.get("/collections/ex.org?step=bogus")).status_code == 200
    assert (await crawler_client.delete("/api/collections/ex.org")).status_code == 204
    assert not d.exists()


async def test_dashboard_form_errors_render_banner(client):
    r = await client.post("/collections", data={"seed_url": "ftp://x", "name": "x"})
    assert r.status_code == 422 and "banner" in r.text and "http(s)" in r.text
    await client.post("/collections", data={"seed_url": "https://a.org", "name": "A"})
    r = await client.post("/collections", data={"seed_url": "https://a.org", "name": "A"})
    assert r.status_code == 422 and "already exists" in r.text
