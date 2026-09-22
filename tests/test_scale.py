"""Behaviour added by the 2026-09-18 scale audit (docs/scale-audit-2026-09-18.md): what a big
collection needs must not change what any collection gets. Real flows through the API."""

import yaml

from tests.conftest import classify, wait_job


async def start(c, cid="ex.org", n=10):
    await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": "Ex", "max_pages": n, "division": "Heliophysics"})
    await c.post(f"/api/collections/{cid}/scrape")
    await wait_job(c, cid)


async def test_an_edit_rewrites_only_the_rows_it_changes(crawler_client):
    """replace_deltas makes the table equal to the recomputed state by writing the difference: a
    row the edit does not touch keeps its physical version (xmin); the edited row, a row a rule
    removes and a row a deleted rule brings back all show up exactly as a full rewrite gave them."""
    c, db = crawler_client, crawler_client.app.state.db
    await start(c)
    await c.post("/api/collections/ex.org/recompute")

    async def versions():
        return {r["url"]: r["v"] for r in await db.fetch("SELECT url, xmin::text AS v FROM delta_urls WHERE collection_id='ex.org'")}

    before = await versions()
    assert len(before) == 8
    await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "title", "value": "Two"})
    after = await versions()
    assert {u for u in before if before[u] != after[u]} == {"https://ex.org/p2"}
    d = (await c.get("/api/collections/ex.org/delta?q=p2")).json()["items"][0]
    assert d["title"] == "Two" and d["edited_by"]

    r = await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p3"})
    ex_id = r.json()["pattern"]["id"]
    assert "https://ex.org/p3" not in await versions() and r.json()["deltas"]["excluded"] == 1
    effects = await db.fetch("SELECT url, field FROM pattern_effects WHERE collection_id='ex.org' ORDER BY url, field")
    assert [(e["url"], e["field"]) for e in effects] == [("https://ex.org/p2", "title"), ("https://ex.org/p3", "excluded")]
    await c.delete(f"/api/collections/ex.org/patterns/{ex_id}")
    back = await versions()
    assert "https://ex.org/p3" in back and back["https://ex.org/p2"] == after["https://ex.org/p2"]
    effects = await db.fetch("SELECT url FROM pattern_effects WHERE collection_id='ex.org'")
    assert [e["url"] for e in effects] == ["https://ex.org/p2"]


async def test_inline_edit_finds_its_rule_under_any_spelling_without_loading_every_rule(crawler_client):
    c = crawler_client
    await start(c)
    await c.post("/api/collections/ex.org/recompute")
    await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "title", "value": "Two"})
    await c.post("/api/collections/ex.org/urls", json={"url": "http://www.ex.org/p2/", "type": "title", "value": "Deux"})
    await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p22", "type": "title", "value": "no p22 page"})
    rules = [(p["match"], p["value"]) for p in (await c.get("/api/collections/ex.org/patterns")).json() if p["type"] == "title"]
    assert rules == [("http://www.ex.org/p2/", "Deux"), ("https://ex.org/p22", "no p22 page")]  # one rule per page
    assert (await c.get("/api/collections/ex.org/delta?q=p2")).json()["items"][0]["title"] == "Deux"


async def test_bulk_changes_on_a_big_collection_run_as_jobs(crawler_client):
    """Over settings.bulk_job_min_urls the long requests answer 202 + a job (CloudFront gives a
    request 60 s); the result is what the request gave, and edits wait for the job."""
    c = crawler_client
    c.app.state.settings.bulk_job_min_urls = 5
    await start(c)
    r = await c.post("/api/collections/ex.org/recompute")
    assert r.status_code == 202 and r.json()["kind"] == "recompute" and r.json()["state"] == "running"
    job = await wait_job(c, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["result"]["new"] == 8
    coll = (await c.get("/api/collections/ex.org")).json()
    assert coll["status"] == "curating" and coll["delta_count"] == 8
    assert (await c.get("/api/collections/ex.org/audit")).json()[0]["action"] == "recompute"

    await c.post("/api/collections/ex.org/suggest/metadata")
    assert (await wait_job(c, "ex.org"))["state"] == "succeeded"
    r = await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept"})
    assert r.status_code == 202 and r.json()["kind"] == "bulk_accept"
    # while it runs the collection is busy, like under any job
    busy = await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p1"})
    job = await wait_job(c, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["result"]["decided"] == 16  # 8 × title + doc type
    assert busy.status_code in (201, 409)  # 409 unless the job had already finished
    assert (await c.post("/api/collections/ex.org/ai/bulk", json={"decision": "accept"})).status_code == 409  # nothing left
    # one URL's suggestions, and a small collection, are still answered in the request
    c.app.state.settings.bulk_job_min_urls = 20_000
    assert (await c.post("/api/collections/ex.org/recompute")).status_code == 200
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200


async def test_patterns_yaml_is_written_in_the_background_for_big_rule_sets(crawler_client):
    c = crawler_client
    c.app.state.patterns_file.inline_max = 0  # every rule set counts as big
    await start(c)
    await c.post("/api/collections/ex.org/recompute")
    await classify(c)  # 8 URLs × 2 fields = 16 per-URL rules (the collection's division is not the AI's)
    await c.post("/api/collections/ex.org/urls", json={"url": "https://ex.org/p2", "type": "title", "value": "Two"})
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200  # promote waits for the file
    path = c.app.state.settings.collections_dir / "ex.org" / "patterns.yaml"
    rules = yaml.safe_load(path.read_text())
    assert len(rules) == 16 and ("https://ex.org/p2", "Two") in {(r["match"], r["value"]) for r in rules}
    assert not list(path.parent.glob("*.tmp"))


async def test_rules_tab_pages_the_per_url_rules(crawler_client, monkeypatch):
    import sde_curation.web.app as webapp

    monkeypatch.setattr(webapp, "RULES_PAGE", 10)
    c = crawler_client
    await start(c)
    await c.post("/api/collections/ex.org/recompute")
    await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p9"})
    await classify(c)  # 7 URLs × 2 fields = 14 per-URL rules (the collection's division is not the AI's)
    page1 = (await c.get("/collections/ex.org?tab=rules")).text
    assert page1.count("<code>https://ex.org/") == 10 and "<code>*/p9</code>" in page1 and "1–10 of 14" in page1
    last = (await c.get("/collections/ex.org?tab=rules&rpage=2")).text
    assert last.count("<code>https://ex.org/") == 4 and "<code>*/p9</code>" in last  # glob rules on every page
    # counts are right for the rules shown, the tab badge counts them all, the API still lists all
    assert 'title="show the matching delta URLs">1</a>' in last and ">Rules" in page1
    api = (await c.get("/api/collections/ex.org/patterns")).json()
    assert len(api) == 15 and all(p["matches"] == 1 and p["in_effect"] == 1 for p in api)
    paged = (await c.get("/api/collections/ex.org/patterns?exact_limit=5&exact_offset=10")).json()
    assert [p["type"] for p in paged][:1] == ["exclude"] and len(paged) == 5


async def test_pages_poll_only_while_a_job_is_live(crawler_client):
    c = crawler_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 40})
    page = (await c.get("/collections/ex.org")).text
    assert "every 10s [jobLive()]" in page and "every 5s [jobLive()]" in page and "every 4s [jobLive()]" in page
    live = " data-job-live>"  # the marker attribute on the header / the jobs strip
    assert live not in page and live not in (await c.get("/jobs/panel")).text
    await c.post("/api/collections/ex.org/scrape")
    assert live in (await c.get("/collections/ex.org/header")).text and live in (await c.get("/jobs/panel")).text
    await wait_job(c, "ex.org")
    assert live not in (await c.get("/collections/ex.org/header")).text and live not in (await c.get("/jobs/panel")).text
    home = (await c.get("/")).text
    assert "every 10s [jobLive('#jobs-panel')]" in home and "function jobLive(" in home


async def test_documents_file_is_streamed_and_a_broken_one_fails_the_job(crawler_client):
    c = crawler_client
    await start(c, n=40)
    assert (await c.get("/api/collections/ex.org")).json()["dump_count"] == 32
    dump = (await c.get("/api/collections/ex.org/dump?limit=1")).json()["items"][0]
    assert dump["scraped_title"].startswith("Page ")
    hashes = await c.app.state.db.dump_content_hashes("ex.org")
    assert len(hashes) == 32 and all(hashes.values())
    run_py = c.app.state.settings.crawler_root / "run.py"
    for broken, message in (('{"url": "https://ex.org/x"}', "not a JSON array"), ('[{"url": "https://ex.org/x", "title": ', "not valid JSON")):
        run_py.write_text(run_py.read_text() + f"\ndocs.write_text({broken!r})\n")
        await c.post("/api/collections/ex.org/scrape")
        job = await wait_job(c, "ex.org")
        assert job["state"] == "failed" and message in job["error"], job
    assert (await c.get("/api/collections/ex.org")).json()["dump_count"] == 32  # the good dump is untouched


async def test_big_rule_sets_get_the_same_patterns_yaml_from_the_fast_writer(crawler_client):
    """Over `inline_max` rules patterns.yaml is emitted directly (PyYAML costs seconds of CPU per
    rewrite at 300k rules): it must load to exactly what PyYAML's file loads to, awkward values included."""
    c, pf = crawler_client, crawler_client.app.state.patterns_file
    await start(c)
    await c.post("/api/collections/ex.org/recompute")
    awkward = ['yes', '123', '2026-09-18', 'a: b # c', ' lead', 'quote " and \' and \\ back', 'ünï — ✓', '{title} | {collection}', '- dash', '']
    for i, v in enumerate(awkward[:-1], start=1):
        r = await c.post("/api/collections/ex.org/urls", json={"url": f"https://ex.org/p{i}", "type": "title", "value": v})
        assert r.status_code == 200, r.text
    await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p1?x=1&y=[2]"})
    path = c.app.state.settings.collections_dir / "ex.org" / "patterns.yaml"
    slow = yaml.safe_load(path.read_text())
    pf.inline_max = 0
    await pf._write("ex.org")
    fast_text = path.read_text()
    assert yaml.safe_load(fast_text) == slow and len(slow) == 10 and fast_text.startswith("- type: \"title\"")
    api = (await c.get("/api/collections/ex.org/patterns")).json()
    assert [(r["id"], r["match"], r["value"], r["created_at"]) for r in slow] == [(p["id"], p["match"], p["value"], p["created_at"]) for p in api]
