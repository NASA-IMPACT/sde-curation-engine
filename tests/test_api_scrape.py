"""POST /scrape end-to-end through the JobManager with the fake crawler."""

from sde_curation.engine.text import content_hash
from tests.conftest import classify, wait_job


async def test_scrape_success_ingests_dump_and_sets_status(crawler_client):
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    r = await crawler_client.post("/api/collections/ex.org/scrape")
    assert r.status_code == 202 and r.json()["state"] == "running"
    # second start while running → 409, and the collection cannot be deleted under a running job
    assert (await crawler_client.post("/api/collections/ex.org/scrape")).status_code == 409
    assert (await crawler_client.delete("/api/collections/ex.org")).status_code == 409

    job = await wait_job(crawler_client, "ex.org")
    assert job["state"] == "succeeded" and job["progress"]["docs"] == 8, job
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "scraped" and c["dump_count"] == 8
    dump = (await crawler_client.get("/api/collections/ex.org/dump?limit=3")).json()
    assert dump["total"] == 8 and len(dump["items"]) == 3 and "full_text" not in dump["items"][0]
    assert (await crawler_client.get("/api/collections/ex.org/dump?q=p7")).json()["total"] == 1
    hist = (await crawler_client.get("/api/collections/ex.org/history")).json()
    assert hist[-1]["new_status"] == "scraped" and "8 documents" in hist[-1]["note"]


async def test_scrape_failure_is_visible(crawler_client):
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 13})
    await crawler_client.post("/api/collections/ex.org/scrape")
    job = await wait_job(crawler_client, "ex.org")
    assert job["state"] == "failed" and "boom" in job["error"]
    assert (await crawler_client.get("/api/collections/ex.org")).json()["status"] == "backlog"
    page = await crawler_client.get("/")
    assert "j-failed" in page.text and "Scrape</button>" in page.text  # failed → back to the Scrape action


async def test_rescrape_of_live_collection_flags_recuration(crawler_client):
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 5})
    await crawler_client.post("/api/collections/ex.org/scrape")
    await wait_job(crawler_client, "ex.org")
    await crawler_client.post("/api/collections/ex.org/recompute")
    await classify(crawler_client)
    assert (await crawler_client.post("/api/collections/ex.org/promote")).status_code == 200
    for s in ("config_generated", "live"):
        assert (await crawler_client.post("/api/collections/ex.org/status", json={"status": s})).status_code == 200
    await crawler_client.post("/api/collections/ex.org/scrape")
    await wait_job(crawler_client, "ex.org")
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "scraped" and c["needs_recuration"] is True and c["delta_count"] == 0


async def test_ingest_strips_nul_bytes(crawler_client):
    """PDF text extraction can emit NUL, which Postgres text rejects: the whole load used to fail."""
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 5})
    jobs = crawler_client.app.state.jobs
    docs = [{"url": "https://ex.org/a.pdf", "title": "A\x00", "full_text": "x\x00y", "content_type": "application/pdf"}]
    failures = [{"url": "https://ex.org/b", "reason": "fail", "detail": "bad\x00byte"}]
    assert await jobs.ingest_dump("ex.org", docs, failures) == 1
    dump = await jobs.db.load_dump("ex.org")
    assert dump[0].scraped_title == "A" and dump[0].content_hash == content_hash("xy")


async def test_ingest_keeps_one_spelling_per_page(crawler_client):
    """A site that links to the same page as http/https, with/without a trailing slash, with a
    #fragment, or under a path that redirects to it gets it crawled once per spelling; curators
    must see it once, under the preferred spelling. A URL alone on its page is not touched."""
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 5})
    jobs = crawler_client.app.state.jobs
    docs = [
        {"url": "http://ex.org/a", "title": "A (http)", "full_text": "a"},
        {"url": "https://ex.org/a", "title": "A", "full_text": "a"},
        {"url": "https://ex.org/map/", "title": "Map (slash)", "full_text": "m"},
        {"url": "https://ex.org/map", "title": "Map", "full_text": "m"},
        {"url": "https://ex.org/faq#top", "title": "FAQ", "full_text": "f"},
        {"url": "https://ex.org/faq", "title": "FAQ", "full_text": "f"},
        {"url": "https://ex.org/maps", "final_url": "https://ex.org/map", "title": "Map", "full_text": "m"},  # redirect
        {"url": "https://www.ex.org/map/", "title": "Map (www)", "full_text": "m"},
        {"url": "http://ex.org/only-http", "title": "B", "full_text": "b"},
        {"url": "https://ex.org/only-slash/", "title": "C", "full_text": "c"},
    ]
    assert await jobs.ingest_dump("ex.org", docs) == 5
    dump = (await crawler_client.get("/api/collections/ex.org/dump?limit=10")).json()
    assert sorted(i["url"] for i in dump["items"]) == [
        "http://ex.org/only-http", "https://ex.org/a", "https://ex.org/faq", "https://ex.org/map", "https://ex.org/only-slash/",
    ]


async def test_dashboard_row_polls_only_while_its_job_is_live(crawler_client):
    """Regression: every row polled every 10s, so a dashboard of hundreds of collections tripped
    the WAF rate limit (CloudFront 403 for everyone behind that IP). Idle rows ride on SSE alone."""
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 40})
    await crawler_client.post("/api/collections", json={"seed_url": "https://idle.org", "name": "Idle"})
    home = (await crawler_client.get("/")).text
    assert "every 10s" not in home.split('id="collections"')[1]

    await crawler_client.post("/api/collections/ex.org/scrape")
    # the refresh swaps the whole <tr>, so the trigger follows the job's state
    row = (await crawler_client.get("/collections/ex.org/row")).text
    assert '<tr id="row-' in row and 'hx-swap="outerHTML"' in row and "every 10s" in row
    assert "every 10s" not in (await crawler_client.get("/collections/idle.org/row")).text

    await wait_job(crawler_client, "ex.org")
    assert "every 10s" not in (await crawler_client.get("/collections/ex.org/row")).text
    assert "sseReopen from:body" in (await crawler_client.get("/rows")).text


async def test_ingest_reports_page_counts_as_it_streams(crawler_client):
    """A crawl is streamed into PostgreSQL after the crawler has gone quiet — minutes of it on a
    multi-GB collection. The job reports the pages it has read instead of sitting on the crawler's
    last figure, and the scrape's own progress carries the counts through to the end."""
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    jobs = crawler_client.app.state.jobs
    seen: list[dict] = []

    async def on_progress(p):
        seen.append(p)

    docs = [{"url": f"https://ex.org/p{i}", "title": f"Page {i}", "full_text": f"t{i}"} for i in range(3)]
    assert await jobs.ingest_dump("ex.org", docs, on_progress=on_progress) == 3
    # the stream is spent and the write-out is what remains, then what the write-out dropped
    assert seen == [{"phase": "ingest", "ingested": 3}, {"phase": "ingest_store", "ingested": 3},
                    {"duplicates_dropped": 0}]

    await crawler_client.post("/api/collections/ex.org/scrape")
    job = await wait_job(crawler_client, "ex.org")
    assert job["state"] == "succeeded"
    # 8 of the 10 pages come back as documents; the other 2 are in the failures log
    assert job["progress"]["ingested"] == 8 and job["progress"]["ingest_total"] == 8
    assert job["progress"]["docs"] == 8 and job["progress"]["failures"] == 2


async def test_ingest_recycles_pool_and_curation_carries_on(crawler_client):
    """The ingest replaces the pooled connections (the one that carried the COPY holds buffers
    the size of the crawl's biggest pages for as long as it lives). Curation must not notice:
    recompute, review and promote run on the replacements."""
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    pool = crawler_client.app.state.jobs.db.pool
    opened = pool.get_stats()["connections_num"]

    await crawler_client.post("/api/collections/ex.org/scrape")
    assert (await wait_job(crawler_client, "ex.org"))["state"] == "succeeded"
    await crawler_client.post("/api/collections/ex.org/recompute")
    await classify(crawler_client)
    r = await crawler_client.post("/api/collections/ex.org/promote")
    assert r.status_code == 200, r.text
    c = (await crawler_client.get("/api/collections/ex.org")).json()
    assert c["status"] == "curated" and c["curated_count"] > 0
    assert pool.get_stats()["connections_num"] > opened  # the drain opened fresh connections


async def test_ingest_chunks_by_text_size_without_losing_pages(crawler_client, monkeypatch):
    """A chunk closes at a page count or a text size, whichever comes first (a run of 1 MB pages
    made 500 MB chunks). The page that crosses the size limit ends its chunk; none is dropped or
    read twice at the boundary."""
    from sde_curation import jobs as jobs_mod

    monkeypatch.setattr(jobs_mod, "_INGEST_BATCH_BYTES", 250)
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    jobs = crawler_client.app.state.jobs
    sizes = [100, 300, 10, 10, 10, 240, 5, 1000, 1]  # chunks: [100,300] [10,10,10,240] [5,1000] [1]
    docs = [{"url": f"https://ex.org/p{i}", "title": f"P{i}", "full_text": f"{i}" * n} for i, n in enumerate(sizes)]
    assert await jobs.ingest_dump("ex.org", docs) == len(sizes)
    dump = {d.url: d for d in await jobs.db.load_dump("ex.org")}
    assert set(dump) == {d["url"] for d in docs}
    assert all(dump[d["url"]].content_hash == content_hash(d["full_text"]) for d in docs)


async def test_scrape_reports_duplicate_links_dropped(crawler_client):
    """A crawl that reached each page under two links stores it once. The job says how many it
    read and how many duplicate links it dropped, or 18 read → 9 stored looks like lost pages."""
    await crawler_client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 11})
    await crawler_client.post("/api/collections/ex.org/scrape")
    job = await wait_job(crawler_client, "ex.org")
    assert job["state"] == "succeeded", job
    p = job["progress"]
    assert (p["ingested"], p["docs"], p["duplicates_dropped"]) == (18, 9, 9)
    assert (await crawler_client.get("/api/collections/ex.org")).json()["dump_count"] == 9
    hist = (await crawler_client.get("/api/collections/ex.org/history")).json()
    assert hist[-1]["note"].endswith("9 documents (18 read, 9 duplicate links dropped)")
    for page in ("/collections/ex.org?tab=activity", "/collections/ex.org"):
        assert "18 read, 9 duplicate links dropped" in (await crawler_client.get(page)).text, page
