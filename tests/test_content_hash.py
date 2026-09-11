"""Content-aware deltas through the app: hash at ingest, carried at promote, flagged on re-scrape."""

from sde_curation.models import DumpUrl
from tests.conftest import wait_job

CID = "ex.org"


async def _scrape(client):
    await client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    await client.post(f"/api/collections/{CID}/scrape")
    await wait_job(client, CID)


async def test_hash_flows_from_dump_to_curated_and_flags_changed_text(crawler_client):
    c = crawler_client
    db = c.app.state.db
    await _scrape(c)
    dump = {d.url: d for d in await db.load_dump(CID)}
    assert all(len(d.content_hash) == 64 for d in dump.values())

    await c.post(f"/api/collections/{CID}/recompute")
    assert (await c.post(f"/api/collections/{CID}/promote")).status_code == 200
    cur = {r.url: r for r in await db.load_curated(CID)}
    assert cur[f"https://{CID}/p1"].content_hash == dump[f"https://{CID}/p1"].content_hash

    # re-dump: same text everywhere except p2 (whitespace-only jitter on p3 must not count)
    rows = []
    for d in dump.values():
        text = "text " * 5
        if d.url.endswith("/p2"):
            text = "brand new body"
        elif d.url.endswith("/p3"):
            text = "text\n\n text   text text text "
        rows.append(DumpUrl(collection_id=CID, url=d.url, scraped_title=d.scraped_title, full_text=text))
    await db.replace_dump(CID, rows)
    r = await c.post(f"/api/collections/{CID}/recompute")
    assert r.json() == {"new": 0, "modified": 1, "deleted": 0, "excluded": 0, "content_changed": 1}

    r = await c.get(f"/api/collections/{CID}/deltas?content_changed=true")
    assert r.json()["total"] == 1 and r.json()["items"][0]["url"].endswith("/p2")
    assert r.json()["items"][0]["content_changed"] is True and r.json()["items"][0]["kind"] == "modified"

    html = (await c.get(f"/collections/{CID}?tab=urls&set=delta&changed=true")).text
    assert "text changed" in html and "/p2" in html
    csv = (await c.get(f"/collections/{CID}/urls/delta?format=csv&changed=true")).text
    assert csv.splitlines()[0].split(",")[3] == "content_changed" and len(csv.splitlines()) == 2
    scope = (await c.get(f"/collections/{CID}?tab=curate")).text
    assert "1 text changed" in scope


async def test_rows_promoted_before_hashing_do_not_storm(client):
    """A curated row with a NULL hash compares as unchanged even when the dump now has one."""
    from sde_curation.models import CuratedUrl

    db = client.app.state.db
    await client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex"})
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=f"https://{CID}/a", scraped_title="A", full_text="body")])
    await db.replace_curated(CID, [CuratedUrl(collection_id=CID, url=f"https://{CID}/a", scraped_title="A")])
    r = await client.post(f"/api/collections/{CID}/recompute")
    assert r.json()["modified"] == 0 and r.json()["content_changed"] == 0


async def test_promote_copies_text_and_export_survives_a_recrawl(crawler_client):
    """The curated set is the source of truth: promote copies the dump text onto each row, and an
    export ships that text even after a later crawl replaced the dump."""
    from sde_curation.engine.export import export_lines

    c = crawler_client
    db = c.app.state.db
    await _scrape(c)
    dump = {d.url: d for d in await db.load_dump(CID)}
    await c.post(f"/api/collections/{CID}/recompute")
    assert (await c.post(f"/api/collections/{CID}/promote")).status_code == 200

    cur = {r.url: r for r in await db.load_curated(CID, with_text=True)}
    p1, text = f"https://{CID}/p1", "text " * 5  # what the crawler fixture puts in every page
    assert cur[p1].full_text == text and cur[p1].content_hash == dump[p1].content_hash
    # the diff/listing paths never carry the text, only its size
    assert all(r.full_text is None for r in await db.load_curated(CID))
    listed, _ = await db.list_curated(CID)
    assert {r.url: r.text_len for r in listed}[p1] == len(text)
    r = await c.get(f"/api/collections/{CID}/curated")
    assert "full_text" not in r.json()["items"][0] or r.json()["items"][0]["full_text"] is None
    assert r.json()["items"][0]["text_len"] > 0
    html = (await c.get(f"/collections/{CID}?tab=urls&set=curated")).text
    assert f">{len(text):,}<" in html
    csv = (await c.get(f"/collections/{CID}/urls/curated?format=csv")).text.splitlines()
    assert "text_len" in csv[0].split(",")

    # a re-crawl replaces the dump: the approved text is untouched and is what the export ships
    rows = [DumpUrl(collection_id=CID, url=d.url, scraped_title=d.scraped_title, full_text="brand new body")
            for d in dump.values()]
    await db.replace_dump(CID, rows)
    lines = {ln.url: ln for ln in export_lines(await db.load_curated(CID, with_text=True))}
    assert lines[p1].full_text == text != "brand new body"

    # the next promote refreshes text and hash together
    await c.post(f"/api/collections/{CID}/recompute")
    assert (await c.post(f"/api/collections/{CID}/promote")).status_code == 200
    cur = {r.url: r for r in await db.load_curated(CID, with_text=True)}
    new = {d.url: d for d in await db.load_dump(CID)}
    assert cur[p1].full_text == "brand new body" and cur[p1].content_hash == new[p1].content_hash


async def test_rows_without_text_keep_their_own_when_absent_from_the_dump(client):
    """`replace_curated` writes a row's own text and only overrides it from the dump for URLs
    the dump has (a promote never leaves a surviving row without one)."""
    from sde_curation.models import CuratedUrl

    db = client.app.state.db
    await client.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex"})
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=f"https://{CID}/a", scraped_title="A", full_text="dump a")])
    await db.replace_curated(CID, [
        CuratedUrl(collection_id=CID, url=f"https://{CID}/a", scraped_title="A", full_text="old a"),
        CuratedUrl(collection_id=CID, url=f"https://{CID}/gone", scraped_title="G", full_text="kept"),
    ])
    cur = {r.url: r.full_text for r in await db.load_curated(CID, with_text=True)}
    assert cur == {f"https://{CID}/a": "dump a", f"https://{CID}/gone": "kept"}
