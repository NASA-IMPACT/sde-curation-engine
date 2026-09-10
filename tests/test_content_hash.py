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
