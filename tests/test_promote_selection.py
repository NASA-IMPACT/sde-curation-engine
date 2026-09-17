"""Promote a selection of delta URLs; rule match counts computed over, and linked to, the URL set
that holds the rows (delta → curated → dump) through a real `?match=` filter; a hand-typed rule
takes effect over accepted AI suggestions (newest rule wins) and the rules it beats read superseded."""

from sde_curation.models import DumpUrl
from tests.conftest import classify, wait_job

CID = "ex.org"
API = f"/api/collections/{CID}"


async def setup(c, n=10):
    await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": "Ex", "max_pages": n})
    await c.post(f"{API}/scrape"); await wait_job(c, CID)
    await c.post(f"{API}/recompute")  # 8 docs: p1..p10 minus multiples of 5


async def coll(c):
    return (await c.get(API)).json()


async def csv_rows(c, set_, **params):
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return (await c.get(f"/collections/{CID}/urls/{set_}?format=csv&{qs}")).text.splitlines()[1:]


def url(i):
    return f"https://{CID}/p{i}"


async def scope_rules(c):
    """The exclude / include rules (the metadata rules from accepted suggestions left out)."""
    return [p for p in (await c.get(f"{API}/patterns")).json() if p["type"] in ("exclude", "include")]


# ── promote a selection ──────────────────────────────────────────────────


async def test_promote_selection_flow(crawler_client):
    c = crawler_client
    await setup(c)
    assert (await coll(c))["delta_count"] == 8
    # a selection without a title, division or document type is refused, and nothing is written
    r = await c.post(f"{API}/promote/urls", json={"urls": [url(1), url(2)]})
    assert r.status_code == 409 and "2 delta URLs cannot be promoted yet" in r.json()["detail"]
    assert (await coll(c))["curated_count"] == 0 and (await coll(c))["delta_count"] == 8
    await classify(c)
    stage = (await coll(c))["curation_stage"]
    r = await c.post(f"{API}/promote/urls", json={"urls": [url(1), url(2)]})
    assert r.status_code == 200 and r.json() == {"curated": 2, "promoted": 2, "left": 6, "status": "curating"}
    k = await coll(c)
    assert k["curated_count"] == 2 and k["delta_count"] == 6 and k["status"] == "curating"
    assert k["curation_stage"] == stage, "a partial promote does not move the stage"
    # htmx json-enc sends one ticked box as a string
    assert (await c.post(f"{API}/promote/urls", json={"urls": url(3)})).json()["promoted"] == 1
    # already promoted → stale; nothing selected → 422
    r = await c.post(f"{API}/promote/urls", json={"urls": [url(1), url(4)]})
    assert r.status_code == 409 and "no longer delta URLs" in r.json()["detail"] and url(1) in r.json()["detail"]
    assert (await c.post(f"{API}/promote/urls", json={"urls": []})).status_code == 422
    # the remaining rows: the queue is empty → curated, and the whole-queue promote is now refused
    r = await c.post(f"{API}/promote/urls", json={"urls": [url(i) for i in (4, 6, 7, 8, 9)]})
    assert r.json() == {"curated": 8, "promoted": 5, "left": 0, "status": "curated"}
    k = await coll(c)
    assert k["status"] == "curated" and k["delta_count"] == 0 and k["curated_count"] == 8 and not k["needs_recuration"]
    assert (await c.post(f"{API}/promote/urls", json={"urls": [url(1)]})).status_code == 409
    activity = (await c.get(f"/collections/{CID}?tab=activity")).text
    assert "promote.urls" in activity and "5 → 8 curated, 0 left" in activity
    # the selection bar and the checkboxes render only on the delta table, only with rows
    assert 'id="promote-bar"' not in (await c.get(f"/collections/{CID}?tab=delta")).text
    await c.post(f"{API}/patterns", json={"type": "title", "match": "*", "value": "{title}!"})
    page = (await c.get(f"/collections/{CID}?tab=delta")).text
    assert 'id="promote-bar"' in page and 'class="pick" type="checkbox" name="urls"' in page and "Promote selected" in page
    assert 'name="urls"' not in (await c.get(f"/collections/{CID}?tab=curated")).text


async def test_partial_promote_leaves_rows_under_review_untouched(crawler_client):
    """The two narrowings: a row still under review keeps the text and the hash its curated
    metadata was approved with, so it stays a delta (and the index never gets unapproved text)."""
    c = crawler_client
    await setup(c)
    await classify(c)
    assert (await c.post(f"{API}/promote")).status_code == 200
    db = c.app.state.db
    old = {r["url"]: r for r in await db.fetch(
        "SELECT url, full_text, content_hash FROM curated_urls WHERE collection_id=%s", (CID,))}
    await db.replace_dump(CID, [
        DumpUrl(collection_id=CID, url=url(i), scraped_title=f"Page {i}",
                full_text="changed text" if i in (1, 2) else "text " * 5)
        for i in (1, 2, 3, 4, 6, 7, 8, 9)
    ])
    assert (await c.post(f"{API}/recompute")).json()["content_changed"] == 2
    assert (await c.post(f"{API}/promote/urls", json={"urls": [url(1)]})).json()["left"] == 1
    now = {r["url"]: r for r in await db.fetch(
        "SELECT url, full_text, content_hash FROM curated_urls WHERE collection_id=%s", (CID,))}
    assert now[url(1)]["full_text"] == "changed text" and now[url(1)]["content_hash"] != old[url(1)]["content_hash"]
    assert now[url(2)] == old[url(2)], "the unpicked row keeps its approved text and hash"
    # and its delta survives a recompute
    assert (await c.post(f"{API}/recompute")).json()["content_changed"] == 1
    assert (await c.get(f"{API}/delta")).json()["items"][0]["url"] == url(2)


async def test_promoted_tombstone_drops_row_and_effects(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post(f"{API}/patterns", json={"type": "title", "match": "*", "value": "{title}!"})
    await c.post(f"{API}/patterns", json={"type": "division", "match": "*", "value": "Heliophysics"})
    await c.post(f"{API}/patterns", json={"type": "document_type", "match": "*", "value": "Data"})
    assert (await c.post(f"{API}/promote")).status_code == 200
    db = c.app.state.db
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(i), scraped_title=f"Page {i}")
                                for i in (1, 2, 3, 6, 7, 8, 9)])  # p4 gone, crawl complete
    await c.post(f"{API}/recompute")
    d = (await c.get(f"{API}/delta")).json()["items"]
    assert [x["kind"] for x in d] == ["deleted"] and d[0]["url"] == url(4)
    assert (await c.post(f"{API}/promote/urls", json={"urls": [url(4)]})).json() == {
        "curated": 7, "promoted": 1, "left": 0, "status": "curated"}
    assert url(4) not in {r["url"] for r in (await c.get(f"{API}/curated?limit=100")).json()["items"]}
    pids = [p["id"] for p in (await c.get(f"{API}/patterns")).json()]
    assert (await db.effect_counts(CID)) == {pid: 7 for pid in pids}  # title, division, type: 7 rows each


# ── match counts and links ────────────────────────────────────────────────


async def test_rule_count_is_over_and_links_to_the_set_with_the_rows(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post(f"{API}/patterns", json={"type": "title", "match": f"https://{CID}/p*", "value": "T: {title}"})
    pats = (await c.get(f"{API}/patterns")).json()
    assert pats[0]["matches"] == 8 and pats[0]["in_effect"] == 8
    rules = (await c.get(f"/collections/{CID}/rules")).text
    assert f'href="/collections/{CID}?tab=delta&match=https%3A//{CID}/p%2A"' in rules and ">superseded<" not in rules
    assert len(await csv_rows(c, "delta", match=f"https://{CID}/p*")) == 8  # the link shows the count's rows
    # an exact rule added later wins on its URL; a glob added after that supersedes the first glob
    await c.post(f"{API}/urls", json={"url": url(2), "type": "title", "value": "Two"})
    by = {p["match"]: p for p in (await c.get(f"{API}/patterns")).json()}
    assert by[f"https://{CID}/p*"]["in_effect"] == 7 and by[url(2)]["in_effect"] == 1
    await c.post(f"{API}/patterns", json={"type": "title", "match": "*", "value": "{title}"})
    by = {p["match"]: p for p in (await c.get(f"{API}/patterns")).json()}
    assert by[f"https://{CID}/p*"]["in_effect"] == 0 and by[url(2)]["in_effect"] == 0 and by["*"]["in_effect"] == 8
    rules = (await c.get(f"/collections/{CID}/rules")).text
    assert rules.count(">superseded<") == 2
    assert (await coll(c))["delta_count"] == 8  # never promoted: every row is still a new delta
    # promote (the rest of each row's metadata typed as rules): the count is now over the curated URLs and links there
    await c.post(f"{API}/patterns", json={"type": "division", "match": "*", "value": "Heliophysics"})
    await c.post(f"{API}/patterns", json={"type": "document_type", "match": "*", "value": "Data"})
    assert (await c.post(f"{API}/promote")).status_code == 200
    rules = (await c.get(f"/collections/{CID}/rules")).text
    assert f'href="/collections/{CID}?tab=curated&match=https%3A//{CID}/p%2A"' in rules
    assert {p["match"]: p["matches"] for p in (await c.get(f"{API}/patterns")).json() if p["type"] == "title"} == {
        f"https://{CID}/p*": 8, url(2): 1, "*": 8}
    assert len(await csv_rows(c, "curated", match=f"https://{CID}/p*")) == 8


async def test_match_filter_on_every_set(crawler_client):
    c = crawler_client
    await setup(c)
    for set_ in ("dump", "delta"):
        assert len(await csv_rows(c, set_, match=f"https://{CID}/p*")) == 8
        assert len(await csv_rows(c, set_, match=f"https://{CID}/p1")) == 1
        # an exact URL finds the page under any spelling, like an exact rule does
        assert len(await csv_rows(c, set_, match=f"http://www.{CID}/p1/")) == 1
        # LIKE wildcards in the glob are literal: no dump URL has an underscore
        assert await csv_rows(c, set_, match=f"https://{CID}/p_") == []
        assert await csv_rows(c, set_, match=f"https://{CID}/%") == []
        page = (await c.get(f"/collections/{CID}?tab={set_}&match=https://{CID}/p*")).text
        assert f"rule <code>https://{CID}/p*</code>" in page and f"format=csv&q=&kind=&excluded=&division=&document_type=&ai=&changed=&edited=&renamed=&unreachable=&match=https%3A//{CID}/p%2A" in page
    await classify(c)
    assert (await c.post(f"{API}/promote")).status_code == 200
    assert len(await csv_rows(c, "curated", match=f"https://{CID}/p*")) == 8
    assert len(await csv_rows(c, "curated", match=f"https://{CID}/p9")) == 1


async def test_ai_field_filter_and_links(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post(f"{API}/suggest/metadata"); await wait_job(c, CID)
    assert len(await csv_rows(c, "delta", ai="title")) == 8
    curate = (await c.get(f"/collections/{CID}?tab=curate")).text
    assert f'href="/collections/{CID}?tab=delta&ai=title"' in curate and f'href="/collections/{CID}?tab=delta&ai=pending"' in curate
    await c.post(f"{API}/ai/bulk", json={"decision": "reject", "field": "title"})
    assert await csv_rows(c, "delta", ai="title") == []
    assert len(await csv_rows(c, "delta", ai="document_type")) == 8  # the fake always suggests a doc type


# ── the motivating case ───────────────────────────────────────────────────


async def test_hand_typed_rule_takes_effect_over_accepted_ai_titles(crawler_client):
    """Accept AI titles (one exact-URL rule each), promote, then type a title glob by hand: it is
    the newest rule, so every URL it matches becomes a delta again, and its count links there."""
    c = crawler_client
    await setup(c)
    await c.post(f"{API}/suggest/metadata"); await wait_job(c, CID)
    await c.post(f"{API}/ai/bulk", json={"decision": "accept"})  # titles, divisions and types: nothing promotes blank
    assert (await c.post(f"{API}/promote")).status_code == 200
    assert (await coll(c))["status"] == "curated"
    r = await c.post(f"{API}/patterns", json={"type": "title", "match": f"https://{CID}/p*", "value": "Hand: {title}"})
    assert r.status_code == 201 and r.json()["deltas"]["modified"] == 8
    k = await coll(c)
    assert k["status"] == "curating" and k["delta_count"] == 8
    d = (await c.get(f"{API}/delta?q=p2")).json()["items"][0]
    assert d["title"] == "Hand: Page 2" and d["edited_by"] == "mixed"  # SME title, AI division and type
    pats = (await c.get(f"{API}/patterns")).json()
    hand = next(p for p in pats if p["match"] == f"https://{CID}/p*")
    assert hand["matches"] == 8 and hand["in_effect"] == 8
    assert all(p["in_effect"] == 0 for p in pats if p["source"] == "llm" and p["type"] == "title")
    assert all(p["in_effect"] == 1 for p in pats if p["source"] == "llm" and p["type"] != "title")  # untouched
    rules = (await c.get(f"/collections/{CID}/rules")).text
    assert rules.count(">superseded<") == 8 and f'?tab=delta&match=https%3A//{CID}/p%2A"' in rules
    # promote a selection of the re-opened rows straight from the queue
    assert (await c.post(f"{API}/promote/urls", json={"urls": [url(1), url(2), url(3)]})).json()["left"] == 5


# ── the toggle on the curated table ───────────────────────────────────────


async def test_curated_toggle_is_idempotent_and_shows_the_pending_state(crawler_client):
    """Excluding a curated row: the rule flags the row excluded in place (no delta), and a
    second click keeps it excluded instead of undoing it. Include on a row a glob excludes adds
    an exact include; include on a row nothing excludes just drops the exact exclude."""
    c = crawler_client
    await setup(c)
    await classify(c)
    assert (await c.post(f"{API}/promote")).status_code == 200
    for _ in range(2):  # clicking twice must not toggle back
        assert (await c.post(f"{API}/urls", json={"url": url(2), "type": "exclude"})).json()["excluded"] == 1
    page = (await c.get(f"/collections/{CID}?tab=curated&q=p2")).text
    assert 'class="kind k-modified nowrap"' not in page and 'class="kind k-excluded"' in page and "✓ include" in page
    assert (await coll(c))["delta_count"] == 0
    # back to included: the exact exclude goes, no include rule is needed; coming back in is a delta to promote
    assert (await c.post(f"{API}/urls", json={"url": url(2), "type": "include"})).json()["excluded"] == 0
    assert await scope_rules(c) == [] and (await coll(c))["delta_count"] == 1
    assert (await c.post(f"{API}/promote")).status_code == 200
    # a glob excludes p*; include on p3 must add an exact include (and repeat clicks keep it)
    await c.post(f"{API}/patterns", json={"type": "exclude", "match": f"https://{CID}/p*"})
    for _ in range(2):
        assert (await c.post(f"{API}/urls", json={"url": url(3), "type": "include"})).json()["excluded"] == 7
    assert [(p["type"], p["match"]) for p in await scope_rules(c)] == [("exclude", f"https://{CID}/p*"), ("include", url(3))]
    # exclude p3 again: the exact include goes and the glob does the rest — no exact exclude is added
    assert (await c.post(f"{API}/urls", json={"url": url(3), "type": "exclude"})).json()["excluded"] == 8
    assert [p["type"] for p in await scope_rules(c)] == ["exclude"]
    # the whole-queue promote sits on the delta table too (a title rule gives it something to promote)
    await c.post(f"{API}/patterns", json={"type": "title", "match": "*", "value": "T"})
    await c.post(f"{API}/urls", json={"url": url(3), "type": "include"})
    assert f'hx-post="/api/collections/{CID}/promote"' in (await c.get(f"/collections/{CID}?tab=delta")).text


# ── the Rules tab ─────────────────────────────────────────────────────────


async def test_rules_tab_at_top_level(crawler_client):
    c = crawler_client
    await setup(c)
    await c.post(f"{API}/patterns", json={"type": "title", "match": "*", "value": "{title}!"})
    page = (await c.get(f"/collections/{CID}?tab=rules")).text
    assert 'aria-selected="true"' in page and ">Rules <span" in page  # its own tab, with the count
    assert "Add a metadata rule by hand" in page and "Add an exclude or include rule by hand" in page
    assert "<code>*</code></td><td>{title}!</td>" in page and '?tab=delta&match=%2A"' in page
    assert '<div id="rules-table" class="rules tbl-wrap">' in page  # app.css styles the table via .rules
    curate = (await c.get(f"/collections/{CID}?tab=curate")).text
    assert ">Curate</a>" in curate and 'href="/collections/ex.org?tab=rules"' in curate  # no count on Curate any more
    assert "Add a metadata rule by hand" in curate  # the by-hand forms stay under their stage too
    tabs = (await c.get(f"/collections/{CID}?tab=delta")).text
    assert tabs.index(">Curate<") < tabs.index(">Rules <span") < tabs.index(">Delta URLs")
