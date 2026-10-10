"""The HTTP layer of the collection routes, against the real app and PostgreSQL: the status code and
reason of every refused request, what htmx gets back, which tab and step a link opens, the dashboard
and URL-table filters (SQL), the files kept beside the database, the SSE event, and the division and
name routes. The rules behind these answers are unit-tested (tests/unit/test_rules.py,
test_curation_service.py, test_g3_*.py); these tests check the routes apply them. They replace the
old tests/integration/test_api_collections.py, test_api_curation.py, test_api_scrape.py,
test_promote_selection.py, test_curation_stage.py, test_collection_division.py,
test_collection_rename.py and test_state_matrix.py (P4, TEST-STRATEGY-2026-10-09.md)."""

from __future__ import annotations

import asyncio
import json
import re

import yaml

from sde_curation.events import sse_format
from sde_curation.models import IndexRun
from tests.support.flows import prepare, wait_job

CID, BACKLOG = "ex.org", "a.org"
CRAWL_DOCUMENTS = 8  # the fake crawl of 10 pages: p5 and p10 fail
INCLUDED = CRAWL_DOCUMENTS - 1  # p1 is excluded by the rule below
TOO_LONG_NAME = "x" * 201
P1, P2 = f"https://{CID}/p1", f"https://{CID}/p2"


async def crawled(c, cid: str = CID, *, division: str = "Heliophysics", curate: bool = True) -> None:
    r = await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 10,
                                               "division": division})
    assert r.status_code == 201, r.text
    await c.post(f"/api/collections/{cid}/scrape")
    assert (await wait_job(c, cid))["state"] == "succeeded"
    if curate:
        assert (await c.post(f"/api/collections/{cid}/recompute")).status_code == 200


async def world(c) -> None:
    """a.org: created, never crawled. ex.org: crawled and curating, p1 excluded by a rule, p2's
    division set by a rule, no document type yet."""
    r = await c.post("/api/collections", json={"seed_url": f"https://{BACKLOG}", "name": "Alpha"})
    assert r.status_code == 201, r.text
    await crawled(c)
    assert (await c.post(f"/api/collections/{CID}/patterns", json={"type": "exclude", "match": "*/p1"})).status_code == 201
    r = await c.post(f"/api/collections/{CID}/patterns", json={"type": "division", "match": "*/p2", "value": "Earth Science"})
    assert r.status_code == 201, r.text


A, X = f"/api/collections/{BACKLOG}", f"/api/collections/{CID}"
REFUSALS = [  # (method, path, json body or form, status, a fragment of the reason or None)
    ("POST", "/api/collections", {"seed_url": f"https://{CID}", "name": "dup"}, 409, "already exists"),
    ("POST", "/api/collections", {"seed_url": "ftp://x", "name": "bad"}, 422, None),
    ("POST", "/api/collections", {"seed_url": "https://x.org", "name": "bad", "collection_id": ".."}, 422, None),
    ("GET", "/api/collections/nope", None, 404, None),
    ("GET", "/collections/nope", None, 404, None),
    ("GET", f"/collections/{CID}/urls/nope", None, 404, None),
    ("DELETE", "/api/collections/nope", None, 404, None),
    ("POST", f"{A}/status", {"status": "live"}, 409, None),
    ("POST", f"{A}/status", {"status": "scraped"}, 409, "scrape first"),
    ("POST", f"{A}/recompute", None, 409, "scrape first"),
    ("POST", f"{A}/stage", {"stage": "metadata"}, 409, "only apply while curating"),
    ("POST", f"{X}/stage", {"stage": "bogus"}, 422, None),
    ("POST", f"{X}/status", {"status": "curated"}, 409, "nothing has been promoted"),
    ("POST", f"{X}/promote", None, 409, f"{INCLUDED} delta URLs cannot be promoted yet ({INCLUDED} without a document type)"),
    ("POST", f"{X}/promote/urls", {"urls": []}, 422, None),
    ("POST", f"{X}/promote/urls", {"urls": [f"https://{CID}/nope"]}, 409, "no longer delta URLs"),
    ("POST", f"{X}/promote/urls", {"urls": P2}, 409, "1 delta URL cannot be promoted yet"),  # one ticked box: a string
    ("POST", f"{X}/urls", {"url": P2, "type": "title"}, 422, None),
    ("POST", f"{X}/urls", {"url": P2, "type": "division", "value": "Nope"}, 422, None),
    ("POST", f"{X}/patterns", {"type": "exclude", "match": "*/p1"}, 409, None),
    ("POST", f"{X}/patterns", {"type": "division", "match": "*", "value": "Nope"}, 422, None),
    ("POST", f"{X}/ai/bulk", {"decision": "accept", "field": "division"}, 409, "no AI division suggestions"),
    ("POST", f"{X}/name", {"name": "   "}, 422, None),
    ("POST", f"{X}/name", {"name": TOO_LONG_NAME}, 422, None),
    ("POST", f"{X}/division", {"division": "Nope"}, 422, None),
    ("FORM", "/collections", {"seed_url": "ftp://x", "name": "x"}, 422, "http(s)"),
    ("FORM", "/collections", {"seed_url": f"https://{BACKLOG}", "name": "A"}, 422, "already exists"),
]


async def test_each_refused_request_gets_its_status_code_and_reason_and_changes_nothing(crawler_client):
    c = crawler_client
    await world(c)
    before = {cid: (await c.get(f"/api/collections/{cid}")).json() for cid in (BACKLOG, CID)}
    for method, path, body, status, reason in REFUSALS:
        if method == "FORM":
            r = await c.post(path, data=body)
        else:
            r = await c.request(method, path, json=body)
        assert r.status_code == status, f"{method} {path} {body}: {r.status_code} {r.text[:300]}"
        if reason:
            assert reason in r.text, f"{method} {path} {body}: {r.text[:300]}"
        if method == "FORM":
            assert 'class="banner' in r.text, f"{path} {body}: the dashboard shows no error banner"
    for cid, was in before.items():
        now = (await c.get(f"/api/collections/{cid}")).json()
        assert {k: now[k] for k in ("status", "delta_count", "curated_count", "name", "division")} == {
            k: was[k] for k in ("status", "delta_count", "curated_count", "name", "division")}, cid
    assert sorted(x["collection_id"] for x in (await c.get("/api/collections")).json()) == [BACKLOG, CID]


async def test_htmx_actions_answer_with_the_row_redirect_or_refresh_the_page_needs(crawler_client):
    """An htmx button on a dashboard row swaps the row; `?then=` navigates (kept whole, query
    string included); anywhere else the page refreshes. A JSON caller gets plain JSON."""
    c = crawler_client
    await world(c)
    hx = {"HX-Request": "true"}

    r = await c.post(f"{X}/status", json={"status": "scraped"}, headers={**hx, "HX-Target": "row-ex_org"})
    assert r.status_code == 200 and 'id="row-ex_org"' in r.text and "scraped" in r.text, r.text[:300]
    row = (await c.get(f"/collections/{CID}/row")).text
    assert "then=/collections/ex.org%3Ftab%3Ddump" in row, "the row's Start curating does not keep its ?then= target"
    r = await c.post(f"{X}/recompute?then=%2Fcollections%2Fex.org%3Ftab%3Ddelta", headers=hx)
    assert r.headers["HX-Redirect"] == "/collections/ex.org?tab=delta"
    r = await c.post(f"{X}/status?then=/x", json={"status": "scraped"}, headers=hx)
    assert (r.status_code, r.headers["HX-Redirect"]) == (200, "/x")
    r = await c.post(f"{X}/status", json={"status": "curating"}, headers=hx)
    assert r.headers["HX-Refresh"] == "true"
    r = await c.post(f"{X}/status", json={"status": "scraped", "note": "manual"})
    assert (r.status_code, r.json()["status"], "HX-Refresh" in r.headers) == (200, "scraped", False)
    history = (await c.get(f"{X}/history")).json()
    assert (history[-1]["new_status"], history[-1]["note"]) == ("scraped", "manual")
    r = await c.delete(A, headers=hx)
    assert (r.status_code, r.headers["HX-Redirect"]) == (200, "/")


PAGES = [  # (path, status, must contain, must not contain) on a.org (never crawled) and ex.org (curating)
    (f"/collections/{BACKLOG}", 200, ['data-tab="overview"', 'data-step="backlog"', "pipeline", "Delete collection"],
     ['class="tabs"']),
    (f"/collections/{BACKLOG}?step=curating", 200, ['class="tabs"', 'data-step="curating"', 'data-tab="overview"'], []),
    (f"/collections/{BACKLOG}?step=curated", 200, ['class="tabs"', 'data-step="curated"', 'data-tab="overview"'], []),
    (f"/collections/{BACKLOG}?step=live", 200, [], ['class="tabs"']),
    (f"/collections/{BACKLOG}?step=bogus", 200, ['data-tab="overview"'], []),
    (f"/collections/{BACKLOG}?tab=dump", 200, ['class="tabs"', 'data-step="curating"', 'data-tab="dump"'], []),
    *[(f"/collections/{BACKLOG}?tab={t}", 200, [f'data-tab="{t}"'], [])
      for t in ("overview", "dump", "curate", "delta", "curated", "activity")],
    (f"/collections/{BACKLOG}?tab=patterns", 200, ['data-tab="curate"'], []),  # the old tab name
    (f"/collections/{BACKLOG}?tab=urls&set=delta", 200, ['data-tab="delta"'], []),  # old ?tab=urls&set= links
    (f"/collections/{BACKLOG}?tab=urls&set=curated", 200, ['data-tab="curated"'], []),
    (f"/collections/{BACKLOG}?tab=urls", 200, ['data-tab="dump"'], []),
    (f"/collections/{BACKLOG}?tab=activity", 200, ["Status history"], []),
    (f"/collections/{BACKLOG}?tab=bogus", 200, ['data-tab="overview"'], []),
    # the header's Next action is not in a table row: a 'closest tr' target there makes htmx drop it
    (f"/collections/{BACKLOG}/header", 200, [f"/api/collections/{BACKLOG}/scrape", 'hx-swap="none"'], ["closest tr"]),
    (f"/collections/{BACKLOG}/row", 200, [f"/api/collections/{BACKLOG}/scrape", 'hx-target="closest tr"'], []),
    ("/", 200, ['sse-connect="/events"', "Alpha"], []),
    ("/static/htmx.min.js", 200, [], []),
    (f"/collections/{CID}/step/scraped", 200, ["Last crawl"], []),
    # the collection's own division: nothing for the AI to suggest, and the workspace says why
    (f"/collections/{CID}?tab=curate", 200, ["The division is yours"], ['class="ptype division"']),
    *[(f"/collections/{CID}?tab=overview&step={s}", 200, ['id="cdiv"'], []) for s in ("backlog", "scraped", "curating")],
    *[(f"/collections/{CID}?tab=overview&step={s}", 200, ["locked — change it under Curating"], ['id="cdiv"'])
      for s in ("curated", "config_generated", "live")],
]


async def test_each_link_opens_the_tab_and_step_it_names(crawler_client):
    c = crawler_client
    await world(c)
    for path, status, present, absent in PAGES:
        r = await c.get(path)
        assert r.status_code == status, f"{path}: {r.status_code}"
        for text in present:
            assert text in r.text, f"{path}: no {text!r}"
        for text in absent:
            assert text not in r.text, f"{path}: {text!r} is there"
    r = await c.get(f"/collections/{CID}/curate?excluded=true")  # the old curation page
    assert (r.status_code, r.headers["location"]) == (302, f"/collections/{CID}?tab=delta&excluded=true")


async def test_static_files_are_cached_for_good_only_under_their_current_hash(client):
    """A deploy changes app.css: the page links it under its content hash, cached for a year; any
    other spelling must be revalidated, or browsers keep the old stylesheet."""
    home = (await client.get("/")).text
    m = re.search(r'href="(/static/app\.css\?v=[0-9a-f]{10})"', home)
    assert m, home[:800]
    for path, cache in ((m.group(1), "public, max-age=31536000, immutable"), ("/static/app.css", "no-cache"),
                        ("/static/app.css?v=stale00000", "no-cache")):
        r = await client.get(path)
        assert (r.status_code, r.headers["cache-control"]) == (200, cache), path


DASHBOARD = [  # (query parameters, the collections shown)
    ([], {"a.org", "b.org", "ex.org", "m.org", "github.com_nasa-ammos"}),
    ([("division", "Earth Science")], {"a.org"}),
    ([("division", "Earth Science"), ("division", "Heliophysics"), ("status", "backlog")], {"a.org", "b.org"}),
    ([("status", "live")], set()),
    ([("q", "beta")], {"b.org"}),
    ([("q", "nasa-ammos")], {"github.com_nasa-ammos"}),
    ([("stage", "exclusions")], {"ex.org"}),
    ([("stage", "metadata")], {"m.org"}),
    ([("status", "backlog"), ("stage", "metadata")], {"a.org", "b.org", "github.com_nasa-ammos", "m.org"}),  # OR
    ([("flag", "needs_recuration")], {"b.org"}),
    ([("flag", "needs_recuration"), ("stage", "exclusions")], set()),  # AND across facets
    ([("curator", "anonymous")], {"b.org", "ex.org", "m.org", "github.com_nasa-ammos"}),
    ([("curator", "__none__")], {"a.org"}),  # added before provenance was recorded: "Unassigned"
    ([("curator", "someone")], set()),
]
ALL = {"a.org", "b.org", "ex.org", "m.org", "github.com_nasa-ammos"}


async def test_the_dashboard_shows_exactly_the_collections_its_filters_admit(crawler_client):
    c = crawler_client
    for seed, name, division in (("https://a.org", "Alpha", "Earth Science"), ("https://b.org", "Beta", "Heliophysics"),
                                 ("https://github.com/NASA-AMMOS/", "NASA-AMMOS GitHub", "General")):
        assert (await c.post("/api/collections", json={"seed_url": seed, "name": name, "division": division})).status_code == 201
    await crawled(c)
    await crawled(c, "m.org")
    assert (await c.post("/api/collections/m.org/stage", json={"stage": "metadata"})).status_code == 200
    await c.app.state.db.set_flag("b.org", True)
    await c.app.state.db.execute("UPDATE collections SET created_by=NULL WHERE collection_id='a.org'")

    home = (await c.get("/")).text
    for facet in ('id="f-division-Earth_Science" class="cnt">1<', 'id="f-stage-exclusions" class="cnt">1<',
                  'id="f-stage-metadata" class="cnt">1<', 'id="f-flag-needs_recuration" class="cnt">1<'):
        assert facet in home, facet
    assert "https://github.com/NASA-AMMOS/" in home  # two collections on one host are told apart by the seed
    for params, shown in DASHBOARD:
        for path in ("/", "/rows"):
            text = (await c.get(path, params=params)).text
            got = {cid for cid in ALL if f'href="/collections/{cid}"' in text}
            assert got == shown, f"{path} {params}: {sorted(got)}"
            count = f"{len(shown)} of {len(ALL)}" if params else f"{len(ALL)}"
            assert re.search(rf'id="f-shown"[^>]*>{count}<', text), f"{path} {params}: the count is not {count!r}"
        if not shown:
            assert "No collections match" in (await c.get("/", params=params)).text, params


async def csv_urls(c, set_: str, **params) -> list[str]:
    r = await c.get(f"/collections/{CID}/urls/{set_}", params={"format": "csv", **params})
    assert r.headers["content-type"].startswith("text/csv"), r.headers
    head, *rows = r.text.strip().splitlines()
    url_column = head.split(",").index("url")
    return sorted(row.split(",")[url_column] for row in rows)


DUMP = sorted(f"https://{CID}/p{i}" for i in (1, 2, 3, 4, 6, 7, 8, 9))
URL_FILTERS = [  # (set, query parameters, the URLs listed)
    ("dump", {}, DUMP),
    ("dump", {"q": "p2"}, [P2]),
    ("dump", {"excluded": "true"}, [P1]),
    ("delta", {}, [u for u in DUMP if u != P1]),
    ("delta", {"excluded": "false"}, [u for u in DUMP if u != P1]),
    ("delta", {"division": "Earth Science"}, [P2]),
    ("dump", {"match": f"https://{CID}/p*"}, DUMP),
    ("delta", {"match": f"https://{CID}/p*"}, [u for u in DUMP if u != P1]),
    ("dump", {"match": P1}, [P1]),
    ("dump", {"match": f"http://www.{CID}/p1/"}, [P1]),  # an exact URL finds its page under any spelling
    ("delta", {"match": f"http://www.{CID}/p2/"}, [P2]),
    ("dump", {"match": f"https://{CID}/p_"}, []),  # LIKE wildcards in a glob are literal
    ("dump", {"match": f"https://{CID}/%"}, []),
]


async def test_the_url_tables_and_their_csv_list_exactly_the_rows_their_filters_admit(crawler_client):
    c = crawler_client
    await world(c)
    for set_, params, urls in URL_FILTERS:
        assert await csv_urls(c, set_, **params) == urls, f"{set_} {params}"
    assert (await c.get(f"/collections/{CID}/urls/delta", params={"format": "csv"})).text.startswith("kind,url,excluded")
    assert (await c.get(f"/collections/{CID}/urls/dump", params={"format": "csv"})).text.startswith("url,excluded")
    dump = (await c.get(f"{X}/dump?limit=3")).json()
    assert (dump["total"], len(dump["items"]), "full_text" in dump["items"][0]) == (CRAWL_DOCUMENTS, 3, False)
    assert "No delta URLs match" in (await c.get(f"/collections/{CID}?tab=delta&per=25&page=2")).text
    page = (await c.get(f"/collections/{CID}?tab=dump&match=https://{CID}/p*")).text
    assert f"rule <code>https://{CID}/p*</code>" in page

    await c.post(f"{X}/suggest/metadata")
    assert (await wait_job(c, CID))["state"] == "succeeded"
    assert len(await csv_urls(c, "delta", ai="title")) == INCLUDED
    assert (await c.post(f"{X}/ai/bulk", json={"decision": "reject", "field": "title"})).status_code == 200
    assert await csv_urls(c, "delta", ai="title") == []
    assert len(await csv_urls(c, "delta", ai="document_type")) == INCLUDED
    assert (await c.post(f"{X}/ai/bulk", json={"decision": "accept"})).status_code == 200
    assert (await c.post(f"{X}/promote")).status_code == 200
    assert len(await csv_urls(c, "curated", match=f"https://{CID}/p*")) == INCLUDED
    assert await csv_urls(c, "curated", match=f"https://{CID}/p9") == [f"https://{CID}/p9"]


async def test_the_collection_files_follow_the_collection(crawler_client):
    """collection.yaml and patterns.yaml sit beside the database, git-trackable; a delete takes them."""
    c = crawler_client
    await world(c)
    folder = c.app.state.settings.collections_dir / CID
    y = yaml.safe_load((folder / "collection.yaml").read_text())
    assert (y["seed_url"], y["status"], y["curation_stage"]) == (f"https://{CID}", "curating", "exclusions")
    assert [p["match"] for p in yaml.safe_load((folder / "patterns.yaml").read_text())] == ["*/p1", "*/p2"]
    assert (await c.post(f"{X}/name", json={"name": "Renamed"})).status_code == 200
    assert yaml.safe_load((folder / "collection.yaml").read_text())["name"] == "Renamed"

    assert (await c.delete(X)).status_code == 204
    assert (folder.exists(), (await c.get(X)).status_code, (await c.delete(X)).status_code) == (False, 404, 404)


async def test_a_status_change_reaches_every_open_browser_as_an_event(crawler_client):
    c = crawler_client
    await world(c)
    got: list[dict] = []

    async def listen():
        async for msg in c.app.state.bus.subscribe():
            got.append(sse_format(msg))
            if got[-1]["event"] == "collection":
                break

    task = asyncio.create_task(listen())
    await asyncio.sleep(0)  # let the subscriber register
    await c.post(f"{X}/status", json={"status": "scraped"})
    await asyncio.wait_for(task, 2)

    data = json.loads(got[-1]["data"])
    assert (data["collection_id"], data["status"]) == (CID, "scraped")


async def audit_actions(c, cid: str) -> list[str]:
    return [a["action"] for a in (await c.get(f"/api/collections/{cid}/audit")).json()]


async def test_a_new_division_is_applied_and_clears_the_division_suggestions_it_makes_moot(crawler_client):
    c = crawler_client
    await crawled(c, division="General")
    await c.post(f"{X}/suggest/metadata")
    assert (await wait_job(c, CID))["state"] == "succeeded"

    r = await c.post(f"{X}/division", json={"division": "Heliophysics"})

    assert (r.status_code, r.json()["division"]) == (200, "Heliophysics")
    rows = (await c.get(f"{X}/delta?limit=100")).json()["items"]
    assert {(d["division"], d["division_ai"]) for d in rows} == {("Heliophysics", None)}
    assert all(d["title_ai"] and d["document_type_ai"] for d in rows), "the run's other suggestions went too"
    assert (await c.post(f"{X}/ai/bulk", json={"decision": "accept", "field": "division"})).status_code == 409
    assert "collection.division" in await audit_actions(c, CID)


async def test_a_division_change_on_a_promoted_collection_reopens_curation(crawler_client):
    c = crawler_client
    await prepare(c)  # Heliophysics, promoted
    curated = (await c.get(f"{X}/curated?limit=100")).json()["items"]

    assert (await c.post(f"{X}/division", json={"division": "Earth Science"})).status_code == 200

    rows = (await c.get(f"{X}/delta?limit=100")).json()["items"]
    assert len(rows) == len(curated) and {(d["kind"], d["division"]) for d in rows} == {("modified", "Earth Science")}
    assert (await c.get(X)).json()["status"] == "curating"
    assert "Earth Science" in (await c.get(f"/collections/{CID}?tab=overview")).text


async def test_a_rename_keeps_the_id_and_reaches_titles_that_render_the_name(crawler_client):
    c = crawler_client
    await prepare(c)
    assert (await c.post(f"{X}/patterns", json={"type": "title", "match": "*", "value": "{title} | {collection}"})).status_code == 201
    assert (await c.post(f"{X}/promote")).status_code == 200

    r = await c.post(f"{X}/name", json={"name": "  NASA Applied Sciences "})

    assert r.status_code == 200, r.text
    col = (await c.get(X)).json()
    assert (col["collection_id"], col["name"], col["index_key"]) == (CID, "NASA Applied Sciences", None)
    rows = (await c.get(f"{X}/delta?limit=100")).json()["items"]
    assert rows and {(d["kind"], d["title"].rsplit(" | ", 1)[1]) for d in rows} == {("modified", "NASA Applied Sciences")}
    page = (await c.get(f"/collections/{CID}?tab=overview")).text
    assert "<code>nasa_applied_sciences</code>" in page and "The index key will follow the new name." in page
    audit = (await c.get(f"{X}/audit")).json()
    assert any(a["action"] == "collection.name" and "ex.org → NASA Applied Sciences" in (a["detail"] or "") for a in audit)
    assert "NASA Applied Sciences" in (await c.get("/")).text


async def test_a_rename_keeps_an_index_key_set_by_hand(crawler_client):
    c = crawler_client
    await world(c)
    r = await c.post(f"{X}/index-key", json={"index_key": "legacy_ex", "index_name": "Legacy Ex"})
    assert r.status_code == 200, r.text
    assert "set by hand (legacy_ex) and stays as it is" in (await c.get(f"/collections/{CID}?tab=overview")).text

    assert (await c.post(f"{X}/name", json={"name": "NASA Applied Sciences"})).status_code == 200

    col = (await c.get(X)).json()
    assert (col["name"], col["index_key"], col["index_name"]) == ("NASA Applied Sciences", "legacy_ex", "Legacy Ex")
    page = (await c.get(f"/collections/{CID}?tab=overview")).text
    assert "<code>legacy_ex</code>" in page and "nasa_applied_sciences" not in page


async def test_the_name_and_the_division_are_refused_once_an_index_run_exists(crawler_client):
    """The index carries the key, name and division of the first run, whatever its outcome."""
    c = crawler_client
    await world(c)
    await c.app.state.db.insert_index_run(IndexRun(run_id="r1", collection_id=CID, target="test", state="failed"))

    for path, body in (("name", {"name": "Example Site"}), ("division", {"division": "Earth Science"})):
        r = await c.post(f"{X}/{path}", json=body)
        assert (r.status_code, "indexed" in r.text) == (409, True), f"{path}: {r.text}"
    col = (await c.get(X)).json()
    assert (col["name"], col["division"]) == (CID, "Heliophysics")
    for step in ("curating", "config_generated"):
        page = (await c.get(f"/collections/{CID}?tab=overview&step={step}")).text
        assert ('id="cname"' in page, 'id="cdiv"' in page, "locked — indexed" in page) == (False, False, True), step
    assert not {"collection.name", "collection.division"} & set(await audit_actions(c, CID))

