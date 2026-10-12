"""What the curation pages and small routes show and refuse, through the real routes and PostgreSQL:
the removal warning, the way back into a promoted collection, the Rules tab pager, the index key
set by hand, the re-curation reason, and the curated count against the curated rows. Replaces page
and route checks of the old integration test_review_round, test_scale, test_curated_counts and e2e
test_index_key (TEST-STRATEGY-2026-10-09.md, P4). The crawl is written straight into the tables."""

import pytest

import sde_curation.web.app as webapp
from sde_curation.models import DumpUrl, Pattern, PatternType

CID = "ex.org"
API = f"/api/collections/{CID}"
PAGES = 8
REMOVAL_FLOOR = 5  # web/app.py removal_warning: at least five pages gone (and promote_removal_warn_ratio)


def url(i: int) -> str:
    return f"https://{CID}/p{i}"


async def crawl(c, pages) -> None:
    await c.app.state.db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(i), scraped_title=f"Page {i}",
                                                    full_text=f"text {i}") for i in pages])


async def started(c) -> None:
    r = await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10,
                                               "division": "Heliophysics"})
    assert r.status_code == 201, r.text
    await crawl(c, range(1, PAGES + 1))
    assert (await c.post(f"{API}/recompute")).status_code == 200


async def promoted(c) -> None:
    """ex.org, 8 pages, every page complete and promoted."""
    await started(c)
    r = await c.post(f"{API}/patterns", json={"type": "document_type", "match": "*", "value": "Documentation"})
    assert r.status_code == 201, r.text
    assert (await c.post(f"{API}/promote")).status_code == 200


async def counts(c) -> tuple[int, int, int, str]:
    col = (await c.get(API)).json()
    return col["curated_count"], col["curated_rows"], col["delta_count"], col["status"]


# ── the removal warning ────────────────────────────────────────────────


@pytest.mark.parametrize(("gone", "warned"), [(REMOVAL_FLOOR - 1, False), (REMOVAL_FLOOR, True)],
                         ids=["four gone: no warning", "five gone: warning"])
async def test_the_curate_tab_warns_when_a_crawl_lost_much_of_the_curated_set(client, gone, warned):
    """A crawl that lost a large share of the curated set is more likely a bad crawl."""
    c = client
    await promoted(c)
    await crawl(c, range(gone + 1, PAGES + 1))

    await c.post(f"{API}/recompute")

    page = (await c.get(f"/collections/{CID}?tab=curate")).text
    assert (f"{gone} of {PAGES} curated URLs are gone from this dump ({gone / PAGES:.0%})" in page) is warned
    assert ("re-scrape instead of promoting" in page) is warned
    assert f"{gone} removed ↗" in page


# ── a promoted collection: the way back in ─────────────────────────────


async def test_a_promoted_collection_offers_check_for_changes_and_re_curate_everything(client):
    c = client
    await promoted(c)

    curate = (await c.get(f"/collections/{CID}?tab=curate")).text
    panel = (await c.get(f"/collections/{CID}?tab=overview")).text

    assert "Check for changes" in curate and "Re-curate everything" in curate
    assert f'hx-post="/api/collections/{CID}/recompute?all=true&amp;then=' in panel  # keeps the ?then= redirect
    assert "(0 delta URLs · 0 calls)" in curate and "Start curating first" in curate  # nothing to suggest on


async def test_re_curate_everything_puts_every_page_back_with_its_promoted_values(client):
    """The stages restart at exclusions, the AI passes have the whole collection again, and the
    curated set does not move until a promote."""
    c = client
    await promoted(c)
    titles = {r["url"]: r["title"] for r in (await c.get(f"{API}/curated?limit=100")).json()["items"]}

    r = await c.post(f"{API}/recompute?all=true")

    col = (await c.get(API)).json()
    rows = (await c.get(f"{API}/delta?limit=100")).json()["items"]
    assert (r.json()["modified"], r.json()["new"]) == (PAGES, 0)
    assert (col["status"], col["curation_stage"], col["curated_count"]) == ("curating", "exclusions", PAGES)
    assert {d["url"]: d["title"] for d in rows} == titles
    assert await c.app.state.db.count_deltas_for_llm(CID) == PAGES
    assert f"re-curating: {PAGES} delta URLs queued for review" in str(await c.app.state.db.status_history(CID))


# ── the Rules tab pager ────────────────────────────────────────────────

RULES_PAGE = 10  # rules per page for the test (webapp.RULES_PAGE)
EXACT_RULES = 14  # a title and a document type on p1..p7


async def test_the_rules_tab_pages_filters_and_shows_the_last_page_for_one_past_the_end(client, monkeypatch):
    monkeypatch.setattr(webapp, "RULES_PAGE", RULES_PAGE)
    c = client
    await started(c)
    await c.app.state.db.insert_patterns(
        [Pattern(collection_id=CID, type=PatternType.EXCLUDE, match="*/p9")]
        + [Pattern(collection_id=CID, type=t, match=url(i), value=v) for i in range(1, 8)
           for t, v in ((PatternType.TITLE, f"T{i}"), (PatternType.DOCUMENT_TYPE, "Data"))])
    total = EXACT_RULES + 1

    first = (await c.get(f"/collections/{CID}?tab=rules")).text
    past_the_end = (await c.get(f"/collections/{CID}?tab=rules&rpage=99")).text
    globs = (await c.get(f"/collections/{CID}?tab=rules&rscope=glob")).text
    found = (await c.get(f"/collections/{CID}?tab=rules&rq=ex.org/p3&rper=25")).text
    desc = (await c.get(f"/collections/{CID}?tab=rules&rsort=match&rdir=desc&rper=25")).text

    assert f"1–{RULES_PAGE} of {total}" in first and "<code>*/p9</code>" in first  # unsorted: globs first
    assert 'data-page="2"' in past_the_end and past_the_end.count(f"<code>https://{CID}/") == total - RULES_PAGE
    assert "1–1 of 1" in globs and f"<code>https://{CID}/" not in globs
    assert found.count(f"<code>https://{CID}/p3") == 2 and "<code>*/p9</code>" not in found
    assert desc.index("<code>*/p9</code>") > desc.index("<code>https://")


# ── the index key set by hand ──────────────────────────────────────────


async def test_the_index_key_can_be_set_by_hand_and_a_bad_one_is_refused(client):
    """For a collection whose COSMOS folder does not follow from its name. A new key does not keep
    the old key's name."""
    c = client
    await started(c)

    bad = await c.post(f"{API}/index-key", json={"index_key": "bad key!"})
    named = await c.post(f"{API}/index-key", json={"index_key": "nasa_legacy", "index_name": "NASA Legacy"})
    pinned = (await c.get(API)).json()
    await c.post(f"{API}/index-key", json={"index_key": "nasa_other"})

    col = (await c.get(API)).json()
    assert (bad.status_code, named.status_code) == (422, 200)
    assert (pinned["index_key"], pinned["index_name"]) == ("nasa_legacy", "NASA Legacy")
    assert (col["index_key"], col["index_name"]) == ("nasa_other", CID)
    assert "index.key" in [a["action"] for a in (await c.get(f"{API}/audit")).json()]


# ── the re-curation reason ─────────────────────────────────────────────


async def test_the_recuration_reason_shows_on_the_header_and_the_dashboard_row(client):
    c = client
    await promoted(c)
    reason = "re-crawled on 2026-10-09 after 8 URLs were promoted"

    await c.app.state.db.set_flag(CID, True, reason)

    assert reason in (await c.get(f"/collections/{CID}/header")).text
    assert reason in (await c.get("/rows")).text


# ── curated count: the indexed subset of the curated rows ──────────────


async def test_an_exclude_on_a_curated_page_drops_the_count_and_keeps_the_row_listed(client):
    c = client
    await promoted(c)

    r = await c.post(f"{API}/urls", json={"url": url(4), "type": "exclude"})

    every = (await c.get(f"{API}/curated")).json()
    page = (await c.get(f"/collections/{CID}?tab=curated")).text
    assert (r.json()["modified"], r.json()["excluded"]) == (0, 1)  # a rule, applied in place: no delta URL
    assert await counts(c) == (PAGES - 1, PAGES, 0, "curated")
    assert every["total"] == PAGES and [x["url"] for x in every["items"] if x["excluded"]] == [url(4)]
    assert (await c.get(f"{API}/curated?excluded=false")).json()["total"] == PAGES - 1
    assert f'>Curated URLs <span class="count ok ">{PAGES - 1}</span>' in page and url(4) in page


async def test_including_a_curated_page_again_counts_it_only_once_promoted(client):
    c = client
    await promoted(c)
    await c.post(f"{API}/urls", json={"url": url(4), "type": "exclude"})

    await c.post(f"{API}/urls", json={"url": url(4), "type": "include"})
    queued = await counts(c)
    r = await c.post(f"{API}/promote")

    assert queued == (PAGES - 1, PAGES, 1, "curating")
    assert r.json() == {"curated": PAGES, "status": "curated"}
    assert await counts(c) == (PAGES, PAGES, 0, "curated")


async def test_a_collection_with_every_curated_page_excluded_is_still_promoted(client):
    """curated_count 0 means "nothing reaches the index", not "nothing was promoted"."""
    c = client
    await promoted(c)

    await c.post(f"{API}/patterns", json={"type": "exclude", "match": "*"})
    moved = await c.post(f"{API}/status", json={"status": "config_generated"})

    assert await counts(c) == (0, PAGES, 0, "config_generated")
    assert moved.status_code == 200
    assert "Nothing promoted yet" not in (await c.get(f"/collections/{CID}?tab=curated")).text
