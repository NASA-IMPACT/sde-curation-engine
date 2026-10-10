"""Duplicate titles on the real database and the real pages: the SQL filters that find the rows two
pages would both be indexed under, and what the curator sees of them. The rules that decide what a
duplicate is are in tests/integration/test_db_contract.py (counts) and tests/unit/test_g1_duplicate_titles.py
(the regenerate pass). State is written straight to the database: no crawl, no LLM job."""

import re

from sde_curation.models import DumpUrl, JobKind, JobRun, JobState

CID = "ex.org"
API = f"/api/collections/{CID}"
TAB = f"/collections/{CID}?tab="
SHARED = "Same"
EVERY_PAGE_FILLED = (("division", "Earth Science"), ("document_type", "Data"))


def url(path: str) -> str:
    return f"https://{CID}{path}"


async def crawl(c, pages: dict[str, str]) -> None:
    """A collection whose crawl is `pages` (path -> scraped title), queued for curation, with a
    division and a document type rule for every page so only the titles can block promote."""
    if (await c.get(API)).status_code == 404:
        await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": "Ex", "max_pages": 10})
        for field, value in EVERY_PAGE_FILLED:
            await c.post(f"{API}/patterns", json={"type": field, "match": "*", "value": value})
    await c.app.state.db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(p), scraped_title=t,
                                                    full_text=f"body of {p}") for p, t in pages.items()])
    assert (await c.post(f"{API}/recompute")).status_code == 200


async def promoted_then_collided(c) -> None:
    """/a is promoted as "Mission Data"; a re-crawl brings /e titled the same, and /d with a title of
    its own."""
    await crawl(c, {"/a": "Mission Data", "/d": "Solo"})
    assert (await c.post(f"{API}/promote")).status_code == 200
    await crawl(c, {"/a": "Mission Data", "/d": "Solo", "/e": "MISSION DATA"})


async def test_the_dup_filters_find_the_delta_row_and_the_curated_row_it_collides_with(client):
    """The delta table lists the new row, the curated table the row it collides with, and the
    per-row lookup names the other page."""
    db = client.app.state.db
    await promoted_then_collided(client)

    deltas, _ = await db.list_deltas(CID, dup_title=True)
    curated, _ = await db.list_curated(CID, dup_title=True)
    info = await db.duplicate_titles_for(CID, [url("/e"), url("/d")])

    assert ([d.url for d in deltas], [r.url for r in curated]) == ([url("/e")], [url("/a")])
    assert info == {url("/e"): {"title": "MISSION DATA", "document_type": "Data", "others": 1, "sample": [url("/a")]}}


async def test_a_shared_title_is_flagged_on_both_tables_and_the_delta_filter_keeps_only_those_rows(client):
    await promoted_then_collided(client)

    delta = (await client.get(f"{TAB}delta&dup=title")).text
    curated = (await client.get(f"{TAB}curated")).text
    csv = (await client.get(f"/collections/{CID}/urls/delta?format=csv&dup=title")).text

    assert "⚠ same title + type ×2" in delta and url("/e") in delta and url("/d") not in delta
    assert "same title + type ×2" in curated
    assert csv.splitlines()[1:] and all(url("/e") in line for line in csv.splitlines()[1:])
    assert len(csv.splitlines()) == 2  # the header and /e


async def test_the_curate_step_counts_the_shared_titles_and_offers_regenerate(client):
    await crawl(client, {"/a": "Mission Data", "/b": "mission   data", "/c": "Other"})

    curate = (await client.get(f"{TAB}curate")).text

    assert "2 URLs share 1 title + document type combination" in curate
    assert "Regenerate duplicate titles" in curate and f"{API}/suggest/titles" in curate


async def test_regenerate_is_refused_when_no_delta_url_shares_a_title(client):
    await crawl(client, {"/a": "Zero", "/b": "One"})

    r = await client.post(f"{API}/suggest/titles")

    assert (r.status_code, r.json()["detail"]) == (
        409, "no delta URL shares its title and document type with another page")


# ── regenerated titles ──────────────────────────────────────────────────────────────────────────


async def regenerated(c, *, titles: dict[str, str]) -> None:
    """/x/alpha and /x/beta shared "Same"; the regenerate pass wrote `titles` (path -> title),
    keeping "Same" as the title they shared. /x/gamma has a title of its own."""
    await crawl(c, {"/x/alpha": SHARED, "/x/beta": SHARED, "/x/gamma": "Unique"})
    await c.app.state.db.set_delta_ai_titles(CID, [{"url": url(p), "title": t, "title_conf": "medium",
                                                    "before": SHARED} for p, t in titles.items()])


TOLD_APART = {"/x/alpha": f"{SHARED} — Alpha", "/x/beta": f"{SHARED} — Beta"}
PUT_BACK_TOGETHER = {"/x/alpha": "One Title", "/x/beta": "One Title"}


async def test_regenerated_titles_are_listed_and_show_the_title_they_shared(client):
    db = client.app.state.db
    await regenerated(client, titles=TOLD_APART)

    rows, _ = await db.list_deltas(CID, retitled=True)
    page = (await client.get(f"{TAB}delta&dup=retitled")).text
    curate = (await client.get(f"{TAB}curate")).text

    assert (await db.delta_ai_counts(CID))["retitled"] == 2
    assert [r.url for r in rows] == [url("/x/alpha"), url("/x/beta")]
    assert "⚠ was same title + type" in page and f"“{SHARED}”" in page and "✎ original" in page
    assert url("/x/gamma") not in page
    assert "2 AI titles were regenerated from duplicates" in curate


async def test_a_row_regenerated_and_still_shared_shows_one_warning_not_two(client):
    """The ⚠ belongs to the badge that says it still collides; the regenerated line drops to a note."""
    await regenerated(client, titles=TOLD_APART)
    apart = (await client.get(f"{TAB}curate")).text
    await client.app.state.db.set_delta_ai_titles(CID, [{"url": url(p), "title": t, "before": SHARED}
                                                        for p, t in PUT_BACK_TOGETHER.items()])

    together = (await client.get(f"{TAB}curate")).text

    assert apart.count("⚠ was same title + type") == 2 and "same title + type ×" not in apart
    assert together.count("same title + type ×2") == 2 and "⚠ was same title + type" not in together
    assert together.count("↻ regenerated, still shared") == 2


async def test_deciding_or_reclassifying_a_regenerated_title_drops_the_title_it_shared(client):
    """The kept title belongs to the suggestion: accept (as edited), reject, or a fresh Suggest
    metadata answer clears it with the suggestion."""
    db = client.app.state.db
    await regenerated(client, titles=TOLD_APART | {"/x/gamma": "Gamma"})

    accepted = await client.post(f"{API}/ai/accept", json={"url": url("/x/alpha"), "field": "title",
                                                           "value": "Same, Part A"})
    rejected = await client.post(f"{API}/ai/reject", json={"url": url("/x/beta"), "field": "title"})
    await db.set_delta_ai(CID, [{"url": url("/x/gamma"), "title": "Fresh", "title_conf": "high"}])

    rows = {d.url: d for d in await db.load_deltas(CID)}
    assert (accepted.status_code, rejected.status_code) == (200, 200)
    assert (rows[url("/x/alpha")].title, rows[url("/x/alpha")].title_ai_before) == ("Same, Part A", None)
    assert rows[url("/x/beta")].title_ai_before is None
    assert (rows[url("/x/gamma")].title_ai, rows[url("/x/gamma")].title_ai_before) == ("Fresh", None)


# ── the metadata review table under Curate ──────────────────────────────────────────────────────


def review_rows(page: str) -> list[str]:
    """The URLs of the metadata review table under Curate › Metadata."""
    table = page.split('class="urls ai-review"')[1].split("</table>")[0]
    return re.findall(r'<td class="url"><a href="(https://ex\.org[^"]*)"', table)


async def suggested(c) -> None:
    """/a and /b would both be indexed as "Same"; /c as "Solo". Every row has a pending title
    suggestion from the model (the division and document type come from the rules `crawl` adds)."""
    await crawl(c, {"/a": "Same - Ex", "/b": "Same - Ex", "/c": "Solo - Ex"})
    await c.app.state.db.set_delta_ai(CID, [{"url": url(p), "title": t, "title_conf": "high", "model": "fake"}
                                            for p, t in (("/a", SHARED), ("/b", SHARED), ("/c", "Solo"))])


async def test_the_badge_links_to_the_table_it_is_shown_in(client):
    await suggested(client)

    curate = (await client.get(f"{TAB}curate")).text
    delta = (await client.get(f"{TAB}delta")).text

    assert 'href="/collections/ex.org?tab=curate&amp;dup=title#metadata"' in curate
    assert 'href="/collections/ex.org?tab=delta&amp;dup=title"' in delta


async def test_a_collision_keeps_its_rows_in_the_review_table_after_their_suggestions_are_decided(client):
    """Deciding a suggestion decides a field, not a collision: the rows stay until the titles differ."""
    await suggested(client)
    assert (await client.post(f"{API}/ai/bulk", json={"decision": "accept"})).status_code == 200

    page = (await client.get(f"{TAB}curate")).text
    undecided = (await client.get(f"{TAB}curate&decided=hide")).text

    assert review_rows(page) == [url("/a"), url("/b"), url("/c")]
    assert "same title + type ×2" in page and "⚠ duplicates 2" in page
    assert "AI suggestions to review" not in page  # the bulk bar goes with the suggestions
    assert review_rows(undecided) == [url("/a"), url("/b")]


async def test_the_dup_filter_narrows_the_review_table_to_the_colliding_rows(client):
    await suggested(client)

    page = (await client.get(f"{TAB}curate&dup=title")).text

    assert review_rows(page) == [url("/a"), url("/b")]
    assert "⚠ duplicates 2" in page and "clear filter" in page


async def test_an_excluded_page_shares_nothing_so_the_filtered_review_table_empties(client):
    await suggested(client)
    await client.post(f"{API}/patterns", json={"type": "exclude", "match": "*/b"})

    filtered = (await client.get(f"{TAB}curate&dup=title")).text
    curate = (await client.get(f"{TAB}curate")).text

    assert review_rows(filtered) == []
    assert "No delta URL shares a title + document type any more." in filtered
    assert "same title + type" not in curate


async def test_with_the_dedupe_pass_off_the_curate_step_says_duplicates_stay_as_generated(client):
    """LLM_DEDUPE_TITLES is off by default: the step tells the curator Suggest metadata only flags
    the duplicates, so regenerating them is their call."""
    await crawl(client, {"/a": "Zero"})

    curate = (await client.get(f"{TAB}curate")).text

    assert "flagged with the title as generated" in curate


async def test_a_finished_regenerate_says_what_it_retitled_and_what_still_shares(client):
    await crawl(client, {"/a": "Zero"})
    await client.app.state.db.insert_job(JobRun(
        collection_id=CID, kind=JobKind.LLM_TITLES, state=JobState.SUCCEEDED,
        progress={"llm": "titles", "titles_total": 3, "retitled": 2, "title_calls": 1, "disambiguated": 1,
                  "still_duplicate": 0}))

    curate = (await client.get(f"{TAB}curate")).text

    assert ("2 of 3 URLs sharing a title and doc type retitled in 1 group call · 1 told apart by their URLs"
            " · 0 still share one") in curate
