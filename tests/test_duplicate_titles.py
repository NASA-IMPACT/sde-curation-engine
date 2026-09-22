"""Title + document type combinations two pages of one collection would both be indexed under:
counted, flagged per row, filterable, and sent back to the LLM (fake provider) for new titles only."""

import re

from sde_curation.engine.export import export_lines
from sde_curation.llm.base import LLMError
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import disambiguate, title_siblings, url_distinctions
from sde_curation.models import DumpUrl
from tests.conftest import wait_job

CID = "ex.org"


async def make(client, pages: dict[str, str]) -> None:
    """A collection whose dump is `pages` (path -> scraped title), recomputed into delta URLs."""
    db = client.app.state.db
    if (await client.get(f"/api/collections/{CID}")).status_code == 404:
        await client.post("/api/collections", json={"seed_url": f"https://{CID}", "name": "Ex", "max_pages": 10})
    await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=f"https://{CID}{p}", scraped_title=t,
                                        full_text=f"body of {p}") for p, t in pages.items()])
    assert (await client.post(f"/api/collections/{CID}/recompute")).status_code == 200


def urls(*paths: str) -> list[str]:
    return [f"https://{CID}{p}" for p in paths]


async def delta(client, path: str) -> dict:
    return (await client.get(f"/api/collections/{CID}/delta", params={"q": f"https://{CID}{path}"})).json()["items"][0]


async def test_duplicates_are_counted_as_the_pages_would_be_indexed(client):
    db = client.app.state.db
    await make(client, {"/a": "Mission Data", "/b": "mission   data", "/c": "Other", "/d": "Solo"})
    # the SME types a title and a division for every page (promote below refuses blanks)
    await client.post(f"/api/collections/{CID}/patterns", json={"type": "title", "match": "*", "value": "{title}"})
    await client.post(f"/api/collections/{CID}/patterns", json={"type": "division", "match": "*", "value": "Earth Science"})
    # case and runs of whitespace do not make two titles different
    assert await db.duplicate_title_counts(CID) == {"urls": 2, "titles": 1, "delta_urls": 2}
    # a rule that gives a third page the same title joins the group
    await client.post(f"/api/collections/{CID}/patterns", json={"type": "title", "match": "*/c", "value": "Mission Data"})
    assert (await db.duplicate_title_counts(CID))["urls"] == 3
    # the same title with a different document type is not a duplicate
    await client.post(f"/api/collections/{CID}/patterns", json={"type": "document_type", "match": "*/c", "value": "Data"})
    assert (await db.duplicate_title_counts(CID))["urls"] == 2
    await client.post(f"/api/collections/{CID}/patterns", json={"type": "document_type", "match": "*", "value": "Data"})
    assert (await db.duplicate_title_counts(CID))["urls"] == 3
    # an excluded page is never indexed, so it shares nothing
    await client.post(f"/api/collections/{CID}/patterns", json={"type": "exclude", "match": "*/b"})
    assert await db.duplicate_title_counts(CID) == {"urls": 2, "titles": 1, "delta_urls": 2}
    # a pending AI title counts as if accepted
    await db.set_delta_ai_titles(CID, [{"url": urls("/d")[0], "title": "Mission data", "title_conf": "high"}])
    assert (await db.duplicate_title_counts(CID))["urls"] == 3

    # promote refuses a collision outright: two pages a search cannot tell apart may not be indexed
    await client.post(f"/api/collections/{CID}/ai/reject", json={"url": urls("/d")[0], "field": "title"})
    r = await client.post(f"/api/collections/{CID}/promote")
    assert r.status_code == 409 and "2 sharing a title and document type with another page" in r.json()["detail"]
    assert await db.incomplete_counts(CID) == {"urls": 2, "title": 0, "division": 0, "general": 0,
                                               "document_type": 0, "duplicate": 2}
    assert sorted(d.url for d in (await db.list_deltas(CID, incomplete=True))[0]) == urls("/a", "/c")
    assert "2 sharing a title + document type with another page" in (await client.get(f"/collections/{CID}?tab=curate")).text

    # a title of its own for one of them settles it (the curator's per-URL edit wins), and it promotes
    r = await client.post(f"/api/collections/{CID}/urls",
                          json={"url": urls("/c")[0], "type": "title", "value": "Other Mission Data"})
    assert r.status_code in (200, 201), r.text
    assert await db.duplicate_title_counts(CID) == {"urls": 0, "titles": 0, "delta_urls": 0}
    assert (await client.post(f"/api/collections/{CID}/promote")).status_code == 200

    # promoted: the curated rows count too, and a new delta URL that collides with them is flagged
    await make(client, {"/a": "Mission Data", "/b": "mission data", "/c": "Other", "/d": "Solo", "/e": "MISSION DATA"})
    assert await db.duplicate_title_counts(CID) == {"urls": 2, "titles": 1, "delta_urls": 1}
    rows = (await db.list_deltas(CID, dup_title=True))[0]
    assert [r.url for r in rows] == urls("/e")
    assert [r.url for r in (await db.list_curated(CID, dup_title=True))[0]] == urls("/a")
    info = await db.duplicate_titles_for(CID, urls("/e", "/d"))
    assert set(info) == set(urls("/e")) and info[urls("/e")[0]]["others"] == 1
    assert info[urls("/e")[0]]["sample"] == urls("/a")
    # and it is the delta row, not the curated one it collides with, that promote holds back —
    # promoting it on its own does not get it past the gate either
    assert (await db.incomplete_counts(CID))["duplicate"] == 1
    assert [d.url for d in (await db.list_deltas(CID, incomplete=True))[0]] == urls("/e")
    r = await client.post(f"/api/collections/{CID}/promote/urls", json={"urls": urls("/e")})
    assert r.status_code == 409 and "1 sharing a title and document type with another page" in r.json()["detail"]

    # the tables flag the row and filter on it; the Curate page says how many and offers the fix
    page = (await client.get(f"/collections/{CID}?tab=delta&dup=title")).text
    assert "⚠ same title + type ×2" in page and "https://ex.org/e" in page and "https://ex.org/d" not in page
    assert "same title + type ×2" in (await client.get(f"/collections/{CID}?tab=curated")).text
    curate = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert "2 URLs share 1 title + document type combination" in curate and "Regenerate duplicate titles" in curate and "suggest/titles" in curate
    csv = (await client.get(f"/collections/{CID}/urls/delta?format=csv&dup=title")).text
    assert csv.count("\n") == 2  # header + /e


async def test_a_shared_scraped_title_blocks_promote_until_the_pages_are_told_apart(client):
    """A site that puts one <title> on every page. The scraped title counts as a title — nothing
    here is blank — but not as one that tells the pages apart, so promote refuses the lot. The fix
    is the regenerate pass, and a suggestion only lifts the gate once it is accepted: promote
    discards undecided ones, so they are not values yet."""
    db = client.app.state.db
    await make(client, {"/a": "Solar Data - NASA", "/b": "Solar Data - NASA", "/c": "Solar Data - NASA"})
    for t, v in (("division", "Heliophysics"), ("document_type", "Data")):
        await client.post(f"/api/collections/{CID}/patterns", json={"type": t, "match": "*", "value": v})
    # every field is filled — and all three pages would be one row in a search
    assert await db.incomplete_counts(CID) == {"urls": 3, "title": 0, "division": 0, "general": 0,
                                               "document_type": 0, "duplicate": 3}
    r = await client.post(f"/api/collections/{CID}/promote")
    assert r.status_code == 409 and "3 sharing a title and document type with another page" in r.json()["detail"]
    assert "Regenerate duplicate titles" in r.json()["detail"]
    # the metadata pass does not settle it by itself: it strips the site suffix and lands on one title
    await client.post(f"/api/collections/{CID}/suggest/metadata"); await wait_job(client, CID)
    await client.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept"})
    assert (await db.incomplete_counts(CID))["duplicate"] == 3

    # ↻ Regenerate duplicate titles answers it, but only an accepted answer is a value
    await client.post(f"/api/collections/{CID}/suggest/titles"); await wait_job(client, CID)
    assert await db.duplicate_title_counts(CID) == {"urls": 0, "titles": 0, "delta_urls": 0}  # as if accepted
    assert (await db.incomplete_counts(CID))["duplicate"] == 3                                # as they stand
    assert (await client.post(f"/api/collections/{CID}/promote")).status_code == 409
    await client.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept", "field": "title"})
    assert (await db.incomplete_counts(CID))["duplicate"] == 0
    assert (await client.post(f"/api/collections/{CID}/promote")).status_code == 200
    titles = [x.title for x in export_lines(await db.load_curated(CID))]
    assert len(set(titles)) == 3, titles


async def test_suggest_metadata_tells_the_titles_it_generated_apart(client):
    await make(client, {"/x/alpha": "Same - Ex", "/x/beta": "Same - Ex", "/x/gamma": "Unique - Ex"})
    client.app.state.jobs.s.llm_dedupe_titles = True  # opt-in: Suggest metadata retitles by itself
    llm = client.app.state.jobs._llm = FakeProvider()
    assert (await client.post(f"/api/collections/{CID}/suggest/metadata")).status_code == 202
    job = await wait_job(client, CID)
    p = job["progress"]
    assert job["state"] == "succeeded", job
    assert p["classified"] == 3 and p["titles_total"] == 2 and p["retitled"] == 2 and p["still_duplicate"] == 0, p
    a, b, g = (await delta(client, "/x/alpha")), (await delta(client, "/x/beta")), (await delta(client, "/x/gamma"))
    assert (a["title_ai"], b["title_ai"], g["title_ai"]) == ("Same — Alpha", "Same — Beta", "Unique")
    assert (a["title_ai_before"], b["title_ai_before"], g["title_ai_before"]) == ("Same", "Same", None)
    assert a["title_ai_conf"] == "medium" and a["document_type_ai"] == "Documentation"  # other fields untouched
    # ONE call for the group, both pages in it: the model tells them apart from each other rather
    # than guessing page by page and landing on the same title again
    retitle = [call["user"] for call in llm.calls if call["schema"] == "DistinctTitles"]
    assert len(retitle) == 1
    sent = retitle[0]
    assert '"shared_title": "Same", "document_type": "Documentation", "pages_sharing_it": 2' in sent
    assert '"pages_to_retitle": 2' in sent
    assert sent.count("\nText:\n") == 2 and "body of /x/alpha" in sent and "body of /x/beta" in sent
    assert "/x/gamma" not in sent  # its title is its own already
    # the differing part of each URL is worked out for the model, not left for it to spot
    assert '"url_differs_at": {"https://ex.org/x/alpha": ["alpha"], "https://ex.org/x/beta": ["beta"]}' in sent
    assert a["document_type_ai"] == "Documentation" and a["document_type_ai_conf"] == "low"  # only the title is redone
    assert "retitled" in (await client.get(f"/collections/{CID}?tab=curate")).text


async def test_the_pass_is_skipped_when_disabled_and_never_fails_the_classification(client, monkeypatch):
    await make(client, {"/x/alpha": "Same - Ex", "/x/beta": "Same - Ex"})
    jobs = client.app.state.jobs
    assert jobs.s.llm_dedupe_titles is False  # the default
    await client.post(f"/api/collections/{CID}/suggest/metadata")
    p = (await wait_job(client, CID))["progress"]
    assert p["classified"] == 2 and "titles_total" not in p

    jobs.s.llm_dedupe_titles = True

    async def broken(*a, **k):
        raise LLMError("model unavailable")

    monkeypatch.setattr("sde_curation.jobs.suggest_distinct_titles", broken)
    monkeypatch.setattr("sde_curation.jobs.disambiguate", broken)  # the floor is out too
    await client.post(f"/api/collections/{CID}/suggest/metadata?all=true")
    job = await wait_job(client, CID)
    assert job["state"] == "succeeded" and job["progress"]["classified"] == 2, job
    assert "model unavailable" in job["progress"]["titles_error"]
    assert (await delta(client, "/x/alpha"))["title_ai"] == "Same"  # the first answer stays, flagged
    assert (await client.app.state.db.duplicate_title_counts(CID))["urls"] == 2


async def test_tell_them_apart_on_demand(client):
    db = client.app.state.db
    await make(client, {"/p0": "Zero", "/p1": "One", "/p2": "Two"})
    assert (await client.post(f"/api/collections/{CID}/suggest/titles")).status_code == 409  # nothing shared
    # a rule gave every page the same title: no AI title pending, so every delta URL is asked
    await client.post(f"/api/collections/{CID}/patterns", json={"type": "title", "match": "*", "value": "Portal"})
    assert (await client.post(f"/api/collections/{CID}/suggest/titles")).status_code == 202
    job = await wait_job(client, CID)
    assert job["kind"] == "llm_titles" and job["state"] == "succeeded", job
    assert job["progress"]["retitled"] == 3 and job["progress"]["still_duplicate"] == 0
    assert (await delta(client, "/p1"))["title_ai"] == "Portal — P1"
    assert (await delta(client, "/p1"))["title_ai_before"] == "Portal"
    assert (await delta(client, "/p1"))["title"] == "Portal"  # a suggestion: nothing applied
    assert (await client.post(f"/api/collections/{CID}/suggest/titles")).status_code == 409

    # where some pages of a group have a pending AI title, those are asked first and the rest keep
    # theirs — but the pass does not stop there: the ones left sharing go round again, so one click
    # still ends at zero
    for u in urls("/p0", "/p1"):
        await client.post(f"/api/collections/{CID}/ai/reject", json={"url": u, "field": "title"})
    assert (await delta(client, "/p0"))["title_ai_before"] is None  # dropped with the AI title
    await db.set_delta_ai_titles(CID, [{"url": urls("/p2")[0], "title": "Portal", "title_conf": "low"}])
    llm = client.app.state.jobs._llm = FakeProvider()
    await client.post(f"/api/collections/{CID}/suggest/titles")
    job = await wait_job(client, CID)
    assert job["progress"]["titles_total"] == 1  # p2 is the one with a pending title, so it goes first
    assert job["progress"]["still_duplicate"] == 0 and job["progress"]["title_pass"] == 2
    assert '"url": "https://ex.org/p2"' in llm.calls[0]["user"]
    assert '"settled_titles": [{"url": "https://ex.org/p0", "title": "Portal"}' in llm.calls[0]["user"]
    # the second pass picked up p0 and p1, which the first one left sharing "Portal"
    assert '"pages_to_retitle": 2' in llm.calls[1]["user"]
    assert {(await delta(client, p))["title_ai"] for p in ("/p0", "/p1", "/p2")} == {
        "Portal — P0", "Portal — P1", "Portal — P2"}
    assert "0 still share one" in (await client.get(f"/collections/{CID}?tab=curate")).text


async def test_by_default_duplicates_are_flagged_with_the_titles_as_generated(client):
    db = client.app.state.db
    await make(client, {"/x/alpha": "Same - Ex", "/x/beta": "Same - Ex", "/x/gamma": "Unique - Ex"})
    llm = client.app.state.jobs._llm = FakeProvider()
    await client.post(f"/api/collections/{CID}/suggest/metadata")
    p = (await wait_job(client, CID))["progress"]
    assert p["classified"] == 3 and "titles_total" not in p
    assert not [c for c in llm.calls if c["schema"] == "DistinctTitles"]  # nothing sent back on its own
    assert (await delta(client, "/x/alpha"))["title_ai"] == "Same"  # the title as generated
    curate = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert "2 URLs share 1 title + document type combination" in curate and "same title + type ×2" in curate
    assert "One run ends at zero" in curate and "flagged with the title as generated" in curate

    # sent back on demand: the new title is the suggestion, the one it shared is kept and shown
    await client.post(f"/api/collections/{CID}/suggest/titles")
    await wait_job(client, CID)
    a = await delta(client, "/x/alpha")
    assert (a["title_ai"], a["title_ai_before"]) == ("Same — Alpha", "Same")
    assert (await db.delta_ai_counts(CID))["retitled"] == 2
    assert [r.url for r in (await db.list_deltas(CID, retitled=True))[0]] == urls("/x/alpha", "/x/beta")
    page = (await client.get(f"/collections/{CID}?tab=delta&dup=retitled")).text
    assert "⚠ was same title + type" in page and "“Same”" in page and "✎ original" in page
    assert "https://ex.org/x/gamma" not in page
    assert "2 AI titles were regenerated from duplicates" in (await client.get(f"/collections/{CID}?tab=curate")).text

    # editing the original instead: accepted as edited, and the kept title goes with the suggestion
    r = await client.post(f"/api/collections/{CID}/ai/accept",
                          json={"url": urls("/x/alpha")[0], "field": "title", "value": "Same, Part A"})
    assert r.status_code == 200
    a = await delta(client, "/x/alpha")
    assert (a["title"], a["title_ai"], a["title_ai_before"]) == ("Same, Part A", None, None)
    # a fresh classification starts over
    await client.post(f"/api/collections/{CID}/suggest/metadata?all=true")
    await wait_job(client, CID)
    assert (await delta(client, "/x/beta"))["title_ai_before"] is None


async def test_one_click_ends_at_zero_even_when_the_model_cannot_tell_them_apart(client):
    """The pass owns the outcome: the curator presses Regenerate once, not until the count reaches
    zero. The fake model answers "<shared title> — <last path segment>" and these three pages share
    their last segment, so it hands back one title for all three however often it is asked — the
    case that used to leave a residue nothing could shift. One click still ends with three distinct
    titles, because what the model cannot separate the URLs do, and no title grows a tail out of the
    pass's own previous answer."""
    db = client.app.state.db
    await make(client, {"/a/access": "Data Access - Ex", "/b/access": "Data Access - Ex",
                        "/c/access": "Data Access - Ex"})
    await client.post(f"/api/collections/{CID}/suggest/metadata")
    assert (await wait_job(client, CID))["state"] == "succeeded"
    assert (await db.duplicate_title_counts(CID))["urls"] == 3

    llm = client.app.state.jobs._llm = FakeProvider()
    assert (await client.post(f"/api/collections/{CID}/suggest/titles")).status_code == 202
    job = await wait_job(client, CID)
    assert job["state"] == "succeeded", job
    p = job["progress"]
    assert p["still_duplicate"] == 0 and p["disambiguated"], p  # the model needed the floor
    assert (await db.duplicate_title_counts(CID)) == {"urls": 0, "titles": 0, "delta_urls": 0}

    rows = [await delta(client, u) for u in ("/a/access", "/b/access", "/c/access")]
    titles = [r["title_ai"] for r in rows]
    assert len(set(titles)) == 3, titles
    # one suffix at most on any of them: a pass never builds on the answer the pass before it gave
    assert all(t.count("—") <= 1 for t in titles), titles
    # only the pages that actually moved carry the title they shared; one may keep it and still be
    # distinct, which is the cheapest way out of a group and costs the SME nothing
    assert [r["title_ai_before"] for r in rows].count("Data Access") >= 2
    assert "Data Access" in titles or all(t.startswith("Data Access — ") for t in titles)
    # a group asked twice is asked the SAME question — always from the title the pages shared, never
    # from the answer the pass before it gave, which is what used to grow the tail
    assert llm.calls and all('"shared_title": "Data Access"' in c["user"] for c in llm.calls)
    # a title the pass resolved from the URLs itself says so, so the SME can see which to check
    assert "rule:url-distinction" in {r["ai_model"] for r in rows}

    # and there is nothing left to send back
    assert (await client.post(f"/api/collections/{CID}/suggest/titles")).status_code == 409
    assert "0 still share one" in (await client.get(f"/collections/{CID}?tab=curate")).text


async def test_a_row_that_still_collides_shows_one_warning_not_two(client):
    """A collision the pass cannot reach — an SME's own titles, or a curated row — leaves a row both
    regenerated and still sharing. The ⚠ belongs to the badge that says it still collides; the
    regenerated line drops to a note, because two warnings on one row contradict each other."""
    db = client.app.state.db
    await make(client, {"/x/alpha": "Same - Ex", "/x/beta": "Same - Ex"})
    await client.post(f"/api/collections/{CID}/suggest/metadata")
    await wait_job(client, CID)
    await client.post(f"/api/collections/{CID}/suggest/titles")
    await wait_job(client, CID)
    assert (await db.duplicate_title_counts(CID))["urls"] == 0

    page = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert page.count("⚠ was same title + type") == 2 and "same title + type ×" not in page

    # the curator puts them back on one title: now both flags apply to the same row
    await db.set_delta_ai_titles(CID, [{"url": u, "title": "One Title", "title_conf": "low"}
                                       for u in urls("/x/alpha", "/x/beta")])
    page = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert page.count("same title + type ×2") == 2
    assert "⚠ was same title + type" not in page
    assert page.count("↻ regenerated, still shared") == 2 and "✎ original" in page

    # sent back again, the group is asked about the title it originally shared, and the answer that
    # put them here is named so the model does not offer it a second time
    llm = client.app.state.jobs._llm = FakeProvider()
    await client.post(f"/api/collections/{CID}/suggest/titles")
    await wait_job(client, CID)
    assert '"shared_title": "Same"' in llm.calls[0]["user"]
    assert '"previous_titles": ["One Title"]' in llm.calls[0]["user"]
    assert (await db.duplicate_title_counts(CID))["urls"] == 0


async def test_several_groups_at_once_are_counted_and_claimed_correctly(client):
    """Groups run against each other in the pool. Two things have to survive that: the counters
    (a read-await-write around the DB loses updates when five groups do it at the same time) and
    the claim on a title, which is what stops two groups being handed the same one."""
    db = client.app.state.db
    pages = {}
    for section in ("alpha", "beta", "gamma", "delta"):
        for i in range(4):
            pages[f"/{section}/page-{i}"] = f"{section.title()} Section - Ex"
    await make(client, pages)  # 4 groups of 4
    await client.post(f"/api/collections/{CID}/suggest/metadata")
    await wait_job(client, CID)
    assert (await db.duplicate_title_counts(CID))["urls"] == 16

    await client.post(f"/api/collections/{CID}/suggest/titles")
    p = (await wait_job(client, CID))["progress"]
    assert p["still_duplicate"] == 0
    # every page that changed is accounted for in exactly one of the two counters
    changed, _ = await db.list_deltas(CID, limit=100)
    moved = [r for r in changed if r.title_ai_before]
    assert p["retitled"] + p["disambiguated"] == len(moved) == 16
    assert len({r.title_ai for r in changed}) == 16  # no two groups took the same title


def review_rows(page: str) -> list[str]:
    """The URLs of the metadata review table under Curate › Metadata."""
    table = page.split('class="urls ai-review"')[1].split("</table>")[0]
    return re.findall(r'<td class="url"><a href="(https://ex\.org[^"]*)"', table)


async def test_the_badge_filters_the_step_it_is_shown_on_and_outlives_the_suggestions(client):
    """The ⚠ badge is a filter link, and it links to the table it is shown in: under Curate ›
    Metadata it filters that table, so a collision is fixed without leaving the step. Deciding a
    suggestion decides a field, not a collision, so the row stays listed until the titles differ."""
    await make(client, {"/a": "Same - Ex", "/b": "Same - Ex", "/c": "Solo - Ex"})
    await client.post(f"/api/collections/{CID}/suggest/metadata")
    assert (await wait_job(client, CID))["state"] == "succeeded"

    page = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert 'href="/collections/ex.org?tab=curate&amp;dup=title#metadata"' in page
    assert "same title + type ×2" in page and review_rows(page) == urls("/a", "/b", "/c")
    # on a URL table it still filters that table
    delta = (await client.get(f"/collections/{CID}?tab=delta")).text
    assert 'href="/collections/ex.org?tab=delta&amp;dup=title"' in delta

    # every suggestion on /a accepted: nothing left to review there, but it still collides with /b
    r = await client.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept", "url": urls("/a")[0]})
    assert r.status_code == 200
    page = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert review_rows(page) == urls("/a", "/b", "/c") and "same title + type ×2" in page

    # ?dup=title narrows the table to the colliding rows, whatever their suggestions are
    page = (await client.get(f"/collections/{CID}?tab=curate&dup=title")).text
    assert review_rows(page) == urls("/a", "/b")
    assert "⚠ duplicates 2" in page and "clear filter" in page

    # an excluded page shares nothing: the collision is gone and so is the filtered table
    await client.post(f"/api/collections/{CID}/patterns", json={"type": "exclude", "match": "*/b"})
    page = (await client.get(f"/collections/{CID}?tab=curate&dup=title")).text
    assert review_rows(page) == [] and "No delta URL shares a title + document type any more." in page
    assert "same title + type" not in (await client.get(f"/collections/{CID}?tab=curate")).text


async def test_a_collision_keeps_the_review_table_open_after_every_suggestion_is_decided(client):
    """Accept all: no suggestions left anywhere, yet the two pages would still be indexed under one
    title + type — the table stays, with the rows that still need the curator."""
    await make(client, {"/a": "Same - Ex", "/b": "Same - Ex", "/c": "Solo - Ex"})
    await client.post(f"/api/collections/{CID}/suggest/metadata")
    await wait_job(client, CID)
    assert (await client.post(f"/api/collections/{CID}/ai/bulk", json={"decision": "accept"})).status_code == 200
    assert (await client.app.state.db.delta_ai_counts(CID))["title"] == 0

    page = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert review_rows(page) == urls("/a", "/b")  # /c is decided and tells itself apart: gone
    assert "same title + type ×2" in page and "⚠ duplicates 2" in page
    assert "AI suggestions to review" not in page  # the bulk bar goes with the suggestions


def test_url_distinctions_are_what_one_url_has_and_the_others_do_not():
    # only a token EVERY URL carries says nothing; one shared with some of them still narrows it
    u = ["https://ex.org/data/ozone/access", "https://ex.org/data/clouds/access",
         "https://ex.org/images/gallery/aurora"]
    d = url_distinctions(u)
    assert d[u[0]] == ["data", "ozone", "access"] and d[u[1]] == ["data", "clouds", "access"]
    assert d[u[2]] == ["images", "gallery", "aurora"]
    # drop the odd one out and "data" / "access" become common, leaving what really differs
    d = url_distinctions(u[:2])
    assert d[u[0]] == ["ozone"] and d[u[1]] == ["clouds"]
    # depths differ: comparing tokens, not positions, still finds the difference
    d = url_distinctions(["https://ex.org/data", "https://ex.org/data/ozone/2024"])
    assert d["https://ex.org/data"] == ["data"] and d["https://ex.org/data/ozone/2024"] == ["ozone", "2024"]
    # a query string is part of what tells two URLs apart
    d = url_distinctions(["https://ex.org/browse?year=2023", "https://ex.org/browse?year=2024"])
    assert d["https://ex.org/browse?year=2024"] == ["year=2024"]
    # the same tokens in another order: the last segment is the fallback, never nothing
    d = url_distinctions(["https://ex.org/a/b", "https://ex.org/b/a"])
    assert d["https://ex.org/a/b"] == ["b"] and d["https://ex.org/b/a"] == ["a"]


def test_disambiguate_always_resolves_a_group():
    """The floor under the whole pass: URLs are unique within a collection, so a distinct title can
    always be built from them. Nothing here asks a model, and nothing comes back colliding."""
    u = ["https://ex.org/data/ozone/access", "https://ex.org/data/clouds/access"]
    out = disambiguate("Data Access", u)
    assert out[u[0]] == "Data Access — Ozone" and out[u[1]] == "Data Access — Clouds"
    # a title already spoken for is stepped over, not written again
    out = disambiguate("Data Access", u, taken=["Data Access — Ozone"])
    assert out[u[0]] == "Data Access — Ozone (2)" and len(set(out.values())) == 2
    # extensions and separators come out readable
    assert disambiguate("Guide", ["https://ex.org/g/user-guide_v2.html", "https://ex.org/g/faq"]) == {
        "https://ex.org/g/faq": "Guide — Faq",
        "https://ex.org/g/user-guide_v2.html": "Guide — User Guide V2",
    }
    # 50 URLs that differ only in a number still come back 50 different titles
    many = [f"https://ex.org/v/{i}" for i in range(50)]
    assert len(set(disambiguate("Volume", many).values())) == 50


def test_title_siblings_are_the_url_order_neighbours():
    members = [{"url": f"u{i:03}", "rewrite": i % 2 == 0} for i in range(100)]
    near = title_siblings(members, "u050", k=4)
    assert [m["url"] for m in near] == ["u048", "u049", "u051", "u052"]
    assert [m["keeps_title"] for m in near] == [False, True, True, False]
    assert [m["url"] for m in title_siblings(members, "u000", k=3)] == ["u001", "u002", "u003"]
    assert [m["url"] for m in title_siblings(members, "u099", k=3)] == ["u096", "u097", "u098"]
    assert len(title_siblings(members[:3], "u001")) == 2


async def test_same_title_with_a_different_document_type_is_not_sent_back(client):
    # the fake model reads "data" / "image" in the URL as the document type: same title, two types
    await make(client, {"/data/one": "Same - Ex", "/image/two": "Same - Ex", "/data/three": "Same - Ex"})
    client.app.state.jobs.s.llm_dedupe_titles = True
    llm = client.app.state.jobs._llm = FakeProvider()
    await client.post(f"/api/collections/{CID}/suggest/metadata")
    p = (await wait_job(client, CID))["progress"]
    assert p["classified"] == 3 and p["titles_total"] == 2 and p["retitled"] == 2, p
    assert (await delta(client, "/image/two"))["title_ai"] == "Same"  # its type sets it apart already
    assert (await delta(client, "/data/one"))["title_ai"] == "Same — One"
    sent = [c["user"] for c in llm.calls if c["schema"] == "DistinctTitles"]
    assert len(sent) == 1 and sent[0].count("\nText:\n") == 2  # one call, the two Data pages in it
    assert "body of /image/two" not in sent[0]
    assert "image/two" not in sent[0]  # not listed as a settled title or in the URL diff either
