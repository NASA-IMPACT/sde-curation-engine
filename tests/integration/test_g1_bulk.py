"""Bulk accept and reject on the real database and the real pages: pattern suggestions by type, AI
metadata suggestions by field (and what accept-all leaves to the curator), the Curate step's gate
and its expanded, paged cards. Adding rules in bulk is unit-tested in
tests/unit/test_curation_service.py; batching Suggest patterns in tests/unit/test_jobs_llm.py.
State is written straight to the database: no crawl, no LLM job."""

import pytest

from sde_curation.config import Settings
from sde_curation.models import DumpUrl, JobKind, JobRun, JobState

CID = "ex.org"
API = f"/api/collections/{CID}"
TAB = f"/collections/{CID}?tab="
PAGES = 3
BATCH_URLS = 50  # the smallest Suggest-patterns batch Settings allows
ONE_MORE_THAN_A_BATCH = BATCH_URLS + 6  # two calls


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(data_dir=tmp_path, llm_provider="fake", llm_pattern_batch_urls=BATCH_URLS)


def url(i: int) -> str:
    return f"https://{CID}/p{i}"


async def crawl(c, pages: int = PAGES) -> None:
    await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": "Ex", "max_pages": 10})
    await c.app.state.db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(i), scraped_title=f"Page {i}",
                                                    full_text=f"text of page {i}") for i in range(1, pages + 1)])
    assert (await c.post(f"{API}/recompute")).status_code == 200


async def exclusions_suggested(c, n: int = 2) -> None:
    await crawl(c)
    await c.app.state.db.add_pattern_suggestions(CID, [{"type": "exclude", "match": url(i), "matches": 1}
                                                       for i in range(1, n + 1)])


async def metadata_suggested(c) -> None:
    await crawl(c)
    await c.app.state.db.set_delta_ai(CID, [{"url": url(i), "model": "fake", "title": f"AI title {i}",
                                             "title_conf": "high", "division": "Heliophysics",
                                             "division_conf": "low", "document_type": "Data",
                                             "document_type_conf": "low"} for i in range(1, PAGES + 1)])


async def rows(c) -> dict[str, dict]:
    return {d["url"]: d for d in (await c.get(f"{API}/delta?limit=100")).json()["items"]}


# ── pattern suggestions ─────────────────────────────────────────────────────────────────────────


async def test_accepting_pattern_suggestions_in_bulk_makes_them_rules(client):
    await exclusions_suggested(client)

    r = await client.post(f"{API}/suggestions/bulk", json={"decision": "accept", "type": "exclude"})

    patterns = (await client.get(f"{API}/patterns")).json()
    assert (r.status_code, r.json()["decided"]) == (200, 2)
    assert sorted((p["type"], p["match"]) for p in patterns) == [("exclude", url(1)), ("exclude", url(2))]
    assert list(await rows(client)) == [url(3)]  # the rules decide the excluded pages: no delta
    assert (await client.get(f"{API}/suggestions")).json() == []
    assert "suggestion.bulk_accept" in (await client.get(f"{TAB}activity")).text


async def test_rejecting_pattern_suggestions_in_bulk_applies_nothing_and_records_who(client):
    await exclusions_suggested(client)

    r = await client.post(f"{API}/suggestions/bulk", json={"decision": "reject"})

    rejected = (await client.get(f"{API}/suggestions?state=rejected")).json()
    assert (r.status_code, r.json()["decided"]) == (200, 2)
    assert (await client.get(f"{API}/patterns")).json() == []
    assert [s["decided_by"] for s in rejected] == ["anonymous", "anonymous"]


@pytest.mark.parametrize(("body", "status"), [
    ({"decision": "accept", "type": "title"}, 409),  # none of that type pending
    ({"decision": "maybe"}, 422),
], ids=["no suggestion of that type", "unknown decision"])
async def test_a_bulk_pattern_decision_on_nothing_or_an_unknown_decision_is_refused(client, body, status):
    await exclusions_suggested(client)

    assert (await client.post(f"{API}/suggestions/bulk", json=body)).status_code == status


async def test_a_bulk_pattern_decision_with_nothing_pending_is_refused(client):
    await crawl(client)

    r = await client.post(f"{API}/suggestions/bulk", json={"decision": "accept"})

    assert (r.status_code, r.json()["detail"]) == (409, "no pending suggestions")


# ── AI metadata suggestions ─────────────────────────────────────────────────────────────────────


async def test_accept_all_passes_over_a_field_an_sme_rule_decides(client):
    """Accepting would write a newer exact-URL rule over the one the SME typed after the metadata
    came in; that row keeps its suggestion, to be accepted row by row."""
    await metadata_suggested(client)
    await client.post(f"{API}/urls", json={"url": url(2), "type": "title", "value": "Manual"})

    r = await client.post(f"{API}/ai/bulk", json={"decision": "accept", "field": "title"})

    got = await rows(client)
    titles = {p["match"]: p["source"] for p in (await client.get(f"{API}/patterns")).json() if p["type"] == "title"}
    assert (r.status_code, r.json()["decided"]) == (200, PAGES - 1)
    assert (got[url(2)]["title"], got[url(2)]["title_ai"]) == ("Manual", "AI title 2")
    assert [got[url(i)]["title"] for i in (1, 3)] == ["AI title 1", "AI title 3"]
    assert titles == {url(1): "llm", url(2): "sme", url(3): "llm"}


async def test_a_glob_rule_typed_after_the_suggestions_holds_its_rows_back_and_the_page_says_so(client):
    await metadata_suggested(client)
    await client.post(f"{API}/patterns", json={"type": "division", "match": "*/p1", "value": "Earth Science"})

    page = (await client.get(f"{TAB}curate")).text

    assert "not in Accept all" in page and "1 on your own rules" in page
    assert f'hx-confirm="Apply {PAGES - 1} AI divisions' in page


async def test_accept_all_with_only_held_back_suggestions_left_says_why(client):
    await metadata_suggested(client)
    await client.post(f"{API}/patterns", json={"type": "division", "match": "*/p*", "value": "Earth Science"})

    r = await client.post(f"{API}/ai/bulk", json={"decision": "accept", "field": "division"})

    assert r.status_code == 409 and "your own rules decide" in r.json()["detail"]


async def test_a_rows_own_accept_still_takes_a_held_back_suggestion(client):
    await metadata_suggested(client)
    await client.post(f"{API}/urls", json={"url": url(2), "type": "title", "value": "Manual"})

    r = await client.post(f"{API}/ai/bulk", json={"decision": "accept", "url": url(2), "field": "title"})

    got = (await rows(client))[url(2)]
    assert r.status_code == 200 and (got["title"], got["title_ai"]) == ("AI title 2", None)


async def test_a_bulk_reject_clears_every_suggestion_of_the_field_held_back_or_not(client):
    await metadata_suggested(client)
    await client.post(f"{API}/patterns", json={"type": "document_type", "match": "*/p1", "value": "Images"})

    r = await client.post(f"{API}/ai/bulk", json={"decision": "reject", "field": "document_type"})

    got = await rows(client)
    assert (r.status_code, r.json()["decided"]) == (200, PAGES)
    assert [(d["document_type"], d["document_type_ai"]) for d in got.values()] == [
        ("Images", None), (None, None), (None, None)]


@pytest.mark.parametrize(("body", "status"), [
    ({"decision": "accept", "field": "bogus"}, 422),
    ({"decision": "accept", "field": "title"}, 409),  # nothing suggested yet
], ids=["unknown field", "nothing to accept"])
async def test_an_ai_bulk_decision_on_an_unknown_field_or_nothing_is_refused(client, body, status):
    await crawl(client)

    assert (await client.post(f"{API}/ai/bulk", json=body)).status_code == status


async def test_the_bulk_bar_shows_only_while_suggestions_are_pending(client):
    await metadata_suggested(client)
    pending = (await client.get(f"{TAB}curate")).text
    await client.post(f"{API}/ai/bulk", json={"decision": "reject"})

    decided = (await client.get(f"{TAB}curate")).text

    assert "AI suggestions to review" in pending and "Reject all" in pending
    assert "AI suggestions to review" not in decided and "Reject all" not in decided


# ── the Curate step ─────────────────────────────────────────────────────────────────────────────


async def test_suggest_metadata_waits_until_the_pattern_suggestions_are_decided(client):
    """Exclusions first: an excluded URL is never classified, so the metadata button is gated, with
    the reason, while a pattern suggestion is pending."""
    await exclusions_suggested(client)

    page = (await client.get(f"{TAB}curate")).text
    refused = await client.post(f"{API}/suggest/metadata")
    await client.post(f"{API}/suggestions/bulk", json={"decision": "reject"})
    after = (await client.get(f"{TAB}curate")).text

    assert "Decide the" in page and "pending suggestion" in page
    assert refused.status_code == 409 and "pending" in refused.text
    assert "Continue to metadata" in after and "Decide the" not in after


async def test_the_curate_step_says_how_many_calls_suggest_patterns_will_make(client):
    await crawl(client, pages=ONE_MORE_THAN_A_BATCH)

    page = (await client.get(f"{TAB}curate")).text

    assert f"({ONE_MORE_THAN_A_BATCH} delta URLs · 2 calls)" in page


async def test_a_finished_suggest_patterns_says_how_many_urls_and_calls_it_took(client):
    await crawl(client)
    await client.app.state.db.insert_job(JobRun(
        collection_id=CID, kind=JobKind.LLM_PATTERNS, state=JobState.SUCCEEDED,
        progress={"llm": "patterns", "suggestions": 2, "urls": ONE_MORE_THAN_A_BATCH, "calls": 2, "done": 2}))

    page = (await client.get(f"{TAB}curate")).text

    assert f"{ONE_MORE_THAN_A_BATCH} URLs in 2 calls" in page


SUGGESTIONS = 60
PER_PAGE = 25


async def many_exclusions(c) -> None:
    await crawl(c)
    await c.app.state.db.add_pattern_suggestions(
        CID, [{"type": "exclude", "match": f"*/s{i:02d}*", "matches": 0} for i in range(SUGGESTIONS)])


async def test_collapsed_cards_show_the_first_fifty_with_expand(client):
    await many_exclusions(client)

    page = (await client.get(f"{TAB}curate")).text

    assert f"Showing the first 50 of {SUGGESTIONS} suggestions" in page and "⤢ Expand</a>" in page
    assert 'id="promote"' in page and "⤡ Collapse" not in page


async def test_an_expanded_card_shows_only_itself_paged(client):
    await many_exclusions(client)

    page = (await client.get(f"{TAB}curate&focus=exclusions&per={PER_PAGE}&page=2")).text

    assert 'id="exclusions"' in page and 'id="metadata"' not in page and 'id="promote"' not in page
    assert "⤡ Collapse" in page and 'href="/collections/ex.org?tab=curate#exclusions"' in page
    assert f"26–50 of {SUGGESTIONS}" in page and "*/s25*" in page and "*/s24*" not in page and "*/s50*" not in page
    assert f"focus=exclusions&per={PER_PAGE}&page=3" in page


async def test_a_page_past_the_end_shows_the_last_page(client):
    """Rows can be decided meanwhile, so a page number past the end is the last page."""
    await many_exclusions(client)

    page = (await client.get(f"{TAB}curate&focus=exclusions&per={PER_PAGE}&page=9")).text

    assert f"51–60 of {SUGGESTIONS}" in page and 'data-page="3"' in page


async def test_an_unknown_focus_shows_the_whole_step(client):
    await many_exclusions(client)

    page = (await client.get(f"{TAB}curate&focus=bogus")).text

    assert 'id="promote"' in page and 'id="exclusions"' in page
