"""The curator's review actions through the real routes and PostgreSQL: accepting, editing and
rejecting exclusion suggestions and AI metadata, what each writes (rules, sources, who edited,
audit) and what each refuses; and the review pages that show them (TEST-STRATEGY-2026-10-09.md, P4).

The jobs that produce suggestions are unit-tested (tests/unit/test_g4_llm_jobs.py); here the
suggestions are written straight into the tables, so each test exercises only the route and the SQL
behind it. The collection: 8 crawled pages p1..p8 of ex.org, Start curating pressed.
"""

import pytest

from sde_curation.models import DumpUrl

CID = "ex.org"
API = f"/api/collections/{CID}"
PAGES = 8
SUGGESTED_PAGE = f"https://{CID}/p8"  # the exclusion the model suggests
GLOBAL_GLOB = "*/p7*"  # an exclusion from the global list (it matches one page)


def url(i: int) -> str:
    return f"https://{CID}/p{i}"


async def started(c, *, division: str | None = "Heliophysics") -> None:
    """Create ex.org, store its crawl, press Start curating."""
    body = {"seed_url": f"https://{CID}", "name": CID, "max_pages": 10} | ({"division": division} if division else {})
    assert (await c.post("/api/collections", json=body)).status_code == 201
    await c.app.state.db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(i), scraped_title=f"Page {i}",
                                                    full_text=f"text of page {i}") for i in range(1, PAGES + 1)])
    assert (await c.post(f"{API}/recompute")).status_code == 200


async def with_suggestions(c) -> dict[str, int]:
    """started(), plus a model suggestion and a global-list suggestion, pending. {match: id}."""
    await started(c)
    await c.app.state.db.add_pattern_suggestions(CID, [
        {"type": "exclude", "match": SUGGESTED_PAGE, "rationale": "the model's", "matches": 1},
        {"type": "exclude", "match": GLOBAL_GLOB, "rationale": "the list's", "matches": 1, "source": "global"},
    ])
    return {s["match"]: s["id"] for s in (await c.get(f"{API}/suggestions")).json()}


async def with_ai(c, *, division: str | None = "Heliophysics") -> None:
    """started(), plus an AI answer on every page, as Suggest metadata saves it."""
    await started(c, division=division)
    await c.app.state.db.set_delta_ai(CID, [
        {"url": url(i), "title": f"Page {i}", "title_conf": "high", "division": "Astrophysics", "division_conf": "low",
         "document_type": "Documentation", "document_type_conf": "low", "model": "fake"} for i in range(1, PAGES + 1)])


async def delta(c, u: str) -> dict:
    return next(d for d in (await c.get(f"{API}/delta?limit=100")).json()["items"] if d["url"] == u)


async def rules(c) -> dict[str, str]:
    return {p["match"]: p["source"] for p in (await c.get(f"{API}/patterns")).json()}


async def audit_actions(c) -> list[str]:
    return [a["action"] for a in (await c.get(f"{API}/audit")).json()]


# ── exclusion suggestions ──────────────────────────────────────────────


async def test_the_suggestions_list_shows_the_global_lists_rows_first_and_marks_them(client):
    c = client
    await with_suggestions(c)

    listed = [s["source"] for s in (await c.get(f"{API}/suggestions")).json()]
    page = (await c.get(f"/collections/{CID}?tab=curate")).text

    assert listed == ["global", "llm"]
    assert "src-global" in page


async def test_accepting_an_exclusion_suggestion_makes_an_ai_rule_that_takes_its_page_out(client):
    c = client
    ids = await with_suggestions(c)

    r = await c.post(f"{API}/suggestions/{ids[SUGGESTED_PAGE]}/accept")

    patterns = (await c.get(f"{API}/patterns")).json()
    dumped = (await c.get(f"{API}/dump?q=p8")).json()["items"][0]
    assert r.status_code == 200
    assert [(p["type"], p["match"], p["source"], p["matches"]) for p in patterns] == [
        ("exclude", SUGGESTED_PAGE, "llm", 1)]
    assert (await c.get(f"{API}/delta?q=p8")).json()["total"] == 0  # decided by the rule, not a delta URL
    assert dumped["excluded"] is True
    assert "suggestion.accept" in await audit_actions(c)


async def test_a_rejected_suggestion_leaves_no_rule(client):
    c = client
    ids = await with_suggestions(c)

    r = await c.post(f"{API}/suggestions/{ids[SUGGESTED_PAGE]}/reject")

    assert r.status_code == 200
    assert await rules(c) == {}
    assert [s["match"] for s in (await c.get(f"{API}/suggestions?state=rejected")).json()] == [SUGGESTED_PAGE]


@pytest.mark.parametrize(("path", "body", "status"), [
    ("{sid}/maybe", None, 422),  # neither accept nor reject
    ("9999/accept", None, 404),  # no such suggestion
    ("{sid}/accept", {"match": "   "}, 422),  # an edited glob left blank
], ids=["unknown decision", "unknown suggestion", "blank edited glob"])
async def test_a_suggestion_decision_that_cannot_apply_is_refused_and_leaves_it_pending(client, path, body, status):
    c = client
    ids = await with_suggestions(c)

    r = await c.post(f"{API}/suggestions/" + path.format(sid=ids[SUGGESTED_PAGE]), json=body)

    assert r.status_code == status
    assert ids[SUGGESTED_PAGE] in {s["id"] for s in (await c.get(f"{API}/suggestions")).json()}  # still pending
    assert await rules(c) == {}


async def test_a_suggestion_already_decided_is_refused(client):
    c = client
    ids = await with_suggestions(c)
    await c.post(f"{API}/suggestions/{ids[SUGGESTED_PAGE]}/accept")

    r = await c.post(f"{API}/suggestions/{ids[SUGGESTED_PAGE]}/accept")

    assert (r.status_code, r.json()["detail"]) == (409, "suggestion already accepted")


async def test_an_edited_suggestion_becomes_an_edited_ai_rule_and_records_what_was_applied(client):
    c = client
    ids = await with_suggestions(c)
    edited = f"https://{CID}/p*8"

    r = await c.post(f"{API}/suggestions/{ids[SUGGESTED_PAGE]}/accept", json={"match": edited})

    decided = (await c.get(f"{API}/suggestions?state=accepted")).json()[0]
    assert r.json() == {"id": ids[SUGGESTED_PAGE], "state": "accepted", "accepted_as": edited}
    assert await rules(c) == {edited: "llm_edited"}
    assert (decided["match"], decided["accepted_as"]) == (SUGGESTED_PAGE, edited)
    assert "suggestion.accept_edited" in await audit_actions(c)


async def test_a_suggestion_edited_into_a_rule_that_exists_is_refused_and_stays_pending(client):
    c = client
    ids = await with_suggestions(c)
    await c.post(f"{API}/suggestions/{ids[SUGGESTED_PAGE]}/accept")

    r = await c.post(f"{API}/suggestions/{ids[GLOBAL_GLOB]}/accept", json={"match": SUGGESTED_PAGE})

    assert r.status_code == 409
    assert [s["id"] for s in (await c.get(f"{API}/suggestions")).json()] == [ids[GLOBAL_GLOB]]


async def test_accepting_every_suggestion_keeps_the_global_lists_rules_marked_global(client):
    c = client
    await with_suggestions(c)

    r = await c.post(f"{API}/suggestions/bulk", json={"decision": "accept"})

    assert r.status_code == 200
    assert await rules(c) == {SUGGESTED_PAGE: "llm", GLOBAL_GLOB: "global"}


# ── AI metadata ────────────────────────────────────────────────────────


async def test_accepting_an_ai_value_as_suggested_makes_it_the_pages_value_set_by_the_ai(client):
    c = client
    await with_ai(c, division=None)  # a collection division is the SME's: p2 would read "mixed"

    r = await c.post(f"{API}/ai/accept", json={"url": url(2), "field": "title"})

    d = await delta(c, url(2))
    assert (r.status_code, "value" in r.json()) == (200, False)
    assert (d["title"], d["title_ai"], d["edited_by"]) == ("Page 2", None, "ai")
    assert await rules(c) == {url(2): "llm"}
    assert "ai.accept" in await audit_actions(c)


async def test_accepting_an_edited_ai_value_makes_it_the_curators(client):
    c = client
    await with_ai(c, division=None)

    r = await c.post(f"{API}/ai/accept", json={"url": url(3), "field": "title", "value": "Three, by hand"})

    d = await delta(c, url(3))
    assert r.json()["value"] == "Three, by hand"
    assert (d["title"], d["title_ai"], d["edited_by"]) == ("Three, by hand", None, "sme")
    assert await rules(c) == {url(3): "llm_edited"}
    assert "ai.accept_edited" in await audit_actions(c)


async def test_a_dismissed_ai_value_is_cleared_leaves_the_page_alone_and_is_not_asked_for_again(client):
    c = client
    await with_ai(c)

    r = await c.post(f"{API}/ai/reject", json={"url": url(4), "field": "document_type"})

    d = await delta(c, url(4))
    assert r.status_code == 200
    assert (d["document_type_ai"], d["document_type"]) == (None, None)
    assert await c.app.state.db.count_deltas_for_llm(CID) == 0  # Suggest metadata does not ask p4 again


@pytest.mark.parametrize(("decision", "body", "status"), [
    ("accept", {"url": url(2), "field": "document_type", "value": "Nope"}, 422),  # not a document type
    ("accept", {"url": url(2), "field": "bogus"}, 422),
    ("maybe", {"url": url(2), "field": "title"}, 422),
    ("accept", {"url": f"https://{CID}/none", "field": "title"}, 404),
], ids=["edited value not in the list", "unknown field", "unknown decision", "not a delta URL"])
async def test_an_ai_decision_that_cannot_apply_is_refused_and_keeps_the_suggestion(client, decision, body, status):
    c = client
    await with_ai(c)

    r = await c.post(f"{API}/ai/{decision}", json=body)

    assert r.status_code == status
    assert (await delta(c, url(2)))["document_type_ai"] == "Documentation"
    assert await rules(c) == {}


async def test_an_ai_decision_on_a_field_with_no_suggestion_left_is_refused(client):
    c = client
    await with_ai(c)
    await c.post(f"{API}/ai/reject", json={"url": url(2), "field": "title"})

    one = await c.post(f"{API}/ai/accept", json={"url": url(2), "field": "title"})
    await c.post(f"{API}/ai/bulk", json={"decision": "accept"})
    every = await c.post(f"{API}/ai/bulk", json={"decision": "accept"})

    assert (one.status_code, one.json()["detail"]) == (409, "no suggestion for that field")
    assert (every.status_code, every.json()["detail"]) == (409, "no AI suggestions to accept")


async def test_the_delta_tab_shows_ai_values_with_their_confidence_and_filters_by_it(client):
    c = client
    await with_ai(c)
    await c.app.state.db.set_delta_ai_errors(CID, [(url(3), "LLMRetryable: 429 too many requests")])

    page = (await c.get(f"/collections/{CID}?tab=delta")).text
    low = (await c.get(f"/collections/{CID}?tab=delta&ai=low")).text
    failed = (await c.get(f"/collections/{CID}?tab=delta&ai=failed")).text
    curate = (await c.get(f"/collections/{CID}?tab=curate")).text

    assert "AI: Page 2" in page and 'class="ai conf-high"' in page and 'class="ai conf-low"' in page
    assert "AI: Documentation" in low
    assert url(3) in failed and url(2) not in failed and "AI metadata failed" in failed
    assert "1 URL failed</a> to classify" in curate


# ── who edited a page ──────────────────────────────────────────────────


async def test_the_edited_by_filter_narrows_the_delta_and_curated_tables_and_their_csv(client):
    """AI values accepted on every page, then an SME division on p2: p2 is "mixed", the rest "ai",
    on the delta URLs and, after promote, on the curated URLs."""
    c = client
    await with_ai(c, division=None)
    for field in ("title", "division", "document_type"):
        await c.post(f"{API}/ai/bulk", json={"decision": "accept", "field": field})
    await c.post(f"{API}/urls", json={"url": url(2), "type": "division", "value": "Earth Science"})

    only_ai = (await c.get(f"/collections/{CID}?tab=delta&edited=ai")).text
    delta_csv = (await c.get(f"/collections/{CID}/urls/delta?format=csv&edited=mixed")).text.splitlines()
    assert (await c.post(f"{API}/promote")).status_code == 200
    only_mixed = (await c.get(f"/collections/{CID}?tab=curated&edited=mixed")).text
    curated_csv = (await c.get(f"/collections/{CID}/urls/curated?format=csv&edited=ai")).text.splitlines()

    assert url(4) in only_ai and url(2) not in only_ai
    assert "edited_by" in delta_csv[0] and len(delta_csv) == 1 + 1
    assert url(2) in only_mixed and url(4) not in only_mixed
    assert len(curated_csv) == 1 + PAGES - 1
