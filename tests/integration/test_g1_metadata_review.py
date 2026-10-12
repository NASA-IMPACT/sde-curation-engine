"""Metadata review and the promote gate on the real database and the real pages: the confidence and
field filters, which rows Suggest metadata asks again, how the pages show the rows promote refuses
(blank fields, the General placeholder). The gate's rules and messages are unit-tested in
tests/unit/test_curation_service.py; the counts it reads in tests/integration/test_db_contract.py.
State is written straight to the database: no crawl, no LLM job."""

import re

from sde_curation.models import Division, DumpUrl

CID = "ex.org"
API = f"/api/collections/{CID}"
TAB = f"/collections/{CID}?tab="
PAGES = 4
# the suggestions `suggested` writes: titles high on every row, divisions medium on p2 and p3 and
# low on the rest, document types low on every row
MEDIUM_DIVISION_PAGES = (2, 3)
HIGH, MEDIUM, LOW = PAGES, len(MEDIUM_DIVISION_PAGES), 2 * PAGES - len(MEDIUM_DIVISION_PAGES)


def url(i: int) -> str:
    return f"https://{CID}/p{i}"


async def crawl(c, titles: dict[int, str | None] | None = None, division: str | None = None) -> None:
    """A collection whose crawl is p1..pN (scraped titles "Page i" unless given), queued for curation."""
    body = {"seed_url": f"https://{CID}", "name": "Ex", "max_pages": 10, **({"division": division} if division else {})}
    await c.post("/api/collections", json=body)
    titles = titles or {i: f"Page {i}" for i in range(1, PAGES + 1)}
    await c.app.state.db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(i), scraped_title=t,
                                                    full_text=f"text of page {i}") for i, t in titles.items()])
    assert (await c.post(f"{API}/recompute")).status_code == 200


async def suggested(c) -> None:
    await crawl(c)
    await c.app.state.db.set_delta_ai(CID, [{
        "url": url(i), "model": "fake",
        "title": f"Page {i}", "title_conf": "high",
        "division": "Heliophysics" if i in MEDIUM_DIVISION_PAGES else "Astrophysics",
        "division_conf": "medium" if i in MEDIUM_DIVISION_PAGES else "low",
        "document_type": "Documentation", "document_type_conf": "low",
    } for i in range(1, PAGES + 1)])


async def delta(c, i: int) -> dict:
    return (await c.get(f"{API}/delta", params={"q": url(i)})).json()["items"][0]


# ── review by confidence and field ──────────────────────────────────────────────────────────────


async def test_the_review_counts_the_suggestions_by_confidence(client):
    await suggested(client)

    page = (await client.get(f"{TAB}curate")).text

    assert f"all {HIGH + MEDIUM + LOW}" in page
    assert f">high {HIGH}<" in page and f">medium {MEDIUM}<" in page and f">low {LOW}<" in page
    assert "accept these" not in page  # no filter, no filtered bulk


async def test_a_confidence_filter_lists_only_those_suggestions_and_offers_to_accept_them(client):
    await suggested(client)

    page = (await client.get(f"{TAB}curate&conf=medium")).text

    assert url(2) in page and url(3) in page and url(4) not in page
    assert "2 URLs · 2 medium-confidence suggestions" in page and "✓ accept these 2" in page
    assert '"conf": "medium"' in page and "clear filter" in page


async def test_a_confidence_and_field_filter_narrows_to_one_field_and_survives_expand(client):
    await suggested(client)

    page = (await client.get(f"{TAB}curate&conf=low&field=division")).text
    empty = (await client.get(f"{TAB}curate&conf=medium&field=title")).text

    assert url(4) in page and url(2) not in page and "2 URLs · 2 low-confidence division suggestions" in page
    assert "focus=metadata&conf=low&field=division" in page
    assert "No medium-confidence title suggestions left to review" in empty


async def test_a_filtered_bulk_accept_decides_exactly_what_the_filter_names(client):
    await suggested(client)

    r = await client.post(f"{API}/ai/bulk", json={"decision": "accept", "conf": "medium", "field": "division"})

    d2, d4 = await delta(client, 2), await delta(client, 4)
    assert (r.status_code, r.json()["decided"]) == (200, len(MEDIUM_DIVISION_PAGES))
    assert (d2["division"], d2["division_ai"]) == ("Heliophysics", None)
    assert d2["title_ai"] and d2["document_type_ai"]  # the other suggestions on the row stay
    assert (d4["division"], d4["division_ai"]) == (None, "Astrophysics")


async def test_a_filtered_bulk_reject_leaves_the_other_confidences(client):
    await suggested(client)

    r = await client.post(f"{API}/ai/bulk", json={"decision": "reject", "conf": "low"})

    items = (await client.get(f"{API}/delta?limit=100")).json()["items"]
    assert (r.status_code, r.json()["decided"]) == (200, LOW)
    assert all(d["title_ai"] for d in items)
    assert [d["division_ai"] for d in items if d["division_ai"]] == ["Heliophysics"] * MEDIUM
    assert not any(d["document_type_ai"] for d in items)


async def test_a_bulk_decision_on_nothing_or_on_an_unknown_confidence_is_refused(client):
    await suggested(client)
    await client.post(f"{API}/ai/bulk", json={"decision": "reject", "conf": "low"})

    nothing = await client.post(f"{API}/ai/bulk", json={"decision": "accept", "conf": "low"})
    unknown = await client.post(f"{API}/ai/bulk", json={"decision": "accept", "conf": "certain"})

    assert (nothing.status_code, unknown.status_code) == (409, 422)


# ── which rows Suggest metadata asks again ──────────────────────────────────────────────────────


async def test_a_field_an_answer_left_blank_is_asked_again_unless_dismissed_or_set_by_a_rule(client):
    """Answers from before every field was required: a value missing with its confidence recorded
    is asked again; one the SME dismissed (no confidence) is not; a rule that sets the field answers it."""
    db = client.app.state.db
    await suggested(client)
    assert await db.count_deltas_for_llm(CID) == 0
    await db.execute("UPDATE delta_urls SET division_ai=NULL WHERE collection_id=%s AND url=%s", (CID, url(2)))
    await db.execute("UPDATE delta_urls SET title_ai=NULL WHERE collection_id=%s AND url=%s", (CID, url(4)))
    blank_answers = await db.count_deltas_for_llm(CID)

    await client.post(f"{API}/ai/reject", json={"url": url(3), "field": "division"})
    after_dismissal = await db.count_deltas_for_llm(CID)
    await client.post(f"{API}/urls", json={"url": url(2), "type": "division", "value": "Earth Science"})

    assert (blank_answers, after_dismissal, await db.count_deltas_for_llm(CID)) == (2, 2, 1)


# ── the promote gate, as the pages show it ──────────────────────────────────────────────────────


async def test_the_missing_filter_lists_only_the_rows_promote_refuses(client):
    db = client.app.state.db
    await crawl(client)
    await client.post(f"{API}/patterns", json={"type": "document_type", "match": "*", "value": "Data"})
    for i in range(1, PAGES + 1):
        if i != 2:
            await client.post(f"{API}/urls", json={"url": url(i), "type": "division", "value": "Earth Science"})

    rows, _ = await db.list_deltas(CID, incomplete=True)
    page = (await client.get(f"{TAB}delta&missing=true")).text
    csv = (await client.get(f"/collections/{CID}/urls/delta?format=csv&missing=true")).text.splitlines()

    assert [r.url for r in rows] == [url(2)]
    assert url(2) in page and url(3) not in page
    assert len(csv) == 2 and url(2) in csv[1]


async def test_the_pages_say_how_many_rows_promote_refuses_and_why(client):
    await crawl(client)

    curate = (await client.get(f"{TAB}curate")).text
    table = (await client.get(f"{TAB}delta")).text

    assert f"⛔ {PAGES} delta URLs cannot be promoted yet" in curate
    assert f"{PAGES} without a division · {PAGES} without a document type" in curate
    assert (f'disabled title="{PAGES} delta URLs still lack a title, division or document type,'
            ' or share a title with another page"') in curate
    assert f"⛔ {PAGES} cannot be promoted yet" in table


async def test_the_pages_drop_the_warning_once_every_row_can_be_promoted(client):
    await crawl(client)
    for field, value in (("division", "Earth Science"), ("document_type", "Data")):
        await client.post(f"{API}/patterns", json={"type": field, "match": "*", "value": value})

    curate = (await client.get(f"{TAB}curate")).text

    assert "cannot be promoted yet" not in curate


async def test_a_page_with_no_title_rule_shows_its_scraped_title(client):
    """The export indexes it under the scraped title, so the cell shows that title, marked as
    scraped, rather than an empty dash."""
    await crawl(client, titles={1: "Page 1"})

    table = (await client.get(f"{TAB}delta")).text

    assert "Page 1" in table and ">scraped<" in table


# ── the General placeholder ─────────────────────────────────────────────────────────────────────


async def on_general(c) -> None:
    """p2 is put back on the General placeholder; every other field of every row is filled."""
    await crawl(c, division=Division.HELIOPHYSICS)
    await c.post(f"{API}/patterns", json={"type": "document_type", "match": "*", "value": "Data"})
    r = await c.post(f"{API}/urls", json={"url": url(2), "type": "division", "value": "General"})
    assert r.status_code == 200, r.text


async def test_a_row_on_general_is_shown_as_not_promotable(client):
    await on_general(client)

    missing = (await client.get(f"{TAB}delta&missing=true")).text
    curate = (await client.get(f"{TAB}curate")).text

    assert "not promotable" in missing and url(2) in missing and url(3) not in missing
    assert "still on <b>General</b>" in curate


async def test_a_row_on_general_shows_it_in_its_cell_though_it_cannot_be_picked_again(client):
    await on_general(client)

    table = (await client.get(f"{TAB}delta")).text

    assert "<option selected>General</option>" in table
    assert table.count("<option selected>General</option>") == 1


async def test_every_write_path_still_takes_general(client):
    """The guard is promote, not the write paths: the collection, a rule and a per-URL edit all
    take the placeholder."""
    await crawl(client, division=Division.HELIOPHYSICS)

    codes = [(await client.post(path, json=body)).status_code for path, body in (
        (f"{API}/division", {"division": "General"}),
        (f"{API}/patterns", {"type": "division", "match": "*/p2", "value": "General"}),
        (f"{API}/urls", {"url": url(3), "type": "division", "value": "General"}),
    )]

    assert codes == [200, 201, 200]


async def test_the_add_collection_form_posts_general_as_a_division(client):
    """The option reads "General — not assigned"; without its own value the browser would post the
    label, which is not a division."""
    form = (await client.get("/")).text
    r = await client.post("/collections", data={"seed_url": "https://two.org", "name": "Two",
                                                 "division": "General", "max_pages": 3})

    select = re.search(r'<select[^>]*name="division".*?</select>', form, re.DOTALL).group(0)
    assert all("value=" in option for option in re.findall(r"<option[^>]*>", select))
    assert r.status_code in (200, 303), r.text
    assert (await client.get("/api/collections/two.org")).json()["division"] == "General"


# every <select> a URL's own division is set through: the cells in the URL tables and under
# Curate › Metadata (the filter selects above the tables are not cells)
CELL_SELECT = re.compile(r'<select class="cell\b.*?</select>', re.DOTALL)


async def test_the_cells_and_the_rule_values_offer_only_the_five_divisions(client):
    """A URL is curated into one of the five: General is offered for the collection only."""
    await suggested(client)

    delta = (await client.get(f"{TAB}delta")).text
    curate = (await client.get(f"{TAB}curate")).text

    cells = CELL_SELECT.findall(delta) + CELL_SELECT.findall(curate)
    values = re.search(r'<datalist id="values">.*?</datalist>', curate, re.DOTALL).group(0)
    assert any("Astrophysics" in cell for cell in cells)
    assert not any("General" in cell for cell in cells)
    assert "Planetary Science" in values and "General" not in values
