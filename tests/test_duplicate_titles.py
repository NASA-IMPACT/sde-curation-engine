"""Title + document type combinations two pages of one collection would both be indexed under:
counted, flagged per row, filterable, and sent back to the LLM (fake provider) for new titles only."""

from sde_curation.llm.base import LLMError
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import title_siblings
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

    # promoted: the curated rows count too, and a new delta URL that collides with them is flagged
    await client.post(f"/api/collections/{CID}/ai/reject", json={"url": urls("/d")[0], "field": "title"})
    assert (await client.post(f"/api/collections/{CID}/promote")).status_code == 200
    assert await db.duplicate_title_counts(CID) == {"urls": 2, "titles": 1, "delta_urls": 0}
    await make(client, {"/a": "Mission Data", "/b": "mission data", "/c": "Other", "/d": "Solo", "/e": "MISSION DATA"})
    assert await db.duplicate_title_counts(CID) == {"urls": 3, "titles": 1, "delta_urls": 1}
    rows = (await db.list_deltas(CID, dup_title=True))[0]
    assert [r.url for r in rows] == urls("/e")
    assert sorted(r.url for r in (await db.list_curated(CID, dup_title=True))[0]) == urls("/a", "/c")
    info = await db.duplicate_titles_for(CID, urls("/e", "/d"))
    assert set(info) == set(urls("/e")) and info[urls("/e")[0]]["others"] == 2
    assert info[urls("/e")[0]]["sample"] == urls("/a", "/c")

    # the tables flag the row and filter on it; the Curate page says how many and offers the fix
    page = (await client.get(f"/collections/{CID}?tab=delta&dup=title")).text
    assert "⚠ same title + type ×3" in page and "https://ex.org/e" in page and "https://ex.org/d" not in page
    assert "same title + type ×3" in (await client.get(f"/collections/{CID}?tab=curated")).text
    curate = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert "3 URLs share 1 title + document type combination" in curate and "Regenerate duplicate titles" in curate and "suggest/titles" in curate
    csv = (await client.get(f"/collections/{CID}/urls/delta?format=csv&dup=title")).text
    assert csv.count("\n") == 2  # header + /e


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
    retitle = [call["user"] for call in llm.calls if call["schema"] == "TitleSuggestion"]
    assert len(retitle) == 2
    alpha = next(u for u in retitle if u.endswith("body of /x/alpha"))
    assert '"shared_title": "Same", "document_type": "Documentation", "pages_sharing_it": 2' in alpha
    assert a["document_type_ai"] == "Documentation" and a["document_type_ai_conf"] == "low"  # only the title is redone
    assert '"other_pages": [{"url": "https://ex.org/x/beta", "keeps_title": false}]' in alpha
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

    monkeypatch.setattr("sde_curation.jobs.suggest_distinct_title", broken)
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

    # where some pages of a group have a pending AI title, only those are asked again; the rest keep theirs
    for u in urls("/p0", "/p1"):
        await client.post(f"/api/collections/{CID}/ai/reject", json={"url": u, "field": "title"})
    assert (await delta(client, "/p0"))["title_ai_before"] is None  # dropped with the AI title
    await db.set_delta_ai_titles(CID, [{"url": urls("/p2")[0], "title": "Portal", "title_conf": "low"}])
    llm = client.app.state.jobs._llm = FakeProvider()
    await client.post(f"/api/collections/{CID}/suggest/titles")
    job = await wait_job(client, CID)
    assert job["progress"]["titles_total"] == 1 and job["progress"]["retitled"] == 1
    assert job["progress"]["still_duplicate"] == 2  # p0 and p1 still share "Portal": flagged for the SME
    assert len(llm.calls) == 1 and '"url": "https://ex.org/p2"' in llm.calls[0]["user"]
    assert '{"url": "https://ex.org/p0", "keeps_title": true}' in llm.calls[0]["user"]
    assert "2 still share one" in (await client.get(f"/collections/{CID}?tab=curate")).text


async def test_by_default_duplicates_are_flagged_with_the_titles_as_generated(client):
    db = client.app.state.db
    await make(client, {"/x/alpha": "Same - Ex", "/x/beta": "Same - Ex", "/x/gamma": "Unique - Ex"})
    llm = client.app.state.jobs._llm = FakeProvider()
    await client.post(f"/api/collections/{CID}/suggest/metadata")
    p = (await wait_job(client, CID))["progress"]
    assert p["classified"] == 3 and "titles_total" not in p
    assert not [c for c in llm.calls if c["schema"] == "TitleSuggestion"]  # nothing sent back on its own
    assert (await delta(client, "/x/alpha"))["title_ai"] == "Same"  # the title as generated
    curate = (await client.get(f"/collections/{CID}?tab=curate")).text
    assert "2 URLs share 1 title + document type combination" in curate and "same title + type ×2" in curate
    assert "Only 2: editing the titles by hand" in curate and "flagged with the title as generated" in curate

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
    sent = [c["user"] for c in llm.calls if c["schema"] == "TitleSuggestion"]
    assert len(sent) == 2 and not any(u.endswith("body of /image/two") for u in sent)
    assert all("image/two" not in u for u in sent)  # not listed as another page either
