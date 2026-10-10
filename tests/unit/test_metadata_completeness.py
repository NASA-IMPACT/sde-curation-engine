"""Nothing reaches the curated set blank: Suggest metadata answers every field (guesses at low
confidence), rows left without a field are asked again, promote refuses a delta URL without a
division, a document type or any title at all (a page with no title rule keeps its scraped title,
which is what the export indexes it under), and the metadata review filters by confidence and field."""
from sde_curation.llm.tasks import METADATA_SYSTEM

CID = "ex.org"
API = f"/api/collections/{CID}"


def url(i):
    return f"https://{CID}/p{i}"


async def delta(c, i):
    return (await c.get(f"{API}/delta", params={"q": url(i)})).json()["items"][0]


# ── Suggest metadata answers every field ───────────────────────────────────


def test_the_prompt_never_allows_a_blank_field():
    assert "No field is ever null or empty" in METADATA_SYSTEM
    assert "use null" not in METADATA_SYSTEM and "prefer a null value" not in METADATA_SYSTEM
    assert "Null only if" not in METADATA_SYSTEM
