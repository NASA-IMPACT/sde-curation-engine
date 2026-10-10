"""The index chips and steps the pages derive from the latest index runs (web/app.py with_validation):
"needs re-indexing" when the latest test run did not pass or the curated set moved past it,
"prod not validated" when a prod publish wrote documents its check did not pass, and steps 5/6 that
show those runs. The runs are written straight into PostgreSQL: the jobs that write them are
unit-tested (tests/unit/test_jobs_index.py), so no indexer subprocess is needed here. Replaces the
page checks of the old e2e test_validate, test_index, test_curated_counts and test_review_round
(TEST-STRATEGY-2026-10-09.md, P4)."""

from sde_curation.models import DumpUrl, IndexRun, JobKind, JobRun, JobState, Status

CID = "ex.org"
API = f"/api/collections/{CID}"
PAGES = 8
REINDEX = ">⚠ needs re-indexing<"
PROD_CHIP = ">⚠ prod not validated<"
REINDEX_ROW_CHIP = f'href="/collections/{CID}?step=config_generated" title="'  # the dashboard row's chip
PROD_ROW_CHIP = f'href="/collections/{CID}?step=live" title="The latest prod publish'
STALE_TIP = "The curated URLs changed after the last test index run"


def url(i: int) -> str:
    return f"https://{CID}/p{i}"


def report(expected: int, indexed: int) -> dict:
    return {"expected_count": expected, "indexed_count": indexed, "count_matches": expected == indexed,
            "title_match_rate": indexed / expected}


async def promoted(c) -> None:
    """ex.org with 8 pages, curated (every page has a title, a division and a document type)."""
    r = await c.post("/api/collections", json={"seed_url": f"https://{CID}", "name": CID, "max_pages": 10,
                                               "division": "Heliophysics"})
    assert r.status_code == 201, r.text
    await c.app.state.db.replace_dump(CID, [DumpUrl(collection_id=CID, url=url(i), scraped_title=f"Page {i}",
                                                    full_text=f"text {i}") for i in range(1, PAGES + 1)])
    assert (await c.post(f"{API}/recompute")).status_code == 200
    r = await c.post(f"{API}/patterns", json={"type": "document_type", "match": "*", "value": "Documentation"})
    assert r.status_code == 201, r.text
    assert (await c.post(f"{API}/promote")).status_code == 200


async def index_run(c, target: str, *, visible: int = PAGES, state: str = "succeeded", validated: bool = True,
                    run_id: str | None = None, move_to: Status | None = None, **fields) -> IndexRun:
    """A finished run of `target`, as the index jobs record it; `move_to` moves the collection too."""
    db = c.app.state.db
    run = await db.insert_index_run(IndexRun(run_id=run_id or f"{target}-1", collection_id=CID, target=target,
                                             exported=PAGES))
    run.state, run.validated_by = state, "direct" if validated else None
    run.validation = report(PAGES, visible) if validated else None
    for k, v in fields.items():
        setattr(run, k, v)
    await db.update_index_run(run)
    if move_to is not None:
        await db.set_status(CID, move_to, force=True, actor="system")
    return run


async def header(c) -> str:
    return (await c.get(f"/collections/{CID}/header")).text


# ── needs re-indexing ──────────────────────────────────────────────────


async def test_a_collection_never_indexed_has_nothing_to_re_index(client):
    c = client
    await promoted(c)

    await c.post(f"{API}/urls", json={"url": url(4), "type": "exclude"})

    assert REINDEX not in await header(c)


async def test_a_curated_set_changed_after_the_last_test_run_needs_re_indexing(client):
    """An exclude rule takes a curated page out in place: the index still holds it."""
    c = client
    await promoted(c)
    await index_run(c, "test", move_to=Status.CONFIG_GENERATED)
    up_to_date = await header(c)

    await c.post(f"{API}/urls", json={"url": url(4), "type": "exclude"})

    assert REINDEX not in up_to_date
    assert REINDEX in await header(c) and STALE_TIP in await header(c)
    assert REINDEX in (await c.get("/?flag=needs_reindexing")).text


async def test_a_test_run_after_the_change_brings_the_chip_down(client):
    c = client
    await promoted(c)
    await index_run(c, "test", run_id="test-1", move_to=Status.CONFIG_GENERATED)
    await c.post(f"{API}/urls", json={"url": url(4), "type": "exclude"})

    await index_run(c, "test", run_id="test-2")

    assert REINDEX not in await header(c)


async def test_an_include_raises_the_chip_only_once_the_page_is_promoted_back(client):
    """✓ include is the way back in: a delta URL to review; the curated set moves on promote."""
    c = client
    await promoted(c)
    await c.post(f"{API}/urls", json={"url": url(4), "type": "exclude"})
    await index_run(c, "test", move_to=Status.CONFIG_GENERATED)

    await c.post(f"{API}/urls", json={"url": url(4), "type": "include"})
    while_queued = await header(c)
    assert (await c.post(f"{API}/promote")).status_code == 200

    assert REINDEX not in while_queued
    assert REINDEX in await header(c)


async def test_a_failed_test_validation_needs_re_indexing_not_re_curation(client):
    """Nothing curated is wrong: a recompute or promote with nothing pending leaves the chip up."""
    c = client
    await promoted(c)
    await index_run(c, "test", visible=PAGES // 2)

    await c.post(f"{API}/recompute")
    await c.post(f"{API}/promote")

    col = (await c.get(API)).json()
    assert (col["status"], col["needs_recuration"]) == ("curated", False)
    assert REINDEX in await header(c) and "needs re-curation" not in await header(c)
    assert REINDEX_ROW_CHIP in (await c.get("/?flag=needs_reindexing")).text
    assert REINDEX_ROW_CHIP not in (await c.get("/?flag=needs_recuration")).text


async def test_a_failed_test_index_run_needs_re_indexing(client):
    c = client
    await promoted(c)

    await index_run(c, "test", state="failed", validated=False, error="export_incomplete")

    assert REINDEX in await header(c)


# ── prod not validated ─────────────────────────────────────────────────


async def test_a_prod_publish_whose_check_failed_is_not_validated_until_a_check_passes(client):
    c = client
    await promoted(c)
    await index_run(c, "test", move_to=Status.CONFIG_GENERATED)
    await index_run(c, "prod", run_id="prod-1", visible=PAGES // 2)
    failed = await header(c)
    on_dashboard = (await c.get("/?flag=prod_not_validated")).text
    live_step = (await c.get(f"/collections/{CID}?tab=overview&step=live")).text

    await index_run(c, "prod", run_id="prod-2", move_to=Status.LIVE)

    assert PROD_CHIP in failed and PROD_ROW_CHIP in on_dashboard
    assert PROD_ROW_CHIP not in (await c.get("/?flag=needs_recuration")).text
    assert ">fail<" in live_step and "via direct" in live_step and "Re-validate prod" in live_step
    assert PROD_CHIP not in await header(c)


async def test_a_prod_publish_never_checked_is_not_validated(client):
    c = client
    await promoted(c)
    await index_run(c, "test", move_to=Status.CONFIG_GENERATED)

    await index_run(c, "prod", validated=False)

    live_step = (await c.get(f"/collections/{CID}?tab=overview&step=live")).text
    assert PROD_CHIP in await header(c)
    assert "not validated" in live_step and "Re-validate prod" in live_step


# ── steps 5 and 6 ──────────────────────────────────────────────────────


async def test_the_index_steps_show_the_runs_their_validation_and_the_front_ends(client):
    c = client
    await promoted(c)
    test_run = await index_run(c, "test", move_to=Status.CONFIG_GENERATED, validated_by="second_pass")
    test_step = (await c.get(f"/collections/{CID}?tab=overview&step=config_generated")).text
    offered = await header(c)
    published = {"mode": "publish_vectors", "source_test_run": test_run.run_id, "indexed": PAGES, "unchanged": 0,
                 "deleted": 0, "index": "sde-web"}

    await index_run(c, "prod", move_to=Status.LIVE, status=published)

    live_step = (await c.get(f"/collections/{CID}?tab=overview&step=live")).text
    assert test_run.run_id in test_step and "counts match" in test_step and "via second_pass" in test_step
    assert 'href="http://d2vsr84ys2zd7q.cloudfront.net/"' in test_step and "Open test front end" in test_step
    assert "Index to prod" in offered
    assert f"from test run {test_run.run_id}" in live_step and f"{PAGES} docs promoted from the test index" in live_step
    assert 'href="https://science.data.nasa.gov/science-discovery-engine/search/sde/home"' in live_step
    assert "Live ✓" in await header(c)


async def test_a_prod_publish_refused_for_mass_deletion_offers_to_continue_on_prod_only(client):
    """Like the indexer's --allow-high-deletion: the prod button then asks to continue; the test
    button is not turned into an override by a prod refusal."""
    c = client
    await promoted(c)
    await index_run(c, "test", move_to=Status.CONFIG_GENERATED)
    await c.app.state.db.insert_job(JobRun(collection_id=CID, kind=JobKind.INDEX_PROD, state=JobState.FAILED,
                                           error="prod publish refused: deletion_threshold_exceeded"))

    live_step = (await c.get(f"/collections/{CID}?tab=overview&step=live")).text
    test_step = (await c.get(f"/collections/{CID}?tab=overview&step=config_generated")).text

    assert "index?target=prod&amp;allow_high_deletion=true" in await header(c)
    assert "index?target=prod&amp;allow_high_deletion=true" in live_step
    assert "delete more than 90% of the documents already in the production index" in live_step
    assert "target=test&amp;allow_high_deletion" not in test_step
