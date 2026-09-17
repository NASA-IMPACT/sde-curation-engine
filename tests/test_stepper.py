"""The pipeline stepper: its polling wrapper must not carry hx-vals.

Regression for the double-click bug: hx-vals on the wrapper (added so the poll could re-read ?step
from the address bar) is inherited by the <a> step links, so one click sent ?step=<clicked>&step=<bar>;
FastAPI takes the last value, the old panel stayed, and only a second click showed the right one.
The poll now adds the step in a config-request hook guarded to the wrapper's own request instead.
"""

import re

from tests.conftest import seed_dump


async def test_stepper_links_send_a_single_step(client):
    r = await client.post("/api/collections", json={"seed_url": "science.nasa.gov", "name": "Sci"})
    cid = r.json()["collection_id"]
    await seed_dump(client, cid)
    page = (await client.get(f"/collections/{cid}")).text
    wrapper = re.search(r'<div id="pipeline-[^"]+" class="pipeline-wrap"[^>]*>', page).group(0)
    assert "hx-vals" not in wrapper, "hx-vals on the wrapper is inherited by the step links (double-click bug)"
    # the poll still carries the step from the address bar, via a hook scoped to the wrapper's own request
    hook = re.search(r'hx-on::config-request="([^"]*)"', wrapper)
    assert hook and "event.detail.elt === this" in hook.group(1) and "parameters.step" in hook.group(1)
    links = re.findall(r'<a href="/collections/[^"]+\?step=\w+"[^>]*>', page)
    assert len(links) >= 5 and not any("hx-vals" in a for a in links)


async def test_single_step_param_selects_that_step(client):
    r = await client.post("/api/collections", json={"seed_url": "science.nasa.gov", "name": "Sci"})
    cid = r.json()["collection_id"]
    await seed_dump(client, cid)
    for step in ("backlog", "scraped", "curating", "curated", "config_generated", "live"):
        page = (await client.get(f"/collections/{cid}?step={step}")).text
        assert f'data-step="{step}"' in page, step
        assert re.search(rf'<li class="\w+ selected"[^>]*>\s*<a href="/collections/{cid}\?step={step}"', page), step


async def test_validation_shows_validating_until_the_job_finishes(client):
    """The indexer's pre-refresh validation.json (usually short on count) is stored on the run before
    the engine's own post-refresh check finishes — the panel must say "validating", not "fail"."""
    from sde_curation.models import IndexRun, JobKind, JobRun, JobState

    r = await client.post("/api/collections", json={"seed_url": "science.nasa.gov", "name": "Sci"})
    cid = r.json()["collection_id"]
    db = client.app.state.db
    short = {"run_id": "r1", "collection_key": cid, "expected_count": 3, "indexed_count": 0, "count_matches": False,
             "titles_missing_in_index": [], "titles_only_in_index": [], "titles_mismatched": [], "title_match_rate": 0.0}
    await db.insert_index_run(IndexRun(run_id="r1", collection_id=cid, target="test", state="succeeded",
                                       exported=3, validation=short, validated_by="indexer"))
    job = await db.insert_job(JobRun(collection_id=cid, kind=JobKind.INDEX_TEST, state=JobState.RUNNING, run_id="r1",
                                     progress={"phase": "done", "exported": 3}))

    panel = (await client.get(f"/collections/{cid}/step/config_generated")).text
    assert "validating…" in panel and ">fail<" not in panel and "validation failed" not in panel
    assert "validating the test index" in panel
    assert ">⚠ needs re-indexing<" not in (await client.get(f"/collections/{cid}/header")).text  # still being checked

    job.state = JobState.SUCCEEDED
    job.progress = {**job.progress, "validation": short, "validation_ok": False}
    await db.update_job(job)
    panel = (await client.get(f"/collections/{cid}/step/config_generated")).text
    assert ">fail<" in panel and "validating…" not in panel  # a real failure still shows
    assert ">⚠ needs re-indexing<" in (await client.get(f"/collections/{cid}/header")).text


async def test_prod_validation_shows_validating_while_revalidate_prod_runs(client):
    from sde_curation.models import IndexRun, JobKind, JobRun, JobState

    r = await client.post("/api/collections", json={"seed_url": "science.nasa.gov", "name": "Sci"})
    cid = r.json()["collection_id"]
    db = client.app.state.db
    short = {"run_id": "p1", "collection_key": cid, "expected_count": 3, "indexed_count": 1, "count_matches": False,
             "titles_missing_in_index": [], "titles_only_in_index": [], "titles_mismatched": [], "title_match_rate": 0.33}
    await db.insert_index_run(IndexRun(run_id="p1", collection_id=cid, target="prod", state="succeeded",
                                       exported=3, validation=short, validated_by="direct"))
    job = await db.insert_job(JobRun(collection_id=cid, kind=JobKind.VALIDATE_PROD, state=JobState.RUNNING, run_id="p1",
                                     progress={"phase": "validating", "validation_attempt": 2, "indexed_so_far": 2, "expected_count": 3}))

    panel = (await client.get(f"/collections/{cid}/step/live")).text
    assert "validating…" in panel and ">fail<" not in panel and "validating the prod index" in panel
    assert ">⚠ prod not validated<" not in (await client.get(f"/collections/{cid}/header")).text  # being checked

    job.state = JobState.SUCCEEDED
    await db.update_job(job)
    panel = (await client.get(f"/collections/{cid}/step/live")).text
    assert ">fail<" in panel and "via direct" in panel and "Re-validate prod" in panel
    assert ">⚠ prod not validated<" in (await client.get(f"/collections/{cid}/header")).text
