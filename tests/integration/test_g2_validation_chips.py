"""While an index or validate job is still checking a run, the run's stored report is provisional
(the indexer's pre-refresh validation.json, usually short on count): the step panel says
"validating…" and the header raises no warning chip. Once the job ends, a short report is a real
failure and both show it. The decision reads the collection's jobs and index runs (web.app
step_context and the chip rules), so it is checked on real rows."""

import pytest

from sde_curation.models import IndexRun, JobKind, JobRun, JobState

CID = "science.nasa.gov"
EXPECTED, INDEXED = 3, 1  # the report is short on count: a fail once it is final

TARGETS = {  # target -> (job kind checking it, stepper step, header chip, panel line while checking, final panel text)
    "test": (JobKind.INDEX_TEST, "config_generated", ">⚠ needs re-indexing<", "validating the test index", ">fail<"),
    "prod": (JobKind.VALIDATE_PROD, "live", ">⚠ prod not validated<", "validating the prod index", "Re-validate prod"),
}


@pytest.mark.parametrize("target", TARGETS)
async def test_a_short_report_reads_validating_while_its_job_runs_and_fail_once_it_ends(client, target):
    kind, step, chip, checking, final = TARGETS[target]
    db = client.app.state.db
    assert (await client.post("/api/collections", json={"seed_url": CID, "name": "Sci"})).status_code == 201
    report = {"run_id": "r1", "collection_key": CID, "expected_count": EXPECTED, "indexed_count": INDEXED,
              "count_matches": False, "titles_missing_in_index": [], "titles_only_in_index": [],
              "titles_mismatched": [], "title_match_rate": INDEXED / EXPECTED}
    await db.insert_index_run(IndexRun(run_id="r1", collection_id=CID, target=target, state="succeeded",
                                       exported=EXPECTED, validation=report, validated_by="indexer"))
    job = await db.insert_job(JobRun(collection_id=CID, kind=kind, state=JobState.RUNNING, run_id="r1",
                                     progress={"phase": "validating", "validation_attempt": 1,
                                               "indexed_so_far": INDEXED, "expected_count": EXPECTED}))

    panel = (await client.get(f"/collections/{CID}/step/{step}")).text
    assert "validating…" in panel and checking in panel and ">fail<" not in panel, "[running] panel"
    assert chip not in (await client.get(f"/collections/{CID}/header")).text, "[running] chip while still checking"

    job.state = JobState.SUCCEEDED
    await db.update_job(job)
    panel = (await client.get(f"/collections/{CID}/step/{step}")).text
    assert "validating…" not in panel and ">fail<" in panel and final in panel, "[ended] panel"
    assert chip in (await client.get(f"/collections/{CID}/header")).text, "[ended] no chip"
