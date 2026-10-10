"""Index to test, index to prod and (re)validation on the in-memory FakeDatabase: the export goes to
moto's S3, the indexer and the prod publisher are fakes, and `validate_direct` (the index read) is
replaced by what the index would answer."""

import json

import pytest

import sde_curation.jobs as jobs_mod
from sde_curation.backends.index import IndexError_
from sde_curation.backends.validate import NoIndexAccess
from sde_curation.engine.export import export_prefix, status_prefix
from sde_curation.models import IndexRun, JobKind, JobRun, JobState, Status
from tests.support.engine import (
    BUCKET,
    CID,
    Indexer,
    Publisher,
    StatusIndexer,
    finished,
    make_engine,
    promoted,
    report,
    until,
    validated_test_run,
)
from tests.support.fake_db import FakeDatabase

PAGES = 3
KEY = "example_org"  # the collection key COSMOS derives from the name "example.org"
PROD = {"opensearch_endpoint_prod": "https://prod.example.aoss.amazonaws.com"}
QUICK_GATE = {"validation_delay_s": 0, "validation_poll_interval_s": 0.01, "validation_timeout_s": 0.05}
FAILING_TITLE_RATE = 0.5


@pytest.fixture
def s3(aws):
    import boto3

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    return client


class Index:
    """What validate_direct reads from the index: each call answers the next report (the last repeats),
    or raises `error`."""

    def __init__(self, *reports, error: Exception | None = None):
        self.reports, self.error, self.asked = list(reports), error, []

    async def __call__(self, settings, *, collection_key, run_id, target, expected_titles):
        self.asked.append((collection_key, target, len(expected_titles)))
        if self.error is not None:
            raise self.error
        return self.reports.pop(0) if len(self.reports) > 1 else self.reports[0]


async def _index(db, tmp_path, monkeypatch, *, index: Index, indexer=None, publisher=None, target="test", **over):
    monkeypatch.setattr(jobs_mod, "validate_direct", index)
    engine = make_engine(db, tmp_path, indexer=indexer, publisher=publisher and (lambda: publisher),
                         **{**QUICK_GATE, **PROD, **over})
    job, run = await engine.start_index(await db.get_collection(CID), target, actor="alice")
    done = await finished(db, job.id)
    await engine.shutdown()
    return done, await db.get_index_run(run.run_id)


# ── index to test ────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("title_match_rate", "status", "ok"), [
    (1.0, Status.CONFIG_GENERATED, True),
    (FAILING_TITLE_RATE, Status.CURATED, False),  # failed gate: back to curated, "needs re-indexing"
])
async def test_index_to_test_exports_dispatches_and_applies_the_validation_gate(tmp_path, monkeypatch, s3, title_match_rate, status, ok):
    db = FakeDatabase()
    await promoted(db, titles=["A", "B", "C"])
    indexer = StatusIndexer(validation=report(PAGES, PAGES))

    job, run = await _index(db, tmp_path, monkeypatch, indexer=indexer,
                            index=Index(report(PAGES, PAGES, title_match_rate)))

    assert job.state is JobState.SUCCEEDED and job.progress["validation_ok"] is ok
    assert (run.state, run.exported, run.external_ref, run.validated_by) == ("succeeded", PAGES, "task/1", "direct")
    assert indexer.started == [run.run_id]
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=export_prefix(KEY, run.run_id))["Contents"]]
    assert sorted(k.rsplit("/", 1)[1] for k in keys) == ["documents.jsonl", "manifest.json"]
    c = await db.get_collection(CID)
    assert (c.status, c.index_key) == (status, KEY)  # the first run pins the key the collection is indexed as


@pytest.mark.parametrize(("timeout_s", "attempts", "ok"), [
    (5, 2, True),  # still short at first: checked again until every document is visible
    (0, 1, False),  # no time left in the window: the short count fails the gate
])
async def test_the_validation_gate_rechecks_a_short_count_until_its_timeout(tmp_path, monkeypatch, s3, timeout_s, attempts, ok):
    db = FakeDatabase()
    await promoted(db, titles=["A", "B", "C"])

    job, _ = await _index(db, tmp_path, monkeypatch, indexer=StatusIndexer(), validation_timeout_s=timeout_s,
                          index=Index(report(PAGES, PAGES - 1), report(PAGES, PAGES)))

    assert (job.progress["validation_attempt"], job.progress["validation_ok"]) == (attempts, ok)


async def test_without_read_access_the_gate_falls_back_to_a_second_indexer_pass(tmp_path, monkeypatch, s3):
    db = FakeDatabase()
    await promoted(db, titles=["A", "B", "C"])
    indexer = StatusIndexer(validation=report(PAGES, PAGES))

    job, run = await _index(db, tmp_path, monkeypatch, indexer=indexer,
                            index=Index(error=NoIndexAccess("403 from the test collection")))

    assert job.state is JobState.SUCCEEDED and job.progress["validation_ok"] is True
    assert (run.validated_by, indexer.started) == ("second_pass", [run.run_id, run.run_id])


async def test_an_indexer_that_reports_failure_fails_the_job_and_its_run(tmp_path, monkeypatch, s3):
    db = FakeDatabase()
    await promoted(db, titles=["A"])

    job, run = await _index(db, tmp_path, monkeypatch, indexer=StatusIndexer(state="failed", error="export_incomplete"),
                            index=Index(report(1, 1)))

    assert (job.state, job.error) == (JobState.FAILED, "indexer failed: export_incomplete")
    assert (run.state, run.error) == ("failed", "export_incomplete")
    assert (await db.get_collection(CID)).status is not Status.CONFIG_GENERATED


async def test_a_collection_whose_curated_pages_are_all_excluded_has_nothing_to_export(tmp_path, monkeypatch, s3):
    db = FakeDatabase()
    await promoted(db, titles=["A", "B"], excluded=True)
    indexer = StatusIndexer()

    job, _ = await _index(db, tmp_path, monkeypatch, indexer=indexer, index=Index(report(0, 0)))

    assert (job.state, job.error) == (JobState.FAILED, "nothing to export: every curated URL is excluded")
    assert indexer.started == []


async def test_a_curator_cancel_during_indexing_stops_the_indexer_task_and_fails_the_run(tmp_path, monkeypatch, s3):
    db = FakeDatabase()
    await promoted(db, titles=["A"])
    indexer = Indexer()  # runs until stopped: never writes status.json
    engine = make_engine(db, tmp_path, indexer=indexer)
    job, run = await engine.start_index(await db.get_collection(CID), "test")
    await until(lambda: _started(indexer))

    cancelled = await engine.cancel(CID, actor="alice")

    assert (cancelled.id, cancelled.state, cancelled.error) == (job.id, JobState.FAILED, "cancelled by alice")
    assert indexer.stopped == ["task/1"]
    assert ((r := await db.get_index_run(run.run_id)).state, r.error) == ("failed", "cancelled by alice")


async def _started(indexer):
    return bool(indexer.started)


@pytest.mark.parametrize(("start", "settings", "error"), [
    (lambda e, c, run: e.start_index(c, "test"), {"cosmos_index_bucket": ""}, "COSMOS_INDEX_BUCKET is not set"),
    (lambda e, c, run: e.start_index(c, "prod"), {}, "OPENSEARCH_ENDPOINT_PROD is not set"),
    (lambda e, c, run: e.start_revalidate(c, run), {}, "OPENSEARCH_ENDPOINT_PROD is not set"),
])
async def test_index_and_validate_refuse_to_start_without_their_destination(tmp_path, start, settings, error):
    db = FakeDatabase()
    c = await promoted(db, titles=["A"])
    prod_run = IndexRun(run_id="p-1", collection_id=CID, target="prod")
    engine = make_engine(db, tmp_path, **settings)

    with pytest.raises(IndexError_, match=error):
        await start(engine, c, prod_run)
    assert engine.active_for(CID) is None


# ── resume after an engine restart ───────────────────────────────────────────────────────────────


async def _resume(db, tmp_path, monkeypatch, job, *, indexer=None, publisher=None, index=None) -> JobRun:
    monkeypatch.setattr(jobs_mod, "validate_direct", index or Index(report(PAGES, PAGES)))
    engine = make_engine(db, tmp_path, indexer=indexer, publisher=publisher and (lambda: publisher),
                         **{**QUICK_GATE, **PROD})
    await engine.recover()
    engine.start_resumes()
    done = await finished(db, job.id)
    await engine.shutdown()
    return done


@pytest.mark.parametrize(("run_state", "external_ref", "status_written"), [
    ("running", "task/7", True),  # dispatched: follow the same task to its status.json
    ("succeeded", "task/7", False),  # the indexer had reported: only validation is left
])
async def test_a_resumed_index_to_test_never_dispatches_again(tmp_path, monkeypatch, s3, run_state, external_ref, status_written):
    db = FakeDatabase()
    await promoted(db, titles=["A", "B", "C"])
    run = await db.insert_index_run(IndexRun(run_id="r-1", collection_id=CID, target="test", exported=PAGES,
                                             external_ref=external_ref))
    run.state = run_state
    await db.update_index_run(run)
    if status_written:  # the task finished while no engine was watching
        s3.put_object(Bucket=BUCKET, Key=f"{status_prefix(KEY, 'r-1')}/status.json", Body=json.dumps(
            {"run_id": "r-1", "collection_key": KEY, "target": "test", "state": "succeeded", "indexed": PAGES}))
    job = await db.insert_job(JobRun(collection_id=CID, kind=JobKind.INDEX_TEST, state=JobState.RUNNING,
                                     run_id="r-1", progress={"phase": "indexing"}))
    indexer = Indexer()

    done = await _resume(db, tmp_path, monkeypatch, job, indexer=indexer)

    assert (done.state, done.progress["validation_ok"], indexer.started) == (JobState.SUCCEEDED, True, [])
    assert (await db.get_index_run("r-1")).validated_by == "direct"


@pytest.mark.parametrize(("kind", "error"), [
    (JobKind.INDEX_TEST, "engine restarted before the index run was recorded"),
    (JobKind.INDEX_PROD, "engine restarted before the prod run was recorded"),
    (JobKind.VALIDATE, "engine restarted before the run to check was recorded"),
])
async def test_a_resumed_job_whose_run_was_never_recorded_fails(tmp_path, monkeypatch, kind, error):
    db = FakeDatabase()
    await promoted(db, titles=["A"])
    job = await db.insert_job(JobRun(collection_id=CID, kind=kind, state=JobState.RUNNING, run_id="never-recorded"))

    done = await _resume(db, tmp_path, monkeypatch, job)

    assert (done.state, done.error) == (JobState.FAILED, error)


# ── index to prod ────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("title_match_rate", "status"), [
    (1.0, Status.LIVE),
    (FAILING_TITLE_RATE, Status.CONFIG_GENERATED),  # published but not validated: not live
])
async def test_index_to_prod_publishes_the_validated_test_run_and_goes_live_only_if_prod_validates(tmp_path, monkeypatch, title_match_rate, status):
    db = FakeDatabase()
    c = await promoted(db, titles=["A", "B", "C"])
    source = await validated_test_run(db, c, exported=PAGES)
    publisher = Publisher(indexed=PAGES, documents_in_export=PAGES)

    job, run = await _index(db, tmp_path, monkeypatch, publisher=publisher, target="prod",
                            index=Index(report(PAGES, PAGES, title_match_rate)))

    assert publisher.runs == [(KEY, run.run_id, source.run_id)]
    assert (job.state, run.state, run.exported, run.external_ref) == (JobState.SUCCEEDED, "succeeded", PAGES,
                                                                      f"publish:{source.run_id}")
    assert (await db.get_collection(CID)).status is status


async def _no_test_run(db, c):
    return None


async def _test_run_under_another_key(db, c):
    return await validated_test_run(db, c, key="old_key")


@pytest.mark.parametrize(("arrange", "error"), [
    (_no_test_run, "prod indexing requires a successful, validated test run first"),
    (_test_run_under_another_key, "the latest test run was indexed as 'old_key' but this collection is now 'example_org'"),
])
async def test_index_to_prod_refuses_without_a_validated_test_run_under_the_same_key(tmp_path, monkeypatch, arrange, error):
    db = FakeDatabase()
    c = await promoted(db, titles=["A"])
    await arrange(db, c)
    publisher = Publisher()

    job, _ = await _index(db, tmp_path, monkeypatch, publisher=publisher, target="prod", index=Index(report(1, 1)))

    assert job.state is JobState.FAILED and job.error.startswith(error)
    assert publisher.runs == []


MISSING_VECTORS = ("publish to prod failed: vectors_missing — 2 documents have no vectors in S3 or the test index,"
                   " e.g. u1, u2")


async def test_a_failed_prod_publish_names_the_documents_without_vectors(tmp_path, monkeypatch):
    db = FakeDatabase()
    c = await promoted(db, titles=["A"])
    await validated_test_run(db, c)
    publisher = Publisher(state="failed", error="vectors_missing", missing=2, missing_urls=["u1", "u2"])

    job, run = await _index(db, tmp_path, monkeypatch, publisher=publisher, target="prod", index=Index(report(1, 1)))

    assert (job.state, job.error) == (JobState.FAILED, MISSING_VECTORS)
    assert run.state == "failed"


async def test_a_prod_publish_that_cannot_be_validated_fails_and_is_not_live(tmp_path, monkeypatch):
    """There is no indexer to fall back to: an unchecked publish must not count as live."""
    db = FakeDatabase()
    c = await promoted(db, titles=["A"])
    await validated_test_run(db, c)

    job, _ = await _index(db, tmp_path, monkeypatch, publisher=Publisher(), target="prod",
                          index=Index(error=NoIndexAccess("403 from prod")))

    assert job.state is JobState.FAILED and job.error.startswith("prod validation could not run: 403 from prod")
    assert (await db.get_collection(CID)).status is Status.CONFIG_GENERATED


@pytest.mark.parametrize(("run_state", "source_kept", "published", "state"), [
    ("succeeded", True, False, JobState.SUCCEEDED),  # interrupted during validation: only that is left
    ("running", True, True, JobState.SUCCEEDED),  # interrupted while publishing: publish again
    ("running", False, False, JobState.FAILED),  # the test run it published from is gone
])
async def test_a_resumed_prod_publish_carries_on_from_its_run(tmp_path, monkeypatch, run_state, source_kept, published, state):
    db = FakeDatabase()
    c = await promoted(db, titles=["A"])
    if source_kept:
        await validated_test_run(db, c)
    run = await db.insert_index_run(IndexRun(run_id="p-1", collection_id=CID, target="prod", external_ref="publish:t-1"))
    run.state = run_state
    await db.update_index_run(run)
    job = await db.insert_job(JobRun(collection_id=CID, kind=JobKind.INDEX_PROD, state=JobState.RUNNING, run_id="p-1"))
    publisher = Publisher()

    done = await _resume(db, tmp_path, monkeypatch, job, publisher=publisher, index=Index(report(1, 1)))

    assert done.state is state
    assert publisher.runs == ([(KEY, "p-1", "t-1")] if published else [])
    if not source_kept:
        assert done.error == "the test run this publish came from is gone"


# ── re-validate ──────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("target", "kind", "status"), [
    ("test", JobKind.VALIDATE, Status.CONFIG_GENERATED),
    ("prod", JobKind.VALIDATE_PROD, Status.LIVE),
])
async def test_revalidate_checks_an_existing_run_again_without_a_new_export(tmp_path, monkeypatch, target, kind, status):
    db = FakeDatabase()
    c = await promoted(db, titles=["A", "B", "C"])
    run = await db.insert_index_run(IndexRun(run_id="r-1", collection_id=CID, target=target, exported=PAGES))
    index = Index(report(PAGES, PAGES))
    monkeypatch.setattr(jobs_mod, "validate_direct", index)
    indexer = Indexer()
    engine = make_engine(db, tmp_path, indexer=indexer, **{**QUICK_GATE, **PROD})

    job = await engine.start_revalidate(c, run, actor="alice")
    done = await finished(db, job.id)
    await engine.shutdown()

    assert (done.kind, done.state, done.run_id) == (kind, JobState.SUCCEEDED, "r-1")
    assert (index.asked, indexer.started) == ([(KEY, target, PAGES)], [])
    assert ((await db.get_index_run("r-1")).validation["count_matches"], (await db.get_collection(CID)).status) == (True, status)


async def test_the_first_index_run_records_the_key_it_pins_in_the_audit_ledger(tmp_path, monkeypatch, s3):
    """The pinned key is what stops a later rename from moving the collection to a second index; the
    audit ledger is where an admin finds when the key was set, and from what."""
    db = FakeDatabase()
    await promoted(db, titles=["A", "B", "C"])

    await _index(db, tmp_path, monkeypatch, indexer=StatusIndexer(validation=report(PAGES, PAGES)),
                 index=Index(report(PAGES, PAGES)))

    pinned = [(a["actor"], a["collection_id"], a["detail"]) for a in db._audit if a["action"] == "index.key"]
    assert pinned == [("system", CID, f"indexed as '{KEY}' ({CID}), from the collection name")]
