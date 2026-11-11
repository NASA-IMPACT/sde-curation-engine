"""Phase 5: export contract (validated against the indexer's own code), S3 write order,
ECS run-task shape, status polling, and the /index job end-to-end (moto)."""
import asyncio
import json
from pathlib import Path

import pytest

from sde_curation.backends.index import (
    ENGINE_TASK_TAG,
    EcsDispatchIndexer,
    IndexError_,
    LocalSubprocessIndexer,
    indexer_command,
)
from sde_curation.backends.s3 import S3
from sde_curation.config import Settings
from sde_curation.engine.export import (
    build_manifest,
    export_lines,
    export_prefix,
    mint_run_id,
    write_jsonl,
)
from sde_curation.models import (
    Collection,
    CuratedUrl,
    Division,
    ExportManifest,
    JobKind,
    JobRun,
    JobState,
    Status,
)
from sde_curation.web.app import HIGH_DELETION_CONFIRM, next_action

INDEXER_ROOT = Path(__file__).resolve().parents[3] / "sde-api-scrapers"
COLL = Collection(collection_id="ex.org", name="Ex", seed_url="https://ex.org", division=Division.HELIOPHYSICS,
                  connector="crawler2", max_pages=10, curated_count=3)


def curated():
    return [
        CuratedUrl(collection_id="ex.org", url="https://ex.org/b", scraped_title="B scraped", title=None, division=None),
        CuratedUrl(collection_id="ex.org", url="https://ex.org/a", scraped_title="A", title="A title", division="Earth Science",
                   document_type="Data", full_text="text a"),
        CuratedUrl(collection_id="ex.org", url="https://ex.org/x", scraped_title="X", excluded=True),
    ]


def test_export_lines_and_manifest(tmp_path):
    lines = list(export_lines(curated()))
    assert [ln.url for ln in lines] == ["https://ex.org/a", "https://ex.org/b"]  # sorted, excluded dropped
    assert lines[1].title == "B scraped" and lines[0].division == "Earth Science"
    assert lines[0].full_text == "text a" and lines[1].full_text is None  # the row's own text, no dump lookup
    p = tmp_path / "d.jsonl"
    with p.open("w") as fh:
        n = write_jsonl(iter(lines), fh)
    assert n == 2 and p.read_text().count("\n") == 2
    m = build_manifest(COLL, "r1", n, "test")
    assert m.collection_key == "ex" and m.document_count == 2 and m.division == "Heliophysics"  # name "Ex"
    assert ExportManifest.model_validate_json(m.model_dump_json())


@pytest.mark.skipif(not (INDEXER_ROOT / "web" / "web_processor.py").is_file(), reason="indexer repo not checked out")
def test_export_matches_indexer_contract(monkeypatch):
    """Round-trip our export through sde-api-scrapers' own reader/processor."""
    monkeypatch.syspath_prepend(str(INDEXER_ROOT))
    from web.cosmos_source import load_manifest
    from web.web_processor import make_web_id, to_web_document

    m = build_manifest(COLL, "r1", 2, "test").model_dump(mode="json")

    class FakeS3:
        def get_object(self, Bucket, Key):
            import io
            return {"Body": io.BytesIO(json.dumps(m).encode())}

    manifest = load_manifest(FakeS3(), "b", "ex", "r1")
    for ln in export_lines(curated()):
        doc = to_web_document(ln.model_dump(exclude_none=True), manifest)
        assert doc["id"] == make_web_id("ex", ln.url) and doc["public_visibility"] is True
        assert doc["division"] in ("Earth Science", "Heliophysics")  # per-URL or manifest default


def test_indexer_command_matches_task_definition():
    assert indexer_command(COLL, "r1", "test") == [
        "python3", "api_scraper.py", "--source", "WEB_COSMOS", "--collection", "ex", "--run-id", "r1", "--target", "test",
    ]
    assert indexer_command(COLL, "r1", "test", allow_high_deletion=True)[-1] == "--allow-high-deletion"
    assert mint_run_id()[8] == "T" and len(mint_run_id()) == 23


def test_next_action_confirms_high_deletion():
    c = COLL.model_copy(update={"status": Status.CURATED})
    job = JobRun(collection_id=c.collection_id, kind=JobKind.INDEX_TEST, state=JobState.FAILED,
                 error="indexer failed: deletion_threshold_exceeded")
    action = next_action(c, job)
    assert "allow_high_deletion=true" in action["url"]
    assert action["confirm"] == HIGH_DELETION_CONFIRM
    assert "allow_high_deletion" not in next_action(c, None)["url"]


async def test_s3_helper(aws):
    import boto3

    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="bkt")
    s3 = S3("bkt")
    assert await s3.get_json("nope.json") is None and not await s3.exists("nope.json")
    await s3.put_json("a.json", {"x": 1})
    assert await s3.get_json("a.json") == {"x": 1} and await s3.exists("a.json")


async def test_ecs_run_task_shape(aws):
    import boto3

    ec2 = boto3.client("ec2", region_name="us-east-1")
    vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
    subnet = ec2.create_subnet(VpcId=vpc, CidrBlock="10.0.1.0/24")["Subnet"]["SubnetId"]
    sg = ec2.create_security_group(GroupName="sg", Description="d", VpcId=vpc)["GroupId"]
    ecs = boto3.client("ecs", region_name="us-east-1")
    ecs.create_cluster(clusterName="api-scrapers-cluster-dev")
    ecs.register_task_definition(
        family="web_cosmos-scraper-dev", requiresCompatibilities=["FARGATE"], networkMode="awsvpc", cpu="256", memory="512",
        containerDefinitions=[{"name": "WEB_COSMOSContainer", "image": "x", "memory": 512, "cpu": 256}],
    )
    s = Settings(index_backend="ecs", indexing_subnets=[subnet], indexing_security_groups=[sg], llm_provider="fake",
                 indexing_dispatch_role_arn=None)
    be = EcsDispatchIndexer(s, ecs=ecs)
    args = be.run_task_args(COLL, "r1", "test")
    assert args["overrides"]["containerOverrides"][0] == {"name": "WEB_COSMOSContainer",
                                                          "command": indexer_command(COLL, "r1", "test")}
    assert args["networkConfiguration"]["awsvpcConfiguration"]["subnets"] == [subnet]
    # moto's Fargate run_task is incomplete (awsvpc ENI bug); exercise dispatch/still_running with a stub
    from typing import ClassVar

    class StubEcs:
        calls: ClassVar[list] = []
        def run_task(self, **kw): self.calls.append(kw); return {"tasks": [{"taskArn": "arn:aws:ecs:us-east-1:1:task/x/abc"}], "failures": []}
        def describe_tasks(self, **kw): return {"tasks": [{"lastStatus": "STOPPED", "stoppedReason": "Essential container exited",
                                                          "containers": [{"exitCode": 0}]}]}
    be = EcsDispatchIndexer(s, ecs=StubEcs())
    d = await be.dispatch(COLL, "r1", "test")
    assert d.external_ref.startswith("arn:aws:ecs:") and StubEcs.calls[0]["taskDefinition"] == "web_cosmos-scraper-dev"
    assert await be.still_running(d) is False and d.detail["exit_code"] == 0
    with pytest.raises(IndexError_, match="INDEXING_SUBNETS"):
        EcsDispatchIndexer(Settings(index_backend="ecs", llm_provider="fake"))
    # export written in contract order and shape
    # collection advanced; run recorded
    # steps 5/6 link the curator to the search front ends so they can eyeball what got indexed


async def test_local_indexer_missing_paths(tmp_path):
    s = Settings(indexer_root=tmp_path, indexer_python=Path("/nonexistent"), llm_provider="fake")
    with pytest.raises(IndexError_, match="INDEXER_ROOT"):
        await LocalSubprocessIndexer(s).dispatch(COLL, "r", "test")
    (tmp_path / "api_scraper.py").write_text("")
    with pytest.raises(IndexError_, match="INDEXER_PYTHON"):
        await LocalSubprocessIndexer(s).dispatch(COLL, "r", "test")
    await asyncio.sleep(0)
    assert export_prefix("k", "r") == "curated_collections/k/r"


def test_every_indexer_task_the_engine_starts_carries_the_engine_tag():
    """L14: the indexer's ECS cluster is shared with other workloads (Bernard, 2026-10-12). The
    engine's role may stop only tasks with this tag, so every task it starts must carry it."""
    s = Settings(index_backend="ecs", indexing_subnets=["subnet-1"], llm_provider="fake")

    args = EcsDispatchIndexer(s, ecs=object()).run_task_args(COLL, "r1", "test")

    assert args["tags"] == [{"key": ENGINE_TASK_TAG, "value": COLL.collection_id}]
    assert ENGINE_TASK_TAG == "sde-curation-engine"  # infra/stacks/engine_stack.py grants StopTask on this key


# ── stall detection: progress, not wall-clock time ───────────────────────────────────────────────

STALL_BUCKET = "index-bucket"
STALL_S = 0.3  # the no-progress window under test (index_stall_timeout_s)
BATCH_EVERY_S = 0.1  # an indexer that writes a batch this often is making progress
WORKS_FOR_S = 3 * STALL_S  # longer than one stall window, so a wall-clock timeout would fire


class _RunningTask:
    """A backend whose task stays RUNNING: only S3 shows what it is doing."""

    async def still_running(self, d):
        return True


def _stall_settings() -> Settings:
    return Settings(llm_provider="fake", cosmos_index_bucket=STALL_BUCKET, index_poll_interval_s=0.02,
                    index_stall_timeout_s=STALL_S)


def _s3(aws) -> S3:
    import boto3

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=STALL_BUCKET)
    return S3(STALL_BUCKET, client=client)


async def _write_batches(s3: S3, run_id: str, n: int) -> None:
    for i in range(1, n + 1):
        await s3.put_json(f"vectorized/{COLL.collection_key}/{run_id}/batch_{i:04d}.jsonl", {})
        await asyncio.sleep(BATCH_EVERY_S)


async def _ignore_progress(p):
    pass


async def test_an_indexer_still_writing_batches_is_not_stalled_after_the_stall_timeout(aws):
    """LPI, 2026-11-04: a 30,682-page run wrote a batch every few minutes, but the engine gave up
    after a fixed 4 h. Time without a new vectorized batch is the stall, not time since dispatch."""
    from sde_curation.backends.index import Dispatch, wait_for_status

    s3 = _s3(aws)
    n = int(WORKS_FOR_S / BATCH_EVERY_S)

    async def indexer():
        await _write_batches(s3, "r1", n)
        await s3.put_json(f"index_runs/{COLL.collection_key}/r1/status.json",
                          {"run_id": "r1", "collection_key": COLL.collection_key, "target": "prod",
                           "state": "succeeded"})

    work = asyncio.create_task(indexer())
    status, _ = await wait_for_status(s3, _stall_settings(), COLL, "r1", _RunningTask(), Dispatch("t", {}),
                                      _ignore_progress, target="prod")
    await work

    assert status.state == "succeeded"


async def test_an_indexer_that_writes_no_new_batch_for_the_stall_timeout_is_stalled(aws):
    """The other side of the rule: a task that is RUNNING but has written nothing new for the whole
    window is stalled, and the error says how many batches it wrote."""
    from sde_curation.backends.index import Dispatch, wait_for_status

    s3 = _s3(aws)
    await _write_batches(s3, "r1", 2)

    with pytest.raises(IndexError_, match=r"no new vectorized batch for \d+s \(2 batches written\)"):
        await wait_for_status(s3, _stall_settings(), COLL, "r1", _RunningTask(), Dispatch("t", {}),
                              _ignore_progress, target="prod")



# ── the reason an engine-stopped task shows in ECS ───────────────────────────────────────────────

class _StopRecorder:
    def __init__(self):
        self.stops = []

    def stop_task(self, **kw):
        self.stops.append(kw)


@pytest.mark.parametrize(("reason", "shown"), [
    ("cancelled by alice in the curation engine", "cancelled by alice in the curation engine"),
    ("x" * 300, "x" * 255),  # ECS StopTask accepts at most 255 characters
])
async def test_an_engine_stop_tells_ecs_why(reason, shown):
    from sde_curation.backends.index import Dispatch

    ecs = _StopRecorder()
    s = Settings(index_backend="ecs", indexing_subnets=["subnet-1"], indexing_ecs_cluster="c", llm_provider="fake")

    await EcsDispatchIndexer(s, ecs=ecs).kill(Dispatch("arn:task/1", {}), reason=reason)

    assert ecs.stops == [{"cluster": "c", "task": "arn:task/1", "reason": shown}]


async def test_an_engine_stop_without_a_reason_keeps_the_old_text():
    from sde_curation.backends.index import Dispatch

    ecs = _StopRecorder()
    s = Settings(index_backend="ecs", indexing_subnets=["subnet-1"], indexing_ecs_cluster="c", llm_provider="fake")

    await EcsDispatchIndexer(s, ecs=ecs).kill(Dispatch("arn:task/1", {}))

    assert ecs.stops[0]["reason"] == "cancelled in the curation engine"
