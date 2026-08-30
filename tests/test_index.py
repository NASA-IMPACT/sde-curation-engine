"""Phase 5: export contract (validated against the indexer's own code), S3 write order,
ECS run-task shape, status polling, and the /index job end-to-end (moto)."""

import asyncio
import json
from pathlib import Path

import pytest

from sde_curation.backends.index import (
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
from sde_curation.models import Collection, CuratedUrl, Division, ExportManifest
from tests.conftest import prepare, wait_job

INDEXER_ROOT = Path(__file__).resolve().parents[2] / "sde-api-scrapers"
COLL = Collection(collection_id="ex.org", name="Ex", seed_url="https://ex.org", division=Division.HELIOPHYSICS,
                  connector="crawler2", max_pages=10, curated_count=3)


def curated():
    return [
        CuratedUrl(collection_id="ex.org", url="https://ex.org/b", scraped_title="B scraped", title=None, division=None),
        CuratedUrl(collection_id="ex.org", url="https://ex.org/a", scraped_title="A", title="A title", division="Earth Science",
                   document_type="Data"),
        CuratedUrl(collection_id="ex.org", url="https://ex.org/x", scraped_title="X", excluded=True),
    ]


def test_export_lines_and_manifest(tmp_path):
    lines = list(export_lines(curated(), {"https://ex.org/a": "text a", "https://ex.org/b": None}))
    assert [ln.url for ln in lines] == ["https://ex.org/a", "https://ex.org/b"]  # sorted, excluded dropped
    assert lines[1].title == "B scraped" and lines[0].division == "Earth Science"
    p = tmp_path / "d.jsonl"
    with p.open("w") as fh:
        n = write_jsonl(iter(lines), fh)
    assert n == 2 and p.read_text().count("\n") == 2
    m = build_manifest(COLL, "r1", n, "test")
    assert m.collection_key == "ex.org" and m.document_count == 2 and m.division == "Heliophysics"
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

    manifest = load_manifest(FakeS3(), "b", "ex.org", "r1")
    for ln in export_lines(curated(), {}):
        doc = to_web_document(ln.model_dump(exclude_none=True), manifest)
        assert doc["id"] == make_web_id("ex.org", ln.url) and doc["public_visibility"] is True
        assert doc["division"] in ("Earth Science", "Heliophysics")  # per-URL or manifest default


def test_indexer_command_matches_task_definition():
    assert indexer_command(COLL, "r1", "test") == [
        "python3", "api_scraper.py", "--source", "WEB_COSMOS", "--collection", "ex.org", "--run-id", "r1", "--target", "test",
    ]
    assert mint_run_id()[8] == "T" and len(mint_run_id()) == 23


@pytest.fixture
def aws(monkeypatch):
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        yield


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






async def test_index_to_test_end_to_end(index_client):
    c = index_client
    assert (await c.post("/api/collections/ex.org/index?target=test")).status_code == 404
    await prepare(c)
    r = await c.post("/api/collections/ex.org/index?target=prod")
    assert r.status_code == 409 and "test" in r.text  # prod requires a validated test run
    r = await c.post("/api/collections/ex.org/index?target=test")
    assert r.status_code == 202, r.text
    run_id = r.json()["run_id"]
    assert (await c.post("/api/collections/ex.org/index?target=test")).status_code == 409  # busy
    job = await wait_job(c, "ex.org", timeout=30)
    assert job["state"] == "succeeded", job
    assert job["progress"]["exported"] == 7 and job["progress"]["status"]["state"] == "succeeded"
    assert job["progress"]["validation"]["count_matches"] is True  # after the gate's second pass
    # export written in contract order and shape
    keys = [o["Key"] for o in c.s3.list_objects_v2(Bucket="cosmos-idx", Prefix=f"curated_collections/ex.org/{run_id}/")["Contents"]]
    assert sorted(keys) == [f"curated_collections/ex.org/{run_id}/documents.jsonl", f"curated_collections/ex.org/{run_id}/manifest.json"]
    m = json.loads(c.s3.get_object(Bucket="cosmos-idx", Key=f"curated_collections/ex.org/{run_id}/manifest.json")["Body"].read())
    assert m["document_count"] == 7 and m["collection_key"] == "ex.org" and m["division"] == "Heliophysics"
    first = json.loads(c.s3.get_object(Bucket="cosmos-idx", Key=f"curated_collections/ex.org/{run_id}/documents.jsonl")["Body"].read().splitlines()[0])
    assert set(first) <= {"url", "title", "full_text", "document_type", "division"} and first["url"] == "https://ex.org/p2"
    # collection advanced; run recorded
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["status"] == "config_generated" and col["last_run_id"] == run_id
    runs = (await c.get("/api/collections/ex.org/index_runs")).json()
    assert runs[0]["run_id"] == run_id and runs[0]["target"] == "test" and runs[0]["state"] == "succeeded"
    page = (await c.get("/collections/ex.org?tab=overview&step=config_generated")).text
    assert run_id in page and "count_matches" not in page and "counts match" in page


async def test_index_failure_is_surfaced(index_client):
    c = index_client
    await prepare(c, "fail.org")
    r = await c.post("/api/collections/fail.org/index?target=test")
    assert r.status_code == 202
    job = await wait_job(c, "fail.org", timeout=30)
    assert job["state"] == "failed" and "export_incomplete" in job["error"]
    assert (await c.get("/api/collections/fail.org")).json()["status"] == "curated"  # not advanced


async def test_index_requires_curated_and_nothing_to_export(index_client):
    c = index_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "Ex", "max_pages": 10})
    assert (await c.post("/api/collections/ex.org/index?target=test")).status_code == 409
    await prepare(c)
    await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*"})
    await c.post("/api/collections/ex.org/promote")
    r = await c.post("/api/collections/ex.org/index?target=test")
    assert r.status_code == 409 and "nothing to export" in r.text


async def test_local_indexer_missing_paths(tmp_path):
    s = Settings(indexer_root=tmp_path, indexer_python=Path("/nonexistent"), llm_provider="fake")
    with pytest.raises(IndexError_, match="INDEXER_ROOT"):
        await LocalSubprocessIndexer(s).dispatch(COLL, "r", "test")
    (tmp_path / "api_scraper.py").write_text("")
    with pytest.raises(IndexError_, match="INDEXER_PYTHON"):
        await LocalSubprocessIndexer(s).dispatch(COLL, "r", "test")
    await asyncio.sleep(0)
    assert export_prefix("k", "r") == "curated_collections/k/r"
