"""Index to prod = publish the validated test run's vectors (S3 vectorized/, then the test index)
straight into the prod index: identity/version parity with the indexer, source selection, upsert
without duplicates, tombstones and their guards, assumed-role credentials."""

import importlib.util
import json
from pathlib import Path

import pytest

import sde_curation.backends.publish as publish_mod
from sde_curation.backends.publish import ProdPublisher, make_version, to_web_document
from sde_curation.backends.s3 import S3
from sde_curation.config import Settings
from tests.fake_aoss import FakeAoss

INDEXER_ROOT = Path(__file__).resolve().parents[2] / "sde-api-scrapers"
KEY, SOURCE = "ex.org", "20260910T120000Z-aaaaaa"
MANIFEST = {"collection_key": KEY, "run_id": SOURCE, "document_count": 0, "collection_name": "Ex",
            "division": "Heliophysics", "document_type": None, "target": "test"}


@pytest.fixture
def aws(monkeypatch):
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        import boto3

        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="cosmos")
        yield


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    async def instant(_s):
        return None

    monkeypatch.setattr(publish_mod.asyncio, "sleep", instant)


def line(path: str, title: str, **kw):
    return {"url": f"https://{KEY}/{path}", "title": title, "full_text": f"text of {path}", **kw}


def export(lines: list[dict], run_id: str = SOURCE) -> dict:
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    manifest = {**MANIFEST, "run_id": run_id, "document_count": len(lines)}
    s3.put_object(Bucket="cosmos", Key=f"curated_collections/{KEY}/{run_id}/documents.jsonl",
                  Body="".join(json.dumps(x) + "\n" for x in lines).encode())
    s3.put_object(Bucket="cosmos", Key=f"curated_collections/{KEY}/{run_id}/manifest.json", Body=json.dumps(manifest).encode())
    return manifest


def vectorized(ln: dict, marker: str, manifest=None) -> dict:
    return {**to_web_document(ln, manifest or {**MANIFEST}), "vectorized_title": [marker],
            "vectorized_full_text": [{"text": ln.get("full_text"), "chunk": {"predicted_value": [marker]}}]}


def put_vectors(run_id: str, records: list[dict], batch: int = 1) -> None:
    import boto3

    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket="cosmos", Key=f"vectorized/{KEY}/{run_id}/batch_{batch:04d}.jsonl",
        Body="\n".join(json.dumps(r) for r in records).encode())


def publisher(prod, test=None, **settings) -> ProdPublisher:
    s = Settings(web_index_name="sde-web", cosmos_index_bucket="cosmos", publish_bulk_docs=2, **settings)
    return ProdPublisher(s, s3=S3("cosmos"), prod=prod, test=test)


async def run(p: ProdPublisher, run_id: str = "20260916T000000Z-bbbbbb") -> dict:
    events: list[dict] = []

    async def progress(e):
        events.append(e)

    status = await p.run(KEY, run_id, SOURCE, progress)
    status["_events"] = events
    return status


# ── contract with the indexer ───────────────────────────────────────────


@pytest.mark.skipif(not (INDEXER_ROOT / "web" / "web_processor.py").exists(), reason="sde-api-scrapers not checked out")
def test_identity_and_version_match_the_indexer():
    spec = importlib.util.spec_from_file_location("indexer_web_processor", INDEXER_ROOT / "web" / "web_processor.py")
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    for ln in (line("a", "A"), line("b", None, document_type="Data"), {"url": "https://ex.org/c", "division": "Astrophysics"},
               line("d", "Ünïcode — title", full_text=None)):
        assert to_web_document(ln, MANIFEST) == mod.to_web_document(ln, MANIFEST)
    assert make_version({"title": "x"}) == mod.make_version({"title": "x"})


# ── the run ─────────────────────────────────────────────────────────────


async def test_publish_picks_newest_matching_vectors_falls_back_to_test_and_tombstones(aws):
    a, b, c, d = line("a", "A new"), line("b", "B"), line("c", "C"), line("d", "D")
    manifest = export([a, b, c, d])
    # older run: A at a stale version, B current; newer run: A current
    put_vectors("20260801T000000Z-000001", [vectorized(line("a", "A old"), "a-old"), vectorized(b, "b-1")])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a-new")])
    prod, test = FakeAoss(), FakeAoss()
    prod.add(vectorized(c, "c-prod", manifest))                                        # unchanged
    prod.add({**vectorized(line("b", "B before"), "b-prod", manifest)})                  # changed → update
    gone = prod.add(vectorized(line("gone", "Gone"), "gone", manifest))                  # removed → tombstone
    prod.add({**vectorized(line("old", "Old"), "old", manifest), "public_visibility": False})  # already a tombstone
    test.add(vectorized(d, "d-test", manifest))                                         # only in the test index

    st = await run(publisher(prod, test))

    assert st["state"] == "succeeded", st
    assert (st["documents_in_export"], st["unchanged"], st["changed"], st["indexed"]) == (4, 1, 3, 3)
    assert (st["from_vectorized"], st["from_test_index"], st["missing"], st["deleted"]) == (2, 1, 0, 1)
    [pa] = prod.by_id(to_web_document(a, manifest)["id"])
    assert pa["vectorized_title"] == ["a-new"] and pa["title"] == "A new" and pa["collection_name"] == "Ex"
    [pb] = prod.by_id(to_web_document(b, manifest)["id"])  # updated in place, not duplicated
    assert pb["vectorized_title"] == ["b-1"] and pb["title"] == "B"
    assert prod.by_id(to_web_document(d, manifest)["id"])[0]["vectorized_title"] == ["d-test"]
    assert prod.store[gone]["public_visibility"] is False
    phases = [e["phase"] for e in st["_events"] if "phase" in e]
    assert phases == ["preflight", "from_vectorized", "from_test_index", "tombstone"]
    # audit copy next to the indexer's status files
    import boto3
    body = boto3.client("s3", region_name="us-east-1").get_object(
        Bucket="cosmos", Key=f"index_runs/{KEY}/20260916T000000Z-bbbbbb/status.json")["Body"].read()
    assert json.loads(body)["state"] == "succeeded"

    # publishing again writes nothing
    again = await run(publisher(prod, test), "20260916T010000Z-cccccc")
    assert again["state"] == "succeeded" and again["changed"] == 0 and again["indexed"] == 0 and again["deleted"] == 0


async def test_missing_vectors_fail_the_run_and_skip_removals(aws):
    a, b = line("a", "A"), line("b", "B")
    manifest = export([a, b])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    gone = prod.add(vectorized(line("gone", "Gone"), "gone", manifest))
    prod.add(vectorized(line("a", "A before"), "a-before", manifest))

    st = await run(publisher(prod, test=None))

    assert st["state"] == "failed" and st["error"] == "vectors_missing"
    assert st["missing"] == 1 and st["missing_urls"] == [b["url"]] and st["indexed"] == 1
    assert st["deleted"] == 0 and st["deletions_skipped"] == ["upsert_incomplete"]
    assert prod.store[gone]["public_visibility"] is True


async def test_failed_bulk_items_are_retried_then_block_removals(aws):
    a, b = line("a", "A"), line("b", "B")
    manifest = export([a, b])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a"), vectorized(b, "b")])
    prod = FakeAoss()
    gone = prod.add(vectorized(line("gone", "Gone"), "gone", manifest))
    prod.add(vectorized(line("a", "A before"), "a-before", manifest))
    prod.fail_ids = {to_web_document(b, manifest)["id"]}

    st = await run(publisher(prod))

    assert st["state"] == "failed" and st["error"] == "upsert_failed"
    assert (st["indexed"], st["failed"]) == (1, 1)
    assert len(prod.bulk_calls) == 3  # first try + 2 retries of the failed item only
    assert prod.store[gone]["public_visibility"] is True and st["deletions_skipped"] == ["upsert_incomplete"]


async def test_deletion_guard_refuses_before_anything_is_written(aws):
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    for i in range(20):
        prod.add(vectorized(line(f"old{i}", f"Old {i}"), "x", manifest))

    st = await run(publisher(prod))
    assert st["state"] == "failed" and st["error"] == "deletion_threshold_exceeded" and not prod.bulk_calls

    st = await run(publisher(prod, publish_deletion_abort_ratio=1.0, publish_deletion_abort_max=5))
    assert st["error"] == "deletion_budget_exceeded" and not prod.bulk_calls


@pytest.mark.parametrize("setup, reason", [
    (lambda prod, m: prod.add({"id": f"/SDE-TDAMM/{KEY}/|https://{KEY}/x", "collection_key": KEY, "public_visibility": True}),
     "id_scheme_collision"),
    (lambda prod, m: [prod.add(vectorized(line("dup", "Dup"), "1", m)), prod.add(vectorized(line("dup", "Dup"), "2", m))],
     "duplicate_business_ids"),
    (lambda prod, m: prod.add({"id": "/SDE/other/|x", "public_visibility": True}), None),
])
async def test_prod_preflight_refusals(aws, setup, reason):
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    setup(prod, manifest)
    st = await run(publisher(prod))
    if reason:
        assert st["state"] == "failed" and st["error"] == reason and not prod.bulk_calls
    else:  # a document of another collection is out of scope, not a refusal
        assert st["state"] == "succeeded" and prod.by_id("/SDE/other/|x")[0]["public_visibility"] is True


async def test_missing_export_or_prod_index_is_refused(aws):
    st = await run(publisher(FakeAoss()))
    assert st["error"] == "export_not_found" and "re-index to test" in st["error_detail"]
    export([line("a", "A")])
    st = await run(publisher(FakeAoss(exists=False)))
    assert st["error"] == "index_not_found"


def test_assumed_role_credentials_refresh(aws):
    from sde_curation.backends.aoss import aoss_client, aoss_credentials

    creds = aoss_credentials(Settings(), "arn:aws:iam::123456789012:role/sde-curation-engine-prod-publisher")
    frozen = creds.get_frozen_credentials()
    assert frozen.access_key and frozen.access_key != "test" and frozen.token
    assert aoss_client(Settings(), "https://abc.us-east-1.aoss.amazonaws.com", None) is not None
