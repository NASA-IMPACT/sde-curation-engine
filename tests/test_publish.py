"""Index to prod = replace the collection in the prod index with the validated test run's vectors:
identity/version parity with the indexer, the wipe (this collection only, every id scheme, before any
write, never on top of missing vectors), source selection (S3, test index, prod), fresh ids, writes
without duplicates, refusals, assumed-role credentials."""

import importlib.util
import json
from pathlib import Path

import pytest

import sde_curation.backends.publish as publish_mod
from sde_curation.backends.publish import ProdPublisher, make_version, make_web_id, to_web_document
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


def ex_doc(path: str, title: str, marker: str, manifest=None, **kw) -> dict:
    """A prod copy of this collection's document (fresh id scheme unless `id` is overridden)."""
    return {**vectorized(line(path, title), marker, manifest), **kw}


def other_doc(key: str, path: str) -> dict:
    ln = {"url": f"https://{key}/{path}", "title": path, "full_text": path}
    return {**to_web_document(ln, {**MANIFEST, "collection_key": key}), "vectorized_title": [key], "vectorized_full_text": []}


def deleted_ids(prod: FakeAoss) -> list[str]:
    return [a["delete"]["_id"] for call in prod.bulk_calls for a in call if "delete" in a]


def ops(prod: FakeAoss) -> list[str]:
    return [next(iter(a)) for call in prod.bulk_calls for a in call if next(iter(a)) in ("index", "update", "delete")]


async def test_publish_wipes_the_collection_and_writes_it_fresh(aws):
    a, b, c, d = line("a", "A new"), line("b", "B"), line("c", "C"), line("d", "D")
    manifest = export([a, b, c, d])
    # older run: A at a stale version, B current; newer run: A current
    put_vectors("20260801T000000Z-000001", [vectorized(line("a", "A old"), "a-old"), vectorized(b, "b-1")])
    put_vectors("20260905T000000Z-000002", [{**vectorized(a, "a-new"), "modified_date": "2024-08-22 21:08:32"}])
    prod, test = FakeAoss(), FakeAoss()
    old_c = prod.add({**vectorized(c, "c-prod", manifest), "id": f"/OLD/{KEY}/{c['url']}"})  # old id scheme; only prod has C's vectors
    old_ids = {
        prod.add(ex_doc("b", "B before", "b-prod", manifest)),                              # changed
        prod.add(ex_doc("gone", "Gone", "gone", manifest)),                                 # no longer curated
        prod.add(ex_doc("old", "Old", "old", manifest, public_visibility=False)),           # hidden earlier
        prod.add({k: v for k, v in ex_doc("legacy", "L", "l", manifest).items() if k != "version"}),  # unversioned
        prod.add({k: v for k, v in ex_doc("noid", "N", "n", manifest).items() if k != "id"}),         # no id at all
        prod.add(ex_doc("dup", "Dup", "1", manifest)), prod.add(ex_doc("dup", "Dup", "2", manifest)),  # duplicates
        prod.add({**ex_doc("tdamm", "T", "t", manifest), "id": f"/SDE-TDAMM/{KEY}/|https://{KEY}/t"}),  # foreign scheme
        old_c,
    }
    test.add({**vectorized(d, "d-test", manifest), "modified_date": "2024-08-22 21:08:32"})  # only in the test index

    st = await run(publisher(prod, test))

    assert st["state"] == "succeeded", st
    assert (st["documents_in_export"], st["changed"], st["wiped"], st["deleted"], st["indexed"]) == (4, 4, 9, 9, 4)
    assert (st["from_vectorized"], st["from_test_index"], st["from_prod_index"], st["missing"]) == (2, 1, 1, 0)
    assert not old_ids & set(prod.store) and len(prod.store) == 4
    fresh = {to_web_document(x, manifest)["id"] for x in (a, b, c, d)}
    assert {s["id"] for s in prod.store.values()} == fresh  # exactly the export, fresh ids, no duplicates
    assert prod.by_id(to_web_document(a, manifest)["id"])[0]["vectorized_title"] == ["a-new"]
    assert prod.by_id(to_web_document(b, manifest)["id"])[0]["vectorized_title"] == ["b-1"]
    assert prod.by_id(to_web_document(c, manifest)["id"])[0]["vectorized_title"] == ["c-prod"]  # carried over from the old copy
    assert prod.by_id(to_web_document(d, manifest)["id"])[0]["vectorized_title"] == ["d-test"]
    # one publish stamp on everything, never the date the vectors carried
    import re
    stamps = {s["modified_date"] for s in prod.store.values()}
    assert len(stamps) == 1 and re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", stamps.pop())
    assert all(s["collection_name"] == "Ex" and s["public_visibility"] is True for s in prod.store.values())
    # every delete happened before the first write
    o = ops(prod)
    assert o.index("index") > max(i for i, x in enumerate(o) if x == "delete") and "update" not in o
    phases = [e["phase"] for e in st["_events"] if "phase" in e]
    assert phases == ["preflight", "stage", "wipe", "write"]
    import boto3
    body = boto3.client("s3", region_name="us-east-1").get_object(
        Bucket="cosmos", Key=f"index_runs/{KEY}/20260916T000000Z-bbbbbb/status.json")["Body"].read()
    assert json.loads(body)["state"] == "succeeded" and json.loads(body)["mode"] == "replace"

    # publishing again is again a fresh start: everything wiped and rewritten
    again = await run(publisher(prod, test), "20260916T010000Z-cccccc")
    assert again["state"] == "succeeded" and again["wiped"] == again["indexed"] == 4 and len(prod.store) == 4
    assert {s["id"] for s in prod.store.values()} == fresh


async def test_other_collections_are_never_touched(aws):
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    mine = {prod.add(ex_doc("x", "X", "x", manifest)), prod.add(ex_doc("y", "Y", "y", manifest))}
    others = [other_doc("other", "p"), other_doc("other", "q"), other_doc(f"{KEY}_2", "r"),  # a key sharing a prefix
              other_doc("ex", "s"), {"id": "/SDE/zzz/|x", "public_visibility": True}]      # and one without a key
    other_ids = {prod.add(o): o for o in others}
    before = {aid: dict(prod.store[aid]) for aid in other_ids}

    st = await run(publisher(prod))

    assert st["state"] == "succeeded", st
    assert {aid: prod.store.get(aid) for aid in other_ids} == before  # byte-identical
    assert set(deleted_ids(prod)) == mine


async def test_page_boundaries_and_lagging_deletes_still_empty_the_collection(aws, monkeypatch):
    monkeypatch.setattr(publish_mod, "_PAGE", 1)  # a duplicate id straddles every page boundary
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    prod.visibility_lag = 2  # deleted documents keep showing for a while
    dups = {prod.add(ex_doc("dup", "Dup", str(n), manifest)) for n in range(3)}

    st = await run(publisher(prod, publish_wipe_poll_s=1, publish_wipe_settle_timeout_s=10))

    assert st["state"] == "succeeded", st
    assert st["wiped"] == 3 and not dups & set(prod.store) and len(prod.store) == 1
    assert sorted(deleted_ids(prod)) == sorted(dups)  # each deleted once, the lagging ones not "re-deleted"


async def test_missing_vectors_refuse_before_anything_is_deleted(aws):
    a, b = line("a", "A"), line("b", "B")
    manifest = export([a, b])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    keep = prod.add(ex_doc("gone", "Gone", "gone", manifest))
    prod.add(ex_doc("b", "B before", "b-stale", manifest))  # prod's copy of B is at another version

    st = await run(publisher(prod, test=None))

    assert st["state"] == "failed" and st["error"] == "vectors_missing" and "nothing was deleted" in st["error_detail"]
    assert st["missing"] == 1 and st["missing_urls"] == [b["url"]]
    assert (st["wiped"], st["indexed"]) == (0, 0) and not prod.bulk_calls and keep in prod.store


async def test_a_failed_delete_stops_the_run_before_anything_is_written(aws):
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    prod.add(ex_doc("x", "X", "x", manifest))
    stuck = prod.add(ex_doc("stuck", "Stuck", "s", manifest))
    prod.fail_ids = {prod.store[stuck]["id"]}

    st = await run(publisher(prod))

    assert st["state"] == "failed" and st["error"] == "wipe_incomplete"
    assert (st["wiped"], st["wipe_failed"], st["indexed"]) == (1, 1, 0) and stuck in prod.store
    assert "index" not in ops(prod)


async def test_lagging_deletes_do_not_block_the_write(aws):
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    prod.visibility_lag = 1000  # the deleted copy keeps showing far past the timeout
    old = prod.add(ex_doc("a", "A before", "a-before", manifest))

    st = await run(publisher(prod, publish_wipe_poll_s=1, publish_wipe_settle_timeout_s=3))

    assert st["state"] == "succeeded", st
    assert st["wiped"] == 1 and st["wipe_lagging"] == 1 and st["indexed"] == 1
    [pa] = prod.by_id(to_web_document(a, manifest)["id"])
    assert pa["vectorized_title"] == ["a"] and old not in prod.store
    assert not [x for call in prod.bulk_calls for x in call if "update" in x]  # the lagging copy is never updated


async def test_unseen_documents_keep_the_wipe_waiting_then_fail(aws):
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])

    class Hiding(FakeAoss):  # counts a document of the collection its searches never return
        def search(self, index, body):
            r = super().search(index, body)
            r["hits"]["hits"] = [h for h in r["hits"]["hits"] if h["_id"] != hidden]
            return r

    prod = Hiding()
    prod.add(ex_doc("x", "X", "x", manifest))
    hidden = prod.add(ex_doc("h", "H", "h", manifest))

    st = await run(publisher(prod, publish_wipe_poll_s=1, publish_wipe_settle_timeout_s=3))

    assert st["state"] == "failed" and st["error"] == "wipe_incomplete" and "index" not in ops(prod)
    assert st["wiped"] == 1 and "wipe_lagging" not in st


async def test_failed_writes_are_retried_without_duplicates(aws):
    a, b, c = line("a", "A"), line("b", "B"), line("c", "C")
    manifest = export([a, b, c])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a"), vectorized(b, "b"), vectorized(c, "c")])
    prod = FakeAoss()
    prod.add(ex_doc("a", "A", "a-before", manifest))
    lost, failing = to_web_document(a, manifest)["id"], to_web_document(c, manifest)["id"]
    prod.lose_response_ids = {lost}  # A's insert lands but its response is lost
    prod.fail_ids = {failing}

    st = await run(publisher(prod, publish_wipe_poll_s=1))

    assert st["state"] == "failed" and st["error"] == "upsert_failed" and (st["indexed"], st["failed"]) == (2, 1)
    [pa] = prod.by_id(lost)  # the retry found the landed insert and updated it: no duplicate
    assert pa["vectorized_title"] == ["a"]
    assert [a["update"]["_id"] for call in prod.bulk_calls for a in call if "update" in a]


def test_the_retry_lookup_ignores_copies_the_wipe_removed():
    prod = FakeAoss()
    fresh = make_web_id(KEY, f"https://{KEY}/a")
    ghost = prod.add({"id": fresh, "collection_key": KEY})  # deleted, but the index still shows it
    landed = prod.add({"id": fresh, "collection_key": KEY})
    p = publisher(prod)
    assert p._aoss_ids(KEY, [fresh], {ghost}) == {fresh: [landed]}


@pytest.mark.parametrize("setup, reason", [
    # this collection's fresh id prefix under another key: outside the wipe, so stop
    (lambda prod, m: prod.add({"id": f"/SDE/{KEY}/|https://{KEY}/x", "collection_key": "someone-else", "public_visibility": True}),
     "orphaned_prefixed_docs"),
    (lambda prod, m: prod.add({"id": f"/SDE/{KEY}/|https://{KEY}/x", "public_visibility": True}), "orphaned_prefixed_docs"),
])
async def test_prod_preflight_refusals(aws, setup, reason):
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    keep = prod.add(ex_doc("b", "B", "b", manifest))
    setup(prod, manifest)
    st = await run(publisher(prod))
    assert st["state"] == "failed" and st["error"] == reason and not prod.bulk_calls and keep in prod.store


async def test_old_scheme_and_duplicate_ids_are_no_longer_refusals(aws):
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])
    prod = FakeAoss()
    prod.add({"id": f"/SDE-TDAMM/{KEY}/|https://{KEY}/x", "collection_key": KEY, "public_visibility": True})
    prod.add(ex_doc("dup", "Dup", "1", manifest)), prod.add(ex_doc("dup", "Dup", "2", manifest))
    st = await run(publisher(prod))
    assert st["state"] == "succeeded" and st["wiped"] == 3 and [s["id"] for s in prod.store.values()] == [to_web_document(a, manifest)["id"]]


async def test_a_foreign_hit_in_the_scan_refuses_with_nothing_deleted(aws):
    a = line("a", "A")
    manifest = export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])

    class Leaky(FakeAoss):  # an index whose filter lets another collection through
        def search(self, index, body):
            r = super().search(index, body)
            if "aggs" not in body:
                r["hits"]["hits"] += [{"_id": "x", "_source": {"id": "/SDE/other/|x", "collection_key": "other"}, "sort": ["~"]}]
            return r

    prod = Leaky()
    keep = prod.add(ex_doc("b", "B", "b", manifest))
    st = await run(publisher(prod))
    assert st["state"] == "failed" and st["error"] == "foreign_documents_in_scan" and not prod.bulk_calls and keep in prod.store


async def test_an_ineffective_scope_filter_refuses(aws):
    a = line("a", "A")
    export([a])
    put_vectors("20260905T000000Z-000002", [vectorized(a, "a")])

    class Unscoped(FakeAoss):  # e.g. collection_key mapped as analysed text
        def search(self, index, body):
            if "aggs" in body and "keys" in body["aggs"]:
                return {"aggregations": {"keys": {"buckets": [{"key": KEY}, {"key": "other"}]}, "missing_key": {"doc_count": 0}}}
            return super().search(index, body)

    prod = Unscoped()
    st = await run(publisher(prod))
    assert st["error"] == "scope_filter_ineffective" and not prod.bulk_calls


async def test_missing_empty_export_or_prod_index_is_refused(aws):
    st = await run(publisher(FakeAoss()))
    assert st["error"] == "export_not_found" and "re-index to test" in st["error_detail"]
    prod = FakeAoss()
    prod.add(ex_doc("b", "B", "b"))
    export([])
    st = await run(publisher(prod))
    assert st["error"] == "empty_export" and not prod.bulk_calls and len(prod.store) == 1
    export([line("a", "A")])
    st = await run(publisher(FakeAoss(exists=False)))
    assert st["error"] == "index_not_found"


def test_assumed_role_credentials_refresh(aws):
    from sde_curation.backends.aoss import aoss_client, aoss_credentials

    creds = aoss_credentials(Settings(), "arn:aws:iam::123456789012:role/sde-curation-engine-prod-publisher")
    frozen = creds.get_frozen_credentials()
    assert frozen.access_key and frozen.access_key != "test" and frozen.token
    assert aoss_client(Settings(), "https://abc.us-east-1.aoss.amazonaws.com", None) is not None
