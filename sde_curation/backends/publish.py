"""Publish to prod: write the vectors the validated test run already produced straight into the
production web index. Nothing is re-chunked or re-vectorized.

Source of truth is the gating test run's export (curated_collections/<key>/<test_run>/), i.e.
exactly what was validated. For each document the engine needs a vectorized copy whose `version`
matches the export:

  1. s3://COSMOS_INDEX_BUCKET/vectorized/<key>/<run>/batch_NNNN.jsonl, newest run first. The indexer
     only vectorizes *changed* documents, so a collection's vectors are spread over many runs.
  2. the test index itself (full _source, embeddings included) for anything S3 does not have,
     e.g. documents indexed before the S3 copies existed.

A document found in neither fails the run (and no deletions happen). Once everything is written,
the collection's prod documents the export no longer holds are deleted — really removed, whether or
not they carry a `version` (documents from before the indexer do not) and whether or not an earlier
publish had hidden them.

The identity, versioning, scoping and deletion rules are ports of the indexer's
(sde-api-scrapers/web/{web_processor,scope,id_collision,deletion_guard}.py) and must not drift:
a different id or version would duplicate documents in the shared sde-web index.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import tempfile
import time
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..config import Settings
from ..engine.export import export_prefix, status_prefix
from .index import IndexError_
from .s3 import S3

log = logging.getLogger(__name__)

ProgressCb = Callable[[dict[str, Any]], Awaitable[None]]

# web/web_processor.py
_PASSTHROUGH_FIELDS = ("url", "title", "full_text")
_COLLECTION_DEFAULTED_FIELDS = ("document_type", "division")
_VERSION_FIELDS = ("title", "full_text", "document_type", "division")
VECTOR_FIELDS = ("vectorized_title", "vectorized_full_text")
# Everything an sde-web document carries; anything else in a vectorized record is dropped.
DOC_FIELDS = (*_PASSTHROUGH_FIELDS, *_COLLECTION_DEFAULTED_FIELDS, "id", "collection_key", "collection_name",
              "public_visibility", "is_metadata_viewer", "executive_order_filter", "version", "modified_date",
              *VECTOR_FIELDS)
# web_processor.format_modified_date: the format sde-web already holds, in UTC
_MODIFIED_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_PAGE = 1000
_LOOKUP_CHUNK = 100
_TEST_FETCH_CHUNK = 20  # whole documents with embeddings: keep responses small
_BULK_ATTEMPTS = 3
_MAX_REPORTED = 50


class PublishRefused(IndexError_):
    """The run stopped deliberately before writing anything unsafe; `reason` goes to status.json."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


# ── identity (web/web_processor.py) ────────────────────────────────────


def web_id_prefix(collection_key: str) -> str:
    return f"/SDE/{collection_key}/|"


def make_web_id(collection_key: str, url: str) -> str:
    return f"{web_id_prefix(collection_key)}{url}"


def make_version(doc: dict[str, Any]) -> str:
    payload = json.dumps([doc.get(f) for f in _VERSION_FIELDS], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def to_web_document(line: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    """Export line → sde-web document without embeddings (web_processor.to_web_document)."""
    url = line.get("url")
    if not url:
        raise ValueError(f"export line has no url: {line!r}")
    key = manifest["collection_key"]
    doc: dict[str, Any] = {f: line.get(f) for f in _PASSTHROUGH_FIELDS}
    for f in _COLLECTION_DEFAULTED_FIELDS:
        v = line.get(f)
        doc[f] = v if v is not None else manifest.get(f)
    doc["id"] = make_web_id(key, url)
    doc["collection_key"] = key
    doc["collection_name"] = manifest.get("collection_name")
    doc["public_visibility"] = True
    doc["is_metadata_viewer"] = False
    doc["executive_order_filter"] = False
    doc["version"] = make_version(doc)
    return doc


# ── scope (web/scope.py, web/id_collision.py) ──────────────────────────


def scope_filter(collection_key: str) -> dict[str, Any]:
    """This collection's visible documents: the state scan. A hidden one the export holds again
    reads as changed, so it is written back visible."""
    return {"bool": {"filter": [{"term": {"collection_key": collection_key}},
                                {"term": {"public_visibility": True}}]}}


def collection_filter(collection_key: str) -> dict[str, Any]:
    """Every document of the collection, hidden or not, versioned or not: the scope of deletions."""
    return {"term": {"collection_key": collection_key}}


def assert_owned(collection_key: str, ids, boundary: str) -> None:
    prefix = web_id_prefix(collection_key)
    foreign = [i for i in ids if not (isinstance(i, str) and i.startswith(prefix))]
    if foreign:
        raise PublishRefused(
            "foreign_documents_in_scan",
            f"{boundary}: {len(foreign)} document(s) outside collection '{collection_key}' "
            f"(expected id prefix {prefix!r}, saw {foreign[:5]!r}) — refusing with nothing deleted",
        )


def probe_scope(client, index: str, collection_key: str) -> None:
    body = {"size": 0, "query": scope_filter(collection_key),
            "aggs": {"keys": {"terms": {"field": "collection_key", "size": 5}},
                     "missing_key": {"missing": {"field": "collection_key"}}}}
    aggs = client.search(index=index, body=body).get("aggregations") or {}
    found = [b.get("key") for b in (aggs.get("keys") or {}).get("buckets") or []]
    missing = (aggs.get("missing_key") or {}).get("doc_count") or 0
    if missing or len(found) > 1 or (found and found[0] != collection_key):
        raise PublishRefused(
            "scope_filter_ineffective",
            f"[{index}] the collection filter matched {found!r} (+{missing} without collection_key), "
            f"expected only ['{collection_key}']",
        )


def check_id_collisions(client, index: str, collection_key: str) -> None:
    prefix = web_id_prefix(collection_key)
    mismatched = client.count(index=index, body={"query": {"bool": {
        "filter": [{"term": {"collection_key": collection_key}}],
        "must_not": [{"prefix": {"id": prefix}}],
    }}})["count"]
    if mismatched:
        raise PublishRefused(
            "id_scheme_collision",
            f"[{index}] {mismatched} document(s) of '{collection_key}' do not carry the id prefix {prefix!r}: "
            f"publishing would duplicate them instead of updating — remediate the ids first",
        )
    r = client.search(index=index, body={
        "size": 0, "query": {"term": {"collection_key": collection_key}},
        "aggs": {"dups": {"terms": {"field": "id", "size": 1, "min_doc_count": 2}}},
    })
    buckets = ((r.get("aggregations") or {}).get("dups") or {}).get("buckets") or []
    if buckets:
        raise PublishRefused(
            "duplicate_business_ids",
            f"[{index}] '{collection_key}' has ids carried by more than one document "
            f"(e.g. {buckets[0].get('key')!r} ×{buckets[0].get('doc_count')}) — remediate first",
        )


def scan_state(client, index: str, collection_key: str) -> dict[str, str]:
    """{id: version} of the collection's visible, versioned documents (search_after paging)."""
    out: dict[str, str] = {}
    last = None
    while True:
        body: dict[str, Any] = {"size": _PAGE, "_source": ["id", "version"],
                                "query": scope_filter(collection_key), "sort": [{"id": "asc"}]}
        if last is not None:
            body["search_after"] = last
        hits = client.search(index=index, body=body)["hits"]["hits"]
        for h in hits:
            src = h.get("_source") or {}
            if src.get("id") and src.get("version") is not None:
                out[src["id"]] = str(src["version"])
        last = hits[-1].get("sort") if hits else None
        if len(hits) < _PAGE or last is None:
            return out


def scan_ids(client, index: str, collection_key: str) -> list[str]:
    """Every id the collection holds. Unlike scan_state it needs no `version`, so documents indexed
    before the indexer existed are seen — and removed once they are no longer curated."""
    out: list[str] = []
    last = None
    while True:
        body: dict[str, Any] = {"size": _PAGE, "_source": ["id"], "query": collection_filter(collection_key),
                                "sort": [{"id": "asc"}]}
        if last is not None:
            body["search_after"] = last
        hits = client.search(index=index, body=body)["hits"]["hits"]
        out += [i for h in hits if (i := (h.get("_source") or {}).get("id"))]
        last = hits[-1].get("sort") if hits else None
        if len(hits) < _PAGE or last is None:
            return out


def deletion_decision(candidates: list[str], state_count: int, settings: Settings) -> float:
    """Raise when the deletions would remove too much (web/deletion_guard.py); returns the ratio."""
    if not candidates:
        return 0.0
    ratio = len(candidates) / state_count if state_count else 1.0
    if len(candidates) > settings.publish_deletion_abort_max:
        raise PublishRefused(
            "deletion_budget_exceeded",
            f"{len(candidates)} documents would be removed from prod, above PUBLISH_DELETION_ABORT_MAX="
            f"{settings.publish_deletion_abort_max} — refusing with nothing written",
        )
    if ratio > settings.publish_deletion_abort_ratio:
        raise PublishRefused(
            "deletion_threshold_exceeded",
            f"{len(candidates)}/{state_count} ({ratio:.0%}) of the collection's prod documents would be removed, "
            f"above PUBLISH_DELETION_ABORT_RATIO={settings.publish_deletion_abort_ratio:.0%} — refusing with nothing written",
        )
    return ratio


def has_vectors(doc: dict[str, Any]) -> bool:
    return isinstance(doc.get("vectorized_title"), list) and isinstance(doc.get("vectorized_full_text"), list)


# ── the run ────────────────────────────────────────────────────────────


class ProdPublisher:
    """One publish of one collection. `prod` / `test` are opensearch-py clients (or fakes);
    `test` may be None when there is no test endpoint to fall back to."""

    name = "publish"

    def __init__(self, settings: Settings, *, s3: S3, prod, test=None):
        self.s = settings
        self.s3 = s3
        self.prod = prod
        self.test = test
        self.index = settings.web_index_name

    async def _call(self, fn, *args, **kw):
        return await asyncio.to_thread(fn, *args, **kw)

    async def run(self, collection_key: str, run_id: str, source_run_id: str, on_progress: ProgressCb) -> dict[str, Any]:
        t0 = time.time()
        status: dict[str, Any] = {
            "run_id": run_id, "collection_key": collection_key, "target": "prod", "index": self.index,
            "mode": "publish_vectors", "source_test_run": source_run_id, "state": "failed",
            "documents_in_export": 0, "unchanged": 0, "changed": 0, "indexed": 0, "failed": 0, "deleted": 0,
            "from_vectorized": 0, "from_test_index": 0, "missing": 0, "missing_urls": [],
            "deletion_ratio": 0.0, "deletions_skipped": [], "error": None,
            "started_at": datetime.now(UTC).isoformat(), "finished_at": None,
        }
        try:
            await self._run(collection_key, source_run_id, status, on_progress)
            if status["failed"] or status["missing"]:
                status["error"] = "vectors_missing" if status["missing"] else "upsert_failed"
            else:
                status["state"] = "succeeded"
        except PublishRefused as e:
            status["error"], status["error_detail"] = e.reason, str(e)[:500]
            log.error("[%s] publish refused (%s): %s", collection_key, e.reason, e)
        except Exception as e:  # always reported through status.json
            status["error"], status["error_detail"] = "publish_error", f"{type(e).__name__}: {e}"[:500]
            log.exception("[%s] publish failed", collection_key)
        finally:
            status["finished_at"] = datetime.now(UTC).isoformat()
            status["duration_seconds"] = round(time.time() - t0, 2)
            try:
                await self.s3.put_json(f"{status_prefix(collection_key, run_id)}/status.json", status)
            except Exception as e:  # noqa: BLE001 - audit copy only
                log.warning("could not write publish status.json: %s", e)
        return status

    async def _run(self, key: str, source_run_id: str, status: dict[str, Any], progress: ProgressCb) -> None:
        # 1. what was validated
        await progress({"phase": "preflight", "source_test_run": source_run_id})
        manifest, expected, urls = await self._load_export(key, source_run_id)
        status["documents_in_export"] = len(expected)

        # 2. pre-flight against prod — before anything is written
        try:
            exists = await self._call(self.prod.indices.exists, index=self.index)
        except Exception as e:
            raise PublishRefused("prod_index_unreachable", f"cannot reach the prod index {self.index}: {e}") from e
        if not exists:
            raise PublishRefused("index_not_found", f"the prod index {self.index} does not exist — it is never created here")
        await self._call(probe_scope, self.prod, self.index, key)
        await self._call(check_id_collisions, self.prod, self.index, key)
        state = await self._call(scan_state, self.prod, self.index, key)
        assert_owned(key, state.keys(), "state_scan")

        needed = {i for i, v in expected.items() if state.get(i) != v}
        status["unchanged"] = len(expected) - len(needed)
        status["changed"] = len(needed)
        held = set(await self._call(scan_ids, self.prod, self.index, key)) | set(state)
        assert_owned(key, held, "collection_scan")
        candidates = sorted(held - set(expected))
        status["deletion_ratio"] = round(deletion_decision(candidates, len(held), self.s), 6)
        await progress({"phase": "from_vectorized", "documents_in_export": len(expected),
                        "unchanged": status["unchanged"], "changed": len(needed), "to_remove": len(candidates)})

        batch = _Batch(self.s)

        async def flush() -> None:
            if batch.docs:
                ok, bad = await self._upsert(key, batch.take())
                status["indexed"] += ok
                status["failed"] += bad
                await progress({"indexed": status["indexed"], "failed": status["failed"]})

        # 3. vectors from S3, newest run first
        async for rec in self._vectorized_records(key):
            i = rec.get("id")
            if i in needed and rec.get("version") == expected[i] and has_vectors(rec):
                needed.discard(i)
                status["from_vectorized"] += 1
                if batch.add(self._normalize(rec, manifest)):
                    await flush()
                if not needed:
                    break
        await flush()

        # 4. whatever S3 did not have: the test index
        if needed and self.test is not None:
            await progress({"phase": "from_test_index", "remaining": len(needed)})
            try:
                for chunk in _chunks(sorted(needed), _TEST_FETCH_CHUNK):
                    for src in await self._call(self._test_docs, key, chunk):
                        i = src.get("id")
                        if i in needed and src.get("version") == expected[i] and has_vectors(src):
                            needed.discard(i)
                            status["from_test_index"] += 1
                            if batch.add(self._normalize(src, manifest)):
                                await flush()
                await flush()
            except Exception as e:  # noqa: BLE001 - the rest is reported as missing
                log.warning("[%s] test-index fallback failed: %s", key, e)
                status["test_index_error"] = f"{type(e).__name__}: {e}"[:300]
                await flush()

        status["missing"] = len(needed)
        status["missing_urls"] = sorted(urls[i] for i in needed)[:_MAX_REPORTED]

        # 5. deletions — never on top of an incomplete write
        if status["failed"] or needed:
            if candidates:
                status["deletions_skipped"] = ["upsert_incomplete"]
            return
        if candidates:
            await progress({"phase": "delete", "to_remove": len(candidates)})
            assert_owned(key, candidates, "deletion_candidates")
            status["deleted"], delete_failed = await self._delete(key, candidates)
            if delete_failed:
                status["delete_failed"] = delete_failed

    async def _load_export(self, key: str, run_id: str) -> tuple[dict[str, Any], dict[str, str], dict[str, str]]:
        prefix = export_prefix(key, run_id)
        manifest = await self.s3.get_json(f"{prefix}/manifest.json")
        if manifest is None:
            raise PublishRefused(
                "export_not_found",
                f"the export of test run {run_id} is gone from {self.s3.url(prefix)} (kept 30 days) — re-index to test first",
            )
        if manifest.get("collection_key") != key or manifest.get("run_id") != run_id:
            raise PublishRefused("export_incomplete", f"manifest at {prefix} is for {manifest.get('collection_key')}/{manifest.get('run_id')}")
        expected: dict[str, str] = {}
        urls: dict[str, str] = {}
        lines = 0
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "documents.jsonl"
            await self._call(self.s3.client.download_file, self.s3.bucket, f"{prefix}/documents.jsonl", str(path))
            for line in _read_jsonl(path):
                doc = to_web_document(line, manifest)
                expected[doc["id"]] = doc["version"]
                urls[doc["id"]] = doc["url"]
                lines += 1
        declared = manifest.get("document_count")
        if declared is not None and lines != declared:
            raise PublishRefused("export_incomplete", f"export declares {declared} documents but has {lines} lines")
        if len(expected) != lines:
            raise PublishRefused("export_incomplete", f"{lines} export lines collapse to {len(expected)} unique ids")
        assert_owned(key, expected.keys(), "export_ids")
        return manifest, expected, urls

    async def _vectorized_records(self, key: str):
        client, bucket = self.s3.client, self.s3.bucket
        base = f"vectorized/{key}/"

        def runs() -> list[str]:
            out: list[str] = []
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=base, Delimiter="/"):
                out += [p["Prefix"] for p in page.get("CommonPrefixes") or []]
            return sorted(out, reverse=True)  # run ids start with a UTC timestamp

        def batches(prefix: str) -> list[str]:
            keys: list[str] = []
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                keys += [o["Key"] for o in page.get("Contents") or [] if o["Key"].endswith(".jsonl")]
            return sorted(keys)

        def read(k: str) -> list[dict[str, Any]]:
            body = client.get_object(Bucket=bucket, Key=k)["Body"].read().decode("utf-8")
            return [json.loads(ln) for ln in body.splitlines() if ln.strip()]

        for run_prefix in await self._call(runs):
            for k in await self._call(batches, run_prefix):
                try:
                    records = await self._call(read, k)
                except (ValueError, UnicodeDecodeError) as e:
                    log.warning("skipping unreadable vectorized batch s3://%s/%s: %s", bucket, k, e)
                    continue
                for rec in records:
                    yield rec

    def _normalize(self, rec: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
        doc = {f: rec.get(f) for f in DOC_FIELDS if f in rec}
        # when the indexer wrote this version; records from before it stamped one get the publish time
        if not doc.get("modified_date"):
            doc["modified_date"] = datetime.now(UTC).strftime(_MODIFIED_DATE_FORMAT)
        # non-content fields follow the validated export, not whenever the vectors were made
        doc["collection_key"] = manifest["collection_key"]
        doc["collection_name"] = manifest.get("collection_name")
        doc["public_visibility"] = True
        doc["is_metadata_viewer"] = False
        doc["executive_order_filter"] = False
        return doc

    def _test_docs(self, key: str, ids: list[str]) -> list[dict[str, Any]]:
        r = self.test.search(index=self.index, body={
            "size": len(ids) * 5,
            "query": {"bool": {"filter": [{"term": {"collection_key": key}}, {"terms": {"id": ids}}]}},
        })
        return [h.get("_source") or {} for h in r["hits"]["hits"]]

    def _aoss_ids(self, key: str, ids: list[str]) -> dict[str, list[str]]:
        """Every copy (AOSS _id) of each business id inside the collection, hidden ones included."""
        filters: list[dict[str, Any]] = [collection_filter(key), {"terms": {"id": ids}}]
        r = self.prod.search(index=self.index, body={"size": min(len(ids) * 5, 10_000), "_source": ["id"],
                                                     "query": {"bool": {"filter": filters}}})
        out: dict[str, list[str]] = {}
        for h in r["hits"]["hits"]:
            i = (h.get("_source") or {}).get("id")
            if i:
                out.setdefault(i, []).append(h["_id"])
        return out

    async def _bulk(self, lines: list[dict[str, Any]], keys: list[str]) -> set[str]:
        """Send one bulk request; returns the keys (one per action) whose item failed."""
        body = "\n".join(json.dumps(x, ensure_ascii=False) for x in lines) + "\n"
        r = await self._call(self.prod.bulk, body=body)
        failed: set[str] = set()
        for k, item in zip(keys, r.get("items") or [], strict=False):
            res = next(iter(item.values()), {})
            if res.get("error") or int(res.get("status", 500)) >= 300:
                failed.add(k)
                log.warning("bulk item failed for %s: %s", k, str(res.get("error"))[:300])
        if len(r.get("items") or []) < len(keys):
            failed.update(keys[len(r.get("items") or []):])
        return failed

    async def _upsert(self, key: str, docs: list[dict[str, Any]]) -> tuple[int, int]:
        """update existing copies (matched on the business id, hidden ones included so they come
        back), index the rest. Failed items are re-looked-up and retried, so a lost response to an
        insert becomes an update rather than a duplicate."""
        assert_owned(key, [d["id"] for d in docs], "upsert_batch")
        pending = {d["id"]: d for d in docs}
        for attempt in range(1, _BULK_ATTEMPTS + 1):
            try:
                existing = await self._call(self._aoss_ids, key, list(pending))
                lines: list[dict[str, Any]] = []
                keys: list[str] = []
                for i, d in pending.items():
                    copies = existing.get(i) or []
                    if copies:
                        for aid in copies:
                            lines += [{"update": {"_index": self.index, "_id": aid}}, {"doc": d}]
                            keys.append(i)
                    else:
                        lines += [{"index": {"_index": self.index}}, d]
                        keys.append(i)
                failed = await self._bulk(lines, keys)
            except Exception as e:  # noqa: BLE001 - transport error: retry the whole batch
                log.warning("bulk upsert attempt %d/%d failed: %s", attempt, _BULK_ATTEMPTS, e)
                failed = set(pending)
            pending = {i: d for i, d in pending.items() if i in failed}
            if not pending:
                break
            if attempt < _BULK_ATTEMPTS:
                await asyncio.sleep(2 ** attempt)
        return len(docs) - len(pending), len(pending)

    async def _delete(self, key: str, ids: list[str]) -> tuple[int, int]:
        """Really remove every copy of these business ids; returns (copies deleted, copies failed)."""
        done = failed = 0
        for chunk in _chunks(ids, _LOOKUP_CHUNK):
            copies = await self._call(self._aoss_ids, key, chunk)
            aids = [a for i in chunk for a in copies.get(i, [])]
            for part in _chunks(aids, self.s.publish_bulk_docs):
                bad = await self._bulk([{"delete": {"_index": self.index, "_id": a}} for a in part], part)
                done += len(part) - len(bad)
                failed += len(bad)
        return done, failed


class _Batch:
    def __init__(self, settings: Settings):
        self.s = settings
        self.docs: list[dict[str, Any]] = []
        self.bytes = 0

    def add(self, doc: dict[str, Any]) -> bool:
        """Buffer a document; True when the batch is full."""
        self.docs.append(doc)
        self.bytes += len(json.dumps(doc, ensure_ascii=False).encode("utf-8"))
        return len(self.docs) >= self.s.publish_bulk_docs or self.bytes >= self.s.publish_bulk_max_bytes

    def take(self) -> list[dict[str, Any]]:
        docs, self.docs, self.bytes = self.docs, [], 0
        return docs


def _chunks(items: list, n: int) -> Iterator[list]:
    for i in range(0, len(items), n):
        yield items[i:i + n]


def _read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as fh:
        for ln in fh:
            if ln.strip():
                yield json.loads(ln)


def make_prod_publisher(settings: Settings) -> ProdPublisher:
    from .aoss import aoss_client

    if not settings.cosmos_index_bucket:
        raise IndexError_("COSMOS_INDEX_BUCKET is not set")
    if not settings.opensearch_endpoint_prod:
        raise IndexError_("OPENSEARCH_ENDPOINT_PROD is not set — nowhere to publish to")
    prod = aoss_client(settings, settings.opensearch_endpoint_prod, settings.prod_index_role_arn,
                       session_name="sde-curation-publish", timeout=300)
    test = None
    if settings.opensearch_endpoint_test and settings.opensearch_endpoint_test != settings.opensearch_endpoint_prod:
        test = aoss_client(settings, settings.opensearch_endpoint_test, settings.validation_assume_role_arn,
                           session_name="sde-curation-publish-read", timeout=120)
    return ProdPublisher(settings, s3=S3(settings.cosmos_index_bucket, region=settings.aws_region), prod=prod, test=test)
