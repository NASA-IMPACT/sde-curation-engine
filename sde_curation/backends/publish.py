"""Publish to prod: replace the collection in the production web index with the vectors the
validated test run already produced. Nothing is re-chunked or re-vectorized.

Every publish is a fresh start for the collection. Every document carrying its `collection_key` is
deleted, whatever its `id` looks like (old id schemes, duplicates, hidden, unversioned), and the
whole validated set is written back under freshly minted ids (`/SDE/<key>/|<url>`).

Source of truth is the gating test run's export (curated_collections/<key>/<test_run>/), i.e.
exactly what was validated. For each document the engine needs a vectorized copy (matched on `url`)
whose `version` matches the export:

  1. s3://COSMOS_INDEX_BUCKET/vectorized/<key>/<run>/batch_NNNN.jsonl, newest run first. The indexer
     only vectorizes *changed* documents, so a collection's vectors are spread over many runs.
  2. the test index itself (full _source, embeddings included).
  3. the prod index, read before anything is deleted: a current prod copy is exactly what would be
     written back, whatever id it carried.

Order is the safety net, because there is no backup and no deletion guard:

  export → read-only pre-flight → enumerate the wipe set → stage every vector to a local file →
  wipe (by explicit AOSS _id only) → confirm the collection reads empty → write.

A document without vectors, a foreign document in any scan, or a failed delete stops the run
*before* the write. Nothing outside the collection is ever deleted: every delete target comes from
a `term collection_key` query whose isolation is probed first, and every hit is re-checked in code.

The id and version rules are ports of the indexer's (sde-api-scrapers/web/web_processor.py) and
must not drift. The test index and its indexer-side guards are not touched here; the test index is
only read, as a vector source.
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
_MAX_WINDOW = 10_000  # AOSS max result window
_LOOKUP_CHUNK = 100
_FETCH_CHUNK = 20  # whole documents with embeddings: keep responses small
_BULK_ATTEMPTS = 3
_MAX_REPORTED = 50


class PublishRefused(IndexError_):
    """The run stopped deliberately before writing anything; `reason` goes to status.json."""

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


# ── scope ──────────────────────────────────────────────────────────────


def scope_filter(collection_key: str) -> dict[str, Any]:
    """This collection's visible documents (web/scope.py)."""
    return {"bool": {"filter": [{"term": {"collection_key": collection_key}},
                                {"term": {"public_visibility": True}}]}}


def collection_filter(collection_key: str) -> dict[str, Any]:
    """Every document of the collection, hidden or not, whatever its id: the scope of the wipe."""
    return {"term": {"collection_key": collection_key}}


def assert_owned(collection_key: str, ids, boundary: str) -> None:
    """Every id the engine writes carries the fresh prefix of this collection."""
    prefix = web_id_prefix(collection_key)
    foreign = [i for i in ids if not (isinstance(i, str) and i.startswith(prefix))]
    if foreign:
        raise PublishRefused(
            "foreign_documents_in_scan",
            f"{boundary}: {len(foreign)} document(s) outside collection '{collection_key}' "
            f"(expected id prefix {prefix!r}, saw {foreign[:5]!r}) — refusing with nothing deleted",
        )


def probe_scope(client, index: str, collection_key: str) -> None:
    """The `collection_key` term filter must match exactly this key — it is the wipe boundary."""
    for label, query in (("visible", scope_filter(collection_key)), ("collection", collection_filter(collection_key))):
        body = {"size": 0, "query": query,
                "aggs": {"keys": {"terms": {"field": "collection_key", "size": 5}},
                         "missing_key": {"missing": {"field": "collection_key"}}}}
        aggs = client.search(index=index, body=body).get("aggregations") or {}
        found = [b.get("key") for b in (aggs.get("keys") or {}).get("buckets") or []]
        missing = (aggs.get("missing_key") or {}).get("doc_count") or 0
        if missing or len(found) > 1 or (found and found[0] != collection_key):
            raise PublishRefused(
                "scope_filter_ineffective",
                f"[{index}] the {label} filter matched {found!r} (+{missing} without collection_key), "
                f"expected only ['{collection_key}']",
            )


def check_prefix_orphans(client, index: str, collection_key: str) -> None:
    """Documents under this collection's fresh id prefix but another (or no) collection_key are
    outside the wipe, and would sit next to the fresh documents under the same id: stop, delete nothing."""
    prefix = web_id_prefix(collection_key)
    n = client.count(index=index, body={"query": {"bool": {
        "filter": [{"prefix": {"id": prefix}}],
        "must_not": [collection_filter(collection_key)],
    }}})["count"]
    if n:
        raise PublishRefused(
            "orphaned_prefixed_docs",
            f"[{index}] {n} document(s) carry the id prefix {prefix!r} but not collection_key '{collection_key}': "
            f"they are outside this collection, so they are not deleted, and they would duplicate the fresh "
            f"documents — remediate them first",
        )


def scan_collection_copies(client, index: str, collection_key: str) -> dict[str, str]:
    """{AOSS _id: business id ("" when missing)} of every document of the collection, hidden, versioned
    or not, whatever its id scheme. Every hit is re-checked in code: one with another collection_key
    aborts the run before anything is deleted."""
    out: dict[str, str] = {}

    def take(hits) -> None:
        for h in hits:
            src = h.get("_source") or {}
            if src.get("collection_key") != collection_key:
                raise PublishRefused(
                    "foreign_documents_in_scan",
                    f"[{index}] the scan of '{collection_key}' returned _id {h.get('_id')!r} with collection_key "
                    f"{src.get('collection_key')!r} — refusing with nothing deleted",
                )
            out[h["_id"]] = src.get("id") or ""

    with_id = {"bool": {"filter": [collection_filter(collection_key), {"exists": {"field": "id"}}]}}
    last = None
    while True:
        body: dict[str, Any] = {"size": _PAGE, "_source": ["id", "collection_key"], "query": with_id,
                                "sort": [{"id": "asc"}]}
        if last is not None:
            body["search_after"] = last
        hits = client.search(index=index, body=body)["hits"]["hits"]
        take(hits)
        last = hits[-1].get("sort") if hits else None
        if len(hits) < _PAGE or last is None:
            break
    # no id to sort on: one window; the wipe's settle loop picks up anything past it
    without_id = {"bool": {"filter": [collection_filter(collection_key)], "must_not": [{"exists": {"field": "id"}}]}}
    take(client.search(index=index, body={"size": _MAX_WINDOW, "_source": ["id", "collection_key"],
                                          "query": without_id})["hits"]["hits"])
    return out


def count_collection(client, index: str, collection_key: str) -> int:
    return client.count(index=index, body={"query": collection_filter(collection_key)})["count"]


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
        self.published_at = datetime.now(UTC).strftime(_MODIFIED_DATE_FORMAT)  # one stamp per publish

    async def _call(self, fn, *args, **kw):
        return await asyncio.to_thread(fn, *args, **kw)

    async def run(self, collection_key: str, run_id: str, source_run_id: str, on_progress: ProgressCb) -> dict[str, Any]:
        t0 = time.time()
        self.published_at = datetime.now(UTC).strftime(_MODIFIED_DATE_FORMAT)
        status: dict[str, Any] = {
            "run_id": run_id, "collection_key": collection_key, "target": "prod", "index": self.index,
            "mode": "replace", "source_test_run": source_run_id, "state": "failed",
            "documents_in_export": 0, "changed": 0, "wiped": 0, "wipe_failed": 0, "indexed": 0, "failed": 0,
            "deleted": 0, "from_vectorized": 0, "from_test_index": 0, "from_prod_index": 0,
            "missing": 0, "missing_urls": [], "error": None,
            "started_at": datetime.now(UTC).isoformat(), "finished_at": None,
        }
        try:
            await self._run(collection_key, source_run_id, status, on_progress)
            if status["failed"]:
                status["error"] = "upsert_failed"
            else:
                status["state"] = "succeeded"
        except PublishRefused as e:
            status["error"], status["error_detail"] = e.reason, str(e)[:500]
            log.error("[%s] publish refused (%s): %s", collection_key, e.reason, e)
        except Exception as e:  # always reported through status.json
            status["error"], status["error_detail"] = "publish_error", f"{type(e).__name__}: {e}"[:500]
            log.exception("[%s] publish failed", collection_key)
        finally:
            status["deleted"] = status["wiped"]
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
        status["documents_in_export"] = status["changed"] = len(expected)

        # 2. read-only pre-flight against prod
        try:
            exists = await self._call(self.prod.indices.exists, index=self.index)
        except Exception as e:
            raise PublishRefused("prod_index_unreachable", f"cannot reach the prod index {self.index}: {e}") from e
        if not exists:
            raise PublishRefused("index_not_found", f"the prod index {self.index} does not exist — it is never created here")
        await self._call(probe_scope, self.prod, self.index, key)
        await self._call(check_prefix_orphans, self.prod, self.index, key)

        # 3. what the wipe will remove (verified) — before staging, so a foreign hit stops everything early
        copies = await self._call(scan_collection_copies, self.prod, self.index, key)

        with tempfile.TemporaryDirectory() as tmp:
            staged = Path(tmp) / "staged.jsonl"
            # 4. every vector, before anything is deleted
            await progress({"phase": "stage", "documents_in_export": len(expected), "to_remove": len(copies)})
            await self._stage(key, manifest, expected, urls, status, staged)

            # 5. wipe the collection, and only the collection
            await progress({"phase": "wipe", "to_remove": len(copies)})
            wiped = await self._wipe(key, copies, status)

            # 6. write the whole set under fresh ids
            await progress({"phase": "write", "changed": len(expected), "wiped": status["wiped"], "indexed": 0})
            batch = _Batch(self.s)

            async def flush() -> None:
                if batch.docs:
                    ok, bad = await self._insert(key, batch.take(), wiped)
                    status["indexed"] += ok
                    status["failed"] += bad
                    await progress({"indexed": status["indexed"], "failed": status["failed"]})

            for doc in _read_jsonl(staged):
                if batch.add(doc):
                    await flush()
            await flush()

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
        if not expected:
            raise PublishRefused("empty_export", f"the export of test run {run_id} holds no documents — refusing to empty the collection")
        assert_owned(key, expected.keys(), "export_ids")
        return manifest, expected, urls

    # ── staging ────────────────────────────────────────────────────────

    async def _stage(self, key: str, manifest: dict[str, Any], expected: dict[str, str], urls: dict[str, str],
                     status: dict[str, Any], out: Path) -> None:
        """Write a normalized, vectorized copy of every expected document to `out`; refuse — with
        nothing deleted — when any has no vectors at its current version anywhere."""
        by_url = {u: i for i, u in urls.items()}
        needed = set(expected)

        with out.open("w", encoding="utf-8") as fh:
            def accept(rec: dict[str, Any], source: str) -> None:
                i = by_url.get(rec.get("url"))
                if i in needed and rec.get("version") == expected[i] and has_vectors(rec):
                    needed.discard(i)
                    status[source] += 1
                    fh.write(json.dumps(self._normalize(rec, manifest, i), ensure_ascii=False) + "\n")

            async for rec in self._vectorized_records(key):
                accept(rec, "from_vectorized")
                if not needed:
                    break

            for source, client in (("from_test_index", self.test), ("from_prod_index", self.prod)):
                if not needed or client is None:
                    continue
                try:
                    for chunk in _chunks(sorted(needed), _FETCH_CHUNK):
                        for src in await self._call(self._fetch_docs, client, key, chunk, [urls[i] for i in chunk]):
                            if src.get("collection_key") == key:
                                accept(src, source)
                except Exception as e:  # noqa: BLE001 - the rest is reported as missing
                    log.warning("[%s] %s vector lookup failed: %s", key, source, e)
                    status[f"{source}_error"] = f"{type(e).__name__}: {e}"[:300]

        status["missing"] = len(needed)
        status["missing_urls"] = sorted(urls[i] for i in needed)[:_MAX_REPORTED]
        if needed:
            raise PublishRefused(
                "vectors_missing",
                f"{len(needed)} document(s) have no vectors at their current version in S3, the test index or prod "
                f"(e.g. {', '.join(status['missing_urls'][:3])}) — re-index to test first; nothing was deleted",
            )

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

    def _fetch_docs(self, client, key: str, ids: list[str], urls: list[str]) -> list[dict[str, Any]]:
        """Whole documents (embeddings included) of the collection, by fresh id or by url, so a copy
        stored under an older id scheme is found too."""
        r = client.search(index=self.index, body={
            "size": len(ids) * 5,
            "query": {"bool": {"filter": [collection_filter(key), {"bool": {
                "should": [{"terms": {"id": ids}}, {"terms": {"url": urls}}], "minimum_should_match": 1}}]}},
        })
        return [h.get("_source") or {} for h in r["hits"]["hits"]]

    def _normalize(self, rec: dict[str, Any], manifest: dict[str, Any], fresh_id: str) -> dict[str, Any]:
        doc = {f: rec.get(f) for f in DOC_FIELDS if f in rec}
        # always the freshly minted id, never whatever id the source copy carried
        doc["id"] = fresh_id
        # in prod "modified" is when the document went live, never the test run's stamp on the vectors
        doc["modified_date"] = self.published_at
        # non-content fields follow the validated export, not whenever the vectors were made
        doc["collection_key"] = manifest["collection_key"]
        doc["collection_name"] = manifest.get("collection_name")
        doc["public_visibility"] = True
        doc["is_metadata_viewer"] = False
        doc["executive_order_filter"] = False
        return doc

    # ── wipe ───────────────────────────────────────────────────────────

    async def _wipe(self, key: str, copies: dict[str, str], status: dict[str, Any]) -> set[str]:
        """Delete every document of the collection by explicit AOSS _id, then re-scan and re-count
        until the collection reads empty — catching copies a page boundary skipped, the index showed
        late, or a concurrent writer added. Documents that are deleted but still visible (AOSS lag) do
        not hold the write back: once the scan accounts for everything the count reports and all of it
        is already deleted, the wipe is done. Returns the deleted _ids. Raises (nothing written yet)
        when a delete keeps failing, or documents never scanned or deleted stay visible past the timeout."""
        wiped: set[str] = set()
        rounds = max(1, int(self.s.publish_wipe_settle_timeout_s // max(self.s.publish_wipe_poll_s, 0.001))) + 1
        for attempt in range(1, rounds + 1):
            targets = sorted(set(copies) - wiped)
            if targets:
                failed = await self._delete_copies(copies, targets)
                wiped.update(a for a in targets if a not in failed)
                status["wiped"] = len(wiped)
                if failed:
                    status["wipe_failed"] = len(failed)
                    raise PublishRefused(
                        "wipe_incomplete",
                        f"{len(failed)} of the collection's prod documents could not be deleted after "
                        f"{_BULK_ATTEMPTS} attempts ({len(wiped)} were) — nothing was written; publish again",
                    )
            n = await self._call(count_collection, self.prod, self.index, key)
            if n == 0:
                return wiped
            copies = await self._call(scan_collection_copies, self.prod, self.index, key)
            if not set(copies) - wiped and len(copies) >= n:
                log.info("[%s] %d deleted document(s) still visible in prod (index lag) — writing", key, n)
                status["wipe_lagging"] = n
                return wiped
            if attempt == rounds:
                break
            await asyncio.sleep(self.s.publish_wipe_poll_s)
        remaining = await self._call(count_collection, self.prod, self.index, key)
        raise PublishRefused(
            "wipe_incomplete",
            f"'{key}' still shows {remaining} document(s) in prod {self.s.publish_wipe_settle_timeout_s:.0f}s after "
            f"deleting {len(wiped)} — nothing was written; publish again",
        )

    async def _delete_copies(self, verified: dict[str, str], aids: list[str]) -> set[str]:
        """Delete these AOSS _ids — each must come from a verified scan of this collection — with
        retries; returns the ones still failing. Not found counts as deleted."""
        stray = [a for a in aids if a not in verified]
        if stray:
            raise PublishRefused("foreign_documents_in_scan",
                                 f"delete targets {stray[:5]!r} did not come from the collection scan — refusing")
        failed: set[str] = set()
        for part in _chunks(aids, self.s.publish_bulk_docs):
            pending = list(part)
            for attempt in range(1, _BULK_ATTEMPTS + 1):
                try:
                    bad = await self._bulk([{"delete": {"_index": self.index, "_id": a}} for a in pending], pending,
                                           not_found_ok=True)
                except Exception as e:  # noqa: BLE001 - transport error: retry the whole part
                    log.warning("bulk delete attempt %d/%d failed: %s", attempt, _BULK_ATTEMPTS, e)
                    bad = set(pending)
                pending = [a for a in pending if a in bad]
                if not pending:
                    break
                if attempt < _BULK_ATTEMPTS:
                    await asyncio.sleep(2 ** attempt)
            failed.update(pending)
        return failed

    # ── write ──────────────────────────────────────────────────────────

    def _aoss_ids(self, key: str, ids: list[str], exclude: set[str]) -> dict[str, list[str]]:
        """Every live copy (AOSS _id) of each business id inside the collection, minus `exclude`
        (copies the wipe removed that the index may still show)."""
        r = self.prod.search(index=self.index, body={"size": min(len(ids) * 5, _MAX_WINDOW), "_source": ["id"],
                                                     "query": {"bool": {"filter": [collection_filter(key),
                                                                                   {"terms": {"id": ids}}]}}})
        out: dict[str, list[str]] = {}
        for h in r["hits"]["hits"]:
            i = (h.get("_source") or {}).get("id")
            if i and h["_id"] not in exclude:
                out.setdefault(i, []).append(h["_id"])
        return out

    async def _bulk(self, lines: list[dict[str, Any]], keys: list[str], *, not_found_ok: bool = False) -> set[str]:
        """Send one bulk request; returns the keys (one per action) whose item failed."""
        body = "\n".join(json.dumps(x, ensure_ascii=False) for x in lines) + "\n"
        r = await self._call(self.prod.bulk, body=body)
        failed: set[str] = set()
        for k, item in zip(keys, r.get("items") or [], strict=False):
            res = next(iter(item.values()), {})
            code = int(res.get("status", 500))
            if not_found_ok and code == 404 and not res.get("error"):
                continue
            if res.get("error") or code >= 300:
                failed.add(k)
                log.warning("bulk item failed for %s: %s", k, str(res.get("error"))[:300])
        if len(r.get("items") or []) < len(keys):
            failed.update(keys[len(r.get("items") or []):])
        return failed

    async def _insert(self, key: str, docs: list[dict[str, Any]], wiped: set[str]) -> tuple[int, int]:
        """Write fresh documents into the wiped collection. The first attempt only indexes; a retry
        first looks the failed ids up — a lost response may hide a successful insert — and updates
        what it finds instead of indexing a duplicate, ignoring copies the wipe removed."""
        assert_owned(key, [d["id"] for d in docs], "write_batch")
        pending = {d["id"]: d for d in docs}
        for attempt in range(1, _BULK_ATTEMPTS + 1):
            try:
                existing = {} if attempt == 1 else await self._call(self._aoss_ids, key, list(pending), wiped)
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
                log.warning("bulk write attempt %d/%d failed: %s", attempt, _BULK_ATTEMPTS, e)
                failed = set(pending)
            pending = {i: d for i, d in pending.items() if i in failed}
            if not pending:
                break
            if attempt < _BULK_ATTEMPTS:
                await asyncio.sleep(2 ** attempt)
        return len(docs) - len(pending), len(pending)


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
