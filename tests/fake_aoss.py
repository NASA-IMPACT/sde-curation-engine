"""In-memory stand-in for an OpenSearch Serverless index: just the query shapes the publisher and
validation send (term/terms/prefix/exists/bool, search_after on id, the probe/dups aggregations, bulk).
`visibility_lag` simulates eventual consistency: a deleted document keeps showing in search/count for
that many count() calls (deleting it again answers 404)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any


class FakeAoss:
    def __init__(self, *, exists: bool = True):
        self.store: dict[str, dict[str, Any]] = {}  # AOSS _id → _source
        self.fail_ids: set[str] = set()  # business ids whose bulk items always fail
        self.bulk_calls: list[list[dict[str, Any]]] = []
        self.visibility_lag = 0
        self.lose_response_ids: set[str] = set()  # business ids whose first insert lands but answers 503
        self.ghosts: dict[str, list] = {}  # AOSS _id → [_source, count() calls left visible]
        self._n = 0
        self.indices = SimpleNamespace(exists=lambda index: exists)

    def add(self, src: dict[str, Any]) -> str:
        self._n += 1
        aid = f"aoss-{self._n}"
        self.store[aid] = dict(src)
        return aid

    def by_id(self, business_id: str) -> list[dict[str, Any]]:
        return [s for s in self.store.values() if s.get("id") == business_id]

    # ── queries ────────────────────────────────────────────────────────

    def _match(self, src: dict[str, Any], q: dict[str, Any] | None) -> bool:
        if not q or "match_all" in q:
            return True
        if "bool" in q:
            b = q["bool"]
            must = [*_list(b.get("filter")), *_list(b.get("must"))]
            should = _list(b.get("should"))
            return (all(self._match(src, x) for x in must)
                    and not any(self._match(src, x) for x in _list(b.get("must_not")))
                    and (not should or any(self._match(src, x) for x in should)))
        (kind, spec), = q.items()
        if kind == "exists":
            return src.get(spec["field"]) is not None
        (field, value), = spec.items()
        if kind == "term":
            return src.get(field) == value
        if kind == "terms":
            return src.get(field) in value
        if kind == "prefix":
            return isinstance(src.get(field), str) and src[field].startswith(value)
        raise NotImplementedError(q)

    def search(self, index: str, body: dict[str, Any]) -> dict[str, Any]:
        hits = sorted(((a, s) for a, s in self._visible() if self._match(s, body.get("query"))),
                      key=lambda x: str(x[1].get("id")))
        out: dict[str, Any] = {}
        aggs = body.get("aggs") or {}
        if "keys" in aggs:
            keys: dict[str, int] = {}
            for _, s in hits:
                if s.get("collection_key") is not None:
                    keys[s["collection_key"]] = keys.get(s["collection_key"], 0) + 1
            out["aggregations"] = {"keys": {"buckets": [{"key": k, "doc_count": n} for k, n in keys.items()]},
                                   "missing_key": {"doc_count": sum(1 for _, s in hits if s.get("collection_key") is None)}}
        if "dups" in aggs:
            ids: dict[str, int] = {}
            for _, s in hits:
                ids[s.get("id")] = ids.get(s.get("id"), 0) + 1
            out["aggregations"] = {"dups": {"buckets": [{"key": k, "doc_count": n} for k, n in ids.items() if n >= 2]}}
        if body.get("search_after"):
            hits = [h for h in hits if str(h[1].get("id")) > body["search_after"][0]]
        hits = hits[: body.get("size", 10)]
        fields = body.get("_source")
        out["hits"] = {"hits": [
            {"_id": a, "_source": {k: v for k, v in s.items() if fields is None or k in fields}, "sort": [s.get("id")]}
            for a, s in hits
        ]}
        return out

    def _visible(self):
        # lagging deletes first: with equal sort values they hide live copies behind a page boundary
        return [*((a, g[0]) for a, g in self.ghosts.items()), *self.store.items()]

    def count(self, index: str, body: dict[str, Any]) -> dict[str, int]:
        n = sum(1 for _, s in self._visible() if self._match(s, body.get("query")))
        for a in list(self.ghosts):
            self.ghosts[a][1] -= 1
            if self.ghosts[a][1] <= 0:
                del self.ghosts[a]
        return {"count": n}

    def bulk(self, body: str) -> dict[str, Any]:
        lines = [json.loads(x) for x in body.splitlines() if x.strip()]
        self.bulk_calls.append(lines)
        items = []
        pairs, it = [], iter(lines)
        for action in it:  # a delete action carries no payload line
            pairs.append((action, {} if "delete" in action else next(it)))
        for action, payload in pairs:
            (op, meta), = action.items()
            business_id = payload.get("doc", payload).get("id") or self.store.get(meta.get("_id"), {}).get("id")
            if business_id in self.fail_ids:
                items.append({op: {"status": 429, "error": {"type": "too_many_requests"}}})
                continue
            if op == "delete":
                gone = self.store.pop(meta["_id"], None)
                found = gone is not None
                if found and self.visibility_lag:
                    self.ghosts[meta["_id"]] = [gone, self.visibility_lag]
                items.append({op: {"_id": meta["_id"], "status": 200 if found else 404}})
            elif op == "update":
                if meta["_id"] not in self.store:
                    items.append({op: {"_id": meta["_id"], "status": 404, "error": {"type": "document_missing_exception"}}})
                    continue
                self.store[meta["_id"]].update(payload["doc"])
                items.append({op: {"_id": meta["_id"], "status": 200}})
            elif business_id in self.lose_response_ids:
                self.lose_response_ids.discard(business_id)
                self.add(payload)
                items.append({op: {"status": 503, "error": {"type": "response_lost"}}})
            else:
                items.append({op: {"_id": self.add(payload), "status": 201}})
        return {"errors": any("error" in next(iter(i.values())) for i in items), "items": items}


def _list(x) -> list:
    return [] if x is None else x if isinstance(x, list) else [x]
