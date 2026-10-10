"""An in-memory stand-in for `sde_curation.db.Database`, for unit tests of `sde_curation/jobs.py`
(JobManager) and `sde_curation/curation.py` (CurationService).

It implements exactly the Database methods those two modules call (plus `insert_collection`, the
one way to give a test a collection to work on), with the same names, signatures, defaults,
async-ness and return types, and the semantics of the SQL in db.py: the same filters, the same
ORDER BY, the same NULL handling, the same counters on the collection row (dump/delta/curated
counts, `excluded_count` unknown = None), canonical keys on dump rows, curated rows and exact-URL
rules, ids handed out in increasing order per table (an INSERT that hits a unique constraint uses
up an id, as a PostgreSQL identity column does), and timestamps from `models.utcnow`. Any other
Database method is absent on purpose: a unit test that reaches for one fails with AttributeError
instead of passing against behaviour nobody checked. tests/integration/test_db_contract.py runs
the same scenarios against this fake and the real Database and asserts the same results.

Everything is plain dicts in memory. Rows are stored as dicts of column values (enums stored as
their text, JSON columns stored as their JSON round trip) and every read builds new objects from
copies, so a caller can never change the store by changing what it was handed. Each method applies
its changes only once it has worked out all of them, so a method that raises leaves the store as it
was, like a rolled-back transaction.

Simplifications against PostgreSQL (none of them visible to the unit tests' callers):

- Text order is Python code-point order. `ORDER BY url` (and the duplicate-title key order) follows
  the database collation in PostgreSQL. The test database (postgres:17-alpine, en_US.utf8 on musl)
  sorts by code point too; a glibc/ICU en_US database (RDS) can order punctuation and case
  differently, which no unit test may depend on. Lists the SQL returns with no ORDER BY come back in insertion
  order here; PostgreSQL promises no order for them, so a caller must not depend on one.
- `lower()` and ILIKE case folding are Python's `str.lower()`; `\\s` in the duplicate-title key is
  the six ASCII whitespace characters, `btrim` trims spaces only (both as PostgreSQL does them).
  Non-ASCII case folding and Unicode spaces may differ from a given database's locale.
- No read-scope caching: `_coalesced` / `_stored` page-count caching and `touch()` / `changed()` are
  performance details of page views (the work scope a job runs in always reads the tables), so the
  fake always counts from its tables. `recycle_connections` is a no-op.
- Constraints: the primary keys and unique keys the methods rely on are enforced (ON CONFLICT
  behaviour, ConflictError on a duplicate rule, UniqueViolation on a duplicate index run id), and
  inserting rows for a collection that does not exist raises ForeignKeyViolation, as the real
  foreign keys do. A batch that would make PostgreSQL fail with "ON CONFLICT DO UPDATE command
  cannot affect row a second time" (the same URL twice in one replace_deltas / replace_curated
  call) is not detected: the last row wins here.
- Concurrency: one asyncio process, no row locks. Each method runs without awaiting in the middle
  (except between the chunks of the two iterators, which re-read the tables per chunk as the real
  keyset pagination does), so methods never interleave.
"""

from __future__ import annotations

import copy
import json
import logging
import re
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from datetime import datetime
from enum import Enum
from typing import Any

from psycopg import errors as pg_errors

from sde_curation.db import ConflictError, Database
from sde_curation.engine.patterns import is_exact
from sde_curation.engine.text import content_hash
from sde_curation.engine.urls import canonical_key, duplicate_docs
from sde_curation.models import (
    Collection,
    CuratedUrl,
    CurationStage,
    DeltaUrl,
    DumpFailure,
    DumpUrl,
    IndexRun,
    JobRun,
    JobState,
    Pattern,
    PatternType,
    Rule,
    RuleSource,
    Status,
    check_transition,
    utcnow,
)

_DELTA_COLS: tuple[str, ...] = Database._DELTA_COLS
_AI_COLS: frozenset[str] = Database._AI_COLS
_DELTA_DATA = tuple(c for c in _DELTA_COLS if c not in ("collection_id", "url") and c not in _AI_COLS)
_CURATED_COLS = ("collection_id", "url", "scraped_title", "title", "division", "document_type", "excluded",
                 "content_hash", "edited_by", "crawl_failure")
_INSERTED_COLLECTION_COLS = ("collection_id", "name", "seed_url", "division", "document_type", "connector",
                             "max_pages", "status", "needs_recuration", "created_at", "updated_at", "dump_count",
                             "delta_count", "curated_count", "created_by")
# The collection columns INSERT INTO collections leaves to their defaults.
_COLLECTION_DEFAULTS: dict[str, Any] = {
    "curation_stage": None, "recuration_reason": None, "last_scraped_at": None, "last_crawl_capped": False,
    "last_run_id": None, "curated_by": None, "index_key": None, "index_name": None, "curated_rows": 0,
    "excluded_count": None, "review_round": False, "curated_changed_at": None, "deltas_current": True,
}
_INDEX_RUN_COLS = ("run_id", "collection_id", "target", "state", "exported", "external_ref", "status",
                   "validation", "validated_by", "error", "started_at", "finished_at", "started_by")
log = logging.getLogger(__name__)
_PG_SPACE = re.compile(r"[ \t\n\r\f\v]+")  # PostgreSQL's \s ([[:space:]]) on ASCII text


def _t(v: Any) -> Any:
    """A value as the database stores it: an enum as its text."""
    return v.value if isinstance(v, Enum) else v


def _jsonb(v: Any) -> Any:
    """What a jsonb column gives back: the JSON round trip (tuples become lists, keys strings)."""
    return None if v is None else json.loads(json.dumps(v))


def _btrim(s: str | None) -> str | None:
    """PostgreSQL btrim(text): spaces only."""
    return None if s is None else s.strip(" ")


def _nullif_blank(s: str | None) -> str | None:
    """NULLIF(btrim(s), '')."""
    t = _btrim(s)
    return None if t == "" else t


def _coalesce(*vals: Any) -> Any:
    return next((v for v in vals if v is not None), None)


def _like(pattern: str, *, ci: bool = False) -> re.Pattern[str]:
    """A SQL LIKE pattern (default escape `\\`) as an anchored regex; `ci` = ILIKE."""
    out, i = [], 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\" and i + 1 < len(pattern):
            out.append(re.escape(pattern[i + 1]))
            i += 2
            continue
        out.append(".*" if ch == "%" else "." if ch == "_" else re.escape(ch))
        i += 1
    return re.compile("".join(out), re.DOTALL | (re.IGNORECASE if ci else 0))


def _ilike(value: str | None, q: str) -> bool:
    return value is not None and _like(f"%{q}%", ci=True).fullmatch(value) is not None


def _no_division(v: str | None) -> bool:
    return v is None or v == "General"


def _no_title(title: str | None, scraped: str | None) -> bool:
    return (_btrim(_coalesce(_nullif_blank(title), scraped, "")) or "") == ""


async def _aiter(rows: Iterable[Any] | AsyncIterable[Any]) -> AsyncIterator[Any]:
    if isinstance(rows, AsyncIterable):
        async for r in rows:
            yield r
    else:
        for r in rows:
            yield r


class FakeDatabase:
    """In-memory Database for unit tests (see the module docstring for what it covers)."""

    def __init__(self) -> None:
        self.on_status_change = None  # same hook as Database.on_status_change
        self._collections: dict[str, dict[str, Any]] = {}
        self._status_history: list[dict[str, Any]] = []
        self._page_text: dict[tuple[str, str], str] = {}
        self._dump: dict[str, dict[str, dict[str, Any]]] = {}
        self._dump_failures: dict[str, dict[str, dict[str, Any]]] = {}
        self._deltas: dict[str, dict[str, dict[str, Any]]] = {}
        self._curated: dict[str, dict[str, dict[str, Any]]] = {}
        self._patterns: dict[int, dict[str, Any]] = {}
        self._effects: dict[tuple[int, str, str], str] = {}  # (pattern_id, url, field) -> collection_id
        self._suggestions: dict[int, dict[str, Any]] = {}
        self._index_runs: dict[str, dict[str, Any]] = {}
        self._jobs: dict[int, dict[str, Any]] = {}
        self._audit: list[dict[str, Any]] = []
        self._ids: dict[str, int] = {}

    # ── internals ──────────────────────────────────────────────────────

    def _next_id(self, table: str) -> int:
        self._ids[table] = self._ids.get(table, 0) + 1
        return self._ids[table]

    def _require(self, collection_id: str) -> None:
        if collection_id not in self._collections:
            raise pg_errors.ForeignKeyViolation(f"collection {collection_id!r} does not exist")

    def _coll_update(self, collection_id: str, **values: Any) -> None:
        row = self._collections.get(collection_id)
        if row is not None:
            row.update(values)

    def _gc_page_text(self, collection_id: str) -> None:
        held = {r["content_hash"] for r in self._dump.get(collection_id, {}).values()}
        held |= {r["content_hash"] for r in self._curated.get(collection_id, {}).values()}
        for key in [k for k in self._page_text if k[0] == collection_id and k[1] not in held]:
            del self._page_text[key]

    def _recount_curated(self, collection_id: str, *, changed: bool) -> int:
        row = self._collections.get(collection_id)
        if row is None:
            return 0
        rows = self._curated.get(collection_id, {}).values()
        now = utcnow()
        row["curated_count"] = sum(1 for r in rows if not r["excluded"])
        row["curated_rows"] = len(rows)
        row["updated_at"] = now
        if changed:
            row["curated_changed_at"] = now
        return row["curated_count"]

    def _forget_excluded_count(self, collection_id: str, deleted_types: list[str]) -> None:
        if {"exclude", "include"} & set(deleted_types):
            self._coll_update(collection_id, excluded_count=None)

    def _delete_pattern_rows(self, ids: list[int]) -> list[str]:
        """Delete these rules and their effects (ON DELETE CASCADE); returns their types."""
        types = []
        for pid in ids:
            types.append(self._patterns.pop(pid)["type"])
        gone = set(ids)
        for key in [k for k in self._effects if k[0] in gone]:
            del self._effects[key]
        return types

    def _delta_row(self, d: DeltaUrl) -> dict[str, Any]:
        return {**{c: _t(getattr(d, c)) for c in _DELTA_COLS}, "ai_job": None}

    def _upsert_deltas(self, collection_id: str, deltas: list[DeltaUrl]) -> None:
        """INSERT … ON CONFLICT DO UPDATE of the non-AI columns (a new row takes every column)."""
        for d in deltas:
            table = self._deltas.setdefault(d.collection_id, {})
            row = self._delta_row(d)
            old = table.get(d.url)
            if old is None:
                table[d.url] = row
            else:
                old.update({c: row[c] for c in _DELTA_DATA})

    def _check_effects(self, effects: Iterable[tuple[int, str, str]]) -> None:
        for pid, _u, _f in effects:
            if pid not in self._patterns:
                raise pg_errors.ForeignKeyViolation(f"pattern {pid} does not exist")

    def _projected(self, collection_id: str, *, pending: bool) -> list[dict[str, Any]]:
        """Database._projected_titles: the title + document type each included page would carry."""
        deltas = self._deltas.get(collection_id, {})
        renamed = {d["renamed_from"] for d in deltas.values() if d["renamed_from"] is not None}
        out = []
        for d in deltas.values():
            if d["kind"] == "deleted" or d["excluded"]:
                continue
            if pending:
                title = _coalesce(d["title_ai"], _nullif_blank(d["title"]), d["scraped_title"])
                doc = _coalesce(d["document_type_ai"], d["document_type"])
                flags = (d["title_ai"] is not None, d["title_ai_before"])
            else:
                title = _coalesce(_nullif_blank(d["title"]), d["scraped_title"])
                doc = d["document_type"]
                flags = (False, None)
            out.append({"url": d["url"], "delta": True, "pending_ai": flags[0], "shared_before": flags[1],
                        "title": title, "document_type": doc})
        for c in self._curated.get(collection_id, {}).values():
            if c["excluded"] or c["url"] in deltas or c["url"] in renamed:
                continue
            out.append({"url": c["url"], "delta": False, "pending_ai": False, "shared_before": None,
                        "title": _coalesce(_nullif_blank(c["title"]), c["scraped_title"]),
                        "document_type": c["document_type"]})
        rows = []
        for r in out:
            if (_btrim(_coalesce(r["title"], "")) or "") == "":
                continue
            r["k"] = _PG_SPACE.sub(" ", _btrim(r["title"])).lower() + chr(31) + (r["document_type"] or "")
            rows.append(r)
        return rows

    def _duplicates(self, collection_id: str, *, pending: bool) -> list[dict[str, Any]]:
        rows = self._projected(collection_id, pending=pending)
        n: dict[str, int] = {}
        for r in rows:
            n[r["k"]] = n.get(r["k"], 0) + 1
        return [{**r, "n": n[r["k"]]} for r in rows if n[r["k"]] > 1]

    def _llm_rows(self, collection_id: str, *, only_missing: bool,
                  skip_job: int | None = None) -> list[tuple[dict, dict | None]]:
        """(delta row, its dump row or None) for Database._LLM_WHERE (+ _LLM_MISSING)."""
        dump = self._dump.get(collection_id, {})
        out = []
        for d in self._deltas.get(collection_id, {}).values():
            if d["kind"] == "deleted" or d["excluded"]:
                continue
            if skip_job is not None and d["ai_job"] == skip_job:
                continue
            u = dump.get(d["url"])
            if only_missing and not self._llm_missing(d, u):
                continue
            out.append((d, u))
        return out

    @staticmethod
    def _llm_missing(d: dict[str, Any], u: dict[str, Any] | None) -> bool:
        u_hash = u["content_hash"] if u is not None else None
        return bool(
            (d["title_ai"] is None and d["division_ai"] is None and d["document_type_ai"] is None)
            or d["ai_error"] is not None
            or (d["title_ai"] is None and d["title_ai_conf"] is not None and d["title"] is None)
            or (d["division_ai"] is None and d["division_ai_conf"] is not None and d["division"] is None)
            or (d["document_type_ai"] is None and d["document_type_ai_conf"] is not None
                and d["document_type"] is None)
            or (d["division_skipped"] and _no_division(d["division"]))
            # content_changed AND (ai hash IS NULL OR ai hash != dump hash): != against NULL is NULL
            or (d["content_changed"] and (d["ai_content_hash"] is None
                                          or (u_hash is not None and d["ai_content_hash"] != u_hash)))
        )

    def _text_of(self, collection_id: str, u: dict[str, Any] | None) -> str | None:
        if u is None or u["content_hash"] is None:
            return None
        return self._page_text.get((collection_id, u["content_hash"]))

    @staticmethod
    def _job(row: dict[str, Any]) -> JobRun:
        d = copy.deepcopy(row)
        d["progress"] = d["progress"] or {}
        return JobRun(**d)

    @staticmethod
    def _pattern(row: dict[str, Any]) -> Pattern:
        return Pattern(**{k: v for k, v in copy.deepcopy(row).items() if k != "canonical_key"})

    def _coll_patterns(self, collection_id: str) -> list[dict[str, Any]]:
        """The collection's rules in id order."""
        return [p for pid, p in sorted(self._patterns.items()) if p["collection_id"] == collection_id]

    # ── connections ────────────────────────────────────────────────────

    async def recycle_connections(self) -> None:
        """No pooled connections to replace."""

    # ── collections ────────────────────────────────────────────────────

    async def insert_collection(self, c: Collection) -> Collection:
        if c.collection_id in self._collections:
            raise pg_errors.UniqueViolation(f"collection {c.collection_id!r} exists")
        row = {k: _t(getattr(c, k)) for k in _INSERTED_COLLECTION_COLS}
        self._collections[c.collection_id] = {**row, **_COLLECTION_DEFAULTS}
        self._status_history.append({
            "id": self._next_id("status_history"), "collection_id": c.collection_id, "old_status": None,
            "new_status": _t(c.status), "note": "created", "at": utcnow(), "actor": c.created_by,
        })
        return c

    async def get_collection(self, collection_id: str) -> Collection | None:
        row = self._collections.get(collection_id)
        return Collection(**copy.deepcopy(row)) if row else None

    async def set_status(
        self, collection_id: str, new: Status, note: str | None = None, *, force: bool = False,
        actor: str | None = None,
    ) -> Collection:
        row = self._collections.get(collection_id)
        if row is None:
            raise KeyError(collection_id)
        c = Collection(**copy.deepcopy(row))
        if not force:
            check_transition(c.status, new)
        now = utcnow()
        if new is Status.CURATING:
            stage = c.curation_stage if c.status is Status.CURATING and c.curation_stage else CurationStage.EXCLUSIONS
        else:
            stage = None
        row.update(status=_t(new), curation_stage=_t(stage), updated_at=now)
        self._status_history.append({
            "id": self._next_id("status_history"), "collection_id": collection_id, "old_status": _t(c.status),
            "new_status": _t(new), "note": note, "at": now, "actor": actor,
        })
        if self.on_status_change:
            try:
                await self.on_status_change(collection_id, c.status, new, note, actor)
            except Exception as e:  # noqa: BLE001 - as Database: a hook never breaks a transition
                log.warning("status hook failed: %s", e)
        c.status, c.curation_stage, c.updated_at = new, stage, now
        return c

    async def set_last_scraped(self, collection_id: str, at: datetime, *, capped: bool = False) -> None:
        self._coll_update(collection_id, last_scraped_at=at, last_crawl_capped=capped)

    async def set_index_key(self, collection_id: str, index_key: str, index_name: str | None) -> None:
        self._coll_update(collection_id, index_key=index_key, index_name=index_name, updated_at=utcnow())

    async def set_flag(self, collection_id: str, needs_recuration: bool, reason: str | None = None) -> None:
        self._coll_update(collection_id, needs_recuration=needs_recuration,
                          recuration_reason=(reason or None) if needs_recuration else None, updated_at=utcnow())

    async def set_review_round(self, collection_id: str, open_: bool) -> None:
        self._coll_update(collection_id, review_round=open_)

    # ── dump ───────────────────────────────────────────────────────────

    async def replace_dump(
        self, collection_id: str, rows: Iterable[DumpUrl] | AsyncIterable[DumpUrl],
        failures: list[DumpFailure] | None = None, *, dedupe_spellings: bool = False,
    ) -> int:
        new_failures: dict[str, dict[str, Any]] = {}
        for f in failures or []:
            if f.url not in new_failures:
                new_failures[f.url] = {"collection_id": collection_id, "url": f.url, "reason": f.reason,
                                       "status": f.status, "detail": f.detail}
        staged: list[dict[str, Any]] = []
        seen: set[str] = set()
        async for r in _aiter(rows):
            if r.url in seen:
                continue
            seen.add(r.url)
            staged.append({"url": r.url, "final_url": r.final_url, "scraped_title": r.scraped_title,
                           "full_text": r.full_text, "content_type": r.content_type, "depth": r.depth,
                           "content_hash": r.content_hash or content_hash(r.full_text),
                           "canonical_key": canonical_key(r.url)})
        if staged or new_failures:
            self._require(collection_id)
        if dedupe_spellings:
            drop = duplicate_docs([{"url": s["url"], "final_url": s["final_url"]} for s in staged])
            staged = [s for i, s in enumerate(staged) if i not in drop]
        for s in staged:
            if s["content_hash"] is not None and s["full_text"] is not None:
                self._page_text.setdefault((collection_id, s["content_hash"]), s["full_text"])
        self._dump[collection_id] = {
            s["url"]: {"collection_id": collection_id, "url": s["url"], "scraped_title": s["scraped_title"],
                       "content_type": s["content_type"], "depth": s["depth"], "content_hash": s["content_hash"],
                       "canonical_key": s["canonical_key"]}
            for s in staged
        }
        self._dump_failures[collection_id] = new_failures
        self._gc_page_text(collection_id)
        n = len(self._dump[collection_id])
        self._coll_update(collection_id, dump_count=n, excluded_count=None, review_round=False, deltas_current=False,
                          updated_at=utcnow())
        return n

    async def load_dump(self, collection_id: str, *, keys: list[str] | None = None) -> list[DumpUrl]:
        wanted = None if keys is None else set(keys)
        return [DumpUrl(**{k: v for k, v in r.items() if k != "canonical_key"})
                for r in self._dump.get(collection_id, {}).values()
                if wanted is None or r["canonical_key"] in wanted]

    async def dump_urls(self, collection_id: str) -> list[str]:
        return list(self._dump.get(collection_id, {}))

    async def set_urls(self, collection_id: str, set_: str) -> list[str]:
        table = {"dump": self._dump, "delta": self._deltas, "curated": self._curated}[set_]
        return list(table.get(collection_id, {}))

    async def load_dump_failures(self, collection_id: str) -> dict[str, str]:
        return {u: f["reason"] for u, f in self._dump_failures.get(collection_id, {}).items()}

    async def dump_content_hashes(self, collection_id: str) -> dict[str, str | None]:
        return {u: r["content_hash"] for u, r in self._dump.get(collection_id, {}).items()}

    async def effect_counts(self, collection_id: str, ids: list[int] | None = None) -> dict[int, int]:
        wanted = None if ids is None else set(ids)
        out: dict[int, int] = {}
        for (pid, _u, _f), cid in self._effects.items():
            if cid == collection_id and (wanted is None or pid in wanted):
                out[pid] = out.get(pid, 0) + 1
        return out

    # ── deltas ─────────────────────────────────────────────────────────

    async def load_deltas(self, collection_id: str, *, urls: list[str] | None = None) -> list[DeltaUrl]:
        wanted = None if urls is None else set(urls)
        return [DeltaUrl(**copy.deepcopy(r)) for u, r in self._deltas.get(collection_id, {}).items()
                if wanted is None or u in wanted]

    async def count_deltas_by_kind(self, collection_id: str) -> dict[str, int]:
        rows = list(self._deltas.get(collection_id, {}).values())
        return {
            "total": len(rows),
            "new": sum(1 for r in rows if r["kind"] == "new"),
            "modified": sum(1 for r in rows if r["kind"] == "modified"),
            "deleted": sum(1 for r in rows if r["kind"] == "deleted"),
            "content_changed": sum(1 for r in rows if r["content_changed"]),
            "renamed": sum(1 for r in rows if r["renamed_from"] is not None),
        }

    async def replace_deltas(
        self, collection_id: str, deltas: list[DeltaUrl], effects: list[tuple[int, str, str]],
        *, keep_effects: bool = False, excluded_count: int | None = None, full: bool = False,
    ) -> None:
        if deltas:
            self._require(collection_id)
        if not keep_effects and effects:
            self._check_effects(effects)
        table = self._deltas.setdefault(collection_id, {})
        keep = {d.url for d in deltas}
        for url in [u for u in table if u not in keep]:
            del table[url]
        self._upsert_deltas(collection_id, deltas)
        if not keep_effects:
            wanted = set(effects)
            for key in [k for k, cid in self._effects.items()
                        if cid == collection_id and k not in wanted]:
                del self._effects[key]
            for pid, url, field in sorted(wanted):
                self._effects.setdefault((pid, url, field), collection_id)
            self._coll_update(collection_id, delta_count=len(deltas), updated_at=utcnow(),
                              excluded_count=excluded_count if excluded_count is not None
                              else (None if effects else 0),
                              deltas_current=self._collections.get(collection_id, {}).get("deltas_current", True) or full)
        else:
            self._coll_update(collection_id, delta_count=len(deltas), updated_at=utcnow())

    async def keyed(self, collection_id: str) -> bool:
        return (all(r["canonical_key"] is not None for r in self._dump.get(collection_id, {}).values())
                and all(r["canonical_key"] is not None for r in self._curated.get(collection_id, {}).values())
                and all(p["canonical_key"] is not None for p in self._coll_patterns(collection_id)
                        if "*" not in p["match"]))

    async def excluded_among(self, collection_id: str, urls: list[str]) -> int:
        if not urls:
            return 0
        wanted = set(urls)
        return sum(1 for (pid, url, field), cid in self._effects.items()
                   if cid == collection_id and field == "excluded" and url in wanted
                   and self._patterns[pid]["type"] == "exclude")

    async def replace_deltas_scoped(
        self, collection_id: str, urls: list[str], deltas: list[DeltaUrl], effects: list[tuple[int, str, str]],
        *, excluded_change: int,
    ) -> None:
        if deltas:
            self._require(collection_id)
        if effects:
            self._check_effects(effects)
        scope = set(urls)
        keep = {d.url for d in deltas}
        table = self._deltas.setdefault(collection_id, {})
        for url in [u for u in table if u in scope and u not in keep]:
            del table[url]
        self._upsert_deltas(collection_id, deltas)
        for key in [k for k, cid in self._effects.items() if cid == collection_id and k[1] in scope]:
            del self._effects[key]
        for pid, url, field in sorted(set(effects)):
            self._effects.setdefault((pid, url, field), collection_id)
        row = self._collections.get(collection_id)
        if row is not None:
            row["delta_count"] = len(table)
            row["excluded_count"] = None if row["excluded_count"] is None else row["excluded_count"] + excluded_change
            row["updated_at"] = utcnow()

    async def rule_match_counts(self, collection_id: str, set_: str, *, globs: list[tuple[int, str]],
                                exact: list[tuple[int, str]]) -> dict[int, int]:
        table = {"dump": self._dump, "delta": self._deltas, "curated": self._curated}[set_]
        rows = table.get(collection_id, {})
        out: dict[int, int] = {}
        for i, p in globs:
            rx = _like(p)
            out[i] = sum(1 for u in rows if rx.fullmatch(u))
        if exact:
            keys = {k for _, k in exact}
            per_key: dict[str, int] = {}
            if set_ == "delta":
                dump = self._dump.get(collection_id, {})
                curated = self._curated.get(collection_id, {})
                for u in rows:
                    if u in dump:
                        k = dump[u]["canonical_key"]
                    elif u in curated:
                        k = curated[u]["canonical_key"]
                    else:
                        continue
                    if k in keys:
                        per_key[k] = per_key.get(k, 0) + 1
            else:
                for r in rows.values():
                    if r["canonical_key"] in keys:
                        per_key[r["canonical_key"]] = per_key.get(r["canonical_key"], 0) + 1
            out.update({i: per_key.get(k, 0) for i, k in exact})
        return out

    async def exact_rule_ids(self, collection_id: str, types: list[str], keys: list[str]) -> list[int]:
        ts, ks = {_t(t) for t in types}, set(keys)
        return [p["id"] for p in self._coll_patterns(collection_id)
                if p["type"] in ts and p["canonical_key"] is not None and p["canonical_key"] in ks]

    async def delete_deltas(self, collection_id: str, urls: list[str]) -> int:
        if not urls:
            return 0
        table = self._deltas.get(collection_id, {})
        gone = [u for u in set(urls) if u in table]
        for u in gone:
            del table[u]
        self._coll_update(collection_id, delta_count=len(table), updated_at=utcnow())
        return len(gone)

    async def delete_effects(self, collection_id: str, urls: list[str]) -> None:
        if not urls:
            return
        wanted = set(urls)
        for key in [k for k, cid in self._effects.items() if cid == collection_id and k[1] in wanted]:
            del self._effects[key]

    async def set_delta_ai(self, collection_id: str, items: list[dict[str, Any]], *, job: int | None = None) -> int:
        if not items:
            return 0
        table = self._deltas.get(collection_id, {})
        for i in items:
            row = table.get(i["url"])
            if row is None:
                continue
            row.update(
                title_ai=_t(i.get("title")), division_ai=_t(i.get("division")),
                document_type_ai=_t(i.get("document_type")), title_ai_conf=_t(i.get("title_conf")),
                division_ai_conf=_t(i.get("division_conf")), document_type_ai_conf=_t(i.get("document_type_conf")),
                ai_model=i.get("model"), ai_content_hash=i.get("content_hash"), ai_error=None, ai_failures=0,
                title_ai_before=None, division_skipped=bool(i.get("division_skipped")), ai_job=job,
            )
        return len(items)

    async def set_delta_ai_errors(self, collection_id: str, items: list[tuple[str, str]], *,
                                  job: int | None = None) -> int:
        if not items:
            return 0
        table = self._deltas.get(collection_id, {})
        for url, err in items:
            row = table.get(url)
            if row is not None:
                row["ai_error"] = err[:1000]
                row["ai_failures"] += 1
                row["ai_job"] = job
        return len(items)

    async def set_delta_ai_titles(self, collection_id: str, items: list[dict[str, Any]]) -> int:
        if not items:
            return 0
        table = self._deltas.get(collection_id, {})
        for i in items:
            row = table.get(i["url"])
            if row is not None:
                row.update(title_ai=i["title"], title_ai_conf=_t(i.get("title_conf")), ai_model=i.get("model"),
                           title_ai_before=_coalesce(row["title_ai_before"], i.get("before")))
        return len(items)

    # ── curated ────────────────────────────────────────────────────────

    def _curated_model(self, row: dict[str, Any], *, with_text: bool) -> CuratedUrl:
        d = {c: row[c] for c in _CURATED_COLS}
        if with_text:
            h = row["content_hash"]
            d["full_text"] = None if h is None else self._page_text.get((row["collection_id"], h))
        return CuratedUrl(**d)

    async def load_curated(self, collection_id: str, *, with_text: bool = False,
                           keys: list[str] | None = None) -> list[CuratedUrl]:
        wanted = None if keys is None else set(keys)
        return [self._curated_model(r, with_text=with_text) for r in self._curated.get(collection_id, {}).values()
                if wanted is None or r["canonical_key"] in wanted]

    async def iter_curated_for_export(self, collection_id: str, chunk: int = 500) -> AsyncIterator[list[CuratedUrl]]:
        urls = sorted(u for u, r in self._curated.get(collection_id, {}).items() if not r["excluded"])
        for i in range(0, len(urls), chunk):
            table = self._curated.get(collection_id, {})
            yield [self._curated_model(table[u], with_text=True) for u in urls[i:i + chunk] if u in table]

    async def set_curated_edited_by(self, collection_id: str, items: list[tuple[str, str | None]]) -> None:
        table = self._curated.get(collection_id, {})
        for url, eb in items:
            if url in table:
                table[url]["edited_by"] = _t(eb)

    async def set_curated_excluded(self, collection_id: str, items: list[tuple[str, bool]]) -> None:
        if not items:
            return
        table = self._curated.get(collection_id, {})
        for url, excluded in items:
            if url in table:
                table[url]["excluded"] = excluded
        self._recount_curated(collection_id, changed=True)

    async def set_curated_crawl_failure(self, collection_id: str, items: list[tuple[str, str | None]]) -> None:
        table = self._curated.get(collection_id, {})
        for url, reason in items:
            if url in table:
                table[url]["crawl_failure"] = reason

    async def count_curated_unreachable(self, collection_id: str) -> int:
        return sum(1 for r in self._curated.get(collection_id, {}).values() if r["crawl_failure"] is not None)

    async def replace_curated(self, collection_id: str, rows: list[CuratedUrl], *, changed: bool = True) -> int:
        staged = []
        for r in rows:
            row = {c: _t(getattr(r, c)) for c in _CURATED_COLS}
            row["content_hash"] = r.content_hash or content_hash(r.full_text)
            row["canonical_key"] = canonical_key(r.url)
            staged.append((row, r.full_text))
        for row, _ in staged:
            self._require(row["collection_id"])
        for row, text in staged:
            if row["content_hash"] is not None and text is not None:
                self._page_text.setdefault((row["collection_id"], row["content_hash"]), text)
        keep = {row["url"] for row, _ in staged}
        table = self._curated.setdefault(collection_id, {})
        for url in [u for u in table if u not in keep]:
            del table[url]
        for row, _ in staged:
            target = self._curated.setdefault(row["collection_id"], {})
            if row["url"] in target:
                target[row["url"]].update(row)
            else:
                target[row["url"]] = row
        self._gc_page_text(collection_id)
        return self._recount_curated(collection_id, changed=changed)

    # ── index runs ─────────────────────────────────────────────────────

    async def insert_index_run(self, r: IndexRun) -> IndexRun:
        if r.run_id in self._index_runs:
            raise pg_errors.UniqueViolation(f"index run {r.run_id!r} exists")
        self._require(r.collection_id)
        row = {c: copy.deepcopy(getattr(r, c)) for c in _INDEX_RUN_COLS}
        row["status"] = _jsonb(r.status) if r.status else None
        row["validation"] = _jsonb(r.validation) if r.validation else None
        self._index_runs[r.run_id] = row
        self._coll_update(r.collection_id, last_run_id=r.run_id, updated_at=utcnow())
        return r

    async def update_index_run(self, r: IndexRun) -> None:
        row = self._index_runs.get(r.run_id)
        if row is None:
            return
        row.update(state=r.state, exported=r.exported, external_ref=r.external_ref,
                   status=_jsonb(r.status) if r.status else None,
                   validation=_jsonb(r.validation) if r.validation else None,
                   validated_by=r.validated_by, error=r.error, finished_at=r.finished_at)

    async def close_orphan_index_runs(self, *, keep: list[str]) -> int:
        kept, now, n = set(keep), utcnow(), 0
        for row in self._index_runs.values():
            if row["state"] == "running" and row["run_id"] not in kept:
                row.update(state="failed", error="engine restarted", finished_at=now)
                n += 1
        return n

    async def get_index_run(self, run_id: str) -> IndexRun | None:
        row = self._index_runs.get(run_id)
        return IndexRun(**copy.deepcopy(row)) if row else None

    async def last_index_run(self, collection_id: str, target: str | None = None) -> IndexRun | None:
        rows = [r for r in self._index_runs.values()
                if r["collection_id"] == collection_id and (not target or r["target"] == target)]
        if not rows:
            return None
        # ORDER BY started_at DESC LIMIT 1; a tie has no defined winner in SQL — the later insert here
        best = max(enumerate(rows), key=lambda ir: (ir[1]["started_at"], ir[0]))[1]
        return IndexRun(**copy.deepcopy(best))

    # ── LLM suggestions ────────────────────────────────────────────────

    async def clear_pending_pattern_suggestions(self, collection_id: str) -> None:
        for sid in [s for s, r in self._suggestions.items()
                    if r["collection_id"] == collection_id and r["state"] == "pending"]:
            del self._suggestions[sid]

    async def add_pattern_suggestions(self, collection_id: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        self._require(collection_id)
        now, added = utcnow(), 0
        for r in rows:
            sid = self._next_id("pattern_suggestions")  # used up on a conflict too
            t = _t(r["type"])
            if any(s["collection_id"] == collection_id and s["type"] == t and s["match"] == r["match"]
                   for s in self._suggestions.values()):
                continue
            self._suggestions[sid] = {
                "id": sid, "collection_id": collection_id, "type": t, "match": r["match"], "value": r.get("value"),
                "rationale": r.get("rationale"), "matches": r.get("matches", 0), "state": "pending",
                "source": _t(r.get("source", "llm")), "created_at": now, "decided_by": None, "accepted_as": None,
            }
            added += 1
        return added

    async def count_pending_pattern_suggestions(self, collection_id: str) -> int:
        return sum(1 for r in self._suggestions.values()
                   if r["collection_id"] == collection_id and r["state"] == "pending")

    async def pending_urls_for_patterns(self, collection_id: str) -> list[tuple[str, str | None]]:
        return [(d["url"], d["scraped_title"]) for d, _u in
                sorted(self._llm_rows(collection_id, only_missing=False), key=lambda x: x[0]["url"])]

    async def count_deltas_for_llm(self, collection_id: str, *, only_missing: bool = True,
                                   skip_job: int | None = None) -> int:
        return len(self._llm_rows(collection_id, only_missing=only_missing, skip_job=skip_job))

    async def iter_deltas_for_llm(
        self, collection_id: str, *, only_missing: bool = True, chunk: int = 200, skip_job: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        last = ""
        while True:
            rows = sorted((x for x in self._llm_rows(collection_id, only_missing=only_missing, skip_job=skip_job)
                           if x[0]["url"] > last), key=lambda x: x[0]["url"])[:chunk]
            if not rows:
                return
            for d, u in rows:
                yield {"url": d["url"], "title": d["scraped_title"], "text": self._text_of(collection_id, u),
                       "content_hash": u["content_hash"] if u is not None else None}
            last = rows[-1][0]["url"]

    async def urls_titled_by_job(self, collection_id: str, job: int) -> set[str]:
        return {d["url"] for d in self._deltas.get(collection_id, {}).values()
                if d["ai_job"] == job and d["title_ai"] is not None and d["ai_error"] is None}

    async def incomplete_counts(self, collection_id: str, urls: list[str] | None = None) -> dict[str, int]:
        dup = {r["url"] for r in self._duplicates(collection_id, pending=False) if r["delta"]}
        wanted = None if urls is None else set(urls)
        out = dict.fromkeys(("urls", "title", "division", "general", "document_type", "duplicate"), 0)
        for d in self._deltas.get(collection_id, {}).values():
            incomplete = (d["kind"] != "deleted" and not d["excluded"]
                          and (_no_title(d["title"], d["scraped_title"]) or _no_division(d["division"])
                               or d["document_type"] is None))
            if not (incomplete or d["url"] in dup) or (wanted is not None and d["url"] not in wanted):
                continue
            out["urls"] += 1
            out["title"] += _no_title(d["title"], d["scraped_title"])
            out["division"] += _no_division(d["division"])
            out["general"] += d["division"] == "General"
            out["document_type"] += d["document_type"] is None
            out["duplicate"] += d["url"] in dup
        return out

    async def title_keys(self, collection_id: str) -> dict[str, str]:
        return {r["url"]: r["k"] for r in self._projected(collection_id, pending=True)}

    async def text_sizes(self, collection_id: str, urls: list[str]) -> dict[str, int]:
        if not urls:
            return {}
        dump, wanted = self._dump.get(collection_id, {}), set(urls)
        return {u: len(self._text_of(collection_id, dump.get(u)) or "")
                for u in self._deltas.get(collection_id, {}) if u in wanted}

    async def docs_for_llm(self, collection_id: str, urls: list[str]) -> list[dict[str, Any]]:
        if not urls:
            return []
        dump, wanted = self._dump.get(collection_id, {}), set(urls)
        out = []
        for u in sorted(u for u in self._deltas.get(collection_id, {}) if u in wanted):
            d, dr = self._deltas[collection_id][u], dump.get(u)
            out.append({"url": u, "title": d["scraped_title"], "text": self._text_of(collection_id, dr),
                        "content_hash": dr["content_hash"] if dr is not None else None})
        return out

    async def duplicate_title_counts(self, collection_id: str) -> dict[str, int]:
        rows = self._duplicates(collection_id, pending=True)
        return {"urls": len(rows), "titles": len({r["k"] for r in rows}),
                "delta_urls": sum(1 for r in rows if r["delta"])}

    async def duplicate_title_groups(self, collection_id: str) -> list[dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for r in sorted(self._duplicates(collection_id, pending=True), key=lambda r: (r["k"], r["url"])):
            g = groups.setdefault(r["k"], {"title": r["title"], "document_type": r["document_type"], "members": []})
            g["members"].append({"url": r["url"], "delta": r["delta"], "pending_ai": r["pending_ai"],
                                 "before": r["shared_before"]})
        return list(groups.values())

    # ── patterns ───────────────────────────────────────────────────────

    def _pattern_row(self, p: Pattern, pid: int) -> dict[str, Any]:
        return {"id": pid, "collection_id": p.collection_id, "type": _t(p.type), "match": p.match,
                "value": p.value, "created_at": p.created_at, "created_by": p.created_by, "source": _t(p.source),
                "canonical_key": canonical_key(p.match) if is_exact(p.match) else None}

    def _rule_exists(self, collection_id: str, type_: str, match: str) -> bool:
        return any(p["collection_id"] == collection_id and p["type"] == type_ and p["match"] == match
                   for p in self._patterns.values())

    async def insert_pattern(self, p: Pattern) -> Pattern:
        self._require(p.collection_id)
        pid = self._next_id("patterns")  # used up on a conflict too, as an identity column's is
        if self._rule_exists(p.collection_id, _t(p.type), p.match):
            raise ConflictError(f"{p.type} rule {p.match!r} already exists")
        self._patterns[pid] = self._pattern_row(p, pid)
        p.id = pid
        return p

    async def insert_patterns(self, rows: list[Pattern]) -> int:
        if not rows:
            return 0
        for p in rows:
            self._require(p.collection_id)
        inserted = 0
        for p in rows:
            pid = self._next_id("patterns")
            if self._rule_exists(p.collection_id, _t(p.type), p.match):
                continue
            self._patterns[pid] = self._pattern_row(p, pid)
            inserted += 1
        return inserted

    async def delete_exact_patterns(self, collection_id: str, type_: str, matches: list[str]) -> int:
        if not matches:
            return 0
        wanted, t = set(matches), _t(type_)
        ids = [p["id"] for p in self._coll_patterns(collection_id) if p["type"] == t and p["match"] in wanted]
        self._delete_pattern_rows(ids)
        if ids:
            self._forget_excluded_count(collection_id, [t])
        return len(ids)

    async def load_rules(self, collection_id: str, *, keys: list[str] | None = None) -> list[Rule]:
        wanted = None if keys is None else set(keys)
        return [Rule(p["id"], PatternType(p["type"]), p["match"], p["value"], RuleSource(p["source"]))
                for p in self._coll_patterns(collection_id)
                if wanted is None or "*" in p["match"] or p["canonical_key"] in wanted]

    async def exact_pattern_matches(self, collection_id: str, types: list[str]) -> list[tuple[int, str, str]]:
        ts = {_t(t) for t in types}
        return [(p["id"], p["type"], p["match"]) for p in self._coll_patterns(collection_id)
                if p["type"] in ts and "*" not in p["match"]]

    async def delete_patterns(self, collection_id: str, ids: list[int]) -> int:
        if not ids:
            return 0
        wanted = set(ids)
        found = [p["id"] for p in self._coll_patterns(collection_id) if p["id"] in wanted]
        self._forget_excluded_count(collection_id, self._delete_pattern_rows(found))
        return len(found)

    async def list_patterns(self, collection_id: str, *, types: list[str] | None = None,
                            exact: bool | None = None, limit: int | None = None, offset: int = 0) -> list[Pattern]:
        ts = None if types is None else {str(t) for t in types}
        rows = [p for p in self._coll_patterns(collection_id)
                if (ts is None or p["type"] in ts) and (exact is None or ("*" not in p["match"]) == exact)]
        if limit is not None:
            rows = rows[offset:offset + limit]
        return [self._pattern(p) for p in rows]

    async def rules_page(self, collection_id: str, *, q: str | None = None, type_: str | None = None,
                         source: str | None = None, scope: str | None = None,
                         sort: str | None = None, desc: bool = False, limit: int = 200,
                         offset: int = 0) -> tuple[list[Pattern], int]:
        rows = self._coll_patterns(collection_id)
        if q:
            rows = [p for p in rows if _ilike(p["match"], q) or _ilike(p["value"], q)]
        if type_:
            rows = [p for p in rows if p["type"] == _t(type_)]
        if source:
            rows = [p for p in rows if p["source"] == _t(source)]
        if scope in ("glob", "url"):
            rows = [p for p in rows if ("*" in p["match"]) == (scope == "glob")]
        keys = {
            "type": lambda p: p["type"], "match": lambda p: p["match"].lower(),
            "value": lambda p: (p["value"] or "").lower(), "source": lambda p: p["source"],
            "added": lambda p: p["id"], "by": lambda p: (p["created_by"] or "").lower(),
        }
        if sort in keys:
            rows = sorted(rows, key=lambda p: (keys[sort](p), p["id"]), reverse=desc)
        else:
            rows = sorted(rows, key=lambda p: ("*" not in p["match"], p["id"]))
        return [self._pattern(p) for p in rows[offset:offset + limit]], len(rows)

    async def delete_pattern(self, collection_id: str, pattern_id: int) -> bool:
        p = self._patterns.get(pattern_id)
        if p is None or p["collection_id"] != collection_id:
            return False
        types = self._delete_pattern_rows([pattern_id])
        if "*" in p["match"]:  # a per-URL rule keeps the stored count (Database.delete_pattern)
            self._forget_excluded_count(collection_id, types)
        return True

    # ── jobs ───────────────────────────────────────────────────────────

    async def insert_job(self, j: JobRun) -> JobRun:
        self._require(j.collection_id)
        jid = self._next_id("job_runs")
        self._jobs[jid] = {
            "id": jid, "collection_id": j.collection_id, "kind": _t(j.kind), "state": _t(j.state),
            "run_id": j.run_id, "external_ref": j.external_ref, "progress": _jsonb(j.progress), "error": j.error,
            "started_at": j.started_at, "finished_at": j.finished_at, "started_by": j.started_by,
        }
        j.id = jid
        return j

    async def update_job(self, j: JobRun) -> None:
        row = self._jobs.get(j.id)  # type: ignore[arg-type]
        if row is None:
            return
        row.update(state=_t(j.state), run_id=j.run_id, external_ref=j.external_ref, progress=_jsonb(j.progress),
                   error=j.error, finished_at=j.finished_at)

    async def finish_job(self, j: JobRun, state: JobState, error: str | None = None) -> None:
        j.state, j.error, j.finished_at = state, error, utcnow()
        await self.update_job(j)

    async def get_job(self, job_id: int) -> JobRun | None:
        row = self._jobs.get(job_id)
        return self._job(row) if row else None

    async def active_jobs(self) -> list[JobRun]:
        return [self._job(r) for _, r in sorted(self._jobs.items()) if r["state"] in ("queued", "running")]

    async def jobs_ended_by_shutdown(self) -> list[JobRun]:
        latest: dict[str, dict[str, Any]] = {}
        for _, r in sorted(self._jobs.items()):
            latest[r["collection_id"]] = r
        return [self._job(r) for r in sorted(latest.values(), key=lambda r: r["id"])
                if r["state"] == "failed" and r["error"] == "cancelled by shutdown"]

    # ── audit ledger ───────────────────────────────────────────────────

    async def audit(
        self, actor: str, action: str, collection_id: str | None = None, detail: str | None = None
    ) -> None:
        self._audit.append({"id": self._next_id("audit_log"), "at": utcnow(), "actor": actor,
                            "collection_id": collection_id, "action": action, "detail": detail})


# Every Database method the fake stands in for (the contract test checks it covers each one).
IMPLEMENTED = tuple(sorted(
    name for name, v in vars(FakeDatabase).items() if not name.startswith("_") and callable(v)
))
