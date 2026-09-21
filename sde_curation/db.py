"""PostgreSQL state store (psycopg 3, async pool). Every public method is one transaction: the
pool hands out a connection whose context commits on success and rolls back on any exception.
The schema lives in `schema.py` as numbered migrations applied at connect()."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import psycopg
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .engine.patterns import glob_to_like, is_exact
from .engine.text import content_hash
from .engine.urls import spellings
from .models import (
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
    Role,
    Status,
    StatusHistory,
    User,
    check_transition,
    utcnow,
)
from .schema import migrate_async

# Human-readable names for `patterns.source` (SME is the default and reads as no label).
SOURCE_LABEL = {"sme": "SME", "llm": "AI", "llm_edited": "AI, edited", "global": "global list"}

log = logging.getLogger(__name__)


class ConflictError(Exception):
    """A unique constraint refused the write (e.g. the username is taken)."""


async def _scalar(cur: psycopg.AsyncCursor) -> Any:
    row = await cur.fetchone()
    return None if row is None else next(iter(row.values()))


# Every curated column except the page text (most of the bytes; only the export reads it).
_CURATED_COLS = ("collection_id,url,scraped_title,title,division,document_type,excluded,content_hash,edited_by,"
                 "crawl_failure")



# ── column sorting ────────────────────────────────────────────────────
# Sortable columns per table, keyed by the name the UI sends (?sort=…). Each maps to one or more
# SQL expressions; an unknown key falls back to the table's default order, so user input never
# reaches the SQL. The default order is always appended as the tiebreak so paging stays stable.
DUMP_SORTS: dict[str, tuple[str, ...]] = {
    "url": ("d.url",), "scraped_title": ("d.scraped_title",), "content_type": ("d.content_type",),
    "depth": ("d.depth",), "text_len": ("text_len",), "excluded": ("excluded",),
    "vs_curated": ("in_deltas", "in_curated"),
}
DELTA_SORTS: dict[str, tuple[str, ...]] = {
    "kind": ("kind",), "url": ("url",), "excluded": ("excluded",), "title": ("COALESCE(title, scraped_title)",),
    "division": ("division",), "document_type": ("document_type",), "edited_by": ("edited_by",),
}
CURATED_SORTS: dict[str, tuple[str, ...]] = {
    "url": ("url",), "excluded": ("excluded",), "title": ("COALESCE(title, scraped_title)",),
    "division": ("division",), "document_type": ("document_type",), "text_len": ("text_len",),
    "edited_by": ("edited_by",),
}
AUDIT_SORTS: dict[str, tuple[str, ...]] = {
    "at": ("id",), "actor": ("actor",), "collection": ("collection_id",), "action": ("action",),
}


def order_by(sorts: dict[str, tuple[str, ...]], sort: str | None, desc: bool, default: str) -> str:
    """`ORDER BY …` for a whitelisted column (NULLS LAST either way) with the default order as the
    tiebreak; the default alone when `sort` is unknown or unset."""
    exprs = sorts.get(sort or "")
    if not exprs:
        return f" ORDER BY {default}"
    d = "DESC" if desc else "ASC"
    return " ORDER BY " + ", ".join(f"{e} {d} NULLS LAST" for e in exprs) + f", {default}"


AI_FIELDS = ("title", "division", "document_type")

# ── duplicate titles ──────────────────────────────────────────────────
# The title and document type each included page of a collection will be indexed with once the
# delta URLs are promoted: a pending AI suggestion as if accepted, else the effective value; the
# title falls back to the scraped title (the export's fallback). A curated row that a delta row
# stands in for (same URL, a rename or a removal) counts once, as the delta row. Two pages are
# duplicates when BOTH match: the title ignoring case and runs of whitespace, and the document type
# (two pages with the same title but different types are told apart by the type). Both queries take
# the collection id twice.
_PROJECTED_TITLES = """
SELECT url, delta, pending_ai, title, document_type,
       lower(regexp_replace(btrim(title), '\\s+', ' ', 'g')) || chr(31) || COALESCE(document_type, '') AS k FROM (
  SELECT d.url, true AS delta, d.title_ai IS NOT NULL AS pending_ai,
         COALESCE(d.title_ai, d.title, d.scraped_title) AS title,
         COALESCE(d.document_type_ai, d.document_type) AS document_type
    FROM delta_urls d WHERE d.collection_id=%s AND d.kind!='deleted' AND NOT d.excluded
  UNION ALL
  SELECT c.url, false, false, COALESCE(c.title, c.scraped_title), c.document_type
    FROM curated_urls c WHERE c.collection_id=%s AND NOT c.excluded
     AND NOT EXISTS (SELECT 1 FROM delta_urls x WHERE x.collection_id=c.collection_id AND x.url=c.url)
     AND NOT EXISTS (SELECT 1 FROM delta_urls x WHERE x.collection_id=c.collection_id AND x.renamed_from=c.url)
) p WHERE btrim(COALESCE(title, '')) != ''"""
# A delta URL that promote would write into the curated set without a title, a division or a
# document type (its effective values: rules or the curated row, never a pending suggestion).
# Removals and excluded rows carry no metadata to the index, so they never count.
_INCOMPLETE = ("kind!='deleted' AND NOT excluded"
               " AND (btrim(COALESCE(title, ''))='' OR division IS NULL OR document_type IS NULL)")

_DUPLICATE_TITLES = (f"SELECT * FROM (SELECT t.*, COUNT(*) OVER (PARTITION BY k) AS n FROM ({_PROJECTED_TITLES}) t) w"
                     " WHERE n > 1")


def match_clause(match: str, col: str) -> tuple[str, list[Any]]:
    """`?match=<glob>` as SQL: the URLs a rule (or a not-yet-saved suggestion) matches, the same
    set the engine's glob_to_regex selects. A glob is one LIKE. An exact-URL rule matches by
    canonical key in the engine, which SQL has not: it becomes every spelling of its page
    (https/http, www., trailing slash) plus their #fragment forms."""
    if is_exact(match):
        sp = spellings(match)
        return f"({col} = ANY(%s) OR {col} LIKE ANY(%s))", [sp, [glob_to_like(u) + "#%" for u in sp]]
    return f"{col} LIKE %s", [glob_to_like(match)]


class Database:
    def __init__(self, dsn: str, *, pool_size: int = 8, connect_timeout_s: float = 30.0):
        self.dsn = dsn
        self.pool_size = pool_size
        self.connect_timeout_s = connect_timeout_s
        self._pool: AsyncConnectionPool | None = None
        # optional async hook(collection_id, old_status, new_status, note, actor) after every history row
        self.on_status_change = None

    @property
    def pool(self) -> AsyncConnectionPool:
        if self._pool is None:
            raise RuntimeError("database not connected")
        return self._pool

    def _conn(self):
        """One pooled connection = one transaction (commit on exit, rollback on exception)."""
        return self.pool.connection()

    async def connect(self) -> Database:
        self._pool = AsyncConnectionPool(
            self.dsn, min_size=1, max_size=max(1, self.pool_size), open=False, name="engine",
            kwargs={"row_factory": dict_row, "options": "-c timezone=UTC"},
        )
        await self._pool.open()
        try:
            await self._pool.wait(timeout=self.connect_timeout_s)  # fail fast when unreachable
            async with self._conn() as conn:
                await migrate_async(conn)
        except BaseException:
            await self._pool.close()
            self._pool = None
            raise
        return self

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def ping(self) -> bool:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT 1 AS ok")
            return (await cur.fetchone()) is not None

    # ── raw access (operators, tests, the importer) ────────────────────

    async def execute(self, sql: str, params: Any = ()) -> int:
        """Run one statement in its own transaction; returns the affected row count."""
        async with self._conn() as conn:
            cur = await conn.execute(sql, params)
            return cur.rowcount

    async def fetch(self, sql: str, params: Any = ()) -> list[dict[str, Any]]:
        async with self._conn() as conn:
            cur = await conn.execute(sql, params)
            return list(await cur.fetchall())

    async def fetchval(self, sql: str, params: Any = ()) -> Any:
        """First column of the first row (None when there is no row)."""
        async with self._conn() as conn:
            return await _scalar(await conn.execute(sql, params))

    # ── collections ────────────────────────────────────────────────────

    async def insert_collection(self, c: Collection) -> Collection:
        async with self._conn() as conn:
            await conn.execute(
                """INSERT INTO collections (collection_id,name,seed_url,division,document_type,connector,
                   max_pages,status,needs_recuration,created_at,updated_at,dump_count,delta_count,curated_count,
                   created_by) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    c.collection_id, c.name, c.seed_url, c.division, c.document_type, c.connector,
                    c.max_pages, c.status, c.needs_recuration, c.created_at,
                    c.updated_at, c.dump_count, c.delta_count, c.curated_count, c.created_by,
                ),
            )
            await conn.execute(
                "INSERT INTO status_history (collection_id,old_status,new_status,note,at,actor) VALUES (%s,%s,%s,%s,%s,%s)",
                (c.collection_id, None, c.status, "created", utcnow(), c.created_by),
            )
        return c

    async def get_collection(self, collection_id: str) -> Collection | None:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM collections WHERE collection_id=%s", (collection_id,))
            row = await cur.fetchone()
        return Collection(**row) if row else None

    async def list_collections(self) -> list[Collection]:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM collections ORDER BY created_at DESC")
            return [Collection(**r) for r in await cur.fetchall()]

    async def delete_collection(self, collection_id: str) -> bool:
        """Everything of the collection goes with it (ON DELETE CASCADE); audit rows keep its id."""
        async with self._conn() as conn:
            cur = await conn.execute("DELETE FROM collections WHERE collection_id=%s", (collection_id,))
            return cur.rowcount > 0

    async def set_status(
        self, collection_id: str, new: Status, note: str | None = None, *, force: bool = False,
        actor: str | None = None,
    ) -> Collection:
        async with self._conn() as conn:
            # Row lock: the read-check-write below must not interleave with another transition.
            cur = await conn.execute(
                "SELECT * FROM collections WHERE collection_id=%s FOR UPDATE", (collection_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise KeyError(collection_id)
            c = Collection(**row)
            if not force:
                check_transition(c.status, new)
            now = utcnow()
            # Stage rule, applied for every caller: entering `curating` starts at exclusions, staying in
            # it keeps the current stage, leaving it clears the stage.
            if new is Status.CURATING:
                stage = c.curation_stage if c.status is Status.CURATING and c.curation_stage else CurationStage.EXCLUSIONS
            else:
                stage = None
            await conn.execute(
                "UPDATE collections SET status=%s, curation_stage=%s, updated_at=%s WHERE collection_id=%s",
                (new, stage, now, collection_id),
            )
            await conn.execute(
                "INSERT INTO status_history (collection_id,old_status,new_status,note,at,actor) VALUES (%s,%s,%s,%s,%s,%s)",
                (collection_id, c.status, new, note, now, actor),
            )
        if self.on_status_change:  # every history row (the hook decides what to notify / persist)
            try:
                await self.on_status_change(collection_id, c.status, new, note, actor)
            except Exception as e:  # noqa: BLE001 - notifications must never break a transition
                log.warning("status hook failed: %s", e)
        c.status, c.curation_stage, c.updated_at = new, stage, now
        return c

    async def set_stage(self, collection_id: str, stage: CurationStage) -> bool:
        """Move a curating collection between stages; no-op (False) outside `curating`."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "UPDATE collections SET curation_stage=%s, updated_at=%s WHERE collection_id=%s AND status='curating'",
                (stage, utcnow(), collection_id),
            )
            return cur.rowcount > 0

    async def set_last_scraped(self, collection_id: str, at: datetime, *, capped: bool = False) -> None:
        """When the dump was crawled, and whether that crawl stopped at its page cap (then a curated
        URL missing from the dump is not evidence that it is gone)."""
        async with self._conn() as conn:
            await conn.execute(
                "UPDATE collections SET last_scraped_at=%s, last_crawl_capped=%s WHERE collection_id=%s",
                (at, capped, collection_id),
            )

    async def set_index_key(self, collection_id: str, index_key: str, index_name: str | None) -> None:
        """The OpenSearch collection_key / collection_name this collection is indexed under."""
        async with self._conn() as conn:
            await conn.execute(
                "UPDATE collections SET index_key=%s, index_name=%s, updated_at=%s WHERE collection_id=%s",
                (index_key, index_name, utcnow(), collection_id),
            )

    async def set_flag(self, collection_id: str, needs_recuration: bool, reason: str | None = None) -> None:
        """Raise or clear the needs-re-curation flag; the reason is kept only while it is up."""
        async with self._conn() as conn:
            await conn.execute(
                "UPDATE collections SET needs_recuration=%s, recuration_reason=%s, updated_at=%s WHERE collection_id=%s",
                (needs_recuration, (reason or None) if needs_recuration else None, utcnow(), collection_id),
            )

    @staticmethod
    async def _recount_curated(conn, collection_id: str, *, changed: bool) -> int:
        """Refresh the stored curated counters from the table and return the included count.
        `curated_count` is the URLs that reach the index (excluded rows are listed but not counted),
        `curated_rows` the whole set. `changed` also stamps `curated_changed_at` — what the
        "needs re-indexing" chip compares the last index run against."""
        cur = await conn.execute(
            "UPDATE collections c SET curated_count=s.included, curated_rows=s.n, updated_at=%(now)s"
            + (", curated_changed_at=%(now)s" if changed else "")
            + " FROM (SELECT COUNT(*) AS n, COUNT(*) FILTER (WHERE NOT excluded) AS included"
              " FROM curated_urls WHERE collection_id=%(cid)s) s"
              " WHERE c.collection_id=%(cid)s RETURNING c.curated_count",
            {"now": utcnow(), "cid": collection_id},
        )
        row = await cur.fetchone()
        return (row["curated_count"] if isinstance(row, dict) else row[0]) if row else 0

    async def update_counts(self, collection_id: str, **counts: int) -> None:
        allowed = {"dump_count", "delta_count", "curated_count", "curated_rows"}
        bad = set(counts) - allowed
        if bad:
            raise ValueError(f"unknown counters {bad}")
        if not counts:
            return
        sets = ", ".join(f"{k}=%s" for k in counts)
        async with self._conn() as conn:
            await conn.execute(
                f"UPDATE collections SET {sets}, updated_at=%s WHERE collection_id=%s",
                (*counts.values(), utcnow(), collection_id),
            )

    async def status_history(self, collection_id: str) -> list[StatusHistory]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM status_history WHERE collection_id=%s ORDER BY id", (collection_id,)
            )
            return [StatusHistory(**r) for r in await cur.fetchall()]

    # ── dump urls ──────────────────────────────────────────────────────

    async def replace_dump(
        self, collection_id: str, rows: list[DumpUrl], failures: list[DumpFailure] | None = None,
    ) -> int:
        """Bulk-replace the dump for a collection in one transaction; returns row count. `failures`
        are the URLs the crawler tried and could not fetch (replaced along with the dump: the two
        together are what one crawl found out)."""
        async with self._conn() as conn:
            await conn.execute("DELETE FROM dump_urls WHERE collection_id=%s", (collection_id,))
            await conn.execute("DELETE FROM dump_failures WHERE collection_id=%s", (collection_id,))
            if failures:
                async with conn.cursor() as cur, cur.copy(
                    "COPY dump_failures (collection_id,url,reason,status,detail) FROM STDIN"
                ) as copy:
                    seen_f: set[str] = set()
                    for f in failures:
                        if f.url in seen_f:
                            continue
                        seen_f.add(f.url)
                        await copy.write_row((collection_id, f.url, f.reason, f.status, f.detail))
            async with conn.cursor() as cur, cur.copy(
                "COPY dump_urls (collection_id,url,scraped_title,full_text,content_type,depth,content_hash)"
                " FROM STDIN"
            ) as copy:
                seen: set[str] = set()
                for r in rows:
                    if r.url in seen:  # the SQLite version did INSERT OR REPLACE
                        continue
                    seen.add(r.url)
                    await copy.write_row((
                        r.collection_id, r.url, r.scraped_title, r.full_text, r.content_type, r.depth,
                        r.content_hash or content_hash(r.full_text),
                    ))
            n = await _scalar(await conn.execute(
                "SELECT COUNT(*) FROM dump_urls WHERE collection_id=%s", (collection_id,)
            ))
            await conn.execute(
                "UPDATE collections SET dump_count=%s, updated_at=%s WHERE collection_id=%s",
                (n, utcnow(), collection_id),
            )
            return n

    async def list_dump(
        self, collection_id: str, limit: int = 100, offset: int = 0, q: str | None = None,
        match: str | None = None, sort: str | None = None, desc: bool = False, excluded: bool | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Dump rows (no full_text) plus `text_len` and `in_curated` / `in_deltas` flags. `excluded`
        is the pending delta's, else the curated row's, else whether an exclude rule keeps the URL
        out (excluded URLs have no delta row: the rule decides them)."""
        where, args = ["d.collection_id=%s"], [collection_id]
        if q:
            where.append("(d.url ILIKE %s OR d.scraped_title ILIKE %s)"); args += [f"%{q}%"] * 2
        if match:
            m, a = match_clause(match, "d.url"); where.append(m); args += a
        excl = """COALESCE(x.excluded, c.excluded, EXISTS(
                      SELECT 1 FROM pattern_effects e JOIN patterns p ON p.id=e.pattern_id
                      WHERE e.collection_id=d.collection_id AND e.url=d.url
                        AND e.field='excluded' AND p.type='exclude'))"""
        if excluded is not None:
            where.append(f"{excl}=%s"); args.append(excluded)
        w = " AND ".join(where)
        joins = """LEFT JOIN curated_urls c ON c.collection_id=d.collection_id AND c.url=d.url
                    LEFT JOIN delta_urls x ON x.collection_id=d.collection_id AND x.url=d.url"""
        async with self._conn() as conn:
            total = await _scalar(await conn.execute(
                f"SELECT COUNT(*) FROM dump_urls d {joins if excluded is not None else ''} WHERE {w}", args))
            cur = await conn.execute(
                f"""SELECT d.collection_id, d.url, d.scraped_title, d.content_type, d.depth,
                           length(d.full_text) AS text_len,
                           (c.url IS NOT NULL)::int AS in_curated, (x.url IS NOT NULL)::int AS in_deltas,
                           {excl} AS excluded,
                           COALESCE(x.edited_by, c.edited_by) AS edited_by
                    FROM dump_urls d
                    {joins}
                    WHERE {w}{order_by(DUMP_SORTS, sort, desc, "d.url")} LIMIT %s OFFSET %s""",
                [*args, limit, offset],
            )
            return list(await cur.fetchall()), total

    async def urls_with_deltas(self, collection_id: str, urls: list[str]) -> set[str]:
        if not urls:
            return set()
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT url FROM delta_urls WHERE collection_id=%s AND url = ANY(%s)", (collection_id, list(urls))
            )
            return {r["url"] for r in await cur.fetchall()}

    async def deltas_for(self, collection_id: str, urls: list[str]) -> dict[str, DeltaUrl]:
        """The pending delta row, if any, for each of these URLs (the Curated table shows the
        values a row will have once promoted, not the ones it was promoted with)."""
        if not urls:
            return {}
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM delta_urls WHERE collection_id=%s AND url = ANY(%s)", (collection_id, list(urls))
            )
            return {r["url"]: DeltaUrl(**r) for r in await cur.fetchall()}

    async def effects_for(self, collection_id: str, urls: list[str]) -> dict[str, dict[str, str]]:
        """{url: {field: 'type match → value (by who · source)'}} — which pattern produced each
        effective field ("excluded" = the exclude / include rule that decided the row)."""
        if not urls:
            return {}
        async with self._conn() as conn:
            cur = await conn.execute(
                """SELECT e.url, e.field, p.type, p.match, p.value, p.created_by, p.source FROM pattern_effects e
                   JOIN patterns p ON p.id=e.pattern_id
                   WHERE e.collection_id=%s AND e.url = ANY(%s)""", (collection_id, list(urls)),
            )
            rows = await cur.fetchall()
        out: dict[str, dict[str, str]] = {}
        for r in rows:
            by, source, value = r["created_by"], r["source"], r["value"]
            out.setdefault(r["url"], {})[r["field"]] = (
                f"{r['type']} {r['match']}" + (f" → {value}" if value else "") + (f" (by {by})" if by else "")
                + (f" · {SOURCE_LABEL[source]}" if source in SOURCE_LABEL and source != "sme" else "")
            )
        return out

    async def list_curated(
        self, collection_id: str, limit: int = 100, offset: int = 0, q: str | None = None,
        excluded: bool | None = None, edited: str | None = None, unreachable: bool | None = None,
        match: str | None = None, dup_title: bool = False, sort: str | None = None, desc: bool = False,
    ) -> tuple[list[CuratedUrl], int]:
        where, args = ["collection_id=%s"], [collection_id]
        if dup_title:
            where.append(f"url IN (SELECT url FROM ({_DUPLICATE_TITLES}) g)"); args += [collection_id] * 2
        if q:
            where.append("(url ILIKE %s OR title ILIKE %s OR scraped_title ILIKE %s)"); args += [f"%{q}%"] * 3
        if match:
            m, a = match_clause(match, "url"); where.append(m); args += a
        if excluded is not None:
            where.append("excluded=%s"); args.append(excluded)
        if edited:
            where.append("edited_by=%s"); args.append(edited)
        if unreachable is not None:
            where.append("crawl_failure IS NOT NULL" if unreachable else "crawl_failure IS NULL")
        w = " AND ".join(where)
        async with self._conn() as conn:
            total = await _scalar(await conn.execute(f"SELECT COUNT(*) FROM curated_urls WHERE {w}", args))
            cur = await conn.execute(
                f"SELECT {_CURATED_COLS}, length(full_text) AS text_len FROM curated_urls WHERE {w}"
                f"{order_by(CURATED_SORTS, sort, desc, 'url')} LIMIT %s OFFSET %s", [*args, limit, offset]
            )
            return [CuratedUrl(**r) for r in await cur.fetchall()], total

    async def load_dump(self, collection_id: str) -> list[DumpUrl]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT collection_id,url,scraped_title,content_type,depth,content_hash FROM dump_urls WHERE collection_id=%s",
                (collection_id,),
            )
            return [DumpUrl(**r) for r in await cur.fetchall()]

    async def dump_urls(self, collection_id: str) -> list[str]:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT url FROM dump_urls WHERE collection_id=%s", (collection_id,))
            return [r["url"] for r in await cur.fetchall()]

    async def set_urls(self, collection_id: str, set_: str) -> list[str]:
        """Every URL of one set (dump / delta / curated): what a rule's match count is taken over."""
        table = {"dump": "dump_urls", "delta": "delta_urls", "curated": "curated_urls"}[set_]
        async with self._conn() as conn:
            cur = await conn.execute(f"SELECT url FROM {table} WHERE collection_id=%s", (collection_id,))
            return [r["url"] for r in await cur.fetchall()]

    async def effect_counts(self, collection_id: str) -> dict[int, int]:
        """pattern_id -> how many URLs the rule currently decides. pattern_effects holds only the
        winner per (url, field) and a rule has one type, so COUNT(*) is a URL count; it is as of
        the last recompute (a promote keeps the effects)."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT pattern_id, COUNT(*) AS n FROM pattern_effects WHERE collection_id=%s GROUP BY pattern_id",
                (collection_id,),
            )
            return {r["pattern_id"]: r["n"] for r in await cur.fetchall()}

    async def load_dump_failures(self, collection_id: str) -> dict[str, str]:
        """url -> crawler reason for every URL the current crawl tried and could not fetch."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT url, reason FROM dump_failures WHERE collection_id=%s", (collection_id,)
            )
            return {r["url"]: r["reason"] for r in await cur.fetchall()}

    async def list_dump_failures(self, collection_id: str) -> list[DumpFailure]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT collection_id,url,reason,status,detail FROM dump_failures WHERE collection_id=%s ORDER BY url",
                (collection_id,),
            )
            return [DumpFailure(**r) for r in await cur.fetchall()]

    async def dump_content_hashes(self, collection_id: str) -> dict[str, str | None]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT url, content_hash FROM dump_urls WHERE collection_id=%s", (collection_id,)
            )
            return {r["url"]: r["content_hash"] for r in await cur.fetchall()}

    # ── deltas / curated ───────────────────────────────────────────────

    async def load_deltas(self, collection_id: str) -> list[DeltaUrl]:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM delta_urls WHERE collection_id=%s", (collection_id,))
            return [DeltaUrl(**r) for r in await cur.fetchall()]

    async def get_delta(self, collection_id: str, url: str) -> DeltaUrl | None:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM delta_urls WHERE collection_id=%s AND url=%s", (collection_id, url)
            )
            row = await cur.fetchone()
        return DeltaUrl(**row) if row else None

    async def list_deltas(
        self, collection_id: str, *, kind: str | None = None, excluded: bool | None = None,
        q: str | None = None, division: str | None = None, document_type: str | None = None,
        ai_pending: bool = False, ai_conf: str | None = None, ai_field: str | None = None,
        ai_failed: bool = False, content_changed: bool | None = None, edited: str | None = None, renamed: bool | None = None,
        match: str | None = None, dup_title: bool = False, retitled: bool = False, incomplete: bool = False,
        limit: int = 100, offset: int = 0,
        sort: str | None = None, desc: bool = False,
    ) -> tuple[list[DeltaUrl], int]:
        """`ai_field`: rows with a pending suggestion for that one field (title / division /
        document_type); `match`: rows a rule's glob or exact URL matches (see match_clause);
        `dup_title`: rows whose title and document type another page of the collection will also have;
        `retitled`: rows whose duplicate AI title was regenerated (the title they shared is kept);
        `incomplete`: rows promote refuses (no title, division or document type yet)."""
        where, args = ["collection_id=%s"], [collection_id]
        if dup_title:
            where.append(f"url IN (SELECT url FROM ({_DUPLICATE_TITLES}) g WHERE delta)"); args += [collection_id] * 2
        if retitled:
            where.append("title_ai IS NOT NULL AND title_ai_before IS NOT NULL")
        if incomplete:
            where.append(f"({_INCOMPLETE})")
        if ai_field in AI_FIELDS:
            where.append(f"{ai_field}_ai IS NOT NULL")
        if match:
            m, a = match_clause(match, "url"); where.append(m); args += a
        if kind:
            where.append("kind=%s"); args.append(kind)
        if renamed is not None:
            where.append("renamed_from IS NOT NULL" if renamed else "renamed_from IS NULL")
        if edited:
            where.append("edited_by=%s"); args.append(edited)
        if excluded is not None:
            where.append("excluded=%s"); args.append(excluded)
        if content_changed is not None:
            where.append("content_changed=%s"); args.append(content_changed)
        if ai_failed:
            where.append("ai_error IS NOT NULL")
        if ai_pending:
            where.append("(title_ai IS NOT NULL OR division_ai IS NOT NULL OR document_type_ai IS NOT NULL)")
        if ai_conf:
            where.append("((title_ai IS NOT NULL AND title_ai_conf=%s) OR (division_ai IS NOT NULL AND division_ai_conf=%s)"
                         " OR (document_type_ai IS NOT NULL AND document_type_ai_conf=%s))")
            args += [ai_conf] * 3
        if division:
            where.append("division=%s"); args.append(division)
        if document_type:
            where.append("document_type=%s"); args.append(document_type)
        if q:
            where.append("(url ILIKE %s OR title ILIKE %s OR scraped_title ILIKE %s OR renamed_from ILIKE %s)")
            args += [f"%{q}%"] * 4
        w = " AND ".join(where)
        async with self._conn() as conn:
            total = await _scalar(await conn.execute(f"SELECT COUNT(*) FROM delta_urls WHERE {w}", args))
            cur = await conn.execute(
                f"SELECT * FROM delta_urls WHERE {w}{order_by(DELTA_SORTS, sort, desc, 'kind, url')}"
                " LIMIT %s OFFSET %s",
                [*args, limit, offset],
            )
            return [DeltaUrl(**r) for r in await cur.fetchall()], total

    async def replace_deltas(
        self, collection_id: str, deltas: list[DeltaUrl], effects: list[tuple[int, str, str]],
        *, keep_effects: bool = False,
    ) -> None:
        """Replace the delta URLs (and, unless `keep_effects`, the rule→URL effects). Promote
        keeps the effects: the rules did not change, and the Curated table still explains its values."""
        async with self._conn() as conn:
            await conn.execute("DELETE FROM delta_urls WHERE collection_id=%s", (collection_id,))
            async with conn.cursor() as cur:
                async with cur.copy(
                    "COPY delta_urls (collection_id,url,kind,renamed_from,crawl_failure,scraped_title,title,"
                    "division,document_type,excluded,content_changed,edited_by,title_ai,division_ai,"
                    "document_type_ai,title_ai_conf,division_ai_conf,document_type_ai_conf,ai_model,"
                    "ai_content_hash,ai_error,ai_failures,title_ai_before) FROM STDIN"
                ) as copy:
                    for d in deltas:
                        await copy.write_row((
                            d.collection_id, d.url, d.kind, d.renamed_from, d.crawl_failure, d.scraped_title,
                            d.title, d.division, d.document_type, d.excluded, d.content_changed, d.edited_by,
                            d.title_ai, d.division_ai, d.document_type_ai, d.title_ai_conf, d.division_ai_conf,
                            d.document_type_ai_conf, d.ai_model, d.ai_content_hash, d.ai_error, d.ai_failures,
                            d.title_ai_before,
                        ))
                if not keep_effects:
                    await cur.execute("DELETE FROM pattern_effects WHERE collection_id=%s", (collection_id,))
                    if effects:
                        await cur.executemany(
                            "INSERT INTO pattern_effects (pattern_id,collection_id,url,field) VALUES (%s,%s,%s,%s)"
                            " ON CONFLICT DO NOTHING",
                            [(pid, collection_id, url, fld) for pid, url, fld in effects],
                        )
            await conn.execute(
                "UPDATE collections SET delta_count=%s, updated_at=%s WHERE collection_id=%s",
                (len(deltas), utcnow(), collection_id),
            )

    async def delete_deltas(self, collection_id: str, urls: list[str]) -> int:
        """Drop these rows from the review queue (a partial promote) and recount in SQL. The
        rule→URL effects stay: the rows became curated rows and the Curated table still explains
        them (same reason a full promote keeps the effects)."""
        if not urls:
            return 0
        async with self._conn() as conn:
            cur = await conn.execute(
                "DELETE FROM delta_urls WHERE collection_id=%s AND url = ANY(%s)", (collection_id, list(urls))
            )
            await conn.execute(
                "UPDATE collections SET delta_count=(SELECT COUNT(*) FROM delta_urls WHERE collection_id=%s),"
                " updated_at=%s WHERE collection_id=%s",
                (collection_id, utcnow(), collection_id),
            )
            return cur.rowcount

    async def delete_effects(self, collection_id: str, urls: list[str]) -> None:
        """Forget the rule→URL effects of URLs that are in neither set any more (promoted tombstones)."""
        if not urls:
            return
        async with self._conn() as conn:
            await conn.execute(
                "DELETE FROM pattern_effects WHERE collection_id=%s AND url = ANY(%s)", (collection_id, list(urls))
            )

    async def set_delta_ai(self, collection_id: str, items: list[dict[str, Any]]) -> int:
        """Bulk-write AI suggestions for whole rows (never touches the effective fields). A
        re-classification replaces the previous answer, confidence included, and clears any
        recorded failure."""
        if not items:
            return 0
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.executemany(
                """UPDATE delta_urls SET title_ai=%s, division_ai=%s, document_type_ai=%s,
                   title_ai_conf=%s, division_ai_conf=%s, document_type_ai_conf=%s,
                   ai_model=%s, ai_content_hash=%s, ai_error=NULL, ai_failures=0, title_ai_before=NULL
                   WHERE collection_id=%s AND url=%s""",
                [(i.get("title"), i.get("division"), i.get("document_type"),
                  i.get("title_conf"), i.get("division_conf"), i.get("document_type_conf"),
                  i.get("model"), i.get("content_hash"),
                  collection_id, i["url"]) for i in items],
            )
        return len(items)

    async def set_delta_ai_errors(self, collection_id: str, items: list[tuple[str, str]]) -> int:
        """Record (url, error) for URLs whose Suggest metadata call failed. A previous answer stays:
        a failed re-classification does not throw away what the model said last time."""
        if not items:
            return 0
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.executemany(
                "UPDATE delta_urls SET ai_error=%s, ai_failures=ai_failures+1 WHERE collection_id=%s AND url=%s",
                [(err[:1000], collection_id, url) for url, err in items],
            )
        return len(items)

    async def load_curated(self, collection_id: str, *, with_text: bool = False) -> list[CuratedUrl]:
        """The whole curated set. `with_text` also loads the approved page text (the export needs
        it; the diff and the pages do not, and it is most of the bytes)."""
        cols = "*" if with_text else _CURATED_COLS
        async with self._conn() as conn:
            cur = await conn.execute(f"SELECT {cols} FROM curated_urls WHERE collection_id=%s", (collection_id,))
            return [CuratedUrl(**r) for r in await cur.fetchall()]

    async def set_curated_edited_by(self, collection_id: str, items: list[tuple[str, str | None]]) -> None:
        """Re-attribute unchanged curated rows (no delta) after a recompute."""
        if not items:
            return
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.executemany(
                "UPDATE curated_urls SET edited_by=%s WHERE collection_id=%s AND url=%s",
                [(eb, collection_id, url) for url, eb in items],
            )

    async def set_curated_excluded(self, collection_id: str, items: list[tuple[str, bool]]) -> None:
        """Flag curated rows an exclude rule now keeps out, in place: exclusions are decided by the
        rules, never queued as delta URLs (the next index run drops the rows). The row stays in the
        Curated URLs list; it leaves `curated_count` and marks the curated set as changed, so the
        count drops and the "needs re-indexing" chip goes up as soon as the rule is added."""
        if not items:
            return
        async with self._conn() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    "UPDATE curated_urls SET excluded=%s WHERE collection_id=%s AND url=%s",
                    [(excluded, collection_id, url) for url, excluded in items],
                )
            await self._recount_curated(conn, collection_id, changed=True)

    async def count_excluded_by_rules(self, collection_id: str) -> int:
        """Dump URLs an exclude rule keeps out (no include overrides it) — they have no delta row."""
        async with self._conn() as conn:
            return await _scalar(await conn.execute(
                "SELECT COUNT(DISTINCT e.url) FROM pattern_effects e JOIN patterns p ON p.id=e.pattern_id"
                " JOIN dump_urls d ON d.collection_id=e.collection_id AND d.url=e.url"
                " WHERE e.collection_id=%s AND e.field='excluded' AND p.type='exclude'",
                (collection_id,),
            ))

    async def set_curated_crawl_failure(self, collection_id: str, items: list[tuple[str, str | None]]) -> None:
        """Flag (reason) or clear (None) curated rows after a recompute: the current dump lacks the
        URL but the crawl does not prove it gone, or the crawl fetched it again."""
        if not items:
            return
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.executemany(
                "UPDATE curated_urls SET crawl_failure=%s WHERE collection_id=%s AND url=%s",
                [(reason, collection_id, url) for url, reason in items],
            )

    async def count_curated_unreachable(self, collection_id: str) -> int:
        async with self._conn() as conn:
            return await _scalar(await conn.execute(
                "SELECT COUNT(*) FROM curated_urls WHERE collection_id=%s AND crawl_failure IS NOT NULL",
                (collection_id,),
            ))

    async def replace_curated(
        self, collection_id: str, rows: list[CuratedUrl], *, text_from_dump: bool = True,
        text_urls: list[str] | None = None, changed: bool = True,
    ) -> int:
        """Bulk-replace the curated set in one transaction; returns the included count (what
        `curated_count` holds — excluded rows are written but not counted). `changed` stamps
        `curated_changed_at`: a promote that moved nothing (the "mark curated" shortcut) leaves the
        index as up to date as it was. With `text_from_dump`
        (a promote) every row that is in the dump takes the dump's current page text — copied inside
        PostgreSQL, so the text never travels through the app; `text_urls` limits that to the rows
        just promoted (a partial promote: a row still under review keeps the text its curated
        metadata was approved with). A row's own `full_text` is written first; a row written
        without text keeps the text it already had in the table (a promote loads the curated set
        without text, and a row the dump lacks — kept through a crawl failure — must not lose the
        text the index holds for it)."""
        async with self._conn() as conn:
            await conn.execute(
                "CREATE TEMP TABLE curated_in (LIKE curated_urls INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            async with conn.cursor() as cur, cur.copy(
                "COPY curated_in (collection_id,url,scraped_title,title,division,document_type,excluded,"
                "content_hash,edited_by,full_text,crawl_failure) FROM STDIN"
            ) as copy:
                for r in rows:
                    await copy.write_row((
                        r.collection_id, r.url, r.scraped_title, r.title, r.division, r.document_type,
                        r.excluded, r.content_hash, r.edited_by, r.full_text, r.crawl_failure,
                    ))
            await conn.execute(
                "DELETE FROM curated_urls c WHERE c.collection_id=%s"
                " AND NOT EXISTS (SELECT 1 FROM curated_in i WHERE i.url=c.url)",
                (collection_id,),
            )
            await conn.execute(
                "INSERT INTO curated_urls (collection_id,url,scraped_title,title,division,document_type,excluded,"
                "content_hash,edited_by,full_text,crawl_failure)"
                " SELECT collection_id,url,scraped_title,title,division,document_type,excluded,content_hash,"
                "edited_by,full_text,crawl_failure FROM curated_in"
                " ON CONFLICT (collection_id, url) DO UPDATE SET scraped_title=EXCLUDED.scraped_title,"
                " title=EXCLUDED.title, division=EXCLUDED.division, document_type=EXCLUDED.document_type,"
                " excluded=EXCLUDED.excluded, content_hash=EXCLUDED.content_hash, edited_by=EXCLUDED.edited_by,"
                " full_text=COALESCE(EXCLUDED.full_text, curated_urls.full_text), crawl_failure=EXCLUDED.crawl_failure"
            )
            if text_from_dump:
                await conn.execute(
                    "UPDATE curated_urls c SET full_text = d.full_text FROM dump_urls d"
                    " WHERE c.collection_id=%s AND d.collection_id=c.collection_id AND d.url=c.url"
                    + ("" if text_urls is None else " AND c.url = ANY(%s)"),
                    (collection_id,) if text_urls is None else (collection_id, list(text_urls)),
                )
            return await self._recount_curated(conn, collection_id, changed=changed)

    # ── index runs ─────────────────────────────────────────────────────

    async def insert_index_run(self, r: IndexRun) -> IndexRun:
        async with self._conn() as conn:
            await conn.execute(
                """INSERT INTO index_runs (run_id,collection_id,target,state,exported,external_ref,status,validation,
                   validated_by,error,started_at,finished_at,started_by)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (r.run_id, r.collection_id, r.target, r.state, r.exported, r.external_ref,
                 Jsonb(r.status) if r.status else None, Jsonb(r.validation) if r.validation else None,
                 r.validated_by, r.error, r.started_at, r.finished_at, r.started_by),
            )
            await conn.execute(
                "UPDATE collections SET last_run_id=%s, updated_at=%s WHERE collection_id=%s",
                (r.run_id, utcnow(), r.collection_id),
            )
        return r

    async def update_index_run(self, r: IndexRun) -> None:
        async with self._conn() as conn:
            await conn.execute(
                """UPDATE index_runs SET state=%s, exported=%s, external_ref=%s, status=%s, validation=%s,
                   validated_by=%s, error=%s, finished_at=%s WHERE run_id=%s""",
                (r.state, r.exported, r.external_ref, Jsonb(r.status) if r.status else None,
                 Jsonb(r.validation) if r.validation else None, r.validated_by, r.error, r.finished_at,
                 r.run_id),
            )

    @staticmethod
    def _index_run(row: dict[str, Any]) -> IndexRun:
        return IndexRun(**row)

    async def list_index_runs(self, collection_id: str, limit: int = 20) -> list[IndexRun]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM index_runs WHERE collection_id=%s ORDER BY started_at DESC LIMIT %s",
                (collection_id, limit),
            )
            return [self._index_run(r) for r in await cur.fetchall()]

    async def get_index_run(self, run_id: str) -> IndexRun | None:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM index_runs WHERE run_id=%s", (run_id,))
            row = await cur.fetchone()
        return self._index_run(row) if row else None

    async def last_index_run(self, collection_id: str, target: str | None = None) -> IndexRun | None:
        q, args = "SELECT * FROM index_runs WHERE collection_id=%s", [collection_id]
        if target:
            q += " AND target=%s"; args.append(target)
        async with self._conn() as conn:
            cur = await conn.execute(q + " ORDER BY started_at DESC LIMIT 1", args)
            row = await cur.fetchone()
        return self._index_run(row) if row else None

    async def latest_runs(self, target: str) -> dict[str, IndexRun]:
        """The newest `target` run of every collection that has one, in one query (dashboard chips/filter)."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT DISTINCT ON (collection_id) * FROM index_runs WHERE target=%s"
                " ORDER BY collection_id, started_at DESC", (target,),
            )
            return {r["collection_id"]: self._index_run(r) for r in await cur.fetchall()}

    async def curated_export_count(self, collection_id: str) -> int:
        async with self._conn() as conn:
            return await _scalar(await conn.execute(
                "SELECT COUNT(*) FROM curated_urls WHERE collection_id=%s AND NOT excluded", (collection_id,)
            ))

    # ── LLM suggestions ────────────────────────────────────────────────

    async def clear_pending_pattern_suggestions(self, collection_id: str) -> None:
        async with self._conn() as conn:
            await conn.execute(
                "DELETE FROM pattern_suggestions WHERE collection_id=%s AND state='pending'", (collection_id,)
            )

    @staticmethod
    async def _add_pattern_suggestions(conn: AsyncConnection, collection_id: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        now = utcnow()
        async with conn.cursor() as cur:
            await cur.executemany(
                """INSERT INTO pattern_suggestions
                   (collection_id,type,match,value,rationale,matches,state,source,created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,'pending',%s,%s) ON CONFLICT DO NOTHING""",
                [(collection_id, r["type"], r["match"], r.get("value"), r.get("rationale"), r.get("matches", 0),
                  r.get("source", "llm"), now) for r in rows],
            )
            return cur.rowcount

    async def add_pattern_suggestions(self, collection_id: str, rows: list[dict[str, Any]]) -> int:
        """Insert pending suggestions; a (type, match) already present — pending from another
        batch, or accepted/rejected earlier — is skipped. Returns how many were new."""
        async with self._conn() as conn:
            return await self._add_pattern_suggestions(conn, collection_id, rows)

    async def replace_pattern_suggestions(self, collection_id: str, rows: list[dict[str, Any]]) -> int:
        async with self._conn() as conn:
            await conn.execute(
                "DELETE FROM pattern_suggestions WHERE collection_id=%s AND state='pending'", (collection_id,)
            )
            return await self._add_pattern_suggestions(conn, collection_id, rows)

    async def count_pending_pattern_suggestions(self, collection_id: str) -> int:
        async with self._conn() as conn:
            return await _scalar(await conn.execute(
                "SELECT COUNT(*) FROM pattern_suggestions WHERE collection_id=%s AND state='pending'", (collection_id,)
            ))

    async def list_pattern_suggestions(
        self, collection_id: str, state: str | None = "pending", *, limit: int | None = None, offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Global-list hits first, then the model's, biggest match count first within each."""
        q = "SELECT * FROM pattern_suggestions WHERE collection_id=%s"
        args: list[Any] = [collection_id]
        if state:
            q += " AND state=%s"; args.append(state)
        q += " ORDER BY (source='global') DESC, matches DESC, id"
        if limit is not None:
            q += " LIMIT %s OFFSET %s"; args += [limit, offset]
        async with self._conn() as conn:
            cur = await conn.execute(q, args)
            return list(await cur.fetchall())

    async def pattern_suggestion_counts(self, collection_id: str) -> dict[str, Any]:
        """Pending suggestions: {"total": n, "by_type": {type: n}} without loading the rows."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT type, COUNT(*) AS n FROM pattern_suggestions WHERE collection_id=%s AND state='pending'"
                " GROUP BY type", (collection_id,),
            )
            by_type = {r["type"]: r["n"] for r in await cur.fetchall()}
        return {"total": sum(by_type.values()), "by_type": by_type}

    async def get_pattern_suggestion(self, collection_id: str, sid: int) -> dict[str, Any] | None:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM pattern_suggestions WHERE id=%s AND collection_id=%s", (sid, collection_id)
            )
            return await cur.fetchone()

    async def set_pattern_suggestion_state(
        self, collection_id: str, sid: int, state: str, *, actor: str | None = None,
        accepted_as: str | None = None,
    ) -> None:
        """`accepted_as` records the glob actually applied when the curator edited it before accepting."""
        async with self._conn() as conn:
            await conn.execute(
                "UPDATE pattern_suggestions SET state=%s, decided_by=%s, accepted_as=COALESCE(%s, accepted_as)"
                " WHERE collection_id=%s AND id=%s",
                (state, actor, accepted_as, collection_id, sid),
            )

    async def set_pattern_suggestions_state(
        self, collection_id: str, ids: list[int], state: str, *, actor: str | None = None
    ) -> int:
        if not ids:
            return 0
        async with self._conn() as conn:
            cur = await conn.execute(
                "UPDATE pattern_suggestions SET state=%s, decided_by=%s WHERE collection_id=%s AND id = ANY(%s)",
                (state, actor, collection_id, [int(i) for i in ids]),
            )
            return cur.rowcount

    # Pending, included URLs the LLM should classify. `only_missing` = never classified, or the
    # page text changed since the model last saw it (content_changed and a different hash).
    _LLM_WHERE = "d.collection_id=%s AND d.kind!='deleted' AND NOT d.excluded"
    # "missing": no suggestion left on the row, the last call failed, the model left a field empty
    # and no rule sets it (answers from before every field was required; a value the SME dismissed
    # has no confidence either and is not re-asked), or the text changed since the answer
    _LLM_MISSING = (" AND ((d.title_ai IS NULL AND d.division_ai IS NULL AND d.document_type_ai IS NULL)"
                    " OR d.ai_error IS NOT NULL"
                    " OR (d.title_ai IS NULL AND d.title_ai_conf IS NOT NULL AND d.title IS NULL)"
                    " OR (d.division_ai IS NULL AND d.division_ai_conf IS NOT NULL AND d.division IS NULL)"
                    " OR (d.document_type_ai IS NULL AND d.document_type_ai_conf IS NOT NULL AND d.document_type IS NULL)"
                    " OR (d.content_changed AND (d.ai_content_hash IS NULL OR d.ai_content_hash != u.content_hash)))")

    async def pending_urls_for_patterns(self, collection_id: str) -> list[tuple[str, str | None]]:
        """(url, scraped_title) of every included delta URL — what Suggest exclusions looks at.
        Same predicate as the metadata job: removed rows are gone anyway and excluded rows are decided."""
        async with self._conn() as conn:
            cur = await conn.execute(
                f"SELECT d.url, d.scraped_title FROM delta_urls d WHERE {self._LLM_WHERE} ORDER BY d.url",
                (collection_id,),
            )
            return [(r["url"], r["scraped_title"]) for r in await cur.fetchall()]

    async def count_deltas_for_llm(self, collection_id: str, *, only_missing: bool = True) -> int:
        q = (f"SELECT COUNT(*) FROM delta_urls d LEFT JOIN dump_urls u ON u.collection_id=d.collection_id"
             f" AND u.url=d.url WHERE {self._LLM_WHERE}") + (self._LLM_MISSING if only_missing else "")
        async with self._conn() as conn:
            return await _scalar(await conn.execute(q, (collection_id,)))

    async def iter_deltas_for_llm(
        self, collection_id: str, *, only_missing: bool = True, chunk: int = 200
    ) -> AsyncIterator[dict[str, Any]]:
        """{url, title, text, content_hash} rows with the FULL page text, streamed in keyset-paginated
        chunks so a 100k-URL collection never sits in memory at once. Each chunk is its own short
        transaction, and rows written by the running job are always behind the cursor, so
        concurrent set_delta_ai calls are safe."""
        q = (f"SELECT d.url, d.scraped_title AS title, u.full_text AS text, u.content_hash"
             f" FROM delta_urls d LEFT JOIN dump_urls u ON u.collection_id=d.collection_id AND u.url=d.url"
             f" WHERE {self._LLM_WHERE}") + (self._LLM_MISSING if only_missing else "")
        last = ""
        while True:
            async with self._conn() as conn:
                cur = await conn.execute(q + " AND d.url > %s ORDER BY d.url LIMIT %s", (collection_id, last, chunk))
                rows = list(await cur.fetchall())
            if not rows:
                return
            for r in rows:
                yield r
            last = rows[-1]["url"]

    async def deltas_for_llm(self, collection_id: str, *, only_missing: bool = True) -> list[dict[str, Any]]:
        return [r async for r in self.iter_deltas_for_llm(collection_id, only_missing=only_missing)]

    async def deltas_with_ai(
        self, collection_id: str, field: str, url: str | None = None, conf: str | None = None,
    ) -> list[tuple[str, str]]:
        """(url, suggested value) for every pending, non-removed URL with an AI suggestion for `field`
        (just that one row when `url` is given; only suggestions of that confidence when `conf` is)."""
        assert field in ("title", "division", "document_type")
        sql = (f"SELECT url, {field}_ai AS v FROM delta_urls WHERE collection_id=%s AND kind!='deleted'"
               f" AND {field}_ai IS NOT NULL")
        args: list[Any] = [collection_id]
        if url is not None:
            sql += " AND url=%s"; args.append(url)
        if conf is not None:
            sql += f" AND {field}_ai_conf=%s"; args.append(conf)
        async with self._conn() as conn:
            cur = await conn.execute(sql + " ORDER BY url", args)
            return [(r["url"], r["v"]) for r in await cur.fetchall()]

    @staticmethod
    def _ai_filter(field: str | None, conf: str | None) -> tuple[str, list[Any]]:
        """Rows with a pending suggestion for `field` (any field when None), of confidence `conf`
        (any when None)."""
        fields = [field] if field in AI_FIELDS else list(AI_FIELDS)
        if conf is None:
            return "(" + " OR ".join(f"{f}_ai IS NOT NULL" for f in fields) + ")", []
        return ("(" + " OR ".join(f"({f}_ai IS NOT NULL AND {f}_ai_conf=%s)" for f in fields) + ")",
                [conf] * len(fields))

    async def list_delta_ai(
        self, collection_id: str, limit: int = 50, offset: int = 0, *,
        field: str | None = None, conf: str | None = None,
    ) -> tuple[list[DeltaUrl], int]:
        """Pending, non-removed delta URLs that carry at least one AI suggestion (for `field`, of
        confidence `conf`, when given), by URL — the review table under Curate › Metadata — and how
        many there are in all."""
        cond, cargs = self._ai_filter(field, conf)
        where, args = f"collection_id=%s AND kind!='deleted' AND {cond}", [collection_id, *cargs]
        async with self._conn() as conn:
            total = await _scalar(await conn.execute(f"SELECT COUNT(*) FROM delta_urls WHERE {where}", args))
            cur = await conn.execute(
                f"SELECT * FROM delta_urls WHERE {where} ORDER BY url LIMIT %s OFFSET %s", [*args, limit, offset],
            )
            return [DeltaUrl(**r) for r in await cur.fetchall()], total

    async def count_ai_suggestions(self, collection_id: str, *, field: str | None = None,
                                   conf: str | None = None) -> int:
        """Pending field-level AI suggestions for `field` (every field when None) of confidence `conf`
        (any when None): what an accept / reject of the filtered review decides."""
        fields = [field] if field in AI_FIELDS else list(AI_FIELDS)
        parts, args = [], []
        for f in fields:
            parts.append(f"COUNT(*) FILTER (WHERE {f}_ai IS NOT NULL" + (f" AND {f}_ai_conf=%s)" if conf else ")"))
            args += [conf] if conf else []
        async with self._conn() as conn:
            return await _scalar(await conn.execute(
                f"SELECT {' + '.join(parts)} FROM delta_urls WHERE collection_id=%s AND kind!='deleted'",
                [*args, collection_id],
            )) or 0

    async def delta_ai_counts(self, collection_id: str) -> dict[str, Any]:
        """Pending AI suggestions per field, plus `by_conf`: suggestions (field-level) per confidence,
        `failed`: included URLs whose last Suggest metadata call failed, and `retitled`: pending AI
        titles regenerated over a title the page shared (Regenerate duplicate titles)."""
        conf = ("COUNT(*) FILTER (WHERE title_ai IS NOT NULL AND title_ai_conf=%s)"
                " + COUNT(*) FILTER (WHERE division_ai IS NOT NULL AND division_ai_conf=%s)"
                " + COUNT(*) FILTER (WHERE document_type_ai IS NOT NULL AND document_type_ai_conf=%s)")
        async with self._conn() as conn:
            cur = await conn.execute(
                f"""SELECT COUNT(title_ai) AS t, COUNT(division_ai) AS d, COUNT(document_type_ai) AS dt,
                           {conf} AS hi, {conf} AS med, {conf} AS lo,
                           COUNT(*) FILTER (WHERE ai_error IS NOT NULL AND NOT excluded) AS failed,
                           COUNT(*) FILTER (WHERE title_ai IS NOT NULL AND title_ai_before IS NOT NULL) AS retitled
                    FROM delta_urls WHERE collection_id=%s AND kind!='deleted'""",
                ("high",) * 3 + ("medium",) * 3 + ("low",) * 3 + (collection_id,),
            )
            r = await cur.fetchone()
        return {"title": r["t"] or 0, "division": r["d"] or 0, "document_type": r["dt"] or 0,
                "by_conf": {"high": r["hi"] or 0, "medium": r["med"] or 0, "low": r["lo"] or 0},
                "failed": r["failed"] or 0, "retitled": r["retitled"] or 0}

    async def clear_delta_ai_field(
        self, collection_id: str, field: str, url: str | None = None, conf: str | None = None,
    ) -> int:
        assert field in ("title", "division", "document_type")
        extra = ", title_ai_before=NULL" if field == "title" else ""
        sql = (f"UPDATE delta_urls SET {field}_ai=NULL, {field}_ai_conf=NULL{extra}"
               f" WHERE collection_id=%s AND {field}_ai IS NOT NULL")
        args: list[Any] = [collection_id]
        if url is not None:
            sql += " AND url=%s"; args.append(url)
        if conf is not None:
            sql += f" AND {field}_ai_conf=%s"; args.append(conf)
        async with self._conn() as conn:
            cur = await conn.execute(sql, args)
            return cur.rowcount

    async def clear_delta_ai(self, collection_id: str, url: str, field: str) -> None:
        assert field in ("title", "division", "document_type")
        async with self._conn() as conn:
            extra = ", title_ai_before=NULL" if field == "title" else ""
            await conn.execute(
                f"UPDATE delta_urls SET {field}_ai=NULL, {field}_ai_conf=NULL{extra} WHERE collection_id=%s AND url=%s",
                (collection_id, url),
            )

    async def incomplete_counts(self, collection_id: str, urls: list[str] | None = None) -> dict[str, int]:
        """Delta URLs promote would refuse (see _INCOMPLETE): how many, and how many lack each field.
        `urls`: only these rows (a promote of a selection)."""
        where, args = f"collection_id=%s AND {_INCOMPLETE}", [collection_id]
        if urls is not None:
            where += " AND url = ANY(%s)"; args.append(list(urls))
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*) AS urls, COUNT(*) FILTER (WHERE btrim(COALESCE(title, ''))='') AS title,"
                " COUNT(*) FILTER (WHERE division IS NULL) AS division,"
                f" COUNT(*) FILTER (WHERE document_type IS NULL) AS document_type FROM delta_urls WHERE {where}", args,
            )
            r = await cur.fetchone()
        return {k: r[k] or 0 for k in ("urls", "title", "division", "document_type")}

    async def set_delta_ai_titles(self, collection_id: str, items: list[dict[str, Any]]) -> int:
        """Replace just the AI title suggestion (value, confidence, model) of these rows: the other
        fields' suggestions and any recorded failure stay as they are. `before`: the title the row
        shared with other pages — kept as `title_ai_before`, unless an earlier retitle already kept one
        (the first title stays: it is the one the SME needs to see)."""
        if not items:
            return 0
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.executemany(
                "UPDATE delta_urls SET title_ai=%s, title_ai_conf=%s, ai_model=%s,"
                " title_ai_before=COALESCE(title_ai_before, %s) WHERE collection_id=%s AND url=%s",
                [(i["title"], i.get("title_conf"), i.get("model"), i.get("before"), collection_id, i["url"])
                 for i in items],
            )
        return len(items)

    async def docs_for_llm(self, collection_id: str, urls: list[str]) -> list[dict[str, Any]]:
        """{url, title, text, content_hash} of these delta URLs, with the full page text."""
        if not urls:
            return []
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT d.url, d.scraped_title AS title, u.full_text AS text, u.content_hash"
                " FROM delta_urls d LEFT JOIN dump_urls u ON u.collection_id=d.collection_id AND u.url=d.url"
                " WHERE d.collection_id=%s AND d.url = ANY(%s) ORDER BY d.url",
                (collection_id, list(urls)),
            )
            return list(await cur.fetchall())

    async def duplicate_title_counts(self, collection_id: str) -> dict[str, int]:
        """URLs whose title and document type another page of the collection will also have (see
        _PROJECTED_TITLES), how many distinct title + type combinations they share (`titles`), and
        how many of those URLs are delta URLs (the ones a suggestion can still change)."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*) AS urls, COUNT(DISTINCT k) AS titles, COUNT(*) FILTER (WHERE delta) AS delta_urls"
                f" FROM ({_DUPLICATE_TITLES}) g", (collection_id, collection_id),
            )
            r = await cur.fetchone()
        return {"urls": r["urls"] or 0, "titles": r["titles"] or 0, "delta_urls": r["delta_urls"] or 0}

    async def duplicate_title_groups(self, collection_id: str) -> list[dict[str, Any]]:
        """Every title + document type shared by more than one page:
        {"title", "document_type", "members": [{url, delta, pending_ai}]}, members by URL."""
        async with self._conn() as conn:
            cur = await conn.execute(f"SELECT * FROM ({_DUPLICATE_TITLES}) g ORDER BY k, url",
                                     (collection_id, collection_id))
            rows = await cur.fetchall()
        groups: dict[str, dict[str, Any]] = {}
        for r in rows:
            g = groups.setdefault(r["k"], {"title": r["title"], "document_type": r["document_type"], "members": []})
            g["members"].append({"url": r["url"], "delta": r["delta"], "pending_ai": r["pending_ai"]})
        return list(groups.values())

    async def duplicate_titles_for(self, collection_id: str, urls: list[str]) -> dict[str, dict[str, Any]]:
        """For the rows on a page: url -> {"title", "document_type", "others": how many other pages
        share both, "sample": up to 5 of those URLs}. Rows whose combination is not shared are absent."""
        if not urls:
            return {}
        async with self._conn() as conn:
            cur = await conn.execute(
                f"WITH dup AS (SELECT g.*, row_number() OVER (PARTITION BY k ORDER BY url) AS rn FROM ({_DUPLICATE_TITLES}) g)"
                " SELECT url, title, document_type, k, n FROM dup WHERE k IN (SELECT k FROM dup WHERE url = ANY(%s))"
                " AND (rn <= 6 OR url = ANY(%s)) ORDER BY k, url",
                (collection_id, collection_id, list(urls), list(urls)),
            )
            rows = await cur.fetchall()
        by_key: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            by_key.setdefault(r["k"], []).append(r)
        wanted, out = set(urls), {}
        for group in by_key.values():
            for r in group:
                if r["url"] in wanted:
                    out[r["url"]] = {"title": r["title"], "document_type": r["document_type"], "others": r["n"] - 1,
                                     "sample": [o["url"] for o in group if o["url"] != r["url"]][:5]}
        return out

    # ── patterns ───────────────────────────────────────────────────────

    async def insert_pattern(self, p: Pattern) -> Pattern:
        """Raises ConflictError when the collection already has that (type, match) rule."""
        try:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "INSERT INTO patterns (collection_id,type,match,value,created_at,created_by,source)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                    (p.collection_id, p.type, p.match, p.value, p.created_at, p.created_by, p.source),
                )
                p.id = (await cur.fetchone())["id"]
        except psycopg.errors.UniqueViolation as e:
            raise ConflictError(f"{p.type} rule {p.match!r} already exists") from e
        return p

    async def insert_patterns(self, rows: list[Pattern]) -> int:
        """Bulk insert; rows identical to an existing (type, match) are skipped. One transaction."""
        if not rows:
            return 0
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO patterns (collection_id,type,match,value,created_at,created_by,source)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                [(p.collection_id, p.type, p.match, p.value, p.created_at, p.created_by, p.source) for p in rows],
            )
            return cur.rowcount

    async def delete_exact_patterns(self, collection_id: str, type_: str, matches: list[str]) -> int:
        if not matches:
            return 0
        async with self._conn() as conn:
            cur = await conn.execute(
                "DELETE FROM patterns WHERE collection_id=%s AND type=%s AND match = ANY(%s)",
                (collection_id, type_, list(matches)),
            )
            return cur.rowcount

    async def list_patterns(self, collection_id: str) -> list[Pattern]:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM patterns WHERE collection_id=%s ORDER BY id", (collection_id,))
            return [Pattern(**r) for r in await cur.fetchall()]

    async def delete_pattern(self, collection_id: str, pattern_id: int) -> bool:
        async with self._conn() as conn:
            cur = await conn.execute(
                "DELETE FROM patterns WHERE id=%s AND collection_id=%s", (pattern_id, collection_id)
            )
            return cur.rowcount > 0

    # ── jobs ───────────────────────────────────────────────────────────

    async def insert_job(self, j: JobRun) -> JobRun:
        async with self._conn() as conn:
            cur = await conn.execute(
                """INSERT INTO job_runs (collection_id,kind,state,run_id,external_ref,progress,error,
                   started_at,finished_at,started_by) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    j.collection_id, j.kind, j.state, j.run_id, j.external_ref,
                    Jsonb(j.progress), j.error, j.started_at, j.finished_at, j.started_by,
                ),
            )
            j.id = (await cur.fetchone())["id"]
        return j

    async def update_job(self, j: JobRun) -> None:
        async with self._conn() as conn:
            await conn.execute(
                """UPDATE job_runs SET state=%s, run_id=%s, external_ref=%s, progress=%s, error=%s,
                   finished_at=%s WHERE id=%s""",
                (j.state, j.run_id, j.external_ref, Jsonb(j.progress), j.error, j.finished_at, j.id),
            )

    async def finish_job(self, j: JobRun, state: JobState, error: str | None = None) -> None:
        j.state, j.error, j.finished_at = state, error, utcnow()
        await self.update_job(j)

    @staticmethod
    def _job(row: dict[str, Any]) -> JobRun:
        d = dict(row)
        d["progress"] = d["progress"] or {}
        return JobRun(**d)

    async def get_job(self, job_id: int) -> JobRun | None:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM job_runs WHERE id=%s", (job_id,))
            row = await cur.fetchone()
        return self._job(row) if row else None

    async def list_jobs(self, collection_id: str, limit: int = 20) -> list[JobRun]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM job_runs WHERE collection_id=%s ORDER BY id DESC LIMIT %s", (collection_id, limit)
            )
            return [self._job(r) for r in await cur.fetchall()]

    async def latest_job(self, collection_id: str) -> JobRun | None:
        jobs = await self.list_jobs(collection_id, limit=1)
        return jobs[0] if jobs else None

    async def latest_job_of_kind(self, collection_id: str, kind: str) -> JobRun | None:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM job_runs WHERE collection_id=%s AND kind=%s ORDER BY id DESC LIMIT 1",
                (collection_id, str(kind)),
            )
            row = await cur.fetchone()
        return self._job(row) if row else None

    async def job_exists(self, collection_id: str, kind: str) -> bool:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT 1 FROM job_runs WHERE collection_id=%s AND kind=%s LIMIT 1", (collection_id, str(kind))
            )
            return await cur.fetchone() is not None

    async def active_jobs(self) -> list[JobRun]:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM job_runs WHERE state IN ('queued','running') ORDER BY id")
            return [self._job(r) for r in await cur.fetchall()]

    async def list_recent_jobs(self, limit: int = 20, state: str | None = None) -> list[JobRun]:
        """Newest jobs across every collection, optionally only one state (e.g. 'failed')."""
        async with self._conn() as conn:
            if state:
                cur = await conn.execute(
                    "SELECT * FROM job_runs WHERE state=%s ORDER BY id DESC LIMIT %s", (state, limit)
                )
            else:
                cur = await conn.execute("SELECT * FROM job_runs ORDER BY id DESC LIMIT %s", (limit,))
            return [self._job(r) for r in await cur.fetchall()]

    # ── users ──────────────────────────────────────────────────────────

    @staticmethod
    def _user(row: dict[str, Any]) -> User:
        return User(**row)

    async def create_user(self, username: str, password_hash: str, role: Role = Role.CURATOR) -> User:
        """Raises ConflictError when the username (case-insensitive) is taken."""
        now = utcnow()
        try:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "INSERT INTO users (username,password_hash,role,active,session_version,created_at,updated_at) "
                    "VALUES (%s,%s,%s,true,1,%s,%s) RETURNING *",
                    (username, password_hash, role, now, now),
                )
                row = await cur.fetchone()
        except psycopg.errors.UniqueViolation as e:
            raise ConflictError(f"username {username!r} is taken") from e
        return self._user(row)

    async def get_user(self, user_id: int) -> User | None:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM users WHERE id=%s", (user_id,))
            row = await cur.fetchone()
        return self._user(row) if row else None

    async def get_user_by_username(self, username: str) -> User | None:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM users WHERE lower(username)=lower(%s)", (username,))
            row = await cur.fetchone()
        return self._user(row) if row else None

    async def list_users(self) -> list[User]:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT * FROM users ORDER BY lower(username)")
            return [self._user(r) for r in await cur.fetchall()]

    async def count_users(self) -> int:
        async with self._conn() as conn:
            return await _scalar(await conn.execute("SELECT COUNT(*) FROM users"))

    async def set_password(self, user_id: int, password_hash: str) -> None:
        """Also invalidates every existing session of that user."""
        async with self._conn() as conn:
            await conn.execute(
                "UPDATE users SET password_hash=%s, session_version=session_version+1, updated_at=%s WHERE id=%s",
                (password_hash, utcnow(), user_id),
            )

    async def set_role(self, user_id: int, role: Role) -> None:
        async with self._conn() as conn:
            await conn.execute(
                "UPDATE users SET role=%s, updated_at=%s WHERE id=%s", (role, utcnow(), user_id)
            )

    async def set_active(self, user_id: int, active: bool) -> None:
        async with self._conn() as conn:
            await conn.execute(
                "UPDATE users SET active=%s, updated_at=%s WHERE id=%s", (active, utcnow(), user_id)
            )

    # ── audit ledger ───────────────────────────────────────────────────

    async def audit(
        self, actor: str, action: str, collection_id: str | None = None, detail: str | None = None
    ) -> None:
        async with self._conn() as conn:
            await conn.execute(
                "INSERT INTO audit_log (at,actor,collection_id,action,detail) VALUES (%s,%s,%s,%s,%s)",
                (utcnow(), actor, collection_id, action, detail),
            )

    async def list_audit(
        self, collection_id: str | None = None, limit: int = 100, *, q: str | None = None, before: int | None = None,
        sort: str | None = None, desc: bool = True, offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Newest first by default; `sort` (an AUDIT_SORTS key) + `desc` order by another column,
        with `offset` for paging since `before` only pages by id. audit_log has no foreign key on collections on purpose: rows outlive the
        collection they name, so the global ledger still shows what was done to a deleted one.
        `q` matches actor, action, collection id or detail (case-insensitive substring); `before`
        pages by row id (the id of the last row shown)."""
        where, args = [], []
        if collection_id is not None:
            where.append("collection_id=%s"); args.append(collection_id)
        if q:
            where.append("(actor ILIKE %s OR action ILIKE %s OR collection_id ILIKE %s OR detail ILIKE %s)")
            args += [f"%{q}%"] * 4
        if before is not None:
            where.append("id < %s"); args.append(before)
        sql = ("SELECT * FROM audit_log" + (" WHERE " + " AND ".join(where) if where else "")
               + order_by(AUDIT_SORTS, sort, desc, "id DESC") + " LIMIT %s OFFSET %s")
        async with self._conn() as conn:
            cur = await conn.execute(sql, (*args, limit, offset))
            return list(await cur.fetchall())
