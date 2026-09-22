"""PostgreSQL state store (psycopg 3, async pool). Every public method is one transaction: the
pool hands out a connection whose context commits on success and rolls back on any exception.
The schema lives in `schema.py` as numbered migrations applied at connect()."""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from datetime import datetime
from typing import Any

import psycopg
from psycopg import AsyncConnection
from psycopg.rows import dict_row, tuple_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .engine.patterns import glob_to_like, is_exact
from .engine.text import content_hash
from .engine.urls import canonical_key, duplicate_docs, spellings
from .models import (
    Collection,
    CuratedUrl,
    CurationStage,
    DeltaUrl,
    Division,
    DumpFailure,
    DumpUrl,
    IndexRun,
    JobRun,
    JobState,
    Pattern,
    PatternType,
    Role,
    Rule,
    RuleSource,
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


async def _aiter[T](rows: Iterable[T] | AsyncIterable[T]) -> AsyncIterator[T]:
    if isinstance(rows, AsyncIterable):
        async for r in rows:
            yield r
    else:
        for r in rows:
            yield r


# After a bulk write the planner still believes the table is as small as before until autovacuum
# analyses it (up to a minute later). A join over it planned in that window nested-looped
# 100k × 100k rows and ran for ten minutes (the Suggest-metadata pre-check, seconds after an ingest).
# So bulk writes refresh the statistics of the columns the joins use, inside their transaction.
_BULK_ROWS = 5000


async def _scalar(cur: psycopg.AsyncCursor) -> Any:
    row = await cur.fetchone()
    return None if row is None else next(iter(row.values()))


# Every curated column. The page text is not one of them: since V9 it lives once in `page_text`,
# keyed by the `content_hash` the dump and the curated set both carry, and is fetched by
# `_page_text` only where it is actually wanted (the export) — it is most of the bytes.
_CURATED_COLS = ("collection_id,url,scraped_title,title,division,document_type,excluded,content_hash,edited_by,"
                 "crawl_failure")


def _page_text(table: str, expr: str = "p.full_text", alias: str = "full_text") -> str:
    """The page text `table`'s content hash points at, as a scalar subquery. A subquery and not a
    join so a caller can keep writing its WHERE and ORDER BY against the single table it selects
    from; `page_text`'s primary key makes it one index lookup per row, and every caller either
    paginates or streams in chunks. NULL hash (an empty page) gives NULL, as the column did."""
    return (f"(SELECT {expr} FROM page_text p WHERE p.collection_id={table}.collection_id"
            f" AND p.content_hash={table}.content_hash) AS {alias}")


# The dump text joined onto a query that already has `dump_urls u` — the LLM queries, which are
# fully qualified and want the text of a delta URL's dump row.
_TEXT_JOIN = (" LEFT JOIN page_text p ON p.collection_id=u.collection_id"
              " AND p.content_hash=u.content_hash")


# ── column sorting ────────────────────────────────────────────────────
# Base URLs first. Plain lexicographic order on the whole URL reads nothing like a site: it puts a
# deep page above a shallower sibling branch (…/data/aerosol/access before …/data/ozone, because
# "a" < "o"), and the seed only lands first because it happens to be the shortest prefix. Order by
# host, then by how deep the path is (query string and a trailing slash never count), then
# alphabetically — so https://x/data comes before https://x/data/ozone and before
# https://x/images/gallery/aurora, and each branch is read from its base down.
def url_order(col: str = "url") -> tuple[str, ...]:
    path = f"rtrim(split_part({col}, '?', 1), '/')"
    return (f"split_part({col}, '/', 3)", f"length({path}) - length(replace({path}, '/', ''))", col)


def url_order_sql(col: str = "url") -> str:
    return ", ".join(url_order(col))


# Sortable columns per table, keyed by the name the UI sends (?sort=…). Each maps to one or more
# SQL expressions; an unknown key falls back to the table's default order, so user input never
# reaches the SQL. The default order is always appended as the tiebreak so paging stays stable.
DUMP_SORTS: dict[str, tuple[str, ...]] = {
    "url": url_order("d.url"), "scraped_title": ("d.scraped_title",), "content_type": ("d.content_type",),
    "depth": ("d.depth",), "text_len": ("text_len",), "excluded": ("excluded",),
    "vs_curated": ("in_deltas", "in_curated"),
}
# A delta row is shown with its pending AI suggestions as if they had been accepted (see
# _projected_titles, pending=True), and a sorted column sorts by exactly what is shown: the
# projected value, never the stored one. Accepting a suggestion then writes the value the row was
# already sorted under, so ✓ leaves the row where the curator is looking at it. Sorting on the
# stored value instead put every undecided row in the NULLS LAST block, tied, and the first accept
# sent that one row to the top of the list while everything above it shifted down.
_P_TITLE = "COALESCE(NULLIF(btrim(title), ''), title_ai, scraped_title)"
_P_DIVISION = "COALESCE(division, division_ai)"
_P_DOCUMENT_TYPE = "COALESCE(document_type, document_type_ai)"
# edited_by once the pending suggestions are accepted (models.edited_by_of): a suggestion makes the
# row 'ai', or 'mixed' where a curator has already set a field of it.
_P_EDITED_BY = (
    "CASE WHEN title_ai IS NULL AND division_ai IS NULL AND document_type_ai IS NULL THEN edited_by"
    " WHEN edited_by IN ('sme', 'mixed') THEN 'mixed' ELSE 'ai' END"
)
DELTA_SORTS: dict[str, tuple[str, ...]] = {
    "kind": ("kind",), "url": url_order(), "excluded": ("excluded",), "title": (_P_TITLE,),
    "division": (_P_DIVISION,), "document_type": (_P_DOCUMENT_TYPE,), "edited_by": (_P_EDITED_BY,),
}
CURATED_SORTS: dict[str, tuple[str, ...]] = {
    "url": url_order(), "excluded": ("excluded",), "title": ("COALESCE(title, scraped_title)",),
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
# delta URLs are promoted; the title falls back to the scraped title (the export's fallback). A
# curated row that a delta row stands in for (same URL, a rename or a removal) counts once, as the
# delta row. Two pages are duplicates when BOTH match: the title ignoring case and runs of
# whitespace, and the document type (two pages with the same title but different types are told
# apart by the type). Every query below takes the collection id twice.
#
# Two views of the same thing, because a pending suggestion is a proposal, not a value:
#   pending=True   what the review tables show — every AI suggestion as if it had been accepted
#                  (that is the collision the curator is deciding about)
#   pending=False  what promote would actually write, which discards undecided suggestions: the
#                  set the promote gate refuses on
def _projected_titles(*, pending: bool) -> str:
    title = ("COALESCE(d.title_ai, NULLIF(btrim(d.title), ''), d.scraped_title)" if pending
             else "COALESCE(NULLIF(btrim(d.title), ''), d.scraped_title)")
    doc = "COALESCE(d.document_type_ai, d.document_type)" if pending else "d.document_type"
    flags = ("d.title_ai IS NOT NULL AS pending_ai, d.title_ai_before AS shared_before" if pending
             else "false AS pending_ai, NULL::text AS shared_before")
    return f"""
SELECT url, delta, pending_ai, shared_before, title, document_type,
       lower(regexp_replace(btrim(title), '\\s+', ' ', 'g')) || chr(31) || COALESCE(document_type, '') AS k FROM (
  SELECT d.url, true AS delta, {flags},
         {title} AS title,
         {doc} AS document_type
    FROM delta_urls d WHERE d.collection_id=%s AND d.kind!='deleted' AND NOT d.excluded
  UNION ALL
  SELECT c.url, false, false, NULL::text, COALESCE(NULLIF(btrim(c.title), ''), c.scraped_title), c.document_type
    FROM curated_urls c WHERE c.collection_id=%s AND NOT c.excluded
     AND NOT EXISTS (SELECT 1 FROM delta_urls x WHERE x.collection_id=c.collection_id AND x.url=c.url)
     AND NOT EXISTS (SELECT 1 FROM delta_urls x WHERE x.collection_id=c.collection_id AND x.renamed_from=c.url)
) p WHERE btrim(COALESCE(title, '')) != ''"""


_PROJECTED_TITLES = _projected_titles(pending=True)
_EFFECTIVE_TITLES = _projected_titles(pending=False)
# A delta URL that promote would write into the curated set without a title, a division or a
# document type (its effective values: rules or the curated row, never a pending suggestion).
# Removals and excluded rows carry no metadata to the index, so they never count.
# `division='General'` counts as no division: General is the placeholder a collection carries until
# a curator assigns one (models.Division.GENERAL), so a row still on it has no division decided and
# must not reach the index.
def _no_division(col: str = "division") -> str:
    return f"({col} IS NULL OR {col} = 'General')"


# No title at all — not even a scraped one. A row with no title rule is still indexed under the
# title the crawl read off the page (engine.export.export_lines falls back to it), so it is not
# blank and promote must not hold it back; only a page the crawler found untitled is.
def _no_title(title: str = "title", scraped: str = "scraped_title") -> str:
    return f"btrim(COALESCE(NULLIF(btrim({title}), ''), {scraped}, ''))=''"


_NO_DIVISION = _no_division()
_NO_TITLE = _no_title()
_INCOMPLETE = ("kind!='deleted' AND NOT excluded"
               f" AND ({_NO_TITLE} OR {_NO_DIVISION} OR document_type IS NULL)")

_DUPLICATE_TITLES = (f"SELECT * FROM (SELECT t.*, COUNT(*) OVER (PARTITION BY k) AS n FROM ({_PROJECTED_TITLES}) t) w"
                     " WHERE n > 1")
# The delta URLs promote would write into the curated set under a title + document type another
# page of the collection already has (or would have). Two pages a search cannot tell apart is not
# something the index may be left holding, so promote refuses these exactly as it refuses a blank
# — undecided AI titles are not counted, because promote discards them.
_DUPLICATE_EFFECTIVE = ("SELECT url FROM (SELECT t.*, COUNT(*) OVER (PARTITION BY k) AS n FROM"
                        f" ({_EFFECTIVE_TITLES}) t) w WHERE n > 1 AND delta")
# Everything promote refuses, in one predicate: blank fields or a shared title. Takes the
# collection id twice (the duplicate scan), then whatever the clause it is used in takes.
_UNPROMOTABLE = f"(({_INCOMPLETE}) OR url IN ({_DUPLICATE_EFFECTIVE}))"


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

    async def set_division(self, collection_id: str, division: Division) -> None:
        """The curator's division for the whole collection (General = not assigned, so the AI is
        asked per page). The next recompute applies it to every URL no division rule decides."""
        async with self._conn() as conn:
            await conn.execute(
                "UPDATE collections SET division=%s, updated_at=%s WHERE collection_id=%s",
                (division.value, utcnow(), collection_id),
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
        self, collection_id: str, rows: Iterable[DumpUrl] | AsyncIterable[DumpUrl],
        failures: list[DumpFailure] | None = None, *, dedupe_spellings: bool = False,
    ) -> int:
        """Bulk-replace the dump for a collection in one transaction; returns row count. `failures`
        are the URLs the crawler tried and could not fetch (replaced along with the dump: the two
        together are what one crawl found out).

        `dedupe_spellings` keeps one row per page where the crawl found several spellings of it
        (`engine.urls.duplicate_docs`). That is a judgement about crawler output, so only the
        ingest asks for it — `JobRunner.ingest_dump`, which needs it done here because the decision
        wants every URL of the crawl and the crawl is a forward-only stream. A caller that has
        already decided what the dump is writes it as given."""
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
            # The crawl arrives in one COPY, into a temp table (not WAL-logged, dropped on commit),
            # because it has to reach two places and be filtered on the way: `page_text` once per
            # distinct content hash, `dump_urls` as the hash alone, and only for the URLs that
            # survive `duplicate_docs`. Staging it here is what lets the caller stream the crawl
            # from S3 exactly once — the duplicate pass used to be a second read of the whole
            # file, for two fields it can just as well read back from this table.
            await conn.execute(
                "CREATE TEMP TABLE dump_in (seq bigint, url text, final_url text, scraped_title text,"
                " full_text text, content_type text, depth integer, content_hash text) ON COMMIT DROP"
            )
            async with conn.cursor() as cur, cur.copy(
                "COPY dump_in (seq,url,final_url,scraped_title,full_text,content_type,depth,content_hash)"
                " FROM STDIN"
            ) as copy:
                seen: set[str] = set()
                seq = 0
                async for r in _aiter(rows):
                    if r.url in seen:  # the SQLite version did INSERT OR REPLACE
                        continue
                    seen.add(r.url)
                    await copy.write_row((
                        seq, r.url, r.final_url, r.scraped_title, r.full_text, r.content_type, r.depth,
                        r.content_hash or content_hash(r.full_text),
                    ))
                    seq += 1
            # One row per page: a site that links the same page as http and https, with and without
            # a trailing slash, with a #fragment, or under two paths that redirect to one gets it
            # crawled once per spelling, and only the preferred spelling is kept. Reading the two
            # URL columns back costs a scan of the narrow columns; re-reading the crawl cost a
            # second pass over every page's text.
            drop: list[int] = []
            if dedupe_spellings:
                cur = await conn.execute("SELECT seq, url, final_url FROM dump_in ORDER BY seq")
                seen_urls = [(r["seq"], r["url"], r["final_url"]) for r in await cur.fetchall()]
                dupes = await asyncio.to_thread(
                    duplicate_docs, [{"url": u, "final_url": f} for _, u, f in seen_urls]
                )
                drop = [seen_urls[i][0] for i in sorted(dupes)]
                if drop:
                    log.info("dump %s: dropping %d documents that are another URL of a page also present",
                             collection_id, len(drop))
            # Filtered on the way out rather than deleted first: both statements scan the staging
            # table anyway, so the duplicates cost nothing extra to leave behind.
            keep = " AND NOT (seq = ANY(%s))"
            # Pages that share their text share one blob, here and with whatever the curated set
            # already holds (DO NOTHING: same hash, same normalised text, so the copy on disk is
            # as good as this one).
            await conn.execute(
                "INSERT INTO page_text (collection_id, content_hash, full_text)"
                " SELECT %s, content_hash, full_text FROM dump_in"
                " WHERE content_hash IS NOT NULL AND full_text IS NOT NULL" + keep +
                " ON CONFLICT (collection_id, content_hash) DO NOTHING",
                (collection_id, drop),
            )
            await conn.execute(
                "INSERT INTO dump_urls (collection_id,url,scraped_title,content_type,depth,content_hash)"
                " SELECT %s,url,scraped_title,content_type,depth,content_hash FROM dump_in"
                " WHERE true" + keep,
                (collection_id, drop),
            )
            n = await _scalar(await conn.execute(
                "SELECT COUNT(*) FROM dump_urls WHERE collection_id=%s", (collection_id,)
            ))
            if n >= _BULK_ROWS:
                await conn.execute("ANALYZE dump_urls (collection_id, url, content_hash)")
            # the crawl this replaced may have been the only holder of some pages' text
            await self._gc_page_text(conn, collection_id)
            await conn.execute(
                "UPDATE collections SET dump_count=%s, updated_at=%s WHERE collection_id=%s",
                (n, utcnow(), collection_id),
            )
            return n

    @staticmethod
    async def _gc_page_text(conn, collection_id: str) -> int:
        """Drop the collection's page text that neither its dump nor its curated set points at any
        more, and return how many blobs went. Runs inside the transaction that dropped the last
        reference, so the text of a page is never visible as missing to a row that still wants it:
        a curated row keeps the text it was approved with for exactly as long as it holds the hash
        (`engine.diff.promote`), whatever the newest crawl says about that URL."""
        return await _scalar(await conn.execute(
            "WITH gone AS (DELETE FROM page_text p WHERE p.collection_id=%s"
            " AND NOT EXISTS (SELECT 1 FROM dump_urls d"
            "                 WHERE d.collection_id=p.collection_id AND d.content_hash=p.content_hash)"
            " AND NOT EXISTS (SELECT 1 FROM curated_urls c"
            "                 WHERE c.collection_id=p.collection_id AND c.content_hash=p.content_hash)"
            " RETURNING 1) SELECT COUNT(*) FROM gone",
            (collection_id,),
        ))

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
                           (SELECT length(p.full_text) FROM page_text p
                            WHERE p.collection_id=d.collection_id AND p.content_hash=d.content_hash) AS text_len,
                           (c.url IS NOT NULL)::int AS in_curated, (x.url IS NOT NULL)::int AS in_deltas,
                           {excl} AS excluded,
                           COALESCE(x.edited_by, c.edited_by) AS edited_by
                    FROM dump_urls d
                    {joins}
                    WHERE {w}{order_by(DUMP_SORTS, sort, desc, url_order_sql("d.url"))} LIMIT %s OFFSET %s""",
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
                f"SELECT {_CURATED_COLS}, {_page_text('curated_urls', 'length(p.full_text)', 'text_len')}"
                f" FROM curated_urls WHERE {w}"
                f"{order_by(CURATED_SORTS, sort, desc, url_order_sql())} LIMIT %s OFFSET %s", [*args, limit, offset]
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

    async def effect_counts(self, collection_id: str, ids: list[int] | None = None) -> dict[int, int]:
        """pattern_id -> how many URLs the rule currently decides. pattern_effects holds only the
        winner per (url, field) and a rule has one type, so COUNT(*) is a URL count; it is as of
        the last recompute (a promote keeps the effects)."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT pattern_id, COUNT(*) AS n FROM pattern_effects WHERE collection_id=%s"
                + ("" if ids is None else " AND pattern_id = ANY(%s)") + " GROUP BY pattern_id",
                (collection_id,) if ids is None else (collection_id, list(ids)),
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
        `dup_title`: rows whose title and document type another page of the collection will also have
        (counting pending AI suggestions as accepted);
        `retitled`: rows whose duplicate AI title was regenerated (the title they shared is kept);
        `incomplete`: rows promote refuses — no title, division or document type yet, or a title
        another page would be indexed under too."""
        where, args = ["collection_id=%s"], [collection_id]
        if dup_title:
            where.append(f"url IN (SELECT url FROM ({_DUPLICATE_TITLES}) g WHERE delta)"); args += [collection_id] * 2
        if retitled:
            where.append("title_ai IS NOT NULL AND title_ai_before IS NOT NULL")
        if incomplete:
            where.append(_UNPROMOTABLE); args += [collection_id] * 2
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
                f"SELECT * FROM delta_urls WHERE {w}{order_by(DELTA_SORTS, sort, desc, f'kind, {url_order_sql()}')}"
                " LIMIT %s OFFSET %s",
                [*args, limit, offset],
            )
            return [DeltaUrl(**r) for r in await cur.fetchall()], total

    _DELTA_COLS = (
        "collection_id", "url", "kind", "renamed_from", "crawl_failure", "scraped_title", "title", "division",
        "document_type", "excluded", "content_changed", "edited_by", "title_ai", "division_ai", "document_type_ai",
        "title_ai_conf", "division_ai_conf", "document_type_ai_conf", "ai_model", "ai_content_hash", "ai_error",
        "ai_failures", "title_ai_before", "division_skipped",
    )

    async def replace_deltas(
        self, collection_id: str, deltas: list[DeltaUrl], effects: list[tuple[int, str, str]],
        *, keep_effects: bool = False,
    ) -> None:
        """Make the delta URLs (and, unless `keep_effects`, the rule→URL effects) equal to the given
        state. Promote keeps the effects: the rules did not change, and the Curated table still
        explains its values. The recompute always hands over the complete new state; only the rows
        that differ from the table are written (an inline edit changes one row of 100k, and
        rewriting them all was most of what the edit cost)."""
        cols = self._DELTA_COLS
        data = [c for c in cols if c not in ("collection_id", "url")]
        async with self._conn() as conn, conn.cursor() as cur:
            written = 0
            if not deltas:
                await cur.execute("DELETE FROM delta_urls WHERE collection_id=%s", (collection_id,))
                written += cur.rowcount
            else:
                await cur.execute("CREATE TEMP TABLE delta_in (LIKE delta_urls INCLUDING DEFAULTS) ON COMMIT DROP")
                async with cur.copy(f"COPY delta_in ({','.join(cols)}) FROM STDIN") as copy:
                    for d in deltas:
                        await copy.write_row(tuple(getattr(d, c) for c in cols))
                # indexed + analysed: the anti-join below must be a lookup per row whatever plan is
                # chosen — without the index a generic (prepared) plan nested-looped 100k × 100k rows
                await cur.execute("CREATE INDEX ON delta_in (url)")
                await cur.execute("ANALYZE delta_in")
                await cur.execute(
                    "DELETE FROM delta_urls d WHERE d.collection_id=%s"
                    " AND NOT EXISTS (SELECT 1 FROM delta_in i WHERE i.url=d.url)", (collection_id,),
                )
                written += cur.rowcount
                await cur.execute(
                    f"INSERT INTO delta_urls ({','.join(cols)}) SELECT {','.join(cols)} FROM delta_in"
                    " ON CONFLICT (collection_id, url) DO UPDATE SET "
                    + ", ".join(f"{c}=EXCLUDED.{c}" for c in data)
                    + f" WHERE ({','.join('delta_urls.' + c for c in data)})"
                      f" IS DISTINCT FROM ({','.join('EXCLUDED.' + c for c in data)})"
                )
                written += cur.rowcount
            if written >= _BULK_ROWS:
                await cur.execute("ANALYZE delta_urls (collection_id, url, kind, excluded)")
            if not keep_effects:
                if not effects:
                    await cur.execute("DELETE FROM pattern_effects WHERE collection_id=%s", (collection_id,))
                else:
                    await cur.execute(
                        "CREATE TEMP TABLE effects_in (pattern_id bigint, url text, field text) ON COMMIT DROP"
                    )
                    async with cur.copy("COPY effects_in (pattern_id,url,field) FROM STDIN") as copy:
                        for row in set(effects):
                            await copy.write_row(row)
                    await cur.execute("CREATE INDEX ON effects_in (pattern_id, url, field)")
                    await cur.execute("ANALYZE effects_in")
                    await cur.execute(
                        "DELETE FROM pattern_effects e WHERE e.collection_id=%s AND NOT EXISTS (SELECT 1 FROM"
                        " effects_in i WHERE i.pattern_id=e.pattern_id AND i.url=e.url AND i.field=e.field)",
                        (collection_id,),
                    )
                    changed = cur.rowcount
                    await cur.execute(
                        "INSERT INTO pattern_effects (pattern_id,collection_id,url,field)"
                        " SELECT pattern_id,%s,url,field FROM effects_in ON CONFLICT DO NOTHING", (collection_id,),
                    )
                    if changed + cur.rowcount >= _BULK_ROWS:
                        await cur.execute("ANALYZE pattern_effects (pattern_id, collection_id, url)")
            await cur.execute(
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
                   ai_model=%s, ai_content_hash=%s, ai_error=NULL, ai_failures=0, title_ai_before=NULL,
                   division_skipped=%s
                   WHERE collection_id=%s AND url=%s""",
                [(i.get("title"), i.get("division"), i.get("document_type"),
                  i.get("title_conf"), i.get("division_conf"), i.get("document_type_conf"),
                  i.get("model"), i.get("content_hash"), bool(i.get("division_skipped")),
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
        cols = f"{_CURATED_COLS}, {_page_text('curated_urls')}" if with_text else _CURATED_COLS
        async with self._conn() as conn:
            cur = await conn.execute(f"SELECT {cols} FROM curated_urls WHERE collection_id=%s", (collection_id,))
            return [CuratedUrl(**r) for r in await cur.fetchall()]

    async def iter_curated_for_export(self, collection_id: str, chunk: int = 500) -> AsyncIterator[list[CuratedUrl]]:
        """The exportable curated rows (not excluded) with the text they were approved with, in URL
        order (code-point order, as sorted() gives), `chunk` at a time: the URLs first, then each
        page of rows by primary key in its own short transaction — the consumer is slow (it writes
        the file), and a cursor held open for the whole export would keep one of the pool's few
        connections from every request. Nothing edits the curated set during an index job."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT url FROM curated_urls WHERE collection_id=%s AND NOT excluded", (collection_id,)
            )
            urls = sorted(r["url"] for r in await cur.fetchall())
        for i in range(0, len(urls), chunk):
            page = urls[i:i + chunk]
            async with self._conn() as conn:
                cur = await conn.execute(
                    f"SELECT {_CURATED_COLS}, {_page_text('curated_urls')} FROM curated_urls"
                    " WHERE collection_id=%s AND url = ANY(%s)", (collection_id, page)
                )
                rows = {r["url"]: r for r in await cur.fetchall()}
            yield [CuratedUrl(**rows[u]) for u in page if u in rows]

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

    async def count_curated_excluded(self, collection_id: str) -> int:
        async with self._conn() as conn:
            return await _scalar(await conn.execute(
                "SELECT COUNT(*) FROM curated_urls WHERE collection_id=%s AND excluded", (collection_id,)
            ))

    async def count_curated_unreachable(self, collection_id: str) -> int:
        async with self._conn() as conn:
            return await _scalar(await conn.execute(
                "SELECT COUNT(*) FROM curated_urls WHERE collection_id=%s AND crawl_failure IS NOT NULL",
                (collection_id,),
            ))

    async def replace_curated(self, collection_id: str, rows: list[CuratedUrl], *, changed: bool = True) -> int:
        """Bulk-replace the curated set in one transaction; returns the included count (what
        `curated_count` holds — excluded rows are written but not counted). `changed` stamps
        `curated_changed_at`: a promote that moved nothing (the "mark curated" shortcut) leaves the
        index as up to date as it was.

        A row carries the page text it was approved with as its `content_hash`, which `page_text`
        resolves: `engine.diff.promote` gives each promoted row the dump's current hash and leaves
        every other row — one still under review in a partial promote, one the dump lacks because
        the crawl could not fetch it — on the hash it already had. So the text follows the hash on
        its own, and the pair of copy-the-text-from-the-dump steps this used to need (`text_from_dump`,
        `text_urls`) are gone with the second copy of the text itself.

        Blobs the rows this replaced were the last holders of are collected before the commit."""
        async with self._conn() as conn:
            await conn.execute(
                "CREATE TEMP TABLE curated_in (LIKE curated_urls INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            await conn.execute("ALTER TABLE curated_in ADD COLUMN full_text text")
            async with conn.cursor() as cur, cur.copy(
                "COPY curated_in (collection_id,url,scraped_title,title,division,document_type,excluded,"
                "content_hash,edited_by,crawl_failure,full_text) FROM STDIN"
            ) as copy:
                for r in rows:
                    await copy.write_row((
                        r.collection_id, r.url, r.scraped_title, r.title, r.division, r.document_type,
                        r.excluded, r.content_hash or content_hash(r.full_text), r.edited_by,
                        r.crawl_failure, r.full_text,
                    ))
            # A row handed to us with its own text keeps it: file the blob under the hash that
            # fingerprints it, exactly as `replace_dump` does. A promote loads the curated set
            # without text and this does nothing — the row's hash already points at a blob the
            # dump filed — but it keeps the contract of this method self-contained.
            await conn.execute(
                "INSERT INTO page_text (collection_id, content_hash, full_text)"
                " SELECT collection_id, content_hash, full_text FROM curated_in"
                " WHERE content_hash IS NOT NULL AND full_text IS NOT NULL"
                " ON CONFLICT (collection_id, content_hash) DO NOTHING"
            )
            await conn.execute("CREATE INDEX ON curated_in (url)")
            await conn.execute("ANALYZE curated_in")
            await conn.execute(
                "DELETE FROM curated_urls c WHERE c.collection_id=%s"
                " AND NOT EXISTS (SELECT 1 FROM curated_in i WHERE i.url=c.url)",
                (collection_id,),
            )
            await conn.execute(
                "INSERT INTO curated_urls (collection_id,url,scraped_title,title,division,document_type,excluded,"
                "content_hash,edited_by,crawl_failure)"
                " SELECT collection_id,url,scraped_title,title,division,document_type,excluded,content_hash,"
                "edited_by,crawl_failure FROM curated_in"
                " ON CONFLICT (collection_id, url) DO UPDATE SET scraped_title=EXCLUDED.scraped_title,"
                " title=EXCLUDED.title, division=EXCLUDED.division, document_type=EXCLUDED.document_type,"
                " excluded=EXCLUDED.excluded, content_hash=EXCLUDED.content_hash, edited_by=EXCLUDED.edited_by,"
                " crawl_failure=EXCLUDED.crawl_failure"
            )
            if len(rows) >= _BULK_ROWS:
                await conn.execute("ANALYZE curated_urls (collection_id, url, excluded)")
            await self._gc_page_text(conn, collection_id)
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
                    # the collection had a division assigned when the row was classified, so no
                    # division was asked for; it is back to the placeholder and nothing fills the field
                    f" OR (d.division_skipped AND {_no_division('d.division')})"
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
        q = (f"SELECT d.url, d.scraped_title AS title, p.full_text AS text, u.content_hash"
             f" FROM delta_urls d LEFT JOIN dump_urls u ON u.collection_id=d.collection_id AND u.url=d.url"
             f"{_TEXT_JOIN} WHERE {self._LLM_WHERE}") + (self._LLM_MISSING if only_missing else "")
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

    # A field on a URL whose winning rule a person wrote: a glob or per-URL rule the SME typed, or
    # an AI suggestion they edited before accepting. Such a field is left out of the accept-all
    # buttons — see `deltas_with_ai(skip_human=True)`.
    _HUMAN_RULE = ("EXISTS (SELECT 1 FROM pattern_effects e JOIN patterns p ON p.id=e.pattern_id"
                   " WHERE e.collection_id=delta_urls.collection_id AND e.url=delta_urls.url"
                   " AND e.field=%s AND p.source IN ('sme','llm_edited'))")

    async def deltas_with_ai(
        self, collection_id: str, field: str, url: str | None = None, conf: str | None = None,
        *, skip_human: bool = False,
    ) -> list[tuple[str, str]]:
        """(url, suggested value) for every pending, non-removed URL with an AI suggestion for `field`
        (just that one row when `url` is given; only suggestions of that confidence when `conf` is).

        `skip_human`: leave out the rows where a rule the SME wrote already decides `field`. Accept-all
        passes it so a bulk accept never overwrites a rule a person added after the metadata was
        generated — those rows stay in the review table and are accepted one at a time if wanted."""
        assert field in ("title", "division", "document_type")
        sql = (f"SELECT url, {field}_ai AS v FROM delta_urls WHERE collection_id=%s AND kind!='deleted'"
               f" AND {field}_ai IS NOT NULL")
        args: list[Any] = [collection_id]
        if url is not None:
            sql += " AND url=%s"; args.append(url)
        if conf is not None:
            sql += f" AND {field}_ai_conf=%s"; args.append(conf)
        if skip_human:
            sql += f" AND NOT {self._HUMAN_RULE}"; args.append(field)
        async with self._conn() as conn:
            cur = await conn.execute(sql + " ORDER BY url", args)
            return [(r["url"], r["v"]) for r in await cur.fetchall()]

    async def human_set_fields(self, collection_id: str, urls: list[str]) -> dict[str, set[str]]:
        """{url: {fields whose winning rule a person wrote}} — the rows the accept-all buttons pass
        over, marked as such in the review table."""
        if not urls:
            return {}
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT e.url, e.field FROM pattern_effects e JOIN patterns p ON p.id=e.pattern_id"
                " WHERE e.collection_id=%s AND e.url = ANY(%s) AND p.source IN ('sme','llm_edited')",
                (collection_id, list(urls)),
            )
            out: dict[str, set[str]] = {}
            for r in await cur.fetchall():
                out.setdefault(r["url"], set()).add(r["field"])
            return out

    @staticmethod
    def _ai_filter(field: str | None, conf: str | None) -> tuple[str, list[Any]]:
        """Rows with a pending suggestion for `field` (any field when None), of confidence `conf`
        (any when None)."""
        fields = [field] if field in AI_FIELDS else list(AI_FIELDS)
        if conf is None:
            return "(" + " OR ".join(f"{f}_ai IS NOT NULL" for f in fields) + ")", []
        return ("(" + " OR ".join(f"({f}_ai IS NOT NULL AND {f}_ai_conf=%s)" for f in fields) + ")",
                [conf] * len(fields))

    def _ai_review_where(
        self, collection_id: str, *, field: str | None, conf: str | None,
        with_dups: bool, dups_only: bool, undecided_only: bool, dup_cte: bool = False,
    ) -> tuple[str, list[Any]]:
        """(WHERE, args) for the metadata review table — what `list_delta_ai` lists, in one place so
        the table and the counts under it can never drift apart. `dup_cte`: read the duplicate set
        from a `dup` CTE the caller has already declared, instead of inlining it (and its two args)."""
        dup = "url IN (SELECT url FROM dup)" if dup_cte else f"url IN (SELECT url FROM ({_DUPLICATE_TITLES}) g WHERE delta)"
        dup_args: list[Any] = [] if dup_cte else [collection_id] * 2
        if dups_only:
            cond, cargs = dup, dup_args
        else:
            cond, cargs = self._ai_filter(field, conf)
            if with_dups:
                cond, cargs = f"({cond} OR {dup})", [*cargs, *dup_args]
                if not undecided_only:
                    cond = f"({cond} OR ai_model IS NOT NULL)"
        return f"collection_id=%s AND kind!='deleted' AND {cond}", [collection_id, *cargs]

    async def count_delta_ai(
        self, collection_id: str, *, field: str | None = None, conf: str | None = None,
        with_dups: bool = False, dups_only: bool = False,
    ) -> tuple[int, int]:
        """(rows in the review table, of which still to decide) — the whole metadata round and what
        is left of it, in one pass, so the page that shows both does not scan for duplicates twice."""
        kw = {"field": field, "conf": conf, "with_dups": with_dups, "dups_only": dups_only, "dup_cte": True}
        whole, wargs = self._ai_review_where(collection_id, undecided_only=False, **kw)  # type: ignore[arg-type]
        left, largs = self._ai_review_where(collection_id, undecided_only=True, **kw)  # type: ignore[arg-type]
        # The rows still to decide are a subset of the round, so the round is the WHERE (it keeps
        # the scan off the delta rows no Suggest metadata run has touched) and only the subset is a
        # FILTER. The duplicate scan is a CTE: it is the expensive half and both conditions read it.
        async with self._conn() as conn:
            cur = await conn.execute(
                f"WITH dup AS (SELECT url FROM ({_DUPLICATE_TITLES}) g WHERE delta)"
                f" SELECT COUNT(*) AS whole, COUNT(*) FILTER (WHERE {left}) AS few"
                f" FROM delta_urls WHERE {whole}",
                [collection_id, collection_id, *largs, *wargs],
            )
            r = await cur.fetchone()
        return r["whole"] or 0, r["few"] or 0

    async def list_delta_ai(
        self, collection_id: str, limit: int = 50, offset: int = 0, *,
        field: str | None = None, conf: str | None = None,
        with_dups: bool = False, dups_only: bool = False, undecided_only: bool = False,
    ) -> tuple[list[DeltaUrl], int]:
        """The non-removed delta URLs of the current metadata round, by URL — the review table under
        Curate › Metadata — and how many there are in all.

        The round is every row the last Suggest metadata run classified (`ai_model`, which a
        decision does not clear), not just the rows still carrying a suggestion: a row the curator
        has finished keeps its place and its number in the list, marked as decided, instead of
        vanishing and renumbering every row below it on whichever accept happened to be its last.
        `undecided_only` (the "Hide decided" toggle) drops them again, for a curator who wants only
        what is left.

        `with_dups` also lists the rows another page of the collection will be indexed under the
        same title + document type as: deciding a suggestion does not resolve a collision, so those
        rows have to stay on the page the curator fixes them on until the titles differ.
        `dups_only` narrows the table to them (the ⚠ badge links here).

        A field / confidence filter is a question about suggestions only, so it lists exactly the
        rows that still carry one."""
        where, args = self._ai_review_where(collection_id, field=field, conf=conf, with_dups=with_dups,
                                            dups_only=dups_only, undecided_only=undecided_only)
        async with self._conn() as conn:
            total = await _scalar(await conn.execute(f"SELECT COUNT(*) FROM delta_urls WHERE {where}", args))
            cur = await conn.execute(
                f"SELECT * FROM delta_urls WHERE {where} ORDER BY {url_order_sql()} LIMIT %s OFFSET %s",
                [*args, limit, offset],
            )
            return [DeltaUrl(**r) for r in await cur.fetchall()], total

    async def count_ai_suggestions(self, collection_id: str, *, field: str | None = None,
                                   conf: str | None = None, skip_human: bool = False) -> int:
        """Pending field-level AI suggestions for `field` (every field when None) of confidence `conf`
        (any when None): what an accept / reject of the filtered review decides. `skip_human` counts
        only the ones accept-all would actually apply (see `deltas_with_ai`)."""
        fields = [field] if field in AI_FIELDS else list(AI_FIELDS)
        parts, args = [], []
        for f in fields:
            cond = f"{f}_ai IS NOT NULL" + (f" AND {f}_ai_conf=%s" if conf else "")
            parts.append(f"COUNT(*) FILTER (WHERE {cond}" + (f" AND NOT {self._HUMAN_RULE})" if skip_human else ")"))
            args += [conf] if conf else []
            args += [f] if skip_human else []
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
        urls: list[str] | None = None,
    ) -> int:
        """Drop the pending suggestion for `field`. `urls`: only these rows — an accept clears
        exactly the rows it applied, so the ones it passed over stay in the review table."""
        assert field in ("title", "division", "document_type")
        extra = ", title_ai_before=NULL" if field == "title" else ""
        sql = (f"UPDATE delta_urls SET {field}_ai=NULL, {field}_ai_conf=NULL{extra}"
               f" WHERE collection_id=%s AND {field}_ai IS NOT NULL")
        args: list[Any] = [collection_id]
        if url is not None:
            sql += " AND url=%s"; args.append(url)
        if conf is not None:
            sql += f" AND {field}_ai_conf=%s"; args.append(conf)
        if urls is not None:
            sql += " AND url = ANY(%s)"; args.append(list(urls))
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
        """Delta URLs promote would refuse (see _UNPROMOTABLE): how many, and why.
        `title` counts only the rows with no title at all: a row with no title rule is indexed under
        its scraped title, so it is complete. `general` is the subset of `division` still carrying
        the retired "General" placeholder — the same problem, but it reads differently to the
        curator. `duplicate` is the rows that would be indexed under a title + document type
        another page already has (undecided AI titles do not count: promote discards them), and it
        overlaps the field counts — a row can be short of both. `urls`: only these rows (a promote
        of a selection). One pass: the duplicate scan is a CTE, read twice, computed once."""
        where, args = "collection_id=%s AND ((" + _INCOMPLETE + ") OR url IN (SELECT url FROM dup))", [collection_id]
        if urls is not None:
            where += " AND url = ANY(%s)"; args.append(list(urls))
        async with self._conn() as conn:
            cur = await conn.execute(
                f"WITH dup AS ({_DUPLICATE_EFFECTIVE})"
                f" SELECT COUNT(*) AS urls, COUNT(*) FILTER (WHERE {_NO_TITLE}) AS title,"
                f" COUNT(*) FILTER (WHERE {_NO_DIVISION}) AS division,"
                " COUNT(*) FILTER (WHERE division = 'General') AS general,"
                " COUNT(*) FILTER (WHERE document_type IS NULL) AS document_type,"
                " COUNT(*) FILTER (WHERE url IN (SELECT url FROM dup)) AS duplicate"
                f" FROM delta_urls WHERE {where}", [collection_id, collection_id, *args],
            )
            r = await cur.fetchone()
        return {k: r[k] or 0 for k in ("urls", "title", "division", "general", "document_type", "duplicate")}

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

    async def title_keys(self, collection_id: str) -> dict[str, str]:
        """Every included page of the collection as {url: the title + document type it would be
        indexed under} (see _PROJECTED_TITLES). What a new title has to stay clear of: telling a
        group apart within itself is not enough, because the title it picks can be one another group
        — or a page that was never in a group at all — already has."""
        async with self._conn() as conn:
            cur = await conn.execute(f"SELECT url, k FROM ({_PROJECTED_TITLES}) p",
                                     (collection_id, collection_id))
            return {r["url"]: r["k"] for r in await cur.fetchall()}

    async def text_sizes(self, collection_id: str, urls: list[str]) -> dict[str, int]:
        """How long each of these delta URLs' page text is. Read before the text itself so a group
        can be packed into calls by size without holding every page in memory at once."""
        if not urls:
            return {}
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT d.url, COALESCE(length(p.full_text), 0) AS n FROM delta_urls d"
                " LEFT JOIN dump_urls u ON u.collection_id=d.collection_id AND u.url=d.url"
                + _TEXT_JOIN +
                " WHERE d.collection_id=%s AND d.url = ANY(%s)", (collection_id, list(urls)),
            )
            return {r["url"]: r["n"] for r in await cur.fetchall()}

    async def docs_for_llm(self, collection_id: str, urls: list[str]) -> list[dict[str, Any]]:
        """{url, title, text, content_hash} of these delta URLs, with the full page text."""
        if not urls:
            return []
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT d.url, d.scraped_title AS title, p.full_text AS text, u.content_hash"
                " FROM delta_urls d LEFT JOIN dump_urls u ON u.collection_id=d.collection_id AND u.url=d.url"
                + _TEXT_JOIN +
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
        {"title", "document_type", "members": [{url, delta, pending_ai, before}]}, members by URL.
        `before` is the title the row shared before an earlier regeneration, so a repeat run can ask
        the model the same question again instead of building on its last answer."""
        async with self._conn() as conn:
            cur = await conn.execute(f"SELECT * FROM ({_DUPLICATE_TITLES}) g ORDER BY k, url",
                                     (collection_id, collection_id))
            rows = await cur.fetchall()
        groups: dict[str, dict[str, Any]] = {}
        for r in rows:
            g = groups.setdefault(r["k"], {"title": r["title"], "document_type": r["document_type"], "members": []})
            g["members"].append({"url": r["url"], "delta": r["delta"], "pending_ai": r["pending_ai"],
                                 "before": r["shared_before"]})
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
        """Bulk insert; rows identical to an existing (type, match) are skipped. One transaction.
        COPY + one INSERT … SELECT: bulk-accepting AI metadata inserts three rules per URL."""
        if not rows:
            return 0
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(
                "CREATE TEMP TABLE patterns_in (n bigint, collection_id text, type text, match text, value text,"
                " created_at timestamptz, created_by text, source text) ON COMMIT DROP"
            )
            async with cur.copy(
                "COPY patterns_in (n,collection_id,type,match,value,created_at,created_by,source) FROM STDIN"
            ) as copy:
                for n, p in enumerate(rows):
                    await copy.write_row((n, p.collection_id, p.type, p.match, p.value, p.created_at, p.created_by, p.source))
            # ORDER BY n: ids are handed out in the order given, and the newest (highest id) rule wins
            await cur.execute(
                "INSERT INTO patterns (collection_id,type,match,value,created_at,created_by,source)"
                " SELECT collection_id,type,match,value,created_at,created_by,source FROM patterns_in ORDER BY n"
                " ON CONFLICT DO NOTHING"
            )
            inserted = cur.rowcount
            if inserted >= _BULK_ROWS:
                await cur.execute("ANALYZE patterns (collection_id, type, match)")
            return inserted

    async def delete_exact_patterns(self, collection_id: str, type_: str, matches: list[str]) -> int:
        if not matches:
            return 0
        async with self._conn() as conn:
            cur = await conn.execute(
                "DELETE FROM patterns WHERE collection_id=%s AND type=%s AND match = ANY(%s)",
                (collection_id, type_, list(matches)),
            )
            return cur.rowcount

    async def load_rules(self, collection_id: str) -> list[Rule]:
        """Every rule, oldest first, as the slim engine view (models.Rule), read through a
        server-side cursor so the driver's row dicts never pile up next to the result."""
        types, sources = {t.value: t for t in PatternType}, {x.value: x for x in RuleSource}
        out: list[Rule] = []
        async with self._conn() as conn, conn.cursor(name="rules", row_factory=tuple_row) as cur:
            await cur.execute(
                "SELECT id, type, match, value, source FROM patterns WHERE collection_id=%s ORDER BY id",
                (collection_id,),
            )
            while rows := await cur.fetchmany(10_000):
                out += [Rule(i, types[t], m, sys.intern(v) if v is not None else None, sources[x])
                        for i, t, m, v, x in rows]
        return out

    async def iter_pattern_rows(self, collection_id: str, chunk: int = 5000) -> AsyncIterator[list[dict[str, Any]]]:
        """Every rule as a plain row, oldest first, `chunk` at a time (the patterns.yaml writer). Keyset pages, each
        its own short transaction: the consumer is slow (it serialises YAML), and a cursor held
        open for the whole file would keep one of the pool's few connections from every request."""
        last = 0
        while True:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "SELECT * FROM patterns WHERE collection_id=%s AND id > %s ORDER BY id LIMIT %s",
                    (collection_id, last, chunk),
                )
                rows = await cur.fetchall()
            if not rows:
                return
            last = rows[-1]["id"]
            yield rows

    async def count_patterns(self, collection_id: str) -> int:
        async with self._conn() as conn:
            return await _scalar(await conn.execute(
                "SELECT COUNT(*) FROM patterns WHERE collection_id=%s", (collection_id,)
            ))

    async def get_pattern(self, collection_id: str, pattern_id: int) -> Pattern | None:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM patterns WHERE collection_id=%s AND id=%s", (collection_id, pattern_id)
            )
            row = await cur.fetchone()
            return Pattern(**row) if row else None

    async def exact_patterns_for(self, collection_id: str, url: str, type_: str | None = None) -> list[Pattern]:
        """The exact-URL rules for this page under any spelling of its URL (an exact rule matches by
        canonical key), oldest first — without loading a collection's every per-URL rule to find one."""
        key = canonical_key(url)
        q = ("SELECT * FROM patterns WHERE collection_id=%s AND position('*' in match) = 0"
             " AND position(lower(%s) in lower(match)) > 0")
        args: list[Any] = [collection_id, key.rstrip("/")]  # the site root is also written without its "/"
        if type_ is not None:
            q += " AND type=%s"
            args.append(type_)
        async with self._conn() as conn:
            cur = await conn.execute(q + " ORDER BY id", args)
            return [p for p in (Pattern(**r) for r in await cur.fetchall()) if canonical_key(p.match) == key]

    async def exact_pattern_matches(self, collection_id: str, types: list[str]) -> list[tuple[int, str, str]]:
        """(id, type, match) of every exact-URL rule of these types, as plain tuples."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT id, type, match FROM patterns WHERE collection_id=%s AND type = ANY(%s)"
                " AND position('*' in match) = 0", (collection_id, list(types)),
            )
            return [(r["id"], r["type"], r["match"]) for r in await cur.fetchall()]

    async def delete_patterns(self, collection_id: str, ids: list[int]) -> int:
        if not ids:
            return 0
        async with self._conn() as conn:
            cur = await conn.execute(
                "DELETE FROM patterns WHERE collection_id=%s AND id = ANY(%s)", (collection_id, list(ids))
            )
            return cur.rowcount

    async def list_patterns(self, collection_id: str, *, types: list[str] | None = None,
                            exact: bool | None = None, limit: int | None = None, offset: int = 0) -> list[Pattern]:
        """The collection's rules, oldest first. `types` / `exact` (per-URL rules only, or globs only)
        narrow it in SQL, `limit` / `offset` page it: a collection can hold three per-URL rules for
        every URL."""
        q, args = "SELECT * FROM patterns WHERE collection_id=%s", [collection_id]
        if types is not None:
            q += " AND type = ANY(%s)"
            args.append([str(t) for t in types])
        if exact is not None:
            q += f" AND position('*' in match) {'=' if exact else '>'} 0"
        q += " ORDER BY id"
        if limit is not None:
            q += " LIMIT %s OFFSET %s"
            args += [limit, offset]
        async with self._conn() as conn:
            cur = await conn.execute(q, args)
            return [Pattern(**r) for r in await cur.fetchall()]

    async def pattern_counts(self, collection_id: str) -> dict[str, Any]:
        """How many rules there are, how many of them per-URL (exact), and how many per source."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT source, COUNT(*) AS n, COUNT(*) FILTER (WHERE position('*' in match) = 0) AS exact"
                " FROM patterns WHERE collection_id=%s GROUP BY source ORDER BY source", (collection_id,),
            )
            rows = await cur.fetchall()
        return {"total": sum(r["n"] for r in rows), "exact": sum(r["exact"] for r in rows),
                "by_source": {r["source"]: r["n"] for r in rows}}

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

    async def jobs_ended_by_shutdown(self) -> list[JobRun]:
        """Collections whose *latest* job was cancelled by an engine shutdown (a deploy): nothing
        has happened on them since, so the job can be picked up where it stopped."""
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM (SELECT DISTINCT ON (collection_id) * FROM job_runs ORDER BY collection_id, id DESC) j"
                " WHERE state='failed' AND error='cancelled by shutdown' ORDER BY id"
            )
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
