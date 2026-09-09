"""SQLite state store (aiosqlite). Schema is created if missing; bulk ops use executemany."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from .models import (
    Collection,
    CuratedUrl,
    CurationStage,
    DeltaUrl,
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

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS collections (
  collection_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  seed_url TEXT NOT NULL,
  division TEXT NOT NULL,
  document_type TEXT,
  connector TEXT NOT NULL,
  max_pages INTEGER NOT NULL,
  status TEXT NOT NULL,
  curation_stage TEXT,
  needs_recuration INTEGER NOT NULL DEFAULT 0,
  last_scraped_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  dump_count INTEGER NOT NULL DEFAULT 0,
  delta_count INTEGER NOT NULL DEFAULT 0,
  curated_count INTEGER NOT NULL DEFAULT 0,
  last_run_id TEXT,
  created_by TEXT
);

CREATE TABLE IF NOT EXISTS status_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  collection_id TEXT NOT NULL REFERENCES collections(collection_id) ON DELETE CASCADE,
  old_status TEXT,
  new_status TEXT NOT NULL,
  note TEXT,
  at TEXT NOT NULL,
  actor TEXT
);

CREATE TABLE IF NOT EXISTS dump_urls (
  collection_id TEXT NOT NULL REFERENCES collections(collection_id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  scraped_title TEXT,
  full_text TEXT,
  content_type TEXT,
  depth INTEGER,
  PRIMARY KEY (collection_id, url)
);

CREATE TABLE IF NOT EXISTS delta_urls (
  collection_id TEXT NOT NULL REFERENCES collections(collection_id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  kind TEXT NOT NULL,
  scraped_title TEXT,
  title TEXT,
  division TEXT,
  document_type TEXT,
  excluded INTEGER NOT NULL DEFAULT 0,
  title_ai TEXT,
  division_ai TEXT,
  document_type_ai TEXT,
  PRIMARY KEY (collection_id, url)
);

CREATE TABLE IF NOT EXISTS curated_urls (
  collection_id TEXT NOT NULL REFERENCES collections(collection_id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  scraped_title TEXT,
  title TEXT,
  division TEXT,
  document_type TEXT,
  excluded INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (collection_id, url)
);

CREATE TABLE IF NOT EXISTS patterns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  collection_id TEXT NOT NULL REFERENCES collections(collection_id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  match TEXT NOT NULL,
  value TEXT,
  created_at TEXT NOT NULL,
  created_by TEXT,
  UNIQUE (collection_id, type, match)
);

CREATE TABLE IF NOT EXISTS pattern_effects (
  pattern_id INTEGER NOT NULL REFERENCES patterns(id) ON DELETE CASCADE,
  collection_id TEXT NOT NULL,
  url TEXT NOT NULL,
  field TEXT NOT NULL,
  PRIMARY KEY (pattern_id, url, field)
);

CREATE TABLE IF NOT EXISTS pattern_suggestions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  collection_id TEXT NOT NULL REFERENCES collections(collection_id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  match TEXT NOT NULL,
  value TEXT,
  rationale TEXT,
  matches INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL,
  decided_by TEXT,
  UNIQUE (collection_id, type, match)
);

CREATE TABLE IF NOT EXISTS index_runs (
  run_id TEXT PRIMARY KEY,
  collection_id TEXT NOT NULL REFERENCES collections(collection_id) ON DELETE CASCADE,
  target TEXT NOT NULL,
  state TEXT NOT NULL,
  exported INTEGER NOT NULL DEFAULT 0,
  external_ref TEXT,
  status TEXT,
  validation TEXT,
  validated_by TEXT,
  error TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  started_by TEXT
);
CREATE INDEX IF NOT EXISTS index_runs_coll ON index_runs(collection_id, started_at DESC);

CREATE TABLE IF NOT EXISTS job_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  collection_id TEXT NOT NULL REFERENCES collections(collection_id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  state TEXT NOT NULL,
  run_id TEXT,
  external_ref TEXT,
  progress TEXT NOT NULL DEFAULT '{}',
  error TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  started_by TEXT
);
CREATE INDEX IF NOT EXISTS job_runs_coll ON job_runs(collection_id, id DESC);

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE COLLATE NOCASE,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'curator',
  active INTEGER NOT NULL DEFAULT 1,
  session_version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- append-only provenance ledger; no FK so it survives collection deletion
CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL,
  actor TEXT NOT NULL,
  collection_id TEXT,
  action TEXT NOT NULL,
  detail TEXT
);
CREATE INDEX IF NOT EXISTS audit_log_coll ON audit_log(collection_id, id DESC);
"""


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


class Database:
    def __init__(self, path: Path | str, *, exclusive: bool = False):
        self.path = str(path)
        self.exclusive = exclusive
        self._conn: aiosqlite.Connection | None = None
        # optional async hook(collection_id, old_status, new_status, note, actor) after every history row
        self.on_status_change = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("database not connected")
        return self._conn

    async def connect(self) -> Database:
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        if self.exclusive:
            # WAL over NFS (EFS) is unsupported because the wal-index is a shared mmap; with
            # EXCLUSIVE locking SQLite keeps it in heap memory instead. Must precede journal_mode.
            await self._conn.execute("PRAGMA locking_mode=EXCLUSIVE")
        await self._conn.executescript(SCHEMA)
        await self._migrate()
        await self._conn.commit()
        return self

    async def _migrate(self) -> None:
        """Idempotent schema evolution for databases created by earlier versions."""
        cur = await self._conn.execute("PRAGMA table_info(delta_urls)")
        cols = {r[1] for r in await cur.fetchall()}
        for f in ("title", "division", "document_type"):
            if f"{f}_ml" in cols:  # legacy *_ml → *_ai rename
                await self._conn.execute(f"ALTER TABLE delta_urls RENAME COLUMN {f}_ml TO {f}_ai")
        for table, col in (
            ("collections", "last_run_id"), ("index_runs", "validated_by"),
            # provenance (rows from before these columns existed keep NULL = unknown)
            ("collections", "created_by"), ("status_history", "actor"), ("patterns", "created_by"),
            ("pattern_suggestions", "decided_by"), ("index_runs", "started_by"), ("job_runs", "started_by"),
            ("collections", "curation_stage"), ("collections", "last_scraped_at"),
        ):
            await self._add_column(table, col, "TEXT")
        # Rows from before last_scraped_at existed: take the last successful scrape job.
        await self._conn.execute(
            """UPDATE collections SET last_scraped_at = (
                 SELECT MAX(finished_at) FROM job_runs j
                 WHERE j.collection_id = collections.collection_id AND j.kind='scrape' AND j.state='succeeded')
               WHERE last_scraped_at IS NULL AND dump_count > 0"""
        )
        # Collections already curating when stages were introduced start at the first stage.
        await self._conn.execute(
            "UPDATE collections SET curation_stage='scope' WHERE status='curating' AND curation_stage IS NULL"
        )
        await self._conn.commit()

    async def _add_column(self, table: str, col: str, ddl: str) -> None:
        cur = await self._conn.execute(f"PRAGMA table_info({table})")
        if col not in {r[1] for r in await cur.fetchall()}:
            await self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def ping(self) -> bool:
        cur = await self.conn.execute("SELECT 1")
        return (await cur.fetchone()) is not None

    # ── collections ────────────────────────────────────────────────────

    async def insert_collection(self, c: Collection) -> Collection:
        await self.conn.execute(
            """INSERT INTO collections (collection_id,name,seed_url,division,document_type,connector,
               max_pages,status,needs_recuration,created_at,updated_at,dump_count,delta_count,curated_count,
               created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                c.collection_id, c.name, c.seed_url, c.division, c.document_type, c.connector,
                c.max_pages, c.status, int(c.needs_recuration), _iso(c.created_at),
                _iso(c.updated_at), c.dump_count, c.delta_count, c.curated_count, c.created_by,
            ),
        )
        await self.conn.execute(
            "INSERT INTO status_history (collection_id,old_status,new_status,note,at,actor) VALUES (?,?,?,?,?,?)",
            (c.collection_id, None, c.status, "created", _iso(utcnow()), c.created_by),
        )
        await self.conn.commit()
        return c

    async def get_collection(self, collection_id: str) -> Collection | None:
        cur = await self.conn.execute(
            "SELECT * FROM collections WHERE collection_id=?", (collection_id,)
        )
        row = await cur.fetchone()
        return Collection(**dict(row)) if row else None

    async def list_collections(self) -> list[Collection]:
        cur = await self.conn.execute("SELECT * FROM collections ORDER BY created_at DESC")
        return [Collection(**dict(r)) for r in await cur.fetchall()]

    async def delete_collection(self, collection_id: str) -> bool:
        cur = await self.conn.execute(
            "DELETE FROM collections WHERE collection_id=?", (collection_id,)
        )
        await self.conn.commit()
        return cur.rowcount > 0

    async def set_status(
        self, collection_id: str, new: Status, note: str | None = None, *, force: bool = False,
        actor: str | None = None,
    ) -> Collection:
        c = await self.get_collection(collection_id)
        if c is None:
            raise KeyError(collection_id)
        if not force:
            check_transition(c.status, new)
        now = utcnow()
        # Stage rule, applied for every caller: entering `curating` starts at scope, staying in
        # it keeps the current stage, leaving it clears the stage.
        if new is Status.CURATING:
            stage = c.curation_stage if c.status is Status.CURATING and c.curation_stage else CurationStage.SCOPE
        else:
            stage = None
        await self.conn.execute(
            "UPDATE collections SET status=?, curation_stage=?, updated_at=? WHERE collection_id=?",
            (new, stage, _iso(now), collection_id),
        )
        await self.conn.execute(
            "INSERT INTO status_history (collection_id,old_status,new_status,note,at,actor) VALUES (?,?,?,?,?,?)",
            (collection_id, c.status, new, note, _iso(now), actor),
        )
        await self.conn.commit()
        if self.on_status_change:  # every history row (the hook decides what to notify / persist)
            try:
                await self.on_status_change(collection_id, c.status, new, note, actor)
            except Exception as e:  # noqa: BLE001 - notifications must never break a transition
                logging.getLogger(__name__).warning("status hook failed: %s", e)
        c.status, c.curation_stage, c.updated_at = new, stage, now
        return c

    async def set_stage(self, collection_id: str, stage: CurationStage) -> bool:
        """Move a curating collection between stages; no-op (False) outside `curating`."""
        cur = await self.conn.execute(
            "UPDATE collections SET curation_stage=?, updated_at=? WHERE collection_id=? AND status='curating'",
            (stage, _iso(utcnow()), collection_id),
        )
        await self.conn.commit()
        return cur.rowcount > 0

    async def set_last_scraped(self, collection_id: str, at: datetime) -> None:
        await self.conn.execute(
            "UPDATE collections SET last_scraped_at=? WHERE collection_id=?", (_iso(at), collection_id)
        )
        await self.conn.commit()

    async def set_flag(self, collection_id: str, needs_recuration: bool) -> None:
        await self.conn.execute(
            "UPDATE collections SET needs_recuration=?, updated_at=? WHERE collection_id=?",
            (int(needs_recuration), _iso(utcnow()), collection_id),
        )
        await self.conn.commit()

    async def update_counts(self, collection_id: str, **counts: int) -> None:
        allowed = {"dump_count", "delta_count", "curated_count"}
        bad = set(counts) - allowed
        if bad:
            raise ValueError(f"unknown counters {bad}")
        if not counts:
            return
        sets = ", ".join(f"{k}=?" for k in counts)
        await self.conn.execute(
            f"UPDATE collections SET {sets}, updated_at=? WHERE collection_id=?",
            (*counts.values(), _iso(utcnow()), collection_id),
        )
        await self.conn.commit()

    async def status_history(self, collection_id: str) -> list[StatusHistory]:
        cur = await self.conn.execute(
            "SELECT * FROM status_history WHERE collection_id=? ORDER BY id", (collection_id,)
        )
        return [StatusHistory(**dict(r)) for r in await cur.fetchall()]

    # ── dump urls ──────────────────────────────────────────────────────

    async def replace_dump(self, collection_id: str, rows: list[DumpUrl]) -> int:
        """Bulk-replace the dump for a collection in one transaction; returns row count."""
        await self.conn.execute("DELETE FROM dump_urls WHERE collection_id=?", (collection_id,))
        await self.conn.executemany(
            """INSERT OR REPLACE INTO dump_urls
               (collection_id,url,scraped_title,full_text,content_type,depth) VALUES (?,?,?,?,?,?)""",
            [(r.collection_id, r.url, r.scraped_title, r.full_text, r.content_type, r.depth)
             for r in rows],
        )
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM dump_urls WHERE collection_id=?", (collection_id,)
        )
        n = (await cur.fetchone())[0]
        await self.conn.execute(
            "UPDATE collections SET dump_count=?, updated_at=? WHERE collection_id=?",
            (n, _iso(utcnow()), collection_id),
        )
        await self.conn.commit()
        return n

    async def list_dump(
        self, collection_id: str, limit: int = 100, offset: int = 0, q: str | None = None
    ) -> tuple[list[dict[str, Any]], int]:
        """Dump rows (no full_text) plus `text_len` and `in_curated` / `in_deltas` flags."""
        where, args = ["d.collection_id=?"], [collection_id]
        if q:
            where.append("(d.url LIKE ? OR d.scraped_title LIKE ?)"); args += [f"%{q}%"] * 2
        w = " AND ".join(where)
        cur = await self.conn.execute(f"SELECT COUNT(*) FROM dump_urls d WHERE {w}", args)
        total = (await cur.fetchone())[0]
        cur = await self.conn.execute(
            f"""SELECT d.collection_id, d.url, d.scraped_title, d.content_type, d.depth,
                       length(d.full_text) AS text_len,
                       (c.url IS NOT NULL) AS in_curated, (x.url IS NOT NULL) AS in_deltas
                FROM dump_urls d
                LEFT JOIN curated_urls c ON c.collection_id=d.collection_id AND c.url=d.url
                LEFT JOIN delta_urls x ON x.collection_id=d.collection_id AND x.url=d.url
                WHERE {w} ORDER BY d.url LIMIT ? OFFSET ?""", [*args, limit, offset],
        )
        return [dict(r) for r in await cur.fetchall()], total

    async def urls_with_deltas(self, collection_id: str, urls: list[str]) -> set[str]:
        if not urls:
            return set()
        marks = ",".join("?" * len(urls))
        cur = await self.conn.execute(
            f"SELECT url FROM delta_urls WHERE collection_id=? AND url IN ({marks})", [collection_id, *urls]
        )
        return {r[0] for r in await cur.fetchall()}

    async def effects_for(self, collection_id: str, urls: list[str]) -> dict[str, dict[str, str]]:
        """{url: {field: 'type match → value'}} — which pattern produced each effective field."""
        if not urls:
            return {}
        marks = ",".join("?" * len(urls))
        cur = await self.conn.execute(
            f"""SELECT e.url, e.field, p.type, p.match, p.value, p.created_by FROM pattern_effects e
                JOIN patterns p ON p.id=e.pattern_id
                WHERE e.collection_id=? AND e.url IN ({marks})""", [collection_id, *urls],
        )
        out: dict[str, dict[str, str]] = {}
        for url, field, ptype, match, value, by in await cur.fetchall():
            out.setdefault(url, {})[field] = (
                f"{ptype} {match}" + (f" → {value}" if value else "") + (f" (by {by})" if by else "")
            )
        return out

    async def list_curated(
        self, collection_id: str, limit: int = 100, offset: int = 0, q: str | None = None,
        excluded: bool | None = None,
    ) -> tuple[list[CuratedUrl], int]:
        where, args = ["collection_id=?"], [collection_id]
        if q:
            where.append("(url LIKE ? OR title LIKE ? OR scraped_title LIKE ?)"); args += [f"%{q}%"] * 3
        if excluded is not None:
            where.append("excluded=?"); args.append(int(excluded))
        w = " AND ".join(where)
        cur = await self.conn.execute(f"SELECT COUNT(*) FROM curated_urls WHERE {w}", args)
        total = (await cur.fetchone())[0]
        cur = await self.conn.execute(
            f"SELECT * FROM curated_urls WHERE {w} ORDER BY url LIMIT ? OFFSET ?", [*args, limit, offset]
        )
        return [CuratedUrl(**dict(r)) for r in await cur.fetchall()], total

    async def load_dump(self, collection_id: str) -> list[DumpUrl]:
        cur = await self.conn.execute(
            "SELECT collection_id,url,scraped_title,content_type,depth FROM dump_urls WHERE collection_id=?",
            (collection_id,),
        )
        return [DumpUrl(**dict(r)) for r in await cur.fetchall()]

    async def dump_urls(self, collection_id: str) -> list[str]:
        cur = await self.conn.execute(
            "SELECT url FROM dump_urls WHERE collection_id=?", (collection_id,)
        )
        return [r[0] for r in await cur.fetchall()]

    async def dump_full_text(self, collection_id: str) -> dict[str, str | None]:
        cur = await self.conn.execute(
            "SELECT url, full_text FROM dump_urls WHERE collection_id=?", (collection_id,)
        )
        return {r[0]: r[1] for r in await cur.fetchall()}

    # ── deltas / curated ───────────────────────────────────────────────

    async def load_deltas(self, collection_id: str) -> list[DeltaUrl]:
        cur = await self.conn.execute(
            "SELECT * FROM delta_urls WHERE collection_id=?", (collection_id,)
        )
        return [DeltaUrl(**dict(r)) for r in await cur.fetchall()]

    async def get_delta(self, collection_id: str, url: str) -> DeltaUrl | None:
        cur = await self.conn.execute(
            "SELECT * FROM delta_urls WHERE collection_id=? AND url=?", (collection_id, url)
        )
        row = await cur.fetchone()
        return DeltaUrl(**dict(row)) if row else None

    async def list_deltas(
        self, collection_id: str, *, kind: str | None = None, excluded: bool | None = None,
        q: str | None = None, division: str | None = None, document_type: str | None = None,
        ai_pending: bool = False, limit: int = 100, offset: int = 0,
    ) -> tuple[list[DeltaUrl], int]:
        where, args = ["collection_id=?"], [collection_id]
        if kind:
            where.append("kind=?"); args.append(kind)
        if excluded is not None:
            where.append("excluded=?"); args.append(int(excluded))
        if ai_pending:
            where.append("(title_ai IS NOT NULL OR division_ai IS NOT NULL OR document_type_ai IS NOT NULL)")
        if division:
            where.append("division=?"); args.append(division)
        if document_type:
            where.append("document_type=?"); args.append(document_type)
        if q:
            where.append("(url LIKE ? OR title LIKE ? OR scraped_title LIKE ?)")
            args += [f"%{q}%"] * 3
        w = " AND ".join(where)
        cur = await self.conn.execute(f"SELECT COUNT(*) FROM delta_urls WHERE {w}", args)
        total = (await cur.fetchone())[0]
        cur = await self.conn.execute(
            f"SELECT * FROM delta_urls WHERE {w} ORDER BY kind, url LIMIT ? OFFSET ?",
            [*args, limit, offset],
        )
        return [DeltaUrl(**dict(r)) for r in await cur.fetchall()], total

    async def replace_deltas(
        self, collection_id: str, deltas: list[DeltaUrl], effects: list[tuple[int, str, str]]
    ) -> None:
        await self.conn.execute("DELETE FROM delta_urls WHERE collection_id=?", (collection_id,))
        await self.conn.executemany(
            """INSERT INTO delta_urls (collection_id,url,kind,scraped_title,title,division,document_type,
               excluded,title_ai,division_ai,document_type_ai) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            [(d.collection_id, d.url, d.kind, d.scraped_title, d.title, d.division, d.document_type,
              int(d.excluded), d.title_ai, d.division_ai, d.document_type_ai) for d in deltas],
        )
        await self.conn.execute(
            "DELETE FROM pattern_effects WHERE collection_id=?", (collection_id,)
        )
        await self.conn.executemany(
            "INSERT OR IGNORE INTO pattern_effects (pattern_id,collection_id,url,field) VALUES (?,?,?,?)",
            [(pid, collection_id, url, fld) for pid, url, fld in effects],
        )
        await self.conn.execute(
            "UPDATE collections SET delta_count=?, updated_at=? WHERE collection_id=?",
            (len(deltas), _iso(utcnow()), collection_id),
        )
        await self.conn.commit()

    async def set_delta_ai(self, collection_id: str, items: list[dict[str, Any]]) -> int:
        """Bulk-write AI suggestions (never touches the effective fields)."""
        await self.conn.executemany(
            """UPDATE delta_urls SET title_ai=COALESCE(?, title_ai), division_ai=COALESCE(?, division_ai),
               document_type_ai=COALESCE(?, document_type_ai) WHERE collection_id=? AND url=?""",
            [(i.get("title"), i.get("division"), i.get("document_type"), collection_id, i["url"])
             for i in items],
        )
        await self.conn.commit()
        return len(items)

    async def load_curated(self, collection_id: str) -> list[CuratedUrl]:
        cur = await self.conn.execute(
            "SELECT * FROM curated_urls WHERE collection_id=?", (collection_id,)
        )
        return [CuratedUrl(**dict(r)) for r in await cur.fetchall()]

    async def replace_curated(self, collection_id: str, rows: list[CuratedUrl]) -> int:
        await self.conn.execute("DELETE FROM curated_urls WHERE collection_id=?", (collection_id,))
        await self.conn.executemany(
            """INSERT INTO curated_urls (collection_id,url,scraped_title,title,division,document_type,excluded)
               VALUES (?,?,?,?,?,?,?)""",
            [(r.collection_id, r.url, r.scraped_title, r.title, r.division, r.document_type,
              int(r.excluded)) for r in rows],
        )
        await self.conn.execute(
            "UPDATE collections SET curated_count=?, updated_at=? WHERE collection_id=?",
            (len(rows), _iso(utcnow()), collection_id),
        )
        await self.conn.commit()
        return len(rows)

    # ── index runs ─────────────────────────────────────────────────────

    async def insert_index_run(self, r: IndexRun) -> IndexRun:
        await self.conn.execute(
            """INSERT INTO index_runs (run_id,collection_id,target,state,exported,external_ref,status,validation,
               validated_by,error,started_at,finished_at,started_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r.run_id, r.collection_id, r.target, r.state, r.exported, r.external_ref,
             json.dumps(r.status) if r.status else None, json.dumps(r.validation) if r.validation else None,
             r.validated_by, r.error, _iso(r.started_at), _iso(r.finished_at), r.started_by),
        )
        await self.conn.execute(
            "UPDATE collections SET last_run_id=?, updated_at=? WHERE collection_id=?",
            (r.run_id, _iso(utcnow()), r.collection_id),
        )
        await self.conn.commit()
        return r

    async def update_index_run(self, r: IndexRun) -> None:
        await self.conn.execute(
            """UPDATE index_runs SET state=?, exported=?, external_ref=?, status=?, validation=?, validated_by=?,
               error=?, finished_at=? WHERE run_id=?""",
            (r.state, r.exported, r.external_ref, json.dumps(r.status) if r.status else None,
             json.dumps(r.validation) if r.validation else None, r.validated_by, r.error, _iso(r.finished_at),
             r.run_id),
        )
        await self.conn.commit()

    @staticmethod
    def _index_run(row: Any) -> IndexRun:
        d = dict(row)
        d["status"] = json.loads(d["status"]) if d["status"] else None
        d["validation"] = json.loads(d["validation"]) if d["validation"] else None
        return IndexRun(**d)

    async def list_index_runs(self, collection_id: str, limit: int = 20) -> list[IndexRun]:
        cur = await self.conn.execute(
            "SELECT * FROM index_runs WHERE collection_id=? ORDER BY started_at DESC LIMIT ?", (collection_id, limit)
        )
        return [self._index_run(r) for r in await cur.fetchall()]

    async def get_index_run(self, run_id: str) -> IndexRun | None:
        cur = await self.conn.execute("SELECT * FROM index_runs WHERE run_id=?", (run_id,))
        row = await cur.fetchone()
        return self._index_run(row) if row else None

    async def last_index_run(self, collection_id: str, target: str | None = None) -> IndexRun | None:
        q, args = "SELECT * FROM index_runs WHERE collection_id=?", [collection_id]
        if target:
            q += " AND target=?"; args.append(target)
        cur = await self.conn.execute(q + " ORDER BY started_at DESC LIMIT 1", args)
        row = await cur.fetchone()
        return self._index_run(row) if row else None

    async def curated_export_count(self, collection_id: str) -> int:
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM curated_urls WHERE collection_id=? AND excluded=0", (collection_id,)
        )
        return (await cur.fetchone())[0]

    # ── LLM suggestions ────────────────────────────────────────────────

    async def replace_pattern_suggestions(self, collection_id: str, rows: list[dict[str, Any]]) -> int:
        await self.conn.execute(
            "DELETE FROM pattern_suggestions WHERE collection_id=? AND state='pending'", (collection_id,)
        )
        await self.conn.executemany(
            """INSERT OR IGNORE INTO pattern_suggestions (collection_id,type,match,value,rationale,matches,state,created_at)
               VALUES (?,?,?,?,?,?,'pending',?)""",
            [(collection_id, r["type"], r["match"], r.get("value"), r.get("rationale"), r.get("matches", 0),
              _iso(utcnow())) for r in rows],
        )
        await self.conn.commit()
        return len(rows)

    async def list_pattern_suggestions(self, collection_id: str, state: str | None = "pending") -> list[dict[str, Any]]:
        q = "SELECT * FROM pattern_suggestions WHERE collection_id=?"
        args: list[Any] = [collection_id]
        if state:
            q += " AND state=?"; args.append(state)
        cur = await self.conn.execute(q + " ORDER BY id", args)
        return [dict(r) for r in await cur.fetchall()]

    async def get_pattern_suggestion(self, collection_id: str, sid: int) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            "SELECT * FROM pattern_suggestions WHERE id=? AND collection_id=?", (sid, collection_id)
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def set_pattern_suggestion_state(
        self, collection_id: str, sid: int, state: str, *, actor: str | None = None
    ) -> None:
        await self.set_pattern_suggestions_state(collection_id, [sid], state, actor=actor)

    async def set_pattern_suggestions_state(
        self, collection_id: str, ids: list[int], state: str, *, actor: str | None = None
    ) -> int:
        n = 0
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            cur = await self.conn.execute(
                f"UPDATE pattern_suggestions SET state=?, decided_by=? WHERE collection_id=? AND id IN ({','.join('?' * len(chunk))})",
                (state, actor, collection_id, *chunk),
            )
            n += cur.rowcount
        await self.conn.commit()
        return n

    async def deltas_for_llm(self, collection_id: str, *, only_missing: bool = True) -> list[dict[str, Any]]:
        """Non-deleted, non-excluded delta URLs joined with dump text for classification."""
        q = """SELECT d.url, d.scraped_title AS title, substr(u.full_text, 1, 1500) AS text
               FROM delta_urls d LEFT JOIN dump_urls u ON u.collection_id=d.collection_id AND u.url=d.url
               WHERE d.collection_id=? AND d.kind!='deleted' AND d.excluded=0"""
        if only_missing:
            q += " AND d.title_ai IS NULL AND d.division_ai IS NULL AND d.document_type_ai IS NULL"
        cur = await self.conn.execute(q + " ORDER BY d.url", (collection_id,))
        return [dict(r) for r in await cur.fetchall()]

    async def deltas_with_ai(self, collection_id: str, field: str) -> list[tuple[str, str]]:
        """(url, suggested value) for every pending, non-removed URL with an AI suggestion for `field`."""
        assert field in ("title", "division", "document_type")
        cur = await self.conn.execute(
            f"SELECT url, {field}_ai FROM delta_urls WHERE collection_id=? AND kind!='deleted' AND {field}_ai IS NOT NULL ORDER BY url",
            (collection_id,),
        )
        return [(r[0], r[1]) for r in await cur.fetchall()]

    async def delta_ai_counts(self, collection_id: str) -> dict[str, int]:
        cur = await self.conn.execute(
            """SELECT SUM(title_ai IS NOT NULL), SUM(division_ai IS NOT NULL), SUM(document_type_ai IS NOT NULL)
               FROM delta_urls WHERE collection_id=? AND kind!='deleted'""",
            (collection_id,),
        )
        t, d, dt = await cur.fetchone()
        return {"title": t or 0, "division": d or 0, "document_type": dt or 0}

    async def clear_delta_ai_field(self, collection_id: str, field: str) -> int:
        assert field in ("title", "division", "document_type")
        cur = await self.conn.execute(
            f"UPDATE delta_urls SET {field}_ai=NULL WHERE collection_id=? AND {field}_ai IS NOT NULL", (collection_id,)
        )
        await self.conn.commit()
        return cur.rowcount

    async def clear_delta_ai(self, collection_id: str, url: str, field: str) -> None:
        assert field in ("title", "division", "document_type")
        await self.conn.execute(
            f"UPDATE delta_urls SET {field}_ai=NULL WHERE collection_id=? AND url=?", (collection_id, url)
        )
        await self.conn.commit()

    # ── patterns ───────────────────────────────────────────────────────

    async def insert_pattern(self, p: Pattern) -> Pattern:
        cur = await self.conn.execute(
            "INSERT INTO patterns (collection_id,type,match,value,created_at,created_by) VALUES (?,?,?,?,?,?)",
            (p.collection_id, p.type, p.match, p.value, _iso(p.created_at), p.created_by),
        )
        await self.conn.commit()
        p.id = cur.lastrowid
        return p

    async def insert_patterns(self, rows: list[Pattern]) -> int:
        """Bulk insert; rows identical to an existing (type, match) are skipped. One commit."""
        n = 0
        for p in rows:
            cur = await self.conn.execute(
                "INSERT OR IGNORE INTO patterns (collection_id,type,match,value,created_at,created_by) VALUES (?,?,?,?,?,?)",
                (p.collection_id, p.type, p.match, p.value, _iso(p.created_at), p.created_by),
            )
            n += cur.rowcount
        await self.conn.commit()
        return n

    async def delete_exact_patterns(self, collection_id: str, type_: str, matches: list[str]) -> int:
        n = 0
        for i in range(0, len(matches), 500):
            chunk = matches[i:i + 500]
            cur = await self.conn.execute(
                f"DELETE FROM patterns WHERE collection_id=? AND type=? AND match IN ({','.join('?' * len(chunk))})",
                (collection_id, type_, *chunk),
            )
            n += cur.rowcount
        await self.conn.commit()
        return n

    async def list_patterns(self, collection_id: str) -> list[Pattern]:
        cur = await self.conn.execute(
            "SELECT * FROM patterns WHERE collection_id=? ORDER BY id", (collection_id,)
        )
        return [Pattern(**dict(r)) for r in await cur.fetchall()]

    async def delete_pattern(self, collection_id: str, pattern_id: int) -> bool:
        cur = await self.conn.execute(
            "DELETE FROM patterns WHERE id=? AND collection_id=?", (pattern_id, collection_id)
        )
        await self.conn.commit()
        return cur.rowcount > 0

    # ── jobs ───────────────────────────────────────────────────────────

    async def insert_job(self, j: JobRun) -> JobRun:
        cur = await self.conn.execute(
            """INSERT INTO job_runs (collection_id,kind,state,run_id,external_ref,progress,error,
               started_at,finished_at,started_by) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                j.collection_id, j.kind, j.state, j.run_id, j.external_ref,
                json.dumps(j.progress), j.error, _iso(j.started_at), _iso(j.finished_at), j.started_by,
            ),
        )
        await self.conn.commit()
        j.id = cur.lastrowid
        return j

    async def update_job(self, j: JobRun) -> None:
        await self.conn.execute(
            """UPDATE job_runs SET state=?, run_id=?, external_ref=?, progress=?, error=?,
               finished_at=? WHERE id=?""",
            (
                j.state, j.run_id, j.external_ref, json.dumps(j.progress), j.error,
                _iso(j.finished_at), j.id,
            ),
        )
        await self.conn.commit()

    async def finish_job(self, j: JobRun, state: JobState, error: str | None = None) -> None:
        j.state, j.error, j.finished_at = state, error, utcnow()
        await self.update_job(j)

    @staticmethod
    def _job(row: Any) -> JobRun:
        d = dict(row)
        d["progress"] = json.loads(d["progress"] or "{}")
        return JobRun(**d)

    async def get_job(self, job_id: int) -> JobRun | None:
        cur = await self.conn.execute("SELECT * FROM job_runs WHERE id=?", (job_id,))
        row = await cur.fetchone()
        return self._job(row) if row else None

    async def list_jobs(self, collection_id: str, limit: int = 20) -> list[JobRun]:
        cur = await self.conn.execute(
            "SELECT * FROM job_runs WHERE collection_id=? ORDER BY id DESC LIMIT ?",
            (collection_id, limit),
        )
        return [self._job(r) for r in await cur.fetchall()]

    async def latest_job(self, collection_id: str) -> JobRun | None:
        jobs = await self.list_jobs(collection_id, limit=1)
        return jobs[0] if jobs else None

    async def latest_job_of_kind(self, collection_id: str, kind: str) -> JobRun | None:
        cur = await self.conn.execute(
            "SELECT * FROM job_runs WHERE collection_id=? AND kind=? ORDER BY id DESC LIMIT 1",
            (collection_id, str(kind)),
        )
        row = await cur.fetchone()
        return self._job(row) if row else None

    async def job_exists(self, collection_id: str, kind: str) -> bool:
        cur = await self.conn.execute(
            "SELECT 1 FROM job_runs WHERE collection_id=? AND kind=? LIMIT 1", (collection_id, str(kind))
        )
        return await cur.fetchone() is not None

    async def active_jobs(self) -> list[JobRun]:
        cur = await self.conn.execute(
            "SELECT * FROM job_runs WHERE state IN ('queued','running') ORDER BY id"
        )
        return [self._job(r) for r in await cur.fetchall()]

    async def list_recent_jobs(self, limit: int = 20, state: str | None = None) -> list[JobRun]:
        """Newest jobs across every collection, optionally only one state (e.g. 'failed')."""
        if state:
            cur = await self.conn.execute(
                "SELECT * FROM job_runs WHERE state=? ORDER BY id DESC LIMIT ?", (state, limit)
            )
        else:
            cur = await self.conn.execute("SELECT * FROM job_runs ORDER BY id DESC LIMIT ?", (limit,))
        return [self._job(r) for r in await cur.fetchall()]

    # ── users ──────────────────────────────────────────────────────────

    @staticmethod
    def _user(row: Any) -> User:
        d = dict(row)
        d["active"] = bool(d["active"])
        return User(**d)

    async def create_user(self, username: str, password_hash: str, role: Role = Role.CURATOR) -> User:
        """Raises sqlite3.IntegrityError when the username (case-insensitive) is taken."""
        now = _iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO users (username,password_hash,role,active,session_version,created_at,updated_at) "
            "VALUES (?,?,?,1,1,?,?)",
            (username, password_hash, role, now, now),
        )
        await self.conn.commit()
        return (await self.get_user(cur.lastrowid))  # type: ignore[return-value]

    async def get_user(self, user_id: int) -> User | None:
        cur = await self.conn.execute("SELECT * FROM users WHERE id=?", (user_id,))
        row = await cur.fetchone()
        return self._user(row) if row else None

    async def get_user_by_username(self, username: str) -> User | None:
        cur = await self.conn.execute("SELECT * FROM users WHERE username=? COLLATE NOCASE", (username,))
        row = await cur.fetchone()
        return self._user(row) if row else None

    async def list_users(self) -> list[User]:
        cur = await self.conn.execute("SELECT * FROM users ORDER BY username COLLATE NOCASE")
        return [self._user(r) for r in await cur.fetchall()]

    async def count_users(self) -> int:
        cur = await self.conn.execute("SELECT COUNT(*) FROM users")
        return (await cur.fetchone())[0]

    async def set_password(self, user_id: int, password_hash: str) -> None:
        """Also invalidates every existing session of that user."""
        await self.conn.execute(
            "UPDATE users SET password_hash=?, session_version=session_version+1, updated_at=? WHERE id=?",
            (password_hash, _iso(utcnow()), user_id),
        )
        await self.conn.commit()

    async def set_role(self, user_id: int, role: Role) -> None:
        await self.conn.execute(
            "UPDATE users SET role=?, updated_at=? WHERE id=?", (role, _iso(utcnow()), user_id)
        )
        await self.conn.commit()

    async def set_active(self, user_id: int, active: bool) -> None:
        await self.conn.execute(
            "UPDATE users SET active=?, updated_at=? WHERE id=?", (int(active), _iso(utcnow()), user_id)
        )
        await self.conn.commit()

    # ── audit ledger ───────────────────────────────────────────────────

    async def audit(
        self, actor: str, action: str, collection_id: str | None = None, detail: str | None = None
    ) -> None:
        await self.conn.execute(
            "INSERT INTO audit_log (at,actor,collection_id,action,detail) VALUES (?,?,?,?,?)",
            (_iso(utcnow()), actor, collection_id, action, detail),
        )
        await self.conn.commit()

    async def list_audit(self, collection_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if collection_id is None:
            cur = await self.conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,))
        else:
            cur = await self.conn.execute(
                "SELECT * FROM audit_log WHERE collection_id=? ORDER BY id DESC LIMIT ?", (collection_id, limit)
            )
        return [dict(r) for r in await cur.fetchall()]
