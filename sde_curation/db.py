"""SQLite state store (aiosqlite). Schema is created if missing; bulk ops use executemany."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from .models import (
    Collection,
    JobRun,
    JobState,
    Pattern,
    Status,
    StatusHistory,
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
  needs_recuration INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  dump_count INTEGER NOT NULL DEFAULT 0,
  delta_count INTEGER NOT NULL DEFAULT 0,
  curated_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS status_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  collection_id TEXT NOT NULL REFERENCES collections(collection_id) ON DELETE CASCADE,
  old_status TEXT,
  new_status TEXT NOT NULL,
  note TEXT,
  at TEXT NOT NULL
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
  title_ml TEXT,
  division_ml TEXT,
  document_type_ml TEXT,
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
  UNIQUE (collection_id, type, match)
);

CREATE TABLE IF NOT EXISTS pattern_effects (
  pattern_id INTEGER NOT NULL REFERENCES patterns(id) ON DELETE CASCADE,
  collection_id TEXT NOT NULL,
  url TEXT NOT NULL,
  field TEXT NOT NULL,
  PRIMARY KEY (pattern_id, url, field)
);

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
  finished_at TEXT
);
CREATE INDEX IF NOT EXISTS job_runs_coll ON job_runs(collection_id, id DESC);
"""


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


class Database:
    def __init__(self, path: Path | str):
        self.path = str(path)
        self._conn: aiosqlite.Connection | None = None

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
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()
        return self

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
               max_pages,status,needs_recuration,created_at,updated_at,dump_count,delta_count,curated_count)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                c.collection_id, c.name, c.seed_url, c.division, c.document_type, c.connector,
                c.max_pages, c.status, int(c.needs_recuration), _iso(c.created_at),
                _iso(c.updated_at), c.dump_count, c.delta_count, c.curated_count,
            ),
        )
        await self.conn.execute(
            "INSERT INTO status_history (collection_id,old_status,new_status,note,at) VALUES (?,?,?,?,?)",
            (c.collection_id, None, c.status, "created", _iso(utcnow())),
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
        self, collection_id: str, new: Status, note: str | None = None, *, force: bool = False
    ) -> Collection:
        c = await self.get_collection(collection_id)
        if c is None:
            raise KeyError(collection_id)
        if not force:
            check_transition(c.status, new)
        now = utcnow()
        await self.conn.execute(
            "UPDATE collections SET status=?, updated_at=? WHERE collection_id=?",
            (new, _iso(now), collection_id),
        )
        await self.conn.execute(
            "INSERT INTO status_history (collection_id,old_status,new_status,note,at) VALUES (?,?,?,?,?)",
            (collection_id, c.status, new, note, _iso(now)),
        )
        await self.conn.commit()
        c.status, c.updated_at = new, now
        return c

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

    # ── patterns ───────────────────────────────────────────────────────

    async def insert_pattern(self, p: Pattern) -> Pattern:
        cur = await self.conn.execute(
            "INSERT INTO patterns (collection_id,type,match,value,created_at) VALUES (?,?,?,?,?)",
            (p.collection_id, p.type, p.match, p.value, _iso(p.created_at)),
        )
        await self.conn.commit()
        p.id = cur.lastrowid
        return p

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
               started_at,finished_at) VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                j.collection_id, j.kind, j.state, j.run_id, j.external_ref,
                json.dumps(j.progress), j.error, _iso(j.started_at), _iso(j.finished_at),
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

    async def active_jobs(self) -> list[JobRun]:
        cur = await self.conn.execute(
            "SELECT * FROM job_runs WHERE state IN ('queued','running') ORDER BY id"
        )
        return [self._job(r) for r in await cur.fetchall()]
