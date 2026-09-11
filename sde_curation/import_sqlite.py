"""One-off cutover: copy a SQLite-era `engine.db` into the PostgreSQL database.

    python -m sde_curation.import_sqlite /data/engine.db            # target from DATABASE_URL / DB_*
    python -m sde_curation.import_sqlite engine.db --dsn postgresql://… [--replace]

Reads the file read-only, refuses a target that already holds data (unless --replace, which
truncates every table first), copies every table in dependency order with COPY, keeping ids,
then runs the backfills the SQLite `_migrate` used to apply on boot and resets the identity
sequences. Meant to run from `ecs exec` inside the new task, which has EFS and the DB env.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from .schema import TABLES, migrate_sync

BOOL_COLS = {
    ("collections", "needs_recuration"), ("delta_urls", "excluded"), ("delta_urls", "content_changed"),
    ("curated_urls", "excluded"), ("users", "active"),
}
TS_COLS = {
    ("collections", "last_scraped_at"), ("collections", "created_at"), ("collections", "updated_at"),
    ("status_history", "at"), ("patterns", "created_at"), ("pattern_suggestions", "created_at"),
    ("index_runs", "started_at"), ("index_runs", "finished_at"), ("job_runs", "started_at"),
    ("job_runs", "finished_at"), ("users", "created_at"), ("users", "updated_at"), ("audit_log", "at"),
}
JSON_COLS = {("index_runs", "status"), ("index_runs", "validation"), ("job_runs", "progress")}
IDENTITY_TABLES = ("status_history", "patterns", "pattern_suggestions", "job_runs", "users", "audit_log")

# Columns the last SQLite release added at boot. A file without them was never opened by that
# release; the importer does not replay ALTERs, so ask for a boot on the old version first.
REQUIRED_COLS = {
    "collections": {"curation_stage", "recuration_reason", "last_scraped_at", "created_by", "last_run_id"},
    "status_history": {"actor"}, "dump_urls": {"content_hash"},
    "delta_urls": {"title_ai", "title_ai_conf", "ai_model", "ai_content_hash", "edited_by", "content_changed"},
    "curated_urls": {"content_hash", "edited_by"}, "patterns": {"source", "created_by"},
    "pattern_suggestions": {"accepted_as", "source", "decided_by"},
    "index_runs": {"validated_by", "started_by"}, "job_runs": {"started_by"}, "users": set(), "audit_log": set(),
}

# Portable versions of the data fix-ups the SQLite `_migrate` ran on every boot.
BACKFILLS = (
    # SQLite never stored the approved text on the curated rows; the export shipped the dump text,
    # so the dump text is what the index holds for them
    """UPDATE curated_urls c SET full_text = d.full_text FROM dump_urls d
       WHERE d.collection_id = c.collection_id AND d.url = c.url AND c.full_text IS NULL""",
    # rules that came from an accepted suggestion keep their origin instead of reading as SME
    """UPDATE patterns SET source = s.source FROM pattern_suggestions s
       WHERE s.collection_id = patterns.collection_id AND s.type = patterns.type
         AND s.match = patterns.match AND s.state = 'accepted' AND patterns.source = 'sme'""",
    # per-URL AI accepts from before `source` existed left an exact audit line
    """UPDATE patterns SET source = 'llm'
       WHERE source = 'sme' AND value IS NOT NULL AND EXISTS (
         SELECT 1 FROM audit_log a
         WHERE a.collection_id = patterns.collection_id AND a.action = 'ai.accept'
           AND a.detail = patterns.type || ' ' || patterns.match || ' → ' || patterns.value)""",
    # rows from before last_scraped_at existed: take the last successful scrape job
    """UPDATE collections SET last_scraped_at = (
         SELECT MAX(finished_at) FROM job_runs j
         WHERE j.collection_id = collections.collection_id AND j.kind='scrape' AND j.state='succeeded')
       WHERE last_scraped_at IS NULL AND dump_count > 0""",
    # collections already curating when stages were introduced start at the first stage
    "UPDATE collections SET curation_stage='exclusions' WHERE status='curating' AND curation_stage IS NULL",
    "UPDATE collections SET curation_stage='exclusions' WHERE curation_stage='scope'",
)


class ImportError_(Exception):
    pass


def _ts(v: Any) -> datetime | None:
    if v is None or v == "":
        return None
    dt = datetime.fromisoformat(v)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _convert(table: str, col: str, v: Any) -> Any:
    key = (table, col)
    if key in BOOL_COLS:
        return bool(v)
    if key in TS_COLS:
        return _ts(v)
    if key in JSON_COLS:
        return Jsonb(json.loads(v)) if v else None
    return v


def _source_columns(src: sqlite3.Connection) -> dict[str, list[str]]:
    have = {r[0] for r in src.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    missing = [t for t in TABLES if t not in have]
    if missing:
        raise ImportError_(f"source has no table(s) {missing}: it was never opened by the last SQLite release")
    cols = {t: [r[1] for r in src.execute(f"PRAGMA table_info({t})")] for t in TABLES}
    for t, needed in REQUIRED_COLS.items():
        lacking = needed - set(cols[t])
        if lacking:
            raise ImportError_(
                f"source table {t} lacks {sorted(lacking)}: start the last SQLite release once against this "
                "file so its boot migration runs, then import again"
            )
    return cols


def run(sqlite_path: str | Path, dsn: str, *, replace: bool = False,
        log: Callable[[str], None] = lambda s: None) -> dict[str, int]:
    """Copy `sqlite_path` into the database at `dsn`; returns rows copied per table."""
    path = Path(sqlite_path)
    if not path.is_file():
        raise ImportError_(f"{path}: no such file")
    src = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row
    counts: dict[str, int] = {}
    try:
        src_cols = _source_columns(src)
        with psycopg.connect(dsn) as pg:
            migrate_sync(pg)
            with pg.transaction():
                existing = sum(pg.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in TABLES)
                if existing and not replace:
                    raise ImportError_(f"target already holds {existing} rows; pass --replace to wipe it first")
                if existing:
                    pg.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE")
                    log(f"truncated {existing} existing rows")
                for t in TABLES:
                    pg_cols = [r[0] for r in pg.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema='public'"
                        " AND table_name=%s ORDER BY ordinal_position", (t,))]
                    cols = [c for c in pg_cols if c in src_cols[t]]
                    n = 0
                    with pg.cursor() as cur, cur.copy(f"COPY {t} ({','.join(cols)}) FROM STDIN") as copy:
                        for row in src.execute(f"SELECT {','.join(cols)} FROM {t}"):
                            copy.write_row(tuple(_convert(t, c, row[c]) for c in cols))
                            n += 1
                    counts[t] = n
                    log(f"{t}: {n} rows")
                for sql in BACKFILLS:
                    pg.execute(sql)
                for t in IDENTITY_TABLES:
                    pg.execute(
                        f"SELECT setval(pg_get_serial_sequence('{t}', 'id'), COALESCE(MAX(id), 1), MAX(id) IS NOT NULL)"
                        f" FROM {t}"
                    )
    finally:
        src.close()
    return counts


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sqlite_path", help="the engine.db to read (opened read-only)")
    ap.add_argument("--dsn", help="target PostgreSQL URL (default: DATABASE_URL or DB_* from the environment)")
    ap.add_argument("--replace", action="store_true", help="truncate every table of the target first")
    args = ap.parse_args(argv)
    if args.dsn:
        dsn = args.dsn
    else:
        from .config import Settings

        dsn = Settings().resolved_database_url
    try:
        counts = run(args.sqlite_path, dsn, replace=args.replace, log=print)
    except ImportError_ as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    print(f"imported {sum(counts.values())} rows from {args.sqlite_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
