"""The SQLite → PostgreSQL cutover importer (`python -m sde_curation.import_sqlite`)."""

import os
import sqlite3
from datetime import UTC, datetime

import pytest

from sde_curation.db import Database
from sde_curation.import_sqlite import ImportError_, main, run
from sde_curation.models import Pattern, PatternType

# The schema the last SQLite release created (final shape after its boot migration).
SQLITE_SCHEMA = """
CREATE TABLE collections (collection_id TEXT PRIMARY KEY, name TEXT NOT NULL, seed_url TEXT NOT NULL,
  division TEXT NOT NULL, document_type TEXT, connector TEXT NOT NULL, max_pages INTEGER NOT NULL,
  status TEXT NOT NULL, curation_stage TEXT, needs_recuration INTEGER NOT NULL DEFAULT 0, recuration_reason TEXT,
  last_scraped_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, dump_count INTEGER NOT NULL DEFAULT 0,
  delta_count INTEGER NOT NULL DEFAULT 0, curated_count INTEGER NOT NULL DEFAULT 0, last_run_id TEXT, created_by TEXT);
CREATE TABLE status_history (id INTEGER PRIMARY KEY AUTOINCREMENT, collection_id TEXT NOT NULL, old_status TEXT,
  new_status TEXT NOT NULL, note TEXT, at TEXT NOT NULL, actor TEXT);
CREATE TABLE dump_urls (collection_id TEXT NOT NULL, url TEXT NOT NULL, scraped_title TEXT, full_text TEXT,
  content_type TEXT, depth INTEGER, content_hash TEXT, PRIMARY KEY (collection_id, url));
CREATE TABLE delta_urls (collection_id TEXT NOT NULL, url TEXT NOT NULL, kind TEXT NOT NULL, scraped_title TEXT,
  title TEXT, division TEXT, document_type TEXT, excluded INTEGER NOT NULL DEFAULT 0,
  content_changed INTEGER NOT NULL DEFAULT 0, edited_by TEXT, title_ai TEXT, division_ai TEXT, document_type_ai TEXT,
  title_ai_conf TEXT, division_ai_conf TEXT, document_type_ai_conf TEXT, ai_model TEXT, ai_content_hash TEXT,
  PRIMARY KEY (collection_id, url));
CREATE TABLE curated_urls (collection_id TEXT NOT NULL, url TEXT NOT NULL, scraped_title TEXT, title TEXT,
  division TEXT, document_type TEXT, excluded INTEGER NOT NULL DEFAULT 0, content_hash TEXT, edited_by TEXT,
  PRIMARY KEY (collection_id, url));
CREATE TABLE patterns (id INTEGER PRIMARY KEY AUTOINCREMENT, collection_id TEXT NOT NULL, type TEXT NOT NULL,
  match TEXT NOT NULL, value TEXT, created_at TEXT NOT NULL, created_by TEXT, source TEXT NOT NULL DEFAULT 'sme',
  UNIQUE (collection_id, type, match));
CREATE TABLE pattern_effects (pattern_id INTEGER NOT NULL, collection_id TEXT NOT NULL, url TEXT NOT NULL,
  field TEXT NOT NULL, PRIMARY KEY (pattern_id, url, field));
CREATE TABLE pattern_suggestions (id INTEGER PRIMARY KEY AUTOINCREMENT, collection_id TEXT NOT NULL,
  type TEXT NOT NULL, match TEXT NOT NULL, value TEXT, rationale TEXT, matches INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'pending', source TEXT NOT NULL DEFAULT 'llm', created_at TEXT NOT NULL,
  decided_by TEXT, accepted_as TEXT, UNIQUE (collection_id, type, match));
CREATE TABLE index_runs (run_id TEXT PRIMARY KEY, collection_id TEXT NOT NULL, target TEXT NOT NULL,
  state TEXT NOT NULL, exported INTEGER NOT NULL DEFAULT 0, external_ref TEXT, status TEXT, validation TEXT,
  validated_by TEXT, error TEXT, started_at TEXT NOT NULL, finished_at TEXT, started_by TEXT);
CREATE TABLE job_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, collection_id TEXT NOT NULL, kind TEXT NOT NULL,
  state TEXT NOT NULL, run_id TEXT, external_ref TEXT, progress TEXT NOT NULL DEFAULT '{}', error TEXT,
  started_at TEXT NOT NULL, finished_at TEXT, started_by TEXT);
CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE COLLATE NOCASE,
  password_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'curator', active INTEGER NOT NULL DEFAULT 1,
  session_version INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL,
  collection_id TEXT, action TEXT NOT NULL, detail TEXT);
"""
T0 = "2026-01-01T00:00:00+00:00"
T1 = "2026-02-01T12:30:00+00:00"


def make_source(path):
    con = sqlite3.connect(path)
    con.executescript(SQLITE_SCHEMA)
    con.executescript(f"""
        INSERT INTO collections (collection_id,name,seed_url,division,connector,max_pages,status,needs_recuration,
          created_at,updated_at,dump_count,delta_count,curated_count,created_by)
          VALUES ('k','K','https://k','General','crawler2',10,'curating',1,'{T0}','{T0}',2,1,1,'alice');
        INSERT INTO status_history (collection_id,old_status,new_status,note,at,actor) VALUES
          ('k',NULL,'backlog','created','{T0}','alice'), ('k','backlog','scraped',NULL,'{T1}','system');
        INSERT INTO dump_urls VALUES ('k','https://k/p','P','text',NULL,0,'h1'), ('k','https://k/q','Q','more',NULL,1,'h2');
        INSERT INTO delta_urls (collection_id,url,kind,scraped_title,excluded,content_changed,title_ai,title_ai_conf)
          VALUES ('k','https://k/q','new','Q',1,0,'Q title','high');
        INSERT INTO curated_urls VALUES ('k','https://k/p','P','P title','Heliophysics',NULL,0,'h1','sme');
        INSERT INTO patterns (collection_id,type,match,value,created_at) VALUES
          ('k','exclude','*/login*',NULL,'{T0}'), ('k','exclude','*/feed*',NULL,'{T0}'), ('k','title','https://k/p','T','{T0}'),
          ('k','division','https://k/q','Heliophysics','{T0}');
        INSERT INTO pattern_effects VALUES (3,'k','https://k/p','title');
        INSERT INTO pattern_suggestions (collection_id,type,match,state,source,created_at) VALUES
          ('k','exclude','*/login*','accepted','global','{T0}'), ('k','exclude','*/feed*','accepted','llm','{T0}'),
          ('k','exclude','*/tag*','rejected','llm','{T0}');
        INSERT INTO index_runs (run_id,collection_id,target,state,exported,status,started_at,finished_at)
          VALUES ('r1','k','test','succeeded',1,'{{"indexed": 1}}','{T0}','{T1}');
        INSERT INTO job_runs (collection_id,kind,state,progress,started_at,finished_at,started_by)
          VALUES ('k','scrape','succeeded','{{"docs": 2}}','{T0}','{T1}','alice');
        INSERT INTO users (username,password_hash,role,active,session_version,created_at,updated_at)
          VALUES ('Alice','x','admin',0,3,'{T0}','{T0}');
        INSERT INTO audit_log (at,actor,collection_id,action,detail) VALUES ('{T0}','alice','k','ai.accept','division https://k/q → Heliophysics');
    """)
    con.commit(); con.close()


async def test_import_copies_converts_and_backfills(tmp_path):
    src = tmp_path / "engine.db"
    make_source(src)
    dsn = os.environ["DATABASE_URL"]
    counts = run(src, dsn)
    assert counts == {"collections": 1, "status_history": 2, "dump_urls": 2, "delta_urls": 1, "curated_urls": 1,
                      "patterns": 4, "pattern_effects": 1, "pattern_suggestions": 3, "index_runs": 1,
                      "job_runs": 1, "users": 1, "audit_log": 1}
    db = await Database(dsn).connect()
    try:
        c = await db.get_collection("k")
        assert c.needs_recuration is True and c.created_at == datetime(2026, 1, 1, tzinfo=UTC)
        assert c.curation_stage == "exclusions"  # curating without a stage → first stage
        assert c.last_scraped_at == datetime(2026, 2, 1, 12, 30, tzinfo=UTC)  # from the scrape job
        # accepted suggestions and the audit line lift rule sources (what the SQLite boot used to do)
        assert {p.match: str(p.source) for p in await db.list_patterns("k")} == {
            "*/login*": "global", "*/feed*": "llm", "https://k/p": "sme", "https://k/q": "llm"}
        d = await db.get_delta("k", "https://k/q")
        assert d.excluded is True and d.content_changed is False and d.title_ai_conf == "high"
        job = (await db.list_jobs("k"))[0]
        assert job.progress == {"docs": 2} and job.finished_at.tzinfo is not None
        assert (await db.get_index_run("r1")).status == {"indexed": 1}
        u = await db.get_user_by_username("alice")
        assert u.username == "Alice" and u.active is False and u.session_version == 3
        assert (await db.status_history("k"))[1].actor == "system"
        assert (await db.effects_for("k", ["https://k/p"]))["https://k/p"]["title"].startswith("title https://k/p → T")
        # identity sequences continue after the imported ids
        p = await db.insert_pattern(Pattern(collection_id="k", type=PatternType.EXCLUDE, match="*/new*"))
        assert p.id == 5
        assert (await db.list_audit("k"))[0]["at"] == datetime(2026, 1, 1, tzinfo=UTC)
    finally:
        await db.close()
    # a second run refuses a populated target unless told to replace it
    with pytest.raises(ImportError_, match="already holds"):
        run(src, dsn)
    assert run(src, dsn, replace=True)["patterns"] == 4
    assert main([str(src), "--dsn", dsn]) == 2  # same refusal through the CLI
    assert main([str(src), "--dsn", dsn, "--replace"]) == 0


def test_import_rejects_pre_migration_file(tmp_path):
    src = tmp_path / "old.db"
    con = sqlite3.connect(src)
    con.executescript(SQLITE_SCHEMA.replace(", source TEXT NOT NULL DEFAULT 'sme',", ","))
    con.close()
    with pytest.raises(ImportError_, match="patterns lacks \\['source'\\]"):
        run(src, os.environ["DATABASE_URL"])
    with pytest.raises(ImportError_, match="no such file"):
        run(tmp_path / "missing.db", os.environ["DATABASE_URL"])
