"""The SQLite → PostgreSQL cutover importer (`python -m sde_curation.import_sqlite`)."""



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
        # accepted suggestions and the audit line lift rule sources (what the SQLite boot used to do)
        # identity sequences continue after the imported ids
    # a second run refuses a populated target unless told to replace it
