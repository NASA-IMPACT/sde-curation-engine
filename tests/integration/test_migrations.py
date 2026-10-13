"""I1: migrations (TEST-STRATEGY-2026-10-09.md section 4). An empty database migrates to the latest
version, and a second run changes nothing. Dev's database, with its data, upgrades when the engine
starts, and the engine then serves it.

Dev's code is at V10 (the last dev merge, d8cbfbb); its database also has V11 from the 2026-10-06
stress deploy. So both starting points are tested. Each test uses its own database on the test
server, dropped at the end. The V2 and V12 data cases are in test_db_pg.py.
"""

from __future__ import annotations

import asyncio

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from sde_curation.config import Settings
from sde_curation.engine.urls import canonical_key
from sde_curation.schema import MIGRATIONS, TABLES, migrate_sync
from sde_curation.web.app import create_app
from tests.support.journeys import every_page_renders

LATEST = MIGRATIONS[-1][0]
DEV_CODE_VERSION = 10  # sde_curation/schema.py at d8cbfbb
CID = "dev.org"
STALE_EXCLUDED_COUNT = 999  # V11 on dev, then code that never updated it (V12 forgets it)


@pytest.fixture
def new_database(pg_url):
    """An empty database of its own on the test server; its URL."""
    admin = pg_url.rsplit("/", 1)[0] + "/postgres"
    names: list[str] = []

    def make(name: str) -> str:
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
            conn.execute(f"CREATE DATABASE {name}")
        names.append(name)
        return pg_url.rsplit("/", 1)[0] + "/" + name

    yield make
    with psycopg.connect(admin, autocommit=True) as conn:
        for name in names:
            conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def migrate_to(conn, version: int) -> None:
    """The schema as an engine at `version` left it."""
    conn.execute("CREATE TABLE schema_version (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
    for v, sql in MIGRATIONS:
        if v <= version:
            conn.execute(sql)
            conn.execute("INSERT INTO schema_version (version) VALUES (%s)", (v,))


def schema_shape(conn) -> list[tuple]:
    """Every column, index and table option: what a migration can change."""
    return (conn.execute("SELECT table_name, column_name, data_type, is_nullable, column_default"
                         " FROM information_schema.columns WHERE table_schema='public' ORDER BY 1, 2").fetchall()
            + conn.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY 1").fetchall()
            + conn.execute("SELECT relname, reloptions::text FROM pg_class WHERE relnamespace='public'::regnamespace"
                           " AND relkind='r' ORDER BY 1").fetchall())


def url(page: int) -> str:
    return f"https://{CID}/p{page}"


def seed_dev(conn, version: int) -> dict[str, int]:
    """A collection as dev holds one: promoted once (p1–p3 curated), crawled again (p1–p5; p4 kept out
    by an exclude glob, p5 new, p6 failed), Start curating pressed (p5 queued), a hand title on p1.
    Returns each table's row count."""
    excluded = ", excluded_count" if version >= 11 else ""
    conn.execute(f"""
      INSERT INTO collections (collection_id, name, seed_url, division, connector, max_pages, status, created_at,
                               updated_at, dump_count, delta_count, curated_count, curated_rows, curated_by,
                               index_key{excluded})
      VALUES ('{CID}', 'Dev', 'https://{CID}', 'Heliophysics', 'crawler2', 10, 'curating', now(), now(),
              5, 1, 3, 3, 'alice', 'dev_org'{f", {STALE_EXCLUDED_COUNT}" if version >= 11 else ""})""")
    conn.execute(f"INSERT INTO status_history (collection_id, new_status, at, actor)"
                 f" VALUES ('{CID}', 'curating', now(), 'alice')")
    for i in range(1, 6):
        conn.execute("INSERT INTO page_text VALUES (%s, %s, %s)", (CID, f"h{i}", f"text of page {i}"))
        conn.execute("INSERT INTO dump_urls (collection_id, url, scraped_title, content_type, depth, content_hash)"
                     " VALUES (%s, %s, %s, 'text/html', 1, %s)", (CID, url(i), f"Page {i}", f"h{i}"))
    conn.execute("INSERT INTO dump_failures VALUES (%s, %s, 'http_404', 404, 'not found')", (CID, url(6)))
    for i in range(1, 4):
        conn.execute("INSERT INTO curated_urls (collection_id, url, scraped_title, title, division, document_type,"
                     " content_hash, edited_by) VALUES (%s, %s, %s, %s, 'Heliophysics', 'Documentation', %s, 'sme')",
                     (CID, url(i), f"Page {i}", "Hand title" if i == 1 else f"Page {i}", f"h{i}"))
    conn.execute("INSERT INTO delta_urls (collection_id, url, kind, scraped_title, title, division, document_type)"
                 " VALUES (%s, %s, 'new', 'Page 5', 'Page 5', 'Heliophysics', 'Documentation')", (CID, url(5)))
    conn.execute("INSERT INTO patterns (id, collection_id, type, match, created_at, created_by)"
                 " VALUES (1, %s, 'exclude', '*/p4', now(), 'alice')", (CID,))
    conn.execute("INSERT INTO patterns (id, collection_id, type, match, value, created_at, created_by)"
                 " VALUES (2, %s, 'title', %s, 'Hand title', now(), 'alice')", (CID, url(1)))
    conn.execute("INSERT INTO patterns (id, collection_id, type, match, value, created_at, created_by)"
                 " VALUES (3, %s, 'document_type', '*', 'Documentation', now(), 'alice')", (CID,))
    conn.execute("SELECT setval(pg_get_serial_sequence('patterns', 'id'), 3)")
    conn.execute("INSERT INTO pattern_effects VALUES (1, %s, %s, 'excluded'), (2, %s, %s, 'title')",
                 (CID, url(4), CID, url(1)))
    conn.execute("INSERT INTO pattern_suggestions (collection_id, type, match, rationale, created_at)"
                 " VALUES (%s, 'exclude', '*/tag/*', 'site chrome', now())", (CID,))
    conn.execute("INSERT INTO index_runs (run_id, collection_id, target, state, exported, started_at, finished_at)"
                 " VALUES ('run-1', %s, 'test', 'succeeded', 3, now(), now())", (CID,))
    conn.execute("INSERT INTO job_runs (collection_id, kind, state, started_at, finished_at)"
                 " VALUES (%s, 'scrape', 'succeeded', now(), now())", (CID,))
    conn.execute("INSERT INTO users (username, password_hash, role, created_at, updated_at)"
                 " VALUES ('alice', 'x', 'curator', now(), now())")
    conn.execute("INSERT INTO audit_log (at, actor, action, collection_id) VALUES (now(), 'alice', 'recompute', %s)",
                 (CID,))
    return row_counts(conn)


def row_counts(conn) -> dict[str, int]:
    return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            for t in TABLES if t != "collection_stats"}


def test_an_empty_database_gets_every_migration_once(new_database):
    db = new_database("mig_empty")
    with psycopg.connect(db, autocommit=True) as conn:
        assert migrate_sync(conn) == LATEST
        applied = conn.execute("SELECT version FROM schema_version ORDER BY 1").fetchall()
        assert [v for (v,) in applied] == [v for v, _ in MIGRATIONS]
        shape = schema_shape(conn)

        assert migrate_sync(conn) == LATEST  # a second start-up: nothing to do

        assert conn.execute("SELECT count(*) FROM schema_version").fetchone()[0] == len(MIGRATIONS)
        assert schema_shape(conn) == shape


@pytest.mark.parametrize("dev_version", [DEV_CODE_VERSION, 11], ids=["dev-code-V10", "dev-database-V11"])
async def test_devs_database_upgrades_on_start_up_and_the_engine_serves_it(new_database, tmp_path, dev_version):
    db = new_database(f"mig_dev_v{dev_version}")
    with psycopg.connect(db, autocommit=True) as conn:
        migrate_to(conn, dev_version)
        before = seed_dev(conn, dev_version)

    app = create_app(Settings(data_dir=tmp_path, llm_provider="fake", database_url=db))
    async with app.router.lifespan_context(app), AsyncClient(transport=ASGITransport(app=app),
                                                             base_url="http://t") as c:
        c.app = app
        await asyncio.wait_for(app.state.key_backfill, 30)

        with psycopg.connect(db) as conn:
            assert conn.execute("SELECT max(version) FROM schema_version").fetchone()[0] == LATEST
            assert row_counts(conn) == before, "the upgrade lost or added rows"
            for table, column in (("dump_urls", "url"), ("curated_urls", "url")):
                rows = conn.execute(f"SELECT {column}, canonical_key FROM {table}").fetchall()
                assert all(key == canonical_key(u) for u, key in rows), f"{table} keys not filled in: {rows}"
            keys = dict(conn.execute("SELECT match, canonical_key FROM patterns").fetchall())
            assert keys == {"*/p4": None, url(1): canonical_key(url(1)), "*": None}
            stored = conn.execute("SELECT excluded_count, review_round FROM collections").fetchone()
            assert stored == (None, False), "a stale excluded count survived, or a review round opened"

        col = (await c.get(f"/api/collections/{CID}")).json()
        assert (col["status"], col["dump_count"], col["curated_count"]) == ("curating", 5, 3)
        await every_page_renders(c, CID, "after the upgrade")

        # Start curating gives the same delta set dev had: the rules mean what they meant before.
        assert (await c.post(f"/api/collections/{CID}/recompute")).status_code == 200
        delta = (await c.get(f"/api/collections/{CID}/delta")).json()["items"]
        assert [(d["url"], d["kind"]) for d in delta] == [(url(5), "new")]
        await every_page_renders(c, CID, "after Start curating")

        # A per-URL edit works on upgraded rows (the scoped recompute needs their keys).
        r = await c.post(f"/api/collections/{CID}/patterns", json={"type": "title", "match": url(2), "value": "Two"})
        assert r.status_code == 201, r.text
        delta = (await c.get(f"/api/collections/{CID}/delta")).json()["items"]
        assert {d["url"]: d["title"] for d in delta} == {url(2): "Two", url(5): None}  # p5: no title rule
        await every_page_renders(c, CID, "after a per-URL edit")
