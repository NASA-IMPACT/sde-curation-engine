"""Behaviour that the PostgreSQL store must keep from the SQLite days, plus what the pool adds."""
import asyncio
import os
from itertools import pairwise

import pytest

from sde_curation.db import ConflictError, Database
from sde_curation.models import Collection, Division, DumpUrl, Pattern, PatternType, Role, Status


@pytest.fixture
async def db():
    d = await Database(os.environ["DATABASE_URL"], pool_size=4).connect()
    yield d
    await d.close()


async def coll(db, cid="c1"):
    return await db.insert_collection(Collection(
        collection_id=cid, name=cid, seed_url=f"https://{cid}", division=Division.GENERAL, connector="crawler2", max_pages=5,
    ))


async def test_concurrent_transitions_are_serialised(db):
    """Requests racing from `scraped` to `curating` and back to `backlog`: the row lock makes every
    later transaction see the winner's state, so the losers are refused and the history chains."""
    await coll(db)
    await db.set_status("c1", Status.SCRAPED)
    targets = [Status.CURATING, Status.BACKLOG] * 3
    results = await asyncio.gather(
        *(db.set_status("c1", t, actor=f"t{i}") for i, t in enumerate(targets)), return_exceptions=True,
    )
    winners = [r for r in results if isinstance(r, Collection)]
    assert len(winners) == 3 and len({w.status for w in winners}) == 1  # one target, three same-state no-ops
    assert sum(isinstance(r, ValueError) for r in results) == 3
    hist = await db.status_history("c1")
    assert all(b.old_status == a.new_status for a, b in pairwise(hist))
    assert (await db.get_collection("c1")).status == winners[0].status


async def test_bulk_pattern_insert_counts_only_new_rows(db):
    await coll(db)
    rows = [Pattern(collection_id="c1", type=PatternType.EXCLUDE, match=f"*/x{i}*") for i in range(3)]
    assert await db.insert_patterns(rows) == 3
    assert await db.insert_patterns(rows + [Pattern(collection_id="c1", type=PatternType.EXCLUDE, match="*/y*")]) == 1
    with pytest.raises(ConflictError):
        await db.insert_pattern(Pattern(collection_id="c1", type=PatternType.EXCLUDE, match="*/y*"))
    assert await db.delete_exact_patterns("c1", "exclude", [f"*/x{i}*" for i in range(3)] + ["nope"]) == 3
    sugg = [{"type": "exclude", "match": f"*/s{i}*", "matches": i} for i in range(5)]
    assert await db.add_pattern_suggestions("c1", sugg) == 5
    assert await db.add_pattern_suggestions("c1", sugg[:2] + [{"type": "exclude", "match": "*/s9*"}]) == 1
    ids = [s["id"] for s in await db.list_pattern_suggestions("c1")]
    assert await db.set_pattern_suggestions_state("c1", ids + [10**6], "rejected", actor="a") == 6


async def test_large_in_lists_need_no_chunking(db):
    await coll(db)
    urls = [f"https://c1/p{i:05d}" for i in range(1200)]
    await db.replace_dump("c1", [DumpUrl(collection_id="c1", url=u) for u in urls])
    assert (await db.get_collection("c1")).dump_count == 1200
    assert await db.urls_with_deltas("c1", urls) == set()
    assert await db.effects_for("c1", urls) == {}


async def test_usernames_are_case_insensitive(db):
    u = await db.create_user("Alice", "h", Role.ADMIN)
    assert u.id and u.active is True and u.role is Role.ADMIN
    with pytest.raises(ConflictError):
        await db.create_user("alice", "h")
    assert (await db.get_user_by_username("ALICE")).id == u.id
    await db.create_user("bob", "h")
    assert [x.username for x in await db.list_users()] == ["Alice", "bob"]


async def test_search_is_case_insensitive(db):
    await coll(db)
    await db.replace_dump("c1", [DumpUrl(collection_id="c1", url="https://c1/Docs/A", scraped_title="Title"),
                                 DumpUrl(collection_id="c1", url="https://c1/other")])
    rows, total = await db.list_dump("c1", q="docs")
    assert total == 1 and rows[0]["url"] == "https://c1/Docs/A" and rows[0]["in_curated"] == 0
    rows, total = await db.list_dump("c1", q="TITLE")
    assert total == 1


async def test_health_and_raw_helpers(db):
    assert await db.ping() is True
    await coll(db)
    assert await db.fetchval("SELECT COUNT(*) FROM collections WHERE collection_id=%s", ("c1",)) == 1
    assert await db.execute("UPDATE collections SET name=%s WHERE collection_id=%s", ("renamed", "c1")) == 1
    assert (await db.fetch("SELECT name FROM collections"))[0]["name"] == "renamed"
    assert await db.fetchval("SELECT 1 WHERE false") is None


def test_hot_tables_are_vacuumed_and_analysed_at_two_percent(pg_url):
    """V14: the tables every recompute rewrites get autovacuum and autoanalyze at 2 % of their rows,
    not the 20 % default that let delta_urls grow to nine times its live size (2026-09-18 audit)."""
    import psycopg

    with psycopg.connect(pg_url) as conn:
        rows = dict(conn.execute(
            "SELECT relname, reloptions FROM pg_class"
            " WHERE relname IN ('delta_urls', 'pattern_effects', 'patterns') AND relkind = 'r'"
        ).fetchall())
    assert set(rows) == {"delta_urls", "pattern_effects", "patterns"}
    for name, opts in rows.items():
        assert "autovacuum_vacuum_scale_factor=0.02" in opts, name
        assert "autovacuum_analyze_scale_factor=0.02" in opts, name


def test_query_statistics_extension_is_created_where_postgres_has_it(pg_url):
    """V13 creates pg_stat_statements when the server ships it, and never fails a database that does
    not (the migration then only logs a notice)."""
    import psycopg

    with psycopg.connect(pg_url) as conn:
        available = conn.execute(
            "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_statements'").fetchone()
        installed = conn.execute(
            "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'").fetchone()
    assert bool(installed) == bool(available)


def test_the_busy_filters_and_rule_loads_have_indexes(pg_url):
    """V16: the delta table's kind / excluded filter, the renamed_from anti-join of the duplicate-title
    scan, rule loads in id order and the rule-effect lookups by field each have an index."""
    import psycopg

    with psycopg.connect(pg_url) as conn:
        defs = dict(conn.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema()").fetchall())
    assert "(collection_id, kind, excluded)" in defs["delta_urls_kind"]
    assert "(collection_id, renamed_from) WHERE (renamed_from IS NOT NULL)" in defs["delta_urls_renamed_from"]
    assert "(collection_id, id)" in defs["patterns_coll_id"]
    assert "(collection_id, field)" in defs["pattern_effects_coll_field"]


def test_v12_forgets_counts_stored_while_older_code_ran(pg_url):
    """Dev, 2026-10-06: V11 applied (counts stored), then code from before V11 ran recomputes that never
    updated them. V12 makes every stored count unknown again; the engine recounts each one when first
    wanted, and a second migrate leaves later counts alone."""
    import psycopg

    from sde_curation.schema import MIGRATIONS, migrate_sync

    with psycopg.connect(pg_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS mig12 CASCADE; CREATE SCHEMA mig12; SET search_path TO mig12")
        conn.execute("CREATE TABLE schema_version (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
        for version, sql in MIGRATIONS:
            if version <= 11:
                conn.execute(sql)
                conn.execute("INSERT INTO schema_version (version) VALUES (%s)", (version,))
        conn.execute("INSERT INTO collections (collection_id,name,seed_url,division,connector,max_pages,status,"
                     "created_at,updated_at,excluded_count) VALUES"
                     " ('a','A','https://a','General','crawler',10,'curating',now(),now(),97111),"
                     " ('b','B','https://b','General','crawler',10,'curating',now(),now(),NULL)")
        try:
            assert migrate_sync(conn) == MIGRATIONS[-1][0]
            assert conn.execute("SELECT collection_id, excluded_count FROM collections ORDER BY 1").fetchall() == [
                ("a", None), ("b", None)]
            conn.execute("UPDATE collections SET excluded_count=3 WHERE collection_id='a'")  # recounted since
            assert migrate_sync(conn) == MIGRATIONS[-1][0]  # applied once: a later count stays
            assert conn.execute("SELECT excluded_count FROM collections WHERE collection_id='a'").fetchone() == (3,)
        finally:
            conn.execute("DROP SCHEMA mig12 CASCADE")


def test_v2_backfills_curated_text_from_the_dump(pg_url):
    """Upgrading a v1 database: curated rows take the dump text (what the export shipped for them),
    a curated URL the dump no longer has stays NULL, and re-running migrations is a no-op."""
    import psycopg

    from sde_curation.schema import MIGRATIONS, migrate_sync

    v1 = dict(MIGRATIONS)[1]
    with psycopg.connect(pg_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS mig CASCADE; CREATE SCHEMA mig; SET search_path TO mig")
        conn.execute(v1)
        conn.execute("CREATE TABLE schema_version (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
        conn.execute("INSERT INTO schema_version (version) VALUES (1)")
        conn.execute("INSERT INTO collections (collection_id,name,seed_url,division,connector,max_pages,status,created_at,updated_at)"
                     " VALUES ('c','C','https://c','Earth Science','crawler2',10,'curated',now(),now()),"
                     " ('g','G','https://g','General','crawler2',10,'backlog',now(),now())")
        conn.execute("INSERT INTO dump_urls (collection_id,url,full_text) VALUES ('c','https://c/a','body a')")
        conn.execute("INSERT INTO audit_log (at,actor,collection_id,action) VALUES"
                     " (now(),'bob','c','recompute'), (now(),'carol','c','recompute.all'),"
                     " (now(),'dave','c','pattern.add'), (now(),'erin','g','collection.create')")
        conn.execute("INSERT INTO curated_urls (collection_id,url,excluded) VALUES"
                     " ('c','https://c/a',false), ('c','https://c/gone',false), ('c','https://c/out',true)")
        try:
            assert migrate_sync(conn) == MIGRATIONS[-1][0]
            # V2 put the dump text on the curated row; V9 moved both onto one blob keyed by the
            # content hash it gives a row that predates hashing, so the text survives shared
            rows = dict(conn.execute(
                "SELECT c.url, p.full_text FROM curated_urls c"
                " LEFT JOIN page_text p ON p.collection_id=c.collection_id AND p.content_hash=c.content_hash"
                " ORDER BY c.url").fetchall())
            assert rows == {"https://c/a": "body a", "https://c/gone": None, "https://c/out": None}
            assert conn.execute("SELECT COUNT(*) FROM page_text").fetchone() == (1,), "one copy, not two"
            # V7 derives both curated counters from the table: the count is the included rows only
            assert conn.execute("SELECT curated_count, curated_rows, curated_changed_at FROM collections"
                                " WHERE collection_id='c'").fetchone() == (2, 3, None)
            # divisions are left exactly as they were: General is the "not assigned" placeholder,
            # not something to migrate away from
            assert dict(conn.execute("SELECT collection_id, division FROM collections").fetchall()) == {
                "c": "Earth Science", "g": "General"}
            # V8 adds the flag that records "the model was never asked for this row's division"
            assert conn.execute("SELECT division_skipped FROM delta_urls").fetchall() == []
            # V10: the curator is whoever last pressed curate (recompute), not the last to act at all;
            # a collection nobody has curated stays NULL
            assert dict(conn.execute("SELECT collection_id, curated_by FROM collections").fetchall()) == {
                "c": "carol", "g": None}
            # V11: the excluded count is not backfilled — unknown until it is first wanted
            assert conn.execute("SELECT COUNT(*) FROM collections WHERE excluded_count IS NULL").fetchone() == (2,)
            assert migrate_sync(conn) == MIGRATIONS[-1][0]  # idempotent: nothing left to apply
        finally:
            conn.execute("DROP SCHEMA mig CASCADE")


async def test_a_promote_and_the_key_backfill_running_together_both_succeed(client):
    """After the V18 deploy, the key backfill updates curated rows while a curator may promote. The
    two must not deadlock. A test-only trigger slows every curated-row update by 1 ms so the two
    statements overlap on the same rows, the way they do at scale."""
    import asyncio

    import psycopg

    from sde_curation.models import CuratedUrl

    db = client.app.state.db
    cid = "dl.org"
    await client.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 100000})
    n = 1500
    outcomes = []
    with psycopg.connect(db.dsn, autocommit=True) as conn:
        conn.execute("CREATE OR REPLACE FUNCTION zz_slow() RETURNS trigger LANGUAGE plpgsql AS"
                     " $$ BEGIN PERFORM pg_sleep(0.001); RETURN NEW; END $$")
        conn.execute("CREATE TRIGGER zz_slow BEFORE UPDATE ON curated_urls FOR EACH ROW EXECUTE FUNCTION zz_slow()")
    try:
        for offset in (0.0, 0.05):
            await db.fetch("DELETE FROM curated_urls WHERE collection_id=%s RETURNING 1", (cid,))
            await db.fetch(  # rows without keys, stored in the reverse of the promote's order
                "INSERT INTO curated_urls (collection_id, url, title, content_hash) SELECT %s,"
                " 'https://dl.org/p' || lpad(g::text, 6, '0'), 'Old', 'h' FROM generate_series(%s, 1, -1) g"
                " RETURNING 1", (cid, n))
            rows = [CuratedUrl(collection_id=cid, url=f"https://dl.org/p{g:06d}", title="New", content_hash="h")
                    for g in range(1, n + 1)]

            async def backfill(offset=offset):
                await asyncio.sleep(offset)
                return await db.backfill_keys(batch=n)

            got = await asyncio.gather(db.replace_curated(cid, rows), backfill(), return_exceptions=True)
            outcomes += [type(g).__name__ for g in got if isinstance(g, BaseException)]
    finally:
        with psycopg.connect(db.dsn, autocommit=True) as conn:
            conn.execute("DROP TRIGGER IF EXISTS zz_slow ON curated_urls")
            conn.execute("DROP FUNCTION IF EXISTS zz_slow()")
    assert outcomes == []
