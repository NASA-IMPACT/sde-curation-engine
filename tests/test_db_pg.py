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
                     " VALUES ('c','C','https://c','Earth Science','crawler2',10,'curated',now(),now())")
        conn.execute("INSERT INTO dump_urls (collection_id,url,full_text) VALUES ('c','https://c/a','body a')")
        conn.execute("INSERT INTO curated_urls (collection_id,url) VALUES ('c','https://c/a'), ('c','https://c/gone')")
        try:
            assert migrate_sync(conn) == 3
            rows = dict(conn.execute("SELECT url, full_text FROM curated_urls ORDER BY url").fetchall())
            assert rows == {"https://c/a": "body a", "https://c/gone": None}
            assert migrate_sync(conn) == 3
        finally:
            conn.execute("DROP SCHEMA mig CASCADE")
