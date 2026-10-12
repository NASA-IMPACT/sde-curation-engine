"""The in-memory FakeDatabase (tests/support/fake_db.py) runs inside the unit level, where
tests/unit/conftest.py refuses every database connection, and stands in for Database in
CurationService. tests/integration/test_db_contract.py checks it behaves like PostgreSQL."""

import inspect

from sde_curation.curation import CurationService
from sde_curation.db import Database
from sde_curation.models import (
    Collection,
    ConnectorType,
    DeltaKind,
    DumpUrl,
    PatternCreate,
    PatternType,
)
from tests.support.fake_db import IMPLEMENTED, FakeDatabase

CID = "example.com"


def test_same_methods_signatures_and_async_ness_as_database():
    for name in IMPLEMENTED:
        real, fake = getattr(Database, name), getattr(FakeDatabase, name)
        assert inspect.signature(fake) == inspect.signature(real), name
        assert inspect.iscoroutinefunction(fake) == inspect.iscoroutinefunction(real), name
        assert inspect.isasyncgenfunction(fake) == inspect.isasyncgenfunction(real), name
    assert not hasattr(FakeDatabase(), "list_audit")  # not used by jobs.py or curation.py: absent


async def test_curation_service_recomputes_on_the_fake():
    db = FakeDatabase()
    c = await db.insert_collection(Collection(collection_id=CID, name="Example", seed_url="https://example.com/",
                                              connector=ConnectorType.CRAWLER, max_pages=100))
    urls = [f"https://example.com/{p}" for p in ("docs/a", "docs/b", "tags/x")]
    assert await db.replace_dump(CID, [DumpUrl(collection_id=CID, url=u, full_text=u) for u in urls]) == 3

    service = CurationService(db)
    await service.recompute(c)
    assert sorted((d.url, d.kind) for d in await db.load_deltas(CID)) == [(u, DeltaKind.NEW) for u in urls]

    p, _ = await service.add_pattern(c, PatternCreate(type=PatternType.EXCLUDE, match="https://example.com/tags/*"))
    assert p.id == 1
    assert sorted(d.url for d in await db.load_deltas(CID) if not d.excluded) == urls[:2]
    after = await db.get_collection(CID)
    assert (after.delta_count, after.excluded_count) == (2, 1)
    assert await db.effect_counts(CID) == {1: 1}


async def test_reads_are_copies():
    db = FakeDatabase()
    await db.insert_collection(Collection(collection_id=CID, name="Example", seed_url="https://example.com/",
                                          connector=ConnectorType.CRAWLER, max_pages=100))
    got = await db.get_collection(CID)
    got.dump_count = 99
    assert (await db.get_collection(CID)).dump_count == 0
