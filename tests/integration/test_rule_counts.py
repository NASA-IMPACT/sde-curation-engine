"""The Rules tab's match counts come from SQL (Database.rule_match_counts) and must equal what the
engine counts in Python (engine.patterns.match_counts over the set's URLs) for every rule and every
URL set: globs as LIKE, exact-URL rules by canonical key, the delta set through the dump and curated
rows its URLs come from.
"""

from __future__ import annotations

import random

from sde_curation.engine.patterns import glob_to_like, is_exact, match_counts
from sde_curation.engine.urls import canonical_key
from sde_curation.models import CuratedUrl, DumpUrl, Pattern, PatternType, RuleSource
from tests.integration.test_scoped_recompute import build

SETS = ("dump", "delta", "curated")


async def assert_counts_equal(db, cid: str) -> int:
    rules = await db.list_patterns(cid)
    checked = 0
    for set_ in SETS:
        python = match_counts(rules, await db.set_urls(cid, set_))
        sql = await db.rule_match_counts(
            cid, set_,
            globs=[(p.id, glob_to_like(p.match)) for p in rules if not is_exact(p.match)],
            exact=[(p.id, canonical_key(p.match)) for p in rules if is_exact(p.match)],
        )
        for p in rules:
            assert sql.get(p.id, 0) == python.get(p.id, 0), (cid, set_, p.type, p.match, sql.get(p.id), python.get(p.id))
            checked += 1
    return checked


async def test_sql_counts_equal_the_engines_on_random_collections(client):
    db = client.app.state.db
    checked = 0
    for n in range(30):
        cid, _ = await build(client, random.Random(5000 + n), f"count{n}.org")
        assert await db.keyed(cid)
        checked += await assert_counts_equal(db, cid)
    assert checked > 300


async def test_sql_counts_equal_the_engines_with_awkward_urls(client):
    """LIKE wildcards and its escape character inside URLs, a host in mixed case, and several
    spellings of one page in each set."""
    c = client
    db, svc = c.app.state.db, c.app.state.curation
    cid = "odd.org"
    assert (await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 50})).status_code == 201
    urls = [f"https://{cid}/a_b/100%25", f"https://{cid}/a_b/100%25/", f"http://www.{cid}/a_b/100%25",
            f"https://{cid}/axb/1000", f"https://{cid}/back\\slash", "https://Odd.ORG/Case/Page",
            f"https://{cid}/case/page", f"https://{cid}/q?x=1&y=_", f"https://{cid}/q?x=1&y=z"]
    await db.replace_dump(cid, [DumpUrl(collection_id=cid, url=u, scraped_title=u[-6:]) for u in urls])
    await db.replace_curated(cid, [CuratedUrl(collection_id=cid, url=u, scraped_title="t") for u in
                                   (f"http://{cid}/a_b/100%25", f"https://{cid}/gone", "https://ODD.org/Case/Page/")])
    rules = [Pattern(collection_id=cid, type=PatternType.EXCLUDE, match=m, source=RuleSource.SME) for m in (
        f"https://{cid}/a_b/*", "*/100%25*", "*\\slash", f"https://{cid}/q?x=1&y=_", "*Case*", "*case*",
        f"https://{cid}/a_b/100%25", "https://odd.org/case/page/", "http://www.odd.org/Case/Page",
        f"https://{cid}/gone/")]
    await db.insert_patterns(rules)
    await svc.recompute(await db.get_collection(cid))
    assert await db.keyed(cid)
    assert await assert_counts_equal(db, cid) == len(rules) * len(SETS)


async def test_the_rules_tab_shows_the_same_counts_as_before(client):
    """Through the page: the counts the Rules tab renders, with keys, equal the Python counts it
    rendered before (keys removed, so CurationService._match_counts falls back to Python)."""
    c = client
    db = c.app.state.db
    cid, _ = await build(c, random.Random(77), "tab.org")
    with_keys = (await c.get(f"/collections/{cid}?tab=rules")).text
    await db.fetch("UPDATE dump_urls SET canonical_key=NULL WHERE collection_id=%s RETURNING 1", (cid,))
    assert not await db.keyed(cid)
    without_keys = (await c.get(f"/collections/{cid}?tab=rules")).text
    assert with_keys == without_keys
