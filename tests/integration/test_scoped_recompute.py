"""A per-URL edit recomputes only its page (CurationService._recompute_keys), and the result must be
exactly what a recompute of the whole collection gives.

The test builds random collections that exercise everything the engine decides — several spellings of
one page in the dump and in the curated set (renames), curated pages the crawl did not find (removed,
or kept because the crawler failed on them or the crawl hit its page cap), glob rules of every type,
per-URL rules written under different spellings of a page, a collection division or none, an open
"Re-curate everything" round, pending AI suggestions — and applies random sequences of per-URL edits
through the scoped path: title, division and document type edits, ✗ exclude and ✓ include, one row's
AI accept, and the no-op edit.

Every collection is built twice from the same seed: a twin that edits through the scoped path, and a
twin forced onto the full recompute. Both get the same edits. After every edit the two must hold the
same delta rows (every column), rule effects, curated rows and stored counts, and report the same
counts. (Checking only that a full recompute after a scoped edit changes nothing is not enough: a
scoped edit that wrongly drops a page from an open review round leaves a state the full recompute
cannot restore, because what stays queued depends on what was queued before.) As a second check, a
full recompute run after the scoped twin's edit must change nothing.

SCOPED_SEQUENCES (default 60) sets how many random collections; the plan's validation uses 300.
"""

from __future__ import annotations

import os
import random

from sde_curation.engine.diff import promote
from sde_curation.models import (
    CuratedUrl,
    DocumentType,
    DumpFailure,
    DumpUrl,
    Pattern,
    PatternCreate,
    PatternType,
    RuleSource,
)

SEQUENCES = int(os.environ.get("SCOPED_SEQUENCES", "60"))
EDITS_PER_SEQUENCE = 8
DIVISIONS = ["Astrophysics", "Earth Science", "Heliophysics", "Planetary Science"]
DOC_TYPES = [d.value for d in DocumentType]


def spelling(rng: random.Random, host: str, path: str) -> str:
    scheme = rng.choice(["https", "http"])
    h = rng.choice([host, "www." + host])
    tail = rng.choice(["", "/"]) if path else "/"
    return f"{scheme}://{h}{path}{tail}"


def random_rules(rng: random.Random, cid: str, pages: list[str]) -> list[Pattern]:
    rules: list[Pattern] = []
    for _ in range(rng.randint(0, 4)):  # globs
        t = rng.choice(list(PatternType))
        match = f"https://{cid}/s{rng.randrange(3)}/*" if rng.random() < 0.7 else f"*/p{rng.randrange(18)}*"
        value = None
        if t is PatternType.TITLE:
            value = rng.choice(["{title} ({collection})", "Fixed", "{url}"])
        elif t is PatternType.DIVISION:
            value = rng.choice(DIVISIONS)
        elif t is PatternType.DOCUMENT_TYPE:
            value = rng.choice(DOC_TYPES)
        rules.append(Pattern(collection_id=cid, type=t, match=match, value=value, source=RuleSource.SME))
    for _ in range(rng.randint(0, 8)):  # per-URL rules, under any spelling
        p = rng.choice(pages)
        t = rng.choice(list(PatternType))
        value = {PatternType.TITLE: f"Set {p}", PatternType.DIVISION: rng.choice(DIVISIONS),
                 PatternType.DOCUMENT_TYPE: rng.choice(DOC_TYPES)}.get(t)
        rules.append(Pattern(collection_id=cid, type=t, match=spelling(rng, cid, p), value=value,
                             source=rng.choice([RuleSource.SME, RuleSource.LLM])))
    seen, unique = set(), []  # distinct (type, match): the table refuses duplicates
    for p in rules:
        if (p.type, p.match) not in seen:
            seen.add((p.type, p.match)); unique.append(p)
    return unique


async def build(c, rng: random.Random, cid: str):
    """A random collection, recomputed in full once. Returns (cid, page paths).

    Most collections are built the way they arise: crawled, curated with rules, promoted, then crawled
    again with a few changes (pages gone or failed, text changed, new pages, other spellings), so most
    pages are unchanged and renames, removals and kept pages all occur. The rest get a curated set of
    arbitrary values, to cover states no promote would leave."""
    db, svc = c.app.state.db, c.app.state.curation
    division = rng.choice(["General", *DIVISIONS])
    r = await c.post("/api/collections", json={"seed_url": f"https://{cid}", "name": cid, "max_pages": 100,
                                               "division": division})
    assert r.status_code == 201, r.text
    pages = [f"/s{rng.randrange(3)}/p{i}" for i in range(rng.randint(8, 18))]
    spelled = {p: spelling(rng, cid, p) for p in pages}
    hashes = {p: f"{rng.randrange(3):064x}" for p in pages}

    if rng.random() < 0.7:  # crawl, rules, promote everything, then crawl again
        first = [DumpUrl(collection_id=cid, url=spelled[p], scraped_title=f"Page {p}", content_hash=hashes[p])
                 for p in pages if rng.random() < 0.9]
        await db.replace_dump(cid, first)
        if rules := random_rules(rng, cid, pages):
            await db.insert_patterns(rules)
        ds = await svc.recompute(await db.get_collection(cid))
        curated = promote([], ds.deltas, content_hashes={d.url: d.content_hash for d in first})
        if curated:
            await db.replace_curated(cid, curated)
        await db.replace_deltas(cid, [], [], keep_effects=True)
        for p in pages:  # the second crawl's changes
            roll = rng.random()
            if roll < 0.1:
                spelled[p] = spelling(rng, cid, p)  # found under another spelling: a rename
            elif roll < 0.2:
                hashes[p] = f"{rng.randrange(3, 6):064x}"  # the text changed
        in_dump = {p for p in pages if rng.random() < 0.85}
    else:  # arbitrary curated values
        in_dump = {p for p in pages if rng.random() < 0.8}
        curated = [CuratedUrl(collection_id=cid, url=spelling(rng, cid, p), scraped_title=f"Page {p}",
                              title=rng.choice([None, f"Old {p}"]), division=rng.choice([None, *DIVISIONS]),
                              document_type=rng.choice([None, *DOC_TYPES]), excluded=rng.random() < 0.1,
                              content_hash=f"{rng.randrange(3):064x}")
                   for p in pages if rng.random() < 0.6]
        if curated:
            await db.replace_curated(cid, curated)
        if rules := random_rules(rng, cid, pages):
            await db.insert_patterns(rules)

    dump = [DumpUrl(collection_id=cid, url=spelled[p], scraped_title=f"Page {p}", content_hash=hashes[p])
            for p in pages if p in in_dump]
    failures = [DumpFailure(collection_id=cid, url=spelling(rng, cid, p),
                            reason=rng.choice(["http_404", "http_410", "http_403", "timeout"]))
                for p in pages if p not in in_dump and rng.random() < 0.5]
    await db.replace_dump(cid, dump, failures)
    if rng.random() < 0.3:
        await db.fetch("UPDATE collections SET last_crawl_capped=true WHERE collection_id=%s RETURNING 1", (cid,))
    coll = await db.get_collection(cid)
    if rng.random() < 0.3:  # an open "Re-curate everything" round
        await svc.recompute(coll, review_all=True)
        await db.set_review_round(cid, True)
    else:
        await svc.recompute(coll)
    # in URL order (without the host), so both twins draw the same numbers for the same rows
    deltas = sorted(await db.load_deltas(cid), key=lambda d: d.url.replace(cid, ""))
    if deltas:  # pending suggestions on some rows
        await db.set_delta_ai(cid, [{"url": d.url, "title": f"AI {d.url[-6:]}", "title_conf": "high",
                                     "division": rng.choice(DIVISIONS), "document_type": rng.choice(DOC_TYPES),
                                     "model": "fake"} for d in deltas if rng.random() < 0.5])
    return cid, pages


async def snapshot(db, cid: str) -> dict:
    """The collection's state, comparable between twins: the collection's host is written as <C> and a
    rule effect names its rule by (type, match) rather than by id."""
    deltas = await db.fetch("SELECT * FROM delta_urls WHERE collection_id=%s ORDER BY url", (cid,))
    effects = await db.fetch("SELECT p.type, p.match, e.url, e.field FROM pattern_effects e"
                             " JOIN patterns p ON p.id = e.pattern_id WHERE e.collection_id=%s"
                             " ORDER BY p.type, p.match, e.url, e.field", (cid,))
    curated = await db.fetch("SELECT url, scraped_title, title, division, document_type, excluded, content_hash,"
                             " edited_by, crawl_failure, canonical_key FROM curated_urls WHERE collection_id=%s"
                             " ORDER BY url", (cid,))
    coll = await db.fetch("SELECT delta_count, excluded_count, curated_count, curated_rows FROM collections"
                          " WHERE collection_id=%s", (cid,))
    return _same_host({"deltas": deltas, "effects": effects, "curated": curated, "collection": coll}, cid)


def first_difference(a: dict, b: dict) -> str | None:
    """Where two snapshots first differ: part, row and fields — or None when they are equal."""
    for part, rows_a in a.items():
        rows_b = b[part]
        if rows_a == rows_b:
            continue
        if len(rows_a) != len(rows_b):
            only_a = [r for r in rows_a if r not in rows_b][:2]
            only_b = [r for r in rows_b if r not in rows_a][:2]
            return f"{part}: {len(rows_a)} rows vs {len(rows_b)}; only first: {only_a}; only second: {only_b}"
        for ra, rb in zip(rows_a, rows_b, strict=True):
            if ra != rb:
                fields = {k: (ra.get(k), rb.get(k)) for k in set(ra) | set(rb) if ra.get(k) != rb.get(k)}
                return f"{part}: row {ra.get('url', ra)} differs in {fields}"
    return None


def _same_host(value, cid: str):
    if isinstance(value, str):
        return value.replace(cid, "<C>")
    if isinstance(value, dict):
        return {k: _same_host(v, cid) for k, v in value.items()}
    if isinstance(value, list):
        return [_same_host(v, cid) for v in value]
    return value


async def edit(c, rng: random.Random, cid: str, pages: list[str]):
    """One random per-URL edit through the paths the routes use. Returns its DeltaSet."""
    db, svc = c.app.state.db, c.app.state.curation
    coll = await db.get_collection(cid)
    url = spelling(rng, cid, rng.choice(pages))  # the same draws give the same edit on either twin
    kind = rng.choice(["title", "division", "document_type", "exclude", "include", "row", "noop"])
    if kind in ("exclude", "include"):
        return await svc.set_excluded(coll, url, kind == "exclude", actor="t")
    if kind == "noop":
        return await svc.recompute_page(coll, url)
    if kind == "row":  # one row's ✓: every field of one URL at once
        bodies = [PatternCreate(type=PatternType.TITLE, match=url, value=f"Row {url[-5:]}"),
                  PatternCreate(type=PatternType.DIVISION, match=url, value=rng.choice(DIVISIONS)),
                  PatternCreate(type=PatternType.DOCUMENT_TYPE, match=url, value=rng.choice(DOC_TYPES))]
        return await svc.replace_exact_patterns(coll, bodies, actor="t", source=RuleSource.LLM)
    value = {"title": f"Edit {rng.randrange(1000)}", "division": rng.choice(DIVISIONS),
             "document_type": rng.choice(DOC_TYPES)}[kind]
    existing = await db.exact_patterns_for(cid, url, kind)
    return await svc.replace_exact_pattern(
        coll, PatternCreate(type=PatternType(kind), match=url, value=value),
        old_id=existing[0].id if existing else None, actor="t",
        source=rng.choice([RuleSource.SME, RuleSource.LLM, RuleSource.LLM_EDITED]))


async def test_a_per_url_edit_gives_exactly_what_the_full_recompute_gives(client):
    c = client
    db, svc = c.app.state.db, c.app.state.curation
    scoped_runs = 0
    original = svc._recompute_keys

    async def scoped_or_full(cc, *args, **kwargs):
        nonlocal scoped_runs
        if cc.collection_id.startswith("full"):
            return None  # the twin that always takes the full recompute
        ds = await original(cc, *args, **kwargs)
        scoped_runs += ds is not None
        return ds

    svc._recompute_keys = scoped_or_full
    for n in range(SEQUENCES):
        seed = 1000 + n
        scoped_cid, full_cid = f"scoped{n}.org", f"full{n}.org"
        _, pages = await build(c, random.Random(seed), scoped_cid)
        await build(c, random.Random(seed), full_cid)
        diff = first_difference(await snapshot(db, scoped_cid), await snapshot(db, full_cid))
        assert diff is None, f"seed {seed}: the twins differ before any edit: {diff}"
        for step in range(EDITS_PER_SEQUENCE):
            where = f"sequence {n} (seed {seed}), edit {step}"
            ds = await edit(c, random.Random(seed * 100 + step), scoped_cid, pages)
            ds_full = await edit(c, random.Random(seed * 100 + step), full_cid, pages)
            scoped, full = await snapshot(db, scoped_cid), await snapshot(db, full_cid)
            diff = first_difference(scoped, full)
            assert diff is None, f"{where}: the scoped edit differs from the full recompute: {diff}"
            assert (ds.counts, ds.total) == (ds_full.counts, ds_full.total), (where, ds.counts, ds_full.counts)
            # and a full recompute of the scoped twin now changes nothing
            await svc.recompute(await db.get_collection(scoped_cid))
            diff = first_difference(scoped, await snapshot(db, scoped_cid))
            assert diff is None, f"{where}: a full recompute changed the scoped twin: {diff}"
    assert scoped_runs >= SEQUENCES * EDITS_PER_SEQUENCE * 0.9, f"only {scoped_runs} edits took the scoped path"


async def test_a_collection_with_rows_from_before_v18_uses_the_full_recompute_until_filled(client):
    """Rows written before schema V18 have no canonical key: per-URL edits fall back to the full
    recompute (same result, the old cost) until backfill_keys has filled them in."""
    c = client
    db, svc = c.app.state.db, c.app.state.curation
    cid, _ = await build(c, random.Random(7), "r9999.org")
    await db.fetch("UPDATE dump_urls SET canonical_key=NULL WHERE collection_id=%s RETURNING 1", (cid,))
    assert not await db.keyed(cid)
    coll = await db.get_collection(cid)
    assert await svc._recompute_keys(coll, ["r9999.org/s0/p0"]) is None
    filled = await db.backfill_keys(batch=3)
    assert filled >= 1 and await db.keyed(cid)
    assert await db.fetch("SELECT 1 FROM dump_urls WHERE canonical_key IS NULL") == []
    keys = await db.fetch("SELECT url, canonical_key FROM dump_urls WHERE collection_id=%s", (cid,))
    from sde_curation.engine.urls import canonical_key
    assert all(r["canonical_key"] == canonical_key(r["url"]) for r in keys)
    assert await svc._recompute_keys(await db.get_collection(cid), [keys[0]["canonical_key"]]) is not None
