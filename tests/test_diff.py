"""Delta computation and promotion — including a 100k-URL performance check."""

import time

from sde_curation.engine.diff import promote, recompute
from sde_curation.models import CuratedUrl, DeltaKind, DumpUrl, Pattern, PatternType


def dump(*pairs):
    return [DumpUrl(collection_id="x", url=u, scraped_title=t) for u, t in pairs]


def cur(*rows):
    return [CuratedUrl(collection_id="x", **r) for r in rows]


def rc(d, c, p=(), prev=None):
    return recompute(collection_id="x", collection_name="X", dump=d, curated=c, patterns=list(p), previous=prev)


def test_new_modified_deleted_unchanged():
    d = dump(("https://x/a", "A"), ("https://x/b", "B2"), ("https://x/c", "C"))
    c = cur(
        {"url": "https://x/b", "scraped_title": "B", "title": "B"},   # title changed → modified
        {"url": "https://x/c", "scraped_title": "C"},                 # unchanged
        {"url": "https://x/gone", "scraped_title": "G"},              # deleted
    )
    ds = rc(d, c)
    by = {x.url: x for x in ds.deltas}
    assert by["https://x/a"].kind is DeltaKind.NEW
    assert by["https://x/b"].kind is DeltaKind.MODIFIED and by["https://x/b"].title == "B"  # curated kept
    assert "https://x/c" not in by
    assert by["https://x/gone"].kind is DeltaKind.DELETED
    assert ds.counts == {"new": 1, "modified": 1, "deleted": 1, "excluded": 0}


def test_pattern_change_on_curated_row_creates_modified_delta_and_effects():
    d = dump(("https://x/a", "A"))
    c = cur({"url": "https://x/a", "scraped_title": "A"})
    assert rc(d, c).deltas == []
    ds = rc(d, c, [Pattern(id=7, collection_id="x", type=PatternType.EXCLUDE, match="https://x/*")])
    assert ds.deltas[0].kind is DeltaKind.MODIFIED and ds.deltas[0].excluded is True
    ds = rc(d, c, [Pattern(id=8, collection_id="x", type=PatternType.DIVISION, match="*", value="General")])
    assert ds.deltas[0].division == "General" and ds.effects == [(8, "https://x/a", "division")]


def test_recompute_preserves_ml_suggestions():
    d = dump(("https://x/a", "A"))
    first = rc(d, [])
    first.deltas[0].division_ml = "Heliophysics"
    again = rc(d, [], prev=first.deltas)
    assert again.deltas[0].division_ml == "Heliophysics"


def test_promote_upserts_and_tombstones():
    c = cur({"url": "https://x/keep", "scraped_title": "K"}, {"url": "https://x/gone", "scraped_title": "G"})
    ds = rc(dump(("https://x/keep", "K"), ("https://x/new", "N")), c,
            [Pattern(id=1, collection_id="x", type=PatternType.TITLE, match="*/new", value="New!")])
    out = {r.url: r for r in promote(c, ds.deltas)}
    assert set(out) == {"https://x/keep", "https://x/new"}
    assert out["https://x/new"].title == "New!"
    # promoting again with no deltas is a no-op; recompute after promote yields no deltas
    assert rc(dump(("https://x/keep", "K"), ("https://x/new", "N")), list(out.values()),
              [Pattern(id=1, collection_id="x", type=PatternType.TITLE, match="*/new", value="New!")]).deltas == []


def test_100k_urls_under_5s():
    n = 100_000
    d = [DumpUrl(collection_id="x", url=f"https://x.org/s{i % 50}/p{i}", scraped_title=f"T{i}") for i in range(n)]
    c = [CuratedUrl(collection_id="x", url=f"https://x.org/s{i % 50}/p{i}", scraped_title=f"T{i}") for i in range(0, n, 2)]
    pats = [Pattern(id=i, collection_id="x", type=PatternType.DIVISION, match=f"https://x.org/s{i}/*", value="General") for i in range(20)]
    pats += [Pattern(id=99, collection_id="x", type=PatternType.EXCLUDE, match="*/p1*"),
             Pattern(id=100, collection_id="x", type=PatternType.TITLE, match="*", value="{title} | X")]
    t0 = time.perf_counter()
    ds = rc(d, c, pats)
    promote(c, ds.deltas)
    dt = time.perf_counter() - t0
    assert len(ds.deltas) == n  # every row changed (title pattern)
    assert dt < 5, f"took {dt:.1f}s"
