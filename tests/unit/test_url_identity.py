"""URL identity and crawl failures in the diff.

A page is the same page under http/https, with or without a trailing slash, with or without a
#fragment (engine/urls.py `canonical_key`): the diff pairs dump and curated rows by it and exact
rules match by it. A curated URL missing from the dump is removed only when the crawl is evidence
it is gone (404, or never met in a complete crawl); a fetch failure or a capped crawl keeps it.
"""
import json

from sde_curation.engine.diff import match_urls, promote, recompute
from sde_curation.engine.patterns import resolve_all
from sde_curation.engine.urls import canonical_key, spellings
from sde_curation.models import NOT_VISITED, CuratedUrl, DeltaKind, DumpUrl, Pattern, PatternType


def dump(*pairs, hash_=None):
    return [DumpUrl(collection_id="x", url=u, scraped_title=t, content_hash=hash_) for u, t in pairs]


def cur(*rows):
    return [CuratedUrl(collection_id="x", **r) for r in rows]


def rc(d, c, p=(), **kw):
    return recompute(collection_id="x", collection_name="X", dump=d, curated=c, patterns=list(p), **kw)


# ── engine: renames ──────────────────────────────────────────────────────


def test_match_urls_exact_first_then_canonical_with_best_spelling():
    dump_urls = ["https://x/a", "http://x/b/", "https://x/b", "https://x/c#top", "https://x/d"]
    curated = ["https://x/a", "http://x/b", "https://x/c/", "https://x/gone"]
    assert match_urls(dump_urls, curated) == {
        "https://x/a": "https://x/a",       # exact
        "https://x/b": "http://x/b",        # https preferred over http://x/b/ for the same page
        "https://x/c#top": "https://x/c/",  # fragment + trailing slash
    }


def test_spelling_change_is_one_modified_delta_not_new_plus_removed():
    d = dump(("https://x/a", "A"), ("https://x/b", "B"), ("https://x/c", "C"))
    c = cur(
        {"url": "http://x/a", "scraped_title": "A", "title": "Title A", "division": "Earth Science"},
        {"url": "https://x/b/", "scraped_title": "B"},
        {"url": "https://x/c#top", "scraped_title": "C"},
    )
    ds = rc(d, c)
    by = {x.url: x for x in ds.deltas}
    assert set(by) == {"https://x/a", "https://x/b", "https://x/c"}
    assert all(x.kind is DeltaKind.MODIFIED for x in ds.deltas)
    assert by["https://x/a"].renamed_from == "http://x/a" and by["https://x/a"].title == "Title A"  # curated values carried
    assert by["https://x/a"].division == "Earth Science" and by["https://x/a"].content_changed is False
    assert by["https://x/b"].renamed_from == "https://x/b/" and by["https://x/c"].renamed_from == "https://x/c#top"
    assert ds.counts == {"new": 0, "modified": 3, "deleted": 0, "excluded": 0, "content_changed": 0, "renamed": 3, "kept": 0}
    # promote moves the rows: old spellings gone, values kept
    out = {r.url: r for r in promote(c, ds.deltas, content_hashes={"https://x/a": "h"})}
    assert set(out) == {"https://x/a", "https://x/b", "https://x/c"}
    assert out["https://x/a"].title == "Title A" and out["https://x/a"].content_hash == "h"
    # and the next recompute is quiet
    assert rc(d, list(out.values())).deltas == []


def test_second_spelling_of_a_renamed_page_is_new_and_exact_match_wins_over_canonical():
    # both spellings crawled: https becomes the rename, the other spelling is a new row
    ds = rc(dump(("http://x/a/", "A"), ("https://x/a", "A")), cur({"url": "http://x/a", "scraped_title": "A"}))
    by = {x.url: x for x in ds.deltas}
    assert by["https://x/a"].kind is DeltaKind.MODIFIED and by["https://x/a"].renamed_from == "http://x/a"
    assert by["http://x/a/"].kind is DeltaKind.NEW and by["http://x/a/"].renamed_from is None
    # the curated set holds two spellings, the dump one: the exact match is unchanged and the
    # other spelling is a removal (the duplicate is gone), not a rename
    ds = rc(dump(("https://x/a", "A")), cur({"url": "https://x/a", "scraped_title": "A"}, {"url": "http://x/a", "scraped_title": "A"}))
    assert [(x.url, x.kind) for x in ds.deltas] == [("http://x/a", DeltaKind.DELETED)]


def test_rename_with_text_and_title_change_is_still_one_delta():
    d = dump(("https://x/a", "A2"), hash_="new")
    c = cur({"url": "http://x/a", "scraped_title": "A", "content_hash": "old"})
    ds = rc(d, c)
    assert len(ds.deltas) == 1 and ds.deltas[0].renamed_from == "http://x/a" and ds.deltas[0].content_changed


# ── engine: crawl failures ───────────────────────────────────────────────


def test_fetch_failures_keep_rows_and_404_removes():
    d = dump(("https://x/a", "A"))
    c = cur(
        {"url": "https://x/a", "scraped_title": "A"},
        {"url": "https://x/gone", "scraped_title": "G"},           # 404 → removed
        {"url": "https://x/blocked", "scraped_title": "B"},        # 403 → kept, flagged
        {"url": "http://x/slow", "scraped_title": "S"},            # timeout logged under another spelling → kept
        {"url": "https://x/unseen", "scraped_title": "U"},         # never met, complete crawl → removed
    )
    failures = {"https://x/gone": "http_404", "https://x/blocked": "http_403", "https://x/slow/": "crawl_unsuccessful"}
    ds = rc(d, c, failures=failures)
    by = {x.url: x for x in ds.deltas}
    assert set(by) == {"https://x/gone", "https://x/unseen"}
    assert by["https://x/gone"].kind is DeltaKind.DELETED and by["https://x/gone"].crawl_failure == "http_404"
    assert by["https://x/unseen"].kind is DeltaKind.DELETED and by["https://x/unseen"].crawl_failure is None
    assert ds.kept == {"https://x/blocked": "http_403", "http://x/slow": "crawl_unsuccessful"}
    assert sorted(ds.curated_crawl_failure) == [("http://x/slow", "crawl_unsuccessful"), ("https://x/blocked", "http_403")]
    assert ds.counts["deleted"] == 2 and ds.counts["kept"] == 2
    # promote: kept rows survive untouched (hash, flag), the removals go
    kept = [r.model_copy(update={"crawl_failure": "http_403"}) if r.url == "https://x/blocked" else r for r in c]
    out = {r.url: r for r in promote(kept, ds.deltas, content_hashes={"https://x/a": "h"})}
    assert set(out) == {"https://x/a", "https://x/blocked", "http://x/slow"}
    assert out["https://x/blocked"].crawl_failure == "http_403" and out["https://x/a"].content_hash == "h"


def test_capped_crawl_keeps_unmet_rows_but_a_404_still_removes():
    d = dump(("https://x/a", "A"))
    c = cur({"url": "https://x/a", "scraped_title": "A"}, {"url": "https://x/gone", "scraped_title": "G"},
            {"url": "https://x/unseen", "scraped_title": "U"})
    ds = rc(d, c, failures={"https://x/gone": "http_404"}, capped=True)
    assert [(x.url, x.kind, x.crawl_failure) for x in ds.deltas] == [("https://x/gone", DeltaKind.DELETED, "http_404")]
    assert ds.kept == {"https://x/unseen": NOT_VISITED} and ds.curated_crawl_failure == [("https://x/unseen", NOT_VISITED)]
    # the flag is written only when it changes
    flagged = [r.model_copy(update={"crawl_failure": NOT_VISITED}) if r.url.endswith("unseen") else r for r in c]
    assert rc(d, flagged, failures={"https://x/gone": "http_404"}, capped=True).curated_crawl_failure == []


def test_flag_comes_down_when_the_crawl_fetches_the_page_again():
    c = cur({"url": "https://x/a", "scraped_title": "A", "crawl_failure": "http_403"})
    ds = rc(dump(("https://x/a", "A")), c)  # unchanged: no delta, flag cleared by write-back
    assert ds.deltas == [] and ds.curated_crawl_failure == [("https://x/a", None)] and ds.kept == {}
    ds = rc(dump(("https://x/a", "A (new)")), c)  # changed: the delta replaces the row without a flag
    assert ds.deltas[0].kind is DeltaKind.MODIFIED
    assert promote(c, ds.deltas)[0].crawl_failure is None


# ── patterns: exact rules match every spelling ───────────────────────────


def test_spellings_are_every_form_of_one_canonical_key():
    sp = spellings("https://www.X.org/a/?q=1")
    assert {canonical_key(u) for u in sp} == {"x.org/a?q=1"} and len(sp) == 8
    assert "http://x.org/a?q=1" in sp and "https://www.x.org/a/?q=1" in sp
    assert spellings("https://x.org") == ["https://x.org/", "https://www.x.org/", "http://x.org/", "http://www.x.org/"]


def test_exact_rule_matches_other_spellings_and_newest_wins():
    urls = ["https://x.org/a", "https://x.org/b/", "http://x.org/c"]
    pats = [
        Pattern(id=1, collection_id="x", type=PatternType.TITLE, match="http://x.org/a/", value="One"),
        Pattern(id=2, collection_id="x", type=PatternType.TITLE, match="https://x.org/a#x", value="Two"),
        Pattern(id=3, collection_id="x", type=PatternType.EXCLUDE, match="https://x.org/b"),
        Pattern(id=4, collection_id="x", type=PatternType.DIVISION, match="https://x.org/c", value="Earth Science"),
    ]
    r = resolve_all(urls, pats, base={}, scraped_titles={}, collection_name="X")
    assert r["https://x.org/a"].title == "Two" and r["https://x.org/a"].effects["title"] == 2  # newest of the two
    assert r["https://x.org/b/"].excluded and r["https://x.org/b/"].effects["excluded"] == 3
    assert r["http://x.org/c"].division == "Earth Science"


# ── over the API ─────────────────────────────────────────────────────────





    # a re-crawl: p1 gained a trailing slash, p2 a fragment, p3 was blocked (403), p4 is gone (404),
    # p6 was never met (a complete crawl → removed), p7-9 unchanged
    # the Overview and Curate tabs show the same counts (one grouped query, not a sort per count)
    # the search finds a renamed row by its old spelling too

    # the pages say what happened

    # promote: renamed rows moved, removals gone, the blocked row kept with the text the index holds

    # the next crawl fetches p3 again, unchanged: no delta, and the flag comes down


    # the next (still capped) crawl only got to p1 and p2
    # a complete crawl (the summary says the cap was not reached) turns the same absence into a removal


    # a rule typed under the http spelling excludes the https row the dump has
    # excluding it again under the row's own spelling is a no-op; including it finds the rule under
    # the other spelling and removes it
    # a per-URL title under one spelling, then edited under another: one rule survives, the newest


def test_scrape_result_helpers(tmp_path):
    from sde_curation.backends.scrape import FileDocuments, ScrapeResult, parse_failures

    f = tmp_path / "f.jsonl"
    f.write_text('{"url": "https://x/a", "reason": "http_403", "status": 403}\nnot json\n{"url": "", "reason": "x"}\n'
                 '{"url": "https://x/b", "reason": "crawl_unsuccessful"}\n\n')
    assert [r["url"] for r in parse_failures(f.read_bytes())] == ["https://x/a", "https://x/b"]
    docs = FileDocuments(tmp_path / "d.json")
    res = ScrapeResult(documents=docs, failures_source=FileDocuments(f), summary={"max_pages": 100})
    assert len(res.failures()) == 2 and res.capped(100) and not res.capped(99)
    res = ScrapeResult(documents=docs, failures_source=FileDocuments(tmp_path / "missing.jsonl"))
    assert res.failures() == [] and res.capped(5, 5) and not res.capped(5, None) and not res.capped(4, 5)
    assert json.dumps(res.summary) == "{}"
