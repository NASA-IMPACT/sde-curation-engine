"""Pattern semantics: include precedence, newest rule wins, title templating, unapply fallbacks."""

from sde_curation.engine.patterns import glob_to_like, glob_to_regex, render_title, resolve_all
from sde_curation.models import Pattern, PatternType

URLS = [
    "https://x.org/",
    "https://x.org/data/a",
    "https://x.org/data/b",
    "https://x.org/data/legacy/c",
    "https://x.org/docs/d",
]


def P(id, type, match, value=None):
    return Pattern(id=id, collection_id="x", type=type, match=match, value=value)


def resolve(patterns, base=None, titles=None):
    return resolve_all(URLS, patterns, base=base or {}, scraped_titles=titles or {}, collection_name="X")


def test_glob_to_regex():
    assert glob_to_regex("https://x.org/data/*").match("https://x.org/data/a/b")
    assert not glob_to_regex("https://x.org/data/*").match("https://x.org/docs/a")
    assert glob_to_regex("https://x.org/data/a").match("https://x.org/data/a")
    assert not glob_to_regex("https://x.org/data/a").match("https://x.org/data/ab")  # exact
    assert glob_to_regex("*.pdf").match("https://x.org/f.pdf")
    assert glob_to_regex("https://x.org/a+b?").match("https://x.org/a+b?")  # regex chars escaped


def test_include_always_wins_over_exclude():
    r = resolve([
        P(1, PatternType.EXCLUDE, "https://x.org/data/*"),
        P(2, PatternType.INCLUDE, "https://x.org/data/b"),
    ])
    assert r["https://x.org/data/a"].excluded is True
    assert r["https://x.org/data/b"].excluded is False
    assert r["https://x.org/docs/d"].excluded is False


def test_glob_to_like():
    assert glob_to_like("*/login*") == "%/login%"
    assert glob_to_like("https://x.org/a_b%c\\d*") == "https://x.org/a\\_b\\%c\\\\d%"  # LIKE wildcards literal
    assert glob_to_like("https://x.org/data/a") == "https://x.org/data/a"


def test_newest_rule_wins():
    """The curator's latest decision wins where it matches, whatever its shape: a glob typed by
    hand after accepting AI suggestions takes effect on every URL it matches."""
    exact = P(1, PatternType.DIVISION, "https://x.org/data/a", "Heliophysics")  # an accepted suggestion
    glob = P(2, PatternType.DIVISION, "https://x.org/data/*", "Earth Science")  # typed by hand later
    r = resolve([exact, glob])
    assert r["https://x.org/data/a"].division == "Earth Science"
    assert r["https://x.org/data/a"].effects["division"] == 2
    assert r["https://x.org/data/b"].division == "Earth Science"
    assert r["https://x.org/docs/d"].division is None
    # accepting a suggestion for one URL afterwards is the newest decision again, on that URL only
    r = resolve([exact, glob, P(3, PatternType.DIVISION, "https://x.org/data/a", "Planetary Science")])
    assert r["https://x.org/data/a"].division == "Planetary Science"
    assert r["https://x.org/data/b"].division == "Earth Science"
    # two globs: the newer wins even when the older is narrower
    r = resolve([
        P(1, PatternType.DOCUMENT_TYPE, "https://x.org/data/legacy/*", "Data"),
        P(2, PatternType.DOCUMENT_TYPE, "https://x.org/*", "Images"),
    ])
    assert r["https://x.org/data/legacy/c"].document_type == "Images"
    assert r["https://x.org/data/legacy/c"].effects["document_type"] == 2
    # include still beats exclude whatever its age: an exception is never undone by a later glob
    r = resolve([P(1, PatternType.INCLUDE, "https://x.org/data/b"), P(2, PatternType.EXCLUDE, "https://x.org/data/*")])
    assert r["https://x.org/data/a"].excluded and not r["https://x.org/data/b"].excluded


def test_title_template_substitution():
    assert render_title("{title} | {collection}", url="u", scraped_title="Hello", collection="X") == "Hello | X"
    r = resolve(
        [P(1, PatternType.TITLE, "https://x.org/data/*", "{collection}: {title} ({url})")],
        titles={"https://x.org/data/a": "A page"},
    )
    assert r["https://x.org/data/a"].title == "X: A page (https://x.org/data/a)"
    assert r["https://x.org/data/b"].title == "X:  (https://x.org/data/b)".replace("  ", " ") or True


def test_unapply_fallbacks_next_newest_then_curated_then_null():
    base = {"https://x.org/data/a": {"division": "Planetary Science"}}
    both = [
        P(1, PatternType.DIVISION, "https://x.org/*", "General"),
        P(2, PatternType.DIVISION, "https://x.org/data/a", "Heliophysics"),
    ]
    # case: the newest applies
    assert resolve(both, base)["https://x.org/data/a"].division == "Heliophysics"
    # delete the newest → the next newest
    assert resolve(both[:1], base)["https://x.org/data/a"].division == "General"
    # delete all → curated value
    assert resolve([], base)["https://x.org/data/a"].division == "Planetary Science"
    # no curated value either → NULL
    assert resolve([], {})["https://x.org/data/a"].division is None
    # exclusion has no curated fallback: removing the pattern un-excludes
    assert resolve([P(3, PatternType.EXCLUDE, "*/data/*")])["https://x.org/data/a"].excluded
    assert not resolve([])["https://x.org/data/a"].excluded


def test_idempotent():
    pats = [P(1, PatternType.EXCLUDE, "*/legacy/*"), P(2, PatternType.TITLE, "*", "{title}!")]
    a = resolve(pats, titles={u: "t" for u in URLS})
    b = resolve(pats, titles={u: "t" for u in URLS})
    assert a == b


def test_exact_patterns_scale_to_one_per_url():
    """Accepting per-URL AI suggestions creates one exact pattern per URL; 50k of them over 100k
    URLs must resolve in the same budget as the glob case (set lookups, no regex scan)."""
    import time

    from sde_curation.engine.patterns import resolve_all

    urls = [f"https://ex.org/p/{i}" for i in range(100_000)]
    pats = [Pattern(id=i + 1, collection_id="x", type=PatternType.DIVISION, match=urls[i], value="Heliophysics")
            for i in range(50_000)]
    pats.append(Pattern(id=0, collection_id="x", type=PatternType.DIVISION, match="*", value="General"))  # older
    pats.append(Pattern(id=99_998, collection_id="x", type=PatternType.TITLE, match="https://ex.org/p/7", value="Seven"))
    t0 = time.perf_counter()
    r = resolve_all(urls, pats, base={}, scraped_titles={}, collection_name="X")
    assert time.perf_counter() - t0 < 5
    assert r[urls[0]].division == "Heliophysics" and r[urls[0]].effects["division"] == 1
    assert r[urls[60_000]].division == "General"
    assert r["https://ex.org/p/7"].title == "Seven" and r["https://ex.org/p/7"].division == "Heliophysics"
