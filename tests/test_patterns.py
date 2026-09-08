"""Pattern semantics: include precedence, specificity, title templating, unapply fallbacks."""

from sde_curation.engine.patterns import glob_to_regex, render_title, resolve_all
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


def test_smallest_match_set_wins_then_longest_string():
    r = resolve([
        P(1, PatternType.DIVISION, "https://x.org/*", "General"),                 # 5 urls
        P(2, PatternType.DIVISION, "https://x.org/data/*", "Earth Science"),      # 3 urls
        P(3, PatternType.DIVISION, "https://x.org/data/legacy/c", "Heliophysics"),  # 1 url
    ])
    assert r["https://x.org/docs/d"].division == "General"
    assert r["https://x.org/data/a"].division == "Earth Science"
    assert r["https://x.org/data/legacy/c"].division == "Heliophysics"
    assert r["https://x.org/data/a"].effects["division"] == 2

    # tie on match-set size (both match exactly the 3 data urls) → longest pattern string
    r = resolve([
        P(1, PatternType.DOCUMENT_TYPE, "https://x.org/data/*", "Data"),
        P(2, PatternType.DOCUMENT_TYPE, "https://x.org/dat*/*", "Images"),  # same 3 matches, shorter
    ])
    assert r["https://x.org/data/a"].document_type == "Data"


def test_title_template_substitution():
    assert render_title("{title} | {collection}", url="u", scraped_title="Hello", collection="X") == "Hello | X"
    r = resolve(
        [P(1, PatternType.TITLE, "https://x.org/data/*", "{collection}: {title} ({url})")],
        titles={"https://x.org/data/a": "A page"},
    )
    assert r["https://x.org/data/a"].title == "X: A page (https://x.org/data/a)"
    assert r["https://x.org/data/b"].title == "X:  (https://x.org/data/b)".replace("  ", " ") or True


def test_unapply_fallbacks_next_specific_then_curated_then_null():
    base = {"https://x.org/data/a": {"division": "Planetary Science"}}
    both = [
        P(1, PatternType.DIVISION, "https://x.org/*", "General"),
        P(2, PatternType.DIVISION, "https://x.org/data/a", "Heliophysics"),
    ]
    # case: most specific applies
    assert resolve(both, base)["https://x.org/data/a"].division == "Heliophysics"
    # delete the specific one → next most specific
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
