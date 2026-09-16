"""engine/urls.py and the global exclude list."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from sde_curation.engine.urls import batches, canonical_key, dedupe_variants, duplicate_docs
from sde_curation.llm.global_excludes import DEFAULT_PATH, global_exclude_hits, load_global_excludes
from sde_curation.models import GlobalExcludeList


def test_canonical_key_ignores_scheme_www_slash_and_fragment():
    assert canonical_key("https://Ex.org/a/") == canonical_key("http://ex.org/a#top") == "ex.org/a"
    assert canonical_key("https://www.ex.org/a") == canonical_key("https://WWW.ex.org/a/") == "ex.org/a"
    assert canonical_key("https://wwwx.org/a") == "wwwx.org/a"  # only a www. label, not any www prefix
    assert canonical_key("https://ex.org/") == "ex.org/"
    assert canonical_key("https://ex.org/map?obs=1") == "ex.org/map?obs=1" != canonical_key("https://ex.org/map")


def test_dedupe_prefers_https_and_sorts_by_path():
    urls = ["http://ex.org/b", "https://ex.org/b/", "https://ex.org/a", "http://ex.org/a/x", "https://ex.org/b"]
    assert dedupe_variants(urls) == ["https://ex.org/a", "http://ex.org/a/x", "https://ex.org/b"]
    assert batches(list(range(7)), 3) == [[0, 1, 2], [3, 4, 5], [6]] and batches([], 3) == []


def test_duplicate_docs_collapse_redirects_onto_the_resolved_page():
    docs = [
        {"url": "https://ex.org/maps", "final_url": "https://ex.org/map"},  # redirected: drop
        {"url": "https://ex.org/map", "final_url": "https://ex.org/map"},
        {"url": "http://ex.org/map/", "final_url": "https://ex.org/map/"},  # a spelling of the same page: drop
        {"url": "https://ex.org/old", "final_url": "https://ex.org/new"},  # resolved page not crawled itself: kept
        {"url": "https://ex.org/x"},  # no final_url recorded (older crawler): dedupe on url
        {"url": "http://ex.org/x"},
        {"url": ""},
    ]
    assert duplicate_docs(docs) == {0, 2, 5}
    # the aurorasaurus.org case: every page crawled on the apex host and again on www.
    site = [{"url": "https://ex.org/map/"}, {"url": "https://www.ex.org/map"}, {"url": "https://www.ex.org/x"},
            {"url": "https://ex.org/x"}, {"url": "https://www.ex.org/only-www"}]
    assert duplicate_docs(site) == {1, 2}
    # when no requested URL is the resolved page itself, the usual preference (https, shorter) decides
    both = [{"url": "http://ex.org/a", "final_url": "https://ex.org/z"}, {"url": "https://ex.org/b", "final_url": "https://ex.org/z"}]
    assert duplicate_docs(both) == {0}


def test_global_excludes_load_validate_and_hit():
    gl = load_global_excludes(DEFAULT_PATH)
    assert gl.version >= 1 and len(gl.patterns) >= 15 and all(p.source in ("sme", "cosmos") for p in gl.patterns)
    hits = global_exclude_hits(gl, ["https://ex.org/login", "http://ex.org/login/", "https://ex.org/science"])
    assert [(h["match"], h["matches"], h["source"]) for h in hits] == [("*/login*", 2, "global")]
    with pytest.raises(ValidationError):
        GlobalExcludeList.model_validate({"patterns": [{"match": "", "rationale": "x"}]})
    with pytest.raises(ValidationError):
        GlobalExcludeList.model_validate({"patterns": [{"match": "*/x*", "rationale": "x", "source": "me"}]})


def test_global_excludes_custom_path(tmp_path: Path):
    p = tmp_path / "g.yaml"
    p.write_text("version: 2\npatterns:\n  - match: '*/foo*'\n    rationale: r\n    source: cosmos\n")
    gl = load_global_excludes(p)
    assert gl.version == 2 and gl.patterns[0].source == "cosmos"
    p.write_text("version: 3\npatterns: []\n")
    import os
    import time
    os.utime(p, (time.time() + 5, time.time() + 5))  # cache is keyed on mtime
    assert load_global_excludes(p).version == 3
