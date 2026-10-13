"""engine/text.py: normalisation, content hash and the head+tail budget."""

from sde_curation.engine.text import content_hash, normalize_text


def test_normalize_and_hash_ignore_whitespace_jitter():
    assert normalize_text("  a\n\n b\t c ") == "a b c"
    assert content_hash("a b c") == content_hash("a\n\n  b\tc\n")
    assert content_hash("a b c") != content_hash("a b d")
    assert content_hash("") is None and content_hash(None) is None and content_hash("  \n ") is None
    assert len(content_hash("x")) == 64
