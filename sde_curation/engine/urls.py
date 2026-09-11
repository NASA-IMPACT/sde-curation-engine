"""URL normalisation — pure. The same page is usually crawled as http and https, with or without
a trailing slash, sometimes with a #fragment. `canonical_key` is the engine's notion of "the same
page": the diff pairs dump and curated rows by it (engine/diff.py), exact-URL rules match by it
(engine/patterns.py), and the pattern job shows the model each page once, in site-section order."""

from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urlsplit


def canonical_key(url: str) -> str:
    """host + path (+ query), lower-cased host, no scheme, no fragment, no trailing slash."""
    p = urlsplit(url.strip())
    path = p.path.rstrip("/") or "/"
    key = f"{p.netloc.lower()}{path}"
    if p.query:
        key += "?" + p.query
    return key


def dedupe_variants(urls: Iterable[str]) -> list[str]:
    """One representative per canonical key (prefer https, then the shorter spelling), sorted by
    host and path so consecutive URLs belong to the same section of the site."""
    best: dict[str, str] = {}
    for u in urls:
        k = canonical_key(u)
        cur = best.get(k)
        if cur is None or url_rank(u) < url_rank(cur):
            best[k] = u
    return [best[k] for k in sorted(best)]


def url_rank(url: str) -> tuple[int, int]:
    """Lower is the preferred spelling of a page: https first, then the shorter string."""
    return (0 if url.startswith("https://") else 1, len(url))


def batches[T](items: list[T], size: int) -> list[list[T]]:
    size = max(1, size)
    return [items[i : i + size] for i in range(0, len(items), size)]
