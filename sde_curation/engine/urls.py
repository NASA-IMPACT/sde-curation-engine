"""URL normalisation — pure. The same page is usually crawled as http and https, on the apex
host and on www., with or without a trailing slash, sometimes with a #fragment. `canonical_key`
is the engine's notion of "the same page": the dump is loaded with one row per key (jobs.py
ingest_dump), the diff pairs dump and curated rows by it (engine/diff.py), exact-URL rules match
by it (engine/patterns.py), and the pattern job shows the model each page once, in site-section
order."""

from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
from typing import Any
from urllib.parse import urlsplit


@lru_cache(maxsize=1 << 19)  # every recompute asks for the same URLs again; urlsplit is the cost
def canonical_key(url: str) -> str:
    """host + path (+ query): lower-cased host without a leading www., no scheme, no fragment,
    no trailing slash. www. and the apex host are one site everywhere else in the app too
    (a collection id is the apex host)."""
    p = urlsplit(url.strip())
    path = p.path.rstrip("/") or "/"
    key = f"{p.netloc.lower().removeprefix('www.')}{path}"
    if p.query:
        key += "?" + p.query
    return key


def spellings(url: str) -> list[str]:
    """Every spelling of `url` that canonical_key folds into one page: https/http x www. x trailing
    slash (the #fragment forms are open-ended; Database.match_clause adds a LIKE for them). SQL has
    no canonical key, so this is how an exact-URL rule is turned into a `?match=` filter that finds
    the same rows the engine matches."""
    p = urlsplit(url.strip())
    host = p.netloc.lower().removeprefix("www.")
    path = p.path.rstrip("/")
    tail = ("?" + p.query) if p.query else ""
    out: list[str] = []
    for scheme in ("https", "http"):
        for h in (host, "www." + host):
            for pp in ((path or "/"), path + "/"):
                u = f"{scheme}://{h}{pp}{tail}"
                if u not in out:
                    out.append(u)
    return out


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


def duplicate_docs(docs: Iterable[dict[str, Any]]) -> set[int]:
    """Indexes of the crawl documents to drop so each page is stored once. Two documents are the
    same page when their resolved URLs (`final_url`, where the request ended up after redirects;
    the requested `url` when the crawler did not record one) share a canonical key: `/map` and
    `/maps` collapse when the crawler saw both land on one page, and so do http/https, trailing
    slash, www. and #fragment spellings. The survivor is the one whose own URL is the resolved
    page, then https, then the shorter spelling. A document alone on its page is always kept."""
    best: dict[str, tuple[tuple[int, int, int], int]] = {}
    for i, d in enumerate(docs):
        url = d.get("url")
        if not url:
            continue
        resolved = d.get("final_url") or url
        key = canonical_key(resolved)
        rank = ((0 if canonical_key(url) == key else 1), *url_rank(url))
        cur = best.get(key)
        if cur is None or rank < cur[0]:
            best[key] = (rank, i)
    keep = {i for _, i in best.values()}
    return {i for i, d in enumerate(docs) if d.get("url") and i not in keep}


def url_rank(url: str) -> tuple[int, int]:
    """Lower is the preferred spelling of a page: https first, then the shorter string."""
    return (0 if url.startswith("https://") else 1, len(url))


def batches[T](items: list[T], size: int) -> list[list[T]]:
    size = max(1, size)
    return [items[i : i + size] for i in range(0, len(items), size)]
