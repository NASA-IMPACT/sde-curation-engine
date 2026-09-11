"""Delta computation — pure, bulk, idempotent.

recompute(dump, curated, patterns, failures, capped) -> deltas
  new       url in dump, not in curated
  modified  url in both and (scraped title changed OR effective curation values changed OR
            the page text changed — `content_changed` overlay flag, only when both sides have a hash)
            — or the same page under a new spelling (`renamed_from`, see below)
  deleted   url in curated, not in dump, and the crawl is evidence it is gone (tombstone; promoted
            as a removal)
  (no delta) url in both and nothing changed; or url in curated, not in dump, but the crawl is
            not evidence it is gone — the row is kept and flagged (`crawl_failure`)

URL identity. Rows are matched by exact string first, then by canonical key (host + path + query:
no scheme, no trailing slash, no #fragment — `engine.urls.canonical_key`). A curated URL whose
only change is the spelling is one `modified` delta carrying `renamed_from`, not a new row plus a
tombstone; promote moves the row. Two dump spellings of one curated page: the better one
(https, then shorter) is the rename, the other is `new`.

Crawl failures. A curated URL missing from the dump is a tombstone only when the crawler saw it
gone (http_404 / http_410) or, in a complete crawl, never met it at all. A URL the crawler tried
and could not fetch (403, rate limit, timeout, challenge page, empty extract …) stays in the
curated set with `crawl_failure` = the reason; when the crawl stopped at its page cap, every
unmet curated URL stays too (`not_visited`) — an incomplete crawl proves nothing about them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..models import (
    NOT_VISITED,
    CuratedUrl,
    DeltaKind,
    DeltaUrl,
    DumpUrl,
    Pattern,
    edited_by_of,
    failure_means_gone,
)
from .patterns import resolve_all
from .urls import canonical_key, url_rank

# AI suggestions (and their provenance) survive a recompute: copied from the previous delta row.
_AI_FIELDS = ("title_ai", "division_ai", "document_type_ai", "title_ai_conf", "division_ai_conf",
              "document_type_ai_conf", "ai_model", "ai_content_hash")


@dataclass
class DeltaSet:
    deltas: list[DeltaUrl]
    effects: list[tuple[int, str, str]] = field(default_factory=list)  # (pattern_id, url, field)
    # curated rows that did not change but whose "edited by" summary did (rules re-attributed, or
    # rows promoted before the column existed): (url, edited_by) to write back without a delta
    curated_edited_by: list[tuple[str, str | None]] = field(default_factory=list)
    # curated rows whose crawl-failure flag changed: (url, reason-or-None) to write back. A row
    # absent from the dump but not proven gone gets its reason; a row the crawl fetched gets None.
    curated_crawl_failure: list[tuple[str, str | None]] = field(default_factory=list)
    # every curated URL kept despite being absent from the dump, with the reason (for the counts)
    kept: dict[str, str] = field(default_factory=dict)

    @property
    def counts(self) -> dict[str, int]:
        c = {k.value: 0 for k in DeltaKind}
        for d in self.deltas:
            c[d.kind.value] += 1
        c["excluded"] = sum(1 for d in self.deltas if d.excluded and d.kind is not DeltaKind.DELETED)
        c["content_changed"] = sum(1 for d in self.deltas if d.content_changed)
        c["renamed"] = sum(1 for d in self.deltas if d.renamed_from)
        c["kept"] = len(self.kept)
        return c


def match_urls(dump_urls: list[str], curated_urls: list[str]) -> dict[str, str]:
    """dump url -> curated url for pages present on both sides: exact string first, then one
    canonical-key match per curated row among the URLs neither side matched exactly."""
    cur_set = set(curated_urls)
    pairs = {u: u for u in dump_urls if u in cur_set}
    claimed = set(pairs)
    spare_cur: dict[str, str] = {}
    for u in curated_urls:
        if u not in claimed:
            spare_cur.setdefault(canonical_key(u), u)
    if not spare_cur:
        return pairs
    best: dict[str, str] = {}  # canonical key -> best-ranked unmatched dump spelling
    for u in dump_urls:
        if u in claimed:
            continue
        k = canonical_key(u)
        if k in spare_cur and (k not in best or url_rank(u) < url_rank(best[k])):
            best[k] = u
    for k, u in best.items():
        pairs[u] = spare_cur[k]
    return pairs


def recompute(
    *,
    collection_id: str,
    collection_name: str,
    dump: list[DumpUrl],
    curated: list[CuratedUrl],
    patterns: list[Pattern],
    previous: list[DeltaUrl] | None = None,
    failures: dict[str, str] | None = None,
    capped: bool = False,
) -> DeltaSet:
    """`failures`: url -> crawler reason for every URL the crawl tried and could not fetch;
    `capped`: the crawl stopped at its page cap (unmet curated URLs are kept, not removed)."""
    dump_by = {d.url: d for d in dump}
    cur_by = {c.url: c for c in curated}
    prev_by = {p.url: p for p in previous or []}
    urls = list(dump_by)
    source_of = {p.id: str(p.source) for p in patterns}
    pair = match_urls(urls, list(cur_by))  # dump url -> curated url (same page)
    matched_cur = set(pair.values())

    base: dict[str, dict[str, Any]] = {}
    for u, cu in pair.items():
        c = cur_by[cu]
        base[u] = {"title": c.title, "division": c.division, "document_type": c.document_type}
    resolved = resolve_all(
        urls,
        patterns,
        base=base,
        scraped_titles={u: d.scraped_title for u, d in dump_by.items()},
        collection_name=collection_name,
    )

    deltas: list[DeltaUrl] = []
    effects: list[tuple[int, str, str]] = []
    curated_edited_by: list[tuple[str, str | None]] = []
    curated_crawl_failure: list[tuple[str, str | None]] = []
    kept: dict[str, str] = {}
    for u in urls:
        d, r = dump_by[u], resolved[u]
        cu = pair.get(u)
        c = cur_by[cu] if cu else None
        for fld, pid in r.effects.items():
            effects.append((pid, u, fld))
        edited_by = edited_by_of([source_of[pid] for pid in r.effects.values() if pid in source_of])
        # curated excluded flag is only kept if it came from a pattern; otherwise not excluded
        eff = (d.scraped_title, r.title, r.division, r.document_type, r.excluded)
        # A NULL hash on either side means "unknown", never "changed": rows promoted before
        # hashing existed do not all become deltas on the first recompute after it lands.
        content_changed = bool(
            c is not None and c.content_hash and d.content_hash and c.content_hash != d.content_hash
        )
        renamed_from = cu if cu is not None and cu != u else None
        if c is None:
            kind = DeltaKind.NEW
        elif (renamed_from or content_changed
              or eff != (c.scraped_title, c.title, c.division, c.document_type, c.excluded)):
            kind = DeltaKind.MODIFIED
        else:
            if edited_by != c.edited_by:
                curated_edited_by.append((u, edited_by))
            if c.crawl_failure:  # fetched again: the flag comes down
                curated_crawl_failure.append((u, None))
            continue
        prev = prev_by.get(u)
        deltas.append(
            DeltaUrl(
                collection_id=collection_id,
                url=u,
                kind=kind,
                renamed_from=renamed_from,
                scraped_title=d.scraped_title,
                title=r.title,
                division=r.division,
                document_type=r.document_type,
                excluded=r.excluded,
                content_changed=content_changed,
                edited_by=edited_by,
                **({k: getattr(prev, k) for k in _AI_FIELDS} if prev else {}),
            )
        )
    failed = failures or {}
    failed_by_key = {canonical_key(u): reason for u, reason in failed.items()}
    for u, c in cur_by.items():
        if u in matched_cur:
            continue
        reason = failed.get(u)
        if reason is None:
            reason = failed_by_key.get(canonical_key(u))
        if (reason is not None and not failure_means_gone(reason)) or (reason is None and capped):
            reason = reason or NOT_VISITED
            kept[u] = reason
            if c.crawl_failure != reason:
                curated_crawl_failure.append((u, reason))
            continue
        deltas.append(
            DeltaUrl(
                collection_id=collection_id,
                url=u,
                kind=DeltaKind.DELETED,
                crawl_failure=reason,
                scraped_title=c.scraped_title,
                title=c.title,
                division=c.division,
                document_type=c.document_type,
                excluded=c.excluded,
                edited_by=c.edited_by,
            )
        )
    return DeltaSet(deltas, effects, curated_edited_by, curated_crawl_failure, kept)


def promote(
    curated: list[CuratedUrl], deltas: list[DeltaUrl], content_hashes: dict[str, str | None] | None = None
) -> list[CuratedUrl]:
    """Apply deltas to the curated set: tombstones remove, renames move, everything else upserts.
    Every row the dump has takes the current dump text hash (`content_hashes`); the store copies
    the matching dump text onto the row at the same time (`Database.replace_curated`), so the
    curated set carries exactly the text its hash fingerprints and the export ships that. Rows
    the dump lacks (kept through a crawl failure) keep their hash, text and flag."""
    hashes = content_hashes or {}
    by = {c.url: c for c in curated}
    for d in deltas:
        if d.kind is DeltaKind.DELETED:
            by.pop(d.url, None)
        else:
            if d.renamed_from:
                by.pop(d.renamed_from, None)
            by[d.url] = CuratedUrl(
                collection_id=d.collection_id,
                url=d.url,
                scraped_title=d.scraped_title,
                title=d.title,
                division=d.division,
                document_type=d.document_type,
                excluded=d.excluded,
                edited_by=d.edited_by,
            )
    if hashes:
        for u, c in by.items():
            if u in hashes:
                by[u] = c.model_copy(update={"content_hash": hashes[u]})
    return list(by.values())
