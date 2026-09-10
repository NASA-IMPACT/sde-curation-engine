"""Delta computation — pure, bulk, idempotent.

recompute(dump, curated, patterns) -> deltas
  new       url in dump, not in curated
  modified  url in both and (scraped title changed OR effective curation values changed OR
            the page text changed — `content_changed` overlay flag, only when both sides have a hash)
  deleted   url in curated, not in dump (tombstone; promoted as a removal)
  (no delta) url in both and nothing changed
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..models import CuratedUrl, DeltaKind, DeltaUrl, DumpUrl, Pattern, edited_by_of
from .patterns import resolve_all

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

    @property
    def counts(self) -> dict[str, int]:
        c = {k.value: 0 for k in DeltaKind}
        for d in self.deltas:
            c[d.kind.value] += 1
        c["excluded"] = sum(1 for d in self.deltas if d.excluded and d.kind is not DeltaKind.DELETED)
        c["content_changed"] = sum(1 for d in self.deltas if d.content_changed)
        return c


def recompute(
    *,
    collection_id: str,
    collection_name: str,
    dump: list[DumpUrl],
    curated: list[CuratedUrl],
    patterns: list[Pattern],
    previous: list[DeltaUrl] | None = None,
) -> DeltaSet:
    dump_by = {d.url: d for d in dump}
    cur_by = {c.url: c for c in curated}
    prev_by = {p.url: p for p in previous or []}
    urls = list(dump_by)
    source_of = {p.id: str(p.source) for p in patterns}

    base: dict[str, dict[str, Any]] = {
        u: {"title": c.title, "division": c.division, "document_type": c.document_type}
        for u, c in cur_by.items()
    }
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
    for u in urls:
        d, r, c = dump_by[u], resolved[u], cur_by.get(u)
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
        if c is None:
            kind = DeltaKind.NEW
        elif content_changed or eff != (c.scraped_title, c.title, c.division, c.document_type, c.excluded):
            kind = DeltaKind.MODIFIED
        else:
            if edited_by != c.edited_by:
                curated_edited_by.append((u, edited_by))
            continue
        prev = prev_by.get(u)
        deltas.append(
            DeltaUrl(
                collection_id=collection_id,
                url=u,
                kind=kind,
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
    for u, c in cur_by.items():
        if u not in dump_by:
            deltas.append(
                DeltaUrl(
                    collection_id=collection_id,
                    url=u,
                    kind=DeltaKind.DELETED,
                    scraped_title=c.scraped_title,
                    title=c.title,
                    division=c.division,
                    document_type=c.document_type,
                    excluded=c.excluded,
                    edited_by=c.edited_by,
                )
            )
    return DeltaSet(deltas, effects, curated_edited_by)


def promote(
    curated: list[CuratedUrl], deltas: list[DeltaUrl], content_hashes: dict[str, str | None] | None = None
) -> list[CuratedUrl]:
    """Apply deltas to the curated set: tombstones remove, everything else upserts. Every
    surviving row takes the current dump text hash (`content_hashes`): the export always ships
    the current dump text, so after a promote that is what the index holds."""
    hashes = content_hashes or {}
    by = {c.url: c for c in curated}
    for d in deltas:
        if d.kind is DeltaKind.DELETED:
            by.pop(d.url, None)
        else:
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
