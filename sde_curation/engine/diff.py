"""Delta computation — pure, bulk, idempotent.

recompute(dump, curated, patterns) -> deltas
  new       url in dump, not in curated
  modified  url in both and (scraped title changed OR effective curation values changed)
  deleted   url in curated, not in dump (tombstone; promoted as a removal)
  (no delta) url in both and nothing changed
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..models import CuratedUrl, DeltaKind, DeltaUrl, DumpUrl, Pattern
from .patterns import resolve_all


@dataclass
class DeltaSet:
    deltas: list[DeltaUrl]
    effects: list[tuple[int, str, str]] = field(default_factory=list)  # (pattern_id, url, field)

    @property
    def counts(self) -> dict[str, int]:
        c = {k.value: 0 for k in DeltaKind}
        for d in self.deltas:
            c[d.kind.value] += 1
        c["excluded"] = sum(1 for d in self.deltas if d.excluded and d.kind is not DeltaKind.DELETED)
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
    for u in urls:
        d, r, c = dump_by[u], resolved[u], cur_by.get(u)
        for fld, pid in r.effects.items():
            effects.append((pid, u, fld))
        # curated excluded flag is only kept if it came from a pattern; otherwise not excluded
        eff = (d.scraped_title, r.title, r.division, r.document_type, r.excluded)
        if c is None:
            kind = DeltaKind.NEW
        elif eff != (c.scraped_title, c.title, c.division, c.document_type, c.excluded):
            kind = DeltaKind.MODIFIED
        else:
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
                title_ai=prev.title_ai if prev else None,
                division_ai=prev.division_ai if prev else None,
                document_type_ai=prev.document_type_ai if prev else None,
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
                )
            )
    return DeltaSet(deltas, effects)


def promote(curated: list[CuratedUrl], deltas: list[DeltaUrl]) -> list[CuratedUrl]:
    """Apply deltas to the curated set: tombstones remove, everything else upserts."""
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
            )
    return list(by.values())
