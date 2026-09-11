"""Curation service: glue between the pure engine and the database (bulk operations only)."""

from __future__ import annotations

import asyncio

from .db import Database
from .engine.diff import DeltaSet, promote, recompute
from .engine.patterns import is_exact, match_counts
from .engine.urls import canonical_key
from .models import Collection, Pattern, PatternCreate, RuleSource, Status


class CurationService:
    def __init__(self, db: Database, lock_for=None):
        self.db = db
        self._lock_for = lock_for or (lambda cid: asyncio.Lock())

    async def recompute(self, c: Collection) -> DeltaSet:
        """diff + apply patterns in one idempotent pass; persists deltas and pattern effects.
        Serialised per collection so two recomputes (or a recompute and a promote) never
        interleave their delete+insert on delta_urls."""
        async with self._lock_for(c.collection_id):
            return await self._recompute(c)

    async def _recompute(self, c: Collection) -> DeltaSet:
        dump, curated, patterns, previous, failures = (
            await self.db.load_dump(c.collection_id),
            await self.db.load_curated(c.collection_id),
            await self.db.list_patterns(c.collection_id),
            await self.db.load_deltas(c.collection_id),
            await self.db.load_dump_failures(c.collection_id),
        )
        ds = recompute(
            collection_id=c.collection_id,
            collection_name=c.name,
            dump=dump,
            curated=curated,
            patterns=patterns,
            previous=previous,
            failures=failures,
            capped=c.last_crawl_capped,
        )
        await self.db.replace_deltas(c.collection_id, ds.deltas, ds.effects)
        if ds.curated_edited_by:
            await self.db.set_curated_edited_by(c.collection_id, ds.curated_edited_by)
        if ds.curated_crawl_failure:
            await self.db.set_curated_crawl_failure(c.collection_id, ds.curated_crawl_failure)
        return ds

    async def add_pattern(
        self, c: Collection, body: PatternCreate, *, actor: str | None = None,
        source: RuleSource = RuleSource.SME,
    ) -> tuple[Pattern, DeltaSet]:
        p = await self.db.insert_pattern(
            Pattern(collection_id=c.collection_id, created_by=actor, source=source, **body.model_dump())
        )
        return p, await self.recompute(c)

    async def add_patterns(
        self, c: Collection, bodies: list[tuple[PatternCreate, RuleSource]], *, actor: str | None = None
    ) -> tuple[int, DeltaSet]:
        """Bulk accept: insert every pattern with its own source (duplicates skipped), recompute once."""
        async with self._lock_for(c.collection_id):
            n = await self.db.insert_patterns(
                [Pattern(collection_id=c.collection_id, created_by=actor, source=src, **b.model_dump())
                 for b, src in bodies]
            )
            return n, await self._recompute(c)

    async def replace_exact_patterns(
        self, c: Collection, bodies: list[PatternCreate], *, actor: str | None = None,
        source: RuleSource = RuleSource.SME,
    ) -> DeltaSet:
        """Bulk per-URL edits of one field: drop the previous exact-URL rule for each URL (under
        any spelling of it — an exact rule matches by canonical key), insert the new value,
        recompute once."""
        async with self._lock_for(c.collection_id):
            by_type: dict[str, list[str]] = {}
            for b in bodies:
                by_type.setdefault(str(b.type), []).append(b.match)
            for t, matches in by_type.items():
                await self.db.delete_exact_patterns(c.collection_id, t, matches)
            await self._delete_other_spellings(c, by_type)
            await self.db.insert_patterns(
                [Pattern(collection_id=c.collection_id, created_by=actor, source=source, **b.model_dump())
                 for b in bodies]
            )
            return await self._recompute(c)

    async def _delete_other_spellings(self, c: Collection, by_type: dict[str, list[str]]) -> None:
        """Exact rules of the same type for another spelling of the same page would still match
        (and the newest would win): remove them so one page has one per-URL rule per field."""
        wanted = {(t, canonical_key(m)) for t, ms in by_type.items() for m in ms}
        for p in await self.db.list_patterns(c.collection_id):
            if is_exact(p.match) and (str(p.type), canonical_key(p.match)) in wanted and p.id is not None:
                await self.db.delete_pattern(c.collection_id, p.id)

    async def replace_exact_pattern(
        self, c: Collection, body: PatternCreate, *, old_id: int | None, actor: str | None = None,
        source: RuleSource = RuleSource.SME,
    ) -> DeltaSet:
        """Per-URL edit: drop the previous exact-URL rule for that field, insert the new value."""
        async with self._lock_for(c.collection_id):
            if old_id is not None:
                await self.db.delete_pattern(c.collection_id, old_id)
            await self._delete_other_spellings(c, {str(body.type): [body.match]})
            await self.db.insert_pattern(
                Pattern(collection_id=c.collection_id, created_by=actor, source=source, **body.model_dump())
            )
            return await self._recompute(c)

    async def delete_pattern(self, c: Collection, pattern_id: int) -> DeltaSet | None:
        if not await self.db.delete_pattern(c.collection_id, pattern_id):
            return None
        return await self.recompute(c)

    async def pattern_stats(self, c: Collection) -> list[dict]:
        patterns = await self.db.list_patterns(c.collection_id)
        urls = await self.db.dump_urls(c.collection_id)
        counts = match_counts(patterns, urls)
        return [{**p.model_dump(mode="json"), "matches": counts.get(p.id, 0)} for p in patterns]

    async def promote(self, c: Collection, *, actor: str | None = None) -> int:
        async with self._lock_for(c.collection_id):
            return await self._promote(c, actor)

    async def _promote(self, c: Collection, actor: str | None = None) -> int:
        deltas = await self.db.load_deltas(c.collection_id)
        curated = promote(
            await self.db.load_curated(c.collection_id), deltas,
            content_hashes=await self.db.dump_content_hashes(c.collection_id),
        )
        n = await self.db.replace_curated(c.collection_id, curated)
        # the rules did not change: keep the rule→URL effects so the Curated table can still say why
        await self.db.replace_deltas(c.collection_id, [], [], keep_effects=True)
        await self.db.set_flag(c.collection_id, False)
        note = f"promoted {len(deltas)} delta URLs → {n} curated URLs" if deltas else "no delta URLs → curated"
        await self.db.set_status(c.collection_id, Status.CURATED, note=note, force=True, actor=actor)
        return n
