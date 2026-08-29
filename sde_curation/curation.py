"""Curation service: glue between the pure engine and the database (bulk operations only)."""

from __future__ import annotations

import asyncio

from .db import Database
from .engine.diff import DeltaSet, promote, recompute
from .engine.patterns import match_counts
from .models import Collection, Pattern, PatternCreate, Status


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
        dump, curated, patterns, previous = (
            await self.db.load_dump(c.collection_id),
            await self.db.load_curated(c.collection_id),
            await self.db.list_patterns(c.collection_id),
            await self.db.load_deltas(c.collection_id),
        )
        ds = recompute(
            collection_id=c.collection_id,
            collection_name=c.name,
            dump=dump,
            curated=curated,
            patterns=patterns,
            previous=previous,
        )
        await self.db.replace_deltas(c.collection_id, ds.deltas, ds.effects)
        return ds

    async def add_pattern(self, c: Collection, body: PatternCreate) -> tuple[Pattern, DeltaSet]:
        p = await self.db.insert_pattern(Pattern(collection_id=c.collection_id, **body.model_dump()))
        return p, await self.recompute(c)

    async def replace_exact_pattern(
        self, c: Collection, body: PatternCreate, *, old_id: int | None
    ) -> DeltaSet:
        """Per-URL edit: insert the new value first, then drop the previous one, so a failed
        insert never loses the curator's earlier edit."""
        async with self._lock_for(c.collection_id):
            if old_id is not None:
                await self.db.delete_pattern(c.collection_id, old_id)
            await self.db.insert_pattern(
                Pattern(collection_id=c.collection_id, **body.model_dump())
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

    async def promote(self, c: Collection) -> int:
        async with self._lock_for(c.collection_id):
            return await self._promote(c)

    async def _promote(self, c: Collection) -> int:
        deltas = await self.db.load_deltas(c.collection_id)
        curated = promote(await self.db.load_curated(c.collection_id), deltas)
        n = await self.db.replace_curated(c.collection_id, curated)
        await self.db.replace_deltas(c.collection_id, [], [])
        await self.db.set_flag(c.collection_id, False)
        note = f"promoted {len(deltas)} deltas → {n} curated" if deltas else "nothing pending → curated"
        await self.db.set_status(c.collection_id, Status.CURATED, note=note, force=True)
        return n
