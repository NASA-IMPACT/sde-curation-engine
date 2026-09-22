"""Curation service: glue between the pure engine and the database (bulk operations only)."""

from __future__ import annotations

import asyncio

from .db import Database
from .engine.diff import DeltaSet, promote, recompute
from .engine.patterns import is_exact, match_counts, resolve_all
from .engine.urls import canonical_key
from .models import (
    Collection,
    DeltaKind,
    Pattern,
    PatternCreate,
    PatternType,
    RuleSource,
    Status,
    division_assigned,
)


class IncompleteMetadata(Exception):
    """Promote refused: some delta URLs would reach the curated set without a title, a division or a
    document type. `counts` is Database.incomplete_counts."""

    def __init__(self, counts: dict[str, int]):
        self.counts = counts
        general = counts.get("general", 0)
        missing = ", ".join(f"{counts[f]} without a {label}" for f, label in
                            (("title", "title"), ("division", "division"), ("document_type", "document type"))
                            if counts[f])
        if general:  # the same rows as `division`, but they read as set until you look
            missing += f" (of those, {general} still on the General placeholder)"
        n = counts["urls"]
        super().__init__(f"{n} delta URL{'s' if n != 1 else ''} cannot be promoted yet ({missing}):"
                         " accept the AI suggestions or set the values by hand first")


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
            await self.db.load_rules(c.collection_id),
            await self.db.load_deltas(c.collection_id),
            await self.db.load_dump_failures(c.collection_id),
        )
        # pure CPU over the whole collection: in a thread, so the event loop keeps serving everyone else
        ds = await asyncio.to_thread(
            recompute,
            collection_id=c.collection_id,
            collection_name=c.name,
            dump=dump,
            curated=curated,
            patterns=patterns,
            previous=previous,
            failures=failures,
            capped=c.last_crawl_capped,
            division=c.division if division_assigned(c.division) else None,
        )
        await self.db.replace_deltas(c.collection_id, ds.deltas, ds.effects)
        if ds.curated_edited_by:
            await self.db.set_curated_edited_by(c.collection_id, ds.curated_edited_by)
        if ds.curated_crawl_failure:
            await self.db.set_curated_crawl_failure(c.collection_id, ds.curated_crawl_failure)
        if ds.curated_excluded:
            await self.db.set_curated_excluded(c.collection_id, ds.curated_excluded)
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
        rules = await self.db.exact_pattern_matches(c.collection_id, list(by_type))
        await self.db.delete_patterns(
            c.collection_id, [pid for pid, t, m in rules if (t, canonical_key(m)) in wanted]
        )

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

    async def set_excluded(self, c: Collection, url: str, excluded: bool, *, actor: str | None = None) -> DeltaSet:
        """The ✗ exclude / ✓ include toggle on a row: make the URL excluded (or included) and stay
        that way however often it is clicked. Per-URL rules saying the opposite go (a stale exact
        include that kept it in, or exact exclude that kept it out); a per-URL rule is added only
        when the glob rules alone would not give the wanted state, so a plain "include" on a row no
        exclude touches leaves no rule behind."""
        async with self._lock_for(c.collection_id):
            key = canonical_key(url)
            wanted = PatternType.EXCLUDE if excluded else PatternType.INCLUDE
            # only exclude / include rules decide this; the per-URL metadata rules (three per URL) do not
            patterns = await self.db.list_patterns(
                c.collection_id, types=[PatternType.EXCLUDE, PatternType.INCLUDE])
            mine = [p for p in patterns if p.type in (PatternType.EXCLUDE, PatternType.INCLUDE)
                    and is_exact(p.match) and canonical_key(p.match) == key]
            drop = [p for p in mine if p.type is not wanted]
            for p in drop:
                await self.db.delete_pattern(c.collection_id, p.id)  # type: ignore[arg-type]
            rest = [p for p in patterns if p not in drop]
            if not any(p.type is wanted for p in mine):
                r = resolve_all([url], rest, base={}, scraped_titles={}, collection_name=c.name,
                                division_default=c.division if division_assigned(c.division) else None)[url]
                if r.excluded != excluded:
                    await self.db.insert_pattern(Pattern(collection_id=c.collection_id, type=wanted, match=url,
                                                         created_by=actor, source=RuleSource.SME))
            return await self._recompute(c)

    async def delete_pattern(self, c: Collection, pattern_id: int) -> DeltaSet | None:
        if not await self.db.delete_pattern(c.collection_id, pattern_id):
            return None
        return await self.recompute(c)

    @staticmethod
    def rows_set(c: Collection) -> str:
        """The URL set a rule's effect is visible in right now: the delta URLs while there is
        something to review, the curated URLs once promoted, the dump before curating starts."""
        return "delta" if c.delta_count else "curated" if c.curated_rows else "dump"

    async def pattern_stats(self, c: Collection, *, exact_limit: int | None = None, exact_offset: int = 0) -> list[dict]:
        """Rules with `matches` = how many URLs of rows_set(c) each matches (the rows the Rules
        table links to, so the number is the number of rows the click shows) and `in_effect` = how
        many URLs it currently decides (0 with matches > 0: a newer rule has superseded it).
        Every rule by default; with `exact_limit` every glob rule plus that page of the per-URL
        rules — there can be three of those per URL, and the Rules tab shows them a page at a time."""
        if exact_limit is None:
            patterns = await self.db.list_patterns(c.collection_id)
        else:
            patterns = (await self.db.list_patterns(c.collection_id, exact=False)
                        + await self.db.list_patterns(c.collection_id, exact=True, limit=exact_limit, offset=exact_offset))
        set_ = self.rows_set(c)
        counts = await asyncio.to_thread(match_counts, patterns, await self.db.set_urls(c.collection_id, set_))
        # exclude rules keep URLs out of the delta URLs altogether, so theirs are counted over the dump
        excludes = [p for p in patterns if p.type is PatternType.EXCLUDE]
        if excludes and set_ != "dump":
            counts.update(await asyncio.to_thread(match_counts, excludes, await self.db.set_urls(c.collection_id, "dump")))
        effects = await self.db.effect_counts(
            c.collection_id, None if exact_limit is None else [p.id for p in patterns if p.id is not None])
        return [{**p.model_dump(mode="json"), "matches": counts.get(p.id, 0), "in_effect": effects.get(p.id, 0),
                 "set": "dump" if p.type is PatternType.EXCLUDE else set_}
                for p in patterns]

    async def promote(self, c: Collection, *, actor: str | None = None) -> int:
        async with self._lock_for(c.collection_id):
            return await self._promote(c, actor)

    async def _check_complete(self, c: Collection, urls: list[str] | None = None) -> None:
        """Nothing reaches the curated set without a title, a division and a document type."""
        counts = await self.db.incomplete_counts(c.collection_id, urls)
        if counts["urls"]:
            raise IncompleteMetadata(counts)

    async def _promote(self, c: Collection, actor: str | None = None) -> int:
        await self._check_complete(c)
        deltas = await self.db.load_deltas(c.collection_id)
        curated = await asyncio.to_thread(
            promote, await self.db.load_curated(c.collection_id), deltas,
            content_hashes=await self.db.dump_content_hashes(c.collection_id),
        )
        # a promote with an empty queue is the "mark curated" shortcut: it moves nothing, so it
        # leaves the index as up to date as it already was
        n = await self.db.replace_curated(c.collection_id, curated, changed=bool(deltas))
        # the rules did not change: keep the rule→URL effects so the Curated table can still say why
        await self.db.replace_deltas(c.collection_id, [], [], keep_effects=True)
        await self.db.set_flag(c.collection_id, False)
        note = f"promoted {len(deltas)} delta URLs → {n} curated URLs" if deltas else "no delta URLs → curated"
        await self.db.set_status(c.collection_id, Status.CURATED, note=note, force=True, actor=actor)
        return n

    async def promote_urls(self, c: Collection, urls: list[str]) -> tuple[int, DeltaSet]:
        """Promote only these delta URLs (refused with IncompleteMetadata if any lacks a title, division or
        document type): the same pure promote(), applied to the picked rows alone
        (it already moves renames and drops tombstones). The rest of the queue stays as it is, so
        the curated set is written whole but only the picked rows take the dump's current text and
        hash — a row still under review must not quietly pick up text its metadata was never
        approved with, nor a hash that would make its pending delta vanish on the next recompute.
        The rules did not change, so the rule→URL effects stay; only a promoted tombstone's go (its
        URL is in neither set any more). Status is the caller's (web _after_curation_change).
        Returns (curated URLs that reach the index, the delta URLs left to review)."""
        async with self._lock_for(c.collection_id):
            wanted = set(urls)
            deltas = await self.db.load_deltas(c.collection_id)
            picked = [d for d in deltas if d.url in wanted]
            left = [d for d in deltas if d.url not in wanted]
            if not picked:
                return c.curated_count, DeltaSet(left)
            await self._check_complete(c, [d.url for d in picked])
            hashes = await self.db.dump_content_hashes(c.collection_id)
            curated = promote(
                await self.db.load_curated(c.collection_id), picked,
                content_hashes={u: h for u, h in hashes.items() if u in wanted},
            )
            picked_urls = [d.url for d in picked]
            n = await self.db.replace_curated(c.collection_id, curated)
            await self.db.delete_deltas(c.collection_id, picked_urls)
            await self.db.delete_effects(c.collection_id, [d.url for d in picked if d.kind is DeltaKind.DELETED])
            if not left:  # the whole queue is through: the re-curation flag comes down, as in _promote
                await self.db.set_flag(c.collection_id, False)
            return n, DeltaSet(left)
