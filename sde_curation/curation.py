"""Curation service: glue between the pure engine and the database (bulk operations only)."""

from __future__ import annotations

import asyncio
from typing import Any

from .db import Database
from .engine.diff import DeltaSet, promote, recompute
from .engine.patterns import glob_to_like, is_exact, match_counts, resolve_all
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
    document type, or under a title another page is already indexed under. `counts` is
    Database.incomplete_counts — a row with no title rule but a scraped title is not blank: the
    export indexes it under the scraped title. It is refused only if that title is shared."""

    def __init__(self, counts: dict[str, int]):
        self.counts = counts
        general = counts.get("general", 0)
        # the General note belongs to the division clause: those rows read as set until you look
        parts = [f"{counts[f]} without a {label}"
                 + (f" (of those, {general} still on the General placeholder)" if f == "division" and general else "")
                 for f, label in (("title", "title"), ("division", "division"), ("document_type", "document type"))
                 if counts[f]]
        dup = counts.get("duplicate", 0)
        blank, fix = bool(parts), []
        if dup:
            parts.append(f"{dup} sharing a title and document type with another page")
        if blank:
            fix.append("accept the AI suggestions or set the values by hand first")
        if dup:
            fix.append("Regenerate duplicate titles gives the shared ones a title of their own"
                       if blank else "run Regenerate duplicate titles, or give them a title of their own by hand")
        n = counts["urls"]
        super().__init__(f"{n} delta URL{'s' if n != 1 else ''} cannot be promoted yet"
                         f" ({', '.join(parts)}): {'; '.join(fix)}")


class CurationService:
    def __init__(self, db: Database, lock_for=None):
        self.db = db
        self._lock_for = lock_for or (lambda cid: asyncio.Lock())

    async def recompute(self, c: Collection, *, review_all: bool = False) -> DeltaSet:
        """diff + apply patterns in one idempotent pass; persists deltas and pattern effects.
        Serialised per collection so two recomputes (or a recompute and a promote) never
        interleave their delete+insert on delta_urls.
        `review_all`: queue every included page for review again, changed or not (see engine.diff)."""
        async with self._lock_for(c.collection_id):
            return await self._recompute(c, review_all=review_all)

    async def _recompute(self, c: Collection, *, review_all: bool = False) -> DeltaSet:
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
            review_all=review_all,
            keep_queued=c.review_round,
        )
        await self.db.replace_deltas(c.collection_id, ds.deltas, ds.effects, excluded_count=ds.excluded)
        if ds.curated_edited_by:
            await self.db.set_curated_edited_by(c.collection_id, ds.curated_edited_by)
        if ds.curated_crawl_failure:
            await self.db.set_curated_crawl_failure(c.collection_id, ds.curated_crawl_failure)
        if ds.curated_excluded:
            await self.db.set_curated_excluded(c.collection_id, ds.curated_excluded)
        return ds

    async def _recompute_keys(self, c: Collection, keys: list[str], *,
                              excluded_before: int | None = None) -> DeltaSet | None:
        """The recompute of a per-URL change, limited to the pages it touches (`keys`: their canonical
        keys). A per-URL rule matches only the dump and curated URLs of its own page, and everything
        the engine decides for a URL depends only on that URL's rows and the rules that match it, so
        running the same pure `recompute` over those rows gives exactly the rows and effects a full
        recompute would, and leaves every other row as it is (tests/test_scoped_recompute.py checks
        that a full recompute after this changes nothing). Milliseconds instead of seconds on 100k URLs.
        `excluded_before`: how many of these pages' dump URLs the rules kept out before the change —
        given by a caller that deletes exclude / include rules first (their effects go with them).
        None when the collection cannot be done this way (rows without a stored key, or an unknown
        excluded count): the caller then runs the full recompute. The caller holds the lock."""
        cid = c.collection_id
        fresh = await self.db.get_collection(cid)
        if fresh is None or fresh.excluded_count is None or not await self.db.keyed(cid):
            return None
        keys = sorted(set(keys))
        dump = await self.db.load_dump(cid, keys=keys)
        curated = await self.db.load_curated(cid, keys=keys)
        rules = await self.db.load_rules(cid, keys=keys)
        urls = sorted({d.url for d in dump} | {x.url for x in curated})
        previous = await self.db.load_deltas(cid, urls=urls)
        wanted = set(keys)
        failures = {u: r for u, r in (await self.db.load_dump_failures(cid)).items() if canonical_key(u) in wanted}
        if excluded_before is None:
            excluded_before = await self.db.excluded_among(cid, [d.url for d in dump])
        ds = await asyncio.to_thread(
            recompute,
            collection_id=cid, collection_name=c.name, dump=dump, curated=curated, patterns=rules,
            previous=previous, failures=failures, capped=fresh.last_crawl_capped,
            division=fresh.division if division_assigned(fresh.division) else None,
            keep_queued=fresh.review_round,
        )
        await self.db.replace_deltas_scoped(cid, urls, ds.deltas, ds.effects,
                                            excluded_change=ds.excluded - excluded_before)
        if ds.curated_edited_by:
            await self.db.set_curated_edited_by(cid, ds.curated_edited_by)
        if ds.curated_crawl_failure:
            await self.db.set_curated_crawl_failure(cid, ds.curated_crawl_failure)
        if ds.curated_excluded:
            await self.db.set_curated_excluded(cid, ds.curated_excluded)
        after = await self.db.get_collection(cid)
        ds.whole = {**(await self.db.count_deltas_by_kind(cid)),
                    "excluded": after.excluded_count if after and after.excluded_count is not None else 0,
                    "kept": await self.db.count_curated_unreachable(cid)}
        return ds

    async def recompute_page(self, c: Collection, url: str) -> DeltaSet:
        """Recompute after a change that can only affect this URL's page (a no-op per-URL edit)."""
        async with self._lock_for(c.collection_id):
            return await self._recompute_keys(c, [canonical_key(url)]) or await self._recompute(c)

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
            keys = {canonical_key(b.match) for b in bodies}
            if len(keys) == 1 and all(is_exact(b.match) for b in bodies):  # one row's ✓ (all its fields)
                ds = await self._recompute_keys(c, list(keys))
                if ds is not None:
                    return ds
            return await self._recompute(c)

    async def _delete_other_spellings(self, c: Collection, by_type: dict[str, list[str]]) -> None:
        """Exact rules of the same type for another spelling of the same page would still match
        (and the newest would win): remove them so one page has one per-URL rule per field."""
        wanted = {(t, canonical_key(m)) for t, ms in by_type.items() for m in ms}
        if await self.db.keyed(c.collection_id):  # by the stored key: no need to load every exact rule
            ids: list[int] = []
            for t, ms in by_type.items():
                ids += await self.db.exact_rule_ids(c.collection_id, [t], [canonical_key(m) for m in ms])
            await self.db.delete_patterns(c.collection_id, ids)
            return
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
            if is_exact(body.match):
                ds = await self._recompute_keys(c, [canonical_key(body.match)])
                if ds is not None:
                    return ds
            return await self._recompute(c)

    async def set_excluded(self, c: Collection, url: str, excluded: bool, *, actor: str | None = None) -> DeltaSet:
        """The ✗ exclude / ✓ include toggle on a row: make the URL excluded (or included) and stay
        that way however often it is clicked. Per-URL rules saying the opposite go (a stale exact
        include that kept it in, or exact exclude that kept it out); a per-URL rule is added only
        when the glob rules alone would not give the wanted state, so a plain "include" on a row no
        exclude touches leaves no rule behind."""
        async with self._lock_for(c.collection_id):
            key = canonical_key(url)
            # kept out before the change: counted now, as the rules deleted below take their effects along
            before = await self.db.excluded_among(
                c.collection_id, [d.url for d in await self.db.load_dump(c.collection_id, keys=[key])])
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
            ds = await self._recompute_keys(c, [key], excluded_before=before)
            return ds if ds is not None else await self._recompute(c)

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
            return await self._with_stats(c, await self.db.list_patterns(c.collection_id), every=True)
        patterns = (await self.db.list_patterns(c.collection_id, exact=False)
                    + await self.db.list_patterns(c.collection_id, exact=True, limit=exact_limit, offset=exact_offset))
        return await self._with_stats(c, patterns)

    async def rules_page(self, c: Collection, **filters: Any) -> tuple[list[dict], int]:
        """One filtered, sorted page of the Rules table (Database.rules_page) with pattern_stats'
        counts for the rules on it, and how many rules pass the filters."""
        patterns, total = await self.db.rules_page(c.collection_id, **filters)
        return await self._with_stats(c, patterns), total

    async def _with_stats(self, c: Collection, patterns: list[Pattern], *, every: bool = False) -> list[dict]:
        """pattern_stats' counts for these rules (`every`: they are all the collection's rules)."""
        set_ = self.rows_set(c)
        counts = await self._match_counts(c, patterns, set_)
        # exclude rules keep URLs out of the delta URLs altogether, so theirs are counted over the dump
        excludes = [p for p in patterns if p.type is PatternType.EXCLUDE]
        if excludes and set_ != "dump":
            counts.update(await self._match_counts(c, excludes, "dump"))
        effects = await self.db.effect_counts(
            c.collection_id, None if every else [p.id for p in patterns if p.id is not None])
        return [{**p.model_dump(mode="json"), "matches": counts.get(p.id, 0), "in_effect": effects.get(p.id, 0),
                 "set": "dump" if p.type is PatternType.EXCLUDE else set_}
                for p in patterns]

    async def _match_counts(self, c: Collection, patterns: list[Pattern], set_: str) -> dict[int, int]:
        """{rule id: how many URLs of `set_` it matches}, as engine.patterns.match_counts counts them. In
        SQL when every row has its canonical key (V18): a glob is one LIKE (glob_to_like selects the same
        URLs as glob_to_regex), an exact-URL rule the rows of its page by the key index. Otherwise the
        set's URLs are loaded and matched in Python, as before."""
        if await self.db.keyed(c.collection_id):
            return await self.db.rule_match_counts(
                c.collection_id, set_,
                globs=[(p.id, glob_to_like(p.match)) for p in patterns if p.id is not None and not is_exact(p.match)],
                exact=[(p.id, canonical_key(p.match)) for p in patterns if p.id is not None and is_exact(p.match)],
            )
        return await asyncio.to_thread(match_counts, patterns, await self.db.set_urls(c.collection_id, set_))

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
        await self.db.set_review_round(c.collection_id, False)
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
            # pure CPU over the whole curated set: in a thread, as _promote does, so the event loop
            # keeps serving everyone else meanwhile
            curated = await asyncio.to_thread(
                promote, await self.db.load_curated(c.collection_id), picked,
                content_hashes={u: h for u, h in hashes.items() if u in wanted},
            )
            picked_urls = [d.url for d in picked]
            n = await self.db.replace_curated(c.collection_id, curated)
            await self.db.delete_deltas(c.collection_id, picked_urls)
            await self.db.delete_effects(c.collection_id, [d.url for d in picked if d.kind is DeltaKind.DELETED])
            if not left:  # the whole queue is through: the re-curation flag comes down, as in _promote
                await self.db.set_flag(c.collection_id, False)
                await self.db.set_review_round(c.collection_id, False)
            return n, DeltaSet(left)
