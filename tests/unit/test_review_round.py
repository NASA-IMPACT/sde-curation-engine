"""2026-09-10 review round: Accept / Edit / Reject, AI-vs-SME provenance, delta-scoped exclusion
suggestions, per-URL edits on every table, re-curation reason, removal warning, rule scope."""


async def delta(c, url, cid="ex.org"):
    return next(d for d in (await c.get(f"/api/collections/{cid}/deltas?limit=100")).json()["items"] if d["url"] == url)


# ── 1. accept / edit / reject ──────────────────────────────────────────


    # blank glob → 422, nothing decided
    # edited glob → rule with the edited match, tagged llm_edited; suggestion records what was applied
    # a second suggestion edited to an existing rule → 409, still pending




    # same value as suggested → llm; a different value → llm_edited; both clear the badge
    # an invalid edited enum value → 422 and the badge stays


# ── 2. edited-by column and rule sources ──────────────────────────────


    # AI on every row, every field (promote refuses a row without a title, division or document type)
    # the excluded p3 is decided by its rule: not a delta, shown excluded under Dump URLs
    # filter + csv on deltas
    # rules table: source column + counts
    # promote: edited_by lands on the curated rows and the tooltips (effects) survive
    # rows promoted without a value (or re-attributed rules) are fixed up by the next recompute, no delta needed
    # a re-crawl that drops p4: the tombstone keeps its edited_by


# ── 3. suggest exclusions only for delta URLs ────────────────────────



    # nothing pending → the button is disabled with a reason and the API refuses
    # re-crawl with two new URLs; excluded pending rows are not candidates either


# ── 4 + 5. removal warning; per-URL edits on curated and crawl tables ───


    # curated table: toggle exclude on a promoted row → the rule applies in place (no delta); the live
    # collection drops back to curated so it gets re-indexed
    # crawl table: the toggle is there, reflects the excluded state, and works both ways
    # promote, then a crawl that lost most of the set → warning banner
    # below the ratio (1 of 8) → no banner


# ── 6. re-curation reason ──────────────────────────────────────────────





    # a recompute or promote with nothing pending does not clear it: the index, not the curation, is what failed


# ── 6b. curating a promoted collection all over again ──────────────────



    # the honest no-op: nothing differs from the dump, so nothing is queued and the status holds
    # the step-4 panel offers the same pair, and its ?all=true keeps the ?then= redirect intact

    # re-curate: the whole collection is back under review, at the first stage
    # the rows carry the values they were promoted with, and the AI passes see all of them again

    # promoting the queue back as it stands is the way out: the curated set is where it was


# ── 7. rules never cross collections ───────────────────────────────────


    # a suggestion accepted in A, a hand rule in A, a per-URL edit in A
    # a glob that would match B's URLs still does nothing there
    # deleting A (in the database: the app has no delete) cascades only A's rules


# ── 8. workers default ─────────────────────────────────────────────────


def test_llm_workers_default_is_16():
    from sde_curation.config import Settings

    assert Settings.model_fields["llm_workers"].default == 16
