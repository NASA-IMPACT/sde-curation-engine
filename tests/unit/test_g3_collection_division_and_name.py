"""The collection's division and name, as the recompute applies them (CurationService on the
in-memory FakeDatabase). The routes (api_set_division, api_set_name) store the new value and run
this recompute with the collection as it now reads; these tests hand the recompute that collection.
Replaces the curation checks of the old tests/integration/test_collection_division.py and
test_collection_rename.py (P4, TEST-STRATEGY-2026-10-09.md)."""

from sde_curation.models import DeltaKind, Division, EditedBy, PatternType
from tests.unit.test_curation import URLS
from tests.unit.test_curation_service import curated, fresh, queued, rule, started

NEW_NAME = "Example Site"
COLLECTION_TITLE = "{title} | {collection}"  # a title rule that renders the collection's name
P2 = URLS[1]


async def test_an_assigned_collection_division_is_every_pages_division_and_the_smes():
    """A division given at creation is the curator's decision for every URL: nothing is left for
    the AI to suggest and the row counts as edited by the SME. No other rule is in force (no
    document-type glob), so the division is the only thing that can make the row the SME's."""
    db, _ = await started(division=Division.HELIOPHYSICS, doc_rule=False)

    rows = await db.load_deltas((await fresh(db)).collection_id)

    assert {(d.division, d.edited_by) for d in rows} == {(Division.HELIOPHYSICS, EditedBy.SME)}


async def test_the_general_placeholder_leaves_every_pages_division_blank_for_the_ai():
    db, _ = await started(division=Division.GENERAL)

    rows = await db.load_deltas((await fresh(db)).collection_id)

    assert {d.division for d in rows} == {None}


async def test_a_new_division_on_a_curated_collection_queues_every_page_with_it():
    """Every curated row is behind the new division: one modified delta each, ready to promote."""
    db, service = await curated(division=Division.HELIOPHYSICS)
    moved = (await fresh(db)).model_copy(update={"division": Division.EARTH_SCIENCE})

    await service.recompute(moved)

    assert await queued(db) == dict.fromkeys(URLS, DeltaKind.MODIFIED)
    assert {d.division for d in await db.load_deltas(moved.collection_id)} == {Division.EARTH_SCIENCE}


async def test_a_division_rule_still_decides_its_page_over_a_new_collection_division():
    db, service = await started(division=Division.HELIOPHYSICS)
    await rule(db, PatternType.DIVISION, P2, Division.PLANETARY)
    moved = (await fresh(db)).model_copy(update={"division": Division.EARTH_SCIENCE})

    await service.recompute(moved)

    divisions = {d.url: d.division for d in await db.load_deltas(moved.collection_id)}
    assert divisions[P2] == Division.PLANETARY
    assert set(divisions.values()) - {Division.PLANETARY} == {Division.EARTH_SCIENCE}


async def test_going_back_to_general_takes_the_collection_division_off_every_page():
    db, service = await started(division=Division.HELIOPHYSICS)
    general = (await fresh(db)).model_copy(update={"division": Division.GENERAL})

    await service.recompute(general)

    assert {d.division for d in await db.load_deltas(general.collection_id)} == {None}


async def test_a_rename_reaches_every_curated_title_that_renders_the_collection_name():
    db, service = await curated()
    await rule(db, PatternType.TITLE, "*", COLLECTION_TITLE)
    await service.recompute(await fresh(db))
    await service.promote(await fresh(db))
    renamed = (await fresh(db)).model_copy(update={"name": NEW_NAME})

    await service.recompute(renamed)

    rows = await db.load_deltas(renamed.collection_id)
    assert {d.url: d.kind for d in rows} == dict.fromkeys(URLS, DeltaKind.MODIFIED)
    assert {d.title.rsplit(" | ", 1)[1] for d in rows} == {NEW_NAME}


async def test_a_rename_without_a_title_rule_that_uses_the_name_queues_nothing():
    db, service = await curated()
    renamed = (await fresh(db)).model_copy(update={"name": NEW_NAME})

    await service.recompute(renamed)

    assert await queued(db) == {}
