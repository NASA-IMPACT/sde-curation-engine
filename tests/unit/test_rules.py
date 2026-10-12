"""The collection status rules (sde_curation.rules): what an action does to a collection's status,
and when it is refused. Plain model objects in, a decision out — no database."""

from types import SimpleNamespace

import pytest

from sde_curation import rules
from sde_curation.models import Collection, ConnectorType, CurationStage, Division, IndexRun, Status
from sde_curation.rules import StatusMove


def col(status: Status = Status.BACKLOG, **kw) -> Collection:
    return Collection(collection_id="c1", name="My Docs", seed_url="https://example.org/",
                      connector=ConnectorType.CRAWLER, max_pages=100, status=status, **kw)


def run(state: str = "succeeded", validation: dict | None = None, target: str = "test") -> IndexRun:
    return IndexRun(run_id="r1", collection_id="c1", target=target, state=state, validation=validation)


PASSING = {"count_matches": True, "title_match_rate": 1.0}


# ── status after a curation change ─────────────────────────────────────


@pytest.mark.parametrize("status", [Status.BACKLOG, Status.SCRAPED])
def test_recrawl_identical_to_curated_set_is_curated_and_clears_the_flag(status):
    move = rules.status_after_curation_change(col(status, curated_rows=5), 0, 0, note="ignored")
    assert move == StatusMove(Status.CURATED, "re-crawl matches the curated URLs: no changes",
                              force=True, clear_flag=True)


@pytest.mark.parametrize("status", [Status.BACKLOG, Status.SCRAPED])
def test_first_recompute_with_nothing_curated_and_no_deltas_leaves_the_status(status):
    assert rules.status_after_curation_change(col(status), 0, 0) is None


@pytest.mark.parametrize("status", [Status.BACKLOG, Status.SCRAPED,
                                    Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE])
def test_deltas_put_the_collection_in_curating(status):
    move = rules.status_after_curation_change(col(status, curated_rows=3), 7, 0)
    assert move == StatusMove(Status.CURATING, "delta URLs recomputed: 7")
    assert not move.clear_flag and move.force


def test_deltas_note_is_the_callers_when_given():
    move = rules.status_after_curation_change(col(Status.LIVE), 2, 0, note="promoted 3 selected delta URLs")
    assert move == StatusMove(Status.CURATING, "promoted 3 selected delta URLs")


def test_deltas_win_over_curated_urls_excluded_in_place():
    move = rules.status_after_curation_change(col(Status.LIVE, curated_rows=3), 1, 4)
    assert move.status is Status.CURATING


@pytest.mark.parametrize("status", [Status.CONFIG_GENERATED, Status.LIVE])
@pytest.mark.parametrize(("k", "note"), [(1, "1 curated URL excluded by rules: re-index to apply"),
                                         (3, "3 curated URLs excluded by rules: re-index to apply")])
def test_curated_urls_excluded_in_place_send_an_indexed_collection_back_to_curated(status, k, note):
    move = rules.status_after_curation_change(col(status, curated_rows=5), 0, k, note="ignored")
    assert move == StatusMove(Status.CURATED, note)


def test_curated_urls_excluded_in_place_on_a_curated_collection_change_nothing():
    assert rules.status_after_curation_change(col(Status.CURATED, curated_rows=5), 0, 2) is None


@pytest.mark.parametrize("status", [Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE])
def test_a_recompute_with_nothing_to_review_never_demotes(status):
    assert rules.status_after_curation_change(col(status, curated_rows=5), 0, 0) is None


def test_curating_with_nothing_left_on_a_promoted_set_is_curated():
    c = col(Status.CURATING, curated_rows=5)
    assert rules.status_after_curation_change(c, 0, 0) == StatusMove(Status.CURATED, "recomputed: no delta URLs")
    assert rules.status_after_curation_change(c, 0, 1, note="promoted 2 selected delta URLs") == \
        StatusMove(Status.CURATED, "promoted 2 selected delta URLs")


def test_curating_stays_curating_with_deltas_or_nothing_promoted():
    assert rules.status_after_curation_change(col(Status.CURATING, curated_rows=5), 4, 0) is None
    assert rules.status_after_curation_change(col(Status.CURATING), 0, 0) is None


def test_recompute_note_and_review_round():
    assert rules.recompute_note(True, 12) == "re-curating: 12 delta URLs queued for review"
    assert rules.recompute_note(False, 12) is None
    assert rules.restarts_review_round(True, 12) is True
    assert rules.restarts_review_round(True, 0) is False
    assert rules.restarts_review_round(False, 12) is False


# ── manual status change ───────────────────────────────────────────────


@pytest.mark.parametrize("new", [Status.SCRAPED, Status.CURATING])
def test_status_needing_a_dump_is_refused_without_one(new):
    assert rules.status_invariant_problem(col(), new) == f"cannot be '{new}': no crawl dump yet — scrape first"
    assert rules.status_invariant_problem(col(dump_count=1), new) is None


@pytest.mark.parametrize("new", [Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE])
def test_promoted_status_needs_a_curated_set_and_no_waiting_deltas(new):
    assert rules.status_invariant_problem(col(dump_count=1), new) == \
        f"cannot be '{new}': nothing has been promoted to the curated set"
    assert rules.status_invariant_problem(col(Status.CURATING, curated_rows=2, delta_count=3), new) == \
        f"cannot be '{new}': 3 delta URLs are waiting — promote (or discard) them first"
    assert rules.status_invariant_problem(col(new, curated_rows=2, delta_count=3), new) is None  # staying put
    assert rules.status_invariant_problem(col(Status.CURATING, curated_rows=2), new) is None


def test_backlog_is_always_allowed():
    assert rules.status_invariant_problem(col(Status.LIVE), Status.BACKLOG) is None


def test_web_app_keeps_its_name_for_the_invariant():
    from sde_curation.web import app
    assert app.status_invariant_problem is rules.status_invariant_problem


# ── refusals ───────────────────────────────────────────────────────────


def test_busy_refusal():
    assert rules.busy_refusal(None) is None
    job = SimpleNamespace(kind="recompute", id=42)
    assert rules.busy_refusal(job) == "recompute job #42 is running — wait for it or cancel it"


def test_recompute_needs_a_dump():
    assert rules.recompute_refusal(col()) == "no dump ingested yet — scrape first"
    assert rules.recompute_refusal(col(dump_count=1)) is None


def test_stages_only_while_curating():
    assert rules.stage_refusal(col(Status.CURATED)) == "stages only apply while curating (status is curated)"
    assert rules.stage_refusal(col(Status.CURATING)) is None


def test_metadata_stage_waits_for_pattern_suggestions():
    assert rules.metadata_stage_refusal(0) is None
    assert rules.metadata_stage_refusal(1) == "1 pattern suggestion is pending — accept or reject them first"
    assert rules.metadata_stage_refusal(2) == "2 pattern suggestions are pending — accept or reject them first"


def test_promote_only_while_curating():
    assert rules.promote_refusal(col(Status.LIVE)) == "promote requires status 'curating' (is live)"
    assert rules.promote_refusal(col(Status.CURATING)) is None


def test_promote_selection_must_still_be_delta_urls():
    assert rules.stale_selection_refusal(["a", "b"], {"a", "b"}) is None
    assert rules.stale_selection_refusal(["a", "b", "c"], {"b"}) == \
        "2 of the selected URLs are no longer delta URLs (e.g. a) — reload the page and pick again"


@pytest.mark.parametrize("status", [Status.BACKLOG, Status.SCRAPED, Status.CURATING])
def test_index_needs_a_promoted_set(status):
    assert rules.index_refusal(col(status)) == \
        f"indexing requires a promoted (curated) set — status is '{status}'"


@pytest.mark.parametrize("status", [Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE])
def test_index_needs_no_waiting_deltas(status):
    assert rules.index_refusal(col(status, delta_count=4)) == "4 delta URLs are waiting — promote them first"
    assert rules.index_refusal(col(status)) is None


def test_index_needs_something_to_export():
    assert rules.export_refusal(0) == "nothing to export: every curated URL is excluded"
    assert rules.export_refusal(1) is None


@pytest.mark.parametrize("last", [None, run("failed", PASSING), run("running", PASSING), run("succeeded"),
                                  run("succeeded", {"count_matches": True, "title_match_rate": 0.5}),
                                  run("succeeded", {"count_matches": False, "title_match_rate": 1.0})])
def test_prod_needs_a_successful_validated_test_run(last):
    assert rules.prod_index_refusal(last, 0.99) == "prod indexing requires a successful, validated test run first"


def test_prod_threshold_is_the_callers():
    good_enough = run("succeeded", {"count_matches": True, "title_match_rate": 0.9})
    assert rules.prod_index_refusal(good_enough, 0.85) is None
    assert rules.prod_index_refusal(good_enough, 0.95) is not None
    assert rules.prod_index_refusal(run("succeeded", PASSING), 0.99) is None


def test_revalidate_needs_a_successful_run():
    assert rules.revalidate_refusal(None, "prod") == "no successful prod index run to validate"
    assert rules.revalidate_refusal(run("failed"), "test") == "no successful test index run to validate"
    assert rules.revalidate_refusal(run("succeeded"), "test") is None


def test_name_and_division_lock_at_the_first_index_run():
    assert rules.rename_refusal(col()) is None
    assert rules.division_refusal(col()) is None
    indexed = col(Status.LIVE, last_run_id="r1", division=Division.HELIOPHYSICS)
    assert rules.rename_refusal(indexed) == (
        "'My Docs' has been indexed (as 'my_docs'), so its name can no longer change:"
        " it has to match the collection key and name it was indexed with")
    assert rules.division_refusal(indexed) == (
        "'My Docs' has been indexed (as 'my_docs') with division 'Heliophysics',"
        " so its division can no longer change")


def test_rename_and_division_change_recompute_only_existing_urls():
    assert rules.has_urls_to_recompute(col()) is False
    assert rules.has_urls_to_recompute(col(delta_count=1)) is True
    assert rules.has_urls_to_recompute(col(curated_rows=1)) is True


def test_suggest_patterns_needs_a_dump_then_deltas():
    assert rules.suggest_patterns_refusal(col()) == "no crawl dump yet — scrape first"
    assert rules.suggest_patterns_refusal(col(dump_count=1)) == \
        "no delta URLs — Start curating first, then suggest exclusions for the delta URLs"
    assert rules.suggest_patterns_refusal(col(dump_count=1, delta_count=1)) is None


def test_suggest_metadata_refusals():
    assert rules.suggest_metadata_refusal(col()) == "no delta URLs — Start curating (recompute) first"
    assert rules.suggest_metadata_refusal(col(delta_count=1)) is None
    assert rules.suggest_metadata_pending_refusal(0) is None
    assert rules.suggest_metadata_pending_refusal(1) == \
        "1 pattern suggestion is pending — accept or reject them before suggesting metadata"
    assert rules.suggest_metadata_pending_refusal(3) == \
        "3 pattern suggestions are pending — accept or reject them before suggesting metadata"
    assert rules.nothing_to_classify_refusal(0) == \
        "nothing to classify: every included delta URL already has suggestions — use ?all=true to redo them"
    assert rules.nothing_to_classify_refusal(5) is None


@pytest.mark.parametrize(("status", "stage", "moves"), [
    (Status.CURATING, CurationStage.EXCLUSIONS, True),
    (Status.CURATING, None, True),
    (Status.CURATING, CurationStage.METADATA, False),
    (Status.CURATED, None, False),
])
def test_suggest_metadata_moves_a_curating_collection_to_the_metadata_stage(status, stage, moves):
    assert rules.moves_to_metadata_stage(col(status, curation_stage=stage)) is moves
