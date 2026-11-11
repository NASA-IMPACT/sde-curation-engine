"""The one-line "what is this job doing" text (templates/partials/job_progress.html, macro job_progress).

One macro renders it on every surface a curator watches a job from: the header chip, the stepper,
the dashboard row and the jobs strip (the page snapshots show it there). These tests pin the text
itself for the job states the snapshots do not hold: a crawl waiting in the crawler host's queue,
an LLM run with calls in flight, the ingest of a crawl into PostgreSQL, and an index run whose
validation is not final yet.
"""

import pytest

from sde_curation.models import JobKind, JobRun, JobState
from sde_curation.web.app import templates

CID = "ex.org"


def progress_text(kind: JobKind, progress: dict, state: JobState = JobState.RUNNING) -> str:
    macro = templates.env.get_template("partials/job_progress.html").module.job_progress
    return str(macro(JobRun(collection_id=CID, kind=kind, state=state, progress=progress)))


@pytest.mark.parametrize(("ahead", "tail"), [(1, "behind 1 crawl"), (2, "behind 2 crawls")])
def test_a_queued_crawl_says_how_many_crawls_are_ahead_of_it(ahead, tail):
    """A crawl waiting in the crawler host's inbox is not stuck: the curator sees its place in line."""
    text = progress_text(JobKind.SCRAPE, {"processed": 0, "docs": 0, "failed": 0, "queued": True,
                                           "queue_ahead": ahead})

    assert text.startswith("queued ") and text.endswith(tail)


def test_a_metadata_run_counts_urls_with_calls_in_flight_and_failures():
    text = progress_text(JobKind.LLM_METADATA, {"llm": "metadata", "done": 3, "total": 8, "inflight": 2, "failed": 1})

    assert text == "LLM calls in progress · 3/8 URLs · 2 in flight · 1 failed"


def test_a_patterns_run_counts_calls_not_urls():
    """Suggest patterns sends URL batches, so its counter is calls; nothing failed means no failure part."""
    text = progress_text(JobKind.LLM_PATTERNS, {"llm": "patterns", "done": 1, "total": 4, "inflight": 3})

    assert text == "LLM calls in progress · 1/4 calls · 3 in flight"


def test_a_crawl_being_stored_shows_pages_read_of_the_total_and_failures():
    text = progress_text(JobKind.SCRAPE, {"phase": "ingest", "ingested": 12500, "ingest_total": 31904, "failures": 7})

    assert text == "storing the crawl · 12,500 of 31,904 pages read · 7 failed"


def test_a_crawl_in_its_final_write_says_it_is_writing_to_the_database():
    text = progress_text(JobKind.SCRAPE, {"phase": "ingest_store", "ingested": 31904, "failures": 7})

    assert text == "storing the crawl · 31,904 pages read · writing them to the database · 7 failed"


@pytest.mark.parametrize(("kind", "progress", "expected"), [
    (JobKind.INDEX_TEST, {"phase": "done", "exported": 3}, "validating the test index"),
    (JobKind.VALIDATE_PROD, {"phase": "validating", "validation_attempt": 2, "indexed_so_far": 2,
                             "expected_count": 3}, "validating the prod index · 2/3 visible so far"),
    (JobKind.INDEX_TEST, {"phase": "done", "exported": 3, "validation_ok": False}, "validation failed"),
], ids=["indexer done, engine check pending", "prod re-check under way", "check finished: final"])
def test_an_index_job_says_validating_until_its_own_check_has_a_result(kind, progress, expected):
    """The indexer's own report is stored before the engine's post-refresh check ends; until that
    check has a result the job says "validating", never a pass or a fail."""
    assert progress_text(kind, progress) == expected


# ── an index run shows the batches the indexer has vectorized (no-progress stall, LPI 2026-11-04) ──

@pytest.mark.parametrize(("batches", "tail"), [
    (614, " · 614 batches vectorized"),
    (1, " · 1 batch vectorized"),
    (0, " · 0 batches vectorized"),     # dispatched, nothing vectorized yet
])
def test_an_index_run_shows_how_many_batches_it_has_vectorized(batches, tail):
    """A long run is moving when this number grows; it stalls after 4 h without a new batch."""
    text = progress_text(JobKind.INDEX_TEST, {"phase": "indexing", "exported": 30682, "waiting_s": 7260,
                                              "batches_written": batches})

    assert text == f"indexing 30682 docs · 121m{tail}"


@pytest.mark.parametrize("progress", [
    {"phase": "indexing", "exported": 707, "waiting_s": 120},                        # an engine before the count
    {"phase": "indexing", "exported": 707, "waiting_s": 120, "batches_written": None},  # S3 could not say yet
])
def test_an_index_run_without_a_batch_count_shows_no_batch_part(progress):
    assert progress_text(JobKind.INDEX_TEST, progress) == "indexing 707 docs · 2m"
