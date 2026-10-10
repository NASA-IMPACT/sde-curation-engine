"""Curation flow over the API: scrape (fake) → recompute → patterns → url edit → promote."""
    # exclude p* (all 8), force-include p2 → 7 excluded; excluded URLs leave the delta URLs (the rule decides them)
    # duplicate pattern → 409; bad value → 422
    # narrow the exclude to p1 only (delete + re-add) so the rest of the flow has one exclusion

    # title template + division for all
    # exclude rules count over the dump; the rest over the delta URLs (p1 is excluded, so not a delta)


    # per-URL edit = exact pattern, the newest rule for that URL → wins over the older "*"
    # the toggle is the wanted state: exclude twice stays excluded; include puts it back (and, with no
    # glob excluding p3, leaves no rule behind)

    # old curate URL redirects into the workbench (filters preserved); URLs tab renders the deltas

    # promote refuses rows without a title, division or document type: the rules above set no type

    # promote

    # recompute after promote with nothing changed must NOT demote to curating (dead end otherwise)
    # an exclude on a curated URL applies in place: no delta, still curated, the row is flagged
    # deleting it is the way back in: a modified delta that reopens curation
    # a pattern that changes something does reopen curation; deleting it (no deltas left) returns to curated
    # manual 'curating' with zero deltas: promote acts as "mark curated"

    # delete the division pattern → unapply: p2 keeps its exact pattern, others fall back to curated value




    # simulate a re-crawl that lost p7..p10 and retitled p1


def test_active_for_ends_when_job_state_is_final():
    """Regression: a job whose final state is recorded must not count as running just because its
    asyncio task has not exited yet (the window in which CI saw 409s after wait_job returned)."""
    from types import SimpleNamespace

    from sde_curation.jobs import JobManager
    from sde_curation.models import JobKind, JobRun, JobState

    jm = JobManager.__new__(JobManager)
    jm._tasks = {}
    jm._pending_resume = {}  # jobs waiting to resume after a restart (none here)
    job = JobRun(collection_id="ex.org", kind=JobKind.SCRAPE, state=JobState.RUNNING)
    jm._tasks[1] = SimpleNamespace(job=job, done=lambda: False)  # task still alive
    assert jm.active_for("ex.org") is job
    job.state = JobState.SUCCEEDED  # what finish_job does, before the task unwinds
    assert jm.active_for("ex.org") is None
