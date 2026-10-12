"""Many curators on many collections at once: what keeps a busy database from taking the engine down.

Test, 2026-10-06: after an exclude accept on a 100k collection, the stepper and the tabs of every
open browser re-ran a 7 s count on every job event; 16 copies of it held every pooled connection,
/health (which needed one) timed out, and the ALB replaced a healthy engine. The fixes under test:
the count is stored by the recompute; page requests read on their own pool under a statement timeout
(503 instead of a pile-up) while actions and jobs keep theirs without one; identical page reads run
once at a time and are shared; /health is liveness only and /health/db reports the database.
"""
import asyncio

import pytest

from sde_curation.db import SingleFlight, db_scope, work_context

# ── the excluded count is stored, not counted per view ─────────────────







    # the overview shows the stored number

    # a re-crawl drops the rule effects with the old delta URLs; the recompute brings them back







# ── page requests vs. work: separate pools, a timeout only on pages ────


async def test_a_job_started_from_a_page_request_works_outside_its_timeout():
    """Whatever starts a background task from a request hands it the work scope."""
    token = db_scope.set("read")
    try:
        assert work_context().run(db_scope.get) == "work"
        assert db_scope.get() == "read"
    finally:
        db_scope.reset(token)
    # work pool of 2: an ingest holds one for its COPY and borrows a second to record the job
        # every work connection taken (jobs holding them): pages still load
        # every read connection taken (pages piling up): an action still goes through, /health
        # stays up, and /health/db says the database side is busy




# ── SingleFlight ───────────────────────────────────────────────────────


async def test_one_run_shared_by_everyone_who_asks_while_it_runs():
    sf, gen, runs = SingleFlight(), {"g": 0}, []

    async def count():
        runs.append(1)
        await asyncio.sleep(0.05)
        return len(runs)

    got = await asyncio.gather(*(sf.run("k", lambda: gen["g"], count) for _ in range(10)))
    assert got == [1] * 10 and len(runs) == 1 and sf.in_flight == 0
    assert await sf.run("k", lambda: gen["g"], count) == 2  # done: the next ask runs again


async def test_after_a_change_nobody_is_handed_the_old_result():
    """A change while a run is in progress: those who asked before it share the old run; those who
    ask after it get a run that started after the change — one, shared, not one each."""
    sf, gen, started = SingleFlight(), {"g": 0}, []

    async def count():
        g = gen["g"]  # what the run sees: the tables as they were when it began
        started.append(g)
        await asyncio.sleep(0.05)
        return g

    before = [asyncio.ensure_future(sf.run("k", lambda: gen["g"], count)) for _ in range(3)]
    await asyncio.sleep(0.01)
    gen["g"] += 1  # a curator's edit lands mid-run
    after = [asyncio.ensure_future(sf.run("k", lambda: gen["g"], count)) for _ in range(3)]
    assert await asyncio.gather(*before) == [0, 0, 0]
    assert await asyncio.gather(*after) == [1, 1, 1]
    assert started == [0, 1]  # two runs in all, never two at the same time


async def test_a_caller_that_gives_up_does_not_cancel_the_run_for_the_others():
    sf = SingleFlight()

    async def count():
        await asyncio.sleep(0.05)
        return 42

    quitter = asyncio.ensure_future(sf.run("k", lambda: 0, count))
    stayer = asyncio.ensure_future(sf.run("k", lambda: 0, count))
    await asyncio.sleep(0.01)
    quitter.cancel()
    assert await stayer == 42


async def test_a_failed_run_fails_everyone_waiting_on_it_and_the_next_ask_runs_again():
    sf, n = SingleFlight(), {"runs": 0}

    async def boom():
        n["runs"] += 1
        await asyncio.sleep(0.01)
        raise RuntimeError("statement timeout")

    got = await asyncio.gather(*(sf.run("k", lambda: 0, boom) for _ in range(3)), return_exceptions=True)
    assert all(isinstance(e, RuntimeError) for e in got) and n["runs"] == 1
    with pytest.raises(RuntimeError):
        await sf.run("k", lambda: 0, boom)
    assert n["runs"] == 2
