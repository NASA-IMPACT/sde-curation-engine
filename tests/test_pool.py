"""llm/pool.py: bounded concurrency, per-item failures, abort and cancel semantics."""

import asyncio

import pytest

from sde_curation.llm.base import LLMError, LLMRetryable
from sde_curation.llm.pool import PoolAborted, run_pool


class Sink:
    """Collects what the pool reports."""

    def __init__(self):
        self.got, self.errs, self.progress = [], [], []

    async def on_result(self, i, r):
        self.got.append((i, r))

    async def on_error(self, i, e):
        self.errs.append((i, str(e)))

    async def on_progress(self, p):
        self.progress.append(dict(p))


async def test_bounded_concurrency_and_counts():
    k = Sink()
    inflight, peak = 0, 0

    async def fn(i):
        nonlocal inflight, peak
        inflight += 1; peak = max(peak, inflight)
        await asyncio.sleep(0.01)
        inflight -= 1
        return i * 2

    stats = await run_pool(range(20), fn, workers=4, on_result=k.on_result, on_error=k.on_error,
                           on_progress=k.on_progress, total=20, progress_interval_s=0)
    assert peak <= 4 and stats.done == 20 and stats.failed == 0
    assert sorted(k.got) == [(i, i * 2) for i in range(20)]
    assert k.progress[-1]["done"] == 20 and k.progress[-1]["total"] == 20 and k.progress[-1]["inflight"] == 0


async def test_item_failures_are_counted_not_fatal():
    k = Sink()

    async def fn(i):
        if i % 3 == 0:
            raise LLMRetryable(f"429 on {i}")
        return i

    async def gen():
        for i in range(9):
            yield i

    stats = await run_pool(gen(), fn, workers=3, on_result=k.on_result, on_error=k.on_error, on_progress=k.on_progress)
    assert stats.done == 6 and stats.failed == 3 and "429" in stats.last_error
    assert {i for i, _ in k.errs} == {0, 3, 6} and len(k.got) == 6


async def test_all_failed_raises_and_consecutive_nonretryable_aborts():
    k = Sink()

    async def bad(i):
        raise LLMError("schema mismatch")

    with pytest.raises(PoolAborted, match="consecutive"):
        await run_pool(range(50), bad, workers=2, on_result=k.on_result, on_error=k.on_error,
                       abort_after_consecutive_failures=5)
    assert 5 <= len(k.errs) < 50

    async def flaky(i):
        raise LLMRetryable("busy")

    with pytest.raises(LLMError, match="all 4 calls failed"):
        await run_pool(range(4), flaky, workers=2, on_result=k.on_result, abort_after_consecutive_failures=2)


async def test_cancel_stops_workers_and_keeps_finished_results():
    k = Sink()

    async def slow(i):
        await asyncio.sleep(0.05)
        return i

    task = asyncio.create_task(run_pool(range(100), slow, workers=2, on_result=k.on_result))
    await asyncio.sleep(0.18)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert 2 <= len(k.got) < 100
    assert not [t for t in asyncio.all_tasks() if t.get_name().startswith("llm-pool")]


async def test_retryable_failures_get_a_quieter_retry_pass():
    k = Sink()
    attempts: dict[int, int] = {}
    peak_retry, inflight = 0, 0

    async def fn(i):
        nonlocal peak_retry, inflight
        attempts[i] = attempts.get(i, 0) + 1
        if attempts[i] > 1:  # the retry pass
            inflight += 1; peak_retry = max(peak_retry, inflight)
            await asyncio.sleep(0.01)
            inflight -= 1
        if i % 4 == 0 and attempts[i] == 1:
            raise LLMRetryable(f"429 on {i}")  # recovers on the retry
        if i == 5:
            raise LLMRetryable("503 forever")
        return i

    stats = await run_pool(range(12), fn, workers=8, on_result=k.on_result, on_error=k.on_error,
                           on_progress=k.on_progress, progress_interval_s=0, retry_passes=1)
    assert stats.done == 11 and stats.failed == 1 and stats.retrying == 0
    assert [i for i, _ in k.errs] == [5]  # on_error only after the last attempt
    assert attempts == {i: (2 if i % 4 == 0 or i == 5 else 1) for i in range(12)}
    assert peak_retry <= 2  # a quarter of the workers
    assert any(p["retrying"] == 4 for p in k.progress) and k.progress[-1]["retrying"] == 0


async def test_everything_failing_after_retries_still_raises():
    k = Sink()

    async def flaky(i):
        raise LLMRetryable("busy")

    with pytest.raises(LLMError, match="all 3 calls failed"):
        await run_pool(range(3), flaky, workers=2, on_result=k.on_result, on_error=k.on_error, retry_passes=2)
    assert sorted(i for i, _ in k.errs) == [0, 1, 2]


async def test_per_item_errors_after_a_success_do_not_abort_early():
    k = Sink()

    async def fn(i):
        if i == 0:
            return i
        await asyncio.sleep(0)
        raise LLMError("context_length_exceeded")

    stats = await run_pool(range(30), fn, workers=1, on_result=k.on_result, on_error=k.on_error,
                           abort_after_consecutive_failures=5)
    assert stats.done == 1 and stats.failed == 29  # 29 < 5 * 10: a run of bad pages, not a broken setup
