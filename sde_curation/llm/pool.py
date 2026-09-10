"""A bounded worker pool for one LLM job: N calls in flight, per-item failures counted rather
than fatal, progress throttled, cancellation clean. Pure asyncio, no provider knowledge."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from .base import LLMError, LLMRetryable

ProgressCb = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass
class PoolStats:
    total: int | None = None
    done: int = 0
    failed: int = 0
    inflight: int = 0
    last_error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def snapshot(self) -> dict[str, Any]:
        out: dict[str, Any] = {"done": self.done, "failed": self.failed, "inflight": self.inflight}
        if self.total is not None:
            out["total"] = self.total
        if self.last_error:
            out["last_error"] = self.last_error
        out.update(self.extra)
        return out


class PoolAborted(LLMError):
    pass


async def _aiter[I](items: Iterable[I] | AsyncIterator[I]) -> AsyncIterator[I]:
    if hasattr(items, "__aiter__"):
        async for x in items:  # type: ignore[union-attr]
            yield x
    else:
        for x in items:  # type: ignore[union-attr]
            yield x


class _Stop:
    pass


async def run_pool[I, R](
    items: Iterable[I] | AsyncIterator[I],
    fn: Callable[[I], Awaitable[R]],
    *,
    workers: int,
    on_result: Callable[[I, R], Awaitable[None]],
    on_error: Callable[[I, Exception], Awaitable[None]] | None = None,
    on_progress: ProgressCb | None = None,
    total: int | None = None,
    progress_interval_s: float = 1.0,
    abort_after_consecutive_failures: int = 10,
) -> PoolStats:
    """Run `fn` over `items` with at most `workers` in flight.

    - `LLMError` from `fn` marks the item failed and the pool continues; `LLMRetryable` does
      not count towards the consecutive-failure abort (a rate-limited run must keep going).
    - `abort_after_consecutive_failures` non-retryable errors in a row (bad key, missing model,
      schema mismatch on every call) raise `PoolAborted` — no point burning the rest.
    - Nothing succeeded and something failed → `LLMError`, so a job cannot "succeed" empty.
    - Cancellation cancels every worker, waits for them, then re-raises; whatever `on_result`
      already received stays with the caller.
    """
    stats = PoolStats(total=total)
    queue: asyncio.Queue = asyncio.Queue(maxsize=max(1, workers * 2))
    last_emit = 0.0
    consecutive = 0
    emit_lock = asyncio.Lock()

    async def emit(force: bool = False) -> None:
        nonlocal last_emit
        if on_progress is None:
            return
        now = time.monotonic()
        if force or now - last_emit >= progress_interval_s:
            async with emit_lock:
                last_emit = now
                await on_progress(stats.snapshot())

    abort: asyncio.Event = asyncio.Event()
    producer_error: BaseException | None = None

    async def producer() -> None:
        nonlocal producer_error
        try:
            async for item in _aiter(items):
                if abort.is_set():
                    break
                await queue.put(item)
        except Exception as e:  # noqa: BLE001 — a failing source must not leave workers waiting forever
            producer_error = e
        finally:
            for _ in range(workers):
                await queue.put(_Stop)

    async def worker() -> None:
        nonlocal consecutive
        while True:
            item = await queue.get()
            if item is _Stop:
                return
            if abort.is_set():
                continue  # drain so the producer can finish and every worker sees its _Stop
            stats.inflight += 1
            try:
                result = await fn(item)
            except LLMError as e:
                stats.inflight -= 1
                stats.failed += 1
                stats.last_error = str(e)[:300]
                if not isinstance(e, LLMRetryable):
                    consecutive += 1
                    if consecutive >= abort_after_consecutive_failures:
                        abort.set()
                if on_error is not None:
                    await on_error(item, e)
                await emit()
                continue
            stats.inflight -= 1
            stats.done += 1
            consecutive = 0
            await on_result(item, result)
            await emit()

    tasks = [asyncio.create_task(producer(), name="llm-pool-producer")]
    tasks += [asyncio.create_task(worker(), name=f"llm-pool-worker-{i}") for i in range(workers)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    finally:
        await emit(force=True)
    if producer_error is not None:
        raise producer_error
    if abort.is_set():
        raise PoolAborted(
            f"aborted after {abort_after_consecutive_failures} consecutive failures: {stats.last_error}"
        )
    if stats.done == 0 and stats.failed:
        raise LLMError(f"all {stats.failed} calls failed: {stats.last_error}")
    return stats
