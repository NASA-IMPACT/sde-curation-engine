"""Event-loop lag probe.

The engine is one asyncio process. Anything that runs on the event loop without yielding — building
100,000 row objects, writing a COPY row by row — freezes every request, every SSE stream and the
health check for that long. The probe makes the freeze a number instead of an inference: a task
sleeps `interval_s` in a loop and records how much later than asked it woke up.

`last_ms` is the most recent lag; `max_ms` the worst since the last `read()` (which resets it), so a
monitor polling /health/db sees the worst freeze between two polls.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

log = logging.getLogger(__name__)


class LoopLagProbe:
    def __init__(self, interval_s: float = 0.1, warn_ms: float = 250.0):
        self.interval_s = interval_s
        self.warn_ms = warn_ms
        self.last_ms = 0.0
        self.max_ms = 0.0
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="loop-lag-probe")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def read(self) -> dict[str, float]:
        """{"last": ms, "max": ms since the previous read}; resets the max."""
        out = {"last": round(self.last_ms, 1), "max": round(self.max_ms, 1)}
        self.max_ms = self.last_ms
        return out

    def record(self, lag_ms: float) -> None:
        self.last_ms = lag_ms
        self.max_ms = max(self.max_ms, lag_ms)
        if lag_ms > self.warn_ms:
            log.warning("event loop blocked for %.0f ms", lag_ms)

    async def _run(self) -> None:
        while True:
            t0 = time.perf_counter()
            await asyncio.sleep(self.interval_s)
            self.record(max((time.perf_counter() - t0 - self.interval_s) * 1000.0, 0.0))
