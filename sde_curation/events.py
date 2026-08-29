"""In-process event bus feeding the SSE endpoint. One queue per subscriber, bounded."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any


class EventBus:
    def __init__(self, maxsize: int = 256):
        self._subs: set[asyncio.Queue[dict[str, Any]]] = set()
        self._maxsize = maxsize

    def publish(self, event: str, data: dict[str, Any]) -> None:
        msg = {"event": event, "data": data}
        for q in list(self._subs):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                # A stalled browser must not block the pipeline; drop oldest.
                q.get_nowait()
                q.put_nowait(msg)

    async def subscribe(self) -> AsyncIterator[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._maxsize)
        self._subs.add(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._subs.discard(q)

    @property
    def subscriber_count(self) -> int:
        return len(self._subs)


def sse_format(msg: dict[str, Any]) -> dict[str, str]:
    """Shape for sse_starlette: event name + JSON payload."""
    return {"event": msg["event"], "data": json.dumps(msg["data"])}
