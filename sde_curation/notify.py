"""Status-transition notifications: Slack-compatible webhook (NOTIFY_WEBHOOK_URL). Best effort."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

import httpx

log = logging.getLogger(__name__)

Poster = Callable[[str, dict], Awaitable[None]]


class Notifier:
    def __init__(self, webhook_url: str | None, *, post: Poster | None = None, base_url: str = ""):
        # Anything that is not an http(s) URL (empty, "disabled", a placeholder secret) = off.
        self.url = webhook_url if webhook_url and webhook_url.startswith(("http://", "https://")) else None
        self.base_url = base_url.rstrip("/")
        self.sent: list[dict] = []
        self._post = post or self._http_post

    async def _http_post(self, url: str, payload: dict) -> None:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()

    async def status_changed(
        self, collection_id: str, old: str | None, new: str, note: str | None, actor: str | None = None
    ) -> None:
        text = (f"*{collection_id}*: {old or '—'} → *{new}*" + (f" — {note}" if note else "")
                + (f" (by {actor})" if actor else ""))
        link = f"{self.base_url}/collections/{collection_id}" if self.base_url else ""
        payload = {"text": text + (f"\n{link}" if link else ""),
                   "collection_id": collection_id, "old_status": old, "new_status": new, "note": note,
                   "actor": actor}
        self.sent.append(payload)
        if not self.url:
            return
        try:
            await self._post(self.url, payload)
        except Exception as e:  # noqa: BLE001 - never fail the transition because Slack is down
            log.warning("notification failed: %s", e)
