"""Deterministic offline provider for tests and demos (LLM_PROVIDER=fake).

Heuristics stand in for the model: division/doc-type from URL keywords, title-cased titles,
and an exclude pattern for any path segment that looks like site chrome.
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urlsplit

from pydantic import ValidationError

from ..models import (
    Division,
    DocumentType,
    MetadataSuggestion,
    MetadataSuggestions,
    PatternSuggestion,
    PatternSuggestions,
    PatternType,
)
from .base import LLMError, T

_CHROME = ("privacy", "terms", "login", "leaderboard", "tag", "feed", "sitemap", "search")
_DIV = {
    "helio": Division.HELIOPHYSICS, "aurora": Division.HELIOPHYSICS, "sun": Division.HELIOPHYSICS,
    "earth": Division.EARTH_SCIENCE, "climate": Division.EARTH_SCIENCE,
    "planet": Division.PLANETARY, "mars": Division.PLANETARY,
    "astro": Division.ASTROPHYSICS, "galaxy": Division.ASTROPHYSICS,
    "bio": Division.BPS,
}
_DT = {
    "data": DocumentType.DATA, "dataset": DocumentType.DATA, "image": DocumentType.IMAGES,
    "gallery": DocumentType.IMAGES, "software": DocumentType.SOFTWARE_TOOLS, "tool": DocumentType.SOFTWARE_TOOLS,
    "mission": DocumentType.MISSIONS_INSTRUMENTS, "instrument": DocumentType.MISSIONS_INSTRUMENTS,
}


class FakeProvider:
    name = "fake"

    def __init__(self, canned: Any | None = None):
        self.canned = canned  # raw JSON/dict to return regardless of schema (tests: malformed output)
        self.calls: list[dict[str, str]] = []

    async def complete(self, *, system: str, user: str, schema: type[T]) -> T:
        self.calls.append({"system": system, "user": user, "schema": schema.__name__})
        if self.canned is not None:
            try:
                return schema.model_validate(self.canned)
            except ValidationError as e:
                raise LLMError(f"response did not match {schema.__name__}: {e}") from e
        payload = json.loads(user.split("\n", 1)[1]) if "\n" in user else {}
        if schema is PatternSuggestions:
            return self._patterns(payload)  # type: ignore[return-value]
        if schema is MetadataSuggestions:
            return self._metadata(payload)  # type: ignore[return-value]
        raise LLMError(f"fake provider has no handler for {schema.__name__}")

    def _patterns(self, payload: dict) -> PatternSuggestions:
        urls = [u["url"] for u in payload.get("urls", [])]
        out: list[PatternSuggestion] = []
        seen: set[str] = set()
        for u in urls:
            parts = urlsplit(u)
            for seg in parts.path.strip("/").split("/"):
                if seg.lower() in _CHROME and seg not in seen:
                    seen.add(seg)
                    out.append(PatternSuggestion(
                        type=PatternType.EXCLUDE, match=f"*/{seg}*",
                        rationale=f"'{seg}' pages are site chrome, not science content"))
        if urls:
            out.append(PatternSuggestion(
                type=PatternType.TITLE, match="*", value="{title} | " + payload.get("collection", ""),
                rationale="append the collection name for context in search results"))
        return PatternSuggestions(suggestions=out)

    def _metadata(self, payload: dict) -> MetadataSuggestions:
        items = []
        for d in payload.get("documents", []):
            hay = (d["url"] + " " + (d.get("title") or "") + " " + (d.get("text") or "")).lower()
            div = next((v for k, v in _DIV.items() if k in hay), None)
            dt = next((v for k, v in _DT.items() if k in hay), DocumentType.DOCUMENTATION)
            title = re.sub(r"\s+[-–|]\s+.*$", "", d.get("title") or "").strip() or None
            items.append(MetadataSuggestion(url=d["url"], title=title, division=div, document_type=dt))
        return MetadataSuggestions(items=items)
