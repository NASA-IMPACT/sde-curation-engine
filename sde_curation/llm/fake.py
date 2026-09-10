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
    Confidence,
    Division,
    DocumentType,
    MetadataSuggestion,
    PatternSuggestion,
    PatternSuggestions,
    PatternType,
)
from .base import Completion, LLMError, T

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

    async def complete(
        self, *, system: str, user: str, schema: type[T], model: str | None = None
    ) -> Completion[T]:
        self.calls.append({"system": system, "user": user, "schema": schema.__name__, "model": model})
        return Completion(
            parsed=self._answer(user, schema), model=model or self.name,
            tokens_in=len(system + user) // 4, tokens_out=32,
        )

    def _answer(self, user: str, schema: type[T]) -> T:
        if self.canned is not None:
            try:
                return schema.model_validate(self.canned)
            except ValidationError as e:
                raise LLMError(f"response did not match {schema.__name__}: {e}") from e
        if schema is PatternSuggestions:
            return self._patterns(json.loads(user.split("\n", 1)[1]))  # type: ignore[return-value]
        if schema is MetadataSuggestion:
            _, header, text = user.split("\n", 2)
            return self._metadata(json.loads(header), text.split("\nText:\n", 1)[-1])  # type: ignore[return-value]
        raise LLMError(f"fake provider has no handler for {schema.__name__}")

    def _patterns(self, payload: dict) -> PatternSuggestions:
        """Exclude globs only: one per site-chrome path segment seen, plus — so every batch
        yields something deterministic — an exact exclude of the last URL of the batch."""
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
            out.append(PatternSuggestion(type=PatternType.EXCLUDE, match=urls[-1],
                                         rationale="fake: the last URL of every batch is excluded"))
        return PatternSuggestions(suggestions=out)

    def _metadata(self, header: dict, text: str) -> MetadataSuggestion:
        """Confidence is deterministic: high when the keyword is in the URL or title, medium when
        only in the page text, low when the value is a default or missing."""
        strong = (header["url"] + " " + (header.get("scraped_title") or "")).lower()
        weak = text.lower()

        def pick(table: dict, default=None):
            for k, v in table.items():
                if k in strong:
                    return v, Confidence.HIGH
            for k, v in table.items():
                if k in weak:
                    return v, Confidence.MEDIUM
            return default, Confidence.LOW

        div, div_c = pick(_DIV)
        dt, dt_c = pick(_DT, DocumentType.DOCUMENTATION)
        title = re.sub(r"\s+[-–|]\s+.*$", "", header.get("scraped_title") or "").strip() or None
        return MetadataSuggestion(
            title=title, title_confidence=Confidence.HIGH if title else Confidence.LOW,
            division=div, division_confidence=div_c, document_type=dt, document_type_confidence=dt_c,
        )
