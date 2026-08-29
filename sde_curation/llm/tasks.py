"""LLM-assisted curation tasks. Every result is Pydantic-validated, then *sanity-checked
against the collection* (a suggested URL must be one we sent; a pattern must match something)
before anything is written. Suggestions never touch effective fields — only *_ai / pending rows."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from ..engine.patterns import glob_to_regex
from ..models import (
    Collection,
    Division,
    DocumentType,
    MetadataSuggestions,
    PatternSuggestion,
    PatternSuggestions,
)
from .base import LLMProvider

ProgressCb = Callable[[dict[str, Any]], Awaitable[None]]

_DIVISIONS = ", ".join(d.value for d in Division)
_DOC_TYPES = ", ".join(d.value for d in DocumentType)

PATTERN_SYSTEM = f"""You help curate web crawls for NASA's Science Discovery Engine.
Given a sample of crawled URLs (with scraped titles) from one collection, propose curation patterns.
Pattern types:
- exclude: URL glob (use * as wildcard) for pages that are not science content (login, tags, feeds, site chrome, duplicates).
- include: glob that must stay even if an exclude matches it.
- title: glob + a title template using {{title}} (scraped title), {{url}}, {{collection}} — only when scraped titles are poor or need context.
- division: glob + one of: {_DIVISIONS}.
- document_type: glob + one of: {_DOC_TYPES}.
Rules: globs must be specific (never just "*" for exclude); prefer few, high-value patterns; every pattern must match at least one sample URL; give a one-sentence rationale each.
The same page often appears under http:// and https:// (and with/without a trailing slash): write host-agnostic globs like */login* or */map/?obs=* rather than https://host/login, so all variants are covered."""

METADATA_SYSTEM = f"""You classify crawled web pages for NASA's Science Discovery Engine.
For each document, return: a clean concise title (strip site-name suffixes/prefixes; keep the page's real subject),
the SMD division (one of: {_DIVISIONS}; omit if unclear), and the document type (one of: {_DOC_TYPES}; omit if unclear).
Return one item per input url, using the url exactly as given."""


def _matches_any(match: str, urls: list[str]) -> bool:
    rx = glob_to_regex(match)
    return any(rx.match(u) for u in urls)


async def suggest_patterns(
    llm: LLMProvider, c: Collection, sample: list[dict[str, Any]], all_urls: list[str]
) -> list[PatternSuggestion]:
    payload = {"collection": c.name, "seed": c.seed_url,
               "urls": [{"url": s["url"], "title": s.get("scraped_title")} for s in sample]}
    result: PatternSuggestions = await llm.complete(
        system=PATTERN_SYSTEM, user="Sample:\n" + json.dumps(payload, ensure_ascii=False), schema=PatternSuggestions
    )
    kept: list[PatternSuggestion] = []
    seen: set[tuple[str, str]] = set()
    for s in result.suggestions:
        key = (s.type, s.match)
        if key in seen or s.match.strip() in ("", "*") and s.type == "exclude":
            continue
        if not _matches_any(s.match, all_urls):
            continue  # hallucinated / over-specific: matches nothing we crawled
        seen.add(key)
        kept.append(s)
    return kept


async def suggest_metadata(
    llm: LLMProvider, docs: list[dict[str, Any]], *, batch_size: int = 20,
    on_progress: ProgressCb | None = None,
) -> list[dict[str, Any]]:
    """docs: {url, title, text}. Returns validated rows {url, title, division, document_type}."""
    out: list[dict[str, Any]] = []
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        payload = {"documents": [{"url": d["url"], "title": d.get("title"), "text": (d.get("text") or "")[:1200]}
                                 for d in batch]}
        result: MetadataSuggestions = await llm.complete(
            system=METADATA_SYSTEM, user="Documents:\n" + json.dumps(payload, ensure_ascii=False),
            schema=MetadataSuggestions,
        )
        sent = {d["url"] for d in batch}
        for item in result.items:
            if item.url not in sent:
                continue  # never write a suggestion for a URL we did not ask about
            out.append({"url": item.url, "title": (item.title or "").strip() or None,
                        "division": item.division, "document_type": item.document_type})
        if on_progress:
            await on_progress({"done": min(i + batch_size, len(docs)), "total": len(docs)})
    return out
