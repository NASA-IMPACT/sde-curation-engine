"""LLM-assisted curation tasks. Every result is Pydantic-validated, then *sanity-checked
against the collection* (a suggested URL must be one we sent; a pattern must match something)
before anything is written. Suggestions never touch effective fields — only *_ai / pending rows."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from ..config import Settings
from ..engine.patterns import glob_to_regex
from ..models import (
    Collection,
    Division,
    DocumentType,
    MetadataSuggestion,
    PatternSuggestion,
    PatternSuggestions,
)
from .base import Completion, LLMProvider

ProgressCb = Callable[[dict[str, Any]], Awaitable[None]]

_DIVISIONS = ", ".join(d.value for d in Division)
_DOC_TYPES = ", ".join(d.value for d in DocumentType)

PATTERN_SYSTEM = """You help curate web crawls for NASA's Science Discovery Engine (SDE), a search engine over
NASA science content. You are given one batch of crawled URLs (with their scraped titles) from one
collection, sorted by path. Propose URL globs for pages that must NOT be searchable in the SDE:
sign-in and account pages, tag / category / author / date archives, feeds and machine formats,
search result pages, site chrome (privacy, terms, contact forms), duplicates of the same page
under another URL (print views, share links, pagination of a listing), and anything else that is
not science content.
Rules:
- Only "exclude" globs. Use * as the wildcard. Never a bare "*". Be specific enough that science
  pages are not swept up; when unsure, leave the page in.
- Every glob must match at least one URL in this batch; globs that match nothing are discarded.
- Prefer few, high-value globs over many narrow ones. Give a one-sentence rationale each.
- The same page often appears under http:// and https:// and with or without a trailing slash:
  write host-agnostic globs like */login* or */map/?obs=* rather than https://host/login.
- A list of globs already applied from a global exclude list is given as style examples; do not
  repeat them."""

METADATA_SYSTEM = f"""You classify one crawled web page at a time for NASA's Science Discovery Engine (SDE).
You receive the page URL, its scraped title and its full text (possibly long). The scraped title
is often the same site-wide string on every page, and the text usually starts with the site's
navigation menu, alerts and login links: skip that chrome and read the page's own content. Return:
- title: the title this page should have in a search result, judged by someone who has not seen
  the site — descriptive and self-contained, typically 4–12 words: the page's real subject, plus
  the project, mission or site name when the subject alone would be ambiguous ("Storm Tracker"
  is not enough; "Aurorasaurus Storm Tracker: Real-Time Geomagnetic Activity" is). Do not copy
  the site-wide scraped title, do not pad with slogans. Null only if no sensible title exists.
- division: the NASA Science Mission Directorate division the content belongs to, one of:
  {_DIVISIONS}. Null if it is not clear.
- document_type: one of: {_DOC_TYPES}. Null if it is not clear.
For each of the three fields also give a confidence:
- high: the answer is stated explicitly in the text or title (a mission page names its division,
  a dataset landing page is obviously "Data").
- medium: a strong inference from the URL, the site or the surrounding context.
- low: a guess. Prefer a null value with low confidence over a wrong value.
Never invent facts that are not in the input."""


def _matches_any(match: str, urls: list[str]) -> bool:
    rx = glob_to_regex(match)
    return any(rx.match(u) for u in urls)


async def suggest_patterns_batch(
    llm: LLMProvider, c: Collection, batch: list[dict[str, Any]], *,
    examples: list[str] = (), batch_no: int = 1, batches: int = 1,
) -> tuple[list[PatternSuggestion], Completion[PatternSuggestions]]:
    """One call over one batch of {url, scraped_title}. A suggestion is kept only if it matches
    a URL the model actually saw (this batch), is a real glob and is not a duplicate."""
    payload = {
        "collection": c.name, "seed": c.seed_url,
        "global_excludes_already_applied": list(examples),
        "batch": f"{batch_no} of {batches}",
        "urls": [{"url": s["url"], "title": s.get("scraped_title")} for s in batch],
    }
    done = await llm.complete(
        system=PATTERN_SYSTEM, user="Batch:\n" + json.dumps(payload, ensure_ascii=False), schema=PatternSuggestions
    )
    urls = [s["url"] for s in batch]
    kept: list[PatternSuggestion] = []
    seen: set[tuple[str, str]] = set()
    for s in done.parsed.suggestions:
        key = (s.type, s.match)
        if key in seen or s.match.strip() in ("", "*"):
            continue
        if not _matches_any(s.match, urls):
            continue  # hallucinated / over-specific: matches nothing the model was shown
        seen.add(key)
        kept.append(s)
    return kept, done


async def suggest_metadata_one(llm: LLMProvider, doc: dict[str, Any], *, settings: Settings) -> dict[str, Any]:
    """One call for one document {url, title, text, content_hash}. Returns the row for
    `Database.set_delta_ai` plus the token usage."""
    text = doc.get("text") or ""  # the whole page, never cut: an accurate title needs all of it
    header = {"url": doc["url"], "scraped_title": doc.get("title"), "text_chars": len(text)}
    user = "Document:\n" + json.dumps(header, ensure_ascii=False) + "\n\nText:\n" + text
    done = await llm.complete(system=METADATA_SYSTEM, user=user, schema=MetadataSuggestion)
    r = done.parsed
    return {
        "url": doc["url"],
        "title": (r.title or "").strip() or None, "title_conf": r.title_confidence,
        "division": r.division, "division_conf": r.division_confidence,
        "document_type": r.document_type, "document_type_conf": r.document_type_confidence,
        "model": done.model, "content_hash": doc.get("content_hash"),
        "tokens_in": done.tokens_in, "tokens_out": done.tokens_out, "tokens_cached": done.tokens_cached,
    }


async def suggest_metadata(
    llm: LLMProvider, docs: list[dict[str, Any]], *, settings: Settings,
    on_progress: ProgressCb | None = None,
) -> list[dict[str, Any]]:
    """Sequential convenience wrapper (tests, scripts): one call per document."""
    out: list[dict[str, Any]] = []
    for i, d in enumerate(docs, 1):
        out.append(await suggest_metadata_one(llm, d, settings=settings))
        if on_progress:
            await on_progress({"done": i, "total": len(docs)})
    return out
