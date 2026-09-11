"""Pattern resolution — pure functions, no I/O.

Semantics (from COSMOS README_PATTERN_* specs, as distilled in docs/plan.md):
  * match: exact URL, or glob where `*` matches anything (converted to a regex). An exact URL
    matches every spelling of that page (http/https, trailing slash, #fragment — the canonical
    key in engine/urls.py): a per-URL edit survives the page moving to https.
  * exclude/include: a URL is excluded iff some exclude pattern matches AND no include matches
  * field patterns (title / division / document_type): the winner is the pattern with the
    smallest match set ("most specific"); ties broken by the longest pattern string
  * title values are templates: {url} {title} {collection}; xpath:// is not supported here
  * effective value = winning pattern value, else the curated value, else NULL
Because resolution is a pure function of (urls, patterns, curated), "unapply" is simply a
recompute after the pattern is gone — the next most specific pattern, then curated, then NULL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from ..models import Pattern, PatternType
from .urls import canonical_key

FIELD_TYPES = (PatternType.TITLE, PatternType.DIVISION, PatternType.DOCUMENT_TYPE)


def glob_to_regex(match: str) -> re.Pattern[str]:
    if "*" not in match:
        return re.compile(re.escape(match) + r"\Z")
    parts = [re.escape(p) for p in match.split("*")]
    return re.compile(".*".join(parts) + r"\Z", re.DOTALL)


def render_title(template: str, *, url: str, scraped_title: str | None, collection: str) -> str:
    return (
        template.replace("{url}", url)
        .replace("{title}", scraped_title or "")
        .replace("{collection}", collection)
        .strip()
    )


@dataclass
class Resolved:
    excluded: bool = False
    title: str | None = None
    division: str | None = None
    document_type: str | None = None
    # which pattern id produced each field (None = fell back to curated/NULL); the key
    # "excluded" names the include rule that forced the URL in, else the exclude rule that kept it out
    effects: dict[str, int] = field(default_factory=dict)


@dataclass
class Compiled:
    pattern: Pattern
    regex: re.Pattern[str]
    matches: set[str] = field(default_factory=set)


def is_exact(match: str) -> bool:
    return "*" not in match


def compile_patterns(patterns: list[Pattern], urls: list[str]) -> list[Compiled]:
    """An exact match (no `*`) is a dict lookup by canonical key, not a regex scan: per-URL edits
    and accepted per-URL AI suggestions are exact patterns, and there can be as many of them as
    URLs. It matches every spelling of its page that the dump has."""
    by_key: dict[str, list[str]] = {}
    for u in urls:
        by_key.setdefault(canonical_key(u), []).append(u)
    out = []
    for p in patterns:
        c = Compiled(p, glob_to_regex(p.match))
        if is_exact(p.match):
            c.matches = set(by_key.get(canonical_key(p.match), ()))
        else:
            c.matches = {u for u in urls if c.regex.match(u)}
        out.append(c)
    return out


def resolve_all(
    urls: list[str],
    patterns: list[Pattern],
    *,
    base: dict[str, dict[str, Any]],
    scraped_titles: dict[str, str | None],
    collection_name: str,
) -> dict[str, Resolved]:
    """Resolve every URL. `base` = curated values per url (title/division/document_type)."""
    compiled = compile_patterns(patterns, urls)

    # url -> the (first) rule that excludes / includes it; the include wins for the effect
    excluded: dict[str, int] = {}
    included: dict[str, int] = {}
    per_field: dict[str, list[Compiled]] = {t: [] for t in FIELD_TYPES}
    # exact patterns always win (the smallest possible match set = most specific), so they are
    # resolved by dict lookup on (type, canonical key) instead of scanning the glob list per URL.
    # Two exact rules for different spellings of one page: the newest (highest id) wins — the
    # curator's latest edit of that row.
    exact: dict[tuple[str, str], Compiled] = {}
    key_of = {u: canonical_key(u) for u in urls}
    for c in compiled:
        if c.pattern.type is PatternType.EXCLUDE:
            for u in c.matches:
                excluded.setdefault(u, c.pattern.id)  # type: ignore[arg-type]
        elif c.pattern.type is PatternType.INCLUDE:
            for u in c.matches:
                included.setdefault(u, c.pattern.id)  # type: ignore[arg-type]
        elif is_exact(c.pattern.match):
            if c.matches:
                k = (c.pattern.type, canonical_key(c.pattern.match))
                if k not in exact or (c.pattern.id or 0) > (exact[k].pattern.id or 0):
                    exact[k] = c
        else:
            per_field[c.pattern.type].append(c)

    # most specific first: smallest match set, then longest pattern string
    for lst in per_field.values():
        lst.sort(key=lambda c: (len(c.matches), -len(c.pattern.match)))

    out: dict[str, Resolved] = {}
    for u in urls:
        r = Resolved(excluded=(u in excluded) and (u not in included))
        if u in excluded:  # an include that overrides nothing has no effect worth recording
            r.effects["excluded"] = included.get(u, excluded[u])
        b = base.get(u, {})
        for t in FIELD_TYPES:
            winner = exact.get((t, key_of[u])) or next((c for c in per_field[t] if u in c.matches), None)
            if winner is None:
                value = b.get(t)
            else:
                value = winner.pattern.value
                if t is PatternType.TITLE:
                    value = render_title(
                        value or "",
                        url=u,
                        scraped_title=scraped_titles.get(u),
                        collection=collection_name,
                    )
                r.effects[t] = winner.pattern.id  # type: ignore[assignment]
            setattr(r, t, value or None)
        out[u] = r
    return out


def match_counts(patterns: list[Pattern], urls: list[str]) -> dict[int, int]:
    return {c.pattern.id: len(c.matches) for c in compile_patterns(patterns, urls)}  # type: ignore[misc]
