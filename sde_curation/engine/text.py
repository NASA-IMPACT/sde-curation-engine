"""Page-text helpers — pure. A content hash tells a re-scrape apart from a re-crawl of the same page."""

from __future__ import annotations

import hashlib


def normalize_text(text: str | None) -> str:
    """Collapse all runs of whitespace to one space and strip; crawler formatting jitter
    (line wrapping, indentation) must not look like a content change."""
    return " ".join((text or "").split())


def content_hash(text: str | None) -> str | None:
    """sha256 of the normalised text; None for an empty page so NULL keeps meaning "unknown"."""
    norm = normalize_text(text)
    if not norm:
        return None
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()
