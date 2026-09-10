"""The global exclude list: a versioned YAML of exclude globs (SME-authored today, COSMOS-mined
later) applied deterministically before the model drafts its own."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from ..engine.patterns import match_counts
from ..models import GlobalExcludeList, Pattern, PatternType

DEFAULT_PATH = Path(__file__).resolve().parent.parent / "data" / "global_excludes.yaml"


@lru_cache(maxsize=8)
def _load(path: str, mtime: float) -> GlobalExcludeList:
    return GlobalExcludeList.model_validate(yaml.safe_load(Path(path).read_text()) or {})


def load_global_excludes(path: Path | None = None) -> GlobalExcludeList:
    p = Path(path) if path else DEFAULT_PATH
    return _load(str(p), p.stat().st_mtime)


def global_exclude_hits(excludes: GlobalExcludeList, all_urls: list[str]) -> list[dict[str, Any]]:
    """Every global glob that matches at least one crawled URL, as pending-suggestion rows."""
    pats = [Pattern(id=i, collection_id="-", type=PatternType.EXCLUDE, match=g.match)
            for i, g in enumerate(excludes.patterns)]
    counts = match_counts(pats, all_urls)
    return [
        {"type": "exclude", "match": g.match, "rationale": g.rationale, "matches": counts[i], "source": "global"}
        for i, g in enumerate(excludes.patterns) if counts.get(i, 0)
    ]
