"""Per-collection git-trackable files under data/collections/<id>/ (workflow.md Option-B layout)."""

from __future__ import annotations

from pathlib import Path

import yaml

from .models import Collection, Pattern


def collection_dir(root: Path, collection_id: str) -> Path:
    d = root / collection_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_collection_yaml(root: Path, c: Collection) -> Path:
    path = collection_dir(root, c.collection_id) / "collection.yaml"
    data = c.model_dump(mode="json", exclude={"dump_count", "delta_count", "curated_count"})
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


def write_patterns_yaml(root: Path, collection_id: str, patterns: list[Pattern]) -> Path:
    path = collection_dir(root, collection_id) / "patterns.yaml"
    data = [p.model_dump(mode="json", exclude={"collection_id"}) for p in patterns]
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path
