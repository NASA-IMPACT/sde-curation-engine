"""Per-collection git-trackable files under data/collections/<id>/ (workflow.md Option-B layout)."""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import shutil
import time
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

import yaml

from .models import Collection, Pattern, StatusHistory

log = logging.getLogger(__name__)


def collection_dir(root: Path, collection_id: str) -> Path:
    d = root / collection_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_collection_yaml(root: Path, c: Collection, history: Sequence[StatusHistory] = ()) -> Path:
    """collection.yaml = the collection record plus its status history with actors (provenance)."""
    path = collection_dir(root, c.collection_id) / "collection.yaml"
    data = c.model_dump(
        mode="json",
        exclude={"dump_count", "delta_count", "curated_count", "curated_rows", "curated_changed_at"},
    )
    data["history"] = [
        h.model_dump(mode="json", include={"at", "old_status", "new_status", "note", "actor"}) for h in history
    ]
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


_Dumper = getattr(yaml, "CSafeDumper", yaml.SafeDumper)  # libyaml when the wheel has it: ~10× faster


def _dump_rules(fh, patterns: list[Pattern]) -> None:
    """Append rules to an open patterns.yaml. Block-style sequences written one after another are
    one YAML list, so the file can be produced a chunk at a time."""
    if patterns:
        yaml.dump([p.model_dump(mode="json", exclude={"collection_id"}) for p in patterns], fh,
                  Dumper=_Dumper, sort_keys=False, default_flow_style=False)


def write_patterns_yaml(root: Path, collection_id: str, patterns: list[Pattern]) -> Path:
    path = collection_dir(root, collection_id) / "patterns.yaml"
    tmp = path.with_suffix(".yaml.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        _dump_rules(fh, patterns) if patterns else fh.write("[]\n")
    tmp.replace(path)  # a reader never sees half a file, however long the write took
    return path


_RULE_KEYS = ("type", "match", "value", "id", "created_at", "created_by", "source")  # Pattern's own order


def _scalar(v) -> str:
    """A YAML scalar for a rule's value. Strings are written as JSON strings — always quoted, which
    every YAML parser reads as exactly that string (no 'yes' → true, no '123' → int, no timestamp)."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, datetime):  # as Pydantic's JSON mode writes it, so both writers give one file format
        return json.dumps(v.isoformat().replace("+00:00", "Z"))
    return json.dumps(str(v), ensure_ascii=False)


def _dump_rules_fast(fh, rows: list[dict]) -> None:
    """The same list of mappings as _dump_rules, emitted directly from database rows. PyYAML needs
    ~8 s of CPU for the 300k per-URL rules of a 100k-URL collection — per rewrite, competing with
    every curator's recompute for the one interpreter; this needs well under one."""
    fh.write("".join(
        "- " + "\n  ".join(f"{k}: {_scalar(r[k])}" for k in _RULE_KEYS) + "\n" for r in rows
    ))


class PatternsFile:
    """Keeps each collection's patterns.yaml equal to its rules without making a rule change pay
    for it. The file is a snapshot of what the database holds, and nothing reads it back; with a
    per-URL rule for every field of every URL, re-serialising it inside each edit (on the event
    loop) was most of what an edit cost and froze every other request while it ran.

    Small rule sets are written before `changed()` returns. Large ones are written in the
    background — a chunk of rules at a time from a cursor, serialised in a thread — once the
    collection's rules have been quiet for `quiet_s` (a run of inline edits costs one write, not
    one each). `flush()` (promote, shutdown) writes at once and waits until the file is current."""

    def __init__(self, db, root: Path, *, inline_max: int = 2000, quiet_s: float = 20.0):
        self.db, self.root, self.inline_max, self.quiet_s = db, root, inline_max, quiet_s
        self._dirty: dict[str, float] = {}  # collection → when its rules last changed
        self._now: set[str] = set()  # collections a flush() is waiting for
        self._tasks: dict[str, asyncio.Task] = {}

    async def _write(self, collection_id: str) -> None:
        path = collection_dir(self.root, collection_id) / "patterns.yaml"
        tmp = path.with_name(f"patterns.yaml.{secrets.token_hex(4)}.tmp")  # two writers never share one
        fast = await self.db.count_patterns(collection_id) > self.inline_max
        with tmp.open("w", encoding="utf-8") as fh:
            n = 0
            async for chunk in self.db.iter_pattern_rows(collection_id):
                n += len(chunk)
                if fast:
                    await asyncio.to_thread(_dump_rules_fast, fh, chunk)
                else:  # the hand-editable look (PyYAML, quotes only where needed) for ordinary rule sets
                    await asyncio.to_thread(_dump_rules, fh, [Pattern(**r) for r in chunk])
            if not n:
                fh.write("[]\n")
        tmp.replace(path)

    async def _drain(self, collection_id: str) -> None:
        try:
            while collection_id in self._dirty:
                wait = self._dirty[collection_id] + self.quiet_s - time.monotonic()
                if wait > 0 and collection_id not in self._now:
                    await asyncio.sleep(min(wait, 0.25))
                    continue
                self._dirty.pop(collection_id, None)
                try:
                    await self._write(collection_id)
                except Exception:
                    log.exception("patterns.yaml for %s was not written", collection_id)
        finally:
            self._tasks.pop(collection_id, None)
            self._now.discard(collection_id)

    async def changed(self, collection_id: str) -> None:
        if collection_id not in self._tasks and await self.db.count_patterns(collection_id) <= self.inline_max:
            await self._write(collection_id)
            return
        self._dirty[collection_id] = time.monotonic()
        if collection_id not in self._tasks:
            self._tasks[collection_id] = asyncio.create_task(self._drain(collection_id), name=f"patterns-yaml-{collection_id}")

    async def flush(self, collection_id: str | None = None) -> None:
        tasks = [(cid, t) for cid, t in list(self._tasks.items()) if collection_id in (None, cid)]
        self._now.update(cid for cid, _ in tasks)
        if tasks:
            await asyncio.gather(*(t for _, t in tasks), return_exceptions=True)


def remove_collection_files(root: Path, collection_id: str) -> None:
    """Test setup only: the app offers no way to delete a collection."""
    shutil.rmtree(root / collection_id, ignore_errors=True)
