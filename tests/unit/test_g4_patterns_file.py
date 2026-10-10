"""patterns.yaml (sde_curation/store.py PatternsFile): the snapshot of a collection's rules on disk.

A big rule set is written in the background by the fast writer; it must load to exactly what
PyYAML's file loads to, and a promote (flush) waits until the file is current. Replaces
tests/integration/test_scale.py's two patterns.yaml tests (TEST-STRATEGY-2026-10-09.md, P4).
"""

import asyncio
from datetime import UTC, datetime

import yaml

from sde_curation.models import Pattern
from sde_curation.store import PatternsFile, _dump_rules, _dump_rules_fast

CID = "ex.org"
WHEN = datetime(2026, 9, 18, 12, 30, tzinfo=UTC)
# Values a hand-written YAML emitter gets wrong: booleans, numbers and dates that must stay strings,
# comment and mapping markers, leading spaces, quotes and backslashes, non-ASCII, braces, a dash.
AWKWARD = ["yes", "123", "2026-09-18", "a: b # c", " lead", "quote \" and ' and \\ back", "ünï — ✓",
           "{title} | {collection}", "- dash"]


def _row(i: int, *, type_: str = "title", match: str | None = None, value: str | None = None) -> dict:
    """A patterns row as Database.iter_pattern_rows yields it."""
    return {"id": i, "collection_id": CID, "type": type_, "match": match or f"https://{CID}/p{i}", "value": value,
            "created_at": WHEN, "created_by": "anonymous", "source": "sme"}


class Rules:
    """The two Database reads PatternsFile uses, over a fixed list of rows."""

    def __init__(self, rows: list[dict]):
        self.rows = rows

    async def count_patterns(self, collection_id: str) -> int:
        return len(self.rows)

    async def iter_pattern_rows(self, collection_id: str, chunk: int = 5000):
        for i in range(0, len(self.rows), chunk):
            await asyncio.sleep(0)  # each chunk is its own database round trip
            yield self.rows[i:i + chunk]


def test_the_fast_writer_loads_to_exactly_what_pyyamls_file_loads_to_for_awkward_values(tmp_path):
    rows = [_row(i, value=v) for i, v in enumerate(AWKWARD, 1)]
    rows.append(_row(len(rows) + 1, type_="exclude", match="*/p1?x=1&y=[2]"))
    slow, fast = tmp_path / "slow.yaml", tmp_path / "fast.yaml"
    with slow.open("w", encoding="utf-8") as fh:
        _dump_rules(fh, [Pattern(**r) for r in rows])
    with fast.open("w", encoding="utf-8") as fh:
        _dump_rules_fast(fh, rows)

    assert yaml.safe_load(fast.read_text()) == yaml.safe_load(slow.read_text())
    assert len(yaml.safe_load(fast.read_text())) == len(AWKWARD) + 1


async def test_a_big_rule_set_is_written_in_the_background_and_a_flush_waits_for_it(tmp_path):
    """Over `inline_max` rules a change returns before the file is written (an edit must not pay
    for serialising every rule); flush() — what a promote calls — returns with the file current
    and no temporary file left behind."""
    rows = [_row(i, value=f"Title {i}") for i in range(1, 17)]
    pf = PatternsFile(Rules(rows), tmp_path, inline_max=0, quiet_s=60)
    path = tmp_path / CID / "patterns.yaml"

    await pf.changed(CID)
    written_before_flush = path.exists()
    await pf.flush(CID)

    assert written_before_flush is False
    assert [(r["match"], r["value"]) for r in yaml.safe_load(path.read_text())] == [
        (r["match"], r["value"]) for r in rows]
    assert list(path.parent.glob("*.tmp")) == []
