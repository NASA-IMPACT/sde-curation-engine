"""LLM jobs carry on across an engine restart (#29 Suggest patterns, #30 Regenerate titles,
#35 Suggest metadata): what was answered is not asked again, and the result equals an uninterrupted run.

The engine is shut down in the middle of a job and started again. A wrapper around the job's model
call counts what is asked and can hold a call until the engine goes down under it.
"""
from __future__ import annotations

import pytest

CID = "ex.org"


# ── #29 Suggest patterns ────────────────────────────────────────────────












# ── #30 Regenerate titles ───────────────────────────────────────────────






# ── T5.0: the checkpoint stays small at 100K URLs ─────────────────────────


@pytest.mark.parametrize(("batch", "answered"), [
    # the default batch size: 100 batches; worst case every other one unanswered
    (1000, set(range(0, 100, 2))),
    # the smallest batch size: 2,000 batches; a normal run, 16 in flight, cut by a restart halfway,
    # with one batch in 20 failed and waiting for its retry pass
    (50, {i for i in range(1000) if i % 20} | set(range(1000, 1016, 2))),
])
def test_the_suggest_patterns_checkpoint_stays_under_4_kb_at_100k_urls(batch, answered):
    import json

    from sde_curation.jobs import from_ranges, to_ranges
    from tests.support.flows import PROGRESS_MAX_BYTES

    progress = {"phase": "suggesting", "calls": 100_000 // batch, "total": 100_000 // batch, "done": len(answered),
                "failed": 3, "inflight": 16, "retrying": 50, "last_error": "x" * 300, "restarts": 3,
                "suggestions": 4_000, "tokens_in": 10**9, "tokens_out": 10**8, "tokens_cache_write": 0,
                "tokens_reasoning": 10**8, "done_batches": to_ranges(answered), "curation": "y" * 80}
    assert from_ranges(progress["done_batches"]) == answered
    assert len(json.dumps(progress)) < PROGRESS_MAX_BYTES
