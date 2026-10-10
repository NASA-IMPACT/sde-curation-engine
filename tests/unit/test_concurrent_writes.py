"""Writes that land while a recompute is between loading the delta rows and writing them back.

A recompute loads every delta row, works for seconds on a large collection, then writes the rows
back. The AI suggestion columns are not the recompute's: Suggest metadata writes them, and accept /
reject clear them. Whatever one of those writes in that window must survive the recompute's write —
before the fix the recompute put back the values it had loaded (a lost suggestion, or a rejected one
that came back).

The window is opened deterministically: `load_dump_failures` is the recompute's last load before it
computes, so a hook on it runs the other write exactly between the load and the write.
"""
from __future__ import annotations


def test_the_columns_a_recompute_leaves_alone_are_exactly_the_ones_it_carries_forward():
    """engine.diff copies the AI fields from the previous row; Database writes them on insert only.
    The two lists must name the same columns, or a new AI column would be overwritten again."""
    from sde_curation.db import Database
    from sde_curation.engine.diff import _AI_FIELDS

    assert set(_AI_FIELDS) == Database._AI_COLS
    assert Database._AI_COLS <= set(Database._DELTA_COLS)
