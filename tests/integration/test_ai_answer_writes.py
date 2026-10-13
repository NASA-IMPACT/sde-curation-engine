"""Suggest metadata writes each batch of answers (and of failures) in one statement.

Dev Run B (2026-10-13): two Suggest metadata jobs on 100K collections, one UPDATE per answered
page, were a third of the database's load while it sat at 99 % CPU. One statement per batch writes
the same rows with the same values (the contract test checks the values on both stores)."""

from __future__ import annotations

import os

import psycopg
import pytest

from sde_curation.db import Database
from sde_curation.models import Collection, ConnectorType, DeltaKind, DeltaUrl

CID = "ex.org"


@pytest.fixture
async def db():
    d = await Database(os.environ["DATABASE_URL"]).connect()
    try:
        yield d
    finally:
        await d.close()


def _answer(i: int) -> dict:
    return {"url": f"https://{CID}/p{i}", "title": f"T{i}", "title_conf": "high", "model": "fake",
            "content_hash": f"h{i}", "division": "Heliophysics", "division_conf": "low"}


async def _collection(db, pages: int) -> None:
    await db.insert_collection(Collection(collection_id=CID, name=CID, seed_url=f"https://{CID}",
                                          connector=ConnectorType.CRAWLER, max_pages=100))
    await db.replace_deltas(CID, [DeltaUrl(collection_id=CID, url=f"https://{CID}/p{i}", kind=DeltaKind.NEW)
                                  for i in range(pages)], [])


def _count_statements(monkeypatch) -> list[str]:
    """Every statement sent that writes delta_urls (the stored-count version bump is another table)."""
    seen: list[str] = []
    execute, executemany = psycopg.AsyncCursor.execute, psycopg.AsyncCursor.executemany

    async def counting_execute(self, query, params=None, **kw):
        if "UPDATE delta_urls" in str(query):
            seen.append("execute")
        return await execute(self, query, params, **kw)

    async def counting_executemany(self, query, params_seq, **kw):
        params_seq = list(params_seq)
        if "UPDATE delta_urls" in str(query):
            seen.extend(["executemany"] * len(params_seq))
        return await executemany(self, query, params_seq, **kw)

    monkeypatch.setattr(psycopg.AsyncCursor, "execute", counting_execute)
    monkeypatch.setattr(psycopg.AsyncCursor, "executemany", counting_executemany)
    return seen


async def test_a_batch_of_answers_is_one_statement(db, monkeypatch):
    await _collection(db, 25)
    seen = _count_statements(monkeypatch)

    assert await db.set_delta_ai(CID, [_answer(i) for i in range(25)], job=7) == 25

    assert seen == ["execute"]


async def test_a_batch_of_failures_is_one_statement(db, monkeypatch):
    await _collection(db, 25)
    seen = _count_statements(monkeypatch)

    assert await db.set_delta_ai_errors(CID, [(f"https://{CID}/p{i}", "timeout") for i in range(25)], job=7) == 25

    assert seen == ["execute"]
