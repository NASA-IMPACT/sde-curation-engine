"""Whole-collection work must not freeze the event loop.

The engine is one asyncio process: while a request does Python work on the loop without yielding,
every other curator's request, SSE stream and health check waits. A recompute moves several hundred
thousand rows each way on a big collection; the probe in sde_curation.looplag measures how long the
loop was blocked, and /health/db reports the worst since the previous read.
"""

from __future__ import annotations

import asyncio

from tests.conftest import seed_dump

N = 100_000
# Measured on a laptop for one per-URL edit on N URLs (2026-10-07): 338–359 ms before the loads and
# COPY writes yielded, 86–94 ms after. The limit sits between the two, with room for a busy machine.
MAX_FREEZE_MS = 200


async def test_an_edit_on_a_big_collection_does_not_freeze_the_server(client):
    c = client
    r = await c.post("/api/collections", json={"seed_url": "https://big.org", "name": "big.org", "max_pages": N})
    assert r.status_code == 201, r.text
    await seed_dump(c, "big.org", n=N)
    c.app.state.settings.bulk_job_min_urls = N + 1  # keep the recompute inline: this measures the request
    assert (await c.post("/api/collections/big.org/recompute")).status_code == 200

    await asyncio.sleep(0.25)
    (await c.get("/health/db")).json()  # reset the worst value
    r = await c.post("/api/collections/big.org/urls",
                     json={"url": "https://big.org/p123", "type": "title", "value": "Edited by hand"})
    assert r.status_code == 200, r.text
    await asyncio.sleep(0.25)  # the probe wakes after the edit
    lag = (await c.get("/health/db")).json()["loop_lag_ms"]
    assert lag["max"] < MAX_FREEZE_MS, f"the event loop was blocked for {lag['max']} ms during one edit"
