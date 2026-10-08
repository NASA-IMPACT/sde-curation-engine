"""Whole-collection work must not freeze the event loop.

The engine is one asyncio process: while a request does Python work on the loop without yielding,
every other curator's request, SSE stream and health check waits. A recompute moves several hundred
thousand rows each way on a big collection; the probe in sde_curation.looplag measures how long the
loop was blocked, and /health/db reports the worst since the previous read.
"""

from __future__ import annotations

import asyncio
import time

from tests.conftest import seed_dump

N = 100_000
# The limit is the worst freeze as a share of the edit's own time, not milliseconds: both grow on a
# slower machine (a CI runner froze 212 ms where the laptop froze 90), but their ratio does not.
# Measured on a laptop for one per-URL edit on N URLs (2026-10-08): 0.143–0.150 before the loads and
# COPY writes yielded (330–352 ms of a 2.2–2.3 s edit), 0.037–0.041 after (81–94 ms).
MAX_FREEZE_SHARE = 0.09


async def test_an_edit_on_a_big_collection_does_not_freeze_the_server(client):
    c = client
    r = await c.post("/api/collections", json={"seed_url": "https://big.org", "name": "big.org", "max_pages": N})
    assert r.status_code == 201, r.text
    await seed_dump(c, "big.org", n=N)
    c.app.state.settings.bulk_job_min_urls = N + 1  # keep the recompute inline: this measures the request
    assert (await c.post("/api/collections/big.org/recompute")).status_code == 200

    await asyncio.sleep(0.25)
    (await c.get("/health/db")).json()  # reset the worst value
    t0 = time.perf_counter()
    r = await c.post("/api/collections/big.org/urls",
                     json={"url": "https://big.org/p123", "type": "title", "value": "Edited by hand"})
    edit_ms = (time.perf_counter() - t0) * 1000
    assert r.status_code == 200, r.text
    await asyncio.sleep(0.25)  # the probe wakes after the edit
    lag = (await c.get("/health/db")).json()["loop_lag_ms"]
    assert lag["max"] / edit_ms < MAX_FREEZE_SHARE, (
        f"the event loop was blocked for {lag['max']:.0f} ms of a {edit_ms:.0f} ms edit "
        f"({lag['max'] / edit_ms:.1%}; limit {MAX_FREEZE_SHARE:.0%})")
