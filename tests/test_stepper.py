"""The pipeline stepper: its polling wrapper must not carry hx-vals.

Regression for the double-click bug: hx-vals on the wrapper (added so the poll could re-read ?step
from the address bar) is inherited by the <a> step links, so one click sent ?step=<clicked>&step=<bar>;
FastAPI takes the last value, the old panel stayed, and only a second click showed the right one.
The poll now adds the step in a config-request hook guarded to the wrapper's own request instead.
"""

import re

from tests.conftest import seed_dump


async def test_stepper_links_send_a_single_step(client):
    r = await client.post("/api/collections", json={"seed_url": "science.nasa.gov", "name": "Sci"})
    cid = r.json()["collection_id"]
    await seed_dump(client, cid)
    page = (await client.get(f"/collections/{cid}")).text
    wrapper = re.search(r'<div id="pipeline-[^"]+" class="pipeline-wrap"[^>]*>', page).group(0)
    assert "hx-vals" not in wrapper, "hx-vals on the wrapper is inherited by the step links (double-click bug)"
    # the poll still carries the step from the address bar, via a hook scoped to the wrapper's own request
    hook = re.search(r'hx-on::config-request="([^"]*)"', wrapper)
    assert hook and "event.detail.elt === this" in hook.group(1) and "parameters.step" in hook.group(1)
    links = re.findall(r'<a href="/collections/[^"]+\?step=\w+"[^>]*>', page)
    assert len(links) >= 5 and not any("hx-vals" in a for a in links)


async def test_single_step_param_selects_that_step(client):
    r = await client.post("/api/collections", json={"seed_url": "science.nasa.gov", "name": "Sci"})
    cid = r.json()["collection_id"]
    await seed_dump(client, cid)
    for step in ("backlog", "scraped", "curating", "curated", "config_generated", "live"):
        page = (await client.get(f"/collections/{cid}?step={step}")).text
        assert f'data-step="{step}"' in page, step
        assert re.search(rf'<li class="\w+ selected"[^>]*>\s*<a href="/collections/{cid}\?step={step}"', page), step
