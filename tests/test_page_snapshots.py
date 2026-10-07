"""Page-snapshot guard: what curators see must not change by accident.

Builds one fixed collection through the real API (fake crawler, fake LLM), renders every page and
fragment a curator can open, and compares the HTML with the files under tests/snapshots/pages/.
A change that is meant to be visible updates them deliberately:

    UPDATE_SNAPSHOTS=1 .venv/bin/python -m pytest tests/test_page_snapshots.py

Anything that differs from run to run is normalized away first: timestamps, elapsed times, the
temporary data directory and static-asset version hashes. Everything else is compared exactly.
"""

from __future__ import annotations

import difflib
import os
import re
from pathlib import Path

import pytest

from sde_curation.models import JobKind, JobRun, JobState
from sde_curation.web.app import templates
from tests.conftest import classify, wait_job

SNAPSHOTS = Path(__file__).parent / "snapshots" / "pages"
UPDATE = os.environ.get("UPDATE_SNAPSHOTS") == "1"

_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:\s?(?:[+-]\d{2}:?\d{2}|Z|UTC))?"
)
_SHORT_TIMESTAMP = re.compile(r"\b\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?\b")  # the jobs tables' MM-DD HH:MM:SS
_JOB_DURATION = re.compile(r'<td class="num">\d+s</td>')  # finished_at - started_at of a job
_STATIC_VERSION = re.compile(r"\?v=[0-9a-f]+")
# A multi-line tooltip ("field: rule …&#10;field: rule …") lists one line per rule effect in the order
# the database returns them, which has no ORDER BY and varies from run to run. Compare the set.
_MULTILINE_TITLE = re.compile(r'title="([^"]*&#10;[^"]*)"')


def _sorted_title(m: re.Match) -> str:
    lines = [x for x in m.group(1).split("&#10;") if x]
    return 'title="' + "&#10;".join(sorted(lines)) + '&#10;"'


def normalize(html: str, tmp_root: str) -> str:
    html = html.replace(tmp_root, "<TMP>")
    html = _TIMESTAMP.sub("<TS>", html)
    html = _SHORT_TIMESTAMP.sub("<TS>", html)
    html = _JOB_DURATION.sub('<td class="num"><DUR></td>', html)
    html = _STATIC_VERSION.sub("?v=<V>", html)
    html = _MULTILINE_TITLE.sub(_sorted_title, html)
    return "\n".join(line.rstrip() for line in html.splitlines()) + "\n"


async def build_fixture(c) -> None:
    """ex.org: crawled, curated through every stage, partly promoted, then edited again.
    run.org: crawled and queued for review, with a Suggest-metadata job shown as running."""
    r = await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "ex.org", "max_pages": 10})
    assert r.status_code in (200, 201), r.text
    await c.post("/api/collections/ex.org/scrape")
    assert (await wait_job(c, "ex.org"))["state"] == "succeeded"
    assert (await c.post("/api/collections/ex.org/recompute")).status_code == 200

    assert (await c.post("/api/collections/ex.org/suggest/patterns")).status_code == 202
    assert (await wait_job(c, "ex.org"))["state"] == "succeeded"
    sugs = (await c.get("/api/collections/ex.org/suggestions")).json()
    items = sugs.get("items", sugs) if isinstance(sugs, dict) else sugs
    pending = [s for s in items if s.get("state") == "pending"]
    if pending:  # accept one, leave the rest pending
        r = await c.post(f"/api/collections/ex.org/suggestions/{pending[0]['id']}/accept")
        assert r.status_code == 200, r.text

    await classify(c)  # Suggest metadata, then accept every suggestion
    r = await c.post("/api/collections/ex.org/patterns", json={"type": "exclude", "match": "*/p2"})
    assert r.status_code == 201, r.text

    deltas = (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"]
    picked = sorted(d["url"] for d in deltas if d["kind"] != "deleted" and not d["excluded"])[:2]
    r = await c.post("/api/collections/ex.org/promote/urls", json={"urls": picked})
    assert r.status_code == 200, r.text
    left = sorted(d["url"] for d in (await c.get("/api/collections/ex.org/delta?limit=100")).json()["items"])
    r = await c.post("/api/collections/ex.org/urls",
                     json={"url": left[0], "type": "title", "value": "Edited by hand"})
    assert r.status_code == 200, r.text

    r = await c.post("/api/collections", json={"seed_url": "https://run.org", "name": "run.org", "max_pages": 10})
    assert r.status_code in (200, 201), r.text
    await c.post("/api/collections/run.org/scrape")
    assert (await wait_job(c, "run.org"))["state"] == "succeeded"
    assert (await c.post("/api/collections/run.org/recompute")).status_code == 200
    # a Suggest-metadata job as the pages show it while it runs (the row a running job keeps)
    await c.app.state.db.insert_job(JobRun(
        collection_id="run.org", kind=JobKind.LLM_METADATA, state=JobState.RUNNING,
        progress={"llm": "metadata", "total": 8, "done": 3, "classified": 3, "failed": 0},
    ))


PAGES: dict[str, str] = {
    "dashboard": "/",
    "dashboard_rows": "/rows",
    "jobs_panel": "/jobs/panel",
    "jobs": "/jobs",
    "history": "/history",
    "ex_overview": "/collections/ex.org?tab=overview",
    "ex_curate": "/collections/ex.org?tab=curate",
    "ex_curate_focus_metadata": "/collections/ex.org?tab=curate&focus=metadata",
    "ex_dump": "/collections/ex.org?tab=dump",
    "ex_delta": "/collections/ex.org?tab=delta",
    "ex_curated": "/collections/ex.org?tab=curated",
    "ex_rules": "/collections/ex.org?tab=rules",
    "ex_activity": "/collections/ex.org?tab=activity",
    "ex_header": "/collections/ex.org/header",
    "ex_pipeline": "/collections/ex.org/pipeline",
    "ex_row": "/collections/ex.org/row",
    "run_curate_job_running": "/collections/run.org?tab=curate",
    "run_overview_job_running": "/collections/run.org?tab=overview",
    "run_header_job_running": "/collections/run.org/header",
    "run_pipeline_job_running": "/collections/run.org/pipeline",
}


@pytest.fixture
def fixed_elapsed_times(monkeypatch):
    """`since` prints "45s" / "12m" against the wall clock; the snapshot keeps where it is shown."""
    monkeypatch.setitem(templates.env.filters, "since", lambda dt: "<SINCE>")


async def test_pages_look_exactly_as_before(crawler_client, tmp_path, fixed_elapsed_times):
    c = crawler_client
    await build_fixture(c)
    SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    changed, missing = [], []
    for name, path in PAGES.items():
        r = await c.get(path)
        assert r.status_code == 200, f"{path}: {r.status_code}"
        got = normalize(r.text, str(tmp_path))
        f = SNAPSHOTS / f"{name}.html"
        if UPDATE:
            f.write_text(got)
            continue
        if not f.exists():
            missing.append(name)
            continue
        want = f.read_text()
        if got != want:
            diff = "".join(list(difflib.unified_diff(
                want.splitlines(keepends=True), got.splitlines(keepends=True),
                fromfile=f"snapshot/{name}", tofile=f"now/{name}", n=2))[:80])
            changed.append(f"{name} ({path}):\n{diff}")
    assert not missing, f"no snapshot yet for {missing}: run with UPDATE_SNAPSHOTS=1 once"
    assert not changed, "pages changed — curators would see this:\n\n" + "\n\n".join(changed)
