"""The fake crawl4ai crawler, one implementation run two ways.

`fake_crawl` writes what the real crawler leaves behind: the job log, the documents file and the
failures file. The end-to-end tests run it as the crawler's `run.py` in a real subprocess
(FAKE_RUN_PY, through LocalSubprocessScraper). The integration tests run the same function
in-process (InProcessCrawler), so a crawl lands in the database through the engine's own ingest
code without a subprocess.

Sentinels (by `max_pages`): 13 simulates a crash, 14 makes p1 a crawler-trap listing of about
360K tokens, 11 crawls every page under a second (http://) link as well. Every 5th page fails (p5
with a 404, the others with a 403), the rest succeed.

A re-crawl that finds the site changed: write `variant.json` in the crawler root (`write_variant`)
with `{"retitle": {page: title}, "retext": [page, ...], "drop": [page, ...]}`; the next crawl gives
those pages a new title, new text, or leaves them out (not a failure: the page is simply not there).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from pathlib import Path
from typing import Any

from sde_curation.backends.scrape import (
    FileDocuments,
    LocalSubprocessScraper,
    LogProgress,
    ScrapeError,
    ScrapeResult,
    build_job,
)


def fake_crawl(root, job, pause_s=0.02):
    """Write one crawl's output under the crawler root `root`. Returns the process exit code."""
    cid = job["collection_id"]
    log = root / "logs" / "jobs" / f"{cid}.log"; log.parent.mkdir(parents=True, exist_ok=True)
    docs = root / "output" / "collections" / f"{cid}.json"; docs.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as out:
        out.write(f"# job={cid}.json collection_id={cid}\n# seed={job['seed']}\n")
        if job.get("max_pages") == 13:  # sentinel: simulate a crash
            out.write("\n# ERROR: RuntimeError('boom')\n# exit=1 elapsed_s=0.1\n")
            return 1
        n = job["max_pages"]
        for i in range(1, n + 1):
            st = "fail" if i % 5 == 0 else "ok"
            out.write(f"  {i:<5} {st:<10} {0:<6} {job['seed']}/p{i}\n"); out.flush()
            if pause_s:
                time.sleep(pause_s)
        out.write(f"  ... {n - n // 5} docs / {n // 5} failures logged  (cap {n})\n")
        out.write("\n# exit=0 elapsed_s=0.5\n")
    variant_file = root / "variant.json"
    variant = json.loads(variant_file.read_text()) if variant_file.is_file() else {}
    retitle = {int(k): v for k, v in variant.get("retitle", {}).items()}
    retext, drop = set(variant.get("retext", [])), set(variant.get("drop", []))
    pages = [
        {"url": f"{job['seed']}/p{i}", "title": retitle.get(i, f"Page {i}"),
         "full_text": ("changed text " if i in retext else "text ") * 5, "content_type": "text/html", "depth": 0}
        for i in range(1, n + 1) if i % 5 and i not in drop
    ]
    if n == 14:  # sentinel: p1 is a crawler-trap listing, ~360K tokens (more than gpt-5-nano takes)
        pages[0]["full_text"] = " ".join(f"code{j} abstract" for j in range(120_000))
    if n == 11:  # sentinel: the site also links every page over http://, and the crawl follows both
        pages += [{**d, "url": d["url"].replace("https://", "http://", 1)} for d in pages]
    docs.write_text(json.dumps(pages))
    fails = root / "logs" / "collections" / f"{cid}_failures.jsonl"; fails.parent.mkdir(parents=True, exist_ok=True)
    with fails.open("w") as out:  # p5 is gone (404); every later failure is a 403
        for i in range(5, n + 1, 5):
            out.write(json.dumps({"url": f"{job['seed']}/p{i}", "reason": "http_404" if i == 5 else "http_403",
                                  "status": 404 if i == 5 else 403, "detail": "HTTP", "title": ""}) + "\n")
    return 0


# The crawler's run.py for the end-to-end tests: fake_crawl, run as a script.
FAKE_RUN_PY = "\n".join([
    "import json, sys, time",
    "from pathlib import Path",
    "",
    inspect.getsource(fake_crawl),
    "ROOT = Path(__file__).resolve().parent",
    'job = json.loads(Path(sys.argv[sys.argv.index("--job") + 1]).read_text())',
    "code = fake_crawl(ROOT, job)",
    "if code:",
    "    sys.exit(code)",
    "# a test may append lines that change the crawl's output, e.g. docs.write_text(...)",
    'docs = ROOT / "output" / "collections" / f\'{job["collection_id"]}.json\'',
    "",
])


class InProcessCrawler(LocalSubprocessScraper):
    """The local crawler backend with fake_crawl run in-process instead of `run.py` in a
    subprocess. Everything after the crawl (the log, documents and failures files, and the
    engine's ingest of them) is the backend's own code path."""

    async def run(self, collection, on_progress) -> ScrapeResult:
        p = self._paths(collection)
        p["job"].parent.mkdir(parents=True, exist_ok=True)
        job: dict[str, Any] = build_job(collection)
        p["job"].write_text(json.dumps(job, indent=2), encoding="utf-8")
        for k in ("docs", "log", "failures"):
            p[k].unlink(missing_ok=True)
        await on_progress({"pid": 0, "processed": 0, "docs": 0, "failed": 0})
        code = await asyncio.to_thread(fake_crawl, Path(self.root), job, 0)
        progress = LogProgress()
        if p["log"].is_file():
            for line in p["log"].read_text(encoding="utf-8", errors="replace").splitlines():
                progress.feed(line)
        await on_progress(progress.snapshot())
        if code != 0:
            raise ScrapeError(f"crawler exited {code}: {progress.error or ''}")
        if not p["docs"].is_file():
            raise ScrapeError(f"crawler exited 0 but no documents file at {p['docs']}")
        summary: dict[str, Any] = {}
        if p["summary"].is_file():
            summary = json.loads(p["summary"].read_text(encoding="utf-8"))
        return ScrapeResult(documents=FileDocuments(p["docs"]), summary=summary, external_ref="in-process",
                            failures_source=FileDocuments(p["failures"]))


def write_variant(root: Path, *, retitle: dict[int, str] | None = None, retext: list[int] | None = None,
                  drop: list[int] | None = None) -> None:
    """Make the next crawl under `root` find the site changed (see the module docstring)."""
    (root / "variant.json").write_text(json.dumps({"retitle": retitle or {}, "retext": retext or [], "drop": drop or []}))

