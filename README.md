# sde-curation-engine

Lightweight FastAPI app that drives the SDE curation pipeline:

```
1 Backlog → 2 Scraped → 3 Curating → 4 Curated → 5 Test index → 6 Live
```

It wraps two existing repos — `../sde-crawl4ai-scraper-v1` (crawling) and
`../sde-api-scrapers` (WEB_COSMOS indexing) — behind a small web UI with a live dashboard,
a clickable pipeline stepper, and a curation grid. Plan and phase status: `docs/plan.md`;
workflow background: `docs/workflow.md`.

**Status:** Phases 0–3 done (collections, scraping, curation, promotion). Phase 4 (LLM assist),
5 (export + test indexing) and 6 (validation + prod) are next; steps 5–6 in the UI are placeholders.

## Quick start
```bash
cp .env.example .env      # paths to the sibling repos, AWS values, OpenAI key (Phase 4)
uv sync
make run                  # http://localhost:8080   (8000 is taken by sde-elastic-wrapper)
make test                 # 57 tests, incl. a state-matrix that fires every action in every status
make lint
```

Crawler prerequisite (one-off): the local scrape backend runs `run.py` from
`../sde-crawl4ai-scraper-v1` with its own Python 3.11 venv:
```bash
cd ../sde-crawl4ai-scraper-v1
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python -m playwright install chromium
```
Point `CRAWLER_PYTHON` in `.env` elsewhere if you use a different interpreter.

## Using it
1. **Dashboard** (`/`): add a collection (seed URL, name, division, max pages). Each row shows
   status, counts (dump / pending deltas / curated), last job, and **one button — the next step**.
2. **Collection page** (`/collections/{id}`): the six pipeline steps are clickable. Each step's
   panel shows what it did, its primary action, and a redo where sensible:

   | Step | Panel actions |
   |---|---|
   | 1 Backlog | Scrape / Re-scrape |
   | 2 Scraped | Start curating (computes deltas → curation page), Re-scrape |
   | 3 Curating | Open curation, Recompute deltas, Promote → curated (or *Mark curated* when nothing is pending) |
   | 4 Curated | Review / re-curate; Index to test *(Phase 5)* |
   | 5 Test index | *(Phase 5)* |
   | 6 Live | *(Phase 6)* |

   A running job shows a spinner, live doc counts and a **Cancel** button. *Advanced* (collapsed)
   holds Re-scrape, a manual status override and Delete.
3. **Curation page** (`/collections/{id}/curate`): add patterns (`exclude`, `include`, `title`,
   `division`, `document_type`; exact URL or `*` glob), see match counts, filter/search/paginate the
   delta grid, edit a single URL inline (click the title, or pick division / doc type; ✗ / ✓ toggles
   exclusion), then **Promote**.

### Curation semantics
Effective value per URL = the most specific matching pattern (smallest match set, tie → longest
pattern string) → the curated value → NULL. `include` always beats `exclude`. Title values are
templates (`{title}` = scraped title, `{url}`, `{collection}`). A per-URL edit is just an exact-URL
pattern, so it is the most specific by construction. Deleting a pattern recomputes — that *is* the
unapply (next most specific → curated → NULL). Diff + apply run as one idempotent bulk pass
(100k URLs in < 5 s).

### Guard rails
- One job per collection; **every mutating action returns 409 while a job runs** (cancel first).
- A per-collection lock serialises scrape ingest, recompute, pattern edits and promote.
- Status changes — even manual overrides — must respect the data: `scraped`/`curating` need a
  dump; `curated` and later need a promoted set and no pending deltas.
- Recompute never demotes when nothing changed; an identical re-crawl returns straight to `curated`.
- A re-scrape clears stale deltas and flags ⚠ *needs re-curation* whenever something was promoted.
- Live views refresh on SSE **and** poll (5–10 s), so a missed event cannot leave a page stale.
- Every job ends in `succeeded` or `failed` with the reason shown; jobs orphaned by a crash are
  marked failed on restart.
- Server-side refusals surface as an alert with the server's message; nothing fails silently.

## Configuration (`.env`, see `.env.example`)
| Key | Purpose |
|---|---|
| `DATA_DIR` | SQLite (`engine.db`) + `collections/<id>/{collection,patterns}.yaml` |
| `CRAWLER_ROOT`, `CRAWLER_PYTHON` | crawl4ai repo and its interpreter |
| `INDEXER_ROOT`, `INDEXER_PYTHON` | sde-api-scrapers repo (Phase 5) |
| `SCRAPE_BACKEND` | `local` (subprocess) or `ssm` (drop job on the EC2 inbox via SSM, poll S3) |
| `CRAWLER_INSTANCE_ID`, `CRAWLER_S3_BUCKET` | needed for `ssm` |
| `INDEX_BACKEND`, `COSMOS_INDEX_BUCKET`, `INDEXING_*` | Phase 5 (local subprocess or `ecs:RunTask`) |
| `LLM_PROVIDER`, `OPENAI_API_KEY`, `OPENAI_MODEL`, `OPENAI_BASE_URL` | Phase 4; any OpenAI-compatible endpoint |
| `NOTIFY_WEBHOOK_URL` | Phase 6 notifications |

## API
Everything the UI does is a JSON endpoint (`/docs` for OpenAPI). HTMX callers get
`HX-Redirect`/`HX-Refresh` headers; JSON callers get plain payloads.

| Route | Purpose |
|---|---|
| `GET /events` | SSE stream: `collection`, `collection_created`, `collection_deleted` |
| `POST /api/collections` | create `{seed_url, name, division?, document_type?, max_pages?}` |
| `GET/DELETE /api/collections/{id}` | read / delete (409 while a job runs) |
| `POST …/status` | `{status, note?, force?}` — transition + data rules enforced |
| `GET …/history`, `…/jobs`, `…/dump` | audit trail, job runs, ingested URLs |
| `POST …/scrape` | run the crawl → job (202; 409 if busy) |
| `POST …/jobs/cancel` | cancel the running job |
| `POST …/recompute` | diff dump vs curated + apply patterns (idempotent) |
| `GET/POST /…/patterns`, `DELETE …/patterns/{pid}` | pattern CRUD with match counts |
| `POST …/urls` | per-URL edit `{url, type, value?}`; exclude/include toggles |
| `GET …/deltas?kind&excluded&q&limit&offset` | paginated deltas |
| `POST …/promote` | deltas → curated set; status `curated` |
| `GET /health` | `{ok, db, sse_clients}` |

## Layout
```
sde_curation/
  config.py        pydantic-settings
  models.py        every boundary model (API, DB rows, indexer contracts, LLM schemas)
  db.py            SQLite (aiosqlite), bulk ops
  engine/          pure: patterns.py (resolution), diff.py (deltas, promote)
  curation.py      engine ↔ DB glue, per-collection locking
  backends/        scrape.py (local subprocess | SSM), index.py (Phase 5)
  jobs.py          JobManager: background tasks, cancel, recovery, SSE events
  events.py        in-process event bus → SSE
  web/             FastAPI app, Jinja templates, vendored htmx (+sse, json-enc)
tests/             pytest; fake crawler fixture, moto for AWS, state-matrix
```
