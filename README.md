# sde-curation-engine

Lightweight FastAPI app that drives the SDE curation pipeline:

```
1 Backlog → 2 Scraped → 3 Curating → 4 Curated → 5 Test index → 6 Live
```

It wraps two existing repos — `../sde-crawl4ai-scraper-v1` (crawling) and
`../sde-api-scrapers` (WEB_COSMOS indexing) — behind a small web UI with a live dashboard,
a clickable pipeline stepper, and a curation grid. Plan and phase status: `docs/plan.md`;
workflow background: `docs/workflow.md`.

**Status:** Phases 0–4 done and verified end-to-end (collections, scraping, curation, promotion,
LLM assist — live-tested with OpenAI `gpt-5.4-mini`). Phase 5 (S3 export + WEB_COSMOS test
indexing) and 6 (validation + prod) are next; steps 5–6 in the UI are placeholders.

## Quick start
```bash
cp .env.example .env      # sibling repo paths, AWS values, OPENAI_API_KEY (or LLM_PROVIDER=fake)
uv sync
make run                  # http://localhost:8080   (8000 is taken by sde-elastic-wrapper)
make test                 # 66 tests, incl. a state-matrix that fires every action in every status
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
2. **Collection workbench** (`/collections/{id}`) — one page, four tabs, a sticky header:
   - **Header**: name, seed link, status badge (icon + label), ⚠ *needs re-curation*, running-job
     chip with **cancel**, and count chips that are links — **Dump · Deltas (new/mod/del/excl) ·
     Curated · Patterns** — plus the one **Next** action for the current step.
   - **Overview**: the clickable pipeline stepper (each step's panel shows what it did, its primary
     action, and a redo where sensible), details, last job, *Advanced* (re-scrape, manual status, delete).
   - **URLs**: sub-tabs **Dump** (raw crawl: title, type, depth, text size, state) · **Deltas**
     (kind badge, scraped → effective title, division, type, exclude — all editable inline; AI badges)
     · **Curated** (read-only approved set with a **Curate ↗** jump when a delta exists). Search,
     kind / excluded / division / type filters, page size, paging, ⇩ CSV of the filtered rows.
     Hover a field to see *which pattern* set it.
   - **Patterns & AI**: add a pattern, the pattern table (match counts link to the matching deltas),
     ✨ suggestions with Accept/Reject, Recompute and Promote.
   - **Activity**: all jobs and the status history.
   Old `/collections/{id}/curate` links redirect into URLs › Deltas.
   Step panel actions:

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
3. Typical loop: **Scrape → Start curating → (URLs › Deltas: fix rows; Patterns & AI: add rules /
   accept suggestions) → Promote**. Every inline edit is an exact-URL pattern, so everything is
   visible and reversible in Patterns & AI.

### LLM assist
Two buttons on the Patterns & AI tab, both background jobs that **never change effective values**.
The page refreshes itself when the job finishes (SSE, with a 4 s poll while running), so results
appear without a manual reload:
- **✨ Suggest patterns** — the model sees a sample of crawled URLs (+ scraped titles) and drafts
  `exclude` / `include` / `title` / `division` / `document_type` patterns with a rationale. They land
  in a *Suggested patterns* table with match counts; **Accept** turns one into a real pattern
  (recompute runs), **Reject** dismisses it. Suggestions that match no crawled URL are dropped before
  you see them.
- **✨ Suggest metadata** — per pending URL (title + first 1.5k chars of text, batched 20 per call)
  the model proposes a clean title, division and document type. These show as purple `AI:` badges
  next to each cell in URLs › Deltas; **✓** accepts (creates an exact-URL pattern, i.e. a manual
  override), **✕** dismisses. Suggestions for URLs that weren't asked about are ignored.

Provider is pluggable (`LLM_PROVIDER`): `openai` (default model `gpt-5.4-mini`; any
OpenAI-compatible endpoint via `OPENAI_BASE_URL`; structured outputs parsed straight into Pydantic
models — a malformed reply fails the job and writes nothing) or `fake` (deterministic heuristics,
used in tests and demos; no key needed). Adding a provider = one module implementing
`complete(system, user, schema)` + one line in `llm/base.py`. Prompts live in `llm/tasks.py`
(they ask for host-agnostic globs like `*/login*` so http/https variants are covered together).

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
| `LLM_PROVIDER` (`openai`\|`fake`), `OPENAI_API_KEY`, `OPENAI_MODEL` (default `gpt-5.4-mini`), `OPENAI_BASE_URL`, `LLM_TIMEOUT_S` | LLM assist; any OpenAI-compatible endpoint |
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
| `GET …/deltas?kind&excluded&division&document_type&q&limit&offset`, `…/dump?q`, `…/curated?q&excluded` | paginated URL sets |
| `GET /collections/{id}/urls/{dump\|deltas\|curated}?format=csv&…` | CSV export of the filtered set |
| `POST …/promote` | deltas → curated set; status `curated` |
| `POST …/suggest/patterns`, `POST …/suggest/metadata?all=` | LLM jobs (202; 409 if busy / nothing to do) |
| `GET …/suggestions?state=`, `POST …/suggestions/{sid}/accept\|reject` | pattern suggestions |
| `POST …/ai/accept\|reject` `{url, field}` | per-URL metadata suggestion |
| `GET /health` | `{ok, db, sse_clients}` |

## Testing the workflow by hand
1. Dashboard → add `https://aurorasaurus.org` (max pages 15) → **Scrape**; watch the spinner, then
   status `scraped`, Dump 15. Bad seed / duplicate show a banner.
2. **Start curating** → lands on URLs › Deltas. In Patterns & AI add `exclude */leaderboard*`,
   `title * {title} — {collection}`; back in Deltas pick a division on one row (exact pattern beats
   `*`), click a title to edit, ✗/✓ a row; delete a pattern (un-applies); use the filters and ⇩ CSV;
   check Dump (state column) and Curated (read-only, Curate ↗).
3. Patterns & AI tab: **✨ Suggest patterns** → Accept/Reject rows; **✨ Suggest metadata** → ✓/✕ the `AI:` badges in URLs › Deltas.
4. **Promote** → `curated`; step 3 → Recompute stays `curated` when nothing changed.
5. Step 1 → **Re-scrape** → `scraped` + ⚠; **Start curating** on an identical crawl → back to `curated`.
6. Guard rails: start a bigger crawl, try Add pattern / Promote / Advanced status → 409; **Cancel**;
   Advanced `curated` with pending deltas → 409; delete a collection in one tab → row gone in another.
7. `curl localhost:8080/api/collections/aurorasaurus.org/history`, `data/collections/aurorasaurus.org/*.yaml`.

## Layout
```
sde_curation/
  config.py        pydantic-settings
  models.py        every boundary model (API, DB rows, indexer contracts, LLM schemas)
  db.py            SQLite (aiosqlite), bulk ops
  engine/          pure: patterns.py (resolution), diff.py (deltas, promote)
  curation.py      engine ↔ DB glue, per-collection locking
  backends/        scrape.py (local subprocess | SSM), index.py (Phase 5)
  llm/             base.py (provider protocol + registry), openai.py, fake.py, tasks.py (prompts, sanity filters)
  jobs.py          JobManager: background tasks, cancel, recovery, SSE events
  events.py        in-process event bus → SSE
  web/             FastAPI app, Jinja templates, vendored htmx (+sse, json-enc)
tests/             pytest; fake crawler fixture, moto for AWS, state-matrix
```
