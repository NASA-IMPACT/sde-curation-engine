# sde-curation-engine

Lightweight FastAPI app that drives the SDE curation pipeline:

```
1 Backlog → 2 Scraped → 3 Curating → 4 Curated → 5 Test index → 6 Live
```

It wraps two existing repos — `../sde-crawl4ai-scraper-v1` (crawling) and
`../sde-api-scrapers` (WEB_COSMOS indexing) — behind a small web UI with a live dashboard,
a clickable pipeline stepper, and a curation grid. Plan and phase status: `docs/plan.md`;
workflow background: `docs/workflow.md`.

**Status:** all six phases built — collections, scraping, curation, promotion, LLM assist, S3 export +
WEB_COSMOS test indexing, validation gate, prod indexing, notifications.

## Quick start
```bash
cp .env.example .env      # sibling repo paths, AWS values, OPENAI_API_KEY (or LLM_PROVIDER=fake)
make install              # uv sync if uv is installed, else python3.13 venv + pip -r requirements-dev.txt
make run                  # http://localhost:8080   (8000 is taken by sde-elastic-wrapper)
make test                 # 91 tests, incl. a state-matrix that fires every action in every status
make lint
```

Crawler prerequisite (one-off): the local scrape backend runs `run.py` from
`../sde-crawl4ai-scraper-v1` with its own Python 3.11 venv:
```bash
cd ../sde-crawl4ai-scraper-v1
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m playwright install chromium
```
Point `CRAWLER_PYTHON` in `.env` elsewhere if you use a different interpreter.

**Dependencies** — `pyproject.toml` + `uv.lock` (and the same pair in `infra/`) are the source of
truth. `requirements.txt` / `requirements-dev.txt` are exported from the locks and committed, so a
developer without uv, CI, and the Docker image all install the exact same versions with pip.
Changing a dependency: edit `pyproject.toml`, `uv lock`, `make requirements`, commit all four files
(CI fails if the exports are stale). Never edit `requirements*.txt` by hand.

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
   | 4 Curated | **Index to test** (export + dispatch), Review / re-curate |
   | 5 Test index | run summary (exported, indexed/changed/deleted, validation pass/fail + how it was validated), Re-index, **Re-validate** |
   | 6 Live | **Index to prod** (only after a validated test run), prod run summary |

   A running job shows a spinner, live doc counts and a **Cancel** button. *Advanced* (collapsed)
   holds Re-scrape, a manual status override and Delete.
3. **User manual** (`/manual`, also in the ☰ menu): the illustrated curator's handbook — quick path,
   screen-by-screen walkthrough, rule semantics, jobs, parallel work, quirks. Template
   `sde_curation/web/templates/manual.html`, screenshots in `static/manual/`.
4. Typical loop: **Scrape → Start curating → (URLs › Deltas: fix rows; Patterns & AI: add rules /
   accept suggestions) → Promote**. Every inline edit is an exact-URL pattern, so everything is
   visible and reversible in Patterns & AI.

### LLM assist
Two buttons on the Patterns & AI tab, both background jobs that **never change effective values**.
The page refreshes itself when the job finishes (SSE, with a 4 s poll while running), so results
appear without a manual reload:
- **✨ Suggest patterns** — **exclude globs only**: which pages must never be searchable. First the
  **global exclude list** (`sde_curation/data/global_excludes.yaml`, entries tagged `source: sme`
  or `cosmos`) is applied deterministically: every glob that matches at least one crawled URL becomes
  a pending suggestion tagged `global`, with its match count. Then the model sees **every** crawled
  URL (URL + scraped title only, http/https and trailing-slash twins collapsed, sorted by path) in
  batches of `LLM_PATTERN_BATCH_URLS` (default 1000), one call per batch through the worker pool,
  and drafts globs with a rationale; a glob is kept only if it matches a URL in the batch the model
  saw. Rows merge by glob across batches. The button says how many URLs and calls that is.
  **Accept** turns a row into a real `exclude` rule (recompute runs) — the URLs are out of scope,
  so they never reach the export or the index, and they are never sent for metadata; **Reject**
  dismisses it. Include / title / division / type rules remain curator tools (the add-rule form).
- **✨ Suggest metadata** — **one call per pending, included URL with the full page text**, up to
  `LLM_WORKERS` (default 24) in flight. The text is never cut: an accurate title needs the whole
  page, and the default model (`gpt-5.6-luna`, 1.05M-token window) takes any page whole; a page
  beyond the model's window fails that one call (counted, shown, retried on the next run) rather
  than being guessed from a slice. The model returns a descriptive search-result title, division and document type, each
  with a **confidence** (`high` = explicit in the text, `medium` = strong inference, `low` = guess).
  These show as `AI:` badges with the confidence next to each cell in URLs › Deltas (filter by
  confidence; the review bar counts them); **✓** accepts (creates an exact-URL pattern, i.e. a
  manual override), **✕** dismisses. Answers are written as they arrive: **cancel keeps what
  finished**, one bad URL is counted as failed and never fails the job, and re-running classifies
  only what is missing — plus any URL whose text changed since it was last classified. The job
  result shows classified / failed counts and tokens in / out.

Provider is pluggable (`LLM_PROVIDER`): `openai` (default model `gpt-5.6-luna`; any
OpenAI-compatible endpoint via `OPENAI_BASE_URL`; structured outputs parsed straight into Pydantic
models — a malformed reply fails the job and writes nothing) or `fake` (deterministic heuristics,
used in tests and demos; no key needed). Adding a provider = one module implementing
`complete(system, user, schema, model=None) -> Completion` + one line in `llm/base.py`. Prompts
live in `llm/tasks.py` (they ask for host-agnostic globs like `*/login*` so http/https variants
are covered together). The worker pool (`llm/pool.py`) retries nothing itself — the OpenAI client
retries 429 / 5xx / timeouts `LLM_MAX_RETRIES` times with backoff — but it keeps going past
per-URL failures and aborts only after ten consecutive non-retryable errors (bad key, bad model).

**Content-aware deltas**: every crawled page gets a `content_hash` (sha256 of its
whitespace-normalised text) at ingest; a promote carries the current hash onto the curated rows.
On the next re-scrape a page whose text changed shows as *modified* with a **text changed** badge
(filter: *text changed since promotion*) even when its title and metadata did not move, and
Suggest metadata re-classifies it. Rows promoted before hashing existed have no hash and compare as
unchanged, so the first run after an upgrade does not flag everything.

### Indexing (Phase 5)
**Index to test** exports the curated, non-excluded URLs as the indexer's contract —
`s3://$COSMOS_INDEX_BUCKET/curated_collections/{key}/{run_id}/documents.jsonl` then
`manifest.json` (written last = "export complete") — and dispatches
`api_scraper.py --source WEB_COSMOS --collection {key} --run-id {run_id} --target test` from
`../sde-api-scrapers`, either as a **local subprocess** (`INDEX_BACKEND=local`, needs the
OpenSearch/SageMaker env in `.env`) or as an **ECS Fargate task** (`INDEX_BACKEND=ecs`,
`ecs:RunTask` on `web_cosmos-scraper-{env}` with the command override; assumes
`INDEXING_DISPATCH_ROLE_ARN` when set, else ambient credentials). Completion is always read from
`index_runs/{key}/{run_id}/status.json` (+ `validation.json` for test) that the indexer writes last;
a stopped task with no status, or `INDEX_STALL_TIMEOUT_S`, fails the run explicitly. Success moves
the collection to `config_generated`; every run is kept in `index_runs` (see the step-5 panel).
### Validation gate and prod (Phase 6)
The indexer validates in-process right after its bulk upsert — before OpenSearch Serverless has
refreshed — so on any run that wrote something its `validation.json` reads `0/N` (reproduced: the
same export re-run a minute later reads `13/13`). The engine therefore does **not** trust that file.
After a test run succeeds it waits `VALIDATION_DELAY_S` (30 s) and validates itself:
1. **Direct** (fast): a SigV4 query of the target index for `collection_key`, comparing counts and
   titles exactly like the indexer's `web/validate.py`. Needs `OPENSEARCH_ENDPOINT_TEST/PROD` and
   AOSS **data access** for the engine's principal — or `VALIDATION_ASSUME_ROLE_ARN` naming a role
   that already has it (e.g. `indexing-helper-role`).
2. **Fallback** on 403 / no endpoint: logs "no AOSS data access", deletes the stale
   `status.json`/`validation.json`, and dispatches a **second pass** of the same export
   (`changed: 0`, nothing re-vectorised) purely to get a fresh `validation.json` from the indexer.

Pass (`count_matches` and titles ≥ `VALIDATION_TITLE_MATCH_THRESHOLD`, default 0.99) → status
`config_generated` with **Index to prod** enabled. Fail → back to `curating` with ⚠ *needs
re-curation* and the mismatches listed. **Re-validate** re-runs the check on demand. A prod run
(`?target=prod`) is refused until the latest test run passed; success → `live` and the ⚠ flag clears.
Every status transition posts to `NOTIFY_WEBHOOK_URL` (Slack-compatible `{"text": …}` with a link
built from `PUBLIC_BASE_URL`); failures to notify never block a transition.

**Deploying (AWS CDK + GitHub Actions)**: `infra/` holds a Python CDK app that runs the engine as
one ECS Fargate task (SQLite on EFS) behind an ALB and CloudFront (HTTPS + WAF), wired to the
crawler over SSM and the WEB_COSMOS indexer over `ecs:RunTask`, with its own read-only AOSS
data-access policy for direct validation. Pushing to `dev` / `test` / `prod` deploys that
environment via `.github/workflows/deploy.yml` (branch = environment = AWS account, OIDC role per
account, same model as `sde-api-scrapers`); pull requests only run tests. Account-specific values
(instance ids, buckets, endpoints, role ARNs) are not in git — they live in SSM Parameter Store per
environment and are resolved at deploy time; API keys and the login password are Secrets Manager
secrets set once per environment. Step-by-step runbook (one-time account setup, first deploy,
verification): `docs/deploy-dev.md`; manual end-to-end test plan: `docs/e2e-test.md`; stack reference and the exact IAM the task role gets:
`infra/README.md`. `APP_PASSWORD` turns on login and seeds the first `admin` account with that value (only while the
users table is empty); admins create accounts at `/users`, everyone changes their own password at
`/account`. Roles: admin (users, delete collections), curator (everything else). Every action is
attributed to the signed-in user: status history, patterns, jobs, an append-only audit trail
(Activity tab, `/api/collections/{id}/audit`) and the per-collection `collection.yaml` /
`patterns.yaml`. Locally it is off, so the UI and tests run as `anonymous`.

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

### Working in parallel (several curators at once)
Concurrency is **per collection, not global**. Any number of collections can have jobs running at
the same time; each collection allows exactly one job, and curation edits on a collection wait
for that collection's job. The app itself puts no cap on how many scrapes, index runs or LLM jobs
are in flight — the ceilings come from the systems behind it.

**Isolation**
- One asyncio lock per collection, shared by scrape ingest, LLM jobs, index runs and every
  curation write. Curators on different collections never block each other; on the same
  collection, writes serialise and a second job start is refused with 409 (not queued).
- Starting a job checks three things — a job being created, a live job task, and a held lock —
  so two people clicking *Scrape* in the same instant cannot both start one.
- Cancel only touches that collection's job and records who cancelled it.

**What actually runs in parallel**
- *Scrapes* — `local`: one subprocess per collection, all at once. `ssm`: the crawler box runs
  one job at a time under `flock`; extra jobs are accepted immediately and shown as *queued*
  with how many crawls are ahead, and a queue can wait indefinitely. So scrapes from several
  curators are accepted in parallel but crawled one at a time.
- *LLM jobs* — each job runs its own pool of `LLM_WORKERS` (default 24) concurrent calls. There
  is no limiter across jobs: five curators classifying at once is up to 120 in-flight calls.
  This is the first place you will hit the provider's rate limit.
- *Index runs* — each dispatches its own ECS task and polls S3. Nothing limits how many run at
  once; runs on different collections are fine as long as the indexer tolerates it.

**Data layer**
- SQLite in WAL mode with one connection per process; writes serialise at the connection. The
  locks live in memory and the ECS service is pinned to one task — **do not run two replicas**,
  the mutual exclusion would not hold across them.
- Curation edits have no stale-edit check. Two curators editing the same row on the same
  collection: last save wins silently. Every write records the actor (activity tab), but nobody
  is warned.

**What other curators see**
- Job starts, progress and completion are pushed over SSE to every open browser; the header,
  pipeline and jobs strip also poll (5–10 s), so a second curator sees status and counts move.
- Another person's pattern/metadata edits are *not* pushed. Header counts catch up on the next
  poll; the curate table body only reloads when a job finishes or the page is refreshed.

**Guidance**
- Assign curators to distinct collections — that is the model the app is built around.
- Expect scrapes to queue on the shared crawler; kick them off early.
- If several people will run LLM jobs at the same time, lower `LLM_WORKERS` or add a
  process-wide semaphore in `llm/pool.py` so total in-flight calls stay under the provider limit.
- If two people must share one collection, agree on who edits; the app will not detect a
  stale edit.

## Configuration (`.env`, see `.env.example`)
| Key | Purpose |
|---|---|
| `DATA_DIR` | SQLite (`engine.db`) + `collections/<id>/{collection,patterns}.yaml` |
| `CRAWLER_ROOT`, `CRAWLER_PYTHON` | crawl4ai repo and its interpreter |
| `INDEXER_ROOT`, `INDEXER_PYTHON` | sde-api-scrapers repo (Phase 5) |
| `SCRAPE_BACKEND` | `local` (subprocess) or `ssm` (drop the job on the EC2 inbox via SSM; the job shows as *queued* until the crawler rewrites its log, then S3 is polled for the documents object) |
| `AWS_PROFILE` | local runs only: the AWS CLI/SSO profile boto3 uses (the app exports it); unset in ECS |
| `CRAWLER_INSTANCE_ID`, `CRAWLER_S3_BUCKET`, `CRAWLER_S3_PREFIX` | needed for `ssm`; the prefix is the folder inside the bucket the crawler writes to (`<prefix>/scraped_collections/…`), empty = bucket root |
| `INDEX_BACKEND` (`local`\|`ecs`), `COSMOS_INDEX_BUCKET`, `WEB_INDEX_NAME` | indexing target bucket / index |
| `INDEXING_ECS_CLUSTER`, `INDEXING_TASK_FAMILY`, `INDEXING_CONTAINER_NAME`, `INDEXING_SUBNETS`, `INDEXING_SECURITY_GROUPS`, `INDEXING_DISPATCH_ROLE_ARN` | `ecs` backend |
| `OPENSEARCH_ENDPOINT_TEST`, `OPENSEARCH_ENDPOINT_PROD`, `SAGEMAKER_ENDPOINT_NAME` | `local` backend (the ECS task def already carries these) |
| `INDEX_POLL_INTERVAL_S`, `INDEX_STALL_TIMEOUT_S` | status.json polling; the stall timeout also bounds a *started* remote crawl's silence |
| `SCRAPE_POLL_INTERVAL_S` | `ssm` backend: how often to look at the crawler host. A queued job waits indefinitely (the UI shows for how long); only a dead `watch_inbox.sh` fails it |
| `VALIDATION_DELAY_S`, `VALIDATION_TITLE_MATCH_THRESHOLD`, `VALIDATION_ASSUME_ROLE_ARN` | validation gate |
| `NOTIFY_WEBHOOK_URL`, `PUBLIC_BASE_URL` | Slack-compatible notifications on every status change |
| `LLM_PROVIDER` (`openai`\|`fake`), `OPENAI_API_KEY`, `OPENAI_MODEL` (default `gpt-5.6-luna`), `OPENAI_BASE_URL`, `LLM_TIMEOUT_S` (per attempt), `LLM_MAX_RETRIES` | LLM assist; any OpenAI-compatible endpoint |
| `LLM_WORKERS` (24), `LLM_PATTERN_BATCH_URLS` (1000), `GLOBAL_EXCLUDES_PATH` | calls in flight per LLM job; URLs per Suggest-patterns call; override the packaged global exclude YAML |
| `APP_PASSWORD`, `SESSION_SECRET`, `SESSION_TTL_S`, `AUTH_COOKIE_SECURE` | login with local accounts (off when `APP_PASSWORD` is empty; the value seeds the bootstrap `admin`); `/health` stays open |
| `DB_LOCKING_MODE` (`normal`\|`exclusive`) | `exclusive` when `engine.db` lives on EFS/NFS |

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
| `POST …/index?target=test\|prod` | export to S3 + dispatch WEB_COSMOS → job (202; 409 unless curated, no pending deltas, something to export; prod needs a validated test run) |
| `POST …/index/revalidate` | re-check the latest test run against the index (direct, or second pass) |
| `GET …/index_runs` | run history: indexer status, validation report, `validated_by` (indexer\|direct\|second_pass) |
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
  engine/          pure: patterns.py (resolution), diff.py (deltas, promote), export.py (indexer contract)
  curation.py      engine ↔ DB glue, per-collection locking
  backends/        scrape.py (local subprocess | SSM), index.py (local subprocess | ECS), validate.py (direct AOSS check), s3.py
  notify.py        Slack-compatible webhook on status transitions
  llm/             base.py (provider protocol + registry), openai.py, fake.py, tasks.py (prompts, sanity filters)
  jobs.py          JobManager: background tasks, cancel, recovery, SSE events
  events.py        in-process event bus → SSE
  web/             FastAPI app, auth.py (local users, roles, signed sessions), Jinja templates, vendored htmx (+sse, json-enc)
tests/             pytest; fake crawler fixture, moto for AWS, state-matrix
infra/             AWS CDK (Python): Fargate + EFS + ALB + CloudFront/WAF; bootstrap/ = GitHub deploy role
.github/workflows/ deploy.yml (push to dev|test|prod → cdk deploy), test.yml (PRs)
Dockerfile         python:3.13-slim + pip -r requirements.txt (exported from uv.lock), non-root, uvicorn on 8080
```
