# SDE Curation Engine — FastAPI web app

## Context
`docs/workflow.md` defines a 6-stage pipeline (Collection → Scrape → Curate → Index(test) →
Validate → Prod). This repo is an empty `uv` project (`main.py` stub, Python 3.13). The goal
is a **minimal, fast, reliable FastAPI app** with a live status dashboard that drives the
pipeline by interfacing with two existing repos, using an open (provider-agnostic) LLM layer
starting with OpenAI, and Pydantic validation on every boundary.

Decisions confirmed with the user: pluggable scrape backends (local subprocess + SSM/EC2);
Jinja2 + HTMX + SSE dashboard; app performs S3 export + `ecs:RunTask` dispatch + status polling
for indexing; no auth in v1.

### Integration contracts discovered (the two sibling repos)

**`../sde-crawl4ai-scraper-v1`** (crawler; Python 3.11, no pyproject, not importable — run as subprocess)
- Invoke: `<crawler-venv>/bin/python run.py --job <abs job.json> [--bucket B]`; exit 0/1.
  `--job` **moves** the job file to `jobs/done|failed/`.
- Job JSON: `{"seed": ..., "collection_id": ..., "max_pages": N, "depth_limit", "delay",
  "concurrent_requests", "obey_robots", "include_subdomains"}` (`sde_crawler/job.py::merge_job`).
- Outputs (under crawler root): `output/collections/<id>.json` (array of
  `{url,title,full_text,content_type,seed,host,depth}` — written only at end),
  `logs/collections/<id>_failures.jsonl` (live), `logs/collections/<id>_failures_summary.json`,
  `logs/jobs/<job-stem>.log` (progress; ends with `# exit=0 elapsed_s=…` or `# ERROR:`).
- S3 (`SDE_S3_BUCKET`): `scraped_collections/<id>.json`, `failure_logs/<id>_failures.jsonl`,
  `failure_logs/<id>_failures_summary.json`.
- Remote: EC2 instance (CFN stack `SdeCrawlerStack`, outputs `InstanceId`, `BucketName`), SG is
  egress-only → only channel is `ssm.send_command(AWS-RunShellScript)` writing
  `/opt/sde-crawler/jobs/incoming/<id>.json` (model: `scripts/drop_job.sh`); completion =
  `s3.head_object(scraped_collections/<id>.json)`.

**`../sde-api-scrapers`** branch `web-indexing` (indexer; Python 3.11)
- Input contract (what *we* must write): `s3://sde-cosmos-indexing-{env}/curated_collections/{collection_key}/{run_id}/documents.jsonl`
  (lines: `url` required, `title`, `full_text`, optional `document_type`, `division`) and
  `manifest.json` **written last** with `collection_key, run_id, document_count` (+ `collection_name,
  division, document_type, target, exported_at`). See `web/cosmos_source.py`, `web/web_processor.py`.
- Dispatch: assume `INDEXING_DISPATCH_ROLE_ARN` (`CosmosIndexingDispatchRole-{env}`) →
  `ecs.run_task(cluster=api-scrapers-cluster-{env}, taskDefinition=web_cosmos-scraper-{env},
  launchType=FARGATE, networkConfiguration={subnets,securityGroups,assignPublicIp}, overrides=
  {containerOverrides:[{name:"WEB_COSMOSContainer", command:["python3","api_scraper.py","--source",
  "WEB_COSMOS","--collection",key,"--run-id",run_id,"--target",test|prod]}]})`.
  Local alternative: `<indexer-venv>/bin/python api_scraper.py --source WEB_COSMOS …` (exit mirrors state).
- Poll: `index_runs/{key}/{run_id}/status.json` (`state: succeeded|failed`, `error` reason codes,
  counts) and, for `--target test`, `validation.json` (`count_matches`, `title_match_rate`,
  `titles_mismatched`, …). Absence past a stall timeout = dead task (fallback `ecs.describe_tasks`).
- Doc id = `/SDE/{key}/|{url}`; version = sha256 of `[title, full_text, document_type, division]`.

## Architecture (one package `sde_curation/`, SQLite state, no build step)

```
sde_curation/
  config.py          pydantic-settings: paths to both repos/venvs, AWS env, LLM provider
  models.py          Pydantic: Collection, Status enum, Pattern, DumpUrl/DeltaUrl/CuratedUrl, JobRun,
                     ExportManifest, IndexStatus, ValidationReport, LLM suggestion schemas
  db.py              SQLite (aiosqlite) schema + bulk set ops; status_history table
  engine/            pure, testable core (no I/O)
    diff.py          dump vs curated → new/modified/deleted deltas
    patterns.py      include/exclude, title substitution, division, doc_type; specificity; unapply
    export.py        curated → documents.jsonl lines + manifest
  backends/
    scrape.py        ScrapeBackend protocol; LocalSubprocessScraper; SsmRemoteScraper
    index.py         IndexBackend protocol; LocalSubprocessIndexer; EcsDispatchIndexer
    s3.py            thin boto3 wrapper (put jsonl+manifest, get json, head)
  llm/
    base.py          LLMProvider protocol: complete(prompt, schema: type[BaseModel]) -> BaseModel
    openai.py        OpenAI structured outputs (response_format=json_schema) → pydantic validate
    tasks.py         suggest_patterns, suggest_metadata (title/division/doc_type) — return *_ai fields
  jobs.py            in-process JobManager: asyncio tasks, per-collection lock, event bus (SSE)
  web/
    app.py           FastAPI app, lifespan starts pollers; routes (JSON + HTMX partials)
    templates/       base.html, dashboard.html, collection.html, partials/*.html
    static/          htmx.min.js (vendored), tiny CSS
tests/               pytest (+ pytest-asyncio, moto for S3/SSM/ECS, httpx TestClient)
```

Key runtime rules
- Long work (scrape, index, LLM batches) runs in `JobManager` background tasks, never in handlers.
- Every async step records explicit `failed` state + error text (`job_runs` table) — visible on dashboard.
- Status machine: `Backlog → Scraped → Curating → Curated → Config Generated → Live` + `needs_recuration`;
  every transition appends `status_history` and emits an SSE event.
- State store: SQLite file (`data/engine.db`) + `collection.yaml`/`patterns.yaml` written per collection
  under `data/collections/<id>/` (git-trackable, mirrors workflow.md Option-B layout).
- Deps: `fastapi, uvicorn[standard], jinja2, pydantic>=2, pydantic-settings, aiosqlite, boto3, httpx,
  openai, pyyaml, sse-starlette`; dev: `pytest, pytest-asyncio, moto, ruff`.

## Phases (each independently verifiable)

### Phase 0 — Scaffold & config — DONE 2026-08-29
- `pyproject.toml` deps above; `sde_curation/config.py` (`Settings`: `CRAWLER_ROOT`, `CRAWLER_PYTHON`,
  `INDEXER_ROOT`, `INDEXER_PYTHON`, `SCRAPE_BACKEND=local|ssm`, `INDEX_BACKEND=local|ecs`, AWS
  region, `CRAWLER_S3_BUCKET`, `COSMOS_INDEX_BUCKET`, `INDEXING_*` (cluster, family, role ARN,
  subnets, SGs, container name), `LLM_PROVIDER=openai`, `OPENAI_API_KEY`, `OPENAI_MODEL`, `DATA_DIR`).
- `models.py` core enums/models; `db.py` schema + migrations-by-create-if-missing.
- `web/app.py` with `GET /health` → `{"ok": true, "db": "ok"}`.
- **Verify:** `uv run uvicorn sde_curation.web.app:app` → `curl /health`; `uv run pytest tests/test_models.py`
  (invalid status transition / bad seed URL rejected by Pydantic).

### Phase 1 — Collections + dashboard skeleton (R1, R7) — DONE 2026-08-29
- `POST /collections` (seed URL, name, division, connector) → `collection_id` (slug from apex host,
  same rule as `sde_crawler/job.py::collection_id_from_seed`), status `Backlog`, writes `collection.yaml`.
- `GET /` dashboard: table of collections (status, last job, counts, error badge); `GET /events` SSE;
  HTMX `hx-sse` swaps the row partial on `collection:<id>` events. `GET /collections/{id}` detail page
  with status history.
- **Verify:** create 2 collections via curl; open `/`; in a 2nd terminal `curl -N /events` while
  `POST /collections/{id}/status` → row updates in the browser without reload; `tests/test_api_collections.py`.

### Phase 2 — Scrape (R2, R6) — DONE 2026-08-29 (SSM path tested with moto only)
- `backends/scrape.py`: `ScrapeBackend.submit(collection) -> JobRun`, `.poll(job) -> JobStatus`.
  - `LocalSubprocessScraper`: write job JSON to `DATA_DIR/scrape_jobs/<id>.json`, spawn
    `CRAWLER_PYTHON run.py --job …` via `asyncio.create_subprocess_exec`; tail `logs/jobs/<stem>.log`
    for the heartbeat (`… N docs / M failed`) → progress events; completion by exit code; then ingest
    `output/collections/<id>.json` → bulk insert `dump_urls`.
  - `SsmRemoteScraper`: `ssm.send_command` (port of `scripts/drop_job.sh`), poll
    `head_object(scraped_collections/<id>.json)` (+ `_failures_summary.json`), download → ingest.
- `POST /collections/{id}/scrape` → job; status `Scraped` on success, `failed` job state on error.
- **Verify:** local: run against a tiny seed (e.g. `max_pages=10`) → dashboard shows live doc count,
  then `Scraped` and `dump_urls` count = documents in `output/collections/<id>.json`.
  Kill the subprocess mid-run → job shows `failed` with stderr tail. `tests/test_scrape_backend.py`
  uses a fake `run.py` fixture; SSM path tested with `moto` (command sent with expected inbox path).

### Phase 3 — Curation engine (R3, R4, R9) — DONE 2026-08-29 (+ fool-proofing review)
- `engine/diff.py`: set-based diff on `(url)` with field compare `title, division, doc_type` →
  `delta_urls` (`new|modified|deleted`), bulk `executemany`.
- `engine/patterns.py`: glob→regex; apply order exclude/include → title → division → doc_type;
  include beats exclude; "smallest match set wins, tie → longest pattern"; idempotent apply via
  `pattern_effects` table; unapply → next-most-specific → curated → NULL. Title substitution
  `{url} {title} {collection}` (xpath deferred).
- `POST /collections/{id}/diff`, `/patterns` CRUD, `/apply`; `POST /promote` → `curated_urls`, status `Curated`.
- Curation page: delta table (paginated, server-side, HTMX), pattern form, include/exclude toggles.
- **Verify:** `tests/test_patterns.py` encodes the spec cases (include-precedence, specificity,
  6-case unapply, idempotent re-apply); `tests/test_diff.py` (new/modified/deleted/tombstone);
  perf test: 100k synthetic URLs diff+apply < 5 s.

### Phase 4 — LLM assist (open provider, OpenAI first) — DONE 2026-08-29 (live OpenAI smoke passed with gpt-5.4-mini)
- `llm/base.py` `LLMProvider` protocol; `llm/openai.py` using structured outputs with the Pydantic
  schema (`PatternSuggestions`, `MetadataSuggestion`); provider chosen by `LLM_PROVIDER` (registry
  dict — adding Anthropic/Ollama = one file). Retries + timeout; results validated by Pydantic
  before touching the DB; stored as `*_ai`, never overwriting `*_manual`.
- `POST /collections/{id}/suggest/patterns` (sample of N URLs → drafted exclude/title/division
  patterns, shown for accept/reject), `POST /suggest/metadata` (batched per-URL title/division/doc_type).
- **Verify:** `tests/test_llm.py` with a `FakeProvider` returning canned JSON — malformed output raises
  `ValidationError` and job is marked failed, nothing written; one live smoke against OpenAI on
  ~20 URLs, suggestions appear in the UI with an "ml" badge.

### UI: Collection workbench — DONE 2026-08-29
COSMOS-inspired, simplified: one page per collection with a sticky header (status badge, ⚠, job chip,
count chips Dump/Deltas/Curated/Patterns, Next action) and tabs Overview · URLs (Dump/Deltas/Curated
sub-tabs, filters, paging, CSV, "why" tooltips from pattern_effects) · Patterns & AI · Activity.
Old `/curate` redirects to URLs › Deltas. Verified by tests (67) and browser walkthrough.

### Phase 5 — Export + test indexing (R5, R6) — DONE 2026-08-29 (moto-verified + live ECS run: 13 docs indexed into sde-web-subset in 27 s)
- `engine/export.py`: curated non-excluded rows → `documents.jsonl` lines
  `{url, title, full_text, document_type, division}`; manifest per contract; `full_text` joined
  from `dump_urls`. Writer streams to a temp file then `put_object` docs **then** manifest.
- `backends/index.py`: `IndexBackend.dispatch(key, run_id, target) -> JobRun`, `.status()`.
  `LocalSubprocessIndexer` (subprocess `api_scraper.py --source WEB_COSMOS …`, env from Settings);
  `EcsDispatchIndexer` (`sts.assume_role` → `ecs.run_task` exactly as above, store `taskArn`).
  Poller reads `status.json` every 30 s; stall timeout → `failed(stalled)` with `describe_tasks` detail.
- `POST /collections/{id}/index?target=test` → `run_id` minted (`YYYYmmddTHHMMSSZ-<short>`),
  status `Config Generated`.
- **Verify:** `tests/test_export.py` — round-trip through the indexer's own `web/web_processor.py::
  to_web_document` and `cosmos_source.load_manifest` (import from `INDEXER_ROOT` in tests) →
  no `ValueError`, ids match `make_web_id`; `moto` S3: manifest key written after documents key;
  `moto` ECS: `run_task` called with the correct family/container/command. Live: one small
  collection dispatched to dev, `status.json.state == "succeeded"` shown on dashboard.

### Phase 6 — Validation + prod (R7, R10) — DONE 2026-08-30 (direct AOSS validation with second-pass fallback; live fallback verified)
- Read `validation.json`; store `ValidationReport`; pass rule: `count_matches and title_match_rate >= threshold`
  (setting, default 0.99). Pass → button enables `POST /index?target=prod` → `Live`.
  Fail → `needs_recuration=True`, status back to `Curating`, mismatches listed on the page.
- Notification hook: `NOTIFY_WEBHOOK_URL` (Slack-compatible) posted on every transition (optional).
- **Verify:** `tests/test_validation.py` (pass/fail branches, flag set, history logged); live: prod
  dispatch on the same small collection → `Live`; `status_history` shows all 6 transitions.

### Phase 7 — Hardening
- Startup recovery: jobs marked `running` in DB but with no live task → re-poll (S3/ECS) or mark failed.
- Per-collection asyncio lock (no concurrent scrape/index on one collection); graceful shutdown.
- `ruff`, `pytest -q` in a `Makefile`/`justfile`; README with run instructions and env table.
- **Verify:** restart the server during a local scrape → job reconciled, dashboard consistent;
  full `pytest` green; end-to-end dry run on one seed through all six stages (workflow.md § Verification).

## Critical existing code to reuse (read-only, called as subprocess/contract)
- `../sde-crawl4ai-scraper-v1/run.py`, `sde_crawler/job.py` (job shape, `collection_id_from_seed`), `scripts/drop_job.sh` (SSM pattern)
- `../sde-api-scrapers/web/cosmos_source.py` (S3 layout), `web/web_processor.py` (doc/id contract),
  `web/validate.py` (report shape), `api_scraper.py::_run_web_cosmos` (CLI), `infrastructure/DEPLOYMENT.md:250` (run-task)

## Out of scope (per workflow.md)
TDAMM tagging, multi-user auth, xpath title extraction (stub only), feedback/EJ apps, Sinequa.
