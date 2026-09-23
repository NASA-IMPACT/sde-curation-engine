# Architecture and workflow

How the engine is put together, what happens at each stage of the curation pipeline, where jobs
live, and what it loads into memory. Describes the code at `3cb06e4` ("stream byte wise",
2026-09-22): schema V9, the streamed ingest with byte-capped chunks, connection recycling after the
ingest, and the streamed export — the ingest numbers confirmed on dev the same day.

Companion docs: `infra/README.md` (the deployed stack), `docs/deploy-dev.md` (deploys),
`docs/workflow.md` (the curator’s view).

---

## 1. The short answer on memory

**With 30 collections loaded, the dashboard holds 30 rows — not 30 collections' worth of URLs.**
The per-set counts shown on each row are denormalized integer columns on `collections`
(`dump_count`, `delta_count`, `curated_count`), maintained by the writers, so rendering the
dashboard never touches `dump_urls`, `delta_urls` or `curated_urls` at all.

What it does do is **one query per collection** for the latest job. Full profile of one dashboard
render (`dashboard_context`, `web/app.py:551`):

| Query | Count | Notes |
|---|---|---|
| `list_collections()` (`db.py:345`) | 1 | all 30 rows, `SELECT *` on a narrow table |
| `latest_runs("test")` + `latest_runs("prod")` (`db.py:1170`) | 2 | `DISTINCT ON (collection_id)` — deliberately batched for the chips |
| `active_jobs()` | 1 | `WHERE state IN ('queued','running')` |
| `list_recent_jobs(5, "failed")` | 1 | jobs strip |
| `list_collections()` again, in `jobs_context` (`web/app.py:627`) | 1 | duplicate of the first; the two contexts are merged but not shared |
| `latest_job(cid)` per shown collection, via `row_context` (`web/app.py:509`) | **30** | sequential `await` in a list comprehension |

So ~36 round trips, each an index lookup — `latest_job` is `ORDER BY id DESC LIMIT 1` served by
`job_runs_coll (collection_id, id DESC)`. It is an N+1, but a cheap one: the cost is 30 serialized
round-trip latencies (single-digit ms each on RDS in-VPC), not scanning. It would collapse to one
`DISTINCT ON` query in the same shape as `latest_runs`, which is the obvious fix if the dashboard
ever feels slow. Filtering, the left-pane counts and column sorting then happen in Python over
those 30 objects (`keep()`, `Counter`) — deliberately, because the pane shows the *unfiltered*
distribution.

**Everything the curator browses is paginated. The batch paths are the ones that load whole sets**,
and none of those carry page text: the three paths that move text in bulk (ingest, LLM, export) all
stream it.

### The four tiers of data access

| Tier | What | Memory |
|---|---|---|
| **Paginated** — `LIMIT`/`OFFSET` + a SQL `COUNT(*)` | `list_dump` (`db.py:593`), `list_curated` (`db.py:672`), `list_deltas`, `list_delta_ai`, `list_audit`, `list_jobs`, `list_index_runs`, `list_pattern_suggestions` | one page (default 100 rows). `list_dump` and `list_curated` return the page text's length (a scalar subquery on `page_text`) rather than the text itself |
| **Streamed, with text** | `iter_deltas_for_llm` (`db.py:1315`) — keyset chunks of 200, each its own transaction; `iter_curated_for_export` (`db.py:974`) — the URLs first, then pages of 500 by primary key, each its own transaction; the crawl ingest (`JobManager.ingest_dump`, `jobs.py:943`) — the S3 object parsed forwards by `iter_documents` (`backends/scrape.py:204`) in chunks of 500 pages or 1 MB of text, straight into a `COPY` | one chunk *with* full text. These are the only places page text is read or written in bulk, and each is bounded by its chunk, not by the collection |
| **Whole set, no text** | `load_dump` (`db.py:700`), `load_curated` (`db.py:966`, default), `load_deltas` (`db.py:757`), `list_patterns` (`db.py:1799`), `dump_urls`, `dump_content_hashes`, `title_keys`, `load_dump_failures` | proportional to URL count, not to crawl size. `load_dump` selects only `url, scraped_title, content_type, depth, content_hash`; `load_curated` uses `_CURATED_COLS`; the text lives only in `page_text` (schema V9) |
| **Whole set, with text** | `load_curated(with_text=True)` (`db.py:966`) — kept, but no production path calls it any more | proportional to **bytes of page text** |

### Why the batch paths load whole sets

Not an oversight — `recompute` is a pure function of the entire collection
(`curation.py:52` → `engine/diff.py:109`):

```
recompute(dump, curated, patterns, previous, failures, capped, division) -> DeltaSet
```

It cannot be paginated without changing semantics:

- **Field rules resolve newest-wins** across *all* patterns (`engine/patterns.py` header), so a
  partial pattern list can pick the wrong winner.
- **The diff pairs rows by canonical key across both full sets** — `new` means "in dump, not in
  curated", `deleted` means the reverse. A page of one side cannot tell you which.
- **Exclusions are decided by the rules alone**, never queued as deltas, so the excluded/included
  split is a function of the whole rule set.

This is the "recompute stays whole" constraint. The mitigation is that none of it carries page
text: it is URLs and short metadata.

### Rough memory, and how to measure it properly

For a 100K-URL collection, a `load_dump` row is ~5 short strings plus a 64-char hash. Counting
Python/pydantic object overhead rather than just the bytes, budget on the order of **0.5–1 KB per
row → ~50–100 MB per whole-set load**, and a recompute holds dump + curated + deltas + patterns at
once, so **~150–300 MB peak** at 100K URLs. Treat those as estimates from field widths, not
measurements — the honest way to get the real numbers is `tracemalloc.get_traced_memory()` around
`CurationService.recompute`, which is the technique the scale work used.

The text-carrying paths are a different order of magnitude entirely: a 6.7 GB crawl is 6.7 GB of
text regardless of URL count. That is why all three stream.

### The text paths

1. **Crawl ingest — streamed, chunk-bounded.**
   `_run_scrape` (`jobs.py:202`) hands `ingest_dump` a `DocumentSource` (`S3Documents` for a remote
   crawl, `FileDocuments` locally); nothing is downloaded. `iter_documents` parses the JSON array
   forwards with `ijson`, and `take()` turns it into `DumpUrl` rows off the event loop, a chunk at
   a time: **500 pages or 1 MB of page text, whichever comes first** (`_INGEST_BATCH_DOCS`,
   `_INGEST_BATCH_BYTES`). The rows go straight into the `COPY` that `replace_dump` (`db.py:477`)
   runs into a temp staging table; the duplicate-spelling pass and the two `INSERT … SELECT`s into
   `page_text` and `dump_urls` then happen in SQL. When the transaction commits, `ingest_dump`
   calls `Database.recycle_connections()` (`db.py:287`, psycopg_pool `drain()`) and glibc
   `malloc_trim(0)`.
   Why each piece is there, measured on an ascl.net-shaped crawl (2.8 GB, 45,024 pages, a band of
   ~1 MB pages), 2026-09-22:

   | | peak RSS | after the job | time |
   |---|---|---|---|
   | chunks of 500 pages only | 1,640–1,750 MB | 445 MB | 88–91 s |
   | + 1 MB byte cap | **337 MB** | 445 MB | **70 s** |
   | + connection recycle + `malloc_trim` | 337 MB | **86–90 MB** | 70 s |

   The byte cap bounds the chunk: a count alone let a run of 1 MB pages make a 500 MB chunk. Below
   1 MB the peak stops falling (~320 MB at 0.25 MB); that floor is not the chunk. The
   after-job figure was not Python objects (tracemalloc: 9 MB live) — ~57% was the buffers of the
   pooled connection that carried the `COPY`, which libpq keeps at their high-water mark for the
   connection's life, and ~35% freed heap glibc had not returned. On dev, before these changes,
   the real 6.7 GB ascl.net crawl peaked at ~5 GB (6.6 GB high-water mark) and left 2.8 GB
   resident; a 2 GB task was OOM-killed on it twice.

2. **Export — streamed.**
   `_run_index` (`jobs.py:680`) iterates `iter_curated_for_export` (`db.py:974`): the exportable
   URLs first, sorted in Python (code-point order, what `sorted()` gave before), then each page of
   500 rows with their approved text by primary key, in its own short transaction — a cursor held
   open across a slow file write would pin one of the pool's connections. Each page is serialised
   off the event loop into a `NamedTemporaryFile`, uploaded, then the manifest is written.
   `export_lines` still `sorted()`s, but only within a page that is already in order.

3. **LLM — streamed.** `iter_deltas_for_llm` reads `delta_urls → dump_urls → page_text` in keyset
   chunks of 200 and sends the whole page text, uncut.

Everything else is either paginated or metadata-only.

---

## 2. Process and component layout

One `uvicorn` process, one container, one task. No queue broker, no worker pool, no cache tier.

```
browser ──HTTPS──> CloudFront + WAF ──HTTP──> ALB ──> uvicorn (FastAPI)
                                                       │
   HTMX partials + SSE (/events) <─────────────────────┤
                                                       │
   ┌───────────────────────────────────────────────────┴──────────────────────┐
   │ create_app()  (web/app.py)                                              │
   │   app.state.db      Database          psycopg async pool (size 8)        │
   │   app.state.jobs    JobManager        asyncio.Task registry + locks      │
   │   app.state.bus     EventBus          in-process SSE fan-out             │
   │   app.state.curation CurationService  recompute / promote / rules        │
   │   app.state.scraper ScrapeBackend     local subprocess | ssm             │
   │                     IndexBackend      local subprocess | ecs            │
   │                     ProdPublisher     AOSS bulk writer                   │
   │                     LLMProvider       openai | fake                      │
   └──────────────────────────────────────────────────────────────────────────┘
        │              │               │              │            │
        ▼              ▼               ▼              ▼            ▼
   RDS Postgres   EFS /data      crawler EC2      indexer ECS    OpenAI
   (all state)    (yaml, logs)   (via SSM)        (RunTask)      (LLM)
                                      │                │
                                      ▼                ▼
                                 crawler S3        cosmos S3 ──> AOSS (sde-web)
```

| Module | Responsibility |
|---|---|
| `web/app.py` | routes, HTMX partials, SSE, auth, all request-shaped logic (1976 lines) |
| `db.py` | every SQL statement; one transaction per method; `Database` is the only DB surface |
| `schema.py` | numbered migrations (`MIGRATIONS`, 9 versions) applied at `connect()` |
| `jobs.py` | `JobManager`: the eight long-running job kinds, their progress and their failure handling |
| `curation.py` | `CurationService`: the glue between the pure engine and the DB (bulk only) |
| `engine/patterns.py` | rule resolution — **pure**, no I/O |
| `engine/diff.py` | dump-vs-curated diff and promote — **pure**, no I/O |
| `engine/urls.py` | URL canonicalization (`canonical_key`), the engine's notion of "same page" — **pure** |
| `engine/export.py` | the WEB_COSMOS export contract (jsonl + manifest) |
| `backends/scrape.py` | crawler drivers: `local` subprocess, `ssm` (drop a job on the crawler EC2 inbox, poll S3) |
| `backends/index.py` | indexer drivers: `local` subprocess, `ecs` (`RunTask` + poll S3 for `status.json`) |
| `backends/publish.py` | "Index to prod": AOSS bulk upsert/delete of the test run's vectors |
| `backends/validate.py` | direct AOSS read-back validation |
| `llm/pool.py` | bounded worker pool: N calls in flight, per-item failures, cancellation |
| `llm/tasks.py` | the prompts and response parsing |
| `events.py` | `EventBus`: one bounded queue per SSE subscriber, drops oldest when full |
| `store.py` | per-collection `collection.yaml` / `patterns.yaml` on EFS (git-trackable provenance) |

The three `engine/` modules are pure functions with no database and no I/O, which is why rule
semantics are testable and why "unapply a rule" is just "delete it and recompute".

---

## 3. Data model

Postgres, 13 tables (`schema.py`). The core of it is **three URL sets per collection**, named the
same way everywhere — tables, count columns, `?set=` query param, and UI labels:

| Set | Table | Meaning |
|---|---|---|
| **Dump** | `dump_urls` | what the crawler found, one row per canonical page, carrying `content_hash` — the key of its page text in `page_text`. Replaced wholesale by each crawl |
| **Delta** | `delta_urls` | the review queue: what would change in the curated set if promoted now. `new` / `modified` / `deleted`. Carries the AI's suggestions and their confidence |
| **Curated** | `curated_urls` | what the index gets. Carries the `content_hash` it was promoted with, so an export never depends on the dump still holding that crawl — the text it names is kept in `page_text` until no row points at it |
| **Page text** | `page_text` | the page text itself, once per `(collection, content_hash)` (schema V9). The dump and the curated rows approved from it share one copy; `Database._gc_page_text` drops a blob in the transaction that removes its last reference |

Supporting tables: `patterns` (the rules), `pattern_effects` (which rule decided which field on
which URL — so the UI can say *why*), `pattern_suggestions` (AI exclude proposals awaiting a
decision), `dump_failures` (URLs the crawler tried and could not fetch), `index_runs`, `job_runs`,
`collections`, `status_history`, `users`, `audit_log` (append-only, no FK, survives collection
deletion).

**`canonical_key`** (`engine/urls.py`) is host + path + query, lower-cased, no scheme, no `www.`,
no trailing slash, no fragment. It is the identity of a page throughout: the dump keeps one row per
key, the diff pairs rows by it, and an exact-URL rule matches every spelling of its page — so a
per-URL edit survives the page moving to https.

---

## 4. The pipeline

Six statuses, with two sub-stages inside `curating` (`models.py:26`, `:46`):

```
backlog ──scrape──> scraped ──start curating──> curating ──promote──> curated
                                                 │  exclusions            │
                                                 │  metadata              │ Index to test
                                                 ▼                        ▼
                                          (recompute loop)        config_generated
                                                                          │ Index to prod
                                                                          ▼
                                                                        live
```

### 1. `backlog` — a collection exists
Name, seed URL, connector, `max_pages`, division (nullable, resolved at recompute — there is no
`*` rule for it). `collection_id` is the apex host.

### 2. `scraped` — the crawl is in the dump
`start_scrape` (`jobs.py:186`) → `ScrapeBackend.run` (or `fetch_existing` to load an existing
crawl) → a `DocumentSource` pointing at the crawl → `ingest_dump`, which streams it (section 1,
*The text paths*).

Duplicate spellings collapse here: `duplicate_docs` (`engine/urls.py:60`) decides, from each
document's `url` and `final_url` read back from the staging table, which spelling of a page
survives — the one whose own URL is the resolved page, then https, then the shorter string. The
job records how many went (`progress.duplicates_dropped`) and says so in the job line and the
history note — "45,024 read, 22,700 duplicate links dropped" for ascl.net, whose crawl reaches
every page over both `http://` and `https://www.` links. `replace_dump` deletes the previous dump
and `COPY`s the new one **in one transaction**, so a failed ingest leaves the old dump intact; its
page text goes to `page_text` once per content hash.

A re-crawl of a collection that already has curated rows raises `needs_recuration` with a reason,
and wipes the deltas (computed against the old dump, now meaningless).

### 3. `curating` — the recompute loop
This is where curators spend their time, and it is one idempotent operation run over and over:

```
recompute = diff(dump, curated) + resolve(patterns) → delta_urls + pattern_effects
```

Triggered by every rule change, every per-URL edit, every accepted AI suggestion, and by "Start
curating". Serialised per collection so two recomputes never interleave their delete+insert on
`delta_urls` (`curation.py:45`).

Two sub-stages, tracked in `collections.curation_stage`:

- **`exclusions`** — decide what is *in*. Exclude rules keep URLs out; an excluded URL gets **no
  delta row** (the rule decides it, there is no delta to review). "Suggest patterns" proposes
  exclude globs.
- **`metadata`** — title, division, document type for everything that stays. All three are
  **required**: promote refuses blanks (`IncompleteMetadata`, `curation.py:23`).

### 4. `curated` — promoted
`promote` (`curation.py:218`) applies the pure `promote()` to the delta queue, writes the curated
set whole, gives the promoted rows the dump's content hash — which is what names their text in
`page_text`; nothing is copied — keeps the rule→URL effects so the Curated table can still explain
itself, and clears `needs_recuration`. `promote_urls` (`curation.py:245`) does the same for a
picked subset, leaving the rest of the queue alone — and only the picked rows take the dump's
current hash, so a row still under review cannot silently acquire text its metadata was never
approved with.

### 5. `config_generated` — indexed to test and validated
`start_index` → export → dispatch → poll → **validation gate** (section 6).

### 6. `live` — published to prod
"Index to prod" republishes the *vectors of the latest validated test run* straight into the
production index — no re-export, no indexer task, no re-vectorizing — then runs the same validation
gate against prod. Only a pass makes the collection `live`; a failure sends it back to
`config_generated`, flagged.

---

## 5. Where jobs live

**Control state in Postgres; execution in memory. There is no resume.**

| Aspect | Where |
|---|---|
| Job record (kind, state, progress, error, actor, external ref) | `job_runs` table, updated as the job runs |
| The running job itself | an `asyncio.Task` in `JobManager._tasks: dict[int, asyncio.Task]` (`jobs.py:123`) |
| Mutual exclusion | `JobManager._locks: dict[str, asyncio.Lock]`, one per collection (`jobs.py:91`) |
| TOCTOU guard on start | `JobManager._starting: set[str]` |
| Who asked for a cancel | `JobManager._cancel_actor: dict[int, str]` |
| Progress delivery | `EventBus` → SSE, plus the UI's 5–10 s polls |

Three consequences:

1. **The service is pinned to one task.** `desired_count=1` in the CDK stack, and `README.md`
   says plainly: *do not run two replicas*. Two processes would each have their own registry and
   their own locks, so nothing would stop two recomputes interleaving on one collection.
2. **A restart kills every running job.** `recover()` (`jobs.py:147`) runs at startup and marks
   anything still `queued`/`running` as `failed` with *"engine restarted while job was running"* —
   deliberately explicit rather than leaving a zombie. Since a deploy replaces the task
   (`minHealthyPercent=0`), **deploy when idle**.
3. **Cancel is cooperative.** `cancel()` finds the task, calls `Task.cancel()`, and waits for the
   job to record `failed` before returning, so a caller that sees the job list never sees a
   half-cancelled job.

### The eight job kinds

| Kind | What it does | Bounded by |
|---|---|---|
| `scrape` | crawler run (or load an existing crawl) → ingest dump | one crawl at a time on the shared crawler EC2 |
| `llm_patterns` | exclude-glob suggestions over included delta URLs | `LLM_PATTERN_BATCH_URLS` (1000) URLs per call |
| `llm_metadata` | title / division / document type per URL, from full page text | `LLM_WORKERS` (16) concurrent calls |
| `llm_titles` | retitle duplicate (title, doc type) groups | `LLM_TITLE_GROUP_CHARS` |
| `index_test` | export → S3 → dispatch indexer → poll → validate | one ECS task per run |
| `index_prod` | republish the validated test run's vectors to prod AOSS | `PUBLISH_BULK_DOCS` / `PUBLISH_BULK_MAX_BYTES` |
| `validate` / `validate_prod` | re-run the validation gate against an existing run | `VALIDATION_TIMEOUT_S` |

Every one runs inside `_guarded` (`jobs.py:266`): take the collection lock, run the body, record
`succeeded`; on `CancelledError` record `failed` with who cancelled and re-raise; on a known error
class record `failed` with the message; on anything else log the traceback and record
`failed`. A job's *effects* are always committed before its record says `succeeded` — so whoever
polls the job list and sees success can rely on the state being visible.

---

## 6. Component mechanics

### The rules engine (`engine/patterns.py`)
Five rule types: `exclude`, `include`, `title`, `division`, `document_type`. Resolution is a pure
function of `(urls, patterns, curated, division_default)`:

- **exclude/include**: a URL is excluded iff some exclude matches **and** no include matches.
  These are *not* age-ranked — an include is an explicit exception and keeps winning however old
  it is, so a later exclude glob cannot silently undo a batch of force-includes.
- **field rules** (title/division/document_type): **the newest matching rule wins** (highest id).
  Specificity plays no part. A hand-typed glob therefore reaches every URL it matches, including
  ones an earlier accepted suggestion had set; accepting a suggestion later overrides the glob on
  that one URL again.
- **effective value** = winning rule, else the curated value, else NULL — except division, where a
  collection-wide default sits between the two, so changing the collection's division reaches rows
  that are already curated.
- Title values are templates: `{url}`, `{title}`, `{collection}`.
- An exact-URL rule matches by canonical key, so it covers every spelling of the page.

`pattern_effects` records which rule decided which field on which URL, which is how the Rules
table can show `matches` (how many URLs a rule matches) separately from `in_effect` (how many it
currently decides — 0 with matches > 0 means a newer rule superseded it).

### The diff (`engine/diff.py`)
- `new` — in dump, not in curated
- `modified` — in both, and the scraped title, an effective curation value, or the page text
  changed (via `content_hash`); or the same page under a new spelling, carrying `renamed_from`
- `deleted` — in curated, not in dump, **and the crawl is evidence it is gone** (http 404/410, or
  a complete crawl never met it). A URL the crawler *tried and failed* to fetch stays in the
  curated set with `crawl_failure` set; when the crawl hit its page cap, every unmet curated URL
  stays as `not_visited` — an incomplete crawl proves nothing.
- Excluded URLs produce no delta. A curated row a rule newly excludes is flagged in place; only the
  way back *in* is a reviewable `modified` delta.

### The LLM subsystem (`llm/`)
`run_pool` (`llm/pool.py`) is a bounded asyncio pool: `LLM_WORKERS` calls in flight, per-item
failures counted rather than fatal, progress throttled, clean cancellation. Per-URL failures are
recorded on the delta row (schema V4) so they are visible per URL and "Retry blanks" can re-run
just those. There is **no limiter across jobs** — five curators classifying at once is up to 80
in-flight calls, which is the first place the provider's rate limit bites.

Metadata answers are flushed to the DB in small chunks (`AI_FLUSH_ROWS = 25`,
`AI_FLUSH_SECONDS = 2.0`) so a cancel keeps what was already answered while commits stay few.

### Indexing and the validation gate
`_run_index` (`jobs.py:614`), in order:
1. **Pin the index key** so a later rename cannot move the collection to a second index.
2. **Export**: `load_curated(with_text=True)` → `write_jsonl` → temp file → S3
   `curated_collections/{key}/{run_id}/documents.jsonl`, **then** `manifest.json` last — the
   manifest's existence is what means "export complete".
3. **Dispatch** the WEB_COSMOS indexer (`ecs:RunTask`, or a local subprocess).
4. **Poll** S3 for `status.json` (written last and unconditionally by the indexer).
5. **Validate** — and this is the interesting part. AOSS makes a bulk upsert searchable some
   unpredictable time after the indexer reports success, so the indexer's own `validation.json` is
   pre-refresh and cannot be trusted. `_validate` (`jobs.py:773`) sleeps `VALIDATION_DELAY_S`,
   then reads the index back directly and re-checks until the counts and titles pass the threshold
   (`VALIDATION_TITLE_MATCH_THRESHOLD`, 0.99) or `VALIDATION_TIMEOUT_S` elapses. On a 403 it falls
   back to dispatching a second indexer pass purely to get a fresh `validation.json`.
   A failed index is **not** a curation problem: it never raises `needs_recuration`; the UI's
   "needs re-indexing" chip is derived from the run, and only a later passing run clears it.

### SSE (`events.py`)
One bounded queue (256) per subscriber. `publish` is non-blocking: a stalled browser gets its
oldest event dropped rather than blocking the pipeline. The bus is in-process, which is a fourth
reason the service cannot be replicated as-is. CloudFront is configured not to buffer or compress
the stream, the ALB idle timeout is 3600 s, and uvicorn's keep-alive is 3620 s to outlast it.

---

## 7. Where state lives

| Store | Contents | Survives task replacement |
|---|---|---|
| **RDS Postgres** | everything that matters: all 14 tables | yes (snapshots + PITR) |
| **EFS `/data`** | `collections/<id>/{collection,patterns}.yaml` (git-trackable provenance), index logs and scrape job files. **Not** the crawl: a remote scrape is streamed from S3 straight into the ingest and never written down here | yes |
| **S3 (crawler bucket)** | the crawler's documents + failure logs | yes |
| **S3 (cosmos bucket)** | exports (`curated_collections/`), run status (`index_runs/`), vectors (`vectorized/`) | yes |
| **AOSS** | the `sde-web` index itself, test and prod | yes |
| **Container temp dir** | the export jsonl (`tempfile.NamedTemporaryFile`, deleted in a `finally`) | no — transient by design |
| **Process memory** | job registry, per-collection locks, SSE queues | **no** |

The last row is the whole of the singleton constraint. Moving it into Postgres is what would let
the service run more than one task, make deploys non-destructive, and let a job survive a restart.

---

## 8. Concurrency and limits

| Dimension | Limit | Where |
|---|---|---|
| Engine tasks | 1, by design | `desired_count=1` |
| Jobs per collection | 1 (lock + active check + `_starting` guard) | `jobs.py:231` |
| Jobs across collections | unlimited | — |
| Crawls | serialized on the shared crawler EC2; the engine accepts them in parallel and they queue there | `backends/scrape.py` |
| LLM calls within a job | `LLM_WORKERS` = 16 | `llm/pool.py` |
| LLM calls across jobs | **unbounded** — the first rate-limit risk | see `README.md` guidance |
| Index runs | unlimited; each dispatches its own ECS task | `backends/index.py` |
| DB connections | `DB_POOL_SIZE` = 16 per process (`config.py:35`; `Database`'s own default of 8 applies only when it is built without settings); replaced wholesale after each crawl ingest (`recycle_connections`) | `config.py:35`, `db.py:248` |
| AOSS bulk request | 100 docs / 8 MB (AOSS caps at 10 MiB) | `config.py:82-83` |
| Curation edits | **no stale-edit check** — two curators on one collection, last save wins silently | `README.md` |

The intended operating model is one curator per collection; the app is built around that and warns
rather than locks.

---

## 9. Scale characteristics

What grows with what:

| Quantity | Cost |
|---|---|
| Number of collections (30, 300) | dashboard round trips (N+1 on `latest_job`); nothing else |
| URLs in a collection | whole-set loads in recompute/promote — URL count × ~0.5–1 KB |
| Bytes of page text | storage (`page_text`, once per hash) and time (~24 s/GB ingest on a laptop); memory only up to one chunk — the ingest, export and LLM paths all stream |
| Concurrent curators | LLM in-flight calls (unbounded across jobs); DB pool at 16 |
| Job history | `job_runs` / `audit_log` growth; both indexed and read with `LIMIT` |

### Known risks, in priority order

1. **The ingest holds a transaction open for the length of the S3 read.** A crawl is streamed
   straight into the `COPY` that fills the staging table, so one pool connection (of
   `DB_POOL_SIZE`) is held for as long as the object takes to read, and a broken stream fails the
   scrape job. The crawl stays in S3, so a re-run is the recovery.
2. **The ingest peak has a ~270 MB floor that is not the chunk.** Most likely libpq's send buffer
   growing while rows are produced faster than PostgreSQL takes them (psycopg does not flush per
   row on Linux). Only pacing the `COPY` itself would lower it; not worth it at 340 MB.
3. **The in-process job registry** blocks replication, makes deploys destructive, and loses
   long jobs to any task replacement.
4. **No cross-job LLM limiter** — concurrent curators can exceed the provider's rate limit.
5. **Migrations run at boot in the serving process**, so a slow `ALTER` on a large table can
   outrun the 120 s ALB health-check grace period and loop. V9 took ~21 s from container start
   to serving on dev; it is the first migration where this could bite.
6. **No stale-edit detection** on curation edits.
7. **Only the app can turn a crawl into rows.** `aws_s3.table_import_from_s3` would let RDS read
   the object itself, but it takes COPY formats only — the crawler would have to stop emitting a
   JSON array, and duplicate-spelling detection and hashing would have to move into SQL.
