# Scale audit — 4 curators × 100K-URL collections

*2026-09-18 · branch `redesigned-curation` @ 5258407 (the dashboard-polling fix; measured on the same code before it was committed)*

## 1. Verdict

The engine as deployed on test (1 vCPU / 2 GB Fargate task, one Python process) **cannot carry four
curators working on 100K-URL collections, and cannot carry one.** Three things break, in this order:

1. **The task is killed for memory.** One 100K collection peaks at 3.5–3.7 GB, four at 6.5 GB. The
   task has 2 GB. An OOM kill restarts the engine for everyone and fails every running job.
2. **The task is killed by its own health check.** CPU work runs on the single event loop; while it
   runs nothing else is served. Measured freezes: 29 s (one curator), 60 s (four). The ALB marks the
   task unhealthy after two missed `/health` checks 30 s apart (≈ 35 s of freeze) and ECS replaces it.
3. **Requests outlive CloudFront.** CloudFront drops a request after 60 s (504). With four curators,
   bulk-accept took 266–304 s, every rule change / inline edit 163–236 s, the recompute after a
   re-crawl 128–196 s — on a laptop core that is faster than the task's vCPU.

None of this is caused by the *design* (rules as the single source of truth, newest rule wins,
promote copying text inside PostgreSQL — those hold up; promote of 100K URLs takes 3 s). It is caused
by **seven implementation details that do O(collection) or O(rules) work where O(1) or O(change) is
enough**, all of them cheap to fix and none changing what the app does. After the code changes in
§5 the expected cost of an inline edit at 100K URLs drops from ~56 s to a few seconds; the resource
changes in §6 are then modest.

What already scales and needs nothing: promote (3–5 s), the index export's S3 side, URL tables
(paged, < 1 s), delta search, dashboard, the LLM metadata job's own bookkeeping (streams rows, flushes
incrementally), the database (RDS is not the bottleneck anywhere).

## 2. What was tested

| | |
|---|---|
| Runs | **A** 4 curators × 100K URLs, concurrent, unprofiled (24 min of compute). **B** 1 curator × 100K, unprofiled. **C** 1 curator × 100K under cProfile per step (times ≈ 2–2.5× slower; used only for *where* time goes). Earlier: 4 × 20K. |
| Data | 100,000 URLs per collection in 50 sections, 3,000 characters of page text each (312 MB crawl JSON per collection). Re-crawl changes the text of every 10th page. |
| Flow per curator | scrape + ingest → Start curating (recompute) → add exclude glob → add title glob `*` → Suggest exclusions → bulk-accept them → Suggest metadata (1 call/URL) → inline edit → **bulk-accept AI metadata (300K per-URL rules)** → inline title edit → inline exclude → add a glob → recompute → promote → index to test → re-crawl → recompute; every page/tab/API loaded at three points. |
| How | The real FastAPI app in-process, driven through its HTTP API (no DB shortcuts), against PostgreSQL 17 in Docker (7.75 GB VM — close to the RDS `m6i.large`'s 8 GB). Fake crawler (writes the documents file), fake LLM (instant answers), moto S3 + fake indexer. |
| Measured | **wall** time per step; **stall** = longest gap between `/health` replies during the step (how long the event loop was blocked = how long every other user, poll and SSE stream froze); **RSS** sampled each second; PostgreSQL CPU/memory every 5 s; table and file sizes; connection-pool statistics. |

**What this does not measure.** (a) Real OpenAI latency — the fake answers instantly, so LLM job
*duration* is estimated in §6.5, not measured. (b) The real indexer and OpenSearch Serverless — the
fake indexer only reads the export. (c) The 1-vCPU x86 Fargate task and RDS over the network: the
laptop (Apple M-series core, local socket to Postgres) is faster, so **every number here is a lower
bound** for test. I did not measure the ratio; treat anything over ~25 s locally as over 60 s deployed.

## 3. What is deployed on test (account 119417011911, us-east-1; read from the live resources)

| Component | Size | Week of CloudWatch (to 2026-09-18) |
|---|---|---|
| Engine: ECS Fargate, 1 task, x86, one uvicorn process (HTTP + SSE + jobs + LLM workers) | 1 vCPU / 2 GB | CPU max 68 %, avg 1 %; memory max 36.5 % (~750 MB). Task stops were deployments, no OOM yet — today's collections are far smaller than 100K |
| RDS PostgreSQL 17.9 `db.m6i.large`, single-AZ | 2 vCPU / 8 GB, 20 GB gp3 → 100 GB autoscale | CPU max 37 %, avg 3 %; 5.0 GB freeable memory; 17.5 GB free; ≤ 8 connections |
| EFS (YAML, logs), elastic throughput | ~1 GB used | |
| CloudFront → WAF → ALB | origin read timeout 60 s; WAF 1000 req / 5 min / IP; ALB health check `/health` every 30 s, timeout 5 s, unhealthy after 2 | |
| Crawler EC2 `m6i.2xlarge` | 8 vCPU / 32 GB, 100 GB gp3 | |
| Indexer Fargate task (`web_cosmos-scraper-test`) | 2 vCPU / 8 GB per run | |
| OpenSearch Serverless `sde-search-test` | account cap 10 indexing + 10 search OCU | |
| LLM | `gpt-5.6-luna`, 16 workers per job, 1000 URLs per exclusion batch | |

## 4. Results

### 4.1 Mutating steps (wall seconds; **bold** = over CloudFront's 60 s)

| Step | kind | 1 curator (run B) | 4 curators (run A, min–max) | worst stall (A) |
|---|---|---|---|---|
| scrape + ingest dump | job | 3.9 | 11–13 | 3.4 s |
| recompute (Start curating) | request | 1.6 | 5–7 | 0.3 s |
| add exclude glob | request | 2.6 | 10–13 | 1.5 s |
| add title glob `*` (touches every row) | request | 7.9 | 31–**79** | 2.1 s |
| Suggest exclusions (fake LLM) | job | 16.2 | 45–71 | 2.1 s |
| bulk-accept pattern suggestions | request | 7.8 | 27–59 | 1.3 s |
| Suggest metadata (fake LLM, 100K calls) | job | 13.8 | 43–58 | 1.0 s |
| **bulk-accept AI metadata** (300K rules) | request | **76** (stall 29 s) | **266–304** | **60 s** |
| inline title edit, 300K rules present | request | ≈ 56 ¹ | — ² | |
| add a glob, 300K rules present | request | 56 | **163–236** | 31 s |
| recompute, 300K rules present | request | 56 | **193–224** | 30 s |
| promote | request | 3.0 | 5–28 | 5 s |
| index to test (export + validate) | job | 6.9 | 17–29 | 1.7 s |
| re-crawl (ingest) | job | 4.0 | 9–25 | 0.1 s |
| recompute after re-crawl | request | 54 | **128–196** | 28 s |

¹ Measured only under the profiler (157 s title edit, 170 s exclude); it is the same code path as
"add a glob" (recompute + YAML), whose unprofiled time is 56 s. Before the bulk-accept the same
inline edit took 18 s profiled (≈ 8 s). ² Added to the script after run A had started.

### 4.2 Pages (4 curators, after the bulk-accept)

| Page | time | size | note |
|---|---|---|---|
| Rules tab | 19–31 s | **215 MB HTML** | renders all 300K rules; unusable in a browser |
| `GET /api/…/patterns` | 12–33 s | 71 MB JSON | same data |
| collection page | 8–**96** s | 18 KB | 2.4 s of its own work; the rest is waiting for the frozen event loop |
| Curate tab | 4–35 s | 20 KB | same |
| delta / dump / curated tabs, search, last page, dashboard, dashboard row | < 1 s own work | ≤ 230 KB | fine; up to 17 s when another curator's step froze the loop |

### 4.3 Memory, database, files

| | 1 curator | 4 curators |
|---|---|---|
| Peak RSS of the engine process | 3.5–3.7 GB | **6.5 GB** |
| … after ingest alone | 1.2 GB | 2.6 GB |
| … during bulk-accept | 2.7–2.8 GB | 6.3 GB |
| … during index export | 2.85 GB (+1.5 GB for the step) | 5.4 GB |
| PostgreSQL container | — | CPU median 9 %, p90 108 % (one core), max 377 %; memory ≤ 485 MB |
| Database size | 0.33 GB | 1.26 GB: `pattern_effects` 392 MB (1.2 M rows), `patterns` 341 MB (1.18 M rows), `dump_urls` 228 MB, `curated_urls` 226 MB, `delta_urls` 61 MB |
| `patterns.yaml` per collection | 51 MB | 4 × 51 MB, rewritten whole on every rule change |
| Connection pool (8) | — | 26,014 requests, 18,569 queued, 1,018 s total wait (≈ 55 ms per queued request; ~14K of the requests are the harness's own `/health` pings) |

The synthetic text is repetitive and compresses far better in TOAST than real pages; real
`dump_urls`/`curated_urls` will be several times larger per URL. The rule tables will not.

### 4.4 Where the time goes (run C, profiled; share of the step)

**Any rule change with 300K per-URL rules present** (inline edit, add/delete rule, recompute,
recompute after re-crawl — 138–159 s profiled, 54–56 s real):

| Part | time | share | code |
|---|---|---|---|
| Re-serialising *every* rule to `patterns.yaml` with pure-Python PyYAML, on the event loop | 82–83 s | 55–60 % | `store.py:31 write_patterns_yaml` ← `app.py:1195 _after_curation_change` |
| Deleting and re-inserting every delta row and every rule→URL effect row | 29–30 s | 20 % | `db.py:609 replace_deltas` (effects go in by `executemany`, not `COPY`) |
| Compiling a regex for each of 293,806 exact-URL rules that are matched by dict lookup and never use it | 16–19 s | 12 % | `patterns.py:87 compile_patterns → glob_to_regex` |
| `canonical_key` (`urlsplit`) recomputed for every URL and rule each time | 5–7 s | 4 % | `urls.py:15` |
| Loading 300K `Pattern` models | 2 s | 1 % | `db.py:1268 list_patterns` |
| The actual diff + rule resolution | ~1 s | < 1 % | `engine/diff.py`, `resolve_all` |

**Bulk-accept AI metadata** (174 s profiled): YAML 83 s (48 %), inserting 300K rules by
`executemany` 29 s (17 %, `db.py:1246`), `replace_deltas` 30 s (17 %), regex compile 16 s (9 %).

**A rule change *before* any per-URL rules exist** (title glob `*`, inline edit: 13–14 s profiled):
`replace_deltas` is 75 % — 100K effect rows by `executemany` plus 100K delta rows re-copied, to
change one row.

**Suggest exclusions** (64 s profiled, 16 s real, all on the event loop): 96 % is
`match_counts(kept, all_urls)` called once per batch (100 times, `jobs.py:317 on_result`), each call
re-deriving `canonical_key` for all 100K URLs — 10.1 M `urlsplit` calls for a number that needs them
zero times (suggestions are globs).

**Rules tab / `/patterns`** (33 s / 30 s profiled): regex compile for exact rules 16 s, Jinja
rendering 300K rows 9.5 s, `jsonable_encoder` over 300K dicts.

**Collection page, header, stepper** (`app.py:947 step_context`): loads all 300K `Pattern` models
to take `len()` (`app.py:976`) and the whole curated set (100K models) to count `crawl_failure` —
~2 s of event-loop CPU per call, and the header polls it every 4 s and the stepper every 5 s, per
open tab.

**Index export** (`jobs.py:518`): `load_curated(with_text=True)` materialises all 100K rows with
their text as Pydantic models (+1.5 GB) before "streaming" them to the temp file.

**Dump ingest** (`scrape.py:143`, `jobs.py:763`): `read_text` → `json.loads` → a `DumpUrl` per
document → COPY: three copies of the crawl's text in memory at once (312 MB file → 1.2 GB).

## 5. Code changes, in order of effect

Each is independent. "Effect" is what the measurements above say the change removes; none has been
implemented or re-measured yet.

### C1. Stop rewriting `patterns.yaml` on every rule change  — *removes 55–60 % of every edit*
**Change.** In `_after_curation_change` (`app.py:1230`) stop calling `write_patterns_yaml` inline.
Either (a) write it only on promote (and on demand from an "export rules" link), or (b) keep writing
it but debounced, in `asyncio.to_thread`, with `yaml.CSafeDumper`, and with per-URL rules split into
an append-only `url_rules.jsonl` so a single edit appends one line.
**Justification.** 82 of 138–159 profiled seconds of every inline edit, glob, recompute and
re-crawl recompute is PyYAML emitting 300K rules (4.4 M `serialize_node` calls), all of it on the
event loop — it is the largest single cause of both the 504s and the 28–83 s freezes. The file is a
snapshot of what the database already holds; nothing reads it back on the request path. It is also
51 MB written to EFS per edit. (a) is the smaller change and removes the cost entirely; (b) keeps
the file always current.

### C2. Make `replace_deltas` incremental and bulk  — *removes ~20 % of every edit; 75 % before bulk-accept*
**Change.** `db.py:609`: (1) load `pattern_effects` with `COPY` like the deltas instead of
`executemany` (300K parameterised INSERTs); (2) better, write only what changed: COPY the new state
into a temp table and `DELETE … WHERE NOT EXISTS` / `INSERT … ON CONFLICT DO UPDATE … WHERE row IS
DISTINCT` — the pattern `replace_curated` already uses.
**Justification.** An inline edit changes one delta row and one effect row, and today rewrites
100K + 300K rows (29–30 s profiled at 100K; PostgreSQL pegged at 100 % of a core during run A). It
is also why `pattern_effects` + `patterns` (733 MB) outweigh the page text in the database, and why
the dead-tuple churn will keep autovacuum busy on RDS.

### C3. Do not compile a regex for exact-URL rules  — *removes ~12 % of every edit and half of the Rules tab*
**Change.** `patterns.py:87`: build `glob_to_regex(p.match)` only when `not is_exact(p.match)`
(make `Compiled.regex` optional). Three lines.
**Justification.** 293,806 `re.compile` calls = 16–19 s per recompute and per Rules-tab load; the
docstring of `compile_patterns` already says exact rules are a dict lookup. This is the "exact
fast path" decided on 2026-09-10 not being fast because of one unconditional line.

### C4. Move CPU work off the event loop  — *removes the freezes and the health-check kills*
**Change.** Run the pure functions — `engine.diff.recompute`, `resolve_all`, `match_counts`,
`promote`, YAML/JSON serialisation, `parse_documents` — through `asyncio.to_thread` (no shared state:
they are pure by design). For true parallelism between curators use a small
`ProcessPoolExecutor` for `recompute`.
**Justification.** Stall = every other curator's page, every poll and every SSE stream frozen:
29 s with one curator, 60 s with four, 6–11 s even at 20K URLs. Two consecutive ALB health checks
(30 s apart, 5 s timeout) landing in a freeze make ECS kill the task, which fails all running jobs
("engine restarted while job was running"). `to_thread` does not make the work faster (GIL) but
lets the loop answer `/health`, polls and SSE between slices; the process pool additionally lets
four curators use four cores. C1–C3 shrink the work; C4 makes what remains harmless to others.

### C5. Turn the long requests into background jobs  — *removes the 504s*
**Change.** `ai/bulk`, `suggestions/bulk`, `recompute`, and rule add/delete on large collections
go through `JobManager` like scrape / LLM / index: return 202, show progress in the jobs panel,
finish with an SSE event. A threshold (e.g. > 20K delta URLs) can keep small collections synchronous.
**Justification.** CloudFront gives a request 60 s. Bulk-accept is 76 s alone and 266–304 s with four
curators; the recompute after a re-crawl 54 s / 128–196 s. After C1–C3 these shrink by an estimated
85–90 %, but bulk-accept still inserts 300K rows and a 1-vCPU task under four curators has no
margin. A 504 is also the worst failure mode here: the server keeps working, the curator sees an
error page and clicks again.

### C6. Bulk-insert accepted rules with COPY  — *removes 17 % of bulk-accept*
**Change.** `db.py:1246 insert_patterns`: COPY into a temp table, then one
`INSERT … SELECT … ON CONFLICT DO NOTHING`. Same for `delete_exact_patterns` + `_delete_other_spellings`
(`curation.py:107` loads every rule and deletes matches one statement at a time — make it one
`DELETE … WHERE canonical key = ANY(…)`, which needs the canonical key stored on the rule row).
**Justification.** 29 s of the profiled bulk-accept is 300K `executemany` INSERTs; over the network
to RDS it will be worse than over a local socket.

### C7. Count in SQL on the polled page contexts  — *removes ~2 s of loop time per poll per open tab*
**Change.** `app.py:976`: `SELECT count(*) FROM patterns WHERE collection_id=…` instead of
`len(await d.list_patterns(…))`; `app.py:959`: `SELECT count(*) … WHERE crawl_failure IS NOT NULL`
instead of `load_curated`. Likewise `app.py:1268/1289/1637` look up one rule by loading all of them
— query by `(collection_id, type, match)`.
**Justification.** `step_context` feeds the collection header (polled every 4 s during a job) and
the stepper (every 5 s). At 100K it builds 300K + 100K Pydantic models per call: 2.4 s measured for
the collection page, of which 1.9 s is `list_patterns`. Four curators with a collection tab open
spend most of the event loop on polls that render a number.

### C8. Page the Rules tab and `/patterns`; summarise per-URL rules
**Change.** Show glob rules in full and per-URL rules as one summary line per source and field
("AI-accepted titles × 99,960 → open in Curated URLs"), with a paged, searchable list behind it
(limit/offset like the URL tables). Compute match counts only for globs; an exact rule's count is
1 by construction.
**Justification.** 215 MB of HTML / 71 MB of JSON, 19–33 s, for a table nobody can scroll. The
counts for exact rules cost 16 s of regex compiles (C3) to report "1" 300K times.

### C9. Compute `canonical_key` once
**Change.** `functools.lru_cache` on `canonical_key` (`urls.py:15`) or store the key as a column on
`dump_urls`/`curated_urls`/`patterns` at write time. In `jobs.py:317` stop calling `match_counts`
over `all_urls` per batch with exact-key derivation: suggestions are globs — count with the regex
only, once at the end, in a thread.
**Justification.** Suggest exclusions spends 96 % of its time (55 of 57 profiled seconds, on the
event loop) in 10.1 M `urlsplit` calls. Every recompute pays another 5–7 s for the same keys.

### C10. Stream the dump ingest and the index export  — *removes ~2.5 GB of peak memory*
**Change.** Ingest: parse the documents file incrementally (`ijson`, or have the crawler also write
JSONL) and feed `COPY` row by row; drop the intermediate `DumpUrl` list (`duplicate_docs` needs only
URLs and `final_url`, not text). Export: a server-side cursor (`conn.cursor(name=…)`) ordered by URL
feeding `write_jsonl`, instead of `load_curated(with_text=True)` + `sorted`.
**Justification.** Ingest holds three copies of the crawl's text (312 MB file → 1.2 GB RSS; real
pages are longer than 3K characters, and four concurrent ingests reached 2.6 GB). Export adds
1.5 GB. Together with C1/C8 (the 51 MB YAML and 215 MB HTML are built in memory too) this is what
decides whether 2 GB, 4 GB or 8 GB is enough.

### C11. Make the remaining pollers SSE-first (continuation of today's 403 fix)
**Change.** `collection.html:11,14`, `partials/pipeline.html:9`, `jobs.html:7`, `dashboard.html:25`:
poll only while a job is live (as `partials/row.html` now does) and rely on SSE + the `sseReopen`
refresh otherwise.
**Justification.** A collection tab with a running job sends ~165 requests / 5 min; four curators
behind one office IP with a dashboard and a collection tab each are at ~800 of the WAF's 1000 before
anyone clicks — the same 403 as this morning, for everyone at once. Each of those polls also costs
C7's 2 s until C7 lands.

### C12. Resume LLM jobs after an engine restart
**Change.** `jobs.py:139 recover()` marks running jobs failed. For `suggest.metadata` /
`suggest.titles`, re-queue them with `only_missing=True` instead (results are already persisted
every 25 rows / 2 s).
**Justification.** At real latencies a 100K metadata job runs for hours (§6.5). Deploys use
`min_healthy_percent=0`, so every deploy to test — and every OOM or health-check kill until the
above is fixed — ends the job and waits for a human to click again.

## 6. Resource changes

### 6.1 Engine task: 1 vCPU / 2 GB → **2 vCPU / 8 GB now**; revisit after C1–C10
**Justification.** Measured peaks are 3.7 GB (one 100K collection) and 6.5 GB (four); 2 GB is below
the *ingest alone* of two concurrent 100K crawls (2.6 GB). 8 GB is the smallest Fargate size that
survives run A. The second vCPU is for PostgreSQL client work, S3 uploads and — after C4 — threads;
it does not speed up a single recompute. After C1, C8 and C10 I expect the four-curator peak to be
well under 4 GB, but that is a prediction: re-run the stress test before sizing down. If C4 uses a
process pool for four parallel curators, 4 vCPU / 8 GB is the matching size. One line in
`infra/config.py` (`cpu`, `memory_mib` on the TEST config). Test is the only engine and publishes to
prod, so it should carry the prod size (the CDK prod config already says 2 vCPU / 4 GB).

### 6.2 Keep one process
**Justification.** `JobManager`, the per-collection locks and the SSE `EventBus` are in-process; a
second uvicorn worker or a second task would split jobs, locks and events. Scale up (6.1) and off
the loop (C4), not out — scaling out is a redesign (locks in Postgres, events via LISTEN/NOTIFY).

### 6.3 ALB health check: unhealthy threshold 2 → 5 until C1–C4 land
**Justification.** Today a ≥ 35 s freeze kills the task; measured freezes are 29–60 s and will be
longer on the task's vCPU. Five misses tolerates ~150 s. This is a stopgap: the freeze is the bug,
and the check should go back to 2 once C4 is in.

### 6.4 RDS `db.m6i.large`: no change. CloudFront timeout: no change. WAF limit: 1000 → 3000 until C11
**Justification.** PostgreSQL sat at a median 9 % CPU and ≤ 485 MB during run A; its p90 of one full
core is C2's rewrite-everything, which goes away. Storage: 0.33 GB per synthetic 100K collection,
dominated by rule tables; real text will add a few GB per 100K collection across dump + curated —
inside the 100 GB autoscale for dozens of such collections; watch `FreeStorageSpace` (17.5 GB free
today). The pool of 8 queued 71 % of requests but for 55 ms on average — not a bottleneck; leave it
(RDS allows far more if it becomes one). CloudFront's 60 s can be raised by quota request, but C5
is the fix; a longer timeout only makes a frozen page wait longer. The WAF limit is per IP and the
curators share one; 3000 covers four curators with today's polling, and can return to 1000 after C11.

### 6.5 LLM throughput (estimated, not measured — the fake LLM answers instantly)
Duration ≈ URLs × seconds-per-call ÷ workers. For one 100K collection at 16 workers: **3.5 h at
2 s/call, 8.7 h at 5 s/call**; four curators run four jobs (64 concurrent calls) if OpenAI's
rate limits allow it. Two things to check before October that this audit could not: the
account's tokens-per-minute limit for `gpt-5.6-luna` against ~64 concurrent full-page calls, and the
real mean seconds-per-call from a finished job's `job_runs.progress`. `llm_workers` (16) is a config
knob; raising it is free for the engine (the job is I/O-bound and its bookkeeping measured 14 s per
100K) and bounded only by the rate limit. C12 matters in proportion to these hours.

### 6.6 Not sized by this audit
The indexer task (2 vCPU / 8 GB), OpenSearch Serverless OCUs and the crawler EC2 were replaced by
fakes. The export the engine hands the indexer for 100K URLs is a ~310 MB `documents.jsonl`; whether
the indexer embeds 100K documents inside its stall timeout is a separate test on `sde-api-scrapers`.

## 7. Suggested order

| # | Change | Size | Removes |
|---|---|---|---|
| 1 | 6.1 task size, 6.3 health check, WAF 3000 | config | OOM and health-check kills today |
| 2 | C3 regex, C7 SQL counts, C9 key cache | hours | ~15 % of every edit; polls that cost 2 s each; Suggest-exclusions freeze |
| 3 | C1 `patterns.yaml` | hours | 55–60 % of every edit and most of the freeze |
| 4 | C2 incremental `replace_deltas`, C6 COPY rules | 1–2 days | most of the rest; PostgreSQL churn |
| 5 | C4 off the event loop | 1 day | freezes between curators |
| 6 | C5 background jobs, C8 paged Rules tab | 2–3 days | 504s; the 215 MB page |
| 7 | C10 streaming ingest/export, C11 pollers, C12 resume | 2 days | memory headroom; 403s; lost LLM hours |

Then re-run the same stress test and size the task from the new peak.

## 8. Reproducing

The harness is deliberately not in the repo (it was an audit tool). It and the raw results
(`run4.json`, `run1.json` with the full profiles, `pg4.log`) are in this session's scratchpad:
`/private/tmp/claude-502/-Users-bbenson-projects-sde-curation-engine/b75f656e-ab0d-44ae-8d68-95c4eea2114c/scratchpad/` —
copy them somewhere permanent if you want to re-run after the fixes:

```
make db-up
.venv/bin/python <path>/stress.py -n 100000 --curators 4 --index --out run4.json
.venv/bin/python <path>/stress.py -n 100000 --curators 1 --index --out run1.json \
    --profile 'bulk-accept,inline edit,add pattern with exact,recompute after re-crawl'
```

It creates and truncates its own `engine_stress` database on the local PostgreSQL; the dev database
is not touched. Run it under `caffeinate -i`: a sleeping laptop pauses the run.
