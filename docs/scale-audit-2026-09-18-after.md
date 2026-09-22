# Scale audit, part 2 — after the changes

*2026-09-18 · branch `redesigned-curation`, working tree on top of 5258407 (nothing committed) ·
follow-up to `docs/scale-audit-2026-09-18.md` (the "before", whose change numbers C1–C12 are used here)*

## 1. Verdict

Same stress test, same machine, same data (4 curators × 100K-URL collections, in lock-step through
the whole workflow; then 1 curator × 100K).

| | before | after |
|---|---|---|
| Requests over CloudFront's 60 s | bulk-accept 266–304 s, every edit with per-URL rules present 163–236 s, recompute after re-crawl 128–196 s | **none.** Slowest request 34 s (4 curators adding a `*` title glob at the same instant); the long operations are background jobs |
| Longest freeze of the server (every other user's pages, polls, SSE) | 60 s (29 s with one curator) → ALB health check would kill the task | **2.8 s** |
| One curator: inline edit on a 100K collection with 300K per-URL rules | ≈ 56 s | **5.8 s** |
| One curator: bulk-accept AI metadata (300K rules) | 76 s, request, 29 s freeze | **17.5 s**, job, 0.6 s freeze |
| One curator: recompute after a re-crawl | 54 s | **7.4 s** |
| Whole workflow, 4 curators | 1,428 s | **397 s** |
| Whole workflow, 1 curator | ≈ 360 s (sum of steps) | **121 s** |
| Rules tab | 215 MB of HTML, 19–31 s | **149 KB, 0.3–2.9 s** (paged) |
| Peak memory, 4 curators | 6.5 GB | **4.6–5.7 GB** (three runs) |
| Peak memory, 1 curator | 3.7 GB | **2.4 GB** |
| Memory after ingesting a 312 MB crawl | 1.2 GB (2.6 GB for four) | **0.22 GB (0.37 GB for four)** |

The 504s and the freezes are gone, with margin. **Memory improved least**: 2 GB is still not
enough, 8 GB is — §6 says where the rest is and what removes it. Everything here is still a laptop
measurement (faster core than Fargate's, local socket to PostgreSQL): the ratios carry over, the
absolute seconds are lower bounds. 276 tests pass (267 before + 9 new), lint and infra tests clean.

## 2. What was changed

All twelve items of the first report, plus five things the re-test found (§3).

| # | Change | Where |
|---|---|---|
| C1 | `patterns.yaml` is no longer rewritten inside each edit. ≤ 2,000 rules: written before the request returns (in a thread), same look as before. More: written in the background 20 s after the collection's last rule change — a run of edits costs one write — by a direct emitter (§3 F4); always written on promote and at shutdown | `store.py` `PatternsFile`, `app.py` `_after_curation_change`, `api_promote` |
| C2 | `replace_deltas` still receives the complete recomputed state, but writes only the rows that differ (temp table → delete the missing → upsert `WHERE … IS DISTINCT FROM`); effects loaded with `COPY` instead of 300K INSERTs | `db.py` `replace_deltas` |
| C3 | No regex is compiled for exact-URL rules; the canonical-key index over the URLs is built only when there is an exact rule to look up | `engine/patterns.py` `compile_patterns` |
| C4 | Whole-collection CPU work runs in threads: diff + rule resolution, match counts, promote, crawl parsing + hashing, YAML/JSONL serialisation | `curation.py`, `jobs.py`, `store.py` |
| C5 | On collections with ≥ `BULK_JOB_MIN_URLS` (20,000) dump URLs, **recompute / Start curating**, **accept all pattern suggestions** and **accept all AI metadata** answer `202` + a job (`recompute`, `bulk_suggestions`, `bulk_accept`) with progress in the header and jobs strip; edits are refused while it runs, as under any job. Smaller collections and single edits are answered in the request as before | `app.py` `run_or_job`, `jobs.py` `start_curation`, `models.py` `JobKind`, `job_progress.html` |
| C6 | Bulk rule insert through `COPY` + one `INSERT … SELECT … ORDER BY` (ids still in the order given — newest wins); other spellings of a page's rule deleted in one statement from plain tuples | `db.py` `insert_patterns`, `exact_pattern_matches`, `delete_patterns`; `curation.py` |
| C7 | Polled page contexts count in SQL (`count_patterns`, `count_curated_excluded`, `count_curated_unreachable`); one rule is fetched by id / by canonical key instead of loading all 300K (`get_pattern`, `exact_patterns_for`); the exclude toggle loads only exclude/include rules | `app.py` `step_context`, `api_delete_pattern`, `api_url_edit`, `ai/accept`; `curation.py` `set_excluded` |
| C8 | Rules tab: every glob rule, per-URL rules 200 at a time (`?rpage=`); match / in-effect counts computed for the rules shown; source counts from SQL. `GET …/patterns?exact_limit=&exact_offset=` pages the same way (without them: every rule, as before) | `curation.py` `pattern_stats`, `app.py` `rules_context`, `rules.html` |
| C9 | `canonical_key` cached; Suggest exclusions no longer re-derives 100K keys per batch (that was 96 % of the job) | `engine/urls.py`, C3 |
| C10 | Dump ingest streams the documents file (`ijson`, new dependency): one pass for URLs → duplicates, one pass into `COPY`, 500 documents at a time in a thread. Index export reads 500 curated rows at a time by primary key | `backends/scrape.py` `iter_documents`, `jobs.py` `ingest_dump`, `db.py` `replace_dump`, `iter_curated_for_export` |
| C11 | Collection header, stepper, tab watcher and jobs strip poll **only while a job is live** (`every Ns [jobLive()]`, marker `data-job-live`), and re-fetch once after the SSE stream reconnects. An idle page sends nothing | `collection.html`, `pipeline.html`, `dashboard.html`, `jobs.html`, `header.html`, `jobs_panel.html`, `base.html` |
| C12 | A Suggest-metadata job interrupted by an engine restart — killed under it, or cancelled by a deploy's shutdown — is started again for the URLs still missing, at most `LLM_RESUME_AFTER_RESTART` (3) times per run; a curator's own cancel stays cancelled | `jobs.py` `recover`, `db.py` `jobs_ended_by_shutdown` |
| 6.1 | Test task 1 vCPU / 2 GB → **2 vCPU / 8 GB** in `infra/config.py` (not deployed) | `infra/config.py` |

**Not changed, on purpose:** what a recompute computes (always the whole delta set — "any change
anywhere shows up in the deltas" is untouched), the rule model (every per-URL edit and accepted AI
value is still a rule; newest wins; delete = undo), where edits are stored (RDS, inside the request,
every time — only the `patterns.yaml` *snapshot* is deferred), the ALB health check and the WAF
limit (§5).

## 3. What the re-test found that the first report did not predict

The first "after" runs hung or disappointed five times; each was a real defect that only shows
at this size, and each is fixed and in the numbers above.

- **F1 — C2's first version could run for minutes.** `DELETE … WHERE NOT EXISTS (SELECT … FROM
  temp table)` was planned as a nested loop over an un-indexed temp table once psycopg switched to a
  generic prepared plan: 100K × 100K rows. Fix: index + `ANALYZE` on the temp tables so the
  anti-join is a lookup under any plan. The same shape in the existing `replace_curated` got the
  same treatment.
- **F2 — a pre-existing query ran for ten minutes.** `count_deltas_for_llm` (the Suggest-metadata
  pre-check, a join of `delta_urls` and `dump_urls`) ran seconds after a 100K-row ingest, before
  autovacuum had analysed the tables, and was planned for empty tables. It never showed before
  because everything in front of it was slow enough for autovacuum to get there first. This can
  happen in production right after any big crawl. Fix: bulk writes (≥ 5,000 rows) `ANALYZE` the join
  columns of the table they wrote, inside their transaction (`db.py` `_BULK_ROWS`).
- **F3 — my first C1/C10 held database connections hostage.** Streaming `patterns.yaml` and the
  export from a server-side cursor kept a pool connection for the whole (slow) write; four of those
  plus four recomputes exhausted the pool of 8, and requests queued for up to 11 s — which looked
  exactly like a frozen server. Fix: both read in short keyset / primary-key pages; `DB_POOL_SIZE`
  default 8 → 16. The harness now measures two heartbeats — `/health` (needs the database) and a
  static file (event loop only) — so the two causes can be told apart.
- **F4 — PyYAML is too slow even in C and in a thread.** 300K rules cost ~8 s of interpreter time
  per rewrite; under the GIL that is 8 s taken from every curator's recompute, and in lock-step it
  put edits back at 12–85 s. Fix: for rule sets over 2,000, a direct emitter (strings as JSON
  strings, which are YAML) — a test asserts the file loads to exactly what PyYAML's does, with
  awkward values (`yes`, `123`, dates, `a: b # c`, quotes, unicode, empty).
- **F5 — 300K Pydantic `Pattern` objects are 420–600 MB and 1 s per recompute.** The engine reads
  five fields of a rule. Fix: `models.Rule`, a frozen slots dataclass, loaded through a server-side
  cursor: 44 MB, 0.5 s. `Pattern` is unchanged everywhere a rule is shown or edited.

## 4. Results

### 4.1 Four curators, lock-step (wall seconds, min–max across the four; **bold** = over 60 s)

| Step | before | after | freeze before → after |
|---|---|---|---|
| scrape + ingest dump | 11–13 | 13 | 3.4 → 0.5 |
| recompute (Start curating) | 5–7 (request) | 6 (job) | 0.3 → 0.7 |
| add exclude glob | 10–13 | 6–11 | 1.5 → 0.6 |
| add title glob `*` (rewrites all 100K rows, legitimately) | 31–**79** | 10–34 | 2.1 → 1.2 |
| Suggest exclusions (fake LLM) | 45–**71** | 13–24 | 2.1 → 1.2 |
| bulk-accept pattern suggestions | 27–59 (request) | 8–23 (job) | 1.3 → 1.3 |
| Suggest metadata (fake LLM, 100K calls each) | 43–58 | 40–54 | 1.0 → 0.8 |
| inline title edit, no per-URL rules yet | — | 6–11 | → 0.8 |
| **bulk-accept AI metadata** (300K rules each) | **266–304** (request) | 37–48 (job) | **60** → 1.8 |
| inline title edit, 300K rules present | — ¹ | 10–19 | → 1.9 |
| inline exclude, 300K rules present | — ¹ | 14–26 | → 1.5 |
| add a glob, 300K rules present | **163–236** | 20–26 | 31 → 1.1 |
| recompute, 300K rules present | **193–224** (request) | 24–26 (job) | 30 → 2.7 |
| promote | 5–28 | 5–13 | 5 → 2.8 |
| index to test (export + validate) | 17–29 | 19–39 | 1.7 → 2.8 |
| re-crawl (ingest) | 9–25 | 7–16 | 0.1 → 0.3 |
| recompute after re-crawl | **128–196** (request) | 13–25 (job) | 28 → 2.7 |

¹ not in the first run; the same code path as "add a glob" (163–236 s).

Pages after the bulk-accept: collection page 8–**96** s → 1.6–5.3; Curate tab 4–35 → 1.9–7.4; Rules
tab 19–31 s / 215 MB → 0.8–2.9 s / 149 KB; paged `/patterns` 0.6–6.2 s / 47 KB; **unpaged**
`/patterns` 12–33 → 6–16 s, still 71 MB (nothing in the UI calls it; §6 R4). Every other page
< 2 s. Worst event-loop freeze anywhere in the run: 2.8 s.

### 4.2 One curator

| Step | before | after |
|---|---|---|
| scrape + ingest (memory after) | 3.9 s (1.2 GB) | 3.7 s (0.22 GB) |
| recompute (Start curating) | 1.6 | 2.3 (job) |
| add exclude glob / title glob `*` | 2.6 / 7.9 | 2.3 / 4.2 |
| Suggest exclusions | 16.2 | 4.5 |
| bulk-accept pattern suggestions | 7.8 | 3.2 (job) |
| inline title edit, before bulk-accept | ≈ 8 | 3.5 |
| bulk-accept AI metadata | 76 (29 s freeze) | 17.5 (job, 0.6 s) |
| inline title edit / inline exclude / add a glob, 300K rules | ≈ 56 each | 5.8 / 5.6 / 5.6 |
| recompute, 300K rules | 56 | 6.1 (job) |
| Rules tab | 11 s, 214 MB | < 1 s, 149 KB |
| promote | 3.0 | 5.1 ² |
| index to test | 6.9 | 7.1 |
| recompute after re-crawl | 54 | 7.4 (job) |
| whole workflow / peak memory | ≈ 360 s / 3.7 GB | 121 s / 2.4 GB |

² promote got slower: it now waits for `patterns.yaml` to be current (a 51 MB file, ~2 s) and
analyses the curated table (F2). Deliberate.

### 4.3 Where an edit's time goes now (100K URLs, 300K rules, one curator, ≈ 5.6 s)

load dump 0.2 s · load curated 0.3 s · load rules 0.5 s · diff + resolve 1.4–1.6 s · write the
difference 1.6 s (almost all of it re-checking 300K effect rows) · status/audit/page ≈ 1 s. Before:
YAML ≈ 30 s, rewrite everything ≈ 11 s, regex ≈ 7 s, resolve ≈ 1 s.

### 4.4 Database and files

PostgreSQL (Docker, 7.75 GB): CPU median 78 % of one core, p90 226 %, max 364 %; memory ≤ 706 MB.
The run is 3.6× shorter, so the same work is packed tighter — in the lock-step worst case four
simultaneous bulk operations do use more than two cores' worth of PostgreSQL. Database 1.26 → 1.3–1.7
GB at the end of the run: `delta_urls` was 508 MB in the last run (56 MB in the one before) — dead
row versions from upserts that autovacuum had not yet reclaimed when the run ended 6 minutes after
it began; transient, but see R5. Pool (16): 26,874 requests, 17,997 queued, 632 s total wait
(35 ms per queued request, half of before; ~14K requests are the harness's heartbeats).
`patterns.yaml` is still 51 MB per 100K collection (it is the same data).

**Harness note.** The engine and moto's in-memory S3 share one process in the test, so every
exported collection adds ~0.3 GB to the measured memory after the index step, before and after
alike. The peaks quoted happen earlier (during bulk-accept), so they are the engine's own.

## 5. Resources, revisited

- **Engine task: 2 vCPU / 8 GB (set in `infra/config.py` for test; deploy it).** Measured worst case
  4.6–5.7 GB; one curator on 100K URLs 2.4 GB; today's real peak on test is ~0.75 GB. 4 GB would
  hold one or two big collections but not four at once; after R1 + R2 (§6) 4 GB becomes plausible —
  re-measure before sizing down. One process still (jobs, locks and SSE bus are in-process).
- **ALB health check: left at 2 misses.** Freezes are ≤ 2.8 s locally against a 5 s timeout, and two
  consecutive checks 30 s apart would both have to land in one. If the Fargate vCPU turns 2.8 s
  into > 5 s it is still a single miss. Not changed; watch `UnHealthyHostCount` after the first big
  collection.
- **WAF: left at 1000 / 5 min / IP.** Idle pages now send nothing. A collection tab with a live job
  sends ~165 requests / 5 min (header 10 s, stepper 5 s, tab watcher 4 s), so four curators behind
  one IP each watching a job ≈ 660 + dashboard rows with live jobs. Under the limit, not by a wide
  margin if people keep two tabs open: raising `waf_rate_limit_per_5min` to 2000 is a free margin.
- **RDS `db.m6i.large`: no change, but watch CPU.** Fine for realistic use; in the lock-step worst
  case PostgreSQL wanted > 2 cores for short bursts (p90 226 %). If `CPUUtilization` sits above
  ~70 % while several curators work, `m6i.xlarge` is the next step. `DB_POOL_SIZE` 16 is far below
  RDS's connection limit.
- **CloudFront 60 s: no change.** Nothing long is a request any more.
- **LLM duration, indexer, OpenSearch, crawler:** still not measured by this harness (fake LLM,
  fake indexer) — the estimates and caveats of part 1 §6.5–6.6 stand.

## 6. What is left, in order

- **R1 — bulk-accept's memory (the 4-curator peak, ~1 GB per concurrent accept).** `_decide_ai_bulk`
  loads 3 × 100K suggestion rows and builds 300K `PatternCreate` + 300K `Pattern` models to insert
  them. Do it in SQL: `INSERT INTO patterns … SELECT … FROM delta_urls WHERE title_ai IS NOT NULL …`
  (three statements), then clear the suggestion columns. Removes the peak and most of the 17 s.
- **R2 — the recompute's working set (~0.8–1 GB per concurrent recompute).** 100K `DumpUrl` + 96K
  `CuratedUrl` + up to 100K `DeltaUrl` Pydantic models in, as many out. Slim read-only views for the
  engine, as `Rule` did for rules (−90 %).
- **R3 — four simultaneous recomputes share one interpreter.** 5.6 s alone, 10–26 s in lock-step:
  threads fixed the freezes (C4), not the throughput. A `ProcessPoolExecutor` for `recompute` gives
  real parallelism and is what would justify 4 vCPU; do it after R2 (the inputs have to be shipped
  to the worker).
- **R4 — unpaged `GET …/patterns`** still returns 71 MB for a 100K collection if someone calls it
  without `exact_limit`. Nothing in the UI does. Consider a default limit with a `total` in the
  response (an API change, so not done here).
- **R5 — autovacuum on the hot tables.** Upserts leave dead row versions; at four big collections
  per ten minutes `delta_urls` reached 9× its live size before autovacuum caught up. Set
  `autovacuum_vacuum_scale_factor = 0.02` (and `…_analyze_scale_factor`) on `delta_urls`,
  `pattern_effects`, `patterns` in a migration.
- **R6 — rule add/delete as a job on big collections.** Slowest remaining request: a `*` title glob
  on 100K URLs, 4.2 s alone, 34 s when four curators do it in the same second, on a laptop. On the
  Fargate vCPU that combination could approach 60 s. `run_or_job` makes it a small change; the
  add-rule form expects `201` + the rule, so the UI needs the same 202 handling the bulk buttons got.
- **R7 — `patterns.yaml` after a hard kill.** A big collection's file can be up to 20 s behind; if the
  engine is killed in that window the file stays stale until the collection's next rule change or
  promote (the database is never behind). A startup pass that rewrites files older than their
  newest rule would close it.
- **R8 — measure on test.** One real 100K collection end to end on the 2 vCPU / 8 GB task: real
  seconds per LLM call, the indexer on a 310 MB export, and the Fargate/laptop ratio this report
  cannot give.

## 7. Behaviour changes curators and operators will notice

- On collections ≥ 20,000 dump URLs, **Start curating / recompute** and the two **accept all**
  buttons start a job: the page reloads with a progress line ("accepting the AI suggestions…") and
  other edits are refused until it finishes (seconds to under a minute). `BULK_JOB_MIN_URLS=0`
  makes every collection behave that way; a very large value restores the old behaviour.
- The Rules tab lists every glob rule and pages the per-URL rules (200 per page).
- Pages no longer poll when nothing is running; they update from SSE, and re-fetch after a reconnect.
- A Suggest-metadata job survives a deploy: it restarts itself for the URLs still missing (the
  interrupted job shows as failed, "cancelled by shutdown", with the resumed one after it).
- `patterns.yaml`: unchanged for collections with ≤ 2,000 rules. Above that it is written up to 20 s
  after the last change (always before promote returns and at shutdown), and every string is
  quoted — same data, a one-time diff for anything tracking the file.
- New dependency `ijson`; new settings `BULK_JOB_MIN_URLS` (20000), `LLM_RESUME_AFTER_RESTART` (3);
  `DB_POOL_SIZE` default 16; `GET …/patterns` accepts `exact_limit` / `exact_offset`. README updated.

## 8. Verification

- `make test`: **276 passed** (267 before). New, all through the API with real flows:
  `tests/test_scale.py` — an edit rewrites only the rows it changes (checked by row version) and
  rule effects stay exact through add / exclude / delete; an inline edit finds its rule under another
  spelling of the URL without loading every rule; the three bulk operations run as jobs over the
  threshold, give the same result, block edits meanwhile, and stay requests under it; `patterns.yaml`
  is current after promote when written in the background; the fast writer's file loads identically
  to PyYAML's; the Rules tab pages per-URL rules with correct counts; pages poll only while a job
  is live; a streamed crawl ingests and a broken documents file fails the job without touching the
  previous dump. `tests/test_llm.py` — a metadata job resumes after an engine restart, a curator's
  cancel does not.
- `make lint`, `make infra-test` (16 passed), `make requirements-check`: clean.
- Stress runs: 4 × 100K launched seven times (one harness bug, one hung on F1, three that led to
  F3–F5, then two clean and consistent: 398 s and 397 s), 1 × 100K twice (the first hung on F2). Raw results (`before/`, `attempt1–4/`, `after4.json`,
  `after1.json`, PostgreSQL samples) and the harness are in the session scratchpad, outside the repo:
  `/private/tmp/claude-502/-Users-bbenson-projects-sde-curation-engine/b75f656e-ab0d-44ae-8d68-95c4eea2114c/scratchpad/`
  — copy `stress.py` somewhere permanent to re-run after R1–R3:
  `.venv/bin/python <path>/stress.py -n 100000 --curators 4 --index --out after.json` (under
  `caffeinate -i`; it uses its own `engine_stress` database, which has been dropped again).

Nothing is committed and nothing is deployed.
