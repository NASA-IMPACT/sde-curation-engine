# Review of the changes since the last dev merge (2026-10-08)

**Range:** `d8cbfbb` (merge of pull request #67 to dev, 2026-10-01) to `9152447` (HEAD of
`featuure/optimize-app`). 9 commits.

**Size:** about 1,950 lines of production code changed in 24 files (`sde_curation/`, `infra/`),
plus about 9,400 lines of tests and page snapshots, and new planning and operations documents.

**How this review was done.** This review was written by Claude (an AI assistant), not by a
person. Four Claude subagents read the full diff in parallel, one area each: the database layer,
jobs and resume, the curation engine and web layer, and infrastructure and deploy. Each finding
had to name the code, the failure scenario and a fix. Then every finding that can run on a laptop
was reproduced, with tests that show the wrong outcome; I re-ran the most serious ones myself and
read their test code. Each finding says how it was checked:
- **Reproduced:** a test or run showed the wrong outcome. "(Re-run)" means I ran it again myself.
- **Code checked:** confirmed by reading the code or the synthesized CloudFormation template.
- **Not reproducible locally:** needs AWS; checked against the code and the AWS documentation.

Everything ran locally: throwaway Postgres containers, the fake LLM, moto S3 and a fake ECS client.
Nothing called OpenAI or AWS. The test suite (451 tests) and `make infra-test` (23 tests) pass on
HEAD. The reproduction tests and scripts are kept outside the repo, in
`~/projects/sde-curation-stress/repros/` (copy a test into `tests/` and run it with
`env -u TEST_DATABASE_URL .venv/bin/pytest <file> -s`).

**Reproduction results:** 30 findings reported; 1 withdrawn (L3, did not reproduce). Of the other
29: 27 reproduced or confirmed in the code or template (two of them narrower than first stated, M4
and M7), 1 measured and found negligible (M8), and 1 not reproducible locally (M1, needs AWS).
Finding IDs are kept from the first version of this review; M4 and M8 moved to Low.

**Status (2026-10-09):** every code finding is fixed in the working tree, not committed (section 8).
The infrastructure, runbook and documentation findings, and the decisions, are pending (section 9).

---

## 1. Verdict

The branch does what it set out to do. On a local stress run with the same ramp as the
2026-10-06 dev run, page and tab-refresh times under 15–20 concurrent jobs fell from 4–11 s to
about 1 s, every job succeeded and nothing went down (`~/projects/sde-curation-stress/results/local-20261008T191933Z/`).
Jobs now survive an engine restart.

**It is not ready to merge to dev as it is.** One finding changes what curators see and can lead
to an incomplete promote (H1). Fix H1 before the merge. Six findings are regressions against dev
(H1, M9, M10, L5, L8, L10); the others are gaps in new features or problems dev already has
(section 4). The other High and Medium items should be
fixed, or consciously accepted, before the first deploy to test.

---

## 2. What changed, at a high level

### By commit

| Commit | What it does |
|---|---|
| `0361da2` Busy-database hardening | The fix for the 2026-10-06 test crashes. The excluded count is stored, not counted on every view (V11, V12). Two connection pools: a read pool for pages (statement timeout, short wait, 503 + `Retry-After` when busy) and a work pool for actions and jobs. Identical page reads share one query (`SingleFlight`). `/health` no longer touches the database; `/health/db` is new. Stepper click fix (htmx attributes). |
| `980a3b6` Tier 0 | Page-snapshot test guard (20 pages), event-loop lag probe on `/health/db`, INFO logging, `pg_stat_statements` (V13) and autovacuum settings for the busiest tables (V14), an RDS parameter group, 7 CloudWatch alarms. |
| `f45b3fc` Recompute fixes | A recompute no longer overwrites AI suggestions written meanwhile. "Re-curate everything" keeps its queue across later recomputes (V15 `review_round`). |
| `e42c1e2` Tier 2 | Big loads and COPY writes yield to the event loop and build rows in threads. Repeated reads in one page render run once (`RequestMemo`). Writes mark the collection changed when they commit (`_touches`), not on every progress event. One progress write and event per job per 3 s. Promote runs off the event loop. Login lookups cached for 30 s. |
| `7324e2a` CI fix | The event-loop test limits the freeze as a share of the edit's time, not in milliseconds. |
| `d508edc` Tier 3 | Indexes on the busy filters (V16). Page counts stored between changes, with version invalidation (V17 `collection_stats`). The 10-second job refresh fetches only the tab body. |
| `a62743d` Tier 4 | A per-URL edit recomputes only that page's rows, using a stored canonical key (V18, with a background backfill). Rules-tab match counts in SQL. |
| `c3496b1` Tier 5 | Every job kind resumes after an engine restart (index to test, validate, index to prod, the three LLM jobs, recompute and the bulk accepts), started after a delay, spaced out, with a limit on heavy work and on restarts. Cancel stops the ECS indexer task. One limit on LLM calls across all jobs (`LLM_WORKERS_TOTAL`, 32). 3 more alarms. |
| `9152447` Freeze fix | Suggest patterns' setup runs off the event loop; the URL key index is built once per job; per-batch counts run one at a time. At 100K URLs the worst freeze fell from 1.1 s to under 40 ms. |

### By area

- **Database (`db.py`, `schema.py`).** Migrations V11–V18. Two pools chosen per request. Shared
  in-flight reads, write-driven change marking and stored counts. Scoped writes for per-URL edits.
  SQL rule counts. Sliced loads and COPY.
- **Jobs (`jobs.py`, `backends/index.py`, `llm/pool.py`).** Progress throttle. Resume framework
  (`_resumers`, `recover`, `start_resumes`, `heavy_phase`, restart counting). Per-kind resume
  logic. ECS task kill. Shared LLM semaphore.
- **Curation and web (`curation.py`, `engine/`, `web/app.py`, templates).** Scoped recompute,
  review round, health endpoints, read/work scope middleware, request memo, `/tab-body`, login
  cache, bulk changes rebuilt for resume.
- **Infrastructure (`infra/`).** Parameter group, 10 alarms on one SNS topic, `ecs:StopTask`,
  `LLM_WORKERS_TOTAL`.
- **Documents.** Changelog of 2026-10-06, architecture assessment, decisions, implementation plan
  with validation log, dev rollback runbook.

### What curators will notice

Approved changes: jobs keep running across a restart or deploy; several LLM jobs at once run
slower each, with fewer failed calls. Not intended: finding H1 below.

---

## 3. Findings

Severity: **High** means wrong curation data, lost or duplicated external work, or an outage.
**Medium** means wrong under realistic timing or load, or an operational trap. **Low** means
minor, hygiene, documentation, or only under a non-default setting.

### High

**H1. An edit after a crawl and before Start curating queues only that page.**
- *Where:* `curation.py` `_recompute_keys` (the guard near line 111); `jobs.py` ingest calls
  `replace_deltas(cid, [], [])`, which stores an excluded count of 0, not "unknown".
- *What happens:* after a crawl (status `scraped`, or a re-crawl of a curated collection), the URL
  tables are editable. If a curator edits one row's title or presses ✗/✓ before Start curating,
  the new scoped recompute runs on an empty queue. The collection moves to `curating` with one
  delta URL. The "Start curating" button disappears. A promote can then mark the collection
  curated while the rest of the crawl's new, changed and removed pages were never reviewed.
- *Reproduced (re-run), 10-page crawl, one title edit:* HEAD gives `curating` with 1 delta URL;
  the baseline `d8cbfbb` gives `curating` with 8. A full recompute on HEAD then gives 8. This is a
  regression from Tier 4: the scoped twin test always runs a full recompute after a crawl.
- *Fix:* allow the scoped path only after a full recompute has run on the current dump (an
  explicit marker that `replace_dump` clears and a full recompute sets). Add a twin-test case:
  crawl, then edit without a recompute.

**H2. An interrupted dispatch starts a second ECS indexer for the same run.**
- *Where:* `jobs.py` lines 1100–1103 (`_export_and_dispatch`).
- *What happens:* `backend.dispatch` starts the Fargate task in a thread; the task ID is saved
  only after it returns. If the engine stops in between, the resumed job exports again
  (overwriting the files the first task may be reading) and dispatches a second task for the same
  run. On a curator's cancel in the same window, the started task is never stopped.
- *Reproduced (re-run):* the real `EcsDispatchIndexer` with a fake ECS client whose `RunTask`
  records the task as started and then answers slowly. After a restart: `RunTask calls=[run, run]`,
  `exports=2`, `stop_task calls=[]`. After a cancel: job and run failed, the started task never
  stopped.
- *Fix:* protect the dispatch and the save of the task ID from cancellation (`asyncio.shield`,
  awaited on cancel). Before re-dispatching on resume, look for a running task started for this
  run ID and adopt it.

### Medium

**M1. The first deploy of this branch will reboot the database of that environment.**
- *Where:* `infra/stacks/engine_stack.py` lines 124–126 (comment), 127–137 (parameter group).
- *What happens:* the comment says attaching the group leaves a reboot pending, done "by hand at a
  quiet moment". The CloudFormation reference for `AWS::RDS::DBInstance` `DBParameterGroupName`
  says: "If the parameter group contains static parameters, whether they were changed or not, an
  update triggers a reboot". `shared_preload_libraries` is static. Expect about 1–2 minutes without
  a database during the deploy, once per environment, on the deploy that first attaches the group
  (a merge to dev reboots dev only; test and prod reboot when the change reaches their branch).
- *Not reproducible locally* (needs AWS). Code checked: `d8cbfbb` has no parameter group; HEAD
  attaches one. Documentation checked. No stateful resource is replaced (synthesized templates
  compared).
- *Fix:* correct the comment and the deploy notes; deploy with no jobs running (a database outage
  fails running jobs; Tier 5 resumes only after an engine restart). After the first deploy, check
  that the `pg_stat_statements` extension exists: migration V13 creates it once and skips quietly
  if the library is not loaded yet.

**M2. Cancelling an index job that waits to resume leaves its ECS task running.**
- *Where:* `jobs.py` `cancel`, the pending-resume branch; also the restart-limit path.
- *Reproduced:* cancel while waiting: `job=failed run=running stop_task calls=[]`, and a new Index
  to test is accepted (202) while the old task runs. Restart limit reached: the job fails and
  `close_orphan_index_runs` closes the run row, but the task is not stopped.
- *Fix:* in both cases, for an index job, stop the task by its saved ID and close the run.

**M3. One error in the resume loop strands every queued resume.**
- *Where:* `jobs.py` `_run_resumes` (no error handling, nothing awaits the task's exception).
- *Reproduced:* `get_collection` raises `PoolTimeout` once: `resume task done=True
  exc=PoolTimeout`, `started=0`, all three jobs `running`, all three collections pending, edits on
  them `409`. The job that hit the error is stranded too.
- *Fix:* handle errors per job inside the loop: take the job off the pending list, mark it
  failed with the reason, log it.

**M5. A resumed "redo all" metadata job finishes as "missing only" and reports success.**
- *Where:* `jobs.py` line 173: the resumer passes `only_missing=True`; the choice is not stored.
- *Reproduced:* `job=succeeded total=16 done=3 classified=3`, asked 4 pages before the restart and
  0 after. The rest keep their old suggestions.
- *Fix:* store `only_missing` and a checkpoint (for example the last URL handed out), and resume a
  redo from it; at least, do not report success for a redo that was resumed.

**M6. The alarms notify nobody.**
- *Where:* `engine_stack.py` line 335: the alarm topic has no subscription; only the docstring
  says to subscribe by hand.
- *Code checked.*
- *Fix:* subscribe an email or chat endpoint from configuration, or add a deploy checklist step.

**M7. No alarm fires when the engine crashes within seconds of starting.**
- *Where:* the alarm set (all treat missing data as "not breaching").
- *Reproduced from the synthesized template:* a task that dies before two failed health checks
  never counts as unhealthy (`UnHealthyHostCount` stays 0); with no target the load balancer
  answers 503 itself (`HTTPCode_ELB_5XX_Count`), which no alarm watches; the other alarms get
  missing data. Correction: a slow failure (for example a start that outlasts the grace period)
  can still trip `UnhealthyEngine`.
- *Fix:* add `HealthyHostCount < 1` for 3 minutes with missing data as breaching, and an alarm on
  `HTTPCode_ELB_5XX_Count`.

**M9. A lost change mark leaves stored page counts stale, across restarts.**
- *Where:* `db.py` `_touches` and `changed` (the mark is a second transaction after the commit).
- *Reproduced:* `changed` made to raise `PoolTimeout` once during the first recompute after a
  crawl. The recompute answers 503 but its 8 delta rows are committed. Three page views and an
  engine restart later the stored counts still say 0 (`count_deltas_by_kind total 0` vs 8 in the
  table); the next write repairs them. Addition: the curator gets an error for a change that was
  saved, and the status move to `curating` and the audit line are lost too.
- *Fix:* mark the change in the writer's own transaction; clear stored counts at startup.

**M10. The excluded count can be subtracted twice and go negative.**
- *Where:* `db.py` `excluded_count` (stores a count while the value is unknown) and the scoped
  recompute (adds its own difference).
- *Reproduced (re-run):* ✗ a page (count 1), then ✓ it; while the ✓ is between its rule delete and
  its recompute, another tab's page view stores the count (0); the ✓ then subtracts again. Result:
  stored −1, fresh 0, and the ✓ response says `excluded: -1`. A full recompute repairs it.
  Worse than first stated: not only off by one, but negative.
- *Fix:* store the lazy count only if a change token has not moved, or let rule deletes subtract
  their effects in the same transaction instead of marking the count unknown.

**M11. A restored database resumes its running jobs, including Index to prod.**
- *Where:* `docs/dev-db-rollback.md` line 187 and `infra/README.md` line 91 both say in-flight
  jobs are marked failed on restart.
- *Reproduced with a real restart:* a database holding a running `index_prod` job (as a snapshot
  would) was started under HEAD's engine. The job resumed (`restart 1`), succeeded, and its
  deletion pass removed a page that prod had gained after the "snapshot".
- *Fix:* tell the operator to mark running jobs failed in a restored database before starting
  the engine; correct both lines.

**N1. An unexpected error in an LLM call or in saving its answer hangs the job forever.** *(Found
2026-10-09 while writing unit tests; not in the first version of this review.)*
- *Where:* `llm/pool.py` `run_pool`, `run_pass`: only `LLMError` is handled per item.
- *What happens:* anything else raised by the model call or the result handler (a bug, or a
  database error while an answer is saved) ends the worker. The pool cancels the producer once;
  the producer's `finally` then waits forever to put its stop marker into a full queue. The job
  stays `running`, its collection refuses every edit with 409, until the engine restarts. A
  curator's cancel still works (it cancels twice).
- *Reproduced:* `tests/unit/test_pool.py::test_an_unexpected_error_fails_the_pool_instead_of_hanging`
  (a `TypeError` in the call: the pool is still running after 2 s). Same code on dev.
- *Fix:* do not block in the producer's `finally` when cancelled (put the stop markers without
  waiting, or let the workers stop on cancellation), and let an unexpected error fail the job.

**N2. Regenerate titles fails when the model gives up on one group, and leaves its duplicates.**
*(Found 2026-10-09 while writing unit tests.)*
- *Where:* `jobs.py` `_retitle_duplicates`: the re-ask pass calls `run_pool` with no error handling,
  before the step that tells pages apart by their URLs.
- *What happens:* when only the failed group is left for a second pass, `run_pool` raises "all 1
  calls failed"; the job fails and the URL step never runs. The group stays duplicated, and a
  duplicate blocks promote. (Suggest metadata, which runs the same pass, catches the error and only
  reports it.)
- *Reproduced:* `tests/unit/test_jobs_llm.py::test_a_group_the_model_keeps_failing_on_is_told_apart_by_its_urls`.
  Same code on dev.
- *Fix:* when a re-ask pass fails, go on to the URL step, as the method's docstring promises.

### Low

| # | Finding | Result |
|---|---|---|
| M4 | A resumed Suggest metadata job skips or narrows its duplicate-title pass. **Moved to Low:** the pass only runs when `LLM_DEDUPE_TITLES=true`, which defaults to false and is set in no environment. | Reproduced with the setting on: restart during the pass → `retitle calls after restart=0`, 4 duplicate URLs left; restart mid-run → the group titled before it left out. |
| M8 | Migrations V11–V18 run in one transaction at startup. **Moved to Low:** measured, not a risk at realistic size. | On 1.77 GB (512K dump rows, 1.13M rule effects, 5 × 100K collections): V11–V18 took 0.87 s, the engine answered `/health` 1.5 s after launch (budget about 180 s). The key backfill (760K rows, about 17 s) runs after the engine is listening. Local SSD, not RDS; test's row counts still worth a look. |
| L1 | Prod-publish attempt totals double-count after two restarts (report note only). | Reproduced: `attempts=[{indexed 2},{indexed 2}]`, `indexed=9` for 7 pages. Needs the second restart before the resumed publisher reports progress. |
| L2 | Resumed LLM jobs ask failed items again and count their failures twice. | Reproduced (metadata): `done=7 failed=2` for 8 pages. Patterns not tested. |
| L3 | ~~Scrape's shutdown path leaves a late progress publish.~~ **Withdrawn.** | Not reproduced in 5 runs: `shutdown()` cancels delayed publishes before it cancels jobs. |
| L4 | Every ✓/✗ that removes an existing exact rule takes the full recompute (correct, slower). | Reproduced: `include → ['scoped-refused', 'full']`. |
| L5 | The canonical-key backfill rescans the table for each batch, and can deadlock with a promote's write. | Reproduced: buffers read grow to the whole table by the last batch (cheap at 100K: about 1 s in all). Deadlock in 1 of 4 orderings without help; if the promote is the victim the curator gets a 500 (`DeadlockDetected` is not treated as "busy"). Only while the backfill runs after the first deploy. |
| L6 | `replace_pattern_suggestions` writes without marking the change. Unused today. | Reproduced: page shows 0 suggestions, table has 2. Delete it or mark it. |
| L7 | A review round stays open after its queue empties without a full promote. | Reproduced via Re-curate everything → partial promote of all but one → ✗ the last: `delta_count=0 status=curated review_round=True`; a later edit set back to its original value stays queued. |
| L8 | A race in the 30 s login cache keeps a just-demoted user's old row. | Reproduced: role in DB `curator`, cached `admin`, the demoted user's admin-only page answers 200 for up to 30 s. |
| L9 | Runbooks tell operators to check `/health` for `"db": "ok"`. | Confirmed: `docs/rds-cutover.md:23`, `docs/rds-migration.md:127`; `/health` no longer has that field; `/health/db` does and requires login (paths are matched exactly). |
| L10 | A stale crawl-failure flag makes the scoped recompute's `kept` count differ from the full one. | Reproduced: full `kept=0`, scoped `kept=1`. Wider than first stated: the page (`step_context`) shows it too; the diff never clears the flag on modified rows. |
| L11 | The 2 s slow-statement log writes bind parameters, with no log retention. | Confirmed: no retention on the RDS log group; a local run logged a full 20,000-character parameter. Correction: the log export already existed at `d8cbfbb`; only the 2 s threshold is new; page text is written by COPY, URL arrays are bind parameters. |
| L12 | The `DbConnections > 80` alarm cannot fire from the engine (maximum 28 connections). | Confirmed from the template and settings. |
| L13 | Rollback runbook: `--max-allocated-storage 100` vs dev's 200 (test 500); reverting removes the parameter group, which reboots again. | Confirmed: `docs/dev-db-rollback.md:212, :308`. |
| L14 | `ecs:StopTask` covers every task in the indexer cluster. | Confirmed from the template: no condition or tag scope. |
| L15 | Stale documents. | Confirmed, wider than stated: `docs/architecture.md:368, 425, 447, 461` and `README.md:394–395, 449` say LLM calls across jobs are unbounded; the README lacks `LLM_WORKERS_TOTAL` and the four `RESUME_*` settings; `CHANGELOG-2026-10-06.md:147` says "no alarms"; `.github/workflows/deploy.yml:9` says in-flight jobs are marked failed on deploy. |
| L16 | Suggest metadata counts each page's tokens on the event loop: 0.2–0.4 s freezes at 100K (also in the baseline). | Measured (stress run). |
| L17 | Cancelling a metadata job during its database write. | Reproduced, worse than first stated: the batch being written is lost (`answers received=400 saved=0` with a widened batch), and the resume asks those pages again (paid twice); about 25 pages per write in progress at shutdown with the real batch size. The final data is correct. |

---

## 4. Introduced on this branch, or already on dev?

Not every finding is a regression. Each one falls in one of three groups, decided by comparing with
dev's code (`d8cbfbb`). On dev, an engine restart fails every job except two kinds: scrapes
resume, and Suggest metadata restarts as a new "missing only" job. A cancel never stops an ECS
indexer.

### Regressions: dev is correct, this branch is not

| Finding | Introduced by | On dev |
|---|---|---|
| H1: an edit before Start curating queues one page | `a62743d` (Tier 4, per-URL recompute) | Full recompute, correct (reproduced on both builds) |
| M9: a lost change mark leaves page counts stale | `e42c1e2`, `d508edc` (Tiers 2–3, stored counts) | Counts computed on every view, never stale |
| M10: the excluded count can go negative | `0361da2` (stored excluded count) with `a62743d` (scoped update) | Counted live: correct, but slow (the 7 s count behind the 2026-10-06 crash) |
| L5: the key backfill can deadlock a promote (500) | `a62743d` (V18 backfill) | No backfill |
| L8: the login cache race keeps a demoted user's role for 30 s | `e42c1e2` (login cache) | Looked up on every request |
| L10: the scoped "kept" count differs from the full one | `a62743d` | One path only, no mismatch |

H1 is the only serious one. M9 and M10 come from the new stored counts.

### Gaps in new features: incomplete, but not worse than dev

| Finding | Why it is not worse than dev |
|---|---|
| H2: an interrupted dispatch starts a second indexer | On dev the job fails while the started task keeps running; re-running it by hand gives the same duplicate. The branch now does it automatically. |
| M2: cancel while waiting to resume leaves the task running | On dev, cancel never stops an ECS indexer at all. |
| M3: one error strands the resume queue | The resume queue is new. |
| M1: the database reboot | The parameter group is new. A one-time cost of turning on query statistics, not a bug. |
| M6, M7, L12: alarm gaps | Dev has no alarms. |
| M11, L13: rollback runbook | The runbook is new. Its "jobs are marked failed" line was already partly wrong on dev, which resumes scrapes. |
| L1, L2: resume counters | The counters exist only with resume. |
| L7: a review round left open | Before `f45b3fc`, the whole re-curate queue was lost at the next edit. This is a leftover edge of that fix. |
| L4: some ✓/✗ still take the full recompute | Dev always takes it: a missed speed-up, not a bug. |
| L6, L9, L11, L14, L15 | A latent unused writer; documents left stale by the changes; the new 2 s slow-statement threshold; the new `ecs:StopTask` permission. |

### Already on dev, carried over

| Finding | On dev |
|---|---|
| M4: a restart narrows the duplicate-title pass (opt-in setting) | Same: dev restarts metadata as a new job that only sees its own titles. |
| M5: a resumed "redo all" ends as "missing only" | Same: dev restarts it as a new "missing only" job. |
| L16: token-count freeze in Suggest metadata | Same code, measured on both builds. |
| L17: the batch being written is lost at shutdown | Seen on both builds. |
| N1: an unexpected error in an LLM call or its result handler hangs the job | Same pool code on dev. |
| N2: Regenerate titles fails before the URL step and leaves duplicates | Same code on dev. |
| L10's underlying stale crawl-failure flag | The diff never cleared it on dev either; only the count mismatch is new. |

Also on dev, and already fixed on this branch: the 1.1 s Suggest patterns freeze (`9152447`) and
the 2026-10-06 crash (`0361da2`).

---

## 5. Checked and found sound

- **No database replacement on deploy.** The synthesized test and prod templates change only the
  database's parameter group name; EFS and the secrets are untouched.
- **Write coverage for stored counts.** Every write to the delta, curated, dump, rule, rule-effect
  and suggestion tables marks the change, except the unused writer in L6 and the key backfill
  (which writes only a column no count reads).
- **Stored-count logic.** A store is refused when the version moved during the count, including
  for a new row. Shared in-flight reads do not hold a connection while waiting.
- **SQL built from strings.** Every interpolated value is a constant, a fixed column list or a
  whitelisted name. No user input reaches it.
- **Rule counts in SQL** match the engine's: LIKE escaping of `\`, `%`, `_`, anchoring and case
  sensitivity; exact rules by canonical key.
- **Scoped recompute** equals the full recompute on every case the twin test builds (2,400 random
  edits), apart from H1, M10 and L10.
- **`/tab-body`** returns the page's tab body (same context, same template), behind the same login
  check as its neighbours.
- **Resume mechanics.** Restarts are counted before a job runs again; a pending resume blocks edits
  and new jobs; a curator's cancel of a running job fails it and stops its ECS task; the 3 s
  throttle never loses the final state; progress stays small; the shared LLM limit is released on
  error and cancel; the bulk changes rebuilt for resume are safe to run twice.
- **Suggest-patterns threads** touch only read-only data; the shared index is assigned on the event
  loop only.
- **Migrations** are fast at realistic size (M8) and additive; old code works on the new schema
  (rollback without a schema rollback).

---

## 6. Recommended order of work

1. **Before the merge to dev:** fix H1 and add its test. Merge to dev when nobody uses dev and no
   jobs run there (M1 reboots dev's database).
2. **Before the first deploy to test:**
   - fix H2, M2, M3 (resume code, small) and M10 (excluded count);
   - correct the reboot comment and plan the deploy as a database reboot (M1);
   - subscribe the alarm topic (M6) and add the missing alarms (M7);
   - update the runbooks: restored jobs (M11), health check (L9), rollback flags (L13).
3. **Soon after:** M5, M9 and the Low items, in any order.

---

## 7. Open questions

- Should `/health/db` stay behind login? It blocks external monitoring of the database check.
- Is the indexer ECS cluster shared with other workloads? (Decides L14.)
- Should a job's restart count reset when it makes progress? Today a long prod publish or metadata
  job fails after 3 deploys even if it was moving.
- A resumed job that follows a local or long-finished ECS indexer waits the full stall timeout
  before deciding. Is that acceptable?

---

## 8. Fixes (2026-10-09)

**Status:** all 19 code findings are fixed in the working tree. Nothing is committed. The 11
infrastructure, runbook and documentation findings are still open (section 9).

**How each fix was checked.** Every code finding had a test that failed before the fix. The test
was marked `xfail(strict=True)` until the fix, then the mark was removed and the test passed. For
three fixes without a test of their own (the cancel half of H2, the restart-limit half of M2, and
the job marker of M4/M5/L2), a new test was added and shown to fail with the fix taken out, or the
contract test was extended to check the real and fake databases against each other.

**Results after the fixes:**

| Level | Result | Time | Gate |
|---|---|---|---|
| Unit | 511 passed | 12 s | under 20 s: met |
| Integration | 209 passed | 99 s | under 90 s: **9 s over** |
| E2E | 56 passed | 111 s | under 4 min: met |

Lint (`ruff`) is clean. The L5 deadlock test depends on timing; it passed on 3 separate runs.

### 8.1 What was fixed

| # | Problem | Fix | Where |
|---|---|---|---|
| H1 | An edit before Start curating queues only that page. | New column `collections.deltas_current` (V19). A crawl ingest sets it to false; a full recompute sets it to true. The per-page recompute refuses while it is false, so the edit takes the full recompute and the whole crawl is queued. | `schema.py` V19; `db.py` `replace_dump`, `replace_deltas(full=)`; `curation.py` `_recompute_keys` |
| H2 | An interrupted dispatch starts a second ECS indexer. | The dispatch sends the run ID as the ECS `RunTask` `clientToken`. A repeat request with the same token starts no new task. A resumed job dispatches again and gets the same task, and does not export again once `run.exported` is saved. A curator's cancel during the dispatch waits for it (up to 60 s) and stops the task it started. | `backends/index.py`; `jobs.py` `_dispatch`, `_export_and_dispatch` |
| M2 | Cancelling an index job that waits to resume leaves its ECS task running. | A cancel of a waiting job, and the restart limit, stop the task by its saved ID and close the index run. | `jobs.py` `_stop_index_task` |
| M3 | One error in the resume loop strands every queued resume. | An error while resuming one job fails that job, with the reason; the loop goes on with the others. | `jobs.py` `_run_resumes` |
| M4 | A restart during the duplicate-title pass leaves duplicates (`LLM_DEDUPE_TITLES=true` only). | A resumed Suggest metadata job finds the pages it titled before the restart (by `ai_job`) and continues its duplicate-title pass. | `jobs.py` `_run_llm_metadata` |
| M5 | A resumed "redo all" Suggest metadata finishes as "missing only". | New column `delta_urls.ai_job` (V19): the job that last answered or failed on the row. The job stores its "missing only" choice in its progress. A resume keeps the choice and skips only the rows its own job settled. | `schema.py` V19; `db.py` `set_delta_ai`, `set_delta_ai_errors`, `count_deltas_for_llm`, `iter_deltas_for_llm`, `urls_titled_by_job`; `jobs.py` |
| L2 | A resumed metadata job asks a failed page again and counts it twice. | Same column: a page the job failed on is not asked again by that job. | as M5 |
| L17 | Answers being saved at shutdown are lost and asked again (paid twice). | A write that the shutdown interrupts puts its rows back in the buffer; the final flush writes them. | `jobs.py` `flush` |
| L1 | A prod publish resumed twice counts the first attempt twice. | When a resume records an attempt, it resets the attempt counters. | `jobs.py` `_run_publish_prod` |
| M9 | A lost change mark leaves stored page counts stale. | The change mark is now the last statement of the write's own transaction. The write and its mark commit or fail together. | `db.py` `_touches`, `_conn`, `changed` |
| M10 | The excluded count can be subtracted twice and go negative. | Deleting a per-URL exclude or include rule keeps the stored excluded count; the recompute that follows corrects it. A page view during a ✓ therefore cannot store a count in between. Deleting a glob rule still clears the count. | `db.py` `delete_pattern` |
| L4 | A ✓ that removes a per-URL exclude rule takes the full recompute. | Same change as M10: the count stays known, so the per-page recompute runs. | as M10 |
| L5 | The key backfill can deadlock a promote (the curator gets a 500). | The backfill selects each batch with `FOR UPDATE SKIP LOCKED` in the transaction that updates it. It never waits for a row a curator's write holds; skipped rows are taken by a later batch. | `db.py` `backfill_keys`, `_fill_keys` |
| L6 | An unused writer does not mark the change. | `replace_pattern_suggestions` is deleted. | `db.py` |
| L7 | A review round stays open after its queue empties. | A recompute that leaves an open round with no delta URLs ends the round. | `curation.py` `_recompute`, `_recompute_keys` |
| L8 | The login cache keeps a demoted user's old role for up to 30 s. | A lookup does not store a row it read before a role change cleared the cache entry. | `db.py` `session_user`, `_forget_session_user` |
| L10 | A changed page crawled again keeps its crawl-failure flag. | The diff clears the flag on a changed page too, not only on an unchanged one. | `engine/diff.py` |
| N1 | An unexpected error in an LLM call hangs the job forever. | A cancelled producer no longer waits to hand over its stop markers; the error fails the job. | `llm/pool.py` |
| N2 | Regenerate titles fails before the URL step and leaves duplicates. | A failed re-ask pass is reported in the progress (`titles_error`), and the job goes on to the URL step. | `jobs.py` `_retitle_duplicates` |

### 8.2 Test changes that go with the fixes

- The 19 `xfail` marks are removed; the section headers now say each test failed before its fix.
- New tests: a cancel during the dispatch stops the task (H2); an index job past its restart limit
  stops its task (M2); the contract test checks `ai_job`, `skip_job` and `urls_titled_by_job` on
  the real and the fake database.
- The fake indexer honours the dispatch token, as ECS does (`tests/support/engine.py`).
- Two test wrappers of `set_delta_ai` pass the new `job` keyword through.
- The e2e resume test leaves `ai_job` out when it compares the twins: it is a job ID, different in
  each run.
- The writer check exempts `_fill_keys` (it writes only `canonical_key`) instead of `backfill_keys`.

### 8.3 Behaviour to know about

- **Migration V19** adds two columns, both cheap: `delta_urls.ai_job` (nullable) and
  `collections.deltas_current` (default true). A collection that is crawled but not yet recomputed
  when V19 lands keeps the old H1 behaviour until its next Start curating.
- **ECS token window:** ECS keeps a `clientToken` for a limited time. How long is not verified. A
  resume after a longer outage could still start a second task.
- **Regenerate titles with a broken model:** when every model call fails, the job now ends with
  titles made from the URLs and records the error in `titles_error`. Before, the job failed.
- **Suggest metadata jobs started before this deploy** have no `ai_job` marks and no stored
  "missing only" choice. If one of them resumes after the deploy, it behaves as before the fix.
- **Contract test finding:** the first version of `urls_titled_by_job` also returned pages the job
  failed on, which keep an older title. The contract test caught it; both databases now leave those
  pages out.

---

## 9. Pending

### 9.1 Findings not fixed: infrastructure, runbooks and documents

| # | What to do | Before |
|---|---|---|
| M1 | Correct the reboot comment in `infra/stacks/engine_stack.py` and the deploy notes: the first deploy that attaches the parameter group reboots the database (about 1–2 minutes). Deploy when no jobs run. After the deploy, check that the `pg_stat_statements` extension exists. | the merge to dev |
| M6 | Subscribe the alarm topic. **Needs an email or chat address from Bernard.** | the first deploy to test |
| M7 | Add `HealthyHostCount < 1` for 3 minutes (missing data as breaching) and an alarm on `HTTPCode_ELB_5XX_Count`. | the first deploy to test |
| M11 | Correct `docs/dev-db-rollback.md:187` and `infra/README.md:91`: a restored database resumes its running jobs, including Index to prod. Tell the operator to mark them failed before starting the engine. | the first deploy to test |
| L9 | Correct `docs/rds-cutover.md:23` and `docs/rds-migration.md:127`: `/health` has no `"db"` field. | the first deploy to test |
| L13 | Correct `docs/dev-db-rollback.md:212, :308`: storage flag values; reverting removes the parameter group and reboots again. | the first deploy to test |
| L11 | Set a retention on the RDS log group. Decide whether the 2 s slow-statement log may contain bind parameters. | soon after |
| L12 | Remove or lower the `DbConnections > 80` alarm (the engine opens at most 28). | soon after |
| L14 | Scope `ecs:StopTask` to the engine's tasks (depends on the open question about the cluster). | soon after |
| L15 | Update stale documents: `docs/architecture.md:368, 425, 447, 461`; `README.md:394–395, 449` (add `LLM_WORKERS_TOTAL` and the `RESUME_*` settings); `CHANGELOG-2026-10-06.md:147`; `.github/workflows/deploy.yml:9`. | soon after |
| L16 | Move Suggest metadata's per-page token count off the event loop (0.2–0.4 s freezes at 100K). This is code, but not a regression. | soon after |

### 9.2 Decisions for Bernard

- **Promote vs Regenerate titles:** promote refuses some duplicate titles that Regenerate titles
  does not change (pages whose AI title is held back as pending). Align them, or accept it.
- **Access rules:** a curator can index to prod and read `/api/audit` and `/docs`. Keep, or make
  admin-only.
- **Unreachable code:** the last-admin lockout guard has a branch no request can reach. Remove it, or
  leave it as a safety net.
- **The open questions in section 7:** `/health/db` behind login; whether the indexer cluster is
  shared; resetting the restart count on progress; the stall-timeout wait after a finished indexer.
- **The `SMOKE_APP_PASSWORD_DEV` secret:** optional; add it or not.

### 9.3 Test strategy (TEST-STRATEGY-2026-10-09.md, step P5)

- **Integration time:** the level takes 99 s against its 90 s gate. The largest tests are the
  per-URL twin test (25 s) and the L5 deadlock test (18 s).
- **Faster integration level (Bernard's decision):** let the app run on the in-memory fake
  database, so most page and route tests move to the unit level.
- **CI timing:** measure the three levels in GitHub Actions and CircleCI.
- **Docs:** describe the test layout in the README and docs.
- **Recurring check:** run the mutation test on a schedule (last result: 195 of 250 mutants caught,
  78 %).

### 9.4 After the next deploy

- Confirm that `pg_stat_statements` exists (M1).
- Confirm that migration V19 ran and that the key backfill finished.
- Watch the first index run after a restart: one ECS task per run (H2).

### 9.5 Not started

- **A new assessment** of the current code: what is regressing and what is failing (Bernard's
  request).

### 9.6 Housekeeping

- **Commit:** nothing is committed. Old test files are deleted in the working tree only; run
  `git add -A tests` before the commit.
- **Local containers:** `mut-pg` (port 55480, used by the mutation runs) and `sde-prof` are still
  running. Remove or keep. The Postgres on port 5432 was not touched.
- **Backup:** a copy of `tests/` from before the old tests were deleted is in the session
  scratchpad (`tests-before-p4`). It is lost when the session's temporary files are cleared.
