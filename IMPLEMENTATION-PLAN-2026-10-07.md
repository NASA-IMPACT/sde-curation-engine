# Implementation plan: multi-curator scaling

Source documents: `ARCHITECTURE-ASSESSMENT-2026-10-07.md` (the reasoning) and
`CHANGE-DECISIONS-2026-10-07.md` (the approved GO / NO-GO list). Item numbers (#1, #2, …) are the
numbers in the decision table.

This plan contains only the approved GO items:
**#1–#12, #13, #14, #15, #16, #17, #18, #22, #23, #24, #29–#35.**

Not in this plan (NO-GO): #13b, #19, #20, #21, #25, #26, #27, #28, the per-run LLM budget, the batch API,
and the SQL recompute.

---

## Rules for the coding agent

Read these before starting. They apply to every step.

1. **Two check boxes per item.**
   - Tick **Done** when the code and tests for the item are written.
   - Tick **Validated locally** only when every check under "Validation" passes on this machine.
     Then add one line to the validation log at the end of this file: date, item, the commands run,
     and the result.
   - Never tick "Validated locally" for a check you did not run. If a check cannot run locally,
     write "NOT RUN: <reason>" in the log and leave the box empty.
2. **Order.** Work tier by tier, top to bottom. Inside a tier, follow the listed order unless the
   item says it is independent. Do not start an item whose "Depends on" items are not validated.
3. **Stop conditions.** Stop and report to Bernard, without working around it, when:
   - a validation check fails and the fix is not obvious;
   - a check marked **STOP IF** fails;
   - a change would make a curator see something different, and the item is not one of the
     approved visible items (#18, #22, #23, #24, #29–#35).
4. **Curators stay locked out of a collection while any job runs on it, as today.** No item in
   this plan may enable an edit, accept, promote or rule change during a job. (#19, #20 and #21 are
   NO-GO.)
5. **No curator-visible changes** except the approved ones. The page-snapshot guard (item T0.1)
   enforces this. Every invisible item must leave the snapshots unchanged, apart from the listed
   allowed differences.
6. **Git.** Do not commit, push, create branches or open PRs. Bernard does all git actions. Never
   add Co-Authored-By or other Claude attribution lines anywhere.
7. **Tests.** New tests go through the real API flows (`crawler_client`, `classify`, `wait_job` in
   `tests/conftest.py`). Use a direct database write in a test only to create a state the UI can
   reach. Tests need Docker (testcontainers) or `TEST_DATABASE_URL`.
8. **Migrations.** Use the next free number after V12 in the order you implement them. Every new
   migration must be additive: add tables, nullable columns, columns with a default, or indexes.
   Never rename or drop a column, and never add `NOT NULL` without a default. (Item #17 turns this
   rule into a test.)
9. **Standard checks.** Unless an item says otherwise, its validation always includes:
   ```
   make lint          # no errors
   make test          # all tests pass, including the new ones
   ```
   Infra items also run `make infra-test`.
10. **Deploys and AWS changes** are Bernard's. The agent edits the CDK code and runs the infra tests
   only.

---

## Progress overview

| Tier | Item | Done | Validated locally |
|---|---|---|---|
| 0 | T0.1 Page-snapshot guard (tooling) | [x] | [x] |
| 0 | T0.2 Baseline measurements (tooling) | [x] | [x] |
| 0 | #2 Event-loop lag probe | [x] | [x] |
| 0 | #14 Monitoring, slow-query log, alarms, app logging | [x] | [x] |
| 0 | #15 Autovacuum tuning | [x] | [x] |
| 1 | #1 Recompute stops overwriting AI columns | [x] | [x] |
| 1 | #18 Fix "Re-curate everything" | [x] | [x] |
| 2 | #3 Row objects and COPY data off the event loop | [x] | [x] |
| 2 | #4 Partial promote computed in a thread | [x] | [x] |
| 2 | #5 Repeated lookups fetched once per page | [x] | [x] |
| 2 | #7 Mark data changed only on real writes | [x] | [x] |
| 2 | #8 Job progress sent at most every 3 s | [x] | [x] |
| 2 | #9 Promote writes only changed curated rows | [x] | [x] |
| 2 | #16 Login lookups cached for 30 s | [x] | [x] |
| 3 | #11 Indexes, including the duplicate-title check | [x] | [x] |
| 3 | #10 Exact stored counts | [x] | [x] |
| 3 | #6 `#job-watch` fetches only the tab body | [x] | [x] |
| 4 | #13 Scoped recompute for per-URL edits | [ ] | [ ] |
| 4 | #12 Rules tab counts from the database (moved: needs #13's `canonical_key`) | [ ] | [ ] |
| 5 | T5.0 Safe-resume foundation (tooling) | [ ] | [ ] |
| 5 | #22 Index runs survive restarts; cancel stops them | [ ] | [ ] |
| 5 | #33 Index to test resumes during export | [ ] | [ ] |
| 5 | #32 Validate and validate-prod resume | [ ] | [ ] |
| 5 | #31 Index to prod resumes | [ ] | [ ] |
| 5 | #29 Suggest patterns resumes | [ ] | [ ] |
| 5 | #30 Regenerate titles resumes | [ ] | [ ] |
| 5 | #35 Suggest metadata resumes in place | [ ] | [ ] |
| 5 | #34 Recompute, bulk accept, bulk suggestions resume | [ ] | [ ] |
| 5 | #24 Shared limit on LLM calls across jobs | [ ] | [ ] |
| 6 | #17 Coordination in the database; safe migrations | [ ] | [ ] |
| 6 | #23 Separate web and worker tasks, rolling deploys | [ ] | [ ] |

---

## Tier 0: guards and measurements

Nothing in this tier changes what curators see. It gives every later item a way to prove that.

### T0.1 Page-snapshot guard (tooling, not in the decision table)

- [x] Done
- [x] Validated locally

**Why.** Bernard's rule is that curators must not notice any change except the approved ones. This
test makes that rule checkable.

**Changes.**
1. Add `tests/test_page_snapshots.py`. It builds a fixed collection through the real API with the
   fake LLM: scrape, recompute, Suggest patterns, accept some suggestions, Suggest metadata, accept
   some AI values, partial promote, then a few more edits.
2. It renders these pages and fragments and saves the HTML:
   - the dashboard, `/rows`, `/jobs/panel`;
   - `/collections/{id}` for each tab: overview, curate (with and without `focus`), dump, delta,
     curated, rules, activity;
   - `/collections/{id}/header`, `/collections/{id}/pipeline`;
   - the same collection page while a fake-LLM metadata job is running (job paused by a test hook).
3. Normalize before comparing: timestamps, relative times ("3 minutes ago"), job and run ids, and
   `?v=` static hashes.
4. Store snapshots under `tests/snapshots/`. Regenerate only with `UPDATE_SNAPSHOTS=1`.

**Validation.**
- [x] `make test` passes twice in a row with no snapshot changes. The test is deterministic.
- [x] A deliberate one-word change in a template makes the test fail. Revert it afterwards.

### T0.2 Baseline measurements (tooling)

- [x] Done
- [x] Validated locally

**Why.** Several items claim "faster only". Each needs a before and after number.

**Changes.**
1. Using the stress tools in `~/projects/sde-curation-stress/` (outside the repo; keep it there),
   run the local 100K profile against `make run` with `LLM_PROVIDER=fake`. Use `newrun.sh` so the
   run gets its own timestamped folder. Use `stress.py` and `profile_pages_edits.py`.
2. Record in the validation log:
   - worst event-loop freeze during a per-URL edit (needs #2, so re-run after #2 lands);
   - server time of one per-URL title edit on 100K URLs with about 300K rules;
   - p95 time of the Curate page, the Delta tab and the Rules tab on that collection;
   - number of SQL statements for one Curate page render;
   - time of one duplicate-title scan (`EXPLAIN ANALYZE` of `duplicate_title_counts`'s query).

**Validation.**
- [x] All five numbers are in the log, with the results folder path.

### #2 Event-loop lag probe

- [x] Done
- [x] Validated locally

**What curators see.** None.

**Changes.**
1. New module `sde_curation/looplag.py`. It runs a background task that sleeps 100 ms in a loop and
   measures how late each wake-up is. It keeps the last lag and the maximum lag since the last read.
2. Start it in the app lifespan (`web/app.py`, `lifespan`) and stop it at shutdown.
3. Log a WARNING when one lag exceeds 250 ms. Include the lag in milliseconds.
4. Add `loop_lag_ms: {"last": n, "max": n}` to the `/health/db` JSON. Reading it resets `max`.
   Leave `/health` unchanged.

**Validation.**
- [x] New test: a route-free coroutine that calls `time.sleep(0.4)` on the loop makes the probe
      report a max of at least 300 ms.
- [x] `/health` response is unchanged (existing `tests/test_db_health.py` passes).
- [x] Re-run the T0.2 freeze measurement and record the baseline.
- [x] Snapshot guard unchanged.

### #14 Monitoring, slow-query log, alarms, app logging

- [x] Done
- [x] Validated locally

**What curators see.** None while running. Applying the RDS parameter group needs one reboot of
about a minute. Bernard schedules the deploy.

**Changes.**
1. App logging: in `create_app`, if the root logger has no handler, configure it at INFO with a
   format that includes time, level, logger name and message. Do not change uvicorn's own loggers.
2. Migration: `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`, inside a `DO` block that ignores
   the "not available" error, so local and test databases without it still migrate.
3. CDK (`infra/stacks/engine_stack.py`): an RDS parameter group for Postgres 17 with
   `shared_preload_libraries = pg_stat_statements`, `pg_stat_statements.track = top`,
   `log_min_duration_statement = 2000`. Attach it to the instance.
4. CDK: an SNS topic with no subscription (Bernard subscribes), and these CloudWatch alarms sending
   to it:
   - RDS `CPUUtilization` above 70 % for 5 minutes;
   - ALB `HTTPCode_Target_5XX_Count` above 10 in 5 minutes;
   - ALB `TargetResponseTime` p95 above 5 s for 5 minutes;
   - target group `UnHealthyHostCount` at least 1 for 2 minutes.
   - T5.0 adds three more: RDS `FreeableMemory`, RDS `DatabaseConnections`, engine task
     `MemoryUtilization`.

**Validation.**
- [x] `make infra-test` passes, with new assertions in `infra/tests/test_synth.py` for the
      parameter group values and the four alarms.
- [x] Locally, `make run` prints INFO lines from `sde_curation.*` loggers (for example a job start).
- [x] The migration applies on the local compose database and in tests.
- [x] Snapshot guard unchanged.

### #15 Autovacuum tuning

- [x] Done
- [x] Validated locally

**What curators see.** None.

**Changes.**
1. Migration: for `delta_urls`, `pattern_effects` and `patterns`, run
   `ALTER TABLE … SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.02)`.

**Validation.**
- [x] New test reads `pg_class.reloptions` for the three tables and finds both settings.
- [x] Snapshot guard unchanged.

---

## Tier 1: correctness

### #1 Recompute stops overwriting the AI suggestion columns

- [x] Done
- [x] Validated locally

**What curators see.** None. A rare lost or returning suggestion stops happening.

**Changes.**
1. In `Database.replace_deltas` (`db.py`), define the AI columns: `title_ai`, `division_ai`,
   `document_type_ai`, `title_ai_conf`, `division_ai_conf`, `document_type_ai_conf`, `ai_model`,
   `ai_content_hash`, `ai_error`, `ai_failures`, `title_ai_before`, `division_skipped`.
2. Keep them in the COPY and in the `INSERT` column list, so a new delta row still gets the values
   carried forward from the previous row.
3. Remove them from the `ON CONFLICT … DO UPDATE SET` list and from the `IS DISTINCT FROM`
   comparison. An existing row keeps the AI values that are in the table at write time.
4. Check every other writer of these columns: `set_delta_ai`, `set_delta_ai_errors`,
   `set_delta_ai_titles`, `clear_delta_ai`, `clear_delta_ai_field`, and the division-assignment
   clear. They stay the only writers for existing rows.

**Validation.**
- [x] New test (interleaving): monkeypatch `Database.load_dump_failures`, the last load before the
      recompute computes, so it first writes `title_ai = "X"` to a delta row with `set_delta_ai`,
      then returns normally. Make one per-URL edit on another row. After the edit, the first row
      still has `title_ai = "X"`.
- [x] Same shape for a reject: the hook calls `clear_delta_ai` on a row with a pending title. After
      the edit, the suggestion stays cleared.
- [x] Existing `tests/test_scale.py::test_an_edit_rewrites_only_the_rows_it_changes` passes.
- [x] Existing LLM and review-round tests pass unchanged.
- [x] Snapshot guard unchanged.

### #18 Fix "Re-curate everything"

- [x] Done
- [x] Validated locally

**What curators see.** After a re-curate, the queue stays full when they edit a row. (Approved
visible change.)

**Changes.**
1. Migration: `ALTER TABLE collections ADD COLUMN review_round boolean NOT NULL DEFAULT false`.
   Add `review_round: bool = False` to `models.Collection`.
2. `engine/diff.py`, `recompute()`: add a parameter `keep_queued: bool = False`. When it is true, an
   included page whose values match the curated row is still queued as `modified` **if its URL was
   in `previous`** (the delta rows before this recompute). Pages not in `previous` follow the normal
   rules. `review_all=True` keeps its current meaning.
3. `CurationService._recompute`: pass `keep_queued=c.review_round`.
4. `api_recompute` (`web/app.py`): when `all=true` and the result has delta rows, set
   `review_round = true`.
5. Set `review_round = false`:
   - at the end of a full promote;
   - at the end of a partial promote that empties the queue (where `set_flag(…, False)` already
     runs);
   - when a new dump is ingested (`replace_dump`).
6. Partially promoted rows are not in `previous` after their promote, so they are not queued again.

**Validation.**
- [x] Add to `tests/test_review_round.py`: re-curate, then one per-URL title edit. The queue size is
      unchanged and the AI suggestions on the other rows are still there. (This is the probe from
      the assessment; today it fails with "queue shrank from 8 to 1".)
- [x] Re-curate, then add an exclude glob. Only the newly excluded rows leave the queue.
- [x] Re-curate, partially promote three rows, then edit another row. The three promoted rows do not
      come back.
- [x] Re-curate, promote all, then "Check for changes". Nothing is queued, and `review_round` is
      false.
- [x] Re-curate, then re-crawl. `review_round` is false after the ingest.
- [x] Existing `test_re_curate_puts_the_whole_collection_back_in_the_queue` passes unchanged.
- [x] Snapshot guard: no change in the default fixture. (Re-curate is not part of the fixture.)

---

## Tier 2: server freezes and per-request cost

All items in this tier are invisible to curators. Each one must leave the snapshots unchanged.

### #3 Row objects and COPY data off the event loop

- [x] Done
- [x] Validated locally

**Depends on.** #2.

**What curators see.** Faster only. Other curators stop freezing while someone edits.

**Changes.**
1. In `db.py`, for `load_dump`, `load_curated`, `load_deltas`, `dump_content_hashes`,
   `load_dump_failures` and `deltas_with_ai`: fetch the rows, release the connection, then build
   the models or dicts in `asyncio.to_thread`.
2. `load_rules`: collect the `fetchmany` tuples, then build the `Rule` objects in one
   `asyncio.to_thread` call after the cursor closes.
3. `replace_deltas`, `replace_curated`, `insert_patterns`: build the COPY row tuples in
   `asyncio.to_thread`. Then write them with `copy.write_row`, and `await asyncio.sleep(0)` after
   every 2,000 rows so other requests run between chunks.
4. `effects` in `replace_deltas`: build `set(effects)` in the same thread call.
5. Do not change any SQL.

**Validation.**
- [x] New test: seed a 100,000-URL collection through `seed_dump`, run one per-URL edit, and read
      `loop_lag_ms.max` from `/health/db`. The worst freeze is under 9 % of the edit's own time.
      (Changed twice: "20,000 URLs, under 250 ms" also passed on the old code; "100,000 URLs, under
      200 ms" failed on a slower CI runner. See the validation log.)
- [x] T0.2 measurement at 100K: worst freeze during a per-URL edit is at most 0.3 s. Record the
      before and after numbers. (The 2026-09-18 audit measured 2.8 s worst.)
- [x] The per-URL edit's own server time is not worse than the baseline by more than 10 %.
- [x] Snapshot guard unchanged.

### #4 Partial promote computed in a thread

- [x] Done
- [x] Validated locally

**What curators see.** Faster only.

**Changes.**
1. `CurationService.promote_urls` (`curation.py`): wrap the `promote(...)` call in
   `asyncio.to_thread`, as `_promote` already does.

**Validation.**
- [x] Existing `tests/test_promote_selection.py` passes.
- [x] Code check: no call to `promote(` in `curation.py` outside `asyncio.to_thread`.
- [x] Snapshot guard unchanged.

### #5 Repeated lookups fetched once per page

- [x] Done
- [x] Validated locally

**What curators see.** None.

**Changes.**
1. Add a per-request memo helper in `web/app.py`: `memo(request, key, factory)`. It stores results
   in `request.state` for the life of one request.
2. Use it for `latest_job(cid)`, `last_index_run(cid, target)`, `list_jobs(cid, limit)`,
   `list_index_runs(cid, limit)`, `latest_job_of_kind(cid, kind)` and
   `count_deltas_for_llm(cid, only_missing)` in `header_context`, `step_context`, `tab_context`,
   `curate_context` and `with_validation`.
3. Hand each caller its own copy of a list or dict result, as `_coalesced` already does.

**Validation.**
- [x] New test: count calls to `Database.latest_job` and `Database.last_index_run` during one
      Curate page GET (monkeypatch wrappers). `latest_job` at most 1. `last_index_run` at most 2
      (test and prod).
- [x] T0.2 statement count for one Curate render drops. Record the number.
- [x] Snapshot guard unchanged.

### #7 Mark data changed only on real writes

- [x] Done
- [x] Validated locally

**What curators see.** None. Counts still update when the data changes.

**Changes.**
1. The bus listener in `lifespan` (`touch_collection`) touches a collection only when the event is
   a curator change (no `job` key), or the job's state is `succeeded` or `failed`.
2. In `jobs.py`, call `db.touch(cid)` after every write that changes page data during a job:
   `set_delta_ai`, `set_delta_ai_errors`, `set_delta_ai_titles`, `add_pattern_suggestions`, the
   ingest commit, the index-run state updates and status changes.
3. `DbScope` keeps touching after every non-GET request, as today.

**Validation.**
- [x] New test: publish a progress event for a running job. `Database._gens[cid]` does not change.
- [x] New test: a fake-LLM metadata flush changes `_gens[cid]`, and the next Curate render shows the
      new suggestion count.
- [x] Existing `tests/test_busy_database.py` passes.
- [x] Snapshot guard unchanged, including the "job running" snapshot.

### #8 Job progress sent at most every 3 s

- [x] Done
- [x] Validated locally

**What curators see.** None. The browser already shows at most one refresh per 3 s per element.

**Changes.**
1. In `JobManager._progress_cb` and the other progress closures in `jobs.py`, merge every progress
   update into `job.progress` at once, but run `update_job` and `_emit` at most once every 3 s per
   job.
2. Keep a trailing emit: if updates arrived since the last emit, emit them when the 3 s window
   ends.
3. Job state changes (`finish_job`, cancel, failure) always emit at once, and flush any pending
   progress first.

**Validation.**
- [x] New test: ten progress updates within one second produce at most two publishes, and the last
      value published equals the last update.
- [x] New test: a job that finishes 0.5 s after a progress update publishes its final state without
      waiting.
- [x] Existing `test_progress_events_refresh_the_header_and_stepper_at_most_every_few_seconds`
      passes.
- [x] Snapshot guard unchanged.

### #9 Promote writes only changed curated rows

- [x] Done
- [x] Validated locally

**What curators see.** Faster only. Promote stays in the request.

**Changes.**
1. In `Database.replace_curated`, add to the `ON CONFLICT (collection_id, url) DO UPDATE` a
   `WHERE (curated_urls.<cols>) IS DISTINCT FROM (EXCLUDED.<cols>)` over every updated column, as
   `replace_deltas` already does.
2. Check that the returned count and `_recount_curated` stay correct when few rows change.

**Validation.**
- [x] New test, same shape as `test_an_edit_rewrites_only_the_rows_it_changes`: promote, edit one
      row, promote again. Only that curated row gets a new row version (`xmin`); the others keep
      theirs.
- [x] Existing promote and curated-count tests pass.
- [x] T0.2: time one full promote on the 100K collection after a one-row change. Record before and
      after.
- [x] Snapshot guard unchanged.

### #16 Login lookups cached for 30 s

- [x] Done
- [x] Validated locally

**What curators see.** None. An admin who deactivates a user sees it take effect up to 30 s later
on other engine tasks; on the same task it is immediate.

**Changes.**
1. In `web/auth.py`, cache `get_user` results per `(user id, session_version)` for 30 s in a
   process-local dict.
2. Every database method that changes a user (`create_user`, password change, deactivate, role
   change) removes that user's entries from the cache in the same process.
3. A cached user that is inactive, or whose `session_version` no longer matches the cookie, is
   treated exactly as today.

**Validation.**
- [x] New test: ten authenticated GETs run `get_user` once.
- [x] New test: deactivating a user, then a request with their cookie, is refused at once.
- [x] New test: a password change ends the old session at once.
- [x] Existing `tests/test_auth.py` passes.

---

## Tier 3: stored counts and indexes

### #11 Indexes, including the duplicate-title check

- [x] Done
- [x] Validated locally

**What curators see.** Faster only.

**Changes.**
1. Migration, plain `CREATE INDEX` (the migrator runs in one transaction):
   - `delta_urls (collection_id, kind, excluded)`;
   - `delta_urls (collection_id, renamed_from) WHERE renamed_from IS NOT NULL`;
   - `patterns (collection_id, id)`;
   - `pattern_effects (collection_id, field)`.
2. **Not done, by measurement (2026-10-08).** Duplicate-title check: add expression indexes, not
   stored columns, so no table is rewritten. Measured on a promoted, re-curated 100K collection: the
   scan reads every included row of both tables and sorts by the key it computes (EXPLAIN: sequential
   scans and a sort), so the indexes were not used and changed nothing (111–130 ms with, 109–134 ms
   without). They would only slow every write. The scan is under the 0.5 s limit without them.
   - On `delta_urls`: the pending-key expression and the effective-key expression used by
     `_projected_titles(pending=True)` and `_projected_titles(pending=False)`, each with
     `collection_id` first.
   - On `curated_urls`: the curated-key expression.
   - Move each expression into one Python constant used by both the query and the index, so they
     cannot drift apart.
3. Measure each migration's run time on the local 100K collection. Record it. The ALB grace period
   is 120 s.

**Validation.**
- [x] `EXPLAIN` of each `list_deltas` filter (kind, excluded, renamed) uses the new index on the
      100K collection.
- [x] `EXPLAIN ANALYZE` of the duplicate-title scan at 100K: under 0.5 s. Record before and after.
      **STOP IF** it is not under 0.5 s. Report the plan and the number to Bernard. Do not
      substitute a background or stale count (#27 is NO-GO).
- [x] Migration time on the 100K collection recorded and under 60 s.
- [x] Snapshot guard unchanged.

### #10 Exact stored counts

- [x] Done
- [x] Validated locally

**Depends on.** #11.

**What curators see.** Faster only. The numbers are identical.

**Built differently from the steps below (2026-10-08), same result.** Recounting in every writer's
transaction would run the accept-all counts (~100 ms each at 100K) on every Suggest-metadata flush,
every few seconds: the cost moves to the job instead of going away. Instead, every write bumps
`collection_stats.version` after it commits (`Database.changed`, through `_touches`); the first page
view after a change counts once and stores the result for that version (`db._stored`); every later
view reads it. A store is refused if the version moved meanwhile, so a stored count is never older
than the newest committed change. Jobs and actions still count from the tables.

**Changes (as planned; superseded by the paragraph above).**
1. Migration: table `collection_stats`, one row per collection, with these integer columns
   (all `NOT NULL DEFAULT 0`) and an `updated_at`:
   - delta: `delta_new`, `delta_modified`, `delta_deleted`, `delta_content_changed`,
     `delta_renamed`;
   - curated: `curated_excluded`, `curated_unreachable`;
   - rules: `pattern_count`;
   - LLM: `llm_candidates_missing`, `llm_candidates_all`;
   - AI: `ai_pending_title`, `ai_pending_division`, `ai_pending_document_type`,
     `ai_accept_title`, `ai_accept_division`, `ai_accept_document_type` (the `skip_human` counts).
2. `Database.refresh_stats(conn, cid, groups)`: recounts the named groups (`delta`, `curated`,
   `rules`, `llm`, `ai`) with the same SQL the live count functions use today, and upserts the row.
   It runs on the writer's connection, inside the writer's transaction.
3. Call it from every writer, with the groups that writer changes:

   | Writer | Groups |
   |---|---|
   | `replace_deltas` | delta, llm, ai |
   | `delete_deltas` | delta, llm, ai |
   | `replace_curated`, `set_curated_excluded`, `set_curated_crawl_failure` | curated |
   | `insert_pattern(s)`, `delete_pattern(s)`, `delete_exact_patterns` | rules, ai |
   | `set_delta_ai`, `set_delta_ai_errors`, `set_delta_ai_titles`, `clear_delta_ai`, `clear_delta_ai_field` | llm, ai |
   | `replace_dump` | all |

4. Page code in the read scope (`step_context`, `curate_context`, `header_context`) reads
   `collection_stats` instead of calling the live count functions. Code in the work scope (jobs,
   actions, the promote gate) keeps calling the live functions.
5. A missing stats row is computed once on first read and stored, as `excluded_count` does today.
6. The duplicate-dependent counts (`incomplete_counts`, `duplicate_title_counts`,
   `count_delta_ai` with duplicates) stay live queries. #11 makes them fast.

**Validation.**
- [x] New test helper `assert_stats_match(cid)`: compares every stored column to its live count.
- [x] Call it after each writer in a real flow: scrape, recompute, rule add and delete, per-URL edit,
      exclude toggle, Suggest patterns, accept-all suggestions, Suggest metadata, accept and reject
      one AI value, accept-all AI, regenerate titles, partial promote, full promote, re-crawl,
      re-curate.
- [x] T0.2: Curate page p95 and statement count at 100K. Record before and after.
- [x] Snapshot guard unchanged. The numbers on every page are identical.

### #6 `#job-watch` fetches only the tab body

- [x] Done
- [x] Validated locally

**Depends on.** #10.

**What curators see.** None. The same updates at the same rhythm.

**Changes.**
1. New route `GET /collections/{id}/tab-body` with the same query parameters as the collection
   page. It renders exactly the `<div id="tab-body" …>` element of `collection.html`. Built with the
   page's full context (header context included, its lookups shared through the #5 memo), so the
   tab body cannot differ from the page's.
2. Move the `#tab-body` block of `collection.html` into a partial used by both the page and the new
   route, so the two cannot drift apart.
3. In `collection.html`, change only `#job-watch`'s `hx-get` to the new route. Keep its
   `hx-trigger`, `hx-select`, `hx-target`, `hx-swap` and `hx-sync` exactly as they are.

**Validation.**
- [x] New test: for each tab, the `#tab-body` element of the full page and the new route's response
      are identical after normalization.
- [x] New test: one `#job-watch` refresh on the Curate tab runs fewer statements than the full page.
      Record both numbers.
- [x] Snapshot guard: the only allowed difference is `#job-watch`'s `hx-get` value. Update that
      snapshot line and note it in the log.
- [x] Manual check with `make run`: start a fake-LLM metadata job, keep the Curate tab open, and
      watch the counts and review rows update every 10 s as before.

## Tier 4: fast per-URL edits

### #13 Scoped recompute for per-URL edits

- [ ] Done
- [ ] Validated locally

**Depends on.** #1, #3, #10.

**What curators see.** Faster only. The resulting rows are identical. Bernard approved this as an
exception to the 2026-09-18 "no partial recomputes" rule, on the condition that the equivalence
test below proves the scoped result equals the full one.

**Changes.**
1. Migration: nullable `canonical_key text` on `dump_urls`, `curated_urls` and `patterns`, with
   indexes `(collection_id, canonical_key)`.
2. Every writer of those tables fills `canonical_key` with `engine.urls.canonical_key` (for
   `patterns`, only exact rules; globs stay NULL).
3. Backfill: a startup task fills NULL keys in batches of 5,000, per collection, each batch in its
   own short transaction. A collection is "keyed" when it has no NULL keys left.
4. **Write the equivalence test before the scoped code** (see Validation). It must fail first,
   because the scoped function does not exist yet.
5. `CurationService.recompute_keys(c, keys)`:
   - loads the dump and curated rows whose `canonical_key` is in `keys`;
   - loads every glob rule of the collection, and the exact rules whose `canonical_key` is in
     `keys`;
   - loads the previous delta rows for those URLs and the crawl failures for them;
   - runs the existing pure `recompute()` on that input, with the same collection settings
     (`capped`, `division`, `keep_queued` from #18);
   - writes the result with a new `Database.replace_deltas_scoped(cid, urls, deltas, effects,
     excluded_delta)`. It deletes and upserts only those URLs' delta rows and effects, adjusts
     `delta_count` and `excluded_count` by the difference, and runs `refresh_stats`. It follows the
     AI-column rule from #1.
   - applies the curated write-backs (`edited_by`, `crawl_failure`, `excluded`) for those URLs only.
6. Use the scoped path only for a rule whose `match` is exact, in: `replace_exact_pattern`,
   `set_excluded`, the no-op edit in `api_url_edit`, and the per-row AI accept. Use it only when the
   collection is keyed and `review_all` is false. Everything else keeps the full recompute: globs,
   bulk accepts, Start curating, Check for changes, Re-curate everything, re-crawl.
7. `exact_patterns_for` uses the `canonical_key` index instead of scanning with `position()`.

**Validation.**
- [ ] Equivalence property test (`tests/test_scoped_recompute.py`): build random collections (dump,
      curated set with renames, failures, a capped crawl, globs of every type, exact rules for
      several spellings). Apply random sequences of per-URL edits: title, division, document type,
      exclude, include, AI accept, rule delete. After each edit, compare the scoped result with a
      full recompute run on a copy: delta rows (every column), `pattern_effects`, curated
      write-backs, `delta_count`, `excluded_count`, `collection_stats`. At least 300 sequences.
      **STOP IF** any case differs and the cause is not a bug in the scoped code. Report it to
      Bernard; do not ship #13.
- [ ] Backfill test: a collection created before the migration gets keys, and the scoped path is
      used only after that.
- [ ] T0.2: per-URL title edit at 100K with about 300K rules under 0.5 s server time. Record before
      (baseline about 5.8 s) and after.
- [ ] Snapshot guard unchanged.

---

### #12 Rules tab counts from the database

**Moved from Tier 3 (2026-10-08). Depends on #13.** A per-URL rule matches by canonical key. In SQL
that can only be approximated today (`match_clause` misses, for example, a host written in mixed
case), so the counts would not be guaranteed identical, which is this item's condition. #13 adds a
stored `canonical_key` column; with it the count is exact and indexed. Today's Rules tab costs
0.14–0.21 s at 100K, so the wait costs little.

- [ ] Done
- [ ] Validated locally

**What curators see.** Faster only. The counts must be identical.

**Changes.**
1. In `CurationService._with_stats`, compute `matches` per rule with SQL instead of loading every
   URL into Python:
   - a glob: `COUNT(*)` over the set's table with `url LIKE glob_to_like(match)`;
   - an exact rule: the same count using `match_clause(match, 'url')`, which already handles every
     spelling of the page.
   - Batch the rules on the page into one statement (`UNION ALL` or a `VALUES` join).
2. Exclude rules count over the dump, as today.
3. `effect_counts` (the "superseded" marker) is unchanged.

**Validation.**
- [ ] New test: on a fixture with globs, exact rules, `%` and `_` in URLs, and several spellings of
      one page, the SQL counts equal today's `match_counts` result for every rule and every set.
- [ ] Existing `test_rules_tab_pages_the_per_url_rules` passes.
- [ ] T0.2: Rules tab p95 at 100K. Record before and after.
- [ ] Snapshot guard unchanged.

---

## Tier 5: jobs

### T5.0 Safe-resume foundation (tooling for #22 and #29–#35)

- [ ] Done
- [ ] Validated locally

**Why.** Bernard's condition for the resume items: resuming must never cause an engine shutdown, a
restart loop, or a memory or connection outage on RDS. Today all interrupted jobs would come back at
the same moment the new engine starts. That is the same pile-up shape as the 2026-10-06 crashes. This
item builds one resume path with those limits, and every resume item uses it.

**What curators see.** None by itself.

**Changes.**
1. **Resume is never part of startup.** `JobManager.recover()` only decides which jobs to resume and
   marks them. The resumes themselves start from a background task after the app is serving, after
   a delay of `RESUME_START_DELAY_S` (default 15 s). `/health` never waits on a resume.
2. **Count restarts before resuming.** Store `progress.restarts` and write it to the database
   before the job's work starts again. A job that crashes the engine during its resume is therefore
   counted. After `RESUME_MAX_RESTARTS` (default 3, the same as scrapes today) the job fails with
   "stopped resuming after 3 engine restarts". Scrape keeps its own counter and setting.
3. **One resume at a time per kind of load, with a gap.**
   - `RESUME_CONCURRENCY` (default 2): at most this many resumed jobs start their heavy phase at
     once. Heavy phases are: export, prod publish pre-flight, recompute, bulk accept, bulk
     suggestions.
   - `RESUME_STAGGER_S` (default 10 s) between two resume starts.
   - LLM and polling phases are light and are not limited by this, but still staggered.
4. **Checkpoints stay small.** Anything a job saves in order to resume goes in `job_runs.progress`
   and must stay under 4 KB per job: counters, batch numbers as ranges, phase names. Never lists of
   URLs or ids. (`progress` is written every 3 s and sent to every browser.)
5. **Resumed work uses the same streaming code as a first run.** No resume path may load a whole
   collection, a whole export or a whole rule set into memory unless the first run already does.
6. **One resume API.** `JobManager.resumable(kind)` registry: each resumable kind registers a
   function `resume(job) -> coroutine`. `recover()` calls it. Shutdown leaves registered kinds
   `running` (as scrape does today) and fails the others as today.
7. **Alarms.** Add to the #14 alarm list: RDS `FreeableMemory` below 1 GB for 5 minutes, RDS
   `DatabaseConnections` above 80 for 5 minutes, and engine task `MemoryUtilization` above 85 % for
   5 minutes.

**Validation.**
- [ ] New test (restart storm): six interrupted jobs of different resumable kinds at startup. `/health`
      answers 200 at once and every second throughout. No more than `RESUME_CONCURRENCY` heavy phases
      run at the same time. Resume starts are at least `RESUME_STAGGER_S` apart.
- [ ] New test (restart loop): a resumable fake job that raises on every start, with the engine
      restarted four times. It resumes three times, then fails with the limit message. The engine
      keeps serving throughout.
- [ ] New test: for each resumable kind on a 100K-row fixture where possible, `len(json.dumps(progress))`
      stays under 4,096 bytes during the run and after a resume.
- [ ] New test: the peak number of work-pool connections in use during the restart-storm test is at
      most `RESUME_CONCURRENCY + 2`.
- [ ] Local measurement on the 100K collection, recorded in the log: engine peak RSS and the
      Postgres container's peak memory (`docker stats`) during a resumed export and a resumed prod
      publish, each within 10 % of the same job run without a restart.
- [ ] `make infra-test` asserts the three new alarms.

### #22 Index runs survive restarts; cancel stops them

- [ ] Done
- [ ] Validated locally

**Depends on.** T5.0. Use its resume registry, restart counter and staggered start.

**What curators see.** After an engine restart, an index-to-test run shows "running" and finishes,
instead of "failed" and a duplicate run on the next click. Cancel stops the indexer task.
(Approved visible change.)

**Changes.**
1. Shutdown: an `index_test` job past dispatch (its `external_ref` is set) stays `running` in the
   database, as a scrape does. Do not kill the indexer task on shutdown.
2. `JobManager.recover`: for such a job, reopen it in place and spawn a resume coroutine. It
   rebuilds the `Dispatch` from `index_runs.external_ref` and continues at the step its
   `progress.phase` names: waiting for `status.json`, or validation. Limit resumes with T5.0's
   `RESUME_MAX_RESTARTS`.
3. A job still in export or before dispatch is failed on restart, as today. Nothing external
   exists yet.
4. `EcsDispatchIndexer.kill(d)`: call `ecs.stop_task` on the cluster and task ARN.
5. A curator's cancel: kill the indexer task, then set the `index_runs` row to `failed` with
   "cancelled by <user>". Today the row stays `running`.
6. On restart, any `index_runs` row left `running` whose job is not resumed is set to `failed`
   with "engine restarted".
7. CDK: grant `ecs:StopTask` on the indexer cluster's tasks to the engine task role.
8. Index to prod, validate and validate-prod resume through #31 and #32. Step 6 applies only to a
   run whose job is not resumed (for example, after the restart limit).

**Validation.**
- [ ] New test with `index_client` and a fake indexer: start an index run, stop the app during the
      status wait, start a new app. The same job finishes, and the fake indexer was dispatched once.
- [ ] Same, stopped during validation: it resumes at validation.
- [ ] Cancel test: `kill` is called, the `index_runs` row is `failed`, the job is `failed` with the
      cancelling user.
- [ ] Restart before dispatch: the job fails as today, the run row is closed.
- [ ] `make infra-test` asserts the `ecs:StopTask` permission.
- [ ] Snapshot guard: no change in the default fixture.

### #33 Index to test resumes during export

- [ ] Done
- [ ] Validated locally

**Depends on.** T5.0, #22.

**What curators see.** After an engine restart during export, the index run stays "running" and
finishes. The "exported" count goes back to 0 and counts up again. (Approved visible change.)

**Changes.**
1. Register `index_test` in the resume registry for the export phase as well (#22 covers the phases
   after dispatch).
2. On resume in export phase: do not insert the `index_runs` row again (it exists). Re-run the pin
   (already idempotent) and the export from the start, with the same run id. The S3 files are
   overwritten; `manifest.json` is still written last.
3. The export phase is a heavy phase under `RESUME_CONCURRENCY`.

**Validation.**
- [ ] New test with `index_client`: stop the app during export, start a new app. The same job and run
      finish, `manifest.json` exists once and matches the documents file, and the indexer was
      dispatched once.
- [ ] T5.0 memory measurement for a resumed export recorded.

### #32 Validate and validate-prod resume

- [ ] Done
- [ ] Validated locally

**Depends on.** T5.0.

**What curators see.** After an engine restart, a validation stays "running" and finishes. Its
30-second wait before checking happens again. (Approved visible change.)

**Changes.**
1. Register `validate` and `validate_prod`. Resume runs the same validation body again from the
   start. Validation only reads the index, so repeating it is safe.
2. The validation phase inside `index_test` is covered by #22, and the one inside `index_prod` by #31.

**Validation.**
- [ ] New test: stop the app during a fake validation, start a new app. The same job finishes with
      the same result a first run gives, and the `index_runs` row has one validation report.

### #31 Index to prod resumes

- [ ] Done
- [ ] Validated locally

**Depends on.** T5.0.

**What curators see.** After an engine restart, Index to prod stays "running" and finishes. Prod is
partly written for a few minutes longer, then completed. (Approved visible change.)

**Changes.**
1. Register `index_prod`. The publisher is already safe to run again: it scans prod first and only
   writes documents whose version differs from the export, and deletions are computed from the same
   scan. So resume runs `ProdPublisher.run` again from the start with the same run id and source test
   run. No position is saved.
2. Keep the counts of the first attempt: store `indexed`, `failed` and `deleted` of each attempt in
   `progress.attempts` (numbers only). The final status reports the totals across attempts, so the
   curator sees the full count, not only what the last attempt wrote.
3. Then run `_validate_prod` as a first run does.
4. The pre-flight scan and the S3 reads are a heavy phase under `RESUME_CONCURRENCY`.

**Validation.**
- [ ] New test with fake prod and test indexes: stop the app after half the documents are written,
      start a new app. The same job finishes, every document is in prod once with the right version,
      deletions happen once, and the reported `indexed` equals the number of changed documents.
- [ ] New test: a resume where the deletion ratio is above the limit refuses exactly as a first run
      does.
- [ ] T5.0 memory measurement for a resumed publish recorded.

### #29 Suggest patterns resumes

- [ ] Done
- [ ] Validated locally

**Depends on.** T5.0.

**What curators see.** After an engine restart, Suggest patterns stays "running". Progress pauses
for a minute or two, then continues. (Approved visible change.)

**Changes.**
1. Register `llm_patterns`.
2. Record finished batch numbers in `progress.done_batches` as compact ranges (for example
   `"0-41,43,45-60"`), updated in `on_result`.
3. On resume: skip `clear_pending_pattern_suggestions` and the global-exclude step (their results
   are already saved). Rebuild the batches exactly as the first run did (`pending_urls_for_patterns`
   is ordered by URL; `dedupe_variants` and `batches` are deterministic). Run only the batches not
   in `done_batches`.
4. Before resuming, check that the number of batches equals `progress.calls`. If it differs, the
   input changed; fail the job with "the delta URLs changed while it was interrupted; run Suggest
   patterns again" instead of guessing.
5. `add_pattern_suggestions` already skips a (type, match) that is present, so a batch answered
   just before the crash is harmless if asked again.

**Validation.**
- [ ] New test with the fake LLM: stop the app after some batches, start a new app. The same job
      finishes, the finished batches are not asked again (count fake-LLM calls), and the suggestions
      equal those of an uninterrupted run.
- [ ] New test: batch building is identical across two calls on the same data.
- [ ] Progress size under 4 KB at 100K URLs (T5.0 test).

### #30 Regenerate titles resumes

- [ ] Done
- [ ] Validated locally

**Depends on.** T5.0.

**What curators see.** After an engine restart, Regenerate titles stays "running" and continues.
(Approved visible change.)

**Changes.**
1. Register `llm_titles`.
2. Store the pass number (`progress.title_pass`) and the disambiguation round
   (`progress.disambiguate_round`).
3. On resume: run `_retitle_duplicates` again. It plans from the current duplicate groups, so groups
   already fixed are not asked again. Start the pass loop at the stored pass number, so the total
   number of passes across restarts never exceeds `llm_title_passes`, and the same for the
   disambiguation rounds.

**Validation.**
- [ ] New test with the fake LLM: stop the app after the first pass, start a new app. The same job
      finishes, groups fixed in the first pass are not asked again, and the total passes do not
      exceed the setting.

### #35 Suggest metadata resumes in place

- [ ] Done
- [ ] Validated locally

**Depends on.** T5.0.

**What curators see.** After an engine restart, the Suggest metadata job keeps running as the same
job. Today it shows as "failed, cancelled by shutdown" with a new job after it. (Approved visible
change.)

**Changes.**
1. Register `llm_metadata`. Shutdown leaves it `running`.
2. Resume runs the same job with `only_missing=True`, which already skips every URL that has an
   answer. Keep the job's counters: the resumed run adds to `done`, `failed` and the token counts
   instead of starting from 0, and `total` stays the original total.
3. Use `progress.restarts` and `RESUME_MAX_RESTARTS` instead of the separate `resumed` counter and
   new-job logic in `recover()`. Remove that new-job path.
4. Keep `llm_resume_after_restart` as an alias for `RESUME_MAX_RESTARTS` for this kind, so existing
   environment settings still work.

**Validation.**
- [ ] Update the existing test in `tests/test_llm.py` that checks the resume-as-new-job behaviour:
      now the same job id finishes, and no URL is asked twice (count fake-LLM calls).
- [ ] The existing test that a curator's own cancel stays cancelled still passes.
- [ ] Progress counters after the resume equal those of an uninterrupted run.

### #34 Recompute, bulk accept and bulk suggestions resume

- [ ] Done
- [ ] Validated locally

**Depends on.** T5.0, #1.

**What curators see.** After an engine restart, these jobs stay "running" and finish. (Approved
visible change.)

**Changes.**
1. Today these jobs run a closure built in `web/app.py` from the request, so nothing can rebuild them
   after a restart. Move the three operations into `CurationService` methods that take plain
   arguments and an actor, not a request:
   - `recompute_job(cid, all: bool, actor)`;
   - `bulk_accept_job(cid, decision, field, conf, actor)`;
   - `bulk_suggestions_job(cid, decision, actor)`.
   The routes call the same methods, inline or as a job, so behaviour is unchanged.
2. Move the request-free part of `_after_curation_change` (status rules, `patterns_file.changed`,
   the event, the audit line) into a function both the routes and the jobs call.
3. Store the arguments in `progress.request` when the job starts.
4. Register `recompute`, `bulk_accept` and `bulk_suggestions`. Resume calls the method again with
   the stored arguments.
5. Make each method safe to run twice, and make it always finish with a full recompute:
   - bulk accept: rules already inserted are skipped (`insert_patterns` skips duplicates); AI values
     already cleared are not accepted again; the final recompute runs even when nothing is left to
     accept;
   - bulk suggestions: suggestions already marked accepted are skipped; the final recompute runs even
     when nothing is pending.
6. These are heavy phases under `RESUME_CONCURRENCY`.

**Validation.**
- [ ] New tests, one per kind: interrupt the job at each internal step (after the rule insert, after
      the AI clear, before the recompute) with a test hook, restart, and check the final delta table,
      rules and AI columns equal an uninterrupted run.
- [ ] Existing `tests/test_scale.py::test_bulk_changes_on_a_big_collection_run_as_jobs` passes.
- [ ] Snapshot guard unchanged.
### #24 Shared limit on LLM calls across jobs

- [ ] Done
- [ ] Validated locally

**What curators see.** When several Suggest-metadata jobs run at once, each shows slower progress,
with fewer failed calls. (Approved visible change.)

**Changes.**
1. Setting `LLM_WORKERS_TOTAL` in `config.py` (default 32). It limits LLM calls in flight across all
   jobs in one engine process. `LLM_WORKERS` stays the per-job limit.
2. One `asyncio.Semaphore(llm_workers_total)` in `JobManager`. `run_pool` (`llm/pool.py`) takes an
   optional shared semaphore and acquires it around each call.
3. Pass it from every LLM job: metadata, patterns, titles, and the retry passes.
4. CDK: set `LLM_WORKERS_TOTAL` on the task from `EnvConfig` (default 32).

**Validation.**
- [ ] New test: three fake-LLM metadata jobs on three collections with `LLM_WORKERS=16` and
      `LLM_WORKERS_TOTAL=20`. The highest number of calls in flight observed is at most 20.
- [ ] One job alone still reaches 16 in flight.
- [ ] `make infra-test` passes.

---

## Tier 6: more than one engine task

### #17 Coordination in the database; safe migrations

- [ ] Done
- [ ] Validated locally

**Depends on.** Tiers 1–5 validated.

**What curators see.** None.

Do the sub-steps in order. Each has its own check boxes.

#### 17.1 Migration safety rule

- [ ] Done
- [ ] Validated locally

1. Add `docs/migrations.md`: every migration must work with the code before and after it. Add,
   then use, then drop in a later deploy.
2. New test: every migration numbered above the current one at the time of this step contains no
   `DROP COLUMN`, `RENAME`, `ALTER COLUMN … SET NOT NULL`, or `ADD COLUMN … NOT NULL` without
   `DEFAULT`, unless its source has a `-- contract:` comment naming the earlier deploy that stopped
   using the column.

Validation:
- [ ] The test passes on the current migrations and fails on a deliberately bad one.

#### 17.2 One transaction and a database lock per curation change

- [ ] Done
- [ ] Validated locally

1. `Database.transaction()`: an async context manager that takes one work-pool connection, opens a
   transaction, and binds the connection to a context variable. `_conn()` returns that bound
   connection, without a new transaction, when one is bound.
2. `CurationService`: every method that changes rules or deltas (`recompute`, `recompute_keys`,
   `add_pattern(s)`, `replace_exact_pattern(s)`, `set_excluded`, `delete_pattern`, `promote`,
   `promote_urls`) runs its loads and writes inside one `Database.transaction()`, and first runs
   `SELECT pg_advisory_xact_lock(hashtext(collection_id))`.
3. The AI clears that follow an accept or reject in `web/app.py` move inside the same transaction.
4. Keep the in-process `asyncio.Lock` for now. Remove it in 17.4.

Validation:
- [ ] New test: an exception raised after the rule insert and before the delta write leaves no rule
      and no change (atomic).
- [ ] New test: two app instances on one database. A recompute in app A holds the lock; a per-URL
      edit in app B waits and then succeeds; the final state equals running them one after the
      other.
- [ ] Snapshot guard unchanged.

#### 17.3 Job ownership in the database

- [ ] Done
- [ ] Validated locally

1. Migration on `job_runs`: `owner text`, `heartbeat_at timestamptz`,
   `cancel_requested boolean NOT NULL DEFAULT false`.
2. Migration: mark older duplicates of active jobs per collection as failed, then
   `CREATE UNIQUE INDEX job_runs_one_active ON job_runs (collection_id) WHERE state IN ('queued','running')`.
3. Each engine process has an id (task ARN on ECS, hostname and pid locally). It sets `owner` on
   the jobs it runs and updates `heartbeat_at` every 10 s.
4. `recover()` fails or resumes only jobs whose `heartbeat_at` is older than 60 s, never jobs with a
   fresh heartbeat from another process.
5. Cancel sets `cancel_requested`. The owning process cancels the task when it sees the flag, at the
   next heartbeat at the latest. A cancel in the owning process still acts at once.

Validation:
- [ ] New test: two app instances. A job running in A is not touched by B's `recover()`.
- [ ] New test: A's job with a heartbeat older than 60 s is recovered by B as today.
- [ ] New test: a cancel requested through B stops a job running in A within 15 s.
- [ ] New test: two concurrent starts of a job on one collection, through A and B, give one job and
      one 409.

#### 17.4 Busy checks read the database

- [ ] Done
- [ ] Validated locally

1. `ensure_idle` and the start checks read the active job from `job_runs` (state and fresh
   heartbeat), not from `_tasks`. Which actions a job blocks does not change: every job still
   blocks every curation action, as today.
2. Remove the in-process `asyncio.Lock` from the curation path; the advisory lock from 17.2 replaces
   it.

Validation:
- [ ] New test: a job started in A makes an edit through B return 409.
- [ ] All existing tests pass.

#### 17.5 Events through Postgres

- [ ] Done
- [ ] Validated locally

1. `EventBus.publish` sends `NOTIFY engine_events, '<json>'`. Each process keeps one dedicated
   connection with `LISTEN engine_events` and fans received events out to its own SSE subscribers
   and listeners (including `touch`).
2. Keep the payload format exactly as today (`json.dumps` default separators). The templates match
   on the substrings `"<collection id>"` and `"state": "succeeded"`.
3. Payload limit: if the JSON exceeds 7,000 bytes, drop `job.progress` keys other than the ones the
   templates or JS read. Check `base.html` and the templates for every field they use, and list
   them in a comment.
4. If the listen connection drops, reconnect and fire the existing `sseReopen` behaviour so open
   pages refresh once.

Validation:
- [ ] New test: an event published in A reaches an SSE subscriber in B.
- [ ] New test: a write in A marks B's coalesced reads stale (`_gens` changes in B).
- [ ] New test: an oversized progress payload is trimmed and still contains the collection id and
      state.
- [ ] Snapshot guard unchanged.

#### 17.6 `patterns.yaml` writes coordinated through the database

- [ ] Done
- [ ] Validated locally

1. Migration: `collections.patterns_dirty_at timestamptz`.
2. `PatternsFile.changed` sets `patterns_dirty_at` instead of keeping in-memory state. The existing
   20 s debounce reads it.
3. The writer takes `pg_try_advisory_lock` on the collection's file key, writes the file, and clears
   `patterns_dirty_at` only if it was not set again meanwhile.
4. Promote still waits for the file to be current.
5. Startup: rewrite any file whose collection has `patterns_dirty_at` set (closes audit R7).

Validation:
- [ ] Existing `patterns.yaml` tests pass.
- [ ] New test: two app instances; a change through A is written once.
- [ ] New test: a dirty flag left by a killed process is written at the next startup.

### #23 Separate web and worker tasks, rolling deploys

- [ ] Done
- [ ] Validated locally

**Depends on.** #17 (all sub-steps), T5.0, #22, #29–#35.

**What curators see.** Deploys of the web tasks no longer drop pages or fail jobs. The live
connection indicator may blink once while the browser reconnects to a new task, and the page
refreshes its parts once, as after any reconnect today. Worker deploys: every job kind carries on
after the new worker starts (#22, #29–#35), within the T5.0 restart limit. (Approved visible
change.)

**Changes.**
1. Setting `ROLE` in `config.py`: `all` (default, local and tests), `web` or `worker`.
2. `web`: serves HTTP and SSE, runs inline curation work under `bulk_job_min_urls`, and creates jobs
   as `queued` rows. It does not run jobs or `recover()`.
3. `worker`: claims `queued` jobs with
   `UPDATE … WHERE id = (SELECT id … FOR UPDATE SKIP LOCKED LIMIT 1)`, runs them with heartbeats,
   runs `recover()` at startup, and serves only `/health`.
4. Wake-up: a job insert sends `NOTIFY engine_jobs`; the worker also polls every 5 s.
5. Worker shutdown (`SIGTERM`): stop claiming new jobs, leave every running job `running` for the
   next worker (all kinds are resumable after Tier 5), and exit within the 120 s stop timeout.
6. CDK:
   - web service: same image, `ROLE=web`, desired 2, `min_healthy_percent=100`,
     `max_healthy_percent=200`, behind the ALB;
   - worker service: same image, `ROLE=worker`, desired 1, `min_healthy_percent=100`,
     `max_healthy_percent=200`, no load balancer, container health check on `/health`,
     `stop_timeout` 120 s;
   - both get the same environment, secrets and EFS mount;
   - check that `SESSION_SECRET` is set on both, so cookies work on every task.
7. Add a short deploy note to `infra/README.md`: check the jobs strip before deploying the worker.

**Validation.**
- [ ] New test: one `web` app and one `worker` app on one database. A job started through web is
      run by worker, and its events reach a web SSE subscriber.
- [ ] New test: two worker apps never run the same job.
- [ ] New test: stopping the worker during each job kind leaves it `running`; a new worker resumes
      it, with T5.0's staggering and limits.
- [ ] Local run: two `ROLE=web` uvicorn processes on different ports and one `ROLE=worker` process
      against the compose database. Open the same collection in both web ports, start a job, and
      see progress in both. Stop one web process: the other keeps serving.
- [ ] `make infra-test` asserts both services and their deployment settings.
- [ ] Snapshot guard unchanged.

---

## Validation log

Add one line per validated item or sub-step. Do not edit earlier lines.

| Date | Item | Commands run | Result and numbers | By |
|---|---|---|---|---|
| 2026-10-07 | T0.1 | `UPDATE_SNAPSHOTS=1 pytest tests/test_page_snapshots.py` once, then the test 5× in a row; a one-word change in `tab_curate.html` ("in force" → "in use"), test run, change reverted, test run | 20 pages and fragments snapshotted under `tests/snapshots/pages/`. Stable 5/5. The one-word change failed the test on `ex_curate` and `run_curate_job_running`; after the revert it passed. Normalized: timestamps, `MM-DD HH:MM:SS`, job durations, `?v=` hashes, temp dir, `since`. Found: the rule-effect tooltip lists its lines in an order that varies between runs (`effects_for` has no ORDER BY); the test compares them as a set, the app is unchanged. | agent |
| 2026-10-07 | #2 | `pytest tests/test_db_health.py`; profiler run below | New test passes: a 0.4 s block shows as `loop_lag_ms.max` ≥ 300 and the next read resets it; `/health` unchanged. The probe logs WARNING above 250 ms. | agent |
| 2026-10-07 | #14 | `pytest tests/test_db_pg.py`; `make infra-test`; uvicorn on a throwaway Postgres (port 55499, removed afterwards) with the fake crawler, `max_pages=11` scrape | V13 creates `pg_stat_statements` (installed, 1.11, in the test database). Infra: 22 passed, incl. parameter group (`postgres17`, preload, track top, 2,000 ms) attached to the instance, and 4 alarms on one SNS topic. Under uvicorn the line `INFO sde_curation.db: dump ex.org: dropping 9 documents …` appears; uvicorn's own lines unchanged, no duplicates. NOT RUN (Bernard): deploy, RDS reboot for the parameter group, subscribing the SNS topic. | agent |
| 2026-10-07 | #15 | `pytest tests/test_db_pg.py` | V14 reloptions present on `delta_urls`, `pattern_effects`, `patterns`: vacuum and analyze scale factor 0.02. | agent |
| 2026-10-07 | T0.2 | `source newrun.sh local "…"`; `profile_pages_edits.py -n 100000` (profiler extended: loop freeze per edit, 5 samples with max, statements per render, duplicate-scan EXPLAIN ANALYZE); results `~/projects/sde-curation-stress/results/local-20261007T202727Z/` | Baseline, laptop, 100K URLs, fake LLM, no curated rows. State A = suggestions pending; state B = all accepted (per-URL rules). **Worst event-loop freeze per edit:** A 393 ms, B 429 ms (bulk accept froze it up to 659 ms). **Per-URL title edit:** A 2.72 s, B 5.74 s median (B steps: replace_deltas 2.45 s, engine 1.87 s, load_deltas 0.56 s, load_rules 0.38 s). **Curate page (job-watch reload):** A 0.97 s median / 1.07 s max, B 1.69 / 1.72 s. **Delta tab fragment:** A 0.47 / 0.50 s, B 0.48 / 0.51 s. **Rules tab fragment:** A 0.04 / 0.06 s, B 0.15 / 0.17 s. **Statements per Curate render:** 97.2. **Duplicate-title scan:** A 116 ms, B 234 ms median. | agent |
| 2026-10-07 | Tier 0 | `make lint`; `make test` | lint clean; 390 passed (386 before + 4 new). | agent |
| 2026-10-07 | #1 | New `tests/test_concurrent_writes.py` run on the unchanged code, then with the change; `pytest` on test_scale, test_llm, test_review_round, test_collection_division, test_duplicate_titles, test_page_snapshots | Before the change both race tests failed: a suggestion written mid-recompute was overwritten (`'Page 1' == 'Newer suggestion'`) and a rejected one came back. After: 4/4 pass (both races, suggestions kept through recomputes, AI column list equals `engine.diff._AI_FIELDS`). 82 related tests pass; snapshots unchanged. Every reader of the AI columns already skips removed rows, so the one case where a row turns into a removal is not visible either. | agent |
| 2026-10-07 | #18 | 6 new tests in `tests/test_review_round.py`; the fix switched off for one run (`keep_queued=False`) and restored | Migration V15 adds `collections.review_round` (default false). New tests pass: one edit keeps the whole queue and every other suggestion; an exclude rule removes only its page; partly promoted pages do not come back; promote (full, or the last rows one by one) closes the round; a new crawl closes it. With the fix off, the edit and partial-promote tests fail. Existing re-curate test passes unchanged; snapshots unchanged. | agent |
| 2026-10-07 | Tier 1 | `make lint`; `make test` | lint clean; 400 passed (390 + 10 new). | agent |
| 2026-10-07 | #3 | `tests/test_event_loop.py` against new and old `db.py` (old restored from git stash, then put back); profiler `results/local-20261007T214108Z/` | Loads now fetch in 2,000-row slices with a yield between, build models in a thread; COPY rows are built in a thread and written in 2,000-row slices. Loop freeze for one per-URL edit (test, laptop): 20K 33–46 → 13–14 ms, 50K 83–96 → 28–70 ms, 100K 338–359 → 86–94 ms. The plan's "20K under 250 ms" passed on the old code too, so it proved nothing; the test runs at 100K with a 200 ms limit (5.6 s). Profiler at 100K: worst freeze per edit A 393 → 147 ms, B 429 → 187 ms (target ≤ 300 ms); edit time A 2.72 → 2.63 s, B 5.74 → 5.54 s (not worse). Seen but outside #3: bulk accept still freezes the loop up to ~0.7 s (it builds 300K rule objects in the request path; audit R1), and the profiler's own blocking EXPLAIN calls show as 0.6–0.7 s freezes. | agent |
| 2026-10-07 | #4 | `pytest tests/test_promote_selection.py`; grep | `promote()` in `promote_urls` runs in `asyncio.to_thread`; no bare call left in `curation.py`. 9 passed. | agent |
| 2026-10-07 | #5 | New test in `tests/test_page_snapshots.py`, also run with the memo switched off; profiler `results/local-20261007T215934Z/` | `RequestMemo` (read scope only) for latest_job, last_index_run, list_jobs, list_index_runs, latest_job_of_kind, count_deltas_for_llm, job_exists. Per Curate GET: latest_job 1, last_index_run 2; without the memo 2 / 4 and the test fails. Statements per render at 100K: Curate 97.2 → 85.2, Overview 36.2 → 27.2. Snapshots unchanged. | agent |
| 2026-10-07 | #7 | New test in `tests/test_busy_database.py`; busy-database, snapshot and scale suites | 28 database writers mark the collection changed after they commit (`db._touches`); the bus listener ignores running-job progress events. Test: a progress event leaves `_gens` alone, a metadata write bumps it and the next Curate render shows the new suggestion, a finished job bumps it. 24 related tests and the snapshots pass. | agent |
| 2026-10-07 | #8 | New `tests/test_progress_throttle.py` (interval shortened to 0.3 s) | `JobManager._publish_progress`: one write + event per job per 3 s, trailing publish with the last value; `phase`, `pid`, `ssm_command`, `external_ref` go out at once; a job's end cancels anything held back. Tests: 10 updates → 2 publishes, last = 10 and in the table; end announced at once, nothing after; phase change at once. Existing progress/poll tests pass. | agent |
| 2026-10-07 | #9 | New test in `tests/test_scale.py`, also run without the guard; scratch timing test outside the repo, 100K curated rows | Curated upsert has `WHERE (…) IS DISTINCT FROM (…)`. Test: after a one-row edit and promote, only that row's `xmin` changes; without the guard every row changes and the test fails. Second promote after a one-row change at 100K: 1.72 → 1.33 s (laptop), and no 100K dead row versions. | agent |
| 2026-10-07 | #16 | `pytest tests/test_auth.py` (2 new tests), role test also run without its cache clear | `Database.session_user`: 30 s per-process cache, dropped by set_password, set_role, set_active. Ten authenticated GETs → one `get_user`. Deactivate, password change and role change take effect on the next request (existing tests warm the cache first). Without the clear on role change the test fails. 16 passed. | agent |
| 2026-10-07 | Tier 2 | `make lint`; `make test` | lint clean; 409 passed (400 + 9 new). | agent |
| 2026-10-08 | #3 (fix) | CI run failed `test_an_edit_on_a_big_collection_does_not_freeze_the_server` (212 ms against 200 ms). Ratio measured on the laptop, new `db.py` and `db.py` from f45b3fc (before Tier 2) | An absolute limit does not carry over to a slower machine. The test now limits the freeze as a share of the edit's own time. Laptop: new 0.037–0.041 (81–94 ms of 2.2–2.3 s), old 0.143–0.150 (330–352 ms). Limit 0.09. New code passes 3/3; old code fails ("352 ms of a 2339 ms edit (15.0%; limit 9%)"). Not yet confirmed on CI. | agent |
| 2026-10-08 | #11 | Scratch bench outside the repo: a promoted, re-curated 100K collection (100K curated + 100K queued rows, pending AI titles) in a database on the profiling server; each candidate index added temporarily; `pytest tests/test_db_pg.py`; V16 timed on the 300K-rule profiling database | Without new indexes: duplicate counts 123 ms, incomplete 120 ms, duplicates for 50 rows 116 ms (all under the 0.5 s STOP limit). With the 4 plain indexes: delta kind filter 14.9 → 1.6 ms, accept-all count 189 → 103 ms, duplicate scans unchanged. With 3 duplicate-key expression indexes as well: no change (111–130 ms); EXPLAIN shows sequential scans and a sort, the expression indexes unused, the partial `renamed_from` index used. Shipped V16 with the 4 plain indexes only. V16 on 100K deltas / 300K rules / 300K effects: 0.25 s. New test checks the 4 index definitions. | agent |
| 2026-10-08 | #10 | New `tests/test_stored_counts.py`, also run with the version bump switched off; profiler `results/local-20261008T151022Z/` | V17 `collection_stats`; `@_stored` on count_deltas_by_kind, count_curated_excluded, count_curated_unreachable, curated_export_count, count_deltas_for_llm, count_ai_suggestions, count_patterns, delta_ai_counts, pattern_suggestion_counts. Flow test: after scrape, Start curating, rule add, rule delete, per-URL edit, exclude toggle, Suggest patterns, accept-all suggestions, Suggest metadata, accept one, reject one, accept-all AI, partial promote, full promote, Re-curate everything and re-crawl, every stored count equals a fresh count from the tables. Second test: three more views of an unchanged page compute nothing; an edit makes the next one count again. With the bump off both tests fail (stale rule count 0 vs 1). Curate page at 100K, median: A 0.95 → 0.72 s, B 1.68 → 1.12 s (the first view after a change still counts: max 1.15 / 1.55 s). `collection_stats` added to the SQLite importer's PG-only tables. | agent |
| 2026-10-08 | #6 | Snapshot diff checked line by line, snapshots updated; new equality test; browser check `~/projects/sde-curation-stress/jobwatchcheck.py` (Playwright, real engine, 35 s crawl); profiler | Only `#job-watch`'s `hx-get` and `hx-on::config-request` changed, on the 10 collection pages. Equality test: for every tab, `/tab-body` returns the page's `#tab-body` character for character. Browser: refresh from `/tab-body` at 4.7, 14.7, 24.7, 34.7 s (every 10 s, as before) and once at the job's end (37.6 s); the in-tab content moved each time; no whole-page fetch, no failed request, no console error: PASS. At 100K: tab-body 85.2 statements against 93.6 for the page; time about equal (0.72 / 1.10 s): the saving is the layout, header and stepper rendering. | agent |
| 2026-10-08 | Tier 3 | `make lint`; `make test` | lint clean; 413 passed. #12 moved to Tier 4 after #13 (see its section). | agent |
