# Changelog — 2026-10-06: test engine crashes, busy-database hardening, stepper click fix

Uncommitted working tree on `featuure/optimize-app` (base `936ee0d`). 386 tests pass.

## The errors

### 1. The test engine crashed four times (17:09, 18:54, 19:48, 20:28 UTC)

- **What curators saw:** pages stopped loading, then the engine restarted. 381 `PoolTimeout` errors and
  112 HTTP 500s, all inside the crash windows. Nine jobs were lost to the restarts ("cancelled by
  shutdown" / "engine restarted while job was running"): `uahirise.org` metadata ×4, `soho` index ×3,
  `lroc` index ×1, and the `tracers.physics.uiowa.edu` bulk accept, which failed with
  `PoolTimeout: couldn't get a connection after 30.00 sec`.
- **Cause:**
  1. A bulk accept of exclusion suggestions (soho at 17:05, tracers at 18:52) turned about 97,000
     URLs into rule exclusions.
  2. The "dump URLs excluded by rules" count (`count_excluded_by_rules`, a `COUNT(DISTINCT)` join over
     `pattern_effects`, `patterns` and `dump_urls`) then took 7 s per call on a 100K collection.
  3. Every page render ran it (`step_context`). Every open tab re-renders on every job event and on
     timers: the stepper, the header, and `#job-watch`, which reloads the whole page every 4–10 s.
     The later crashes were triggered by index jobs, whose progress events refreshed the open soho
     and lroc pages.
  4. The copies piled up. When a browser abandons a request, the server's query keeps running. All 16
     pool connections were held by copies of the same count. RDS CPU was 95–100% on its 2 vCPUs.
     Performance Insights put this one query at 6.8–11 average active sessions.
  5. `/health` needed a pool connection too, so it timed out, and the ALB replaced a working engine.
     The restart did nothing for the database and dropped every page and running job.
- **What was not the cause:** engine memory (peak about 1.7 GB of 16 GB), engine CPU, RDS memory
  (no swap) and storage.

### 2. A stepper step would not open while a job ran

- **What curators saw:** clicking "5 · Indexed to test" while the collection was indexing did nothing.
- **Cause:** three htmx problems, each one enough on its own to lose the click.
  1. **The click was cancelled.** The step links sit inside the stepper wrapper, which refreshes itself
     while a job runs. The links inherited the wrapper's `hx-sync="this:replace"`, so each stepper
     refresh aborted the click's request (`net::ERR_ABORTED`) whenever the step page took longer than
     the gap between refreshes (3–5 s). On test, the 7 s count made that every time.
  2. **The answer was dropped.** A refresh that landed while the click loaded replaced the clicked
     link. htmx resolves `hx-target` from the link, and a detached link finds no `#tab-body`.
  3. **The old step came back.** htmx clears a replaced element's internal data, including its
     "boosted" flag, so the click's URL was never pushed. The next `#job-watch` poll fetched the old
     address and put the previous step back.

### 3. Single edits are slow on large collections (not fixed — see Outstanding)

Every edit runs a full recompute of the collection, by design. The 2026-09-18 scale audit measured
5.8 s on a laptop at 100K URLs.

## The fixes

### Excluded count stored, not counted per view (fix for error 1)

- New column `collections.excluded_count`, migration **V11**. It has no backfill: `NULL` means unknown.
- Every recompute stores the count it already computes (`DeltaSet.excluded`), in the same transaction
  as the rule effects. Cleared effects store 0.
- A new dump, or a deleted exclude/include rule, sets it to `NULL`. Then `Database.excluded_count()`
  counts it once, the first time it is wanted, and stores it.
- `step_context` reads the stored value. Page views no longer run the 7 s join.
- **V12** sets every stored count to `NULL` once. Dev ran V11 during the stress test and then went back
  to code that does not maintain the column. On test and prod, V11 and V12 apply together, so V12
  changes nothing there.
- Files: `schema.py`, `models.py`, `db.py`, `curation.py`, `web/app.py`.

### Two database pools (fix for error 1)

- The **read pool** serves page requests (GET/HEAD): `DB_READ_POOL_SIZE` 12.
  - Statements stop after `DB_READ_STATEMENT_TIMEOUT_S` (30 s).
  - A request waits at most `DB_READ_WAIT_S` (10 s) for a connection, so the wait plus the query stays
    inside CloudFront's 60 s limit.
- The **work pool** serves curator actions and jobs: `DB_POOL_SIZE` 16, with no time limit. A promote
  or recompute on 100K URLs must not stop half-way.
- The pool is chosen by a context variable (`db.db_scope`). The new `DbScope` ASGI middleware sets it
  per request. Jobs and the `patterns.yaml` writer run in `work_context()`.
- A cancelled statement or a pool timeout answers **503 + `Retry-After: 5`** instead of a 500 or a
  hang. That includes the session lookup in the auth middleware. htmx fragments keep what they showed.
- Jobs and page requests can no longer starve each other.
- Files: `db.py`, `config.py`, `jobs.py`, `store.py`, `web/app.py`.

### Identical page reads run once at a time (fix for error 1)

- `db.SingleFlight`: per-collection aggregate counts run once at a time per (collection, arguments).
  Callers that arrive meanwhile wait **without holding a connection** and share the result.
- `Database.touch(cid)` runs after every non-GET request and every bus event. Anyone who asks after a
  change gets a fresh count, so nobody sees numbers from before their own edit.
- Only for page requests: jobs and actions always read straight from the tables.
- Applies to: excluded count, delta counts by kind, curated counts, export count, rule count,
  duplicate-title counts, incomplete counts, AI suggestion counts, LLM candidate counts.
- Files: `db.py`, `events.py` (bus listeners), `web/app.py`.

### Health checks (fix for error 1)

- **`/health`** is liveness only, for the ALB: `{"ok": true, "sse_clients": n}`. It does not touch the
  database, so a busy database no longer gets the engine replaced.
- **`/health/db`** is new, for monitoring: a read-pool ping within `HEALTH_DB_TIMEOUT_S` (3 s), plus
  both pools' counters. It answers 503 when the database does not respond in time. It currently
  requires login.

### Stepper click (fix for error 2)

- Step links: `hx-sync="#tab-body:replace"`, `hx-target="global #tab-body"` and `hx-push-url="true"`
  (`partials/pipeline_inner.html`).
- `#job-watch`: `hx-sync="#tab-body:drop"`. The poll gives way to the curator: it is skipped while a
  click loads, and a click cancels a poll in flight (`collection.html`).
- The Rules and URL-table filter forms: `hx-sync="#tab-body:replace"` (`partials/rules.html`,
  `partials/urls_table.html`).

## Tests and verification

- **New tests:** `tests/test_busy_database.py`, 12 tests through real API flows:
  - the stored count stays exact through rule changes, overrides and re-crawls;
  - 20 tabs trigger one count, not 20;
  - a locked table: the page answers 503 while a job waits and succeeds;
  - each pool stays usable while the other is exhausted;
  - `/health` answers with the database gone;
  - the coalescer: shared runs, freshness after a change, cancellation, errors.
- **Changed tests:**
  - `tests/test_db_pg.py`: migrations to V12, plus a test of dev's V11 → V12 case;
  - `tests/test_db_health.py`: the new `/health` shape and `/health/db`.
- **Dev stress run** (`~/projects/sde-curation-stress/results/dev-20261006T222830Z/`): this build on
  dev with the fake LLM.
  - 111 jobs, 0 failures, 0 errors, no outage, 0 restarts.
  - `/health/db` reached 9 s with zero ALB risk.
  - Within the limits (page p95 ≤ 5 s): 20 concurrent jobs at 5K URLs, 10 at 25K.
- **Stepper fix:** a local Playwright check (`~/projects/sde-curation-stress/step5check.py`, outside
  the repo). It fails on the old templates and passes 3 of 3 with the fix.

## Deploy notes

- **V11 and V12 run at startup and take moments:** one nullable column, then one `UPDATE`.
- After the deploy, the first view of each big collection counts once (about 7 s on soho), shared by
  everyone viewing it, and stores the result.
- New settings, all with defaults: `DB_READ_POOL_SIZE`, `DB_READ_STATEMENT_TIMEOUT_S`, `DB_READ_WAIT_S`,
  `HEALTH_DB_TIMEOUT_S`. Up to 28 database connections per engine (16 + 12), against RDS's 836.
- Dev runs `origin/dev` again (rev 42), but its database is at V11. V12 handles that on the next deploy.

## Outstanding

1. **Slow pages on big collections.** About 2 s p95 at 100K even idle; tab refreshes 7 s p95 at 25K
   × 15 jobs. The duplicate-title scan runs about 6 times per render. Compute it once per render, and
   lighten the polling.
2. **Slow single edits.** Full recompute per edit. Profile one edit at 100K, then the audit's R2 (slim
   models).
3. **The engine uses about 1 of 4 vCPUs** (one Python process). Audit R2, then R3 (process pool).
4. **RDS CPU reached 72%** with 5 jobs on 100K collections. Check again after 1–3 before upsizing.
5. **Index jobs do not resume after a restart.** A re-run dispatches a second indexer task.
6. **No alarms.** `pg_stat_statements` is not enabled on test, and `/health/db` is behind login.
7. **12 high-deletion refusals this week** (93–100% of a collection's documents). Investigate.
8. **The `uavsar` indexer out-of-memory crash** (`sde-api-scrapers`). Decision pending.
9. **Test storage:** 13 GB free of 20 GB allocated. Pre-raise it before 1–8 GB crawls.
10. **Stress coverage gaps:** realistic 1–8 GB crawls, real LLM latency, 100K above 5 concurrent jobs.
11. **The stepper browser check is local only.** CI does not guard it.
