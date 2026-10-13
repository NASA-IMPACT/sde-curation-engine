# Test strategy: fewer tests that catch more (2026-10-09)

**Goal (Bernard).** Stop adding endless tests. End-to-end tests catch regressions in the main
workflows. Integration tests cover only the essential integrations. Everything else is covered by
quick unit tests. Together they must catch every bug that exists today.

**One limit to plan around.** End-to-end tests cannot catch every regression. A few user journeys
catch anything that breaks a main workflow, but most bugs in `REVIEW-SINCE-DEV-MERGE-2026-10-08.md`
appear only at an exact moment or with an exact input (a restart in the middle of one call, a page
view between two writes). Those are cheap and reliable in a unit test with fakes, and slow and
flaky end to end. So: journeys at the top, edge cases and timing at the bottom.

---

## 1. Where we are

The restructure of 2026-10-09 moved the 451 tests into three levels without changing them. What
each level covers, by lines of engine code (coverage.py, `sde_curation/`):

| Level | Tests | Time | Line coverage |
|---|---|---|---|
| Unit | 164 | 8 s | 48 % |
| Integration | 221 | 2 min | 78 % |
| End-to-end | 66 | 2 min | 76 % |
| All | 451 | 4 min | 95 % |

The gap is in the four modules that hold most of the logic and most of the bugs:

| Module | Statements | Unit | Integration | End-to-end |
|---|---|---|---|---|
| `web/app.py` | 1,351 | 26 % | 91 % | 71 % |
| `db.py` | 1,300 | 25 % | 95 % | 78 % |
| `jobs.py` | 925 | 13 % | 59 % | 91 % |
| `curation.py` | 194 | 17 % | 97 % | 62 % |

The pure modules (engine, models, LLM tasks, publish, scrape backends) are already well covered by
unit tests (80–100 %). None of the 451 tests catches any finding in the review: all pass on HEAD.

---

## 2. Target shape

| Level | Purpose | Size | Time | Needs |
|---|---|---|---|---|
| End-to-end | The main user journeys, each one long, with real subprocesses, S3 and restarts | about 10 journeys (some parametrized) | under 4 min | Postgres, Docker |
| Integration | Only what needs real PostgreSQL: SQL, migrations, transactions, races, and the contract that keeps the unit-test fakes honest | about 30 | under 90 s | Postgres |
| Unit | Every rule, decision, edge case and timing case, with an in-memory fake database and fake backends | grows from 164 to roughly 300 | under 20 s | nothing |

Fewer tests in total than today (451), and most of them fast. The count is not the target; the
gates in section 6 are.

---

## 3. End-to-end journeys

Each journey drives the app through its API the way a curator does, with the real crawler and
indexer subprocesses, moto S3, the fake prod index and the fake LLM, and checks the state and the
pages at every step.

| # | Journey | Main steps | Review findings it catches |
|---|---|---|---|
| J1 | First curation of a new collection | crawl → Start curating → exclude glob → per-URL edit → ✗/✓ → Suggest patterns + accept all → Suggest metadata + accept all → regenerate duplicate titles → promote → index to test → validate → index to prod → live | broad regressions; counts shown after ✗/✓ match the tables |
| J2 | Re-crawl of a live collection | crawl again with new, changed, removed and failed pages and a capped crawl → **edit before Start curating** → Start curating → Re-curate everything → partial promote → queue empties → full promote → re-index (prod drops the removed page) | **H1**, L7, L10 |
| J3 | Engine restart during every job kind (parametrized) | start the job, stop the engine at a set point, start a new engine; the job finishes with the result of an uninterrupted run | resume regressions; M5, L1, L2 (as variants) |
| J4 | Cancel paths (parametrized) | cancel a crawl, an index run (task stopped), a job waiting to resume, a resumed job | **M2** |
| J5 | Failures are reported, nothing half-done | crawler crash, broken documents file, indexer failure, validation failure, mass-deletion refusal, every LLM call failing | failure-path regressions |
| J6 | Several curators at once | three collections, jobs running, tabs refreshing; every count a page shows equals the tables after each write; health stays up | stored-count regressions |
| J7 | Accounts and roles | login off and on, admin vs curator, a demotion takes effect | auth regressions |
| J8 | Every action against every status (the existing state matrix) | with and without a running job | fool-proofing |
| J9 | Load an existing crawl from S3 (remote backend) | reuse an existing crawl through the SSM backend | remote-crawl regressions |

---

## 4. Essential integration tests (PostgreSQL only)

| # | What | Why it needs the real database | Findings |
|---|---|---|---|
| I1 | Migrations: empty → latest; dev's schema with data → latest; run twice; the V11/V12 dev case | DDL and data migrations | |
| I2 | **Database contract**: one test set run against PostgreSQL *and* the in-memory fake the unit tests use | keeps the fake honest, so unit tests can trust it | |
| I3 | SQL equals the engine: rule match counts; scoped recompute equals the full one (the twin test); stored counts exact through every kind of write, including mid-job | SQL semantics | |
| I4 | A change mark commits with its write | transactions | **M9** |
| I5 | Races on real rows: concurrent writes during a recompute; the lazy excluded count during a ✓; the key backfill against a promote | locking, MVCC | **M10**, **L5** |
| I6 | API contract: every route's login and role check (one table-driven test); `/tab-body` equals the page's tab body; health endpoints; 503 when the pool is busy | the real request path | |
| I7 | Page snapshots (what curators see must not change by accident) | full renders | |
| I8 | Event-loop freeze guards at 100K URLs (one edit, Suggest patterns) | real data volume | |
| I9 | The SQLite importer | real target database | |

---

## 5. Unit tests: what grows

Unit tests reach the orchestration code through an **in-memory fake database** (the methods the
job manager and the curation service use) and fake backends (crawler, indexer, ECS, prod index,
LLM). The contract test I2 runs the same checks against the fake and against PostgreSQL, so the
fake cannot drift.

| # | Area | Covers | Findings |
|---|---|---|---|
| U1 | `jobs.py` job manager | resume registry, restart counting and limit, cancel (running and pending), progress throttle, heavy-phase limit, shutdown, dispatch and task kill, the resume loop, every job kind's counters | **H2**, **M2**, **M3**, **M5**, L1, L2, L17, M4 |
| U2 | `curation.py` curation service | when the scoped path may run, the excluded-count arithmetic, review round open and close | **H1**, **M10** (logic), L4, L7 |
| U3 | Status rules now inside `web/app.py` | what each action does to the status, stage, flags and audit; the busy check; promote, division and rename guards | (makes the routes thin) |
| U4 | Login session cache | cache and its invalidation, including the race | L8 |
| U5 | Diff | crawl-failure flags on changed rows | L10 |
| U6 | Infrastructure (`infra/tests/test_synth.py`) | the alarm topic has a subscriber, an alarm for "no healthy target", the connection alarm fits the pools, `StopTask` is scoped | M6, M7, L12, L14 |

U3 needs a behaviour-preserving refactor: the status rules move out of the `create_app` closures
into a plain module the routes call. The page snapshots and the journeys guard it.

Findings with no test, by nature: M1 (needs AWS), M8 (measured), M11, L9, L13, L15 (documents),
L16 (performance). N1 (found while writing the unit tests, 2026-10-09) has a unit test.

---

## 6. Gates: nothing is deleted until these pass

1. **Every code bug in the review is caught**: at least one test fails on today's code for each of
   H1, H2, M2, M3, M5, M9, M10, L1, L2, L4, L5, L7, L8, L10, L17 (and M4, M6, M7, L12, L14).
   Until a bug is fixed, its test runs as an expected failure (`xfail(strict=True)`): the suite
   stays green, and the fix turns it into a plain pass.
2. **Coverage does not drop**: all levels together stay at 95 % or more; unit coverage of
   `jobs.py` and `curation.py` reaches 80 % or more.
3. **Bug-finding power does not drop**: mutation testing (small deliberate code changes, each of
   which a good suite should catch) on `engine/`, `curation.py` and `jobs.py` kills at least as many
   mutants as today's suite. This is the measure of "fewer tests that still catch bugs".
4. **Time**: unit under 20 s with no Docker; integration under 90 s; end-to-end under 4 min.
5. A current test is removed only when the test that replaces it is named (section 7 mapping,
   written per file before deleting), and the gates above pass with it gone.

---

## 7. Phases

| Phase | Work | Result |
|---|---|---|
| P1 | Fake database + fake backends; contract test I2; bug-catching tests for every finding (expected failures) | the review's bugs are caught inside the repo |
| P2 | Unit expansion U1–U6, including the U3 refactor | unit coverage of the core modules at 80 % or more |
| P3 | Journeys J1–J9, browser journeys B1–B3 (section 9.1) and integration I1–I9 in their final form | the top two levels in their target shape |
| P4 | Per file: map each old test to its replacement, delete, re-run the gates | the old integration and end-to-end tests reduced to the target |
| P5 | Gates, CI timing, docs; the end-to-end gate before deploy, the post-deploy smoke test against dev, the recurring effectiveness check (section 9.1); the test-writing rules (section 9.2) applied to every new and kept test | the strategy in force |

## 8. Decisions (Bernard, 2026-10-09)

1. **Refactor for testability: yes.** Status rules move out of `web/app.py`; the job manager and
   curation service get a database interface an in-memory fake implements. Curators see no change;
   page snapshots and journeys guard it.
2. **Bug tests: expected failures** (`xfail(strict=True)` with the finding ID).
3. **Delete old tests: yes**, once each one's replacement is named and the gates pass.
4. **Mutation testing: yes, locally**, as the gate for "fewer tests still catch bugs".

---

## 9. Practices from the two CircleCI articles

Sources: "Testing pyramid" (circleci.com/blog/testing-pyramid) and "Test-driven development"
(circleci.com/blog/test-driven-development-tdd). Added 2026-10-09 after checking the plan against
both; the rest of this document already covered their other suggestions.

### 9.1 Pyramid: what was missing

| # | Practice | What we do |
|---|---|---|
| P-a | End-to-end tests run after merge and before deploying to staging or production | `deploy.yml` runs the end-to-end level (at least the journeys) before `cdk deploy`, and stops the deploy when it fails. Pull requests keep running it too (2–4 min). |
| P-b | End-to-end tests drive the user interface | Browser journeys with Playwright, in CI: **B1** open a collection and click through the stepper while a job runs (the 2026-10-06 stepper bug); **B2** accept and reject suggestions in the review table; **B3** sign in and out, and a curator cannot reach admin pages. They replace the laptop-only stepper check. |
| P-c | End-to-end tests in a fully deployed environment | A post-deploy smoke test against dev after each dev deploy: read-only checks plus one small crawl-to-promote run with the fake LLM, from the stress harness. Never against prod. |
| P-d | Keep assessing the suite's effectiveness | The gates of section 6 (bugs caught, coverage, mutation score, timing) re-run once a month and before each test or prod release; the results go in the strategy's log. |

### 9.2 Test-driven development: how tests are written

1. **Test first, for every change.** A bug fix starts from a failing test that shows the bug (red);
   the fix makes it pass (green); then the code is cleaned up with the tests still passing
   (refactor). New behaviour starts the same way, at the lowest level that can show it. The
   failing run is shown before the code is written: for work done with an AI assistant, that run
   is the evidence the test can fail. Think about the behaviour first; the test is the
   specification the code must meet.
2. **One behaviour per unit test**, in arrange, act, assert order, with specific assertions
   (equality, the exact exception and message), never a bare "is truthy".
3. **Names state the behaviour**: `test_a_promote_refuses_blank_titles`, not `test_promote_2`.
4. **Negative, equivalence and boundary cases** for every rule, not only the happy path. Boundaries
   this code has: the restart limit (the third and the fourth restart), progress at 4 KB,
   `bulk_job_min_urls` and one more, an empty collection, a collection of one page, the page cap.
5. **Named constants, not unexplained numbers**: shared values (for example the fake crawl's 10
   pages giving 8 documents and 2 failures) live in `tests/support` with a name.
6. **A comment or docstring says why a test exists**; a test for a known bug names its finding.
7. **Journeys are the exception to "one behaviour"**: each step's assertion names its step, so a
   failure points at the step that broke.
8. **Start with the API surface**: every route's response codes and basic input handling (I6)
   before deeper scenarios.
9. **Static analysis** beyond Ruff's current rules: a complexity limit and a type checker, adopted
   gradually (new and changed code first), so warnings do not pile up.
10. **Tests are the living documentation**: the rules tests and the journeys are where a new team
    member reads what the engine does.

---

## 10. Log

- **2026-10-09, P3 browser journeys B1–B3 done** (`tests/e2e/test_browser.py`). The engine runs as a
  real uvicorn process (fake LLM, fake crawler subprocess, the test database, login on); Chromium
  drives it through Playwright. B1 15 s, B2 4 s, B3 3 s; the end-to-end level is 80 tests in about
  2.5 min. B1 passed 3 of 3 runs on the current templates and failed on templates with the
  2026-10-06 stepper fix taken out (the step never opened), so it replaces the laptop-only
  `step5check.py`. CI installs Chromium before the end-to-end step (`test.yml`, `deploy.yml`).
- **2026-10-09, J3, J8, J9 stay in their existing files for now.** J3 (restart during every job
  kind) is `test_resume.py`, `test_resume_llm.py`, `test_resume_curation.py` and
  `test_index_resume.py`; J8 is `test_state_matrix.py`; J9 is `test_scrape_backend.py` plus the S3
  case in `integration/test_reuse_crawl.py`. Whether they merge into `test_journeys.py` is decided
  in P4, file by file, after the mutation baseline is in.
- **Mutation baseline (old suite, old code):** still running; 87 of 250 mutants done at the time
  of writing, 76 killed and 11 survived.
- **2026-10-09, integration I1–I9 in place.** Two new files fill the gaps; the rest exist:

  | # | File(s) | State |
  |---|---|---|
  | I1 | `test_migrations.py` (new): empty → latest and a second run changes nothing; dev's database at V10 (dev's code) and at V11 (dev's database before the 2026-10-09 rollback), with dev-shaped data, upgraded by the engine at start-up, then every page, Start curating and a per-URL edit. V2 and V12 data cases stay in `test_db_pg.py` | red checked: fails with V12 made a no-op and with the key backfill off |
  | I2 | `test_db_contract.py` | 29 tests, PostgreSQL and the fake |
  | I3 | `test_rule_counts.py`, `test_scoped_recompute.py` (the twin test, 28 s), `test_stored_counts.py` | |
  | I4 | `test_stored_counts.py` (M9 xfail) | |
  | I5 | `test_concurrent_writes.py`, `test_stored_counts.py` (M10 xfail), `test_db_pg.py` (L5 xfail) | |
  | I6 | `test_route_access.py` (new): one table of every route (open, signed in, admin); a route with no row fails; every route sent as anonymous, curator and admin. `/tab-body` equals the page's tab body in `test_page_snapshots.py`; health and the 503 in `test_db_health.py` and `test_busy_database.py` | red checked: fails with the admin check taken off collection delete |
  | I7 | `test_page_snapshots.py` | |
  | I8 | `test_event_loop.py` | |
  | I9 | `test_import_sqlite.py` | |

  The integration level is 257 passed and 3 xfailed in 133 s: over the 90 s gate until P4 removes
  the tests the unit level and the journeys now cover. The twin test (28 s) is the largest single
  cost; P4 checks whether a smaller collection still catches the same mutants.
- **2026-10-09, P4 mapping done; deletion waits for approval.** A per-test coverage map of the whole
  suite (694 passed, 19 xfailed) showed 1,290 of 11,869 covered lines reached only by 39 old files.
  Four groups mapped every test in those files to a kept test (COVERED), a new test at the lowest
  level that shows it (MOVED, each first seen failing on a deliberate break), a journey step
  (JOURNEY) or DROP with a reason. Journey steps added: J1 (handbook, attribution, history, per-row
  AI accept and its audit, accept-all refusals, a duplicate title blocking promote until Regenerate
  titles, the notifier), J4 crawl (second crawl and delete refused, the jobs panel, the htmx cancel,
  the cancel audit), J5 crawler crash (dashboard, panel, /jobs), J7 (history, audit, YAML, rule
  attribution, the curator filter); the per-status matrix moved into the J8 file. One test dropped
  outright: `test_concurrent_recompute_and_promote` still passed with the collection lock removed.
  Old files: e2e/test_auth.py e2e/test_bulk.py e2e/test_busy_database.py e2e/test_collection_division.py e2e/test_collection_rename.py e2e/test_curated_counts.py e2e/test_index.py e2e/test_index_key.py e2e/test_jobs_view.py e2e/test_llm.py e2e/test_llm_shared_limit.py e2e/test_review_round.py e2e/test_scale.py e2e/test_validate.py integration/test_api_collections.py integration/test_api_curation.py integration/test_api_scrape.py integration/test_auth.py integration/test_bulk.py integration/test_collection_division.py integration/test_collection_rename.py integration/test_content_hash.py integration/test_curated_counts.py integration/test_curation_stage.py integration/test_duplicate_titles.py integration/test_general_not_promotable.py integration/test_history.py integration/test_jobs_view.py integration/test_llm.py integration/test_metadata_completeness.py integration/test_progress_throttle.py integration/test_promote_selection.py integration/test_provenance.py integration/test_review_round.py integration/test_scale.py integration/test_sorting.py integration/test_state_matrix.py integration/test_stepper.py integration/test_url_identity.py 
- **Gates with the 39 old files ignored:** coverage 96 % overall (gate 95 %); unit 485 passed + 16
  xfailed in 16.8 s under coverage; integration 206 passed + 3 xfailed in 104.5 s under coverage;
  end-to-end 56 passed in 118 s; every known bug still has its expected failure. The mutation gate
  (baseline: old suite kills 180 of 250, 72.0 %) is running on the new suite.
- **Why the integration level is 206 tests, not about 30:** the routes and pages cannot run on the
  in-memory fake, because `create_app` builds the real `Database` in its lifespan. So most moved
  HTTP and page tests had to be integration tests. Letting `create_app` take a database (and
  growing the fake to the methods pages use) would move most of them to the unit level.
- **2026-10-09, mutation gate passed; the 39 old files deleted (working tree only, not committed).**
  Same 250 mutants as the baseline. New suite: 190 killed (76.0 %) as run; it killed 16 the old
  suite missed and missed 6 the old suite killed. Each of the 6 was traced to the old test that
  killed it: two (`engine/diff.py` 186–187, a collection division makes the row the SME's) to a
  replacement unit test whose setup made every row the SME's anyway (fixed: no document-type rule
  in that test); three to gaps the groups had flagged (`jobs.py` 1459 `_expected_docs`, 1130 the
  `index.key` audit line, 714 Suggest metadata's first progress), now unit tests; one (`jobs.py` 931)
  was a flaky kill in the baseline: the old suite passes with that mutant when run again. Each new or
  fixed test fails on its mutant. Corrected score: 195 of 250 (78.0 %) against the old 180 (72.0 %).
- **After the deletion:** unit 493 passed + 16 xfailed in 13.6 s; integration 206 passed + 3
  xfailed in 93.4 s (gate 90 s: 3 s over); end-to-end 56 passed in 115 s. 28 integration files, 8
  end-to-end files. A backup of tests/ from before P4 is in the session scratchpad.
