# Second assessment: scaling the curation engine for many curators

This is an independent second opinion on `ARCHITECTURE-REVIEW-2026-10-07.md`, which this document
calls "the first review". It changes no code. I re-read the code paths myself and did not take the
first review's claims on trust. Where I could check a claim by running something, I ran it.

The question is the same: how can several curators work on large collections at the same time,
without lag and without crashes? My answer agrees with much of the first review. It differs in
four places, and it adds findings the first review missed. The most important addition is this:
**on a large collection, several curators cannot work at the same time at all while a job runs.**
That is a lock-out, not a lag, and it is the first thing I would fix.

## How I checked

- I read the engine (`engine/patterns.py`, `engine/diff.py`), the curation service, the job
  manager, the database layer, and the web routes for every claim the first review makes.
- I timed how long it takes to build the row objects a recompute loads, for 100,000 rows, on this
  laptop. The numbers are in section 3.3.
- I wrote a small test, outside the repository, that re-curates a collection and then makes one
  ordinary edit. It fails. Section 3.4 has the result and the test.

Laptop numbers are lower bounds for Fargate. The ratio between the two is not measured.

---

## 1. Summary

**Where I agree with the first review.** The 2026-10-06 fix is correct and narrow. Pages count
rows on every render, and that will cause the next incident; the duplicate-title scan is the
likely candidate. SingleFlight is not a cache, and `touch()` on every progress event defeats it.
Promote should be a job, and it should only write rows that changed. Bulk accept should be SQL.
A read replica will not help the curator UI. Coordination must eventually move into Postgres so
the engine can run as more than one task. Index runs must be re-attachable. The LLM jobs need a
budget and a global limit. The COSMOS comparison is fair.

**What the first review missed.**

1. Every job locks its collection for its whole life. A Suggest-metadata run on a 100,000-URL
   collection takes hours, and during those hours no curator can make any edit on that
   collection. The same holds for an index run and its validation. (Section 3.1.)
2. The reason the lock is needed is a small design choice: the recompute rewrites the AI
   suggestion columns. If it stopped doing that, LLM jobs and curator edits could run side by
   side. The same change closes the race the first review describes in its section 4.8.
   (Section 3.2.)
3. Each recompute freezes the whole web server for about a second, because it builds 300,000
   Python objects on the event loop, not in a thread. That freeze hits every curator, not only
   the one who clicked. (Section 3.3.)
4. "Re-curate everything" has a bug. The first ordinary edit after it removes every unchanged page
   from the review queue. I reproduced it. (Section 3.4.)
5. The Rules tab loads every URL of the collection and runs every glob against it in Python on
   each page view. (Section 3.5.)
6. Running more than one task needs migrations that old and new code can both run against. The
   first review's rolling-deploy plan does not mention this. (Section 3.6.)

**Where I disagree with the first review.**

1. Its Tier 2 moves the recompute into SQL. That moves CPU load from the engine, which uses one of
   its four vCPUs, onto RDS, which has two vCPUs and is the component that failed. It is also a
   large, risky port during the October reindex. I would make the common edit cheap a different
   way. (Section 4.1.)
2. Its stats table puts the duplicate-title count in the LLM flush path, which runs every two
   seconds during a metadata job. That moves the expensive scan instead of removing it.
   (Section 4.2.)
3. Its advisory-lock example assumes the recompute is one transaction. Today it is about ten. The
   order of work has to account for that. (Section 4.3.)
4. Its read-replica example has a wrong step. The conclusion still holds. (Section 4.4.)

**What I would do first.** Fix the lock-out and the re-curate bug, take the recompute's model
building off the event loop, and apply the first review's cheap page fixes. All of these are
hours to a few days each, and all of them help during October. Section 6 has the full order.

---

## 2. Claims from the first review that I checked

| Claim | Result |
|---|---|
| A Curate render runs the duplicate-title scan six times | Confirmed. `count_delta_ai` once, `list_delta_ai` twice (count and page), `incomplete_counts`, `duplicate_title_counts`, `duplicate_titles_for` once each. |
| `#job-watch` fetches the whole page while a job runs | Confirmed. Every 10 s while SSE is live, every 4 s when it is not. |
| Every progress event calls `touch()` | Confirmed. The bus listener touches the collection on every publish. The LLM pool publishes at most once per second. |
| A per-URL edit runs a full recompute | Confirmed. `replace_exact_pattern` ends in `_recompute`, which loads dump, curated, rules, deltas and failures whole. |
| `promote_urls` runs `promote()` on the event loop | Confirmed. `curation.py`, the call is not wrapped in `asyncio.to_thread`. |
| The curated upsert has no "only if changed" guard | Confirmed. |
| AI clears run outside the lock | Confirmed. `api_decide_ai` calls `clear_delta_ai` after `replace_exact_pattern` has released the lock. |
| Promote runs inline in the request | Confirmed. It does not go through `run_or_job`. |
| Index jobs cannot be adopted or killed after a restart | Confirmed for the ECS backend. |
| "Postgres handles a 30-million-row LIKE join in seconds" and "an edit becomes sub-second" | Not verified, and the two claims conflict. See section 4.1. |
| The replica timeline creates a second rule on a second click | Wrong. See section 4.4. |

---

## 3. Findings the first review missed

### 3.1 Jobs lock the whole collection for hours

Every job runs inside `_guarded`, which takes the collection's lock and holds it until the job
ends. Every curator action calls `ensure_idle` first, which answers 409 if any job is running on
the collection. So while a job runs, the collection is read-only for everyone.

For small collections this is invisible, because jobs are short. For large ones it is not.

Take soho, with about 100,000 delta URLs after an exclusion pass. A curator starts Suggest
metadata. The job makes one LLM call per URL, 16 at a time. If a call takes 3 seconds, the job
runs for about 100,000 ÷ 16 × 3 s ≈ 5.2 hours. (The 3 seconds is an illustration, not a
measurement.) For those five hours:

- nobody can add an exclusion rule on soho;
- nobody can accept or edit a title, even on rows the job finished an hour ago;
- nobody can promote the rows that are ready.

The index path is similar. The mastcamz test re-index on 2026-09-29 re-upserted 26,301 documents in
about 2.5 hours. The index job holds the lock through dispatch, polling and validation, so the
collection is frozen for the whole run.

So the user's goal, "multiple curators on one large collection at the same time", is not limited
by speed today. It is blocked by design whenever a job runs. Faster pages do not change that.

### 3.2 Why the lock is needed, and how to remove the need

The lock exists for a real reason. A recompute loads the delta rows, computes for a few seconds,
then writes every delta row back, **including the AI suggestion columns**. Look at
`Database._DELTA_COLS`: it lists `title_ai`, `division_ai`, `document_type_ai`, their confidences,
`ai_model`, `ai_error` and the rest. The recompute copies them from the rows it loaded
(`diff.py`, `_AI_FIELDS`) and the upsert writes them back.

Meanwhile, a metadata job writes those same columns every two seconds. If both ran together, this
would happen:

```
t=0.0  recompute loads delta row p17 (title_ai = NULL, not classified yet)
t=0.8  metadata job writes p17: title_ai = "SOHO LASCO C2 Movies"
t=3.0  recompute writes p17 back with title_ai = NULL   ← the suggestion is lost
```

The same shape causes the first review's section 4.8 race, where a rejected suggestion comes back.

The fix is to give each column one owner. The recompute owns the effective columns: `kind`,
`title`, `division`, `document_type`, `excluded`, `edited_by` and so on. The LLM jobs and the
accept/reject actions own the AI columns. In `replace_deltas`, keep inserting AI columns for new
rows, but stop updating them on existing rows:

```python
# db.py, replace_deltas: the ON CONFLICT ... DO UPDATE SET list
_AI_COLS = {"title_ai", "division_ai", "document_type_ai", "title_ai_conf", "division_ai_conf",
            "document_type_ai_conf", "ai_model", "ai_content_hash", "ai_error", "ai_failures",
            "title_ai_before", "division_skipped"}
data = [c for c in cols if c not in ("collection_id", "url") and c not in _AI_COLS]
```

A row that stays a delta keeps whatever AI values are in the table at write time. A row that
leaves the queue is deleted, as now. A new row starts with the carried-forward values, as now.
Without a race, the result is the same as today. With a race, the newer AI value wins, which is
the right answer.

With column ownership in place, the lock can be narrowed. Not every job conflicts with every
action. This is the matrix I would start from. Each "allow" needs a test.

| Running job | Rule add/delete, per-row edits, ✗/✓ | Accept-all AI | Promote | Re-crawl, Start curating |
|---|---|---|---|---|
| Suggest metadata | allow | block (suggestions still arriving) | block | block |
| Suggest patterns | allow | allow | block | block |
| Regenerate titles | block (it reads the duplicate groups) | block | block | block |
| Index to test: export phase (minutes) | block (recompute writes `curated_urls` flags) | block | block | block |
| Index to test: polling and validation (hours) | allow | allow | block | block |
| Index to prod, validate | allow | allow | block | block |
| Scrape and ingest | block | block | block | block |
| Recompute, bulk accept (curation jobs) | block | block | block | block |

To use it, `ensure_idle` takes the action's name and checks it against the running job's kind.
The job stops holding the curation lock for its whole life. It takes the lock only around the
writes that conflict, as the curation jobs already do.

The effect on soho: during a five-hour metadata run, curators keep excluding, editing and
reviewing the rows that already have suggestions. During a two-hour index run, they keep curating
the next round.

### 3.3 Each recompute freezes the whole server for about a second

The first review says the recompute's CPU work runs in a thread and competes for the GIL. That is
true for the diff itself. But a large part of each recompute runs on the event loop, before and
after the thread:

- `load_dump`, `load_curated` and `load_deltas` build one Pydantic model per row, inside
  `async def`, on the event loop.
- `load_rules` builds one `Rule` per row between `fetchmany` calls, on the event loop.
- `replace_deltas` builds the COPY tuples and calls `copy.write_row` for every delta row and every
  effect row, on the event loop.

While the event loop is busy, the server answers nothing: no page, no fragment, no SSE event, no
health check. I timed the model building for 100,000 rows on this laptop:

| Step | Time on this laptop |
|---|---|
| 100,000 `DumpUrl` models | 0.09 s |
| 100,000 `CuratedUrl` models | 0.15 s |
| 100,000 `DeltaUrl` models | 0.28 s |
| COPY tuples for 100,000 delta rows | 0.18 s |
| **Total for these four** | **0.70 s** |

That excludes row decoding, the up-to-400,000 effect rows and the 300,000 rules, so the real
stall is larger. The 2026-09-18 audit measured a worst freeze of 2.8 s, which fits.

Here is what that means with four curators. Each edit freezes the server for about a second. Four
curators each making one edit every ten seconds freeze it for about four seconds in every ten.
Every other curator's click waits behind those freezes. This is a large part of the "lag" curators
feel, and it is separate from the time their own edit takes.

The fix is mechanical. Run the row-to-model conversion in a thread:

```python
async def load_dump(self, collection_id: str) -> list[DumpUrl]:
    async with self._conn() as conn:
        cur = await conn.execute(SQL, (collection_id,))
        rows = await cur.fetchall()
    return await asyncio.to_thread(lambda: [DumpUrl(**r) for r in rows])
```

For the COPY, build the payload in a thread and write it in large chunks, rather than one
`write_row` per row on the loop. Add an event-loop lag probe first. A task that sleeps 100 ms and
logs how late it woke up is enough. Then the freeze becomes a number on a dashboard instead of an
inference.

### 3.4 "Re-curate everything" collapses on the first edit

"Re-curate everything" calls `recompute(review_all=True)`, which queues every included page as
`modified`. The flag is not stored anywhere. The next recompute, which runs after any ordinary
edit, uses `review_all=False`. That recompute finds the unchanged pages unchanged and deletes their
delta rows, together with any AI suggestions on them.

I reproduced it with this test, run against the real API on the test database:

```python
async def test_recurate_queue_survives_one_edit(crawler_client):
    c = crawler_client
    await setup(c); await classify(c)
    await c.post("/api/collections/ex.org/promote")
    queued = (await c.post("/api/collections/ex.org/recompute?all=true")).json()["modified"]
    await c.post("/api/collections/ex.org/urls",
                 json={"url": "https://ex.org/p2", "type": "title", "value": "Edited by hand"})
    assert (await coll(c))["delta_count"] == queued
```

Result:

```
AssertionError: queue shrank from 8 to 1 after one edit
```

On soho this means: a curator re-curates 96,000 pages, runs Suggest metadata for hours, accepts
one title, and 95,999 rows with their suggestions disappear from the queue. The existing test of
this feature does not make an edit after re-curating, so it passes.

The fix is to store the review round. One way: a `review_all` boolean on `collections`, set by
"Re-curate everything", passed to every recompute, and cleared when the queue is promoted. Add the
test above to `tests/test_review_round.py`. This is independent of scale, and I would fix it this
week.

### 3.5 The Rules tab does whole-collection work on every view

`rules_context` calls `CurationService._with_stats` for the rules on the page. That function loads
every URL of the current set with `set_urls`, and every dump URL as well if any exclude rule is on
the page. It then runs `match_counts`, which compiles every glob on the page and runs it as a
regular expression against every URL, in a thread.

On soho, opening the Rules tab loads up to 200,000 URLs and runs each glob on the page against
100,000 of them. Every view of the tab does this again. It holds the GIL for that time and uses
the read pool for the loads.

The cheaper source is already in the database. `pattern_effects` records which rule decided each
URL, and a glob's match count over a set is one `LIKE` count that Postgres can answer with an
index on `url text_pattern_ops` for prefix globs. Or store the match count per rule when the
recompute already knows it.

### 3.6 Rolling deploys need compatible migrations

The first review's Tier 1 sets `min_healthy_percent=100`, so new tasks start while old tasks still
run. Migrations run at startup in the serving process. The first new task migrates the schema while
the old tasks keep serving on it. Any migration that renames or drops a column, or adds a NOT NULL
column without a default, breaks the old tasks for the length of the deploy.

So rolling deploys need a rule: each migration must work with the code before it and the code
after it. Add a column in one deploy, start using it in the next, and drop the old one in a third.
V11 and V12 would have been fine. V9, which dropped two text columns, would not.

---

## 4. Where I disagree with the first review

### 4.1 Moving the recompute into SQL is the wrong first move for edits

The first review's Tier 2 ports the recompute into SQL. I see three problems.

**It loads the component that failed.** On dev, with five jobs on 100,000-URL collections, RDS
reached 72 percent CPU on two vCPUs. The engine task used about one of its four vCPUs. A SQL
recompute moves the most expensive work from the idle side to the busy side.

**The cost claim is not consistent.** The first review estimates the glob join for 300 globs and
100,000 URLs at 30 million `LIKE` comparisons, "seconds, not minutes". It then says an edit
"becomes a sub-second statement set". Both cannot hold if every edit re-runs the whole join. A
few seconds of RDS CPU per edit, with four curators, is the overload that caused the incident.

**The port is larger than it looks.** The Python engine does more than `LIKE` and `DISTINCT ON`:

- `canonical_key` normalization and `url_rank` choose which spelling of a page pairs with which
  curated row;
- an exact include or exclude outranks every glob, and the newest exact rule wins among spellings;
- title rules are templates with `{url}`, `{title}` and `{collection}`;
- the collection's division sits between rules and curated values;
- a missing curated URL is a removal, or is kept with a reason, depending on the crawl failure
  and the page cap;
- `edited_by` is derived from the sources of the winning rules.

Each of these needs an exact SQL twin and a proof of equivalence. That is weeks of careful work.
Doing it during the October reindex is risky.

**What I would do instead.** Most edits are per-URL: accept a title, set a division, ✗ one row.
A per-URL rule matches only the dump and curated URLs that share its page's canonical key. In the
engine, every step of the per-URL loop depends only on that URL's dump row, its curated pair, the
rules that match it, and its previous delta row. So a recompute limited to the URLs with that
canonical key gives exactly the delta rows and effects that a full recompute would give for them,
and leaves every other row as it is.

For soho, a title accept would then:

1. load the dump and curated rows with that canonical key (usually one of each);
2. load the glob rules (hundreds) and the exact rules with that canonical key (an index probe);
3. run the existing `recompute()` function on that tiny input;
4. upsert or delete that one delta row and its effect rows, and adjust the stored counts by the
   difference.

That is milliseconds, in Python, with the existing pure engine. Glob changes, re-crawls, Start
curating and Re-curate everything keep the full recompute.

**This conflicts with a constraint Bernard set on 2026-09-18: do not propose partial
recomputes.** I raise it anyway, because I think it keeps the constraint's purpose. The purpose was
that every change anywhere shows up in the deltas. A per-URL rule cannot change anything outside
its own canonical key, so the scoped result is identical, not approximate. The way to prove it is
a property test: apply random per-URL edits to random collections, run both the scoped and the
full recompute, and assert that the delta tables and effects are equal after each edit. If that
test cannot be made to pass, the idea is wrong and should be dropped. The decision is Bernard's.

If scoped recompute is rejected, the next best order is: take the loads off the event loop
(section 3.3), then slim the row models (audit R2), then add a process pool (audit R3). That keeps
the work on the engine's idle CPUs. Keep the SQL port as a later option, decided by measurement.

### 4.2 The duplicate count does not belong in the LLM flush

The first review's `collection_stats` table is a good idea for most counts: delta counts by kind,
the excluded count, the curated counts, the rule count, and the pending-suggestion counts. Each of
those can be updated in the same transaction as the write that changes it, cheaply.

The duplicate count is different. The review tables count duplicates with pending AI titles
treated as accepted. A metadata job writes new AI titles every two seconds. So an exact stored
duplicate count would have to be recomputed in every LLM flush, which puts the whole-collection
scan on the writer, every two seconds, for the length of the job. That is the same load the review
wants to remove, moved from pages to the job.

Two other details matter:

- The promote gate counts duplicates with pending suggestions **not** accepted. That is a second
  key. One generated column cannot serve both.
- The scan also includes curated rows that no delta row shadows, by URL or by `renamed_from`. An
  index on a title key does not remove that anti-join.

What I would do:

1. Add the title key column and index, and measure the scan on a 100,000-row dev collection. It
   may become fast enough on its own.
2. If it does not, keep the duplicate count out of the write path. Refresh it in the background,
   at most once every few seconds per collection, and store it with the time it was computed.
   When the stored value is older than the collection's last change, the page shows "recounting".
   Curators see an honest number, and nothing scans on every flush.

### 4.3 The advisory lock has to wrap the recompute as it is today

The first review's lock example takes `pg_advisory_xact_lock` inside "the write transaction". It
also says the lock closes the race "once the recompute is one transaction". But it schedules the
lock in Tier 1 and the single transaction in Tier 2. In between, the recompute is about ten
transactions on different pooled connections. A transaction-scoped lock in any one of them does not
cover the others.

Two ways to make Tier 1 correct before Tier 2:

- **One connection, one transaction for the whole recompute.** Pass one connection through the
  loads and the write. Take `pg_advisory_xact_lock` first. This holds one work connection for about
  six seconds per edit on a large collection, which the pool can afford. It is a refactor of the
  `Database` methods, which today each open their own connection.
- **A session-level lock on a dedicated connection.** Take `pg_advisory_lock` on a connection that
  is not returned to the pool until `pg_advisory_unlock`. This is simpler to bolt on, but a crash
  between lock and unlock must close the connection, or the lock leaks.

I prefer the first. It also makes the edit atomic, which fixes the "rule exists but no delta
reflects it" window the first review describes.

### 4.4 The replica example is wrong, but the conclusion holds

The first review's timeline says a curator who clicks ✓ twice, because a lagging replica shows the
old title, creates a second rule. That does not happen. `api_url_edit` looks up the existing exact
rule with `exact_patterns_for`. If the value is the same, it runs a recompute and writes no rule.
If the value differs, `replace_exact_pattern` deletes the old rule before it inserts the new one.

The real harm is simpler. After the click, the page reload comes from the replica and shows the old
value. The curator thinks the click failed, or works on from a stale list, or sees counts that do
not match the row they just changed. The engine was designed so this cannot happen. That is enough
reason not to put the UI on a replica.

### 4.5 "Tier 0, days" mixes hours and weeks

Some Tier 0 items are hours: the per-request memo, `#job-watch` doing nothing on progress, `touch()`
only on writes, the promote fixes, the auth cache, `pg_stat_statements`. The `collection_stats`
table is not. Every writer must update it correctly, including promote, partial promote, ingest,
rule deletes and the background curation jobs, and each needs a test that the stored value equals
the counted value. I would split it out and give it its own week.

---

## 5. COSMOS

I agree with the first review's reading of COSMOS. COSMOS is not a scaling reference. Its pattern
application, its counts and its promote are the patterns to avoid. Its compare-and-swap claim, its
advisory-lock helper and its separate worker container are worth copying. The per-run LLM budget
and the batch API from the COSMOS-Next design are worth copying.

One addition. COSMOS's `ATOMIC_REQUESTS` holds a transaction open for the whole request. Its
pattern application runs inside the request with per-row saves. Together, that means a long
pattern application holds row locks on every URL it touched until the request ends. The engine
should take the atomicity, as the first review says, but keep each transaction short. Section 4.3's
first option holds one connection for the recompute's duration only, not for the request.

---

## 6. What I would do, in order

The order puts first what helps curators during October with the least risk.

### This week: unblock and stop the freezes (hours each)

1. **Fix "Re-curate everything".** Store the review round on the collection and pass it to every
   recompute. Add the test from section 3.4.
2. **Column ownership.** Stop the recompute from updating AI columns on existing rows
   (section 3.2). Add a test that a metadata flush between a recompute's load and its write
   survives.
3. **Event-loop freezes.** Add the loop-lag probe. Move model building and COPY payloads into
   threads (section 3.3). Move `promote_urls`'s `promote()` into a thread.
4. **The first review's cheap page fixes.** Memoize repeated lookups per request. Make `#job-watch`
   idle on progress and refresh once on job end. Call `touch()` on writes and job state changes
   only. Throttle progress to one event per 2–3 s per collection, with a trailing event.
5. **Promote.** Route it through `run_or_job`. Add the `IS DISTINCT FROM` guard.
6. **Observability.** RDS parameter group with `pg_stat_statements` and a 2-second slow-query
   log. Alarms on RDS CPU, ALB 5xx and latency. Configure Python logging at INFO.

### Next one to two weeks: let curators work during jobs, make edits cheap

7. **Narrow the job lock** with the matrix in section 3.2, one job kind at a time, each with a
   test. Start with Suggest metadata, because it is the longest and the most common.
8. **Scoped recompute for per-URL edits**, if Bernard accepts it, behind the equivalence property
   test (section 4.1). Otherwise, slim models and a process pool.
9. **`collection_stats`** for the cheap counts, written in the same transaction as each write.
   Title key and index, measured; background refresh for the duplicate count if needed
   (section 4.2).
10. **Row fragments instead of `HX-Refresh`** for per-row actions, as the first review proposes.
11. **Rules tab** counts from SQL or stored values (section 3.5).
12. **LLM budget** per run.
13. **Index runs**: store the ECS task ARN, re-attach after a restart, add `kill()`.

### After the October peak: more than one task

14. **Coordination in Postgres**: the lock as in section 4.3, job claims with `SKIP LOCKED`,
    heartbeats, `LISTEN/NOTIFY` for events.
15. **Compatible migrations** as a written rule (section 3.6), then **web and worker roles** with
    rolling deploys.
16. **Batch API** for large Suggest-metadata runs, global LLM limit.
17. **SQL recompute** only if measurements after steps 3, 8 and 9 still show the edit path as the
    limit, and only if RDS has headroom.

### What each stage should achieve

These are targets to check with the stress harness, not measurements.

| After | Same large collection while a job runs | Per-row edit, 100K, 4 curators | Server freezes per edit |
|---|---|---|---|
| today | blocked (409) | 10–26 s, full reload | about 1 s, all curators |
| this week | blocked (409) | 6–12 s | near zero |
| next two weeks | open, except promote and re-crawl | under 1 s with scoped recompute; 3–6 s without | near zero |

---

## 7. How to verify

- The test in section 3.4 must pass after the fix and stay in the suite.
- A test that runs a recompute and a metadata flush concurrently on one row, in both orders, and
  checks that the AI value survives.
- For each relaxed cell of the lock matrix, a test that runs the action during that job kind and
  checks both results.
- The scoped-recompute property test, if that path is taken.
- A test that a stored count equals the counted value after each kind of write.
- The loop-lag probe, logged, and read during the next dev stress run. The stress harness should
  add one curator who only clicks ✓ while another curator's metadata job runs on the same
  collection. Today that curator gets 409s. After step 7 they should not.

---

## Appendix: the probe test as run

File kept outside the repository, run with the repository's pytest configuration and fixtures:

```python
"""Probe: does the 'Re-curate everything' queue survive the first ordinary edit?"""
from tests.conftest import classify
from tests.test_review_round import coll, setup


async def test_recurate_queue_survives_one_edit(crawler_client):
    c = crawler_client
    await setup(c)
    await classify(c)
    assert (await c.post("/api/collections/ex.org/promote")).status_code == 200
    r = await c.post("/api/collections/ex.org/recompute?all=true")
    queued = r.json()["modified"]
    e = await c.post("/api/collections/ex.org/urls",
                     json={"url": "https://ex.org/p2", "type": "title", "value": "Edited by hand"})
    k = await coll(c)
    assert k["delta_count"] == queued, f"queue shrank from {queued} to {k['delta_count']} after one edit"
```

```
.venv/bin/python -m pytest -c pyproject.toml -p tests.conftest <scratch>/test_recurate_probe.py -q
FAILED ::test_recurate_queue_survives_one_edit - AssertionError: queue shrank from 8 to 1 after one edit
```
