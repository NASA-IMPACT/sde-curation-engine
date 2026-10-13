# Change decisions: what curators would see

This table lists every change suggested in `ARCHITECTURE-ASSESSMENT-2026-10-07.md`. For each one it
says what curators would see. Fill in the **Decision** column with **GO** or **NO-GO**. The
implementation plan will include only the GO items.

Meaning of the "What curators see" column:

- **None**: curators see nothing different, not even timing.
- **Faster only**: screens and behaviour are identical; things respond sooner.
- **Visible**: curators would notice a difference in what they see or can do.

Left out at your request: the per-run LLM budget and the batch API.

## Invisible to curators

| # | Change | What curators see | Recommendation | Decision |
|---|---|---|---|---|
| 1 | Recompute stops overwriting the AI suggestion columns | None. A rare case where a suggestion vanished, or a rejected one came back, stops happening. | GO | |
| 2 | Event-loop lag probe | None. It is a server-side measurement. | GO | |
| 3 | Build row objects and COPY data off the event loop | Faster only. Other curators stop freezing for about a second each time someone edits. The editor's own wait is unchanged. | GO | |
| 4 | Partial promote's computation moved to a thread | Faster only. Other curators stop freezing during someone's partial promote. | GO | |
| 5 | Fetch repeated lookups once per page | None. | GO | |
| 6 | `#job-watch` keeps its 10-second refresh, but fetches only the tab body and reads stored counts | None. The same updates at the same rhythm. | GO | |
| 7 | Mark data as changed only on real writes, not on progress ticks | None. Counts still update when the data changes. | GO | |
| 8 | Server sends job progress at most every 3 s | None. The browser already shows at most one refresh per 3 s. | GO | |
| 9 | Promote writes only the curated rows that changed | Faster only. Promote stays in the request, as today. | GO | |
| 10 | Exact stored counts instead of counting on every page | Faster only. The numbers are identical. | GO | |
| 11 | Indexes on the delta and rule tables, and an index for the duplicate-title check | Faster only. | GO | |
| 12 | Rules tab match counts taken from the database | Faster only. The counts must match today's exactly; a test will check it. | GO | |
| 13 | Scoped recompute for per-URL edits | Faster only. A ✓ or a title edit drops from seconds to well under one. The resulting rows are identical. **Conflicts with the 2026-09-18 "no partial recomputes" rule.** It ships only behind a test that proves the scoped result equals the full one. | GO, if you accept the rule exception | |
| 13b | Alternative to 13: slimmer row models plus a process pool | Faster only, by less than 13. | GO only if 13 is NO-GO | |
| 14 | Database monitoring, slow-query log, alarms, app logging | None while running. Turning on the database setting needs one RDS reboot, about one minute of downtime, at a time you choose. | GO | |
| 15 | Autovacuum tuning on the busiest tables | None. | GO | |
| 16 | Login lookups cached for 30 s | None for curators. An admin who deactivates a user sees it take effect up to 30 s later. | GO | |
| 17 | Coordination moved into the database, and migrations made safe for rolling deploys | None. | GO | |

## Visible to curators

| # | Change | What curators see | Recommendation | Decision |
|---|---|---|---|---|
| 18 | Fix "Re-curate everything" | After a re-curate, the queue stays full when they edit a row. Today one edit empties it, with its suggestions. | GO | |
| 19 | Allow edits while Suggest metadata runs | Row edits, accepts and rule changes would work during the job. New rows arriving in the review list would renumber it after each accept. A hand-decided row could receive a suggestion later. Accept-all would apply only what has arrived. | NO-GO. Keep today's freeze. | |
| 20 | Allow edits while Suggest patterns runs | Edits and accept-all would work during the job, while the suggestion list keeps growing. The job is short, so the gain is small. | NO-GO. Keep today's freeze. | |
| 21 | Allow edits while an index run is polling or validating | Curators can work on the next round during a test or prod index run, which can take hours. The first minutes of export stay locked. Promote stays disabled. Nothing reorders, because the job does not touch the review lists. | GO | **NO-GO** (Bernard, 2026-10-07): no edits during indexing |
| 22 | Index runs survive an engine restart, and cancel really stops them | After a restart, the run shows "running" and finishes, instead of "failed" and a duplicate run on the next click. | GO | |
| 23 | Separate web and worker tasks, with rolling deploys | Deploys no longer drop pages, break the live connection or fail running jobs. "Engine restarted while job was running" stops appearing. | GO | |
| 24 | Shared limit on LLM calls across all jobs | When several curators run Suggest metadata at once, each job shows slower progress but fewer failed calls. This limits concurrency, not spending. | GO | |

## Visible changes recommended against

Each has an invisible alternative in the first table.

| # | Change | What curators would see | Instead | Recommendation | Decision |
|---|---|---|---|---|---|
| 25 | `#job-watch` idle until the job ends | The tab freezes during a job and jumps at the end. | #6 | NO-GO | |
| 26 | Promote as a background job | A progress line and locked edits instead of a waiting request. | #9 | NO-GO | |
| 27 | Duplicate count refreshed in the background | A "recounting" label and a briefly stale number. | #10 and #11 | NO-GO | |
| 28 | Row swap instead of a page reload after ✓ / ✗ | No page flash, and the scroll position is kept. | Keep the reload; make it cheaper with #5, #6 and #10 | NO-GO | |

## Added 2026-10-07: jobs resume after a restart or deploy

All GO (Bernard). Condition: resuming must never cause an engine shutdown, a restart loop, or an RDS
memory or connection outage. The plan's item T5.0 builds those limits before any of these.

| # | Change | What curators see | Recommendation | Decision |
|---|---|---|---|---|
| 29 | Suggest patterns resumes | The job stays "running"; progress pauses for a minute or two, then continues. Finished batches are not asked again. | GO | GO |
| 30 | Regenerate titles resumes | The job stays "running" and continues. Groups already fixed are not asked again. | GO | GO |
| 31 | Index to prod resumes | The job stays "running" and finishes. Prod is partly written for a few minutes longer, then completed. | GO | GO |
| 32 | Validate and validate-prod resume | The validation stays "running" and finishes. Its 30-second wait happens again. | GO | GO |
| 33 | Index to test resumes during export | The run stays "running" and finishes. The "exported" count goes back to 0 and counts up again. | GO | GO |
| 34 | Recompute, bulk accept, bulk suggestions resume | The job stays "running" and finishes. | GO | GO |
| 35 | Suggest metadata resumes in place | One job keeps running, instead of a failed job followed by a new one. | GO | GO |

## Dependencies

- #13 and #13b are alternatives. Choose one at most.
- #10 must ship before #6. The cheaper tab refresh relies on stored counts.
- #17 must ship before #23. More than one task needs coordination in the database first.
- #22 and #29–#35 all depend on the plan's safe-resume foundation (T5.0).
- #23 depends on #22 and #29–#35, so a worker deploy fails no job.
- With #19, #20 and #21 at NO-GO, a collection stays read-only while any job runs on it, as today. Curators can still work on other collections in parallel.
