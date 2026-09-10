# End-to-end test of the curation engine on dev

A manual walk through every step of the pipeline in the deployed UI, with the AWS-side checks
that prove each hop actually happened. One pass takes about 30 minutes, most of it waiting on the
crawler and the indexer. Deployment and account setup are in `docs/deploy-dev.md`; this document
assumes the app is up and you can sign in.

## 0. Before you start

```bash
export AWS_PROFILE=sde-dev                       # aws sso login --profile sde-dev first
CF=$(aws cloudformation describe-stacks --stack-name CurationEngine-dev \
  --query "Stacks[0].Outputs[?OutputKey=='CloudFrontUrl'].OutputValue" --output text); echo $CF
CRAWLER_BUCKET=$(jq -r .crawler_bucket infra/envs/dev.json)
HANDOFF_BUCKET=$(jq -r .cosmos_index_bucket infra/envs/dev.json)
INDEXER_CLUSTER=$(jq -r .indexing_cluster_name infra/envs/dev.json)
INDEXER_FAMILY=$(jq -r .indexing_task_family infra/envs/dev.json)
```

- Accounts: `admin` with the `app_password` secret (`aws secretsmanager get-secret-value --secret-id /sde-curation-engine/dev/app_password --query SecretString --output text`) unless it was changed at Account; other users are created at **Users**.
- Second terminal: `make logs ENV=dev PROFILE=sde-dev` (the engine log; every step below produces lines there)
- Test site: `https://aurorasaurus.org` with **Max pages** 15. Small, public, stable, and cheap to index.
  Any site works, but keep the cap low: everything you index lands in the shared dev
  `sde-web-subset` index.
- On dev, **Index to test** and **Index to prod** both write to the dev OpenSearch collection.
  The difference is only which target the run is recorded against and which gate applies.

Pipeline the UI walks, as shown in the step bar on a collection page:

| # | Step bar | Status value | Reached by |
|---|---|---|---|
| 1 | Backlog | `backlog` | Add collection |
| 2 | Scraped | `scraped` | Scrape / Re-scrape |
| 3 | Curating | `curating` | Start curating |
| 4 | Curated | `curated` | Promote → curated |
| 5 | Test index | `config_generated` | Index to test + validation passed |
| 6 | Live | `live` | Index to prod |

## 1. Access and login

| Do | Expect |
|---|---|
| `curl -s $CF/health` | `{"ok":true,"db":"ok",…}` without a cookie |
| `curl -s -o /dev/null -w '%{http_code}\n' $CF/api/collections` | `401` |
| Open `$CF` in a browser | redirected to `/login` |
| Enter a wrong username or password | "Wrong username or password." on the page, still on `/login` |
| Sign in as `admin` | the dashboard; header shows **signed in as admin · Account · Users · Sign out** and a green dot (SSE connected) |
| **Users** → add `tester` (curator, 8+ char password) | row appears; the audit trail on any collection's Activity tab later shows `user.create` by admin |
| Sign out, sign in as `tester` | header shows **signed in as tester** with **Account** but no **Users** link; a collection page has no **Delete collection** button; `/users` answers 403 |
| **Account** → change password, sign out, sign in with the new one | works; the old password is refused |
| As admin, **Users** → Disable `tester` while tester is signed in elsewhere | tester's next click lands on `/login`; Enable restores access |
| Click **Sign out** | back on `/login`; the browser back button does not show the dashboard |

## 2. Add a collection

On the dashboard form: **Seed URL** `https://aurorasaurus.org`, **Display name** `Aurorasaurus`,
a **division** (e.g. Heliophysics), **Max pages** `15`, then **Add collection**.

| Expect | Check |
|---|---|
| a new row `aurorasaurus.org` at status **Backlog**, action button **Scrape** | dashboard |
| the collection page opens from the row; step bar shows step 1 of 6 | click the row |
| the same seed URL cannot be added twice | add it again → error "already exists" |
| the record is on disk (EFS) | `aws ecs execute-command …` is overkill; instead trust step 11 below |

## 3. Scrape (crawler over SSM)

Click **Scrape**. The button turns into "scrape running…" and the collection is locked (other
actions return "job is running").

| Expect | Check |
|---|---|
| engine log: a line with the SSM command id | `make logs` |
| the crawler ran the command | `aws ssm list-command-invocations --details --max-items 1 --query "CommandInvocations[0].[Status,CommandId]" --output text` → `Success` |
| the dump landed in the crawler bucket | `aws s3 ls s3://$CRAWLER_BUCKET/scraped_collections/ \| tail -3` → a new file for aurorasaurus.org |
| status **Scraped**, step 2; **Dump** count ≈ 15 | collection page (1–3 min for a small site) |
| **Dump** tab lists the URLs with scraped titles; filters *included only / excluded only / included + excluded* work | Dump tab |
| **Status history** shows backlog → scraped with the note "scrape ok: N documents", **By** = `system`; the audit trail shows `scrape.start` by you | collection page, Activity tab |
| the job is recorded | `curl -s -b <cookie> $CF/api/collections/aurorasaurus.org/jobs` or the job list on the page: kind `scrape`, state `succeeded` |

Negative: while a scrape is running, click **Start curating** → 409 "job … is running". Click
**✕ cancel** on the running job → job `failed` with "cancelled by user", status stays where it was.

## 4. Curate: deltas and patterns

Click **Start curating** → status **Curating**, step 3. The **Deltas** tab now lists every dump
URL as a `new` delta (nothing is applied until promoted).

Patterns (tab **Patterns**, form **Add a pattern**). Add one of each type and watch the effect
on the Delta URLs tab after **Recompute delta URLs** (recompute also runs automatically after each change):

| Type | Example value | Expect |
|---|---|---|
| `exclude` | a URL pattern that matches some dump URLs, e.g. `*/tag/*` or `*/page/*` | those URLs flip to *excluded* in Dump/Deltas; excluded count goes up |
| `include` | a pattern inside the excluded set | those URLs come back (include beats exclude for the most specific match) |
| `title` | pattern `*` with a title template such as `{title} - Aurorasaurus` | every delta's title shows the templated value |
| `division` | pattern `*`, value `Heliophysics` | division column filled on every delta |
| `document_type` | pattern `*`, a document type | document_type column filled |

Per-URL edits: on a Dump/Delta row, change the division or title directly → a pattern for that
exact URL is created (most specific wins), replacing a previous one for the same URL rather than
duplicating it. Validation: a `title` pattern without a value is rejected (422), and an unknown
division is rejected.

Delete a pattern → recompute → its effect disappears. Patterns are also written as YAML under
`collections/` on EFS (the engine's file store), each with `created_by`. The Patterns tab's **By**
column and the delta tooltips ("… (by tester)") name whoever added each pattern.

## 5. LLM assist (OpenAI)

Prerequisite: the OpenAI key secret is set (deploy runbook section 4).

| Do | Expect |
|---|---|
| **✨ Suggest patterns** (button says *all N URLs · K calls*) | a `llm_patterns` job runs; the header chip reads *LLM calls in progress · i/K calls*; **Suggested patterns** appear — `global` rows first, then `llm` rows, all `exclude` — with **Accept** / **Reject**; log has no `openai` error |
| **Accept** one | it becomes a real `exclude` rule, deltas recompute, the matching URLs show *excluded* |
| **Reject** one | it disappears and nothing changes |
| **✨ Suggest metadata** (shows how many URLs are classifiable and which models) | the chip reads *LLM calls in progress · i/N URLs · k in flight*; per-URL title/division/document_type suggestions with a confidence on the Deltas rows; the review bar counts high / medium / low; job result shows tokens in/out |
| **Cancel** a running Suggest metadata, then run it again | rows classified before the cancel keep their `AI:` badges; the second run's total is only the remainder |

If the log shows `401` from OpenAI, the key is missing or wrong. With `LLM_PROVIDER=fake` (local
only) the same buttons return canned suggestions.

## 6. Promote

Click **Promote N deltas → curated** (or **Mark curated** if there are no deltas).

| Expect | Check |
|---|---|
| status **Curated**, step 4; **Curated** count = number of included URLs; excluded URLs are listed as "excluded from indexing"; status history row **By** = you | collection page |
| Delta URLs tab is empty ("No delta URLs") | Delta URLs |
| trying to promote again is a no-op / 409 "promote requires status 'curating'" | button gone |

## 7. Index to test (indexer over ECS) and validation

Click **Index to test**, confirm the dialog ("Export N URLs and index them into the TEST index?").
An `index_test` job runs. On dev this writes to the dev `sde-web-subset` index.

| Phase | Expect | Check |
|---|---|---|
| export | **Exported** N URLs → `s3://<hand-off bucket>/curated_collections/aurorasaurus.org/<run_id>/` | `aws s3 ls s3://$HANDOFF_BUCKET/curated_collections/aurorasaurus.org/ --recursive \| tail -3` → `documents.jsonl` then `manifest.json` |
| dispatch | an indexer task starts | `aws ecs list-tasks --cluster $INDEXER_CLUSTER --family $INDEXER_FAMILY --desired-status RUNNING` → one ARN; its log: `aws logs tail /ecs/api-scrapers-dev --since 5m --follow` |
| finish (2–5 min) | **Indexer** line: `N indexed · changed · deleted · index sde-web-subset · Ns`; `status.json` in `index_runs/` | `aws s3 ls s3://$HANDOFF_BUCKET/index_runs/ --recursive \| tail -3` |
| validation (~30 s later) | **Validation** line: `N / N — counts match · titles 100%`, "via direct" | engine log: `validated_by: direct`, no 403 |
| result | status **Test index** (`config_generated`), step 5; **Index to prod** and **Re-validate** buttons appear; **Re-index to test** also available | collection page |

If validation reports a count mismatch or title mismatches, the collection stays at **Curated**
with the mismatches listed; fix the deltas, promote, **Re-index to test**, or **Re-validate** if
the index simply had not refreshed yet.

## 8. Index to prod → Live

Click **Index to prod**, confirm. An `index_prod` job runs against the prod target (on dev: the
same collection). When it finishes: **Prod run** line on the page, status **Live**, step 6 shows
**Live ✓** with the hint "Re-scrape to start a new cycle". **Re-index to prod** remains available.

Negative: **Index to prod** is not offered until a test run has validated. Deltas pending after
a promote block indexing with "N deltas are pending — promote them first".

## 9. Second cycle: re-scrape flags re-curation

Click **Re-scrape** on the live collection.

| Expect | Check |
|---|---|
| status back to **Scraped**, a "needs re-curation" flag/banner on the row and page | dashboard + page |
| **Start curating** → the delta URLs are now relative to the previous dump: `new`, `modified` (changed title), `deleted` (gone from the crawl) | Deltas tab, kind column |
| curated URLs that disappeared show as `deleted` deltas; promoting removes them from the curated set | promote, then Curated count |
| Index to test again → the indexer reports `changed` / `deleted` counts instead of all-new | Indexer line |

## 10. Manual status changes

**Set status manually** on the collection page (with a note). Allowed moves follow the pipeline
plus the explicit back edges (scraped ← curating, curating ← curated, curating ← test index,
live → curating or scraped). Anything else is refused with "illegal status transition". Data
invariants also hold: you cannot set *scraped* on a collection with no dump, or *curated* with
nothing promoted. Every change appears in **Status history** with the note.

## 11. Persistence and restart behaviour

```bash
make redeploy ENV=dev PROFILE=sde-dev
aws ecs wait services-stable --cluster sde-curation-engine-dev --services sde-curation-engine-dev
```
Expect ~90 s of 503 from the URL, then: the collection, its dump, patterns, deltas, index runs and
status history are all still there (SQLite and the YAML files live on EFS), and the SSE dot goes
green again without a reload.

If a job was running during the restart it is marked `failed` with "cancelled by user or
shutdown" and the collection status is unchanged — start it again.

## 12. Notifications (optional)

Only if `/sde-curation-engine/dev/notify_webhook_url` is set to a Slack-compatible webhook: every
status transition posts a message with a link back to the collection on the CloudFront URL. A
broken webhook never blocks a transition (the error is logged).

## 13. Clean up

**Delete collection** on the page removes the engine's record and its YAML. It does **not** remove
the exported files in the hand-off bucket or the documents already in the dev index; those follow
the indexer's own lifecycle. If you want the test documents out of the shared dev index, re-scrape
with a cap of 1 and re-index, or ask the indexer team.

## Pass/fail sheet

| # | Area | Pass? | Notes |
|---|---|---|---|
| 1 | access, login, users, roles, sign out | | |
| 2 | add collection | | |
| 3 | scrape via SSM, dump | | |
| 4 | deltas + all five pattern types | | |
| 5 | LLM suggestions | | |
| 6 | promote | | |
| 7 | index to test + validation | | |
| 8 | index to prod → live | | |
| 9 | re-scrape cycle, delta kinds | | |
| 10 | manual status rules | | |
| 11 | restart persistence | | |
| 12 | notifications (optional) | | |
