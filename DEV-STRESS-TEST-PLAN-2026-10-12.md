# Dev smoke test and stress test after the optimize-app merge (2026-10-12)

**Purpose.** Prove on dev, with the real Fargate task and RDS, that the merge of
`featuure/optimize-app` (PR #69, `origin/dev` = `963156e`) works, and that the 2026-10-06 test
crash cannot happen again. Then put dev back as it was.

**Rules for the whole run:**
- **No OpenAI calls.** Steps 3–5 run only while dev's task has `LLM_PROVIDER=fake` (step 2). The
  harness checks this itself before it starts and before every level, and refuses to run otherwise.
- **Dev only.** Never point the harness at test or prod.
- **Stop at the first failed check.** Write what you saw in the run log, then decide.
- **Rollback base:** manual snapshot `sde-curation-engine-dev-db-pre-optimize-20261012` (V10, taken
  2026-10-12 18:35 UTC). Rollback steps: `docs/dev-db-rollback.md`, Option C.

**Fixed values used below:**

| What | Value |
|---|---|
| AWS profile, region | `sde-dev`, `us-east-1` (account 998871305517) |
| Engine URL | `https://d320yr630n1a0l.cloudfront.net` |
| ECS cluster and service | `sde-curation-engine-dev` |
| Database | `sde-curation-engine-dev-db` |
| Crawler bucket, prefix | `sdecrawlerstack-crawlbucket0d63eba8-lhkxqnh8ophy`, `sde-curation-engine-prototype` |
| Admin password secret | `/sde-curation-engine/dev/app_password` |
| Indexer ECS cluster (shared) | `api-scrapers-cluster-dev` (SSM `/sde-curation-engine/dev/indexing_cluster_name`) |
| Harness | `~/projects/sde-curation-stress/concurrency.py` (outside the repo) |
| Baseline to compare with | `~/projects/sde-curation-stress/results/dev-20261006T222830Z/full.json` (old build, same ramp) |

Set once per shell:

```bash
export AWS_PROFILE=sde-dev AWS_REGION=us-east-1
C=sde-curation-engine-dev
URL=https://d320yr630n1a0l.cloudfront.net
BUCKET=sdecrawlerstack-crawlbucket0d63eba8-lhkxqnh8ophy PREFIX=sde-curation-engine-prototype
cd ~/projects/sde-curation-engine
# run SQL in the engine task (the image has no psql); read-only SELECTs only
dbq() { T=$(aws ecs list-tasks --cluster $C --service-name $C --query 'taskArns[0]' --output text)
  aws ecs execute-command --cluster $C --task "$T" --container engine --interactive --command \
  "python -c \"import os,psycopg;c=psycopg.connect(host=os.environ['DB_HOST'],dbname=os.environ['DB_NAME'],user=os.environ['DB_USER'],password=os.environ['DB_PASSWORD'],sslmode='require');print(c.execute('''$1''').fetchall())\"" 2>&1 | grep '^\[' ; }
```

---

## Step 0 — read-only smoke test (done 2026-10-12, passed)

Nothing is written. It confirms the deploy itself.

| Check | Command | Expected | Result 2026-10-12 |
|---|---|---|---|
| One task, new revision | `aws ecs describe-services --cluster $C --services $C --query 'services[0].[runningCount,deployments[0].taskDefinition]'` | 1, the newest revision | 1, rev 43 |
| Engine answers | `curl -s -w ' %{http_code}' $URL/health` | `{"ok":true,…} 200` | 200 in 0.2 s |
| Migrations ran | `dbq "select max(version) from schema_version"` | `[(19,)]` | V11–V19 at 18:57:50 UTC |
| Parameter group attached | `aws rds describe-db-instances --db-instance-identifier sde-curation-engine-dev-db --query 'DBInstances[0].DBParameterGroups[0].[DBParameterGroupName,ParameterApplyStatus]'` | `curationengine-dev-dbparams…`, `in-sync` | in-sync |
| Query statistics on | `dbq "select extversion from pg_extension where extname='pg_stat_statements'"` | one row | 1.11 |
| Slow-statement parameters cut | `dbq "show log_parameter_max_length"` | `1kB` | 1kB |
| RDS log kept 1 month | `aws logs describe-log-groups --log-group-name-prefix /aws/rds/instance/sde-curation-engine-dev-db --query 'logGroups[].retentionInDays'` | `[30]` | 30 |
| Key backfill finished | `dbq "select (select count(*) from dump_urls where canonical_key is null),(select count(*) from curated_urls where canonical_key is null)"` | `[(0, 0)]` | 0, 0 |
| Alarm emails confirmed | `aws sns list-subscriptions-by-topic --topic-arn arn:aws:sns:us-east-1:998871305517:sde-curation-engine-dev-alarms --query 'Subscriptions[].[Endpoint,SubscriptionArn]'` | both addresses with an ARN, not `PendingConfirmation` | both confirmed |
| Nothing running | `dbq "select kind,state from job_runs where state in ('queued','running')"` | `[]` | none |
| Shared LLM limit | task definition environment `LLM_WORKERS_TOTAL` | `64` | 64 |

---

## Step 1 — functional smoke test on the normal deploy (indexing and cancel, no LLM)

**Why:** L14 changed the indexer permissions. The engine now tags every indexer task, and may stop
only tagged tasks. A missing permission would make every Index to test fail, and no local test can
show it. This step uses no LLM feature, so the normal deploy (`LLM_PROVIDER=openai`) is safe.

**What it writes:** one cancelled and one full Index to test of an unchanged dev collection (the real
indexer, into the dev search index): the same documents again, nothing deleted. History rows (jobs,
index runs, audit) and a new export folder in S3 remain. Dev's own collections are kept.

1. **Pick a collection that indexing will not change.** Its last Index to test succeeded and it has
   not changed since: status `indexed` (or later), no delta URLs, no "needs re-indexing" chip. Then a
   full run writes the same documents and deletes nothing from the dev search index. Prefer the
   smallest one.
   ```bash
   dbq "select collection_id, status, curated_count, delta_count from collections where delta_count = 0 order by curated_count limit 6"
   ```
2. Find the indexer's cluster:
   ```bash
   IC=$(aws ssm get-parameter --name /sde-curation-engine/dev/indexing_cluster_name --query Parameter.Value --output text); echo $IC
   ```
3. **Index to test, then cancel (first).** In the UI: step 5, **Index to test**. Wait until the job
   shows a task ID (the indexer is dispatched), then press **Cancel**. Note the time.

   | Check | How | Expected |
   |---|---|---|
   | The task started | the job showed a task ID | yes. **If the job fails at once with `AccessDenied` / `ecs:TagResource`, stop: L14's permission is wrong.** |
   | The task carries the tag | `aws ecs describe-tasks --cluster $IC --tasks <task-arn> --include TAGS --query 'tasks[0].tags'` | `[{"key": "sde-curation-engine", "value": "<collection>"}]` |
   | The job fails as cancelled | the job panel | `failed`, "cancelled by <you>" |
   | The engine stopped the task | `aws ecs describe-tasks --cluster $IC --tasks <task-arn> --query 'tasks[0].[lastStatus,stoppedReason]'` | `STOPPED`, `cancelled in the curation engine`. **If the task is still running and the engine log shows `could not stop indexer task … AccessDenied`, stop: the StopTask condition does not match.** |
   | The index run is closed | `dbq "select run_id,state,error from index_runs where collection_id='<collection>' order by started_at desc limit 2"` | the cancelled run is `failed` |

   ECS shows a stopped task for about an hour only: run the `describe-tasks` checks within the hour.
   CloudTrail keeps the `RunTask`, `TagResource` and `StopTask` calls for 90 days.
4. **Index to test, to the end (second).** Start Index to test again on the same collection and let
   it finish. The collection ends in a clean "indexed" state.

   | Check | How | Expected |
   |---|---|---|
   | One task for the run | `aws ecs list-tasks --cluster $IC --started-by "sde-curation-engine:<collection>"` (first 36 characters) during the run | one task (H2) |
   | The task carries the tag | as above | the tag |
   | The job ends | the job panel | `succeeded`, validation passed |
   | Nothing deleted | the job's result / index run | 0 deletions, or the number expected |

**Verify both runs in one go (read-only):**
```bash
cd ~/projects/sde-curation-stress
AWS_PROFILE=sde-dev ~/projects/sde-curation-engine/.venv/bin/python verify_step1.py <collection> <start time, UTC, e.g. 2026-10-12T19:30:00Z>
```
It prints the collection's jobs and index runs, each indexer task's tag, status and stopped reason,
the engine's `RunTask` / `TagResource` / `StopTask` calls in CloudTrail (with any error), and the
engine log's errors. Run it within an hour of the cancel: ECS forgets a stopped task after about an
hour. CloudTrail times print in local time.

Candidates on 2026-10-12: `aurorasaurus.org` (6 curated pages, no delta URLs, last Index to test
succeeded 2026-09-30) or `mdscc.nasa.gov` (175 pages, last succeeded 2026-09-23). Not
`techport.nasa.gov` (23 delta URLs) or `pds.nasa.gov` (never indexed).

5. **Engine log, no errors:**
   ```bash
   aws logs filter-log-events --log-group-name /ecs/sde-curation-engine-dev \
     --start-time $(( ($(date +%s) - 3600) * 1000 )) --filter-pattern '?ERROR ?AccessDenied ?Traceback' \
     --query 'events[].message' --output text | head -20
   ```
   Expected: nothing about the index jobs.

**Pass:** all checks above. Then run step 2.

---

## Step 2 — deploy dev in stress mode (fake LLM)

**What changes:** `LLM_PROVIDER=openai` → `fake`, and the WAF limit 1,000 → 20,000 requests per 5
minutes per IP (the harness polls for many curators from one IP). The task size is already 4 vCPU /
16 GB on dev. The image is built from the local working tree: first check that it equals
`origin/dev`.

```bash
git fetch origin && git diff --stat origin/dev HEAD -- sde_curation infra   # must print nothing
git status --short -- sde_curation infra                                     # must print nothing
make diff   ENV=dev PROFILE=sde-dev CDK_CONTEXT="-c stress=true"
```

| Check on `make diff` | Expected |
|---|---|
| Task definition | only `LLM_PROVIDER` changes (`openai` → `fake`), plus the image if it was rebuilt |
| WAF | the rate rule's limit changes to 20000 |
| Database | **no change** to `Db` or `DbParams`. If either changes, stop: that would reboot the database. |

```bash
make deploy ENV=dev PROFILE=sde-dev CDK_CONTEXT="-c stress=true"
```

| Check after the deploy | Command | Expected |
|---|---|---|
| Fake LLM on the running task | `TD=$(aws ecs describe-services --cluster $C --services $C --query 'services[0].taskDefinition' --output text); aws ecs describe-task-definition --task-definition $TD --query 'taskDefinition.containerDefinitions[0].environment[?name==\`LLM_PROVIDER\`].value'` | `["fake"]` |
| One task | `aws ecs describe-services --cluster $C --services $C --query 'services[0].[runningCount,deployments[].status]'` | `1`, one `PRIMARY` deployment |
| Engine answers | `curl -s $URL/health` | `{"ok":true,…}` |
| Database not rebooted | `aws rds describe-db-instances --db-instance-identifier sde-curation-engine-dev-db --query 'DBInstances[0].DBInstanceStatus'` | `available`, and no reboot event in `aws rds describe-events --source-identifier sde-curation-engine-dev-db --source-type db-instance --duration 60` |

---

## Step 3 — small harness run (about 5 minutes)

**Why:** proves login, the S3 upload of the test crawls, the jobs and the cleanup, before the long
run.

```bash
cd ~/projects/sde-curation-stress && source newrun.sh dev "SMALL RUN after the optimize-app merge (fake LLM)"
export STRESS_PASSWORD=$(aws secretsmanager get-secret-value --secret-id /sde-curation-engine/dev/app_password --query SecretString --output text)
cd ~/projects/sde-curation-engine
.venv/bin/python ~/projects/sde-curation-stress/concurrency.py --url $URL \
  --s3-bucket $BUCKET --s3-prefix $PREFIX --aws-profile sde-dev \
  --sizes 2000:2 --levels 2 --rest 30 --cleanup --out $RUN_DIR/small.json 2>&1 | tee $RUN_DIR/small-run.log
```

| Check | How | Expected |
|---|---|---|
| Fake-LLM guard passed | first lines of `small-run.log` | `LLM_PROVIDER=fake` for the running revision; no `REFUSING TO RUN` |
| Jobs | `small.json` → `levels[].failed`, `succeeded` | every job succeeded, `failed` empty |
| Errors | `levels[].error_count`, `outages_s` | 0, none |
| Cleanup, collections | `grep -c "deleted .*: 204" $RUN_DIR/small-run.log` and `grep "deleted " … \| grep -vc ": 204"` | every run collection deleted, 0 not 204 |
| Cleanup, S3 | `aws s3 ls s3://$BUCKET/$PREFIX/ --recursive \| grep -c sde-stress-test` | 0 |
| No OpenAI | `aws logs filter-log-events --log-group-name /ecs/sde-curation-engine-dev --start-time <run start ms> --filter-pattern openai --query 'length(events)'` | 0 |

---

## Step 4 — full ramp, as on 2026-10-06 (about 50 minutes)

Same sizes, levels, tabs and browsers as the baseline run, so the two compare directly.

```bash
cd ~/projects/sde-curation-stress && source newrun.sh dev "FULL RUN after the optimize-app merge (fake LLM), same ramp as dev-20261006T222830Z"
cd ~/projects/sde-curation-engine
caffeinate -i .venv/bin/python ~/projects/sde-curation-stress/concurrency.py --url $URL \
  --s3-bucket $BUCKET --s3-prefix $PREFIX --aws-profile sde-dev \
  --sizes 5000:20,25000:20,100000:8 --levels 2,5,10,15,20 --mix ingest,recompute,patterns,metadata \
  --tabs-per-job 1 --dashboards 1 \
  --cleanup --out $RUN_DIR/full.json 2>&1 | tee $RUN_DIR/full-run.log
```

**Pass criteria (per level, from `full.json`):**

| Measure | Field | Pass |
|---|---|---|
| Jobs | `failed`, `succeeded` | 0 failed |
| Errors | `error_count` (5xx, 503, WAF 403, connection errors) | 0 |
| Outages | `outages_s` | none |
| Load-balancer risk | `alb_risk_s` | 0 |
| Task restarts | `cloud.task_restarts` | none |
| WAF blocks | `cloud.waf_blocked` | 0 |
| Pages | `page_s.p95` | ≤ 5 s at every level (the 2026-10-06 limit); expected lower than the baseline |
| Database CPU | `cloud.rds_cpu_pct_max` | below 80 % sustained (the `DbCpu` alarm fires at 70 % for 5 minutes: note it if it fires) |
| Connections | `cloud.rds_conns_max` | about 29–31 is normal (both pools full); above 40 means something else is connecting |

**Compare with the baseline:**

```bash
.venv/bin/python - <<'EOF'
import json, os
a = json.load(open(os.path.expanduser("~/projects/sde-curation-stress/results/dev-20261006T222830Z/full.json")))
b = json.load(open(os.environ["RUN_DIR"] + "/full.json"))
key = lambda L: (L["size"], L["level"])
old = {key(L): L for L in a["levels"]}
print(f"{'size x jobs':<14}{'page p95 old→new':<20}{'tabs p95 old→new':<20}{'rds cpu old→new':<18}{'conns old→new'}")
for L in b["levels"]:
    o = old.get(key(L))
    if not o: continue
    f = lambda x, k, s: x[k][s] if x.get(k) else None
    print(f"{L['size']:>6} x {L['level']:<5}{f(o,'page_s','p95')}→{f(L,'page_s','p95')}{'':<8}"
          f"{f(o,'tab_s','p95')}→{f(L,'tab_s','p95')}{'':<8}"
          f"{o['cloud'].get('rds_cpu_pct_max')}→{L['cloud'].get('rds_cpu_pct_max')}{'':<6}"
          f"{o['cloud'].get('rds_conns_max')}→{L['cloud'].get('rds_conns_max')}")
EOF
```

**After the run:**

| Check | How | Expected |
|---|---|---|
| Cleanup | as in step 3 | all run collections deleted, 0 S3 objects left |
| Alarm emails | the two inboxes | note every alarm that fired, with its time; compare with the levels |
| Slow statements | `aws logs filter-log-events --log-group-name /aws/rds/instance/sde-curation-engine-dev-db/postgresql --start-time <run start ms> --filter-pattern duration --query 'events[].message' --output text \| head -40` | statements over 2 s, each parameter cut at 1 kB; list the slowest |
| Top queries | `dbq "select calls, round(total_exec_time) ms, left(query,80) from pg_stat_statements order by total_exec_time desc limit 10"` | record them in the run log |

---

## Step 5 — the 2026-10-06 crash pattern (lane `bulk-exclude`)

**Why:** neither stress run so far reproduced what crashed test: a bulk accept that turned about
97,000 URLs of a 100K collection into exclusion rules, after which every page view ran a 7 s count,
and open tabs refreshed by job progress piled those counts up until `/health` failed.

**The lane** (`concurrency.py --mix bulk-exclude`, added 2026-10-12):

1. **Setup (not timed):** a 100K crawl where 97 % of the URLs sit under `/tag/` and 1 % under
   `/login/`; ingest, recompute, then Suggest patterns (fake LLM). The suggestions include an
   exclude glob for `/tag` (from the fake LLM or the packaged global excludes) and one for `/login`.
   The harness checks that both are there.
2. **Accept every suggestion** (`POST …/suggestions/bulk`, a job above 20,000 URLs): about 97,000
   URLs become excluded by rules.
3. **Delete the `/login` glob** (`DELETE …/patterns/{id}`, a full recompute in the request).
   Deleting a glob clears the stored excluded count, so page views until the recompute stores it
   again count it over the whole collection: the 7 s count of 2026-10-06.
4. **Keep the tabs refreshing** for `--bulk-after-s` seconds.

All along, the collection's tabs (`--tabs-per-job`) refresh the Curate page, the stepper and the
header every 3–10 s, and the dashboards refresh the jobs panel, as on 2026-10-06.

**Proved locally on 2026-10-12** (`~/projects/sde-curation-stress/results/local-20261012T190802Z/`,
1 × 25K, fake LLM, no network): accept 3.0 s (job), glob delete 0.4 s, 24,250 of 25,000 URLs
excluded by the `/tag` glob, 3 tabs and 1 dashboard refreshing, 0 errors. Not yet run at 100K.

**Limit:** the lane runs once per collection (the accept uses up the suggestions), so give one level
per size. The harness refuses a plan with more than one level.

**Run A — the crash shape: one bulk exclude next to one Suggest metadata job** (the metadata job's
progress events keep every tab refreshing, as the index jobs did on 2026-10-06):

```bash
cd ~/projects/sde-curation-stress && source newrun.sh dev "CRASH PATTERN A: bulk-exclude + metadata, 100K, 5 tabs each (fake LLM)"
cd ~/projects/sde-curation-engine
caffeinate -i .venv/bin/python ~/projects/sde-curation-stress/concurrency.py --url $URL \
  --s3-bucket $BUCKET --s3-prefix $PREFIX --aws-profile sde-dev \
  --sizes 100000 --levels 2 --mix bulk-exclude,metadata --tabs-per-job 5 --dashboards 2 \
  --bulk-after-s 300 --cleanup --out $RUN_DIR/crash-a.json 2>&1 | tee $RUN_DIR/crash-a-run.log
```

**Run B — more pressure: two bulk excludes and two metadata jobs at once** (only if A passed):

```bash
… same, with --levels 4 and --out $RUN_DIR/crash-b.json (new RUN_DIR: "CRASH PATTERN B")
```

**Checks (each run):**

| Check | How | Expected |
|---|---|---|
| Fake-LLM guard passed | first lines of the run log | `LLM_PROVIDER=fake`; no `REFUSING TO RUN` |
| Setup | `ready: bulk-exclude 100,000 URLs …` lines | one per bulk-exclude collection; no `Suggest patterns gave globs …` error |
| The lane's steps | the `ok bulk-exclude …` line | `accept` and `delete glob` times printed; `/tag glob excludes` about 97,000; `excluded after` about 97,000 |
| Jobs | `crash-*.json` → `levels[].failed` | empty |
| `/health` | `alb_risk_s`, `outages_s` | 0, none (it does not use the database) |
| `/health/db` | `health_s.max` | may be slow; record it |
| Requests | `error_count`, `errors` | no 500 and no 504. Some 503 with `Retry-After` is acceptable while the read pool is full: count them |
| Pages and tabs | `page_s`, `tab_s` | record p95 and max; the first views after the glob delete may take seconds (one count, shared); later views use the stored count |
| Task restarts | `cloud.task_restarts` | none |
| Database CPU | `cloud.rds_cpu_pct_max` | record the peak (test reached 95–100 % on 2026-10-06) |
| Pool waits | `aws logs filter-log-events --log-group-name /ecs/sde-curation-engine-dev --start-time <run start ms> --filter-pattern PoolTimeout --query 'length(events)'` | 0 |
| Engine freezes | `… --filter-pattern '"event loop"'` | record the worst; the bulk accept still builds every rule object in memory (audit R1) |
| Cleanup | as in step 3 | run collections deleted, 0 S3 objects left |

---

## Step 6 — put dev back

```bash
make deploy ENV=dev PROFILE=sde-dev        # plain deploy: LLM_PROVIDER=openai, WAF 1000
unset STRESS_PASSWORD
```

| Check | Command | Expected |
|---|---|---|
| Real LLM back | the task definition check of step 2 | `["openai"]` |
| WAF back | `make diff ENV=dev PROFILE=sde-dev` | no differences |
| Engine answers | `curl -s $URL/health` | `{"ok":true,…}` |
| Dev's own collections intact | `dbq "select count(*) from collections"` | 4 (the count before the run) |
| No run leftovers | `aws s3 ls s3://$BUCKET/$PREFIX/ --recursive \| grep -c sde-stress-test`; `dbq "select count(*) from collections where collection_id like '%sde-stress-test%'"` | 0, 0 |
| Nothing running | `dbq "select kind,state from job_runs where state in ('queued','running')"` | `[]` |

Free storage on the database dips by a few GB after the run and stays there until autovacuum
reuses the space. That is normal.

---

## Re-run after the fixes for Run A (c, d, e1, e3, 1a, 2a)

Run A (step 5) failed the responsiveness limits: open tabs re-rendered 100K-collection pages while
their data changed, and the database CPU sat at 100 %. The fixes, built and tested locally on
2026-10-12 (not on dev yet):

| Fix | What it does |
|---|---|
| c, frozen counts | While a job runs, its collection's pages show the counts stored before it, renewed at most once a minute (`FROZEN_COUNTS_MAX_AGE_S`); recounted once when it ends. The duplicate-title and promote-refusal counts are stored counts too. The Curate step says "counts as of HH:MM:SSZ" (2a). |
| d, shared render | While a job runs, one tab-body render is reused for 10 s by every viewer of the same page, filters and role (`X-Tab-Body: shared`). |
| e1, visible tabs only | A hidden tab skips its table refreshes during a job and catches up once when shown. |
| e3, on request (off by default) | `JOB_TABLE_REFRESH=on_request`: no table refresh during a job; a notice and "Show the latest". Not wired into the infra: every environment runs `live`. |
| 1a | Every copy of the running job's progress inside the tab follows the header's live text. |
| Clamp | The "on your own rules" count is never negative. |

### R1 — commit, push and merge to dev

A merge to `dev` deploys **normal mode**: the CI deploy (`.github/workflows/deploy.yml`) runs
`cdk deploy -c environment=dev` with no `stress=true`. So the merge also ends stress mode
(LLM_PROVIDER back to `openai`, WAF back to 1,000). No migration in this change set: no database
reboot.

Before the merge, optional: a manual snapshot as a rollback point for this second merge (dev is at
V19 with today's data; `sde-curation-engine-dev-db-pre-optimize-20261012` holds V10):
```bash
aws rds create-db-snapshot --db-instance-identifier sde-curation-engine-dev-db \
  --db-snapshot-identifier sde-curation-engine-dev-db-pre-refresh-fixes-$(date -u +%Y%m%d)
```

| Check after the CI deploy | Expected |
|---|---|
| GitHub Actions "Deploy" run on the merge commit | green |
| `aws ecs describe-services --cluster $C --services $C --query 'services[0].[runningCount,deployments[0].taskDefinition]'` | 1, a new revision |
| `curl -s $URL/health` | `{"ok":true,…}` |
| `dbq "select max(version) from schema_version"` | `[(19,)]` (unchanged) |

### R2 — redeploy dev in stress mode from the merged commit

The stress deploy builds the image from the local working tree: it must be exactly what `dev` runs.
```bash
git fetch origin && git checkout <the merge commit on origin/dev>
git status --short -- sde_curation infra       # must print nothing
make diff   ENV=dev PROFILE=sde-dev CDK_CONTEXT="-c stress=true"
make deploy ENV=dev PROFILE=sde-dev CDK_CONTEXT="-c stress=true"
```
Same checks as step 2: `make diff` shows only `LLM_PROVIDER` (openai → fake), the image and the WAF
limit, and no change to `Db` or `DbParams`; afterwards the running task has `LLM_PROVIDER=fake`, one
task, `/health` 200, no RDS reboot event.

**From here until R5, nobody pushes to `dev`:** a push redeploys normal mode. The harness stops at
its next level check, but a Suggest metadata job already running would carry on after the restart
with real OpenAI calls.

### R3 — re-run step 5 Run A (the crash pattern)

The same command as step 5 Run A, in a new run folder (`source newrun.sh dev "CRASH PATTERN A
re-run after c, d, e1"`). Pass criteria, compared with the first Run A (dev-20261012T202747Z):

| Measure | First Run A | Pass for the re-run |
|---|---|---|
| Pages p95 | 8.32 s | ≤ 5 s |
| Database CPU | 99.9 % for about 3 minutes | below 80 % |
| 503 "took too long" | 7 | record; ideally 0 (the excluded count after a glob delete is not yet fixed: it can still hit the 30 s limit) |
| Requests with no answer in 90 s | 1 | 0 |
| X-Tab-Body: shared | – | present in the engine's answers while the jobs run (the harness's tabs are always visible, so e1 does not show here) |
| Restarts, PoolTimeout, alb-risk | none | none |

Then Run B (two of each, `--levels 4`) only if the re-run passes.

### R4 — optional: re-run step 4 (full ramp)

To check that the fixes did not slow the ordinary ramp: same command as step 4, compared with
dev-20261012T193310Z.

### R5 — put dev back

Step 6: a plain `make deploy ENV=dev PROFILE=sde-dev` (or the next push to `dev`), with its checks.

### Still open after these fixes (from Run A)

- The excluded count still runs (17–30+ s at 97K exclusions) when a glob delete clears it: keep the
  stored count on a glob delete; the recompute that follows stores the right value.
- A glob add or delete on a 100K collection runs a 48 s recompute inside the request: run it as a
  job above BULK_JOB_MIN_URLS (audit R6).
- One request with no answer in 90 s in Run A: cause not found.
- Suggest metadata saves one UPDATE per page (most of step 4's database load): one statement per
  batch (architecture review §4.2).

## Results

Fill in as the run goes; the details go in each run's `RUNLOG.md`.

| Step | Date / time (UTC) | Result | Notes |
|---|---|---|---|
| 0 Read-only smoke test | 2026-10-12 | passed | rev 43, V19, alarms confirmed |
| 1 Index and cancel (L14) | 2026-10-12 19:15–19:17 | **passed** | aurorasaurus.org: cancel stopped the tagged task (StopTask by the engine role, no error); full run 6/6 validated, 0 deleted; `~/projects/sde-curation-stress/results/dev-20261012T191725Z/` |
| 2 Stress-mode deploy | 2026-10-12 19:23–19:28 | **passed** | rev 44, LLM_PROVIDER=fake (harness guard passes), WAF 20000, no database change or reboot; `/Users/bbenson/projects/sde-curation-stress/results/dev-20261012T192820Z/` |
| 3 Small run | 2026-10-12 19:29–19:32 | **passed** | 2/2 jobs, 0 errors, cleanup complete, no OpenAI in the engine log; harness default mix fixed (see run log); `/Users/bbenson/projects/sde-curation-stress/results/dev-20261012T192851Z/` |
| 4 Full ramp | 2026-10-12 19:33–20:23 | **passed** | 111/111 jobs, 1 laptop-side connection error, no 5xx/503, no restart, pages p95 ≤ 3.9 s, RDS CPU ≤ 73 %, no alarm; better than 2026-10-06 at the heavy levels; open points in the run log; `~/projects/sde-curation-stress/results/dev-20261012T193310Z/` |
| 5 Crash pattern | 2026-10-12 20:28–20:43 (Run A) | **no crash; limits failed** | no restart, no PoolTimeout, /health fine; but pages p95 8.3 s, DB CPU 100 % for ~3 min, 7 × 503 (the excluded count hit the 30 s limit), 1 request > 90 s; glob delete 48 s in-request. Run B not run. `~/projects/sde-curation-stress/results/dev-20261012T202747Z/` |
| 6 Dev put back | | | not yet: dev is in stress mode (rev 44) until R1 or R5 |
| R1–R5 Re-run after the fixes | | | not started |
