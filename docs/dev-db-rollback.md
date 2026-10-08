# Rolling the dev database back after an unwanted merge

Written 2026-10-08. Scope: the **dev** environment (`CurationEngine-dev`, profile `sde-dev`).
The same steps work for test with profile `smce-test`, but test is where curators work: do not
roll test back without agreeing on it first.

## The problem

A merge to `dev` deploys new code. On boot, the engine applies every migration above
`MAX(version)` in `schema_version` (`sde_curation/schema.py`, `migrate_sync` / `migrate_async`).
Suppose the merge brings V11–V17 and test is still on V10. You want dev back to where dev and test
match, before more work goes on top.

Facts that decide the options:

- **Migrations only go forward.** `schema.py` has no down steps. Nothing in the engine can take a
  database from V17 back to V10.
- **The engine compares only the highest number.** It checks `MAX(version)`, not names. If a database
  has already run a V11, a *different* V11 written later is silently skipped there.
- **V11–V17 add things and remove nothing.** V11 adds a column, V12 resets values, V13 adds an
  extension, V14 sets autovacuum, V15 adds a column, V16 adds indexes, V17 adds a table. So V10
  code very probably still runs on a V17 database. I have not tested this.
- **The dev RDS has automated backups with point-in-time recovery for 7 days.** The settings are
  `db_backup_days = 7` (`infra/config.py`), instance `sde-curation-engine-dev-db`, PostgreSQL 17,
  `db.m6i.large`, single-AZ, gp3, encrypted with the default `aws/rds` key, not public.
- **CDK owns the instance under a fixed name.** It sets `instance_identifier=f"{cfg.name}-db"` and
  gives the task `DB_HOST` from the instance endpoint (`infra/stacks/engine_stack.py`). Because the
  endpoint hostname comes from the instance name, a new instance **with the same name** gets the
  same `DB_HOST`. The DB password is the one stored when the snapshot or restore point was taken.
  Nothing rotates it.

State on 2026-10-08:
- `origin/dev` and `origin/test` both define V1–V10.
- V11–V17 exist only on `featuure/optimize-app`. Its HEAD (`7324e2a`) also has a V18, which adds
  `canonical_key` columns and indexes. This doc's examples say "V11–V17". In practice, include
  every migration the merge brings.
- From an earlier note, the dev **database** already ran V11 during the 2026-10-06 stress deploy,
  and then went back to V10 code. Check with the query in [Step 1](#step-1--find-the-restore-time)
  before you plan anything.

## Recommendation

| If… | Do this |
|---|---|
| You only need dev to *behave* like test, and dev's data does not matter much | **Option A: revert the code, keep the schema.** Cheapest. This is what COSMOS does. |
| You want dev's schema to match test's again, keep dev's data, and the unwanted migrations only *added* things | **Option E: compensating migration (roll forward).** Recommended for additive migrations. No AWS work. |
| You need dev's database to *be* V10 again, so later work can reuse numbers 11+ | **Option B: point-in-time restore**, using the rename swap below. |
| You took a manual snapshot before the merge | **Option C: restore that snapshot**, with the same rename swap. Simplest restore. |
| You want dev's *data* to equal test's | **Option D: copy test into dev.** Most work: cross-account and encrypted. |

Whichever option you choose, follow these two rules:

1. **Never reuse a version number that a shared database has applied.** Before you number a new
   migration, run `SELECT max(version) FROM schema_version` on dev and on test. Only restoring the
   database (B, C, D) frees numbers again.
2. **Take a manual snapshot of dev before every deploy that brings migrations.** It makes Option C
   available every time. Manual snapshots do not expire with the retention period
   ([AWS: Creating a DB snapshot](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateSnapshot.html)).

---

## Option A — revert the code, keep the schema

1. Revert the merge on `dev`: `git revert -m 1 <merge-sha>`, then push. The Deploy workflow
   (`.github/workflows/deploy.yml`) deploys the V10 code.
2. Leave V11–V17 in the database. V10 code ignores the columns, table and indexes it does not know.
3. The next new migration is **V18**, on every branch and in every environment.
4. **Watch for stale values.** V10 code does not update `excluded_count`, `collection_stats.counts`
   or `review_round`. When the reworked code comes back, add a migration that clears those values,
   as V12 does for `excluded_count`.
5. **Git trap.** After `git revert -m 1`, Git treats the reverted commits as merged. A later merge
   of the same branch brings nothing back. You must either revert the revert, or rebuild the work as
   new commits.

Risk: low. Cost: minutes. It does not get you a V10 database.

---

## Option B — point-in-time restore (PITR), with a rename swap

### Why the rename swap

PITR always creates a **new** instance and leaves the source alone
([AWS: Restoring a DB instance to a specified time](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PIT.html)).
You must then make the stack use it. AWS documents this exact case: rename the old instance, then
give the restored instance the old name, so no application config changes
([AWS: Renaming to replace an existing DB instance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RenameInstance.html#USER_RenameInstance.RR)).

This also suits CloudFormation:
- The stack knows the instance by its identifier.
- With a custom name, CloudFormation "can't perform updates that require replacement"
  ([CFN: AWS::RDS::DBInstance](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-rds-dbinstance.html)).
  A swap done through CDK would hit exactly that.
- The rename swap keeps the name, the endpoint, the secret and the template unchanged.

Facts from the AWS docs that shape the steps:

- A restored instance gets the **default** parameter group unless you pass one. Ours sets
  `shared_preload_libraries = pg_stat_statements`, so you must pass it.
- Without `--vpc-security-group-ids`, it gets the VPC's **default** security group. The engine then
  cannot connect. You must pass `DbSg`.
- `restore-db-instance-to-point-in-time` has no Performance Insights option. Turn it on afterward
  with `modify-db-instance`.
  ([CLI: restore-db-instance-to-point-in-time](https://docs.aws.amazon.com/cli/latest/reference/rds/restore-db-instance-to-point-in-time.html))
- Tags are copied from the source when you pass none. That includes the CloudFormation tags.
- Renaming changes the endpoint. The old DNS name goes away at once. The new one works in about
  10 minutes. **The instance reboots on rename.**
- RDS ships transaction logs to S3 every 5 minutes. You can restore to any second between the
  earliest restorable time and `LatestRestorableTime`.

### What the restore does *not* roll back

Only PostgreSQL goes back in time. These keep their current state:
- **EFS** (`DATA_DIR`): collection YAML, index logs, scrape job folders.
- The dev **OpenSearch Serverless** index.
- **S3** exports and vectors.

Expect some mismatch, for example index runs or scrape jobs that the restored database does not
know. For dev this is usually acceptable. Every curation decision made in dev after the restore time
is lost.

### Step 0 — prerequisites

```bash
export AWS_PROFILE=sde-dev AWS_REGION=us-east-1
DB=sde-curation-engine-dev-db
C=sde-curation-engine-dev            # ECS cluster and service share this name
aws sts get-caller-identity          # check that you are in the SMCE Dev account
```

- You need the `session-manager-plugin` for ECS Exec
  ([AWS: ECS Exec](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-exec.html)).
- Tell the team: **no pushes to `dev`** until the swap is done. A CDK deploy in the middle of a
  rename can fail or point at the wrong instance.

### Step 1 — find the restore time

Read when each migration ran. The engine image has no `psql`, so use Python inside the task:

```bash
T=$(aws ecs list-tasks --cluster $C --service-name $C --query 'taskArns[0]' --output text)
aws ecs execute-command --cluster $C --task "$T" --container engine --interactive --command \
"python -c \"import os,psycopg;c=psycopg.connect(host=os.environ['DB_HOST'],dbname=os.environ['DB_NAME'],user=os.environ['DB_USER'],password=os.environ['DB_PASSWORD'],sslmode='require');[print(r) for r in c.execute('select version,applied_at from schema_version order by version').fetchall()]\""
```

- Choose a restore time a few minutes **before** the `applied_at` of the first unwanted version, for
  example V11.
- Times are UTC. Pass them with a `Z`, for example `2026-10-08T14:05:00Z`.
- Check that the time is inside the window:

```bash
aws rds describe-db-instances --db-instance-identifier $DB \
  --query 'DBInstances[0].[LatestRestorableTime,BackupRetentionPeriod]'
aws rds describe-db-instance-automated-backups --db-instance-identifier $DB \
  --query 'DBInstanceAutomatedBackups[0].RestoreWindow'
```

If the merge happened more than 7 days ago, PITR cannot reach it. Use Option C or D.

### Step 2 — deploy the V10 code first

Revert the merge on `dev` and let the Deploy workflow finish, as in Option A, step 1.

**Why first:** the task that starts on the restored database must run V10 code. If V17 code starts
on a V10 database, it migrates straight back to V17. While this deploy runs, V10 code runs against
the V17 database. That is harmless because the migrations only add things.

### Step 3 — safety snapshot of the current database

```bash
SNAP=$DB-before-rollback-$(date -u +%Y%m%d-%H%M)
aws rds create-db-snapshot --db-instance-identifier $DB --db-snapshot-identifier $SNAP
aws rds wait db-snapshot-available --db-snapshot-identifier $SNAP
```

([CLI: create-db-snapshot](https://docs.aws.amazon.com/cli/latest/reference/rds/create-db-snapshot.html))
This is your way back if the rollback itself goes wrong.

### Step 4 — stop the engine

```bash
aws ecs update-service --cluster $C --service $C --desired-count 0
aws ecs wait services-stable --cluster $C --services $C
```

([CLI: ecs update-service](https://docs.aws.amazon.com/cli/latest/reference/ecs/update-service.html))
- In-flight jobs are marked failed on the next start (`infra/README.md`, Operating notes).
- Do this when nobody is working on dev.

### Step 5 — read the settings the restore must copy

```bash
read SUBNETS PG <<<"$(aws rds describe-db-instances --db-instance-identifier $DB \
  --query 'DBInstances[0].[DBSubnetGroup.DBSubnetGroupName,DBParameterGroups[0].DBParameterGroupName]' --output text)"
SG=$(aws rds describe-db-instances --db-instance-identifier $DB \
  --query 'DBInstances[0].VpcSecurityGroups[].VpcSecurityGroupId' --output text)
echo "$SUBNETS | $PG | $SG"
```

### Step 6 — restore to a temporary name

```bash
aws rds restore-db-instance-to-point-in-time \
  --source-db-instance-identifier $DB \
  --target-db-instance-identifier $DB-restored \
  --restore-time 2026-10-08T14:05:00Z \
  --db-instance-class db.m6i.large \
  --db-subnet-group-name "$SUBNETS" \
  --vpc-security-group-ids $SG \
  --db-parameter-group-name "$PG" \
  --no-multi-az --no-publicly-accessible \
  --storage-type gp3 --max-allocated-storage 100 \
  --backup-retention-period 7 \
  --no-deletion-protection \
  --copy-tags-to-snapshot \
  --enable-cloudwatch-logs-exports postgresql
aws rds wait db-instance-available --db-instance-identifier $DB-restored
```

- Restoring to a temporary name first leaves the source untouched. You can still stop here.
- The values above match `infra/config.py` for dev on 2026-10-08. For test or prod, take them from
  that environment's config.
- Storage keeps loading from S3 in the background after the instance is `available`. It works, but
  it is slower until loading finishes.
- After the instance is available, compare the restored instance with `$DB` field by field:

```bash
for i in $DB $DB-restored; do aws rds describe-db-instances --db-instance-identifier $i --query \
 'DBInstances[0].[DBInstanceClass,StorageType,MaxAllocatedStorage,MultiAZ,BackupRetentionPeriod,DBParameterGroups[0].DBParameterGroupName,VpcSecurityGroups[0].VpcSecurityGroupId,EnabledCloudwatchLogsExports,PerformanceInsightsEnabled,AutoMinorVersionUpgrade,StorageEncrypted,KmsKeyId]' --output text; done
```

### Step 7 — swap the names

```bash
aws rds modify-db-instance --db-instance-identifier $DB \
  --new-db-instance-identifier $DB-pre-rollback --apply-immediately
aws rds wait db-instance-available --db-instance-identifier $DB-pre-rollback

aws rds modify-db-instance --db-instance-identifier $DB-restored \
  --new-db-instance-identifier $DB --apply-immediately
aws rds wait db-instance-available --db-instance-identifier $DB
```

([CLI: modify-db-instance](https://docs.aws.amazon.com/cli/latest/reference/rds/modify-db-instance.html))
- Each rename reboots the instance.
- Give DNS about 10 minutes before you start the engine.

### Step 8 — finish the settings the restore could not set

```bash
aws rds modify-db-instance --db-instance-identifier $DB \
  --enable-performance-insights --apply-immediately
```

Also fix anything else that the comparison in Step 6 showed as different
([AWS: Turning Performance Insights on](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PerfInsights.Enabling.html)).

### Step 9 — start the engine and verify

```bash
aws ecs update-service --cluster $C --service $C --desired-count 1
aws ecs wait services-stable --cluster $C --services $C
```

1. Run the Step 1 query again. `max(version)` must be **10**, with no rows after the restore time.
2. Open the dev CloudFront URL. Log in. Open a collection.
3. Run drift detection. It confirms that the stack still matches the live instance:
   ```bash
   aws cloudformation detect-stack-drift --stack-name CurationEngine-dev
   # then: aws cloudformation describe-stack-resource-drifts --stack-name CurationEngine-dev
   ```
   ([AWS: Detect drift on a stack](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/detect-drift-stack.html))
   A difference on the `Db` resource means a setting from Step 6 or 8 is still wrong.

### Step 10 — clean up, after a few days

- `$DB-pre-rollback` still runs and costs a second `db.m6i.large`. It also still carries the
  CloudFormation tags, but the stack no longer manages it.
- When you no longer need it, delete it with a final snapshot:

```bash
aws rds delete-db-instance --db-instance-identifier $DB-pre-rollback \
  --final-db-snapshot-identifier $DB-pre-rollback-final
```

([CLI: delete-db-instance](https://docs.aws.amazon.com/cli/latest/reference/rds/delete-db-instance.html))

### If something goes wrong

- **Before Step 7:** delete `$DB-restored` and set the service back to 1. Nothing changed.
- **After Step 7:** swap back. Rename `$DB` → `$DB-failed`, then `$DB-pre-rollback` → `$DB`. Or
  restore the Step 3 snapshot with Option C.

---

## Option C — restore a manual snapshot

This is the same procedure as Option B. Only Step 6 changes:

```bash
aws rds restore-db-instance-from-db-snapshot \
  --db-snapshot-identifier <snapshot-taken-before-the-merge> \
  --db-instance-identifier $DB-restored \
  --db-instance-class db.m6i.large \
  --db-subnet-group-name "$SUBNETS" --vpc-security-group-ids $SG \
  --db-parameter-group-name "$PG" \
  --no-multi-az --no-publicly-accessible \
  --storage-type gp3 --max-allocated-storage 100 \
  --backup-retention-period 7 --no-deletion-protection --copy-tags-to-snapshot \
  --enable-cloudwatch-logs-exports postgresql
```

References:
- [AWS: Restoring to a DB instance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RestoreFromSnapshot.html)
- [CLI: restore-db-instance-from-db-snapshot](https://docs.aws.amazon.com/cli/latest/reference/rds/restore-db-instance-from-db-snapshot.html)

I took the flags above from the PITR command. I have not checked each one against this command's
reference page. Check them before you run it.

**The habit that makes this cheap:** before you merge a branch that adds migrations to `dev`, run:

```bash
aws rds create-db-snapshot --db-instance-identifier sde-curation-engine-dev-db \
  --db-snapshot-identifier sde-curation-engine-dev-db-pre-v<N>-$(date -u +%Y%m%d)
```

Manual snapshots are kept until you delete them. You pay for their storage.

### Not recommended: restoring through CDK / CloudFormation

CloudFormation can restore from a snapshot through the `DBSnapshotIdentifier` property, which in CDK
is `rds.DatabaseInstanceFromSnapshot`
([CDK: DatabaseInstanceFromSnapshot](https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_rds/DatabaseInstanceFromSnapshot.html)).
Do not use it here:

- Switching construct, or changing `DBSnapshotIdentifier`, **replaces** the instance.
- With our fixed `instance_identifier`, CloudFormation cannot replace it under the same name.
- The construct also changes how the credentials secret is created.

The rename swap avoids all of this.

---

## Option D — copy test's data into dev

Use this only when dev must hold the same *data* as test. It has two hard parts:

1. **Cross-account plus the default KMS key.** Test (SMCE test) and dev (SMCE Dev) are different
   accounts. Both instances use the default `aws/rds` key. "You can't share a snapshot that has been
   encrypted using the default KMS key". The AWS workaround:
   1. Create a customer managed key in test and grant dev access to it.
   2. Copy the snapshot in test with that key.
   3. Share the copy with dev.
   4. Copy it again in dev with a dev key.
   5. Restore it as in Option C.

   ([AWS: Sharing encrypted snapshots](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/share-encrypted-snapshot.html),
   [AWS: Sharing a DB snapshot](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ShareSnapshot.html),
   [AWS: Copying a DB snapshot](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CopySnapshot.html))
2. **The data then refers to test's world.** Collections, index runs and job IDs point at test's
   EFS, test's OpenSearch Serverless collection and test's crawler state. Dev's own EFS and index do
   not match.

The alternative is `pg_dump` / `pg_restore`. The engine image has no `pg_dump` and both databases
are private, so you would need a one-off ECS task with a `postgres:17` image in each VPC, plus S3 in
between. That is more work than the snapshot path.

---

## Option E — compensating migration (roll forward)

Undo the unwanted migrations with a **new** migration that has the next free number. The schema
returns to the V10 shape. `schema_version` keeps counting up. This is the usual rollback for
forward-only migration systems, and the engine already did it once: V12 cleared the counts that V11
had left stale.

Use it when the unwanted migrations only **added** things: tables, columns, indexes, settings. It
cannot bring back data that a migration overwrote or deleted. For that, use Option B or C.

Compared with a restore:
- dev keeps all its data, including curation done after the merge.
- There is no AWS work, no downtime beyond a normal deploy, and no CDK risk.
- It works the same in every environment, because the migration runs on boot like any other.

### Step 1 — keep the old numbers as no-ops

In `sde_curation/schema.py`, **keep** V11–V17 (and V18, if present) in `MIGRATIONS`, but replace
their SQL with a no-op and a comment. For example:

```python
# V11–V18 were reverted on 2026-10-xx (see V19). dev ran them; test never did. They stay in the
# list as no-ops so the numbers are never reused and a fresh database skips straight to V19.
V11 = V12 = V13 = V14 = V15 = V16 = V17 = V18 = "SELECT 1;"
```

Why keep them:
- **dev** already ran the real V11–V18, so their bodies no longer matter there.
- **test, local databases and CI** are at V10. They step through 11–18 doing nothing, then run V19.
- The numbers stay visibly taken, so nobody writes a new "V11".

Deleting them from the list also works with the `MAX(version)` check. But then the reason for the
gap is lost, and the "never reuse" rule depends on memory.

### Step 2 — write the compensating migration

Every statement needs `IF EXISTS`. dev has the objects. test and fresh databases never had them, so
the same SQL must be a no-op there. Undo in reverse order: indexes, then columns and tables, then
settings.

Example for the V11–V18 on `featuure/optimize-app`:

```sql
-- V19: undo V11–V18 (reverted). Safe where they never ran: every statement is IF EXISTS.
DROP INDEX IF EXISTS delta_urls_kind, delta_urls_renamed_from, patterns_coll_id,
  pattern_effects_coll_field, dump_urls_key, curated_urls_key, patterns_key;      -- V16, V18
DROP TABLE IF EXISTS collection_stats;                                             -- V17
ALTER TABLE collections DROP COLUMN IF EXISTS excluded_count,                      -- V11 (V12 only changed its values)
                        DROP COLUMN IF EXISTS review_round;                        -- V15
ALTER TABLE dump_urls    DROP COLUMN IF EXISTS canonical_key;                      -- V18
ALTER TABLE curated_urls DROP COLUMN IF EXISTS canonical_key;
ALTER TABLE patterns     DROP COLUMN IF EXISTS canonical_key;
ALTER TABLE delta_urls      RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor);  -- V14
ALTER TABLE pattern_effects RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor);
ALTER TABLE patterns        RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor);
```

Checked on 2026-10-08 against local Postgres 17, with the real V1–V18 SQL from `schema.py`:
- **dev path** (V1–V18, then this SQL): runs without errors. Its `pg_dump --schema-only` equals a
  plain V10 database, except for the `pg_stat_statements` extension.
- **test path** (V1–V10, then this SQL): every statement skips with a NOTICE. Its schema equals V10
  exactly.

Notes:
- **V13 (`pg_stat_statements`):** I would keep the extension. It is not part of the app schema, and
  the parameter group loads the library anyway. For an exact match with test, add
  `DROP EXTENSION IF EXISTS pg_stat_statements;`.
- **V12** changed values only. There is nothing to undo once V11's column is gone.
- **`RESET` on a setting that was never set is a no-op.** So test is safe.
- **Dropping a column also drops the indexes on it.** The explicit `DROP INDEX` for the
  `canonical_key` indexes is redundant, but harmless and clearer.
- **The engine runs all pending migrations in one transaction** (`migrate_sync` / `migrate_async`),
  and Postgres DDL is transactional. A failure leaves the database unchanged, and the task fails
  to boot. You do not get a half-undone database.
- **Data that is lost:**
  - `excluded_count` and `collection_stats.counts` are caches. Losing them costs nothing.
  - `canonical_key` can be computed again.
  - **`review_round` is real state.** It records an open "Re-curate everything" round. Dropping it
    loses which collections on dev had a round open.

### Step 3 — revert the code, in the same commit

The application code must go back to its V10 behaviour, and the migration list must change, in
**one** commit:
- `git revert -m 1 <merge-sha>` brings back the V10 code, but it also removes V11–V18 from
  `schema.py`. Before you commit the revert, put V11–V18 back as no-ops (Step 1) and add V19
  (Step 2).
- Other code that names the dropped objects must also go back. One example is
  `TABLES` in `schema.py`, which lists `collection_stats`. The revert handles this, but check it.

### Step 4 — verify before deploying

You can check locally, with no AWS access. The `make db-up` Postgres has `pg_dump`.

1. **Database A, the target:** a fresh database migrated by the old V10 code
   (`git checkout <commit-before-merge>`).
2. **Database B, the dev path:** a fresh database migrated by the merged code (real V11–V18), then
   by the new code (V19).
3. **Database C, the test path:** a fresh database migrated by the new code only (no-op 11–18,
   then V19).
4. Run `pg_dump --schema-only` on all three. Remove the `schema_version` data and diff the dumps.
   A, B and C must be the same.

Also run the test suite. It builds fresh databases, so it exercises path C.

### Step 5 — compare dev and test after the deploy

This read-only query runs through ECS Exec, the same way as in Option B, Step 1. Run it on dev
(`sde-dev`) and on test (`smce-test`). It prints one hash each for columns, indexes, table settings
and extensions. Matching hashes mean matching schemas.

```bash
aws ecs execute-command --cluster $C --task "$T" --container engine --interactive --command \
"python -c \"import os,psycopg,hashlib;c=psycopg.connect(host=os.environ['DB_HOST'],dbname=os.environ['DB_NAME'],user=os.environ['DB_USER'],password=os.environ['DB_PASSWORD'],sslmode='require');q=['select table_name,column_name,data_type,is_nullable,column_default from information_schema.columns where table_schema=current_schema()','select indexdef from pg_indexes where schemaname=current_schema()','select relname,reloptions from pg_class where relnamespace=current_schema()::regnamespace and relkind::text=chr(114)','select extname from pg_extension'];[print(hashlib.md5(repr(sorted(repr(r) for r in c.execute(s).fetchall())).encode()).hexdigest()) for s in q]\""
```

- Expect the fourth hash (extensions) to differ if you kept `pg_stat_statements` on dev.
- `schema_version` itself shows up in the column and index hashes. It is the same table in both
  environments, so it does not cause a difference.
- I have not run this query. Check its output on one environment before you trust a comparison.

### Afterward

- **dev and test both end at version 19**, with the V10 schema shape. The number is only a
  counter. It does not need to equal 10.
- **When the reworked changes return, they get V20 and later**, with plain `CREATE` / `ADD`
  statements. Never reuse 11–19.
- **Git trap:** as in Option A, bringing the original commits back means reverting the revert, or
  rebuilding them as new commits. Either way, the migrations they carry must be renumbered to V20
  and later.

---

## Not recommended: hand-written "down" SQL

You could run SQL by hand that drops V11–V17 and deletes their `schema_version` rows. All seven
migrations only add things, so this is possible. But it is run outside the deploy, it is untested,
and it is easy to get partly wrong, for example the V13 extension or the V14 table settings. Option E
does the same schema work as a normal, reviewed, tested migration, and it keeps the version history
honest. Use Option E.

---

## Context from COSMOS (`~/projects/COSMOS`, `dev` branch, checked 2026-10-08)

COSMOS uses Django migrations. Its practice:

- **Django can reverse migrations** (`manage.py migrate <app> <previous>`), but only when every
  step has a reverse. A `RunPython` without `reverse_code` raises `IrreversibleError`
  ([Django: Reversing migrations](https://docs.djangoproject.com/en/5.2/topics/migrations/#reversing-migrations)).
  - COSMOS has two such migrations: `sde_collections/0072` and `environmental_justice/0005`.
  - `0075` has a proper reverse function. `environmental_justice/0006` uses `RunPython.noop`.
  - I found no sign that COSMOS ever reversed a migration. I searched commit messages and docs only.
- **Prod migrations run by hand.**
  - `compose/production/django/start` runs only `collectstatic` and gunicorn.
  - `compose/local/django/start` runs `migrate` on every start.
- **CI does not check migrations.** `run_full_test_suite.yml` runs tests on PRs into `dev`. It has no
  `makemigrations --check`, and nothing runs on `staging` or `production`.
- **Parallel branches collide on numbers.** Django merge migrations resolve them: 5 merge files, and
  8 duplicate numbers in `sde_collections` (0037, 0045, 0046, 0059, 0060, 0066, 0067, 0068). Django
  tracks migrations by **name**, so it detects these collisions. Our engine tracks one integer, so it
  cannot.
- **Unreleased migrations were rewritten.** Examples: `df88c6b1` squashed and deleted
  `0060_delete_dumpurl` and `0061_dumpurl`. `45e757da`, `0a2e63af`, `80a4bc76` and `4c6a0c49`
  removed or consolidated files. This is safe only while no shared database has run them. I found no
  record of how dev databases were cleaned up afterward. In our engine, the same move causes the
  silent-skip problem described in [The problem](#the-problem).
- **The reverts on `dev` touched code, never migrations** (`d4f2c4ad`, `690b76d4`).
- **Recovery means backup and restore, by hand.**
  - `manage.py database_backup` / `database_restore` (`pg_dump` to `/backups`).
  - The cookiecutter `postgres backup|restore` maintenance scripts
    ([cookiecutter-django: PostgreSQL backups with Docker](https://cookiecutter-django.readthedocs.io/en/latest/4-guides/docker-postgres-backups.html)).
  - `SQLDumpRestoration.md`.
  - Nothing takes a backup before `migrate` automatically.
- On 2026-10-08, `production` and `dev` hold the same migration files. `production` is 5 merge
  commits ahead.

**What to take from COSMOS:**
1. Rolling back code while keeping additive migrations is a normal, working practice. That is
   Option A.
2. Rewrite migrations only before any shared database has run them.
3. Do not copy the gap of migrating with no backup first. Our engine migrates on boot,
   automatically, so a snapshot before a migration deploy (Option C's habit) matters more here.

---

## AWS reference index

| Topic | Link |
|---|---|
| PITR (concepts, restore window, default parameter group) | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PIT.html |
| Automated backups and retention | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.html |
| Rename an instance / replace by renaming | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RenameInstance.html |
| Create a manual snapshot | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateSnapshot.html |
| Restore from a snapshot | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RestoreFromSnapshot.html |
| Copy a snapshot | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CopySnapshot.html |
| Share a snapshot | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ShareSnapshot.html |
| Share an encrypted snapshot (default-key workaround) | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/share-encrypted-snapshot.html |
| Performance Insights on | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PerfInsights.Enabling.html |
| Secrets Manager with RDS | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-secrets-manager.html |
| CLI `restore-db-instance-to-point-in-time` | https://docs.aws.amazon.com/cli/latest/reference/rds/restore-db-instance-to-point-in-time.html |
| CLI `restore-db-instance-from-db-snapshot` | https://docs.aws.amazon.com/cli/latest/reference/rds/restore-db-instance-from-db-snapshot.html |
| CLI `create-db-snapshot` | https://docs.aws.amazon.com/cli/latest/reference/rds/create-db-snapshot.html |
| CLI `copy-db-snapshot` | https://docs.aws.amazon.com/cli/latest/reference/rds/copy-db-snapshot.html |
| CLI `modify-db-instance` | https://docs.aws.amazon.com/cli/latest/reference/rds/modify-db-instance.html |
| CLI `describe-db-instances` | https://docs.aws.amazon.com/cli/latest/reference/rds/describe-db-instances.html |
| CLI `delete-db-instance` | https://docs.aws.amazon.com/cli/latest/reference/rds/delete-db-instance.html |
| CLI `wait db-instance-available` | https://docs.aws.amazon.com/cli/latest/reference/rds/wait/db-instance-available.html |
| CloudFormation `AWS::RDS::DBInstance` (replacement, `DBSnapshotIdentifier`) | https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-rds-dbinstance.html |
| CloudFormation drift | https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-stack-drift.html |
| CDK `DatabaseInstanceFromSnapshot` | https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_rds/DatabaseInstanceFromSnapshot.html |
| ECS `update-service` | https://docs.aws.amazon.com/cli/latest/reference/ecs/update-service.html |
| ECS Exec | https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-exec.html |

## What I have not verified

- I have not run this procedure against dev. The commands follow the AWS references above and the
  stack code. Run it once on a quiet day before you need it in a hurry.
- I have not tested V10 code against a V17 database. The claim that it works comes from reading the
  migrations, which only add things.
- Durations ("about 10 minutes" for DNS) come from AWS docs. I have not measured restore time for our
  database size.
- `--auto-minor-version-upgrade` is not in the restore command. The comparison in Step 6 shows
  whether it needs a `modify-db-instance` afterward.
