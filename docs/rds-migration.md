# State store: SQLite on EFS → RDS PostgreSQL

*Branch `redesigned-curation`, 2026-09-11. Background and the trade-off analysis: `sqlite-vs-rds.md`.
Step-by-step data cutover for an environment that already has data: `rds-cutover.md`.*

## Summary

The engine's database moved from a SQLite file on EFS (one connection, exclusive NFS lock, no
backups) to an RDS PostgreSQL instance per environment. The application's public data-access
surface is unchanged: `Database` keeps the same 77 methods, so `jobs.py`, `curation.py` and the
web layer did not change behaviour. What changed is underneath it, in the infrastructure, and in
how you run the app locally.

| | Before | After |
|---|---|---|
| Engine | SQLite via `aiosqlite`, one connection per process | PostgreSQL 17 via `psycopg` 3 async pool (`DB_POOL_SIZE`, default 8) |
| Where | `engine.db` on the EFS volume | RDS instance per environment (`sde-curation-engine-<env>-db`) |
| Config | `DATA_DIR`, `DB_LOCKING_MODE` | `DATABASE_URL`, or `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` / `DB_SSLMODE` |
| Schema | `CREATE TABLE IF NOT EXISTS` + ad-hoc `ALTER`s on every boot | numbered migrations in `sde_curation/schema.py`, recorded in a `schema_version` table |
| Transactions | implicit (driver-opened) | one explicit transaction per `Database` method; status transitions lock the row |
| Backups | none | automated snapshots + point-in-time recovery (7 days; 35 in prod) |
| Query access | stop the app or copy the file | any Postgres client with VPC access |
| Local dev / tests | nothing to install | PostgreSQL in Docker (`docker-compose.yml`) |
| EFS | database + YAML + logs | YAML + logs only |

Not changed: the service still runs **one task**. The job registry and per-collection locks are
in-process, so deploys still replace the task and still fail in-flight jobs (~90 s of 503).
Multi-task and zero-downtime deploys need those moved into the database first; RDS was a
prerequisite, not the whole job.

## Code changes

- `sde_curation/db.py` — rewritten for psycopg 3. Dialect changes: `%s` placeholders,
  `ON CONFLICT DO NOTHING` for the old `INSERT OR IGNORE`, `RETURNING id` for `lastrowid`,
  `= ANY(%s)` instead of chunked `IN (...)`, `ILIKE` to keep search case-insensitive, native
  `boolean` / `timestamptz` / `jsonb` columns (no more `int()` casts or `json.loads`), a unique
  index on `lower(username)` instead of `COLLATE NOCASE`, and `COPY` for the bulk replaces of
  dump, delta and curated rows. Duplicate users or rules raise `db.ConflictError`; the web layer
  catches that instead of inspecting SQLite's error text.
- `sde_curation/schema.py` — the schema as numbered migrations; applied at connect() under a
  Postgres advisory lock so two starting processes cannot race.
- `sde_curation/import_sqlite.py` — one-off cutover tool (`python -m sde_curation.import_sqlite`):
  copies an `engine.db` table by table with ids intact, converts types, applies the data fix-ups
  the SQLite version used to run at boot, resets the identity sequences. Refuses a populated
  target unless `--replace`.
- `sde_curation/config.py` — `database_url` or the `DB_*` parts (`resolved_database_url` joins
  them); `db_path`, `db_locking_mode` and `resolved_db_path` are gone.
- `sde_curation/web/app.py` — lifespan builds `Database(settings.resolved_database_url, pool_size=…)`;
  `sqlite3` import removed. One template (`tab_activity.html`) formats a datetime it used to slice
  as a string.
- Dependencies: `aiosqlite` → `psycopg[binary,pool]`; dev adds `testcontainers[postgres]`.
  `uv.lock` and the exported `requirements*.txt` are regenerated.

## Tests

- `tests/conftest.py` — session fixture `pg_url`: uses `TEST_DATABASE_URL` when set, otherwise
  starts a throwaway `postgres:17-alpine` with testcontainers (Docker required). The schema is
  created once; every test starts with truncated tables. Existing fixtures are unchanged.
- Three tests that ran raw SQL through the SQLite connection use the new `Database.execute` /
  `fetchval` helpers. Two tests of the SQLite boot-time `ALTER` migration were removed.
- New: `tests/test_import_sqlite.py` (importer: copy, type conversion, backfills, sequences,
  refusal without `--replace`, CLI) and `tests/test_db_pg.py` (transition race with the row lock,
  duplicate counting, large `ANY` lists, case-insensitive usernames and search, raw helpers).
- Result: 171 app tests pass; 12 infra synth assertions pass, including new ones for the RDS
  instance (private, encrypted, gp3, backups, Multi-AZ + deletion protection in prod).
- CI (`test.yml` and the test job of `deploy.yml`) adds a Postgres service container and sets
  `TEST_DATABASE_URL`.

## Infrastructure (`infra/`)

`engine_stack.py` adds, per environment:

- `AWS::RDS::DBInstance` — PostgreSQL 17, in the same public subnets as the task (the default
  VPC has no private ones) but **not publicly accessible**; encrypted gp3 storage, 20 GB
  autoscaling to 100 GB; `postgresql` log exported to CloudWatch; Performance Insights on;
  final snapshot on stack deletion.
- Security group `DbSg` — port 5432 only from the service security group.
- A generated Secrets Manager secret `/sde-curation-engine/<env>/db` (`username` / `password`
  JSON). Nothing to populate by hand.
- Container env `DB_HOST`, `DB_PORT`, `DB_NAME=engine`, `DB_SSLMODE=require`; secrets `DB_USER`
  and `DB_PASSWORD` from that secret's fields. `DB_LOCKING_MODE` removed.
- Outputs `DbEndpoint` and `SecretDb`.

Sizing lives in `config.py` (`EnvConfig`):

| Environment | Instance | Multi-AZ | Backups | Deletion protection | Approx. cost |
|---|---|---|---|---|---|
| dev | `db.m6i.large` (2 vCPU dedicated x86, 8 GiB) | no | 7 days | no | ~$130 / month |
| test | `db.m6i.large` (2 vCPU dedicated x86, 8 GiB) | no | 7 days | no | ~$130 / month |
| prod | `db.m6i.large` (2 vCPU dedicated x86, 8 GiB) | yes | 35 days | yes | ~$255 / month |

No new SSM parameters, so `infra/envs/<env>.json` and `make infra-seed` are untouched.

### Sizing decisions

`sqlite-vs-rds.md` proposed the cheapest sane floor (`db.t4g.micro` dev/test, `db.t4g.medium`
Multi-AZ prod, ~$130/month for the three). On 2026-09-11 that was raised first to `t4g.medium`
(dev) and `t4g.large` (test, prod) for headroom on 100k-URL collections with full page text, and
so that test is sized like prod.

The first two dev deploys that day failed with RDS `insufficient-capacity` for `db.t4g.medium`
in every AZ it tried ("availability zone null"), about 25 minutes in each time, and the orphaned
instance had to be deleted by hand after each rollback. Burstable classes (`t4g`, `t3`) live in
small, shared pools that run dry; the decision was to stop guessing and put every environment on
`db.m6i.large`: dedicated x86 compute, 2 vCPU / 8 GiB, no CPU-credit model, and RDS keeps `m6i`
in much larger pools. About $515/month for the three environments versus ~$130 for the original
floor; the difference buys predictable capacity and no burst accounting.

Alongside, the RDS subnet group was widened from the task's two AZs (a, b) to the five where the
class is orderable (`db_azs` in `config.py`), so RDS has more places to look for capacity.

`infra/config.py` (`EnvConfig.db_instance_class`, `db_multi_az`, `db_backup_days`,
`db_deletion_protection`, per environment in `CONFIGS`) is the single source of truth;
`infra/tests/test_synth.py` pins the classes so a change there is deliberate. Changing a class
takes effect on the next deploy: a short reboot of the instance in dev and test, a failover of
roughly a minute in Multi-AZ prod. The original review keeps its own, older numbers.

## Deploying

Merging to `dev` (or `test` / `prod`) runs the normal Deploy workflow; nothing extra is needed
for the infrastructure. What to expect:

1. The test job runs against its Postgres service container.
2. `cdk deploy` creates the instance, security group and secret, then rolls out the new task
   definition. Budget an extra 10–15 minutes on the first deploy for instance creation. The old
   task keeps serving until the new one replaces it (then the usual ~90 s of 503).
3. The new task connects, creates the schema, and answers `/health` with `"db": "ok"`. The
   verify job passes as before.
4. The database starts **empty**. If the environment has no data worth keeping (dev today), you
   are done: the bootstrap `admin` account is re-created from the `app_password` secret on first
   start. If it does have data, run the importer from a task shell (`rds-cutover.md`); it reads
   the old `engine.db` still on EFS.

Rollback before any data has been created in Postgres: revert the merge and redeploy. The old
image reads `engine.db` from EFS again; the new code never wrote to that file.

The CDK changes pass synth assertions but have not yet been deployed against a real account.
`make diff ENV=dev PROFILE=sde-dev` previews the CloudFormation changes without applying them.

## Local development

```bash
make db-up        # PostgreSQL 17 in Docker on localhost:5432 (engine / engine / engine)
make run          # uvicorn --reload on :8080 (depends on db-up)
make test         # testcontainers starts its own throwaway PostgreSQL
make test-local   # the suite against the compose database (what CI does)
make db-shell     # psql
make db-down      # stop; data stays in the `pgdata` volume (`docker compose down -v` wipes it)
```

`.env` needs `DATABASE_URL=postgresql://engine:engine@localhost:5432/engine` (`.env.example`
has it). To bring over a local `data/engine.db`:

```bash
python -m sde_curation.import_sqlite data/engine.db --dsn postgresql://engine:engine@localhost:5432/engine
```

Docker Desktop must be running for the app and for the tests; there is no SQLite fallback.

## Seeing the database in AWS

**Console.** RDS → Databases → `sde-curation-engine-<env>-db`. The page shows status, the
CPU / connections / storage graphs, Performance Insights (slowest queries), the PostgreSQL log
under *Logs & events*, and every snapshot under *Maintenance & backups*. It cannot show tables
or rows: RDS for PostgreSQL has no built-in query editor (that exists only for Aurora).

**Reading the tables.** The instance is not publicly accessible and only the service security
group may reach port 5432, so a client has to be inside the VPC. From least to most setup:

1. *From the running task* — nothing to add. Shell in and use the app's own driver:
   ```bash
   TASK=$(aws ecs list-tasks --cluster sde-curation-engine-dev --query 'taskArns[0]' --output text --profile sde-dev)
   aws ecs execute-command --cluster sde-curation-engine-dev --task $TASK --container engine \
     --interactive --command bash --profile sde-dev
   python -c "import os, psycopg; c = psycopg.connect(host=os.environ['DB_HOST'], user=os.environ['DB_USER'], \
     password=os.environ['DB_PASSWORD'], dbname='engine'); print(c.execute('SELECT collection_id, status, dump_count FROM collections').fetchall())"
   ```
   Fine for a quick look; clumsy for exploring.
2. *A tunnel to your laptop* — `psql`, DBeaver, pgAdmin or a notebook against `localhost:5432`.
   Needs a small EC2 bastion in the VPC with the SSM agent and an ingress rule on `DbSg` for it,
   then `aws ssm start-session --document-name AWS-StartPortForwardingSessionToRemoteHost`
   forwards the port. This is the standard pattern and the right one for the calibration
   analysis work (not built yet; a CDK addition plus a `make db-tunnel` target).
3. *Temporarily open it* — `publicly_accessible=True` plus an ingress rule for your IP. Fastest;
   do not leave it that way.

Credentials for any of these come from the generated secret:
```bash
aws secretsmanager get-secret-value --secret-id /sde-curation-engine/dev/db --query SecretString --output text --profile sde-dev
```

## Backups

Two mechanisms run automatically, with no configuration beyond the stack:

| Mechanism | Frequency | What it gives you |
|---|---|---|
| Automated daily snapshot | once a day, in a 30-minute window AWS picks unless one is set | restore the whole instance to that point |
| Transaction log archival | continuous; logs uploaded every 5 minutes | point-in-time recovery to any second within retention, typically up to ~5 minutes ago |

Retention: 7 days in dev and test, 35 days in prod (`db_backup_days` in `config.py`). Manual
snapshots can be taken any time and are kept until deleted, and the stack takes a final snapshot
if the instance is ever deleted (`RemovalPolicy.SNAPSHOT`). A restore always creates a **new**
instance from the snapshot or point in time; you then point the app at it (swap `DB_HOST`, or
restore under the stack's identifier after removing the old one) rather than overwriting the
live one.

## Operating notes

- Migrations: add `(2, "ALTER TABLE …")` to `MIGRATIONS` in `schema.py`; each version runs once
  per database and is recorded in `schema_version`.
- Still single-task: keep `desired_count=1` until the job registry moves into the database.
