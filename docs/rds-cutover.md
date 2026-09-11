# Cutover: SQLite on EFS → RDS PostgreSQL

The engine's state store moved from a SQLite file on EFS to RDS PostgreSQL (`sqlite-vs-rds.md` has
the review that led there). A stack deploy creates the database; an existing environment's data
has to be copied over once. Do this per environment, when nobody is running jobs.

## What changes in the deployment

| | Before | After |
|---|---|---|
| state | `engine.db` on EFS (`DB_LOCKING_MODE=exclusive`) | RDS PostgreSQL 17 (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_SSLMODE`; `DB_USER`/`DB_PASSWORD` from the `/sde-curation-engine/<env>/db` secret) |
| EFS | database + YAML + logs | YAML + logs only (`engine.db` stays on the volume as the rollback copy) |
| backups | none | automated snapshots + point-in-time recovery (7 days; 35 in prod) |
| tasks | one | still one: the job registry is in-process (`infra/README.md`) |

Rollback at any point before step 5: deploy the previous commit. It reads `engine.db` from EFS again,
which nothing has touched. Edits made in the meantime in PostgreSQL are lost, so keep the window short.

## Steps

1. **Deploy the new code.** Push to the environment branch (or `make deploy ENV=…`). The first
   deploy creates the instance (~10 min). The new task starts with an **empty** database: the UI
   shows no collections until step 3. Confirm `/health` returns `"db": "ok"`.

2. **Stop the world.** Nobody should be editing: announce it, and check the Jobs page for
   running jobs. (An idle task is fine; the import runs inside it.)

3. **Import.** Shell into the task and run the importer against the EFS file. It refuses a target
   that already has rows (a collection created by someone during step 1, say); `--replace` wipes
   the target first.
   ```bash
   TASK=$(aws ecs list-tasks --cluster sde-curation-engine-dev --query 'taskArns[0]' --output text --profile sde-dev)
   aws ecs execute-command --cluster sde-curation-engine-dev --task $TASK --container engine \
     --interactive --command "python -m sde_curation.import_sqlite /data/engine.db --replace" --profile sde-dev
   ```
   It prints a row count per table and `imported N rows`. It copies every table with ids intact,
   converts `0/1` flags, ISO timestamps and JSON text to native types, and applies the data fix-ups
   the SQLite version used to run at boot (rule sources from accepted suggestions, last-scraped
   dates, curation stages). A file that was last opened by a build older than the final SQLite
   release is rejected with a message saying so — boot that release once, then import.

4. **Verify.** Reload the UI: the collections, counts, patterns, deltas, job history, users and the
   activity tab are all there. `curl …/health` → `"ok":true`. Spot-check one collection's rules and
   one user's login. Users keep their passwords; sessions were signed by the same secret, so they
   stay valid.

5. **Keep the SQLite file for a week**, then delete `/data/engine.db*` from the EFS volume (via
   the task shell). After that, rolling back means restoring an RDS snapshot, not a file.

## Local equivalent

```bash
make db-up
python -m sde_curation.import_sqlite data/engine.db --dsn postgresql://engine:engine@localhost:5432/engine
make run
```
