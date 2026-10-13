# infra — AWS CDK for sde-curation-engine

One stack per environment, `CurationEngine-<env>`: ECS Fargate (1 task) + RDS PostgreSQL (state) +
EFS (per-collection YAML, logs) → ALB (HTTP, CloudFront-only) → CloudFront (HTTPS + WAF). The task role can drive the crawler EC2 box over SSM,
dispatch the WEB_COSMOS indexer with `ecs:RunTask`, and read the OpenSearch Serverless web index
for validation.

**Nothing account-specific is in git.** Instance ids, bucket names, role ARNs and AOSS endpoints
live in SSM Parameter Store of each account under `/sde-curation-engine/<env>/<key>` and are
resolved by CloudFormation at deploy time. The target account is whatever the active AWS profile
resolves to. `config.py` holds only the parameter names and the per-env sizes/knobs.

```
infra/
  app.py                 cdk app: -c environment=dev|test|prod (default dev)
  config.py              PARAMS (the SSM keys) + EnvConfig per environment (sizes, AZs, knobs)
  seed.py                writes envs/<env>.json into SSM  (make infra-seed ENV=…)
  envs/example.json      shape of the per-env value file; envs/<env>.json itself is gitignored
  stacks/engine_stack.py the stack
  bootstrap/             separate one-time CDK app: the GitHub Actions deploy role (make bootstrap-github)
  tests/                 synth assertions for both apps (no AWS calls)
  cdk.json               bootstrap qualifier "sde"
  cdk.context.json       cached VPC lookup, gitignored (regenerated on the first synth with credentials)
```

## How deploys run
`.github/workflows/deploy.yml` deploys on push to `dev` / `test` / `prod` (branch = environment),
assuming `GitHubActions-CurationEngine-<ENV>` in that account over OIDC — the same pattern as
`sde-api-scrapers`. That role is created once per account by the bootstrap app in `bootstrap/`
(`make bootstrap-github ENV=…`) and its ARN goes into the repository secret `AWS_ROLE_<ENV>`. The
role can only assume the CDK toolkit roles (`cdk-sde-*`), so it can deploy this stack and nothing
else. The manual path below produces the identical stack and remains the fallback.
Step-by-step, including the one-time account setup: `docs/deploy-dev.md`.

## Prerequisites
- Docker running (the image is built locally for linux/amd64 and pushed by CDK).
- Python 3.13 (or uv), the `cdk` CLI (`npm i -g aws-cdk`), and an SSO session: `aws sso login --profile sde-dev`.
- The account is CDK-bootstrapped with qualifier `sde`
  (`cdk bootstrap aws://<account>/us-east-1 --qualifier sde --profile <profile>`; dev already is).

## Manual deploy (from the repo root; `ENV=dev PROFILE=sde-dev` are the defaults)
```bash
make infra-install            # infra/.venv: `uv sync` if uv is installed, else venv + pip -r requirements-dev.txt
make infra-test               # synth assertions
cp infra/envs/example.json infra/envs/dev.json   # fill in the account's values (ask a maintainer)
make infra-seed ENV=dev       # → SSM /sde-curation-engine/dev/*
make synth ENV=dev            # first run performs the VPC lookup → infra/cdk.context.json
make diff  ENV=dev
make deploy ENV=dev           # ~10 min the first time (CloudFront)
```
Then populate the secrets the stack created and restart the task so it picks them up:
```bash
aws secretsmanager put-secret-value --secret-id /sde-curation-engine/dev/openai_api_key --secret-string 'sk-…' --profile sde-dev
aws secretsmanager put-secret-value --secret-id /sde-curation-engine/dev/notify_webhook_url --secret-string 'https://hooks.slack.com/…' --profile sde-dev   # optional
make redeploy ENV=dev
aws secretsmanager get-secret-value --secret-id /sde-curation-engine/dev/app_password --query SecretString --output text --profile sde-dev
```
Open the `CloudFrontUrl` output, sign in with that password. `make logs ENV=dev` tails the engine.

Changing an account value later: edit `envs/<env>.json`, `make infra-seed`, `make deploy`
(CloudFormation re-resolves SSM parameters on every update).

## What the stack sets on the container
| Var | Source |
|---|---|
| `DATA_DIR` | `/data` (EFS access point `/engine`): per-collection YAML, index logs, scrape jobs |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_SSLMODE` | the RDS instance endpoint, `engine`, `require` |
| secrets → `DB_USER`, `DB_PASSWORD` | fields of the generated `/sde-curation-engine/<env>/db` secret |
| `SCRAPE_BACKEND` / `INDEX_BACKEND` | `ssm` / `ecs` |
| `CRAWLER_INSTANCE_ID`, `CRAWLER_S3_BUCKET` | SSM `crawler_instance_id`, `crawler_bucket` |
| `INDEXING_ECS_CLUSTER`, `INDEXING_TASK_FAMILY` | SSM `indexing_cluster_name`, `indexing_task_family` |
| `INDEXING_CONTAINER_NAME`, `INDEXING_SUBNETS` | `EnvConfig`; the default-VPC public subnets in `EnvConfig.azs` (Fargate is not offered in us-east-1e) |
| `COSMOS_INDEX_BUCKET`, `OPENSEARCH_ENDPOINT_TEST/PROD` | SSM `cosmos_index_bucket`, `opensearch_endpoint_*` |
| `PROD_INDEX_ROLE_ARN` (test only) | SSM `prod_index_role_arn`, the prod-account role "Index to prod" writes through ([docs/prod-index-access.md](../docs/prod-index-access.md)) |
| `CRAWLER_S3_PREFIX` | `EnvConfig.crawler_s3_prefix` (`sde-curation-engine-prototype` in dev, bucket root in test) |
| `WEB_INDEX_NAME`, `OPENAI_MODEL` | `EnvConfig` (`WEB_INDEX_NAME`: `sde-web` in every env) |
| `PUBLIC_BASE_URL`, `AUTH_COOKIE_SECURE` | the CloudFront URL, `true` |
| secrets → `OPENAI_API_KEY`, `APP_PASSWORD`, `SESSION_SECRET`, `NOTIFY_WEBHOOK_URL` | `/sde-curation-engine/<env>/*` in Secrets Manager |

`INDEXING_DISPATCH_ROLE_ARN` and `VALIDATION_ASSUME_ROLE_ARN` are deliberately unset: the task role
carries the `ecs:RunTask`/`iam:PassRole` statements of `CosmosIndexingDispatchRole-<env>` directly
(that role only trusts `indexing-helper-role`), and the stack adds its own AOSS data-access policy
(`sde-curation-engine-<env>`: read on `index/<collection>/sde-web*`, plus write on the working index
where "Index to prod" publishes into the same collection, i.e. dev) so direct validation and
publishing work without touching policies owned by other stacks. In test, "Index to prod" writes the
SMCE prod collection through `PROD_INDEX_ROLE_ARN` instead: see
[docs/prod-index-access.md](../docs/prod-index-access.md) for the role the prod account has to create.

## Operating notes
- **Single task by design** (in-process job registry and locks). A deploy replaces the task
  (`minHealthyPercent=0`): about 90 s without the engine. Running jobs are **not** failed: every
  job kind carries on after the restart as the same job, started `RESUME_START_DELAY_S` after the
  engine serves. A job fails instead after `RESUME_MAX_RESTARTS` (3) restarts in a row with no
  progress between them, or `RESUME_MAX_RESTARTS_TOTAL` (10) restarts in all. A scrape
  keeps running on the crawler host meanwhile. Deploy when idle anyway: a resumed job repeats the
  step it was in. **A restored database resumes its running jobs too**, including Index to prod:
  fail them first ([dev-db-rollback.md](../docs/dev-db-rollback.md), Step 8b).
- **Database**: RDS PostgreSQL 17, `db.m6i.large` everywhere, single-AZ in dev and test, Multi-AZ
  in prod (`config.py`), 20 GB gp3 autoscaling to `db_max_storage_gib` (200 GB; 500 GB in test),
  encrypted, not publicly accessible, only the service security group may connect. Automated
  backups with point-in-time recovery (7 days; 35 in prod), deletion protection in prod, and a
  final snapshot on stack deletion. Performance Insights and the PostgreSQL log in CloudWatch are
  on. A parameter group loads `pg_stat_statements` and logs statements over 2 s.
- **First deploy with the parameter group (M1)**: the deploy that first attaches the parameter group
  to an environment's database **reboots that database**, about 1–2 minutes without it.
  CloudFormation reboots on attach because `shared_preload_libraries` is a static parameter. It
  happens once per environment: dev on the merge to `dev`, test and prod when the change reaches
  their branch. Before that deploy: check the Jobs page shows nothing running (a database outage
  fails running jobs; only an engine restart resumes them), and tell the curators. After it: check
  that the extension exists, because migration V13 creates it once and skips quietly if the library
  was not loaded yet:
  `python -c "…; print(c.execute(\"select extversion from pg_extension where extname='pg_stat_statements'\").fetchall())"`
  (the `psycopg.connect(...)` from [dev-db-rollback.md](../docs/dev-db-rollback.md), Step 1). An
  empty list means: run `CREATE EXTENSION IF NOT EXISTS pg_stat_statements` once.
- **Alarms**: 9 CloudWatch alarms (database CPU, memory and connections; engine 5xx, slow answers,
  unhealthy, none healthy; load-balancer 5xx; engine memory) notify the SNS topic
  `<name>-alarms`, which emails every address in `alarm_emails` (`config.py`). After each
  environment's first deploy, every address receives an AWS "Subscription Confirmation" email and
  gets no alarms until someone clicks its link.
- **Query the database**: from a shell in the task (below) — `python -c` with `psycopg` and the
  `DB_*` env, or `apt-get`-free: `python -m sde_curation.import_sqlite --help` shows the env it
  reads. For `psql` from a laptop, add an SSM-reachable bastion or a temporary ingress rule on
  the `DbSg` security group; the instance itself is never public.
- **Moving an existing SQLite `engine.db`**: `docs/rds-cutover.md`.
- **SSE through CloudFront**: origin read timeout 60 s, the stream pings every 15 s, the UI also polls.
- **ALB is reachable only from CloudFront** (security group admits the `com.amazonaws.global.cloudfront.origin-facing` prefix list); hitting the ALB DNS directly times out on purpose.
- **WAF**: AWS managed common rule set (with `SizeRestrictions_BODY` set to count so big pattern edits pass) + 1000 req / 5 min / IP.
- **Shell into the task**: `aws ecs execute-command --cluster sde-curation-engine-dev --task <arn> --container engine --interactive --command bash --profile sde-dev` (needs the session-manager plugin).
- **EFS is retained** on `cdk destroy` and the database becomes a final snapshot; delete both by
  hand if you really want the data gone.

## Adding test / prod
There is no prod curation engine: the real engine runs in SMCE test and publishes to SMCE prod from
there ("Index to prod", through `prod_index_role_arn`). The `prod` config/branch remain only as
scaffolding and are not deployed.

Same steps against that account's profile: bootstrap with the `sde` qualifier, write
`envs/<env>.json`, `make infra-seed ENV=test PROFILE=<profile>`, `make bootstrap-github ENV=test PROFILE=<profile>`
(add `-c create_oidc_provider=true` if the account has no GitHub OIDC provider yet), set the
`AWS_ROLE_TEST` secret, then push to the `test` branch (or `make deploy ENV=test PROFILE=<profile>`).
A deploy against an unseeded account fails on the first missing SSM parameter. Per-env sizes live in
`config.py` (`CONFIGS`). `OPENSEARCH_ENDPOINT_TEST` is what "Index to test" validates against (and
must equal the account's indexer); `OPENSEARCH_ENDPOINT_PROD` is where "Index to prod" publishes the
test run's vectors: in test that is the SMCE prod collection, reached through `prod_index_role_arn`
([docs/prod-index-access.md](../docs/prod-index-access.md)).
