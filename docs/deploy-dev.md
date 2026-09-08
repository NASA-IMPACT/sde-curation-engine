# Deploying the curation engine to dev

End state: the engine runs as one ECS Fargate task in the SMCE Dev account behind CloudFront,
drives the dev crawler over SSM, dispatches the dev WEB_COSMOS indexer with `ecs:RunTask`, and
validates against the dev OpenSearch Serverless collection. The dev elastic wrapper reads that same
collection, so anything the engine indexes shows up in the wrapper's search results; the engine
never calls the wrapper directly.

Reference for the stack itself (resources, IAM, operating notes): `infra/README.md`.

**Quick checklist** (each item is a section below):

- [ ] 0. You have the access listed in section 0
- [ ] 2.1 Tools installed, `sde-dev` SSO profile works
- [ ] 2.2 Account is CDK-bootstrapped with qualifier `sde`
- [ ] 2.3 `make bootstrap-github ENV=dev` run, repository secret `AWS_ROLE_DEV` set
- [ ] 2.4 `make infra-seed ENV=dev` run, eleven parameters visible in SSM
- [ ] 3. Merged to `dev`, Deploy workflow green, CloudFront URL known
- [ ] 4. OpenAI key set, `make redeploy`, login password retrieved
- [ ] 5. Verification steps 1–8 pass

Time budget for a first dev deployment: about one hour of hands-on work, of which the CloudFront
creation (~10 min) and the first image build (~5 min) are waiting.

## 0. Access you need before starting

| For | You need | How to check |
|---|---|---|
| sections 2.2–2.4, 4, 5 | an AWS SSO login to the SMCE Dev account with permissions to create IAM roles, write SSM parameters and Secrets Manager values, and read ECS/CloudFormation | `aws sts get-caller-identity --profile sde-dev` prints an account id and your role |
| section 2.3 | admin (or "secrets" write) permission on the GitHub repository `NASA-IMPACT/sde-curation-engine` | `gh secret list --repo NASA-IMPACT/sde-curation-engine` does not error |
| section 3 | permission to push to (or merge a PR into) the `dev` branch | `gh api repos/NASA-IMPACT/sde-curation-engine/branches/dev/protection` (404 = unprotected) |
| section 4 | the OpenAI API key for this project (from whoever owns the OpenAI org) | it starts with `sk-` |
| section 2.4 | either `infra/envs/dev.json` from a maintainer, or read access to the account to discover the values | — |

## 1. How a deploy happens

**Merging to `dev` is the deploy.** GitHub Actions (`.github/workflows/deploy.yml`) runs on every
push to `dev`, `test` or `prod` that touches app, infra or container files, exactly like
`sde-api-scrapers` and `sde-elastic-wrapper`:

| Branch | Environment | AWS account | Role (repository secret) |
|---|---|---|---|
| `dev`  | dev  | SMCE Dev  | `AWS_ROLE_DEV` |
| `test` | test | SMCE Test | `AWS_ROLE_TEST` |
| `prod` | prod | SMCE Prod | `AWS_ROLE_PROD` |

The workflow has four jobs, in order:

| Job | What it does | Typical time | If it fails |
|---|---|---|---|
| **test** | app tests, ruff, CDK synth assertions — no AWS access | 2 min | nothing was touched in AWS; fix the code |
| **deploy** | assumes `GitHubActions-CurationEngine-DEV` over OIDC, builds the Docker image, `cdk diff`, `cdk deploy CurationEngine-dev`, posts the CloudFront URL to the run summary | 5 min (first time ~12) | CloudFormation rolls the stack back to the previous version; the old task keeps running only if this was not the first deploy |
| **verify** | ECS service is `ACTIVE`, 1 running task, rollout `COMPLETED`; `GET /health` through CloudFront returns 200 | 1 min | the deploy went through but the app is not serving — see section 7 |
| **notify-failure** | writes a failure summary on the run | — | — |

What a deploy does *not* do: it does not create or change SSM parameter values or secret values.
Those are account state, set once per environment by a person (sections 2.4 and 4). Pull requests
and pushes to any other branch only run the tests (`.github/workflows/test.yml`) — no AWS
credentials are involved, so a fork or a PR can never deploy.

Doc-only commits (README, `docs/`) do not trigger a deploy. Every real deploy replaces the single
task, so a scrape/index job running at that moment is marked failed — merge when the UI is idle.

## 2. One-time setup for an environment (admin, with an SSO profile)

Everything in this section is done once per AWS account. Dev needs it before the first merge.

### 2.1 Tools on your machine

**Step 1 — install** (macOS with Homebrew; on Linux use your package manager for the same tools):
```bash
brew install python@3.13 awscli gh jq node   # uv is optional: `brew install uv` and `make install` will use it
npm install -g aws-cdk
brew install --cask docker            # only needed for a manual `make deploy`; CI builds the image otherwise
```

**Step 2 — configure the `sde-dev` SSO profile** if you do not have one. You need the SSO start
URL and region for the SMCE organisation (ask a maintainer; it is the same one used for
`sde-api-scrapers`). Then:
```bash
aws configure sso --profile sde-dev   # pick the SMCE Dev account and your role when prompted
aws configure set region us-east-1 --profile sde-dev   # every command and Makefile target relies on the profile's region
aws sso login --profile sde-dev
```
If you already have the profile, still run the `configure set region` line: a profile without a
region makes every `aws` command fail with "You must specify a region".

**Step 3 — verify** every tool answers:
```bash
python3.13 --version && cdk --version && gh --version && jq --version   # (or `uv --version` if you use uv)
aws sts get-caller-identity --profile sde-dev      # → your account id + role; that account is "dev"
gh auth status                                      # logged in to github.com
docker info >/dev/null && echo docker ok            # only for the manual path
```

**Step 4 — install the project dependencies** from the repository root:
```bash
make install                          # .venv: `uv sync` if uv is on PATH, otherwise python3.13 venv + pip (same pinned versions)
make infra-install                    # infra/.venv, same rule, with the CDK libraries
```

### 2.2 CDK bootstrap

The account must be CDK-bootstrapped with qualifier `sde`, which creates the `cdk-sde-*` roles and
the asset bucket that both the workflow and a manual deploy use.

**Step 1 — check** whether it already is (dev is):
```bash
aws cloudformation describe-stacks --stack-name CDKToolkit --profile sde-dev \
  --query "Stacks[0].Parameters[?ParameterKey=='Qualifier'].ParameterValue" --output text
```
Expected output: `sde`. If the command errors with "does not exist", or prints something other
than `sde`, go to step 2.

**Step 2 — bootstrap** (only if step 1 failed; needs admin in the account):
```bash
cdk bootstrap aws://$(aws sts get-caller-identity --profile sde-dev --query Account --output text)/us-east-1 \
  --qualifier sde --profile sde-dev
```

### 2.3 GitHub Actions deploy role

**Step 1 — check the GitHub OIDC identity provider exists** in the account (it is account-wide and
shared with the other SDE repos; dev already has it):
```bash
aws iam list-open-id-connect-providers --profile sde-dev --output text
```
Expected: one line containing `oidc-provider/token.actions.githubusercontent.com`. If there is
none, add `-c create_oidc_provider=true` in step 2 (see the second command).

**Step 2 — create the role** (stack `CurationEngine-Bootstrap-dev`):
```bash
aws sso login --profile sde-dev
make bootstrap-github ENV=dev PROFILE=sde-dev
# account without the OIDC provider (not dev):
# cd infra && . .venv/bin/activate && AWS_PROFILE=sde-dev cdk --app "python bootstrap/app.py" deploy CurationEngine-Bootstrap-dev -c environment=dev -c create_oidc_provider=true
```
This creates `GitHubActions-CurationEngine-DEV`, trusted only by this repository's `dev` branch
(both subject formats GitHub uses: `repo:NASA-IMPACT/sde-curation-engine:ref:refs/heads/dev` and the
immutable `repo:NASA-IMPACT@<org id>/sde-curation-engine@<repo id>:ref:refs/heads/dev`), allowed only to assume the CDK toolkit
roles (`cdk-sde-*`) and read the stack/service for the verify step. The last lines of the output
show `CurationEngine-Bootstrap-dev.GitHubActionsRoleArn = arn:aws:iam::…:role/GitHubActions-CurationEngine-DEV`.

**Step 3 — put the role ARN in the repository secret** `AWS_ROLE_DEV`:
```bash
ROLE_ARN=$(aws cloudformation describe-stacks --stack-name CurationEngine-Bootstrap-dev --profile sde-dev \
  --query "Stacks[0].Outputs[?OutputKey=='GitHubActionsRoleArn'].OutputValue" --output text)
echo "$ROLE_ARN"          # must print arn:aws:iam::…:role/GitHubActions-CurationEngine-DEV; empty = step 2 has not run
[ -n "$ROLE_ARN" ] && gh secret set AWS_ROLE_DEV --repo NASA-IMPACT/sde-curation-engine --body "$ROLE_ARN"
```
Or in the browser: repository → Settings → Secrets and variables → Actions → New repository secret,
name `AWS_ROLE_DEV`, value the ARN.

**Step 4 — verify**:
```bash
gh secret list --repo NASA-IMPACT/sde-curation-engine     # shows AWS_ROLE_DEV with a timestamp
```

### 2.4 Account values → SSM Parameter Store

The stack needs eleven values that identify things already deployed in the account: the crawler,
the indexer, the hand-off bucket and the OpenSearch Serverless collection. They are **not in git**
(this repository is public). They go into a local file, `infra/envs/dev.json`, which
`make infra-seed` pushes into SSM Parameter Store, where CloudFormation reads them at deploy time.

If a maintainer already has `infra/envs/dev.json`, get it from them (Slack DM, not a commit),
drop it in place, and skip to step 4. Otherwise discover the values yourself:

**Step 1 — start from the template**
```bash
cp -n infra/envs/example.json infra/envs/dev.json   # -n: never overwrite a dev.json you already have
aws sso login --profile sde-dev
export AWS_PROFILE=sde-dev            # so the commands below need no --profile
```

**Step 2 (route A: by hand) — look up each value.** Every command prints exactly what goes into the file. Do either step 2 or step 3, not both.

| Key in `dev.json` | Where it comes from | Command |
|---|---|---|
| `crawler_instance_id` | `SdeCrawlerStack` output `InstanceId` (sde-crawl4ai-scraper-v1) | `aws cloudformation describe-stacks --stack-name SdeCrawlerStack --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text` |
| `crawler_bucket` | `SdeCrawlerStack` output `BucketName` | `aws cloudformation describe-stacks --stack-name SdeCrawlerStack --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" --output text` |
| `indexing_cluster_name` | the api-scrapers ECS cluster, `api-scrapers-cluster-dev` | `aws ecs list-clusters --query "clusterArns[?contains(@,'api-scrapers-cluster')]" --output text` → take the part after the last `/` |
| `indexing_task_family` | the WEB_COSMOS task definition family, `web_cosmos-scraper-dev` | `aws ecs list-task-definition-families --family-prefix web_cosmos-scraper --status ACTIVE --query families --output text` |
| `indexing_task_role_arn` | `taskRoleArn` of that task definition | `aws ecs describe-task-definition --task-definition web_cosmos-scraper-dev --query taskDefinition.taskRoleArn --output text` |
| `indexing_execution_role_arn` | `executionRoleArn` of that task definition | `aws ecs describe-task-definition --task-definition web_cosmos-scraper-dev --query taskDefinition.executionRoleArn --output text` |
| `cosmos_index_bucket` | the indexer's own `COSMOS_INDEX_BUCKET` env var (`sde-cosmos-indexing-dev`) | `aws ecs describe-task-definition --task-definition web_cosmos-scraper-dev --query "taskDefinition.containerDefinitions[0].environment[?name=='COSMOS_INDEX_BUCKET'].value" --output text` |
| `opensearch_endpoint_test` | the indexer's `OPENSEARCH_ENDPOINT_TEST` env var — use the **same** value the indexer uses, or validation checks a different index than was written | `aws ecs describe-task-definition --task-definition web_cosmos-scraper-dev --query "taskDefinition.containerDefinitions[0].environment[?name=='OPENSEARCH_ENDPOINT_TEST'].value" --output text` |
| `opensearch_endpoint_prod` | the indexer's `OPENSEARCH_ENDPOINT_PROD` env var (on dev both point at the dev collection) | same command with `OPENSEARCH_ENDPOINT_PROD` |
| `aoss_collection_name` | the OpenSearch Serverless collection behind those endpoints (`sde-binary`) | `aws opensearchserverless list-collections --query "collectionSummaries[].[name,id]" --output text` |
| `aoss_collection_id` | its id — it is also the first label of the endpoint hostname, `https://<id>.us-east-1.aoss.amazonaws.com` | same command; the id must match the endpoint hostname |

Console equivalents, if you prefer clicking: CloudFormation → Stacks → `SdeCrawlerStack` → Outputs;
ECS → Clusters; ECS → Task definitions → `web_cosmos-scraper-dev` → latest revision → JSON (roles
and the container environment); OpenSearch Service → Serverless → Collections.

**Step 3 (route B: automatic) — let the CLI write the file for you.** Same lookups as step 2, one block; skip step 2 if you use this:
```bash
TD=web_cosmos-scraper-dev
envval() { aws ecs describe-task-definition --task-definition "$TD" \
  --query "taskDefinition.containerDefinitions[0].environment[?name=='$1'].value" --output text; }
ENDPOINT_TEST=$(envval OPENSEARCH_ENDPOINT_TEST)
COLLECTION_ID=$(echo "$ENDPOINT_TEST" | sed -E 's#https://([^.]+)\..*#\1#')
python3 - <<PY > infra/envs/dev.json
import json, subprocess
def sh(c): return subprocess.check_output(c, shell=True, text=True).strip()
print(json.dumps({
  "crawler_instance_id":        sh("aws cloudformation describe-stacks --stack-name SdeCrawlerStack --query \"Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue\" --output text"),
  "crawler_bucket":             sh("aws cloudformation describe-stacks --stack-name SdeCrawlerStack --query \"Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue\" --output text"),
  "indexing_cluster_name":      sh("aws ecs list-clusters --query \"clusterArns[?contains(@,'api-scrapers-cluster')]\" --output text").rsplit("/", 1)[-1],
  "indexing_task_family":       "$TD",
  "indexing_task_role_arn":     sh("aws ecs describe-task-definition --task-definition $TD --query taskDefinition.taskRoleArn --output text"),
  "indexing_execution_role_arn":sh("aws ecs describe-task-definition --task-definition $TD --query taskDefinition.executionRoleArn --output text"),
  "cosmos_index_bucket":        "$(envval COSMOS_INDEX_BUCKET)",
  "aoss_collection_name":       sh("aws opensearchserverless list-collections --query \"collectionSummaries[?id=='$COLLECTION_ID'].name\" --output text"),
  "aoss_collection_id":         "$COLLECTION_ID",
  "opensearch_endpoint_test":   "$ENDPOINT_TEST",
  "opensearch_endpoint_prod":   "$(envval OPENSEARCH_ENDPOINT_PROD)",
}, indent=2))
PY
cat infra/envs/dev.json                        # every value filled, none empty, no "<env>" or "xxxx" placeholder left, both endpoints start with https://
```

**Step 4 — check the file and push it to SSM**
```bash
make infra-seed ENV=dev PROFILE=sde-dev        # validates the keys, then put-parameter × 11
```
The command lists the eleven parameter names it wrote. It refuses a file with a missing, extra, or
empty key. It is safe to re-run; re-running overwrites.

**Step 5 — confirm what is in SSM**
```bash
aws ssm get-parameters-by-path --path /sde-curation-engine/dev \
  --query "Parameters[].[Name,Value]" --output table
```
You should see eleven rows and recognise every value. This is the source of truth from now on:
`dev.json` is just your local copy. Keep it (it is gitignored) so you can edit and re-seed later.

## 3. First deploy of dev

**Step 1 — make sure `dev` will contain what you expect**:
```bash
git fetch origin
git checkout dev && git pull
git log --oneline -3 origin/redesigned-curation      # the commits you are about to deploy
make test && make lint && make infra-test            # green locally = the test job will be green
```

**Step 2 — merge**. Either open a PR `redesigned-curation → dev` and merge it in GitHub, or:
```bash
git merge redesigned-curation
git push origin dev                                   # ← this push triggers Deploy
```
The workflow starts within a few seconds. (A merge that only touches docs does not trigger it —
see the `paths:` list in `deploy.yml`.)

**Step 3 — watch the run**:
```bash
gh run list --repo NASA-IMPACT/sde-curation-engine --workflow Deploy --limit 3
gh run watch --repo NASA-IMPACT/sde-curation-engine     # pick the newest run; streams job status
```
Or the Actions tab in the browser. Expected sequence: `test` ✓ (2 min) → `deploy` ✓ (about 12 min
the first time — CloudFront is created in this step) → `verify` ✓ (1 min).

**Step 4 — get the URL**. The run summary (Actions → the run → top of the page) shows
`Deployed dev` with the URL. From the CLI at any later time:
```bash
aws cloudformation describe-stacks --stack-name CurationEngine-dev --profile sde-dev \
  --query "Stacks[0].Outputs[?OutputKey=='CloudFrontUrl'].OutputValue" --output text
```
Keep it; the rest of this document calls it `<CloudFrontUrl>`.

**Step 5 — confirm the app is up**:
```bash
curl -s <CloudFrontUrl>/health
```
Expected: `{"ok":true,"db":"ok",…}`. If CloudFront answers 502/504 in the first minutes after
creation, wait a minute and retry — the distribution is still propagating.

## 4. Secrets — including the OpenAI key (once, right after the first deploy)

The stack **creates** four secrets in Secrets Manager but only knows the value of two of them:

| Secret | Created with | You do |
|---|---|---|
| `/sde-curation-engine/dev/app_password` | random 24 chars | read it out to sign in (step 4) |
| `/sde-curation-engine/dev/session_secret` | random 64 chars | nothing |
| `/sde-curation-engine/dev/openai_api_key` | placeholder `REPLACE_ME` | **set it once** (step 1) |
| `/sde-curation-engine/dev/notify_webhook_url` | placeholder `disabled` | set it if you want Slack notifications (step 1) |

The OpenAI key is never in git, in `.env`, in the CDK code, or in GitHub. Until you set it the app
starts and works, but LLM-assisted curation calls fail (the log shows an OpenAI 401).

**Step 1 — set the values**:
```bash
aws secretsmanager put-secret-value --profile sde-dev \
  --secret-id /sde-curation-engine/dev/openai_api_key --secret-string 'sk-…'

# optional — Slack-compatible webhook for status notifications
aws secretsmanager put-secret-value --profile sde-dev \
  --secret-id /sde-curation-engine/dev/notify_webhook_url --secret-string 'https://hooks.slack.com/…'
```
Each command prints the secret's ARN and a new `VersionId`. To double-check without printing the
key: `aws secretsmanager get-secret-value --secret-id /sde-curation-engine/dev/openai_api_key --profile sde-dev --query SecretString --output text | cut -c1-6` → `sk-…`.

**Step 2 — restart the task** (ECS reads secrets only when a task starts):
```bash
make redeploy ENV=dev PROFILE=sde-dev
aws ecs wait services-stable --cluster sde-curation-engine-dev --services sde-curation-engine-dev --profile sde-dev
```
`make redeploy` prints the deployments (`PRIMARY IN_PROGRESS`, then the old one `ACTIVE`); the
`wait` returns when the new task is healthy, usually within 2–3 minutes.

**Step 3 — confirm the app came back**: `curl -s <CloudFrontUrl>/health` → `"ok":true`.

**Step 4 — get the login password**:
```bash
aws secretsmanager get-secret-value --profile sde-dev \
  --secret-id /sde-curation-engine/dev/app_password --query SecretString --output text
```
Share it with the team through a password manager, not chat. To change it: `put-secret-value` on
that secret with your own value, then `make redeploy`.

The secrets are `RETAIN`ed: later deploys, and even `make destroy`, leave their values alone.
Rotating the OpenAI key is the same `put-secret-value` + `make redeploy`.

## 5. Verify the deployment

Keep `make logs ENV=dev PROFILE=sde-dev` running in a second terminal for all of this. The
commands below read the account values from your local `infra/envs/dev.json` with `jq`.

```bash
export AWS_PROFILE=sde-dev
CF=<CloudFrontUrl>
CRAWLER_BUCKET=$(jq -r .crawler_bucket infra/envs/dev.json)
HANDOFF_BUCKET=$(jq -r .cosmos_index_bucket infra/envs/dev.json)
INDEXER_CLUSTER=$(jq -r .indexing_cluster_name infra/envs/dev.json)
INDEXER_FAMILY=$(jq -r .indexing_task_family infra/envs/dev.json)
```

**1. Up and locked**
```bash
curl -s $CF/health                                   # {"ok":true,…}  — no cookie needed
curl -s -o /dev/null -w '%{http_code}\n' $CF/api/collections     # 401
ALB=$(aws cloudformation describe-stacks --stack-name CurationEngine-dev --query "Stacks[0].Outputs[?OutputKey=='AlbDnsName'].OutputValue" --output text)
curl -s -m 5 http://$ALB/health || echo "ALB refused/timed out: correct, only CloudFront may reach it"
```

**2. Sign in**: open `$CF` in a browser → login page → the password from section 4. You land on
the collection list; the header shows a **Sign out** button and a green SSE dot.

**3. Crawler (SSM)**: on the dashboard fill in **Seed URL** `https://aurorasaurus.org`, a name,
**Max pages** 15, click **Add collection**, then open it and click **Scrape**. Within a few seconds the engine log shows a line with the SSM command id. Then:
```bash
aws ssm list-command-invocations --details --max-items 1 --query "CommandInvocations[0].[Status,CommandId]" --output text   # Success
aws s3 ls s3://$CRAWLER_BUCKET/scraped_collections/ | tail -3                                                              # a new file for aurorasaurus.org
```
In the UI the collection reaches **scraped** with a dump of ~15 URLs (a small site takes 1–3 min).

**4. Indexer (ECS)**: on the collection page click **Start curating** (status → curating), then
**Mark curated** (or **Promote … deltas → curated** if you changed patterns), then **Index to
test** and confirm the dialog. Watch:
```bash
aws s3 ls s3://$HANDOFF_BUCKET/curated_collections/ --recursive | tail -3                 # <collection>/<run_id>/documents.jsonl, then manifest.json
aws ecs list-tasks --cluster $INDEXER_CLUSTER --family $INDEXER_FAMILY --desired-status RUNNING   # one task ARN while it runs
aws logs tail /ecs/api-scrapers-dev --since 5m --follow                              # the indexer's own log
aws s3 ls s3://$HANDOFF_BUCKET/index_runs/ --recursive | tail -3                     # status.json appears when it finishes
```
The indexer task runs 2–5 minutes for a 15-page site.

**5. Validation (AOSS)**: about 30 s after the indexer finishes, the engine validates directly
against the collection. The engine log shows `validated_by: direct` and **no** 403; in the UI the
collection moves to **config_generated** and **Index to prod** becomes available.

**6. Wrapper**: search the dev elastic wrapper (its own URL, from that repo's outputs) for a title
you saw in the dump. It reads the same `sde-web-subset` index the indexer just wrote, so the
document is there.

**7. State survives restarts**:
```bash
make redeploy ENV=dev PROFILE=sde-dev
aws ecs wait services-stable --cluster sde-curation-engine-dev --services sde-curation-engine-dev
```
Reload the UI: the collection and its dump are still there (SQLite lives on EFS), and `/health`
answers.

**8. LLM assist**: on a collection in curation click **✨ Suggest patterns**. Suggested patterns
appear under the pattern list and the log has no `openai` error. If it shows 401, redo section 4.

## 6. Day-two operations

| Task | How |
|---|---|
| ship a code change | merge to `dev` (Deploy runs) |
| re-deploy the same commit | Actions → Deploy → Run workflow (choose `dev`, run from the `dev` branch), or `make deploy ENV=dev PROFILE=sde-dev` |
| roll back to an earlier commit | `git revert <bad commit>` on `dev` and push (preferred, keeps history), or open the earlier successful Deploy run → **Re-run all jobs** |
| change a crawler/indexer/AOSS value | edit `infra/envs/dev.json`, `make infra-seed ENV=dev`, then re-run Deploy (CloudFormation re-resolves SSM on every update) |
| restart the app (picks up new secrets) | `make redeploy ENV=dev PROFILE=sde-dev` |
| tail logs | `make logs ENV=dev PROFILE=sde-dev` |
| shell into the task | `TASK=$(aws ecs list-tasks --cluster sde-curation-engine-dev --query 'taskArns[0]' --output text --profile sde-dev); aws ecs execute-command --cluster sde-curation-engine-dev --task $TASK --container engine --interactive --command bash --profile sde-dev` (needs `brew install --cask session-manager-plugin`) |
| deploy by hand (CI down, or debugging) | `aws sso login --profile sde-dev && make diff ENV=dev && make deploy ENV=dev` — same stack, same result; needs Docker running |
| pause automatic deploys | Actions → Deploy → ⋯ → Disable workflow; re-enable the same way |
| tear down | `make destroy ENV=dev` — EFS (SQLite + collection YAML) and the secrets are kept; the bootstrap stack, its role and the SSM parameters are untouched. To really delete: the EFS in the console, `aws secretsmanager delete-secret`, `aws ssm delete-parameters` |

## 7. If something goes wrong

Where to look first, by job:

| Job | Where the details are |
|---|---|
| test | the job log in Actions; reproduce with `make test && make lint && make infra-test` |
| deploy | the job log shows CloudFormation events; from the CLI: `aws cloudformation describe-stack-events --stack-name CurationEngine-dev --profile sde-dev --max-items 20 --query "StackEvents[?ResourceStatus=='CREATE_FAILED' \|\| ResourceStatus=='UPDATE_FAILED'].[LogicalResourceId,ResourceStatusReason]" --output table` |
| verify / app not serving | the engine log (`make logs`) and the last stopped task's reason: `aws ecs list-tasks --cluster sde-curation-engine-dev --desired-status STOPPED --profile sde-dev` then `aws ecs describe-tasks --cluster sde-curation-engine-dev --tasks <arn> --query "tasks[0].[stoppedReason,containers[0].reason]" --profile sde-dev` |

| Symptom | Cause / fix |
|---|---|
| Deploy job: `Not authorized to perform sts:AssumeRoleWithWebIdentity` | the repository secret is missing/wrong, or the push was not to the branch the role trusts → check `gh secret list` and section 2.3 |
| same error with a correct secret and branch | the token's subject does not match the trust policy — compare `gh api repos/NASA-IMPACT/sde-curation-engine/actions/oidc/customization/sub` (`sub_claim_prefix`) with the role's `StringLike` condition (`aws iam get-role`), then fix `infra/bootstrap/bootstrap_stack.py` and re-run `make bootstrap-github` |
| Deploy job: `Unable to fetch parameters [/sde-curation-engine/dev/…]` | section 2.4 was skipped or run against another account → `make infra-seed ENV=dev`, re-run the workflow |
| Deploy job: `SSM parameter /cdk-bootstrap/sde/version not found` or cannot assume `cdk-sde-*` | account not bootstrapped with `--qualifier sde` (section 2.2) |
| Deploy rolls back with "circuit breaker" / verify: 0 running tasks | the task never became healthy; the stopped-task reason above tells you why (bad SSM value, secret missing, EFS mount denied) |
| Verify: health check never passes but the service is steady | CloudFront still propagating on the very first deploy → re-run the verify job; otherwise `make logs` |
| the UI loads but Scrape/Index fails with AccessDenied | the SSM values are wrong for this account, or the task role lacks a permission — the engine log has the AccessDenied message with the exact action |
| LLM actions fail with 401 | the OpenAI key is still `REPLACE_ME` or wrong → section 4 |
| validation fails with 403 | the AOSS data-access policy did not apply, or the collection name in SSM is wrong → `aws opensearchserverless list-access-policies --type data --profile sde-dev` should list `sde-curation-engine-dev` |
| the browser loops on `/login` | you are hitting the ALB over plain HTTP; use the CloudFront URL (the cookie is `Secure`) |
| `no AWS credentials: set AWS_PROFILE` (local) | SSO session expired → `aws sso login --profile sde-dev` |
| `You must specify a region` (local) | the profile has no region → `aws configure set region us-east-1 --profile sde-dev` |
| a push to `dev` did not start Deploy | the commit touched only files outside the `paths:` filter (docs); run the workflow by hand if you do want a deploy |

## 8. Later: test and prod

Same procedure, against that account. In order:

1. Get an SSO profile for the account (`sde-test` / `sde-prod`) and check it: section 2.1 step 2–3.
2. CDK bootstrap with qualifier `sde` if missing: section 2.2, with `--profile sde-test`.
3. `make bootstrap-github ENV=test PROFILE=sde-test` (add `-c create_oidc_provider=true` via the
   long form if `list-open-id-connect-providers` shows no GitHub provider) and set `AWS_ROLE_TEST`.
4. Discover the account's values into `infra/envs/test.json` (section 2.4, replacing `dev` with
   `test` in every name) and `make infra-seed ENV=test PROFILE=sde-test`. Decide what
   `opensearch_endpoint_test/prod` should be for that environment: they are what the engine's
   "Index to test / prod" targets validate against, and must equal what that account's indexer uses.
5. Create the branch: `git checkout -b test dev && git push -u origin test` → Deploy runs for test.
6. Section 4 (secrets) and section 5 (verification) with `test` in place of `dev`.

The code is identical for every environment; only the SSM parameters, the secrets and the GitHub
role differ, so a `test`/`prod` branch never carries account values. Once `prod` exists, add a
GitHub *environment* named `prod` with a required reviewer and reference it from the deploy job.
