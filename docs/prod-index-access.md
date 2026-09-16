# Prod index access for "Index to prod"

**Index to prod** does not run the indexer. The engine takes the vectors that the validated test run
already produced and writes them straight into the production web index:

- the vectors come from `s3://sde-cosmos-indexing-<env>/vectorized/<collection>/<run>/batch_NNNN.jsonl`
- anything missing there is read from the test index
- the target is the production `sde-web` index

Nothing is re-chunked or re-vectorized (`sde_curation/backends/publish.py`).

The real engine runs in **SMCE test** (account `119417011911`), but the production OpenSearch Serverless
collection (`o2mxw7n9akk8n7o5oiqb`) is in the prod account. To write to it, the engine assumes a
role in the prod account. Someone with admin access to the prod account creates that role once, as
described below.

## 1. Role in the prod account

Suggested name: `sde-curation-engine-prod-publisher`.

**Trust policy.** Only the test engine's task role may assume the role:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": "arn:aws:iam::119417011911:role/sde-curation-engine-test-task-role"},
    "Action": "sts:AssumeRole"
  }]
}
```

**Identity policy.** This is the API access to the collection. `aoss:APIAccessAll` is required for
every data-plane call. What the role may do inside the collection is decided by the data-access
policy in step 2.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "aoss:APIAccessAll",
    "Resource": "arn:aws:aoss:us-east-1:<prod-account>:collection/o2mxw7n9akk8n7o5oiqb"
  }]
}
```

Keep the maximum session duration at 1 h or longer. The engine refreshes the credentials itself
during long publishes.

## 2. AOSS data-access policy in the prod account

This grants the role access to the `sde-web` index only. `<collection-name>` is the name of collection
`o2mxw7n9akk8n7o5oiqb` (`aws opensearchserverless batch-get-collection --ids o2mxw7n9akk8n7o5oiqb`).

```json
[{
  "Description": "sde-curation-engine publishes curated collections to sde-web",
  "Principal": ["arn:aws:iam::<prod-account>:role/sde-curation-engine-prod-publisher"],
  "Rules": [
    {"ResourceType": "collection", "Resource": ["collection/<collection-name>"],
     "Permission": ["aoss:DescribeCollectionItems"]},
    {"ResourceType": "index", "Resource": ["index/<collection-name>/sde-web"],
     "Permission": ["aoss:DescribeIndex", "aoss:ReadDocument", "aoss:WriteDocument", "aoss:UpdateIndex"]}
  ]
}]
```

The policy grants no `CreateIndex` and no `DeleteIndex`:

- the engine never creates the index; it refuses to run when the index is missing
- it never deletes documents; removed URLs are tombstoned with `public_visibility: false`

The collection's **network policy** must allow access from the engine, which runs on public Fargate
subnets with no VPC endpoint into the prod account. That means public access to the collection
endpoint, like the indexer in test already uses.

## 3. Wire it into the test engine

1. In `infra/envs/test.json` (gitignored), set:
   - `opensearch_endpoint_prod`: `https://o2mxw7n9akk8n7o5oiqb.us-east-1.aoss.amazonaws.com`
   - `prod_index_role_arn`: the ARN of the role from step 1
2. Run `make infra-seed ENV=test PROFILE=smce-test`.
3. Redeploy test: re-run the Deploy workflow, or `make deploy ENV=test PROFILE=smce-test`. CloudFormation
   re-resolves SSM on every update.

The test stack already gives its task role these permissions (`infra/stacks/engine_stack.py`):

- `sts:AssumeRole` on that ARN
- `s3:GetObject` on `vectorized/*`

## 4. Check it

- **Before the role exists:** Index to prod fails at the pre-flight with `prod_index_unreachable` or
  an AssumeRole `AccessDenied` in the job error. Nothing is written.
- **Afterwards:** on a small collection that validated on test:
  - **Index to prod** reports `N written (N from S3 vectors · 0 from the test index)`, then a prod
    validation `N / N visible`
  - **Re-index to prod** reports `0 written · N unchanged`

## What the publish refuses to do

These are the same guards as the indexer. Any of them fails the run before anything is written:

| error | meaning |
|---|---|
| `export_not_found` | the test run's export expired (30 days): re-index to test first |
| `index_not_found` | prod `sde-web` does not exist |
| `id_scheme_collision` / `duplicate_business_ids` | the collection's prod documents carry ids the engine would not mint, so updating them would duplicate them |
| `scope_filter_ineffective` | the collection filter does not isolate the collection |
| `deletion_threshold_exceeded` / `deletion_budget_exceeded` | more than `PUBLISH_DELETION_ABORT_RATIO` (90%) or `PUBLISH_DELETION_ABORT_MAX` (5000) of the collection's prod documents would be removed |

Two more failures can happen after writing has started. Neither one removes anything:

| error | meaning |
|---|---|
| `vectors_missing` | some documents have no vectors at their current version in S3 or the test index. Written documents stay, and the job lists the URLs. |
| `upsert_failed` | bulk items still failed after 3 attempts |
