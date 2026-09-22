# Request: let the SDE Curation Engine write to the prod `sde-web` index

**For:** an administrator of the SMCE **prod** AWS account (the account that owns OpenSearch
Serverless collection `o2mxw7n9akk8n7o5oiqb`)
**From:** SDE Curation Engine team
**Effort:** about 15 minutes: one IAM role, one OpenSearch Serverless data-access policy, one check.
**What to send back:** the ARN of the role you create (step 1).

---

## Why

The SDE Curation Engine runs **only in SMCE test** (account `119417011911`). There is no prod
deployment of the engine. When a subject-matter expert (SME) finishes a collection and it passes
validation against the test index, "Index to prod" publishes it to production. It copies the
already-computed embeddings into the prod `sde-web` index, so nothing is re-vectorized.

The prod collection is in your account, so the engine needs a role in your account it can assume.
The engine's own identity is the ECS task role
`arn:aws:iam::119417011911:role/sde-curation-engine-test-task-role`, and that role can only assume
the one role you name.

What the engine does in prod, and nothing else:

| Action | OpenSearch call | Permission |
|---|---|---|
| check the index exists (never creates it) | `HEAD sde-web` | `aoss:DescribeIndex` |
| read one collection's documents (ids, embeddings for re-use), safety checks, validation | `_search`, `_count` | `aoss:ReadDocument` |
| delete that collection's documents, then write it back fresh | `_bulk` (`delete`, `index`, `update`) | `aoss:WriteDocument` |

It never creates, deletes, or remaps indexes. It does delete documents: every publish is a fresh
start for the collection being published, so all of that collection's documents are deleted and the
curated set is written back under fresh ids (document deletes are part of `aoss:WriteDocument`; no
index-level delete permission is asked for). Every write and delete is scoped to a single
`collection_key`, deletes go by explicit document id only, and each one is checked to belong to that
collection. It refuses to run, before deleting anything, if:
- the collection filter does not isolate exactly one collection
- a document outside the collection's `collection_key` carries its id prefix
- any document to be written has no embeddings to re-use

---

## Step 0: find the collection name

```bash
aws opensearchserverless batch-get-collection --ids o2mxw7n9akk8n7o5oiqb \
  --query 'collectionDetails[0].[name,arn]' --output text
```

Use that name wherever `<COLLECTION_NAME>` appears below. `<PROD_ACCOUNT_ID>` is your account id.

## Step 1: IAM role `sde-curation-engine-prod-publisher`

**Trust policy** (`trust.json`):

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

**Permissions policy** (`aoss-api.json`). OpenSearch Serverless requires `aoss:APIAccessAll` on the
collection for any data-plane call. What the role may do *inside* the collection is limited by the
data-access policy in step 2.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "aoss:APIAccessAll",
    "Resource": "arn:aws:aoss:us-east-1:<PROD_ACCOUNT_ID>:collection/o2mxw7n9akk8n7o5oiqb"
  }]
}
```

```bash
aws iam create-role --role-name sde-curation-engine-prod-publisher \
  --assume-role-policy-document file://trust.json \
  --description "SDE Curation Engine (SMCE test) publishes curated collections to prod sde-web"
aws iam put-role-policy --role-name sde-curation-engine-prod-publisher \
  --policy-name aoss-api-access --policy-document file://aoss-api.json
aws iam get-role --role-name sde-curation-engine-prod-publisher --query Role.Arn --output text
```

The default maximum session duration (1 hour) is fine. The engine refreshes its credentials during
long runs.

> If your account requires a permissions boundary or a path for roles, add them. Nothing above
> depends on the role's name, only on its ARN, which you send back.

## Step 2: OpenSearch Serverless data-access policy

`data-access.json`:

```json
[{
  "Description": "SDE Curation Engine publishes curated collections to sde-web",
  "Principal": ["arn:aws:iam::<PROD_ACCOUNT_ID>:role/sde-curation-engine-prod-publisher"],
  "Rules": [
    {"ResourceType": "collection", "Resource": ["collection/<COLLECTION_NAME>"],
     "Permission": ["aoss:DescribeCollectionItems"]},
    {"ResourceType": "index", "Resource": ["index/<COLLECTION_NAME>/sde-web"],
     "Permission": ["aoss:DescribeIndex", "aoss:ReadDocument", "aoss:WriteDocument"]}
  ]
}]
```

```bash
aws opensearchserverless create-access-policy --type data \
  --name sde-curation-engine-publisher \
  --policy file://data-access.json
```

This is a **new** policy. You do not need to edit the policies that already grant the indexer or
the search API access.

## Step 3: check network access

The engine calls the collection endpoint over the public internet from AWS Fargate. It does not use
a VPC endpoint into your account. Check the collection's network policy:

```bash
aws opensearchserverless list-security-policies --type network \
  --query 'securityPolicySummaries[].name' --output text
aws opensearchserverless get-security-policy --type network --name <POLICY_NAME>
```

- **If** the rule covering `collection/<COLLECTION_NAME>` has `"AllowFromPublic": true`, there is
  nothing to do.
- **If** access is VPC-only, tell us. We will need to agree on another path, such as a VPC endpoint
  or a source-IP rule, before this can work.

## Step 4: send back

- the role ARN: `arn:aws:iam::<PROD_ACCOUNT_ID>:role/sde-curation-engine-prod-publisher`
- confirmation that the network policy allows public access (step 3)

---

## What the Curation Engine team does next (no action for you)

1. Put the ARN in SSM `/sde-curation-engine/test/prod_index_role_arn`. It currently holds a
   placeholder. `opensearch_endpoint_prod` is already set to
   `https://o2mxw7n9akk8n7o5oiqb.us-east-1.aoss.amazonaws.com`.
2. Redeploy the test stack. Its task role already has `sts:AssumeRole` on that parameter's value.
3. Publish one small collection and confirm three things:
   - the job reports `… wiped · N written`, then `N / N visible in prod`
   - a second publish reports `N wiped · N written`, and prod still holds N documents for it
   - the document counts of other collections are unchanged
   - the documents show up on the prod search front end

## Revoking access

Delete the data-access policy (`aws opensearchserverless delete-access-policy --type data --name
sde-curation-engine-publisher`) or the role. Either one cuts the engine off immediately. The engine
then fails "Index to prod" with an access error and writes nothing.

---

<details>
<summary>Reference for the engine team: failure codes shown on a prod run</summary>

Refusals before anything is deleted or written:

| error | meaning |
|---|---|
| `prod_index_unreachable` | cannot reach or authenticate to the prod collection (role not created yet, placeholder ARN, network policy) |
| `export_not_found` | the test run's export expired (30 days): re-index to test first |
| `empty_export` | the test run's export holds no documents |
| `index_not_found` | prod `sde-web` does not exist |
| `scope_filter_ineffective` | the collection filter does not isolate the collection |
| `orphaned_prefixed_docs` | documents carry this collection's id prefix under another (or no) `collection_key`: outside the wipe, and they would duplicate the fresh documents |
| `foreign_documents_in_scan` | a scan of the collection returned a document of another collection |
| `vectors_missing` | documents with no vectors at their current version in S3, the test index or prod; the URLs are listed |

After deletes started (the collection may be partial in prod; publish again):

| error | meaning |
|---|---|
| `wipe_incomplete` | deletes still failed after 3 attempts, or the collection did not read empty within `PUBLISH_WIPE_SETTLE_TIMEOUT_S`; nothing was written |
| `upsert_failed` | bulk writes still failed after 3 attempts |

Code: `sde_curation/backends/publish.py`. Stack grants: `infra/stacks/engine_stack.py`.
</details>
