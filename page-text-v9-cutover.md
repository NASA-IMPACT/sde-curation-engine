# Cutover: page text stored once (schema V9) + the crawl stops landing on disk

Two changes ship together, both about not storing or moving a page's text more than once:

- **Schema V9** moves the page text out of `dump_urls.full_text` and `curated_urls.full_text` into
  a content-addressed `page_text` table keyed by `(collection_id, content_hash)`, and **drops both
  columns**.
- **The remote crawl is no longer downloaded.** `ScrapeResult` carries a `DocumentSource`
  (`S3Documents` / `FileDocuments`) instead of a path, and `ingest_dump` streams the S3 object
  straight into the `COPY` that `replace_dump` is running.

The migration is irreversible (it drops columns holding the only copy of the text), so it needs a
snapshot and a pre-flight. Everything below was measured on the code in the working tree, and the
pre-flight was run against the live test database on 2026-09-22.

## What changes in the deployment

| | Before | After |
|---|---|---|
| page text | twice: `dump_urls.full_text` + `curated_urls.full_text` | once: `page_text`, keyed by the `content_hash` both tables already carried |
| a curated row's text | copied from the dump at promote (`text_from_dump`, `text_urls`) | follows the row's `content_hash`; no copy step |
| freeing text | rows deleted with the dump | `Database._gc_page_text` drops blobs nothing references, in the transaction that dropped the last reference |
| remote crawl | `s3.download_file` → `DATA_DIR/scrapes/<cid>.json` on EFS, read twice | streamed from S3 once, never written to this host |
| duplicate spellings | a second pass over the whole documents file | decided in SQL from the staging table's URL columns (`replace_dump(dedupe_spellings=True)`) |
| EFS | a permanent multi-GB copy of every collection's crawl | YAML + logs only |

## Effect on app functionality

**Nothing a curator does behaves differently.** The contract that mattered — a curated row serves
the text it was approved with, whatever a later crawl says — is unchanged, because a re-crawl that
changes a page gives it a new hash and the old blob is kept until no row points at it.

What does change, all of it invisible in the UI:

- **Two pages whose text differs only in whitespace now share one blob.** The hash is of the
  normalised text, so this was already true of the hash; now it is true of the stored bytes. The
  raw spacing kept is whichever was ingested first. The index is the only reader and does not care.
- **Reads resolve the text through a join** (`_page_text()` subquery, or `_TEXT_JOIN` for the LLM
  queries) instead of a column. `page_text`'s primary key makes it one index lookup per row, and
  every caller either paginates or streams.
- **The LLM path is unaffected in shape.** It never read the crawl file — `iter_deltas_for_llm`
  reads `delta_urls → dump_urls → page_text` in keyset-paginated chunks of 200 and sends the whole
  page text, uncut. Only the column it reads through changed.
- **`replace_curated` lost `text_from_dump` and `text_urls`.** One caller (`CurationService`)
  updated.
- **A broken S3 stream fails the scrape job and changes nothing.** `replace_dump` is a single
  transaction: the previous dump and its blobs survive, and the crawl is still in S3, so the job is
  re-runnable.
- **The importer re-homes SQLite-era text** (`_copy_with_text`) and its backfill now copies the
  dump's *hash* onto curated rows rather than the text.

## Effect on memory and storage

Measured on this laptop (M4 Max, Dockerised PostgreSQL), ingesting a synthetic crawl with a
natural-language text distribution:

| Crawl | Total | stream → COPY | duplicate pass | `page_text` + `dump_urls` | Peak RSS | DB after |
|---|---|---|---|---|---|---|
| 6.88 GB / 1M pages | 165.5 s | 117.1 s | 2.5 s | 46.0 s | **1.10 GB** | 4.36 GB |
| 6.59 GB / 100K pages | 158.3 s | 144.6 s | 0.2 s | 13.5 s | **0.29 GB** | 3.10 GB |

- **~24 s per GB, near enough independent of page count** (24.1 vs 24.0 s/GB). The work moves
  between phases; the total tracks bytes.
- **Memory is flat in bytes and grows only with row count** — 1.1 GB at a million pages, from the
  `seen` URL set and the `seq/url/final_url` read-back for the duplicate pass. This is the thing
  that used to `MemoryError` on a 2 GB task.
- **Most of the time is PostgreSQL, not Python.** With no database at all: `ijson` parse alone
  4.6 s for 6.9 GB (1.5 GB/s, C backend); plus `DumpUrl` construction and sha256, 30.7 s. So ~135 s
  of the 165 s is the staging COPY and the TOAST-compressed insert.
- **Storage roughly halves** for any collection whose curated rows were approved with the text the
  dump still holds — which is nearly all of them. EFS stops accumulating a copy of every crawl.
- **Correction from the real crawl (dev, 2026-09-22): memory was *not* flat in bytes when pages are
  big.** The synthetic crawls above had small pages. ascl.net's 6.7 GB crawl has thousands of ~1 MB
  pages, and chunks of a fixed 500 pages made ~500 MB chunks: the engine peaked at ~5 GB (6.6 GB
  high-water mark) and kept 2.8 GB resident afterwards. The working tree fixes both (1 MB byte cap
  per chunk; connection recycle + `malloc_trim` after the ingest) — see *Still open* and
  `docs/architecture.md` §1.

## Measured timings, 100K-URL collection (688 MB crawl)

One curator, through the real HTTP API, same machine:

| Stage | Time |
|---|---|
| scrape + ingest (streamed) | 14.8 s |
| recompute (Start curating) | 2.6 s |
| dashboard | 0.04 s |
| collection page, curate tab | 1.2 s |
| delta API, 100 rows / offset 50K / search | 0.06 / 0.14 / 0.11 s |
| dump tab / rules tab | 0.31 / 0.23 s |
| add exclude glob (~11K URLs) | 2.7 s |
| add title glob `*` (rewrites every row) | 4.7 s |
| delete that glob | 3.0 s |
| inline title edit, one URL | 2.7 s |
| Suggest metadata (**fake** LLM, 89K rows) | 12.2 s |
| accept all AI suggestions (178K rules) | 13.1 s |
| rules tab with 178K per-URL rules | 0.46 s |
| promote (89K rows) | 4.0 s |
| curated tab / search | 0.40 / 0.22 s |
| export → JSONL (89K docs, 607 MB) | 5.9 s (103 MB/s) |
| re-crawl: ingest + recompute | 12.9 s + 5.7 s |

Everything interactive is under 5 s; every read is under 1.5 s.

**The LLM is the only stage not measured in seconds.** The 12.2 s above is engine + database with
the fake provider. The real cost is API-bound and unchanged by any of this work:

```
89,000 calls ÷ llm_workers (16) × per-call latency
  5 s/call → 7.7 h      10 s/call → 15.4 h      20 s/call → 30.9 h
```

Full page text at ~1.7K tokens a page puts a 100K collection at roughly half a day to a day and a
half. `llm_workers` is the only lever, bounded by the provider's rate limit, and there is still no
cross-job limiter, so concurrent curators share that ceiling. **A real per-call latency has not been
measured** — it is the single most valuable number still missing.

These are laptop numbers. Test is a Fargate x86 4 vCPU task against `db.m6i.large` on gp3 at
125 MB/s baseline: expect the database-heavy rows (ingest, promote, export, glob rules) to be
**2–4× slower**; the reads should stay sub-second.

## The migration's one real risk

**V9 trusts the hashes already in the tables.** It inserts `(content_hash, full_text)` into
`page_text` with `ON CONFLICT DO NOTHING`, dump rows first, then curated. If a row's stored hash
does not fingerprint its stored text, *and* another row already owns that hash with different text,
the row silently adopts the other text — and `DROP COLUMN` makes the original unrecoverable.

Verified in a scratch database: a curated row holding `'new body'` under the hash of `'old body'`
served `'old body'` after the migration.

Rows with text but no hash get one computed in SQL; whitespace-only text becomes NULL, which is
what `content_hash` already calls empty.

### Pre-flight (read-only)

Run this against the target database **before deploying**. The second query is decisive: zero rows
means no row can change text.

```sql
-- 1. rows whose stored hash does not fingerprint their stored text
SELECT 'dump_urls' AS t, count(*) FROM dump_urls
 WHERE full_text IS NOT NULL AND content_hash IS NOT NULL
   AND content_hash <> encode(sha256(convert_to(btrim(regexp_replace(full_text,'\s+',' ','g')),'UTF8')),'hex')
UNION ALL
SELECT 'curated_urls', count(*) FROM curated_urls
 WHERE full_text IS NOT NULL AND content_hash IS NOT NULL
   AND content_hash <> encode(sha256(convert_to(btrim(regexp_replace(full_text,'\s+',' ','g')),'UTF8')),'hex');

-- 2. DECISIVE: a hash claimed by two different texts. Empty = safe.
SELECT collection_id, content_hash, count(DISTINCT norm) AS distinct_texts
FROM (
  SELECT collection_id, content_hash, btrim(regexp_replace(full_text,'\s+',' ','g')) AS norm
    FROM dump_urls WHERE content_hash IS NOT NULL AND full_text IS NOT NULL
  UNION ALL
  SELECT collection_id, content_hash, btrim(regexp_replace(full_text,'\s+',' ','g'))
    FROM curated_urls WHERE content_hash IS NOT NULL AND full_text IS NOT NULL
) x GROUP BY 1,2 HAVING count(DISTINCT norm) > 1;

-- 3. rows with text but no hash (V9 computes one in SQL)
SELECT (SELECT count(*) FROM dump_urls    WHERE full_text IS NOT NULL AND content_hash IS NULL),
       (SELECT count(*) FROM curated_urls WHERE full_text IS NOT NULL AND content_hash IS NULL);
```

**Query 1 over-reports.** It flagged 32 dump rows on test; all 32 were then checked against the
app's own `engine.text.content_hash` and **all 32 agreed with the stored hash**. The cause is
control characters 0x1c–0x1f in those pages: Python's `str.split()` treats them as whitespace,
PostgreSQL's `\s` does not. Always confirm a query-1 hit against the app's hasher before believing
it. Query 2 is not affected by this — it compares texts, not hashes.

### Result on test, 2026-09-22

| | |
|---|---|
| schema version | 8 (V9 not yet applied) |
| collections / dump rows / curated rows | 31 / 145,620 / 839 |
| database size | 533 MB (`dump_urls` 488 MB, `curated_urls` 4.9 MB) |
| RDS | `db.m6i.large`, 20 GB allocated, **100 GB ceiling** |
| query 1 | 32 dump rows — all false positives (see above) |
| **query 2** | **empty — no row can change text** |
| query 3 | 0, 0 — the SQL hash backfill does nothing here |

At 533 MB the migration needs trivial headroom and will finish in seconds, so neither the storage
ceiling nor the 120 s health-check grace is a concern on test *today*. Both become real once a
multi-GB crawl lands. Note the deployed ceiling is still 100 GB — the working tree's raise of
`db_max_storage_gib` to 200 has not shipped.

**Re-run on test, 2026-09-22 afternoon** — in a session with `default_transaction_read_only=on`,
`statement_timeout` 5 min and `lock_timeout` 2 s, so nothing could be written or held up. Same
verdict:

| | |
|---|---|
| task | rev 13, 4 vCPU / 16 GB |
| schema version | 8 — `full_text` still on `dump_urls` and `curated_urls` |
| collections / dump rows / curated rows | 31 / 145,620 / 840 |
| database size | 534 MB; the two text tables 493 MB |
| query 1 | 32 dump rows, 0 curated — all 32 agree with `engine.text.content_hash`: **0 real mismatches** |
| **query 2** | **empty** (44 s) |
| query 3 | 0, 0 |

### Result on dev, 2026-09-22

Dev took V9 without the pre-flight or a manual snapshot (it held ~90 dump rows beforehand; the
only earlier backup is the 08:31Z automated snapshot). Checked afterwards:

| | |
|---|---|
| deploy | task def rev 27 (4 vCPU / 16 GB), rollout COMPLETED, **no circuit-breaker rollback** |
| migration | V9 recorded 17:20:56Z; container start → serving ≈ 21 s, inside the 120 s grace |
| logs since | no 5xx, no `UndefinedColumn`, no application tracebacks |
| `full_text` | only on `page_text` |
| integrity | 0 dump / 0 curated rows whose hash has no `page_text` row; 0 rows without a hash; 0 orphan blobs |
| curated text | every included curated row resolves to non-empty text (aurorasaurus 6, hytes 43, techport 23) |
| workflow on V9 | scrape, AI patterns, AI metadata and test index all succeeded (hytes, techport); ascl.net's 6.7 GB crawl ingested (45,024 read → 22,324 kept) |

The database is reachable only from inside the VPC. Run the pre-flight through ECS Exec on the
running task, which already holds the credentials (needs `session-manager-plugin`; the image has no
`psql`, so use python + psycopg, and keep the command short — a long one can end with
`Cannot perform start session: EOF` before the last print):

```bash
export AWS_PROFILE=smce-test AWS_REGION=us-east-1
C=sde-curation-engine-test
T=$(aws ecs list-tasks --cluster $C --service-name $C --query 'taskArns[0]' --output text)
aws ecs execute-command --cluster $C --task "$T" --container engine --interactive \
  --command "python -c \"<one-line script using psycopg and os.environ['DB_HOST'] etc>\""
```

## Steps

Status for **test**, 2026-09-22:

| Step | State |
|---|---|
| 1. Pre-flight | ✅ done twice, query 2 empty |
| 2. Manual snapshot | ⬜ to do, right before the deploy |
| 3. Raise storage ceiling | ➖ not needed: 493 MB of text against a 100 GB ceiling |
| 4. Announce window, check Jobs page | ⬜ to do |
| 5. Deploy | ⬜ to do |
| 6. Watch first boot | ⬜ to do |
| 7. Verify | ⬜ to do |
| 8. `make db-compact` | ⬜ later, in a maintenance window |

Dev: deployed and verified (see *Result on dev*); step 8 still open there too.

1. **Run the pre-flight** (above) against the environment you are about to deploy. Query 2 must be
   empty. If it is not, stop: those rows will change text, and the change is unrecoverable once the
   columns are dropped.

2. **Take a manual snapshot.** It ignores `db_backup_days`, so it survives as your checkpoint.
   Test is single-AZ, so the snapshot briefly suspends I/O — do it when nobody is mid-job.
   ```bash
   export AWS_PROFILE=smce-test AWS_REGION=us-east-1
   aws rds create-db-snapshot \
     --db-instance-identifier sde-curation-engine-test-db \
     --db-snapshot-identifier sde-curation-engine-test-pre-v9-2026-09-22
   aws rds wait db-snapshot-available \
     --db-snapshot-identifier sde-curation-engine-test-pre-v9-2026-09-22
   ```
   The identifier cannot be reused — bump the date suffix on a later attempt. Delete the snapshot
   once V9 has been live long enough to trust; it is billed as storage until then.

3. **Raise the storage ceiling first, as its own deploy,** if the target holds any multi-GB
   collection. V9 extracts the text into `page_text` and NULLs the old columns in one transaction,
   so it transiently needs room for one extra copy of every collection's page text, plus WAL.
   Deploy the `db_max_storage_gib` change and let it apply *before* the migration runs. (Not needed
   for test at 533 MB.)

4. **Announce a window and check the Jobs page.** A deploy replaces the single task, so in-flight
   scrape/index jobs are marked failed regardless.

5. **Deploy.** Push to the environment branch, or `make deploy ENV=test PROFILE=smce-test`.
   Migrations run at boot, in one transaction under an advisory lock, before the app serves — so
   concurrent boots serialise and a failure rolls back cleanly with nothing lost.

6. **Watch the first boot.** `make logs ENV=test PROFILE=smce-test`. Confirm the task reaches
   healthy and `/health` is `"db": "ok"`. If the migration outruns the 120 s health-check grace the
   task is killed mid-migration: the transaction rolls back, but the deployment can loop — see
   Rollback.

7. **Verify.** Open a curated collection and check a row still shows its text length; run an export
   to a test index and confirm the document count matches `curated_count`.

8. **Reclaim the space, later.** `DROP COLUMN` does not free the TOAST the column pointed at. The
   NULLed rows become dead and autovacuum returns them to each table's free space, which is what
   stops the volume growing; to hand space back to the OS, run `make db-compact` (VACUUM FULL,
   ACCESS EXCLUSIVE lock, service stopped) in a maintenance window.

## Rollback

**There is a trap here.** The service runs with `circuit_breaker(rollback=True)`. If the new task
commits V9 and then fails its health check for any unrelated reason, ECS reverts to the *previous*
task definition — whose code queries `full_text`, a column that no longer exists. That is a hard
outage, not a clean rollback.

So:

- **Roll forward, not back.** If the new task is unhealthy after a successful migration, fix and
  redeploy rather than letting the old image serve.
- **The snapshot is not a revert button.** `restore-db-instance-from-db-snapshot` always creates a
  *new* instance with a *new* endpoint, and `DB_HOST` comes from the CDK stack
  (`db.instance_endpoint.hostname`). Recovering means restoring alongside and then renaming the
  instances or pointing the stack at the restored one — a maintenance-window operation. What the
  snapshot buys is that the dropped text still exists somewhere.
- **Before the migration commits**, rollback is free: the transaction is atomic, so a killed task
  leaves the database exactly as it was.

## Still open

- **Migrations run at boot in the serving process.** This is the first one big enough for the
  120 s ALB health-check grace to matter. Running V9 out-of-band, before rolling the image, would
  decouple the schema change from the deploy.
- **A real LLM per-call latency** (see above) — everything else in the workflow is measured.
- **Ingest memory fixes, not yet committed or deployed.** ascl.net on dev peaked at ~5 GB (6.6 GB
  high-water mark) and left 2.8 GB resident after the job. The working tree now caps each ingest
  chunk at 500 pages *or* 1 MB of text, replaces the pooled DB connections after the COPY, and calls
  `malloc_trim`. On a local ascl.net-shaped crawl: peak 1.7 GB → 340 MB, after-job 445 MB → 90 MB,
  ingest 90 s → 70 s. Decide whether they ride along with the test deploy (5.) or follow it.
  `docs/architecture.md` (section 1, *The text paths*) has the breakdown.

Closed:

- ~~`docs/architecture.md` is stale~~ — updated 2026-09-22: the streamed ingest (chunk caps,
  connection recycle), the streamed export, V9's `page_text`, promote naming text by hash, and the
  duplicate-links count.
