# SDE Curation Engine — Top-Level Workflow

## Context
Synthesized from the whiteboard sketch (`IMG_2847.JPG`) and the requirements in
`docs/cosmos_rebuild_study.html` (R1–R11, minus all TDAMM tagging). The whiteboard
adds three things the study doesn't: scraping is done **via SSM**, state/artifacts
live in **S3**, and post-curation validation happens against **OpenSearch** (not
Sinequa). The target is a lightweight engine (study Option A + B: Python CLI/library
wrapped by a Claude skill), not a multi-user web app.

## Pipeline (6 stages, mirrors whiteboard left→right)

```
Collection → Scrape → Curate → Indexing (test) → Validation → Prod Indexing
```

### 1. Collection  (R1)
- Input: seed URL + collection metadata (display name, division, connector type).
- Engine assigns a stable `collection_id`; status = `Backlog`.
- Stored as `collection.yaml` (git-tracked) per the study's Option-B layout.

### 2. Scrape  (R2, R6)
- Generate scrape config from the collection metadata (template-driven).
- Trigger the crawl **via SSM**; poll and track status → `complete | failed`, written to S3.
- On success the raw dump (url, scraped title, text) lands in S3 → ingested as `DumpUrl`.
- Status → `Scraped`.

### 3. Curate  (R3, R4, R9)
- **Calculate deltas**: diff dump vs. curated on `title, division, doc_type`; emit
  new / modified / deleted (tombstone) `DeltaUrl` rows. Bulk set operations, no per-URL writes.
- **Apply patterns** (in this order, deterministically):
  1. Exclude / Include patterns — include always wins over exclude.
  2. Title generation — substitution engine (`{url}`, `{title}`, `{collection}`, batched xpath).
  3. Division assignment.
  4. Document type assignment.
  - Field patterns resolve by "smallest match set wins", tie → longest pattern string.
  - Idempotent apply; unapply on pattern delete falls back to next-most-specific → curated → NULL.
- **LLM assist** (whiteboard brackets title/division/doc-type as LLM): Claude drafts
  patterns from URL structure and suggests title/division/doc-type; suggestions are
  stored separately from manual values (`*_manual` overrides `*_ml`).
- Write deltas + pattern state to S3.
- **SME validation**: bulk (whole collection) or one-by-one review of the delta set via
  a generated HTML review report / sampled conversational QC.
- Status → `Curating` → on approval **promote** deltas → `CuratedUrl` (R5); status → `Curated`.

### 4. Indexing (test)  (R5, R6)
- Export curated URL list in the indexer's consumed shape:
  `{url, title, document_type, file_extension, tree_root}` (no tdamm_tag).
- Generate indexer config from templates; push to the test index.
- Status → `Config Generated`.

### 5. Validation  (R7)
- Compare **counts and titles in OpenSearch vs. the curated set**.
- Pass → proceed; fail → back to Curate with a "needs re-curation" flag (replaces the
  7-state reindexing machine).

### 6. Prod Indexing  (R5, R10)
- Promote config to production; status → `Live`.
- Notification hook (Slack/GitHub PR) on this and each prior transition; status history
  auto-logged (R7).

## Cross-cutting
- **Status machine** (6 states): `Backlog → Scraped → Curating → Curated → Config Generated → Live`,
  plus `needs_recuration` flag.
- **State store**: SQLite/Parquet locally, mirrored to S3; git supplies the audit trail.
- **Error surface**: every async step (SSM scrape, indexing) records explicit failure
  state visible to the curator — the study's pitfall #7.
- **Scale**: apply/diff/promote as bulk set operations to handle 100k-URL collections (R11).
- **Dropped**: TDAMM tagging, feedback app, EJ app, dashboards, backup tooling, 20-state workflow.

## Verification (once implemented)
- pytest suite encoding pattern semantics (include-precedence, specificity, 6-case unapply).
- End-to-end dry run on a small seed collection through all six stages against a test
  OpenSearch index; confirm count/title validation passes and status history is complete.
