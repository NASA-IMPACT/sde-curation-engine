# sde-curation-engine

Lightweight FastAPI app that drives the SDE curation pipeline:

```
1 Backlog → 2 Scraped → 3 Curating → 4 Curated → 5 Test index → 6 Live
```

It wraps two existing repos — `../sde-crawl4ai-scraper` (crawling) and
`../sde-api-scrapers` (WEB_COSMOS indexing) — behind a small web UI with a live dashboard,
a clickable pipeline stepper, and a curation grid. Plan and phase status: `docs/plan.md`;
workflow background: `docs/workflow.md`.

**Status:** all six phases built — collections, scraping, curation, promotion, LLM assist, S3 export +
WEB_COSMOS test indexing, validation gate, prod indexing, notifications.

## Quick start
```bash
cp .env.example .env      # sibling repo paths, AWS values, OPENAI_API_KEY (or LLM_PROVIDER=fake)
make install              # uv sync if uv is installed, else python3.13 venv + pip -r requirements-dev.txt
make run                  # http://localhost:8080   (8000 is taken by sde-elastic-wrapper)
make test                 # 91 tests, incl. a state-matrix that fires every action in every status
make lint
```

Crawler prerequisite (one-off): the local scrape backend runs `run.py` from
`../sde-crawl4ai-scraper` with its own Python 3.11 venv:
```bash
cd ../sde-crawl4ai-scraper
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m playwright install chromium
```
Point `CRAWLER_PYTHON` in `.env` elsewhere if you use a different interpreter.

**Dependencies** — `pyproject.toml` + `uv.lock` (and the same pair in `infra/`) are the source of
truth. `requirements.txt` / `requirements-dev.txt` are exported from the locks and committed, so a
developer without uv, CI, and the Docker image all install the exact same versions with pip.
Changing a dependency: edit `pyproject.toml`, `uv lock`, `make requirements`, commit all four files
(CI fails if the exports are stale). Never edit `requirements*.txt` by hand.

## Using it
1. **Dashboard** (`/`): add a collection (seed URL, name, division — `General` until you assign one — max pages). Each row shows
   status, counts (dump URLs / delta URLs / curated URLs), last job, and **one button — the next step**.
2. **Collection workbench** (`/collections/{id}`) — one page, a sticky header, the pipeline stepper always on top, and under steps Curating / Curated seven tabs (Overview · Dump URLs · Curate · Rules · Delta URLs · Curated URLs · Activity; Start curating lands on Dump URLs):
   - **Header**: name, seed link, status badge (icon + label), ⚠ *needs re-curation* / *needs re-indexing* / *prod not validated*, running-job
     chip with **cancel**, and the one **Next** action for the current step. The counts
     (**Dump URLs · Rules · Delta URLs · Curated URLs**) sit on the tab row.
   - **Pipeline stepper**: each step's panel shows what it did, its primary action, and a redo where
     sensible, with details, last job and *Advanced* (re-scrape, manual status). Under the
     other steps this panel is all there is; under Curating / Curated it is the **Overview** tab.
   - **Dump URLs** (first tab; raw crawl: title, type, depth, text size, state) · **Delta URLs**
     (kind badge, scraped → effective title, division, type, exclude — all editable inline; AI badges)
     · **Curated** (read-only approved set with a **Curate ↗** jump when a delta exists). Search,
     kind / excluded / division / type filters, page size, paging, ⇩ CSV of the filtered rows.
     Hover a field to see *which pattern* set it.
   - **Curate**: ✨ suggestions with Accept/Reject, add a rule by hand, Recompute and Promote. The
     Exclusions and Metadata lists show their first 50 rows in place; **⤢ Expand**
     (`?tab=curate&focus=exclusions|metadata`) opens one on its own paginated page, **⤡ Collapse** goes back.
   - **Excludes are rules, not deltas**: an excluded dump URL has no delta row, and a curated URL an
     exclude rule newly matches is flagged excluded in place (the next index run drops it; a
     Test index / Live collection drops back to Curated). Only the way back in (include, or deleting
     the rule) is a delta to promote. Excluded URLs show under Dump URLs; exclude rules count matches over the dump.
   - **Rules** (right after Curate): every rule in force, the add-by-hand forms, and match counts
     over (and linking to) the delta URLs while reviewing and the curated URLs once promoted.
   - **Activity**: all jobs and the status history.
   Old `/collections/{id}/curate` links (and `set=deltas`) redirect into the Delta URLs tab.
   Step panel actions:

   | Step | Panel actions |
   |---|---|
   | 1 Backlog | Scrape / Re-scrape |
   | 2 Scraped | Start curating (computes the delta URLs → curation page), Re-scrape |
   | 3 Curating | Open curation, Recompute delta URLs, Promote → curated (or *Mark curated* when there are no delta URLs) |
   | 4 Curated | **Index to test** (export + dispatch), Review / re-curate |
   | 5 Test index | run summary (exported, indexed/changed/deleted, validation pass/fail + how it was validated), Re-index, **Re-validate** |
   | 6 Live | **Index to prod** (only after a validated test run), prod run summary |

   A running job shows a spinner, live doc counts and a **Cancel** button. *Advanced* (collapsed)
   holds Re-scrape, a manual status override, the index key, and — for admins — Delete collection.
3. **User manual** (`/manual`, also in the ☰ menu): the illustrated curator's handbook — quick path,
   screen-by-screen walkthrough, rule semantics, jobs, parallel work, quirks. Template
   `sde_curation/web/templates/manual.html`, screenshots in `static/manual/`.
4. Typical loop: **Scrape → Start curating → (URLs › Delta URLs: fix rows; Patterns & AI: add rules /
   accept suggestions) → Promote**. Every inline edit is an exact-URL pattern, so everything is
   visible and reversible in Patterns & AI.

### LLM assist
Two buttons on the Patterns & AI tab, both background jobs that **never change effective values**.
The page refreshes itself when the job finishes (SSE, with a 4 s poll while running), so results
appear without a manual reload:
- **✨ Suggest patterns** — **exclude globs only**: which pages must never be searchable. First the
  **global exclude list** (`sde_curation/data/global_excludes.yaml`, entries tagged `source: sme`
  or `cosmos`) is applied deterministically: every glob that matches at least one crawled URL becomes
  a pending suggestion tagged `global`, with its match count. Then the model sees the **pending,
  included** URLs (URL + scraped title only, http/https and trailing-slash twins collapsed, sorted by
  path) — on a first pass that is the whole crawl, after a re-crawl only what changed — in
  batches of `LLM_PATTERN_BATCH_URLS` (default 1000), one call per batch through the worker pool,
  and drafts globs with a rationale; a glob is kept only if it matches a URL in the batch the model
  saw. Match counts are taken over the whole crawl. Rows merge by glob across batches. The button
  says how many URLs and calls that is and is disabled until "Start curating" has run (409 on the
  API). **Accept** turns a row into a real `exclude` rule (recompute runs) — the URLs are out of
  scope, so they never reach the export or the index, and they are never sent for metadata;
  **Edit** lets the curator change the glob first (`POST …/suggestions/{id}/accept` with
  `{"match": …}`; the rule is tagged `llm_edited`); **Reject**
  dismisses it. Include / title / division / type rules remain curator tools (the add-rule form).
- **✨ Suggest metadata** — **one call per included delta URL with the full page text**, up to
  `LLM_WORKERS` (default 16) in flight. The text is never cut: an accurate title needs the whole
  page, and the default model (`gpt-5.6-luna`, 1.05M-token window) takes any page whole; a page
  beyond the model's window fails that one call (its error recorded on the row, retried on the next run)
  rather than being guessed from a slice. **Every page gets every field** — a title, a division and a
  document type — each with a **confidence** (`high` = explicit in the text, `medium` = strong
  inference, `low` = guess): where the model is unsure it gives its best value at low confidence
  instead of leaving the field blank (an empty title counts as a failed call, retried next run; a row
  whose answer left a field blank with no rule setting it is asked again).
  These show as `AI:` badges with the confidence next to each cell in URLs › Delta URLs (filter by
  confidence; the review bar counts them); **✓** accepts (creates an exact-URL pattern, i.e. a
  manual override), **✕** dismisses. Curate › Metadata filters the review table by confidence
  (all / high / medium / low) and by field, and **✓ accept these / ✕ reject these** decide exactly
  the suggestions the filter names, on every page. With *any field* a row is listed when any of its
  title / division / type suggestions has that confidence; picking a field narrows it to that one.
  A listed row still shows all its suggestions, and its **✓ row** accepts all of them, filter or not.
- **`General` is a placeholder, not a division** — it is what a collection carries until a curator
  assigns one, and it is available everywhere a division can be set (the add-collection form, the
  collection's division, a `division` rule, a per-URL cell). The single guard is at the other end:
  **promote refuses a delta URL whose effective division is General**, exactly as it refuses a blank
  one, so nothing unclassified reaches the curated set or the index. Such a row is marked *not
  promotable* in its cell, counts under "without a division" (with its own sub-count), and is listed
  by `?missing=true`. The model's answer schema is the one place General is absent: suggesting it
  could only ever produce a row that cannot be promoted.
- **A division the curator assigned is not a question for the model** — leave a collection on
  General and the model answers a division per page, as above. Assign one (at creation, or later
  under Overview › Details → `POST …/division`) and it becomes the value of every URL no division
  rule decides — including rows already curated, which become modified deltas so the change reaches
  the index on the next promote. Suggest metadata then asks for the title and document type only:
  the division rides along as context (`collection_division`), the answer schema has no division
  field, and no division suggestion reaches the review table — assigning one also drops the
  suggestions an earlier run left, so nothing can be accepted over it. A `division` rule still
  overrides it on the URLs it matches. Putting the collection **back to General** hands the division
  to the model again: rows classified while it was assigned are marked `division_skipped`, so they
  owe a division nobody was asked for and the next **Suggest metadata** picks up exactly those — not
  a full re-classification of every field of every page.
- **Accept all never overwrites a rule you wrote** — a field whose winning rule is yours (`sme`, or
  an AI suggestion you edited before accepting) drops out of the accept-all buttons: accepting in
  bulk would write a newer exact-URL rule straight over the rule you just typed. The buttons say how
  many they hold back, the cells are marked *your rule · not in Accept all*, and the row's own **✓**
  is never disabled — it still takes the AI's answer for a row you had ruled on. **✕ reject** decides
  exactly what it says, held back or not.
- **No blank metadata in the curated set** — Promote (all, or a selection) is refused with a 409
  while any delta URL it would write has no title, division or document type as an effective value
  (a pending suggestion is not a value until accepted; removals and excluded rows never count). The
  Promote card and the Delta URLs table show how many and which field, and
  `?missing=true` lists those rows. Answers are written as they arrive: **cancel keeps what
  finished**, one bad URL is counted as failed and never fails the job, and re-running classifies
  only what is missing — plus any URL whose text changed since it was last classified. The job
  result shows classified / failed counts and tokens in / out.
- **Duplicate titles** — two included pages of a collection are duplicates when they will be
  indexed under the same title (ignoring case and whitespace) **and** the same document type; the
  same title with different types does not count. Each page is taken as it would be after
  promote: a pending AI suggestion as if accepted, else the effective value (the title falls back to
  the scraped title); curated rows count too. Duplicates get a **⚠ same title + type ×N** chip in
  the Delta / Curated URLs tables (filter `?dup=title`), a count on the Curate › Metadata card and a
  warning above Promote. By default Suggest metadata only flags them, with the titles as generated,
  so an SME can fix a handful by hand. **✨ Regenerate duplicate titles** sends every delta URL of a duplicate
  (including ones set by rules) back to the model for a **new title only** — one call per page,
  full text, with up to 30 of the other URLs; the document type is left alone. The answer replaces
  the AI title suggestion (never applied until accepted) and the title the page shared is kept on
  the row (`title_ai_before`): it shows as **⚠ was same title + type “…”** with **✎ original** to
  edit it instead, and `?dup=retitled` lists those rows. In a group, only pages with a pending AI
  title are re-asked when any has one (the rest are sent as settled); otherwise every delta URL in
  the group is. `LLM_DEDUPE_TITLES=true` makes Suggest metadata run that pass by itself at the end
  (a failure there never fails the classification: the duplicates simply stay flagged).

Provider is pluggable (`LLM_PROVIDER`): `openai` (default model `gpt-5.6-luna`; any
OpenAI-compatible endpoint via `OPENAI_BASE_URL`; structured outputs parsed straight into Pydantic
models — a malformed reply fails the job and writes nothing) or `fake` (deterministic heuristics,
used in tests and demos; no key needed). Adding a provider = one module implementing
`complete(system, user, schema, model=None) -> Completion` + one line in `llm/base.py`. Prompts
live in `llm/tasks.py` (they ask for host-agnostic globs like `*/login*` so http/https variants
are covered together). The worker pool (`llm/pool.py`) retries nothing itself — the OpenAI client
retries 429 / 5xx / timeouts `LLM_MAX_RETRIES` times with backoff — but it keeps going past
per-URL failures and aborts only after ten consecutive non-retryable errors (bad key, bad model).

**The curated set is self-contained**: a promote copies each page's current dump text onto its
curated row (`curated_urls.full_text`, copied inside PostgreSQL) along with the hash of that text,
and **Index** exports the curated rows alone — the dump is never consulted at export time. So the
approved set stays exportable exactly as approved even after a later crawl has replaced the dump,
and the next cycle diffs the new crawl against it as the source of truth. Rows promoted before the
engine kept text (the migration backfills them from the dump, which is what the export shipped
for them anyway) show an empty *Text* size on the Curated URLs table until the next promote.

**Content-aware deltas**: every crawled page gets a `content_hash` (sha256 of its
whitespace-normalised text) at ingest; a promote carries the current hash onto the curated rows.
On the next re-scrape a page whose text changed shows as *modified* with a **text changed** badge
(filter: *text changed since promotion*) even when its title and metadata did not move, and
Suggest metadata re-classifies it. Rows promoted before hashing existed have no hash and compare as
unchanged, so the first run after an upgrade does not flag everything.

**URL identity**: a page is the same page under `http` and `https`, with or without a trailing
slash, with or without a `#fragment` (`engine/urls.py` `canonical_key`: host + path + query).
Dump and curated rows are paired by exact string first, then by that key, so a site moving to
https (or a crawler that now drops the slash) produces one *modified* delta per page with a
**renamed** badge and `renamed_from` (filter: *URL spelling changed*), not a new row plus a
removal; promote moves the row and its metadata. Exact-URL rules (per-URL edits, accepted AI
suggestions) match by the same key, so a title or exclusion set under one spelling follows the
page; a per-URL edit under a new spelling replaces the rule written under the old one.

**Crawl failures are not deletions**: the scrape also ingests the crawler's failures log
(`dump_failures`: URL, reason, HTTP status) and whether the crawl stopped at its page cap
(`collections.last_crawl_capped`). A curated URL missing from the dump becomes a *removed* delta
only when the crawl is evidence it is gone — HTTP 404/410, or never met in a complete crawl
(the delta says which). A URL the crawler tried and could not fetch (403, rate limit, timeout,
challenge page, empty extract …) stays in the curated set, flagged **kept** with the reason
(`curated_urls.crawl_failure`; filter *not fetched by the last crawl*); when the crawl was
capped, every unmet curated URL stays too (`not_visited`). Kept rows keep the text the index
holds for them and export as before; the flag comes down when a crawl fetches the page again.
The Curate tab counts them ("N kept") and explains why. Dumps ingested before this landed have
no failures log, so their next recompute treats absence as before (removed).

### Indexing (Phase 5)
`{key}` is the collection's `collection_key` in the web index: **the collection name, slugified**
(`slugify(name, separator="_")` — "NASA Applied Sciences" → `nasa_applied_sciences`), the same rule
COSMOS derives its `config_folder` with, because the indexer keys every document on it. The
engine's own `collection_id` (from the seed host) is never used as the key. The first index run
pins the key it used on the collection, so renaming it later cannot move it to a second
collection; `POST …/index-key` sets it by hand when a collection's folder does not follow from its
current name.

**Index to test** exports the curated, non-excluded URLs, each with the text it was approved
with, as the indexer's contract —
`s3://$COSMOS_INDEX_BUCKET/curated_collections/{key}/{run_id}/documents.jsonl` then
`manifest.json` (written last = "export complete") — and dispatches
`api_scraper.py --source WEB_COSMOS --collection {key} --run-id {run_id} --target test` from
`../sde-api-scrapers`, either as a **local subprocess** (`INDEX_BACKEND=local`, needs the
OpenSearch/SageMaker env in `.env`) or as an **ECS Fargate task** (`INDEX_BACKEND=ecs`,
`ecs:RunTask` on `web_cosmos-scraper-{env}` with the command override; assumes
`INDEXING_DISPATCH_ROLE_ARN` when set, else ambient credentials). Completion is always read from
`index_runs/{key}/{run_id}/status.json` (+ `validation.json` for test) that the indexer writes last;
a stopped task with no status, or `INDEX_STALL_TIMEOUT_S`, fails the run explicitly. Success moves
the collection to `config_generated`; every run is kept in `index_runs` (see the step-5 panel).
### Validation gate and prod (Phase 6)
The indexer validates in-process right after its bulk upsert — before OpenSearch Serverless has
refreshed — so on any run that wrote something its `validation.json` reads `0/N` (reproduced: the
same export re-run a minute later reads `13/13`). The engine therefore does **not** trust that file.
After a test run succeeds it waits `VALIDATION_DELAY_S` (30 s) and validates itself. The refresh
is not a fixed delay (a 10-document run has read `6/10` at 48 s and `10/10` at 2½ min), so the
direct check is repeated every `VALIDATION_POLL_INTERVAL_S` (15 s) until it passes or
`VALIDATION_TIMEOUT_S` (10 min) has elapsed; only then does a short count fail the gate. The job
row shows `n/N visible so far · waiting …s for OpenSearch to refresh` while it polls.
1. **Direct** (fast): a SigV4 query of the target index for `collection_key`, comparing counts and
   titles exactly like the indexer's `web/validate.py`. Needs `OPENSEARCH_ENDPOINT_TEST/PROD` and
   AOSS **data access** for the engine's principal — or `VALIDATION_ASSUME_ROLE_ARN` naming a role
   that already has it (e.g. `indexing-helper-role`).
2. **Fallback** on 403 / no endpoint: logs "no AOSS data access", deletes the stale
   `status.json`/`validation.json`, and dispatches a **second pass** of the same export
   (`changed: 0`, nothing re-vectorised) purely to get a fresh `validation.json` from the indexer.

Pass (`count_matches` and titles ≥ `VALIDATION_TITLE_MATCH_THRESHOLD`, default 0.99) → status
`config_generated` with **Index to prod** enabled. Fail → back to `curated` with ⚠ *needs
re-indexing* and the mismatches listed (a failed index is not a curation problem, so the re-curation
flag stays down; the chip is read off the latest test run and only a passing run clears it). **Re-validate** re-runs the check on demand. A prod run
(`?target=prod`) is refused until the latest test run passed.

**Index to prod publishes vectors, it does not re-index.** The prod job takes the export of the
latest validated test run and, for every document, the newest record at the same `version` from
`s3://COSMOS_INDEX_BUCKET/vectorized/<key>/*/` (the indexer only vectorizes changed documents, so
they are spread over runs), falling back to the test index for anything S3 lacks. It upserts them
into `OPENSEARCH_ENDPOINT_PROD` / `WEB_INDEX_NAME` (existing copies updated in place, never
duplicated), stamped `modified_date` = the publish time (`2024-08-22 21:08:32` format, UTC, one stamp per publish —
never the date the test run put on the vectors; unchanged documents are not rewritten and keep theirs), then **deletes** the collection's prod documents that are no
longer curated: a real removal, of every document under the `collection_key` the export does not hold,
including ones from before the indexer (no `version`) and ones an earlier publish had hidden. The indexer's guards are ported and checked before anything is written, and removals
never follow a failed or incomplete write. The engine then runs **the same gate against prod**
(delay, direct poll until visible or `VALIDATION_TIMEOUT_S`, counts equal and titles ≥ threshold):
pass → `live`; fail → back to `config_generated` (the written documents stay in prod). Prod has no
indexer to fall back to, so a check that cannot run (403 / no endpoint) fails the job rather than
counting the publish as live. Prod never raises *needs re-curation* (nothing curated is wrong): the UI
shows **⚠ prod not validated**, read off the latest prod run, until a prod check passes.
**Re-validate prod** re-runs the check on demand. In SMCE test the write goes through `PROD_INDEX_ROLE_ARN`, a
role in the prod account: see [docs/prod-index-access.md](docs/prod-index-access.md).
Every status transition posts to `NOTIFY_WEBHOOK_URL` (Slack-compatible `{"text": …}` with a link
built from `PUBLIC_BASE_URL`); failures to notify never block a transition.

**Deploying (AWS CDK + GitHub Actions)**: `infra/` holds a Python CDK app that runs the engine as
one ECS Fargate task with RDS PostgreSQL as its state store (per-collection YAML and logs on
EFS) behind an ALB and CloudFront (HTTPS + WAF), wired to the
crawler over SSM and the WEB_COSMOS indexer over `ecs:RunTask`, with its own read-only AOSS
data-access policy for direct validation. Pushing to `dev` / `test` / `prod` deploys that
environment via `.github/workflows/deploy.yml` (branch = environment = AWS account, OIDC role per
account, same model as `sde-api-scrapers`); pull requests only run tests. Account-specific values
(instance ids, buckets, endpoints, role ARNs) are not in git — they live in SSM Parameter Store per
environment and are resolved at deploy time; API keys and the login password are Secrets Manager
secrets set once per environment. Step-by-step runbook (one-time account setup, first deploy,
verification): `docs/deploy-dev.md`; manual end-to-end test plan: `docs/e2e-test.md`; stack reference and the exact IAM the task role gets:
`infra/README.md`. `APP_PASSWORD` turns on login and seeds the first `admin` account with that value (only while the
users table is empty); admins create accounts at `/users`, everyone changes their own password at
`/account`. Roles: admin (users), curator (everything else). Every action is
attributed to the signed-in user: status history, patterns, jobs, an append-only audit trail
(Activity tab, `/api/collections/{id}/audit`) and the per-collection `collection.yaml` /
`patterns.yaml`. Locally it is off, so the UI and tests run as `anonymous`.

### Curation semantics
**Naming.** The three URL sets are called the same thing everywhere: **Dump URLs** (what the
crawler found; table `dump_urls`, `dump_count`, `?set=dump`), **Delta URLs** (what promote will
change; `delta_urls`, `delta_count`, `?set=delta`) and **Curated URLs** (the approved set;
`curated_urls`, `curated_count`, `?set=curated`). "Pending" is reserved for undecided suggestions.

**Curated counts.** The Curated URLs list holds every approved row, included *and* excluded.
`curated_count` is the rows that reach the index (`NOT excluded`) — the number on the tab, the
dashboard column and the API — and `curated_rows` is the whole set, which is what the checks that
ask "has anything been promoted" read. An exclude rule applies in place, so `curated_count` drops
the moment the rule is added; coming back in is a delta URL, so it rises again on promote.
`curated_changed_at` stamps every change to the set (a promote that moved something, or an exclude
applied in place); a test index run older than it is behind the curated set, which is the second
way the **needs re-indexing** chip goes up (the first is a run that failed or did not validate).

Effective value per URL = the newest matching pattern (highest id — the curator's latest decision,
whether a per-URL edit, an accepted AI suggestion or a glob typed by hand) → the curated value →
NULL. `include` always beats `exclude`, however old. Title values are templates (`{title}` =
scraped title, `{url}`, `{collection}`). A per-URL edit is just an exact-URL pattern. Deleting a
pattern recomputes — that *is* the unapply (next newest → curated → NULL). Promote takes the whole
delta queue (Curate tab) or a ticked selection of it (Delta URLs tab, `POST …/promote/urls`); the
collection stays `curating` until the queue is empty. Diff + apply run as one idempotent bulk pass
(100k URLs in < 5 s). Rules are strictly per collection (`patterns.collection_id`, cascade on
delete); the packaged global exclude list only ever produces *suggestions*, accepted per collection.

**Provenance.** Every rule carries a `source`: `sme` (typed or toggled by a curator), `llm`
(AI suggestion accepted as-is), `llm_edited` (AI suggestion changed before accepting) or `global`
(global exclude list). Recompute derives `edited_by` per URL (`ai` / `sme` / `mixed`) from the
rules that set its fields; promote copies it to the curated row, and the rule→URL effects survive
promote so the Curated table can still say which rule set what. The URL tables show it as an
**Edited by** column (filter `?edited=ai|sme|mixed`, CSV column `edited_by`); the Rules table
shows each rule's source. Every URL table (Dump URLs, Delta URLs, Curated URLs) is editable in place; an edit
on a promoted row becomes a `modified` delta URL. Existing databases are backfilled on
startup from accepted suggestions and per-URL `ai.accept` audit lines; rules from older *bulk* AI
accepts cannot be told apart and read as SME. Curated rows promoted before the column existed get
their value on the next "Start curating" / recompute.

### Guard rails
- One job per collection; **every mutating action returns 409 while a job runs** (cancel first).
- A per-collection lock serialises scrape ingest, recompute, pattern edits and promote.
- Status changes — even manual overrides — must respect the data: `scraped`/`curating` need a
  dump; `curated` and later need a promoted set and no delta URLs.
- Recompute never demotes when nothing changed; an identical re-crawl returns straight to `curated`.
- "Load existing crawl" only offers a *finished* crawl. Crawler v2 rewrites the S3 documents
  object every 100 pages as a checkpoint, so a documents object newer than its failure summary
  (or with no summary) is a crawl still running on the host: the workbench shows it as *crawl in
  progress* and refuses to ingest it.
- A re-scrape (or "load existing") clears the delta URLs and flags ⚠ *needs re-curation* whenever
  something was promoted; a failed test-index validation raises the same flag. The reason is kept
  (`recuration_reason`) and shown on the badge. "Start curating" clears it when nothing changed.
- Promote warns when ≥ `PROMOTE_REMOVAL_WARN_RATIO` (default 0.25, and at least 5 rows) of the
  curated set vanished from the crawl — a partial crawl looks exactly like a site that shrank.
- Live views refresh on SSE **and** poll (5–10 s), so a missed event cannot leave a page stale.
- Every job ends in `succeeded` or `failed` with the reason shown; jobs orphaned by a crash are
  marked failed on restart.
- Server-side refusals surface as an alert with the server's message; nothing fails silently.

### Working in parallel (several curators at once)
Concurrency is **per collection, not global**. Any number of collections can have jobs running at
the same time; each collection allows exactly one job, and curation edits on a collection wait
for that collection's job. The app itself puts no cap on how many scrapes, index runs or LLM jobs
are in flight — the ceilings come from the systems behind it.

**Isolation**
- One asyncio lock per collection, shared by scrape ingest, LLM jobs, index runs and every
  curation write. Curators on different collections never block each other; on the same
  collection, writes serialise and a second job start is refused with 409 (not queued).
- Starting a job checks three things — a job being created, a live job task, and a held lock —
  so two people clicking *Scrape* in the same instant cannot both start one.
- Cancel only touches that collection's job and records who cancelled it.

**What actually runs in parallel**
- *Scrapes* — `local`: one subprocess per collection, all at once. `ssm`: the crawler box runs
  one job at a time under `flock`; extra jobs are accepted immediately and shown as *queued*
  with how many crawls are ahead, and a queue can wait indefinitely. So scrapes from several
  curators are accepted in parallel but crawled one at a time.
- *LLM jobs* — each job runs its own pool of `LLM_WORKERS` (default 16) concurrent calls. There
  is no limiter across jobs: five curators classifying at once is up to 80 in-flight calls.
  This is the first place you will hit the provider's rate limit.
- *Index runs* — each dispatches its own ECS task and polls S3. Nothing limits how many run at
  once; runs on different collections are fine as long as the indexer tolerates it.

**Data layer**
- PostgreSQL through a small connection pool (`DB_POOL_SIZE`, default 8); every `Database`
  method is one transaction, and status transitions lock the collection row. The job registry
  and per-collection locks still live in memory, so the ECS service is pinned to one task —
  **do not run two replicas** until those move into the database.
- Curation edits have no stale-edit check. Two curators editing the same row on the same
  collection: last save wins silently. Every write records the actor (activity tab), but nobody
  is warned.

**What other curators see**
- Job starts, progress and completion are pushed over SSE to every open browser; the header,
  pipeline and jobs strip also poll (5–10 s), so a second curator sees status and counts move.
- Another person's pattern/metadata edits are *not* pushed. Header counts catch up on the next
  poll; the curate table body only reloads when a job finishes or the page is refreshed.

**Guidance**
- Assign curators to distinct collections — that is the model the app is built around.
- Expect scrapes to queue on the shared crawler; kick them off early.
- If several people will run LLM jobs at the same time, lower `LLM_WORKERS` or add a
  process-wide semaphore in `llm/pool.py` so total in-flight calls stay under the provider limit.
- If two people must share one collection, agree on who edits; the app will not detect a
  stale edit.

## Configuration (`.env`, see `.env.example`)
| Key | Purpose |
|---|---|
| `DATABASE_URL` or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_SSLMODE` | PostgreSQL. One URL locally (`make db-up` → `postgresql://engine:engine@localhost:5432/engine`); the ECS task gets the parts, with user/password from the RDS secret |
| `DB_POOL_SIZE` (8) | connections per engine process |
| `DATA_DIR` | `collections/<id>/{collection,patterns}.yaml`, index logs, scrape jobs |
| `CRAWLER_ROOT`, `CRAWLER_PYTHON` | crawl4ai repo and its interpreter |
| `INDEXER_ROOT`, `INDEXER_PYTHON` | sde-api-scrapers repo (Phase 5) |
| `SCRAPE_BACKEND` | `local` (subprocess) or `ssm` (drop the job on the EC2 inbox via SSM; the job shows as *queued* until the crawler rewrites its log, then S3 is polled for the documents object) |
| `AWS_PROFILE` | local runs only: the AWS CLI/SSO profile boto3 uses (the app exports it); unset in ECS |
| `CRAWLER_INSTANCE_ID`, `CRAWLER_S3_BUCKET`, `CRAWLER_S3_PREFIX` | needed for `ssm`; the prefix is the folder inside the bucket the crawler writes to (`<prefix>/scraped_collections/<stem>.json`, where `<stem>` is the slugged seed URL, e.g. `https_science.nasa.gov_photojournal`), empty = bucket root |
| `INDEX_BACKEND` (`local`\|`ecs`), `COSMOS_INDEX_BUCKET`, `WEB_INDEX_NAME` | indexing target bucket / index |
| `TEST_FRONTEND_URL`, `PROD_FRONTEND_URL` | search front ends the "Open test / prod front end" buttons on steps 5 and 6 link to, so the curator can verify what was indexed |
| `INDEXING_ECS_CLUSTER`, `INDEXING_TASK_FAMILY`, `INDEXING_CONTAINER_NAME`, `INDEXING_SUBNETS`, `INDEXING_SECURITY_GROUPS`, `INDEXING_DISPATCH_ROLE_ARN` | `ecs` backend |
| `OPENSEARCH_ENDPOINT_TEST`, `OPENSEARCH_ENDPOINT_PROD`, `SAGEMAKER_ENDPOINT_NAME` | `local` backend (the ECS task def already carries these) |
| `INDEX_POLL_INTERVAL_S`, `INDEX_STALL_TIMEOUT_S` | status.json polling; the stall timeout also bounds a *started* remote crawl's silence |
| `SCRAPE_POLL_INTERVAL_S` | `ssm` backend: how often to look at the crawler host. A queued job waits indefinitely (the UI shows for how long); only a dead `watch_inbox.sh` fails it |
| `VALIDATION_DELAY_S`, `VALIDATION_POLL_INTERVAL_S`, `VALIDATION_TIMEOUT_S`, `VALIDATION_TITLE_MATCH_THRESHOLD`, `VALIDATION_ASSUME_ROLE_ARN` | validation gate: initial wait, then re-check cadence and window for OpenSearch to become consistent |
| `NOTIFY_WEBHOOK_URL`, `PUBLIC_BASE_URL` | Slack-compatible notifications on every status change |
| `LLM_PROVIDER` (`openai`\|`fake`), `OPENAI_API_KEY`, `OPENAI_MODEL` (default `gpt-5.6-luna`), `OPENAI_BASE_URL`, `LLM_TIMEOUT_S` (per attempt), `LLM_MAX_RETRIES`, `LLM_TEMPERATURE` (unset = model default; reasoning models reject any other value) | LLM assist; any OpenAI-compatible endpoint |
| `PROMOTE_REMOVAL_WARN_RATIO` (0.25) | share of the curated set that must vanish from a crawl before Promote warns |
| `LLM_WORKERS` (16), `LLM_RETRY_PASSES` (1), `LLM_RETRY_DELAY_S` (30), `LLM_PATTERN_BATCH_URLS` (1000), `LLM_DEDUPE_TITLES` (false), `GLOBAL_EXCLUDES_PATH` | calls in flight per LLM job; end-of-job retries of rate-limited / 5xx / timed-out calls (a quarter of the workers, after the delay); URLs per Suggest-patterns call; true: Suggest metadata re-titles same title + doc type pages by itself (default: flag only); override the packaged global exclude YAML |
| `APP_PASSWORD`, `SESSION_SECRET`, `SESSION_TTL_S`, `AUTH_COOKIE_SECURE` | login with local accounts (off when `APP_PASSWORD` is empty; the value seeds the bootstrap `admin`); `/health` stays open |

## API
Everything the UI does is a JSON endpoint (`/docs` for OpenAPI). HTMX callers get
`HX-Redirect`/`HX-Refresh` headers; JSON callers get plain payloads.

| Route | Purpose |
|---|---|
| `GET /events` | SSE stream: `collection`, `collection_created` |
| `POST /api/collections` | create `{seed_url, name, division?, document_type?, max_pages?}` (`division` defaults to `General` = not assigned, so the AI decides one per page) |
| `GET /api/collections/{id}` | read |
| `DELETE /api/collections/{id}` | delete the collection and all its data (admin only; 409 if busy) |
| `POST …/index-key` | `{index_key, index_name?}` — index this collection as another `collection_key` |
| `POST …/division` | `{division}` — the curator's division for the whole collection: applied to every URL no division rule decides, and never asked of the model; `General` puts it back to "not assigned" and the AI decides per page |
| `POST …/status` | `{status, note?, force?}` — transition + data rules enforced |
| `GET …/history`, `…/jobs`, `…/dump` | audit trail, job runs, ingested URLs |
| `POST …/scrape` | run the crawl → job (202; 409 if busy) |
| `POST …/jobs/cancel` | cancel the running job |
| `POST …/recompute` | diff dump vs curated + apply patterns (idempotent) |
| `GET/POST /…/patterns`, `DELETE …/patterns/{pid}` | pattern CRUD with match counts |
| `POST …/urls` | per-URL edit `{url, type, value?}`; exclude/include toggles |
| `GET …/dump?q&excluded`, `…/delta?kind&excluded&division&document_type&q&edited&renamed&limit&offset` (`…/deltas` still works), `…/curated?q&excluded&edited&unreachable` | the three URL sets, paginated |
| `GET /collections/{id}/urls/{dump\|delta\|curated}?format=csv&…` | CSV export of the filtered set |
| `POST …/promote` | delta URLs → curated URLs; status `curated` |
| `POST …/index?target=test\|prod` | export to S3 + dispatch WEB_COSMOS → job (202; 409 unless curated, no delta URLs, something to export; prod needs a validated test run) |
| `POST …/index/revalidate?target=test\|prod` | re-check the latest run of that target against its index (test: direct, or second pass; prod: direct only) |
| `GET …/index_runs` | run history: indexer status, validation report, `validated_by` (indexer\|direct\|second_pass) |
| `POST …/suggest/patterns`, `POST …/suggest/metadata?all=` | LLM jobs (202; 409 if busy / nothing to do) |
| `GET …/suggestions?state=`, `POST …/suggestions/{sid}/accept\|reject` | pattern suggestions |
| `POST …/ai/accept\|reject` `{url, field}` | per-URL metadata suggestion |
| `POST …/ai/bulk` `{decision, field?, url?, conf?}` | decide many at once; an accept without `url` passes over the fields your own rules decide |
| `GET /health` | `{ok, db, sse_clients}` |

## Testing the workflow by hand
1. Dashboard → add `https://aurorasaurus.org` (max pages 15) → **Scrape**; watch the spinner, then
   status `scraped`, Dump 15. Bad seed / duplicate show a banner.
2. **Start curating** → lands on URLs › Delta URLs. In Patterns & AI add `exclude */leaderboard*`,
   `title * {title} — {collection}`; back in Delta URLs pick a division on one row (exact pattern beats
   `*`), click a title to edit, ✗/✓ a row; delete a pattern (un-applies); use the filters and ⇩ CSV;
   check Dump (state column) and Curated (read-only, Curate ↗).
3. Curate tab: **✨ Suggest exclusions** → Accept / Edit / Reject rows; **✨ Suggest metadata** → ✓ / ✎ / ✕ the `AI:` badges in URLs › Delta URLs.
4. **Promote** → `curated`; step 3 → Recompute stays `curated` when nothing changed.
5. Step 1 → **Re-scrape** → `scraped` + ⚠; **Start curating** on an identical crawl → back to `curated`.
6. Guard rails: start a bigger crawl, try Add pattern / Promote / Advanced status → 409; **Cancel**;
   Advanced `curated` with delta URLs → 409.
7. `curl localhost:8080/api/collections/aurorasaurus.org/history`, `data/collections/aurorasaurus.org/*.yaml`.

## Layout
```
sde_curation/
  config.py        pydantic-settings
  models.py        every boundary model (API, DB rows, indexer contracts, LLM schemas)
  db.py            PostgreSQL (psycopg 3 pool): one transaction per method, COPY for bulk replaces
  schema.py        numbered migrations (schema_version table)
  import_sqlite.py one-off cutover: copy a SQLite-era engine.db into PostgreSQL
  engine/          pure: patterns.py (resolution), diff.py (delta URLs, promote), export.py (indexer contract)
  curation.py      engine ↔ DB glue, per-collection locking
  backends/        scrape.py (local subprocess | SSM), index.py (local subprocess | ECS), validate.py (direct AOSS check), s3.py
  notify.py        Slack-compatible webhook on status transitions
  llm/             base.py (provider protocol + registry), openai.py, fake.py, tasks.py (prompts, sanity filters)
  jobs.py          JobManager: background tasks, cancel, recovery, SSE events
  events.py        in-process event bus → SSE
  web/             FastAPI app, auth.py (local users, roles, signed sessions), Jinja templates, vendored htmx (+sse, json-enc)
tests/             pytest; fake crawler fixture, moto for AWS, state-matrix
infra/             AWS CDK (Python): Fargate + EFS + ALB + CloudFront/WAF; bootstrap/ = GitHub deploy role
.github/workflows/ deploy.yml (push to dev|test|prod → cdk deploy), test.yml (PRs)
Dockerfile         python:3.13-slim + pip -r requirements.txt (exported from uv.lock), non-root, uvicorn on 8080
```
