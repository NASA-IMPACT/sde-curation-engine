# sde-curation-engine

Lightweight FastAPI app that drives the SDE curation pipeline
(Collection → Scrape → Curate → Index(test) → Validate → Prod).
Plan and phase status: `docs/plan.md`; workflow: `docs/workflow.md`.

## Run
```bash
cp .env.example .env      # edit paths / AWS / OpenAI values
uv sync
make run                  # http://localhost:8080  (8000 is used by sde-elastic-wrapper)
make test && make lint
```

## Endpoints
| Route | Purpose |
|---|---|
| `GET /` | dashboard (HTMX + SSE, live rows) |
| `GET /collections/{id}` | detail: status history, jobs, manual status change |
| `GET /events` | Server-Sent Events stream (`collection`, `collection_deleted`) |
| `POST /api/collections` | create `{seed_url, name, division?, max_pages?}` |
| `POST /api/collections/{id}/status` | `{status, note?, force?}` — validated transition |
| `GET /api/collections/{id}/history` | status history |
| `GET /health` | `{ok, db, sse_clients}` |
| `GET /docs` | OpenAPI |

State: SQLite at `data/engine.db` plus git-trackable `data/collections/<id>/collection.yaml`.
