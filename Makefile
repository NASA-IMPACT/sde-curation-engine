.PHONY: run test lint
run:
	uv run uvicorn sde_curation.web.app:app --reload --port 8080
test:
	uv run pytest -q
lint:
	uv run ruff check sde_curation tests
