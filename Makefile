.PHONY: install requirements requirements-check db-up db-down db-shell run test lint docker-build docker-run infra-install infra-test infra-seed bootstrap-github synth diff deploy destroy redeploy logs
# Dependencies: pyproject.toml + uv.lock are the source of truth; requirements*.txt are exported
# from the lock (`make requirements`) so pip-only developers, CI and the Docker image install the
# exact same versions. `make install` uses uv when it is on PATH and plain venv+pip otherwise;
# every other target only uses .venv/bin/… and works the same either way.
PYTHON ?= python3.13
VENV ?= .venv
IVENV ?= infra/.venv
HAVE_UV := $(shell command -v uv 2>/dev/null)

install:  ## app: .venv with runtime + dev deps and the package (editable)
ifdef HAVE_UV
	uv sync
else
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -r requirements-dev.txt -e .
endif
REQ_EXPORT = uv export -q --frozen --no-hashes --no-emit-project --no-header --no-annotate
REQ_FILES = requirements.txt requirements-dev.txt infra/requirements.txt infra/requirements-dev.txt
requirements:  ## regenerate requirements*.txt from the lock files (needs uv; run after changing deps)
	$(REQ_EXPORT) --no-dev > requirements.txt
	{ echo "-r requirements.txt"; $(REQ_EXPORT) --only-dev; } > requirements-dev.txt
	cd infra && $(REQ_EXPORT) --no-dev > requirements.txt
	cd infra && { echo "-r requirements.txt"; $(REQ_EXPORT) --only-dev; } > requirements-dev.txt
requirements-check:  ## fail if requirements*.txt are stale vs the lock files (CI runs this)
	@tmp=$$(mktemp -d) && mkdir -p "$$tmp/infra" && for f in $(REQ_FILES); do cp "$$f" "$$tmp/$$f"; done \
	  && $(MAKE) -s requirements \
	  && for f in $(REQ_FILES); do cmp -s "$$f" "$$tmp/$$f" || { echo "$$f is out of date: run 'make requirements' and commit"; exit 1; }; done \
	  && echo "requirements*.txt match uv.lock"
# ── PostgreSQL (docker-compose.yml) ────────────────────────────────────
# The app and the tests need a PostgreSQL. `make db-up` starts one in Docker on localhost:5432
# (DATABASE_URL=postgresql://engine:engine@localhost:5432/engine, the .env.example default).
DB_URL ?= postgresql://engine:engine@localhost:5432/engine
db-up:  ## start the local PostgreSQL and wait until it accepts connections
	docker compose up -d --wait postgres
db-down:  ## stop it (data stays in the `pgdata` volume; `docker compose down -v` wipes it)
	docker compose down
db-shell:  ## psql into the local database
	docker compose exec postgres psql -U engine -d engine
run: db-up
	$(VENV)/bin/uvicorn sde_curation.web.app:app --reload --port 8080
test:  ## tests: reuse TEST_DATABASE_URL when set, else testcontainers starts a throwaway PostgreSQL
	$(VENV)/bin/python -m pytest -q
test-local: db-up  ## tests against the compose database (what CI does with a service container)
	TEST_DATABASE_URL=$(DB_URL) $(VENV)/bin/python -m pytest -q
lint:
	$(VENV)/bin/python -m ruff check sde_curation tests

# ── container ──────────────────────────────────────────────────────────
docker-build:
	docker build --platform linux/amd64 -t sde-curation-engine:local .
docker-run: db-up  ## local smoke of the image: fake LLM, login password "dev", YAML/logs in ./.docker-data
	mkdir -p .docker-data && docker run --rm -p 8080:8080 -e LLM_PROVIDER=fake -e APP_PASSWORD=dev \
	  -e DATABASE_URL=postgresql://engine:engine@host.docker.internal:5432/engine \
	  -v $(PWD)/.docker-data:/data sde-curation-engine:local

# ── AWS (CDK, infra/) ──────────────────────────────────────────────────
ENV ?= dev
PROFILE ?= sde-dev
STACK = CurationEngine-$(ENV)
IPY = $(abspath $(IVENV))/bin/python
# cdk.json runs `python app.py`; activating infra/.venv puts that python (and the CDK libs) first
CDK = cd infra && . $(abspath $(IVENV))/bin/activate && AWS_PROFILE=$(PROFILE) JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1 cdk -c environment=$(ENV)
infra-install:  ## infra: infra/.venv with the CDK libs (the cdk CLI itself: npm i -g aws-cdk)
ifdef HAVE_UV
	cd infra && uv sync
else
	test -d $(IVENV) || $(PYTHON) -m venv $(IVENV)
	$(IVENV)/bin/pip install -q --upgrade pip
	$(IVENV)/bin/pip install -q -r infra/requirements-dev.txt
endif
infra-test:
	cd infra && JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1 $(IPY) -m ruff check . && JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1 $(IPY) -m pytest -q
infra-seed:  ## push infra/envs/$(ENV).json into SSM Parameter Store (gitignored; see envs/example.json)
	cd infra && $(IPY) seed.py $(ENV) --profile $(PROFILE)
bootstrap-github:  ## once per account (admin): the IAM role GitHub Actions deploys with → secret AWS_ROLE_$(ENV)
	$(CDK) --app "python bootstrap/app.py" deploy CurationEngine-Bootstrap-$(ENV) --require-approval never
synth:
	$(CDK) synth $(STACK)
diff:
	$(CDK) diff $(STACK)
deploy:
	$(CDK) deploy $(STACK) --require-approval never
destroy:
	$(CDK) destroy $(STACK)
redeploy:  ## restart the single task (e.g. after put-secret-value)
	aws ecs update-service --cluster sde-curation-engine-$(ENV) --service sde-curation-engine-$(ENV) \
	  --force-new-deployment --profile $(PROFILE) --query 'service.deployments[].[status,rolloutState]' --output text
logs:
	aws logs tail /ecs/sde-curation-engine-$(ENV) --follow --since 10m --profile $(PROFILE)
