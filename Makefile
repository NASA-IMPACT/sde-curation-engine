.PHONY: run test lint docker-build docker-run infra-install infra-test infra-seed bootstrap-github synth diff deploy destroy redeploy logs
run:
	uv run uvicorn sde_curation.web.app:app --reload --port 8080
test:
	uv run pytest -q
lint:
	uv run ruff check sde_curation tests

# ── container ──────────────────────────────────────────────────────────
docker-build:
	docker build --platform linux/amd64 -t sde-curation-engine:local .
docker-run:  ## local smoke of the image: fake LLM, login password "dev", state in ./.docker-data
	mkdir -p .docker-data && docker run --rm -p 8080:8080 -e LLM_PROVIDER=fake -e APP_PASSWORD=dev \
	  -e DB_LOCKING_MODE=exclusive -v $(PWD)/.docker-data:/data sde-curation-engine:local

# ── AWS (CDK, infra/) ──────────────────────────────────────────────────
ENV ?= dev
PROFILE ?= sde-dev
STACK = CurationEngine-$(ENV)
CDK = cd infra && AWS_PROFILE=$(PROFILE) JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1 uv run cdk -c environment=$(ENV)
infra-install:
	cd infra && uv sync
infra-test:
	cd infra && JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1 uv run pytest -q
infra-seed:  ## push infra/envs/$(ENV).json into SSM Parameter Store (gitignored; see envs/example.json)
	cd infra && uv run python seed.py $(ENV) --profile $(PROFILE)
bootstrap-github:  ## once per account (admin): the IAM role GitHub Actions deploys with → secret AWS_ROLE_$(ENV)
	$(CDK) --app "uv run python bootstrap/app.py" deploy CurationEngine-Bootstrap-$(ENV) --require-approval never
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
