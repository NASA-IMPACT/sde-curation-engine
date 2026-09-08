# syntax=docker/dockerfile:1.7
# sde-curation-engine — single-process FastAPI app. State lives under DATA_DIR (/data, an EFS
# mount in ECS). Deployed values are injected by the CDK stack in infra/.
FROM python:3.13-slim AS base
COPY --from=ghcr.io/astral-sh/uv:0.7.6 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv \
    PYTHONUNBUFFERED=1 DATA_DIR=/data
WORKDIR /app

# deps first (cached until pyproject/uv.lock change)
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY sde_curation ./sde_curation
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

RUN groupadd -g 1000 app && useradd -u 1000 -g app -m app \
    && mkdir -p /data && chown app:app /data
USER app
EXPOSE 8080
# --proxy-headers: honour X-Forwarded-Proto/For from the ALB (CloudFront in front of it).
CMD ["/opt/venv/bin/uvicorn", "sde_curation.web.app:app", "--host", "0.0.0.0", "--port", "8080", \
     "--proxy-headers", "--forwarded-allow-ips=*"]
