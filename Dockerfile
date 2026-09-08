# syntax=docker/dockerfile:1.7
# sde-curation-engine — single-process FastAPI app. State lives under DATA_DIR (/data, an EFS
# mount in ECS). Deployed values are injected by the CDK stack in infra/.
FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 DATA_DIR=/data
WORKDIR /app

# pinned deps first (layer cached until requirements.txt changes)
COPY requirements.txt ./
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements.txt

# then the package itself, without re-resolving dependencies
COPY pyproject.toml README.md ./
COPY sde_curation ./sde_curation
RUN --mount=type=cache,target=/root/.cache/pip pip install --no-deps .

RUN groupadd -g 1000 app && useradd -u 1000 -g app -m app \
    && mkdir -p /data && chown app:app /data
USER app
EXPOSE 8080
# --proxy-headers: honour X-Forwarded-Proto/For from the ALB (CloudFront in front of it).
CMD ["uvicorn", "sde_curation.web.app:app", "--host", "0.0.0.0", "--port", "8080", \
     "--proxy-headers", "--forwarded-allow-ips=*"]
