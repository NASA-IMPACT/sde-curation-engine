"""Runtime settings. Every value is an env var (or `.env`); nothing is hard-coded."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PROJECTS = _REPO_ROOT.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── local state ────────────────────────────────────────────────────
    data_dir: Path = _REPO_ROOT / "data"  # collections/<id>/*.yaml, index logs, scrape jobs

    # ── database (PostgreSQL) ──────────────────────────────────────────
    # Either one URL (local dev, tests) or the parts (ECS: host/port/name as env, user/password
    # injected from the RDS-generated secret). `resolved_database_url` combines them.
    database_url: str | None = None
    db_host: str | None = None
    db_port: int = 5432
    db_name: str = "engine"
    db_user: str | None = None
    db_password: str | None = None
    db_sslmode: Literal["disable", "prefer", "require", "verify-ca", "verify-full"] = "prefer"
    db_pool_size: int = Field(default=8, ge=1, le=64)  # connections per engine process

    # ── sibling repos ──────────────────────────────────────────────────
    crawler_root: Path = _PROJECTS / "sde-crawl4ai-scraper"
    crawler_python: Path | None = None  # defaults to crawler_root/.venv/bin/python
    indexer_root: Path = _PROJECTS / "sde-api-scrapers"
    indexer_python: Path | None = None  # defaults to indexer_root/.venv/bin/python

    # ── backends ───────────────────────────────────────────────────────
    scrape_backend: Literal["local", "ssm"] = "local"
    index_backend: Literal["local", "ecs"] = "local"

    # ── AWS ────────────────────────────────────────────────────────────
    aws_region: str = "us-east-1"
    # Local runs: the named AWS CLI/SSO profile every boto3 client uses (unset in ECS, where the
    # task role applies). Exported to the process environment once, because .env values are not.
    aws_profile: str | None = None
    crawler_s3_bucket: str | None = None  # SDE_S3_BUCKET of the crawler stack
    # Folder inside that bucket the crawler writes to ("" = bucket root): keys are
    # <prefix>/scraped_collections/<stem>.json and <prefix>/failure_logs/<stem>_failures_summary.json,
    # <stem> being the slugged seed URL (models.crawl_file_stem)
    crawler_s3_prefix: str = ""
    crawler_instance_id: str | None = None  # EC2 instance running watch_inbox.sh
    crawler_remote_inbox: str = "/opt/sde-crawler/jobs/incoming"
    cosmos_index_bucket: str | None = None  # sde-cosmos-indexing-{env}
    indexing_ecs_cluster: str = "api-scrapers-cluster-dev"
    indexing_task_family: str = "web_cosmos-scraper-dev"
    indexing_container_name: str = "WEB_COSMOSContainer"
    indexing_dispatch_role_arn: str | None = None
    indexing_subnets: list[str] = Field(default_factory=list)
    indexing_security_groups: list[str] = Field(default_factory=list)
    indexing_assign_public_ip: bool = True
    web_index_name: str = "sde-web-subset"  # the indexer's working index; live sde-web only at cutover
    # Search front ends the curator opens from steps 5/6 to eyeball what the indexer wrote.
    test_frontend_url: str = "http://d2vsr84ys2zd7q.cloudfront.net/"
    prod_frontend_url: str = "https://science.data.nasa.gov/science-discovery-engine/search/sde/home"
    # test: the local index backend passes it to the indexer; the engine validates against it and
    # reads it for vectors S3 does not have. prod: the engine publishes there itself (below).
    opensearch_endpoint_test: str | None = None
    opensearch_endpoint_prod: str | None = None
    sagemaker_endpoint_name: str | None = None
    index_poll_interval_s: float = 30.0

    # ── publish to prod ────────────────────────────────────────────────
    # "Index to prod" re-uses the vectors the validated test run wrote to
    # s3://COSMOS_INDEX_BUCKET/vectorized/ and writes them to OPENSEARCH_ENDPOINT_PROD directly —
    # nothing is re-vectorized. The prod collection is in another account: the engine assumes this
    # role for the write (unset = ambient credentials, e.g. dev where "prod" is the dev collection).
    prod_index_role_arn: str | None = None
    publish_bulk_docs: int = Field(default=100, ge=1, le=1000)  # docs per bulk request
    publish_bulk_max_bytes: int = Field(default=8_000_000, ge=100_000)  # AOSS caps a request at 10 MiB
    # Same guards as the indexer (web/deletion_guard.py): refuse the whole run when tombstoning would
    # remove more than this share of the collection's prod documents, or more than this many.
    publish_deletion_abort_ratio: float = Field(default=0.90, ge=0.0, le=1.0)
    publish_deletion_abort_max: int = Field(default=5000, ge=0)
    index_stall_timeout_s: float = 4 * 3600
    scrape_poll_interval_s: float = 15.0

    # ── validation gate ────────────────────────────────────────────────
    validation_title_match_threshold: float = 0.99
    # The indexer validates right after its bulk upsert, before OpenSearch Serverless has refreshed,
    # so a run that wrote anything reports 0/N in validation.json. We wait, then validate ourselves:
    # a direct SigV4 query of the index (needs AOSS data access for the engine's principal or
    # VALIDATION_ASSUME_ROLE_ARN); if that is refused (403) we fall back to re-running the same
    # export (changed: 0) just to get a fresh validation.json from the indexer.
    # AOSS refresh is not a fixed delay (a 10-doc run has taken >45s and <3min to become visible),
    # so after the initial wait the direct check is repeated every `validation_poll_interval_s`
    # until it passes or `validation_timeout_s` has elapsed since the wait began; only then does
    # a short count fail the gate.
    validation_delay_s: float = 30.0
    validation_poll_interval_s: float = 15.0
    validation_timeout_s: float = 10 * 60
    validation_assume_role_arn: str | None = None

    # ── LLM ────────────────────────────────────────────────────────────
    llm_provider: Literal["openai", "fake"] = "openai"
    openai_api_key: str | None = None
    openai_model: str = "gpt-5.6-luna"  # 1.05M-token window: every page fits, whole
    openai_base_url: str | None = None  # any OpenAI-compatible endpoint
    # Sent only when set. Reasoning models (gpt-5 family, o-series) reject any value but their
    # default and fail every call with 400; leave unset unless the model is known to accept it.
    llm_temperature: float | None = Field(default=None, ge=0, le=2)
    llm_timeout_s: float = 60.0  # per attempt
    llm_max_retries: int = Field(default=5, ge=0, le=10)  # SDK retries on 429 / 5xx / timeouts
    llm_workers: int = Field(default=16, ge=1, le=64)  # concurrent calls inside one LLM job
    # Calls that still fail with a rate limit / 5xx / timeout after the SDK's own retries are run
    # again at the end of the job, this long after the main pass, with a quarter of the workers.
    llm_retry_passes: int = Field(default=1, ge=0, le=5)
    llm_retry_delay_s: float = Field(default=30.0, ge=0)
    # Suggest metadata always sends the FULL page text (no budget, no truncation, one model), one
    # call per URL. Suggest patterns sends every crawled URL (+ title) in batches of this size.
    llm_pattern_batch_urls: int = Field(default=1000, ge=50, le=10_000)
    # Regenerate duplicate titles is the one pass that calls per GROUP, not per page: the pages that
    # share a title go to the model together so it can tell them apart from each other instead of
    # guessing one at a time and colliding again. Their full texts go in uncut, so what bounds a
    # call is characters, not pages — a group over the budget is split, and each later call is told
    # the titles the earlier ones already used. 800k chars ≈ 200k tokens, a fifth of the window.
    llm_title_group_chars: int = Field(default=800_000, ge=20_000)
    # How many times the pass may re-ask a group it did not manage to tell apart before the URLs
    # decide it (see tasks.disambiguate). 0 = ask once, then disambiguate.
    llm_title_passes: int = Field(default=2, ge=0, le=5)
    # Off (default): Suggest metadata only flags pages whose AI title and document type another page
    # of the collection will also have; the SME sees the titles as generated and edits them, or sends
    # them back with "Regenerate duplicate titles" (one call each, full text, with the other URLs, for a new title
    # only). On: Suggest metadata ends with that pass by itself.
    llm_dedupe_titles: bool = False
    # Warn before promoting when this share (or more) of the curated set vanished from the crawl.
    promote_removal_warn_ratio: float = Field(default=0.25, ge=0.0, le=1.0)
    # Exclude globs applied deterministically before the model's own suggestions.
    global_excludes_path: Path | None = None  # default: the packaged sde_curation/data/global_excludes.yaml

    # ── notifications ──────────────────────────────────────────────────
    notify_webhook_url: str | None = None
    public_base_url: str = "http://localhost:8080"  # used in notification links

    # ── access control (deployed only) ─────────────────────────────────
    # Login with local user accounts gates every page and API route except /health, /login and
    # /static. APP_PASSWORD switches it on and seeds the bootstrap `admin` account with that value
    # (only while the users table is empty; later changes to the secret do nothing). Unset = no
    # auth (local dev, tests). SESSION_SECRET signs the cookie; when unset a random per-process
    # secret is used, so a restart logs everyone out.
    app_password: str | None = None
    session_secret: str | None = None
    session_ttl_s: int = 12 * 3600
    # Force the Secure flag on the login cookie. Behind CloudFront→ALB(HTTP) the proxy headers say
    # "http", so the deployment sets this explicitly instead of sniffing the scheme.
    auth_cookie_secure: bool = False

    @field_validator("data_dir", "crawler_root", "crawler_python", "indexer_root",
                     "indexer_python", mode="after")
    @classmethod
    def _absolute(cls, v: Path | None) -> Path | None:
        # .env paths are written relative to the repo root; make them cwd-independent.
        if v is None or v.is_absolute():
            return v
        return (_REPO_ROOT / v).resolve()

    # ── derived ────────────────────────────────────────────────────────
    @property
    def resolved_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        if not (self.db_host and self.db_user):
            raise ValueError("set DATABASE_URL, or DB_HOST + DB_USER (+ DB_PASSWORD, DB_NAME, DB_PORT)")
        auth = quote(self.db_user, safe="") + (f":{quote(self.db_password, safe='')}" if self.db_password else "")
        return f"postgresql://{auth}@{self.db_host}:{self.db_port}/{self.db_name}?sslmode={self.db_sslmode}"

    @property
    def resolved_crawler_python(self) -> Path:
        return self.crawler_python or (self.crawler_root / ".venv" / "bin" / "python")

    @property
    def resolved_indexer_python(self) -> Path:
        return self.indexer_python or (self.indexer_root / ".venv" / "bin" / "python")

    @property
    def collections_dir(self) -> Path:
        return self.data_dir / "collections"


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
        if _settings.aws_profile and not os.environ.get("AWS_PROFILE"):
            os.environ["AWS_PROFILE"] = _settings.aws_profile
    return _settings


def reset_settings() -> None:
    """Test hook: drop the cached instance so env overrides take effect."""
    global _settings
    _settings = None
