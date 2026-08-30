"""Runtime settings. Every value is an env var (or `.env`); nothing is hard-coded."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PROJECTS = _REPO_ROOT.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── local state ────────────────────────────────────────────────────
    data_dir: Path = _REPO_ROOT / "data"
    db_path: Path | None = None  # defaults to data_dir / "engine.db"

    # ── sibling repos ──────────────────────────────────────────────────
    crawler_root: Path = _PROJECTS / "sde-crawl4ai-scraper-v1"
    crawler_python: Path | None = None  # defaults to crawler_root/.venv/bin/python
    indexer_root: Path = _PROJECTS / "sde-api-scrapers"
    indexer_python: Path | None = None  # defaults to indexer_root/.venv/bin/python

    # ── backends ───────────────────────────────────────────────────────
    scrape_backend: Literal["local", "ssm"] = "local"
    index_backend: Literal["local", "ecs"] = "local"

    # ── AWS ────────────────────────────────────────────────────────────
    aws_region: str = "us-east-1"
    crawler_s3_bucket: str | None = None  # SDE_S3_BUCKET of the crawler stack
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
    opensearch_endpoint_test: str | None = None  # local index backend only (ECS task def carries these)
    opensearch_endpoint_prod: str | None = None
    sagemaker_endpoint_name: str | None = None
    index_poll_interval_s: float = 30.0
    index_stall_timeout_s: float = 4 * 3600
    scrape_poll_interval_s: float = 15.0

    # ── validation gate ────────────────────────────────────────────────
    validation_title_match_threshold: float = 0.99
    # The indexer validates right after its bulk upsert, before OpenSearch Serverless has refreshed,
    # so a run that wrote anything reports 0/N in validation.json. We wait, then validate ourselves:
    # a direct SigV4 query of the index (needs AOSS data access for the engine's principal or
    # VALIDATION_ASSUME_ROLE_ARN); if that is refused (403) we fall back to re-running the same
    # export (changed: 0) just to get a fresh validation.json from the indexer.
    validation_delay_s: float = 30.0
    validation_assume_role_arn: str | None = None

    # ── LLM ────────────────────────────────────────────────────────────
    llm_provider: Literal["openai", "fake"] = "openai"
    openai_api_key: str | None = None
    openai_model: str = "gpt-5.4-mini"
    openai_base_url: str | None = None  # any OpenAI-compatible endpoint
    llm_timeout_s: float = 60.0

    # ── notifications ──────────────────────────────────────────────────
    notify_webhook_url: str | None = None
    public_base_url: str = "http://localhost:8080"  # used in notification links

    @field_validator("data_dir", "db_path", "crawler_root", "crawler_python", "indexer_root",
                     "indexer_python", mode="after")
    @classmethod
    def _absolute(cls, v: Path | None) -> Path | None:
        # .env paths are written relative to the repo root; make them cwd-independent.
        if v is None or v.is_absolute():
            return v
        return (_REPO_ROOT / v).resolve()

    # ── derived ────────────────────────────────────────────────────────
    @property
    def resolved_db_path(self) -> Path:
        return self.db_path or (self.data_dir / "engine.db")

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
    return _settings


def reset_settings() -> None:
    """Test hook: drop the cached instance so env overrides take effect."""
    global _settings
    _settings = None
