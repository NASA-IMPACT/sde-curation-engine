"""Pydantic models: the single source of truth for every boundary (API, DB rows, S3 files)."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utcnow() -> datetime:
    return datetime.now(UTC)


# ── enums ──────────────────────────────────────────────────────────────


class Status(StrEnum):
    BACKLOG = "backlog"
    SCRAPED = "scraped"
    CURATING = "curating"
    CURATED = "curated"
    CONFIG_GENERATED = "config_generated"
    LIVE = "live"


# Forward path plus the explicit back-edges the workflow allows.
ALLOWED_TRANSITIONS: dict[Status, set[Status]] = {
    Status.BACKLOG: {Status.SCRAPED},
    Status.SCRAPED: {Status.CURATING, Status.BACKLOG},
    Status.CURATING: {Status.CURATED, Status.SCRAPED},
    Status.CURATED: {Status.CONFIG_GENERATED, Status.CURATING},
    Status.CONFIG_GENERATED: {Status.LIVE, Status.CURATING},  # validation fail → curating
    Status.LIVE: {Status.CURATING, Status.SCRAPED},  # re-curation / re-scrape
}


def check_transition(current: Status, new: Status) -> None:
    if new == current:
        return
    if new not in ALLOWED_TRANSITIONS[current]:
        raise ValueError(f"illegal status transition {current} -> {new}")


class Division(StrEnum):
    ASTROPHYSICS = "Astrophysics"
    BPS = "Biological and Physical Sciences"
    EARTH_SCIENCE = "Earth Science"
    HELIOPHYSICS = "Heliophysics"
    PLANETARY = "Planetary Science"
    GENERAL = "General"


class DocumentType(StrEnum):
    IMAGES = "Images"
    DATA = "Data"
    DOCUMENTATION = "Documentation"
    SOFTWARE_TOOLS = "Software and Tools"
    MISSIONS_INSTRUMENTS = "Missions and Instruments"


class ConnectorType(StrEnum):
    CRAWLER = "crawler2"
    API = "api"


class JobKind(StrEnum):
    SCRAPE = "scrape"
    INDEX_TEST = "index_test"
    INDEX_PROD = "index_prod"
    LLM_PATTERNS = "llm_patterns"
    LLM_METADATA = "llm_metadata"


class JobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class PatternType(StrEnum):
    EXCLUDE = "exclude"
    INCLUDE = "include"
    TITLE = "title"
    DIVISION = "division"
    DOCUMENT_TYPE = "document_type"


class DeltaKind(StrEnum):
    NEW = "new"
    MODIFIED = "modified"
    DELETED = "deleted"


# ── collection ─────────────────────────────────────────────────────────

_SLUG_RE = re.compile(r"[^a-zA-Z0-9._-]+")


def normalize_seed(url: str) -> str:
    """Mirror of sde_crawler.scope.normalize_seed: http(s) only, scheme prepended if absent."""
    url = url.strip()
    if not url:
        raise ValueError("seed URL is empty")
    if "://" not in url:
        if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", url):
            raise ValueError(f"seed must be http(s), got {url.split(':', 1)[0]!r}")
        url = "https://" + url
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise ValueError(f"seed must be http(s), got {parts.scheme!r}")
    if not parts.netloc:
        raise ValueError(f"seed has no host: {url!r}")
    return url


def apex_host(url: str) -> str:
    host = urlsplit(url).hostname or ""
    return host.removeprefix("www.")


def collection_id_from_seed(seed: str) -> str:
    """Same rule as sde_crawler.job.collection_id_from_seed so ids line up across repos."""
    return _SLUG_RE.sub("_", apex_host(normalize_seed(seed))).strip("._") or "collection"


class CollectionCreate(BaseModel):
    seed_url: str
    name: str = Field(min_length=1, max_length=200)
    division: Division = Division.GENERAL
    document_type: DocumentType | None = None
    connector: ConnectorType = ConnectorType.CRAWLER
    collection_id: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9._-]+$")
    max_pages: int = Field(default=100_000, ge=1, le=100_000)

    @field_validator("seed_url")
    @classmethod
    def _seed(cls, v: str) -> str:
        return normalize_seed(v)

    @model_validator(mode="after")
    def _fill_id(self) -> CollectionCreate:
        if not self.collection_id:
            self.collection_id = collection_id_from_seed(self.seed_url)
        return self


class Collection(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    collection_id: str
    name: str
    seed_url: str
    division: Division
    document_type: DocumentType | None = None
    connector: ConnectorType
    max_pages: int
    status: Status = Status.BACKLOG
    needs_recuration: bool = False
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    # counters kept on the row for a cheap dashboard
    dump_count: int = 0
    delta_count: int = 0
    curated_count: int = 0


class StatusHistory(BaseModel):
    id: int | None = None
    collection_id: str
    old_status: Status | None
    new_status: Status
    note: str | None = None
    at: datetime = Field(default_factory=utcnow)


# ── URLs ───────────────────────────────────────────────────────────────


class DumpUrl(BaseModel):
    collection_id: str
    url: str
    scraped_title: str | None = None
    full_text: str | None = None
    content_type: str | None = None
    depth: int | None = None


class DeltaUrl(BaseModel):
    collection_id: str
    url: str
    kind: DeltaKind
    scraped_title: str | None = None
    title: str | None = None
    division: Division | None = None
    document_type: DocumentType | None = None
    excluded: bool = False
    # ML suggestions never overwrite manual values
    title_ml: str | None = None
    division_ml: Division | None = None
    document_type_ml: DocumentType | None = None


class CuratedUrl(BaseModel):
    collection_id: str
    url: str
    scraped_title: str | None = None
    title: str | None = None
    division: Division | None = None
    document_type: DocumentType | None = None
    excluded: bool = False


# ── patterns ───────────────────────────────────────────────────────────


class PatternCreate(BaseModel):
    type: PatternType
    match: str = Field(min_length=1, description="exact URL or glob with *")
    value: str | None = None  # title template / division / document_type

    @model_validator(mode="after")
    def _value_required(self) -> PatternCreate:
        if self.type in (PatternType.TITLE, PatternType.DIVISION, PatternType.DOCUMENT_TYPE):
            if not self.value:
                raise ValueError(f"{self.type} pattern requires a value")
            if self.type is PatternType.DIVISION:
                Division(self.value)
            if self.type is PatternType.DOCUMENT_TYPE:
                DocumentType(self.value)
        return self


class Pattern(PatternCreate):
    id: int | None = None
    collection_id: str
    created_at: datetime = Field(default_factory=utcnow)


# ── jobs ───────────────────────────────────────────────────────────────


class JobRun(BaseModel):
    id: int | None = None
    collection_id: str
    kind: JobKind
    state: JobState = JobState.QUEUED
    run_id: str | None = None  # index runs
    external_ref: str | None = None  # pid / SSM command id / ECS taskArn
    progress: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
    started_at: datetime = Field(default_factory=utcnow)
    finished_at: datetime | None = None


# ── indexer contracts (sde-api-scrapers/web) ───────────────────────────


class ExportLine(BaseModel):
    """One documents.jsonl line. Field names are the indexer's allow-list."""

    url: str = Field(min_length=1)
    title: str | None = None
    full_text: str | None = None
    document_type: str | None = None
    division: str | None = None


class ExportManifest(BaseModel):
    collection_key: str
    run_id: str
    document_count: int = Field(ge=0)
    collection_name: str | None = None
    division: str | None = None
    document_type: str | None = None
    target: str = "test"
    exported_at: datetime = Field(default_factory=utcnow)
    schema_version: int = 1


class IndexStatus(BaseModel):
    """index_runs/{key}/{run_id}/status.json as written by WebPipeline.run()."""

    model_config = ConfigDict(extra="allow")

    run_id: str
    collection_key: str
    target: str
    index: str | None = None
    state: str  # succeeded | failed
    documents_in_export: int = 0
    changed: int = 0
    indexed: int = 0
    failed: int = 0
    deleted: int = 0
    error: str | None = None
    error_detail: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float | None = None


class ValidationReport(BaseModel):
    model_config = ConfigDict(extra="allow")

    run_id: str | None = None
    collection_key: str | None = None
    expected_count: int
    indexed_count: int
    count_matches: bool
    title_match_rate: float
    titles_missing_in_index: list[str] = Field(default_factory=list)
    titles_only_in_index: list[str] = Field(default_factory=list)
    titles_mismatched: list[dict[str, Any]] = Field(default_factory=list)


# ── LLM suggestion schemas ─────────────────────────────────────────────


class PatternSuggestion(BaseModel):
    type: PatternType
    match: str
    value: str | None = None
    rationale: str


class PatternSuggestions(BaseModel):
    suggestions: list[PatternSuggestion]


class MetadataSuggestion(BaseModel):
    url: str
    title: str | None = None
    division: Division | None = None
    document_type: DocumentType | None = None


class MetadataSuggestions(BaseModel):
    items: list[MetadataSuggestion]
