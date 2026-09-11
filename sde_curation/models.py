"""Pydantic models: the single source of truth for every boundary (API, DB rows, S3 files)."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator


def utcnow() -> datetime:
    return datetime.now(UTC)


# Provenance actors that are not people: job-driven transitions, and "auth is off" (local dev).
SYSTEM_ACTOR = "system"
ANONYMOUS_ACTOR = "anonymous"


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


class CurationStage(StrEnum):
    """Sub-stage while a collection is `curating`: first decide the exclusions, then
    metadata (title / division / document type). Cleared whenever the status leaves curating."""

    EXCLUSIONS = "exclusions"
    METADATA = "metadata"


def check_transition(current: Status, new: Status) -> None:
    if new == current:
        return
    if new not in ALLOWED_TRANSITIONS[current]:
        raise ValueError(f"illegal status transition {current} -> {new}")


class Role(StrEnum):
    ADMIN = "admin"  # manages users, may delete collections
    CURATOR = "curator"


class Division(StrEnum):
    ASTROPHYSICS = "Astrophysics"
    BPS = "Biological and Physical Sciences"
    EARTH_SCIENCE = "Earth Science"
    HELIOPHYSICS = "Heliophysics"
    PLANETARY = "Planetary Science"
    GENERAL = "General"


class Confidence(StrEnum):
    HIGH = "high"      # stated explicitly in the page text or title
    MEDIUM = "medium"  # a strong inference from URL, site or context
    LOW = "low"        # a guess; a null value with low confidence is preferred to a wrong one


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
    VALIDATE = "validate"
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


class RuleSource(StrEnum):
    """Where a rule came from. `sme` = typed or toggled by a curator; `llm` = an AI suggestion
    accepted as-is; `llm_edited` = an AI suggestion the curator changed before accepting;
    `global` = the shared global exclude list, accepted for this collection."""

    SME = "sme"
    LLM = "llm"
    LLM_EDITED = "llm_edited"
    GLOBAL = "global"


class EditedBy(StrEnum):
    """Who set the effective values of one URL: every winning rule is AI, every one is SME, or a mix."""

    AI = "ai"
    SME = "sme"
    MIXED = "mixed"


def edited_by_of(sources: list[str]) -> EditedBy | None:
    """Summarise the sources of the rules that set a URL's fields (None = no rule touched it)."""
    if not sources:
        return None
    ai = {s for s in sources if s in (RuleSource.LLM, RuleSource.GLOBAL)}
    sme = {s for s in sources if s in (RuleSource.SME, RuleSource.LLM_EDITED)}
    if ai and sme:
        return EditedBy.MIXED
    return EditedBy.AI if ai else EditedBy.SME


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

    _validated: bool = PrivateAttr(default=False)  # computed for the UI: latest test run validated

    collection_id: str
    name: str
    seed_url: str
    division: Division
    document_type: DocumentType | None = None
    connector: ConnectorType
    max_pages: int
    status: Status = Status.BACKLOG
    curation_stage: CurationStage | None = None  # only while status == curating
    needs_recuration: bool = False
    recuration_reason: str | None = None  # why the flag is up (re-crawl / validation failure)
    last_scraped_at: datetime | None = None  # when the current dump was crawled (or loaded)
    last_crawl_capped: bool = False  # the current dump stopped at max_pages: absence is not evidence
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    last_run_id: str | None = None  # most recent index run (test or prod)
    created_by: str | None = None  # username; None on rows that predate provenance
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
    actor: str | None = None  # username, SYSTEM_ACTOR, or None (pre-provenance rows)
    at: datetime = Field(default_factory=utcnow)


class User(BaseModel):
    id: int | None = None
    username: str
    password_hash: str
    role: Role = Role.CURATOR
    active: bool = True
    session_version: int = 1  # bumped on password change → existing cookies stop working
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


# ── URLs ───────────────────────────────────────────────────────────────


class DumpUrl(BaseModel):
    collection_id: str
    url: str
    scraped_title: str | None = None
    full_text: str | None = None
    content_type: str | None = None
    depth: int | None = None
    content_hash: str | None = None  # sha256 of the normalised full_text (None = empty/unknown)


class DumpFailure(BaseModel):
    """One URL the crawler tried and could not turn into a document (its failures JSONL)."""

    collection_id: str
    url: str
    reason: str  # crawler reason code: http_404, http_403, http_rate_limit, crawl_unsuccessful, challenge_…, …
    status: int | None = None  # HTTP status when there was one
    detail: str | None = None


# Crawler failure reasons that mean the page is gone, not merely unreachable this time.
GONE_REASONS = frozenset({"http_404", "http_410"})
# Pseudo-reason for a curated URL a capped crawl never got to (no failure record either).
NOT_VISITED = "not_visited"


def failure_means_gone(reason: str | None) -> bool:
    return reason in GONE_REASONS


class DeltaUrl(BaseModel):
    collection_id: str
    url: str
    kind: DeltaKind
    # the curated row this delta replaces when only the URL spelling changed (scheme, trailing
    # slash, #fragment): promote drops that row and writes this URL in its place
    renamed_from: str | None = None
    # tombstones only: the crawler's reason (http_404 …) or None when the crawl simply never saw the URL
    crawl_failure: str | None = None
    scraped_title: str | None = None
    title: str | None = None
    division: Division | None = None
    document_type: DocumentType | None = None
    excluded: bool = False
    content_changed: bool = False  # page text differs from the promoted (curated) version
    edited_by: EditedBy | None = None  # ai / sme / mixed — from the rules that set the fields
    # AI suggestions never overwrite manual values; each carries the model's own confidence
    title_ai: str | None = None
    division_ai: Division | None = None
    document_type_ai: DocumentType | None = None
    title_ai_conf: Confidence | None = None
    division_ai_conf: Confidence | None = None
    document_type_ai_conf: Confidence | None = None
    ai_model: str | None = None  # which model answered
    ai_content_hash: str | None = None  # hash of the text the model saw (resume / re-classify logic)


class CuratedUrl(BaseModel):
    collection_id: str
    url: str
    scraped_title: str | None = None
    title: str | None = None
    division: Division | None = None
    document_type: DocumentType | None = None
    excluded: bool = False
    content_hash: str | None = None  # hash of the text that was promoted (NULL = before hashing existed)
    full_text: str | None = None  # the text the row was approved with (promote copies it from the dump);
    # the export ships this, never the dump. Listing queries leave it out and fill `text_len`.
    text_len: int | None = None
    edited_by: EditedBy | None = None  # carried over from the delta row at promote time
    # set by recompute: the current dump lacks this URL but the crawl does not prove it gone
    # (http_403, timeout, challenge page, or `not_visited` when the crawl hit its page cap), so the
    # row is kept as it is instead of becoming a removal; None once a crawl fetches it again
    crawl_failure: str | None = None


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
    created_by: str | None = None
    source: RuleSource = RuleSource.SME  # never settable through the API body (PatternCreate)


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
    started_by: str | None = None


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


class IndexRun(BaseModel):
    """One export + WEB_COSMOS dispatch. status/validation are the indexer's own JSON files."""

    run_id: str
    collection_id: str
    target: str  # test | prod
    state: str = "running"  # running | succeeded | failed
    exported: int = 0
    external_ref: str | None = None
    status: dict[str, Any] | None = None
    validation: dict[str, Any] | None = None
    validated_by: str | None = None  # indexer | direct | second_pass
    error: str | None = None
    started_at: datetime = Field(default_factory=utcnow)
    finished_at: datetime | None = None
    started_by: str | None = None

    def validation_passes(self, threshold: float = 0.99) -> bool | None:
        v = self.validation
        if not v:
            return None
        return bool(v.get("count_matches")) and float(v.get("title_match_rate", 0)) >= threshold

    @property
    def validation_ok(self) -> bool | None:
        return self.validation_passes()


# ── LLM suggestion schemas ─────────────────────────────────────────────


class PatternSuggestion(BaseModel):
    """What the model may propose: exclude globs only. Everything else (include overrides,
    titles, divisions, document types) is either a curator's decision or per-URL metadata."""

    type: Literal[PatternType.EXCLUDE]
    match: str = Field(min_length=1)
    rationale: str

    @model_validator(mode="after")
    def _same_rules_as_patterns(self) -> PatternSuggestion:
        PatternCreate(type=self.type, match=self.match)
        return self


class PatternSuggestions(BaseModel):
    suggestions: list[PatternSuggestion]


class GlobalExclude(BaseModel):
    match: str = Field(min_length=1)
    rationale: str
    source: Literal["sme", "cosmos"] = "sme"

    @model_validator(mode="after")
    def _valid_glob(self) -> GlobalExclude:
        PatternCreate(type=PatternType.EXCLUDE, match=self.match)
        return self


class GlobalExcludeList(BaseModel):
    version: int = 1
    patterns: list[GlobalExclude] = []


class MetadataSuggestion(BaseModel):
    """The model's answer for ONE document (one call per URL). Confidence is per field and
    required, so the schema forces the model to commit."""

    title: str | None = None
    title_confidence: Confidence
    division: Division | None = None
    division_confidence: Confidence
    document_type: DocumentType | None = None
    document_type_confidence: Confidence
