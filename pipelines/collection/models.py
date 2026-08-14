from __future__ import annotations

import re
import socket
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SOURCE_ID_PATTERN = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SourceType(str, Enum):
    SUPPLIER = "supplier"
    MARKET = "market"
    REFERENCE = "reference"


class MarketScope(str, Enum):
    NATIONAL_B2B = "national_b2b"
    REGIONAL_B2B = "regional_b2b"
    INTERNATIONAL = "international"
    MARKETPLACE = "marketplace"
    RETAILER = "retailer"
    LOCAL_RETAILER = "local_retailer"
    MANUFACTURER = "manufacturer"


class CollectionStrategy(str, Enum):
    API = "api"
    JSON = "json"
    XML = "xml"
    CSV = "csv"
    HTTP = "http"
    PARSEL = "parsel"
    ADAPTIVE_PLAYWRIGHT = "adaptive_playwright"
    PLAYWRIGHT = "playwright"
    MANUAL = "manual"

    @property
    def uses_browser(self) -> bool:
        return self in {
            CollectionStrategy.ADAPTIVE_PLAYWRIGHT,
            CollectionStrategy.PLAYWRIGHT,
        }


class AccessClassification(str, Enum):
    PUBLIC_API = "PUBLIC_API"
    PUBLIC_FEED = "PUBLIC_FEED"
    PUBLIC_HTTP = "PUBLIC_HTTP"
    PUBLIC_BROWSER = "PUBLIC_BROWSER"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    COMMERCIAL_INTEGRATION_REQUIRED = "COMMERCIAL_INTEGRATION_REQUIRED"
    NOT_VIABLE = "NOT_VIABLE"
    COLLECTION_BLOCKED = "COLLECTION_BLOCKED"


class CollectionStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"


class CollectionErrorType(str, Enum):
    NETWORK_ERROR = "NETWORK_ERROR"
    TIMEOUT = "TIMEOUT"
    HTTP_429 = "HTTP_429"
    HTTP_403 = "HTTP_403"
    HTTP_5XX = "HTTP_5XX"
    HTTP_ERROR = "HTTP_ERROR"
    PARSER_ERROR = "PARSER_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    AUTH_ERROR = "AUTH_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    COLLECTION_BLOCKED = "COLLECTION_BLOCKED"
    UNKNOWN = "UNKNOWN"


class RetryPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_retries: int = Field(default=3, ge=0, le=20)
    backoff_base_seconds: float = Field(default=1.0, ge=0, le=300)
    backoff_max_seconds: float = Field(default=30.0, ge=0, le=3600)
    jitter_seconds: float = Field(default=0.5, ge=0, le=60)

    @model_validator(mode="after")
    def validate_backoff(self) -> "RetryPolicy":
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_max_seconds must be >= backoff_base_seconds")
        return self


class TimeoutPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_seconds: float = Field(default=30.0, gt=0, le=1800)
    handler_seconds: float = Field(default=90.0, gt=0, le=3600)
    collection_seconds: float = Field(default=1800.0, gt=0, le=86400)

    @model_validator(mode="after")
    def validate_order(self) -> "TimeoutPolicy":
        if self.handler_seconds < self.request_seconds:
            raise ValueError("handler_seconds must be >= request_seconds")
        if self.collection_seconds < self.handler_seconds:
            raise ValueError("collection_seconds must be >= handler_seconds")
        return self


class ConcurrencyPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    desired_concurrency: int = Field(default=1, ge=1, le=100)
    max_concurrency: int = Field(default=1, ge=1, le=100)
    max_tasks_per_minute: int = Field(default=60, ge=1, le=10000)

    @model_validator(mode="after")
    def validate_concurrency(self) -> "ConcurrencyPolicy":
        if self.desired_concurrency > self.max_concurrency:
            raise ValueError("desired_concurrency must be <= max_concurrency")
        return self


class BrowserPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    headless: bool = True
    block_images: bool = True
    block_fonts: bool = True
    block_media: bool = True
    block_tracking: bool = True
    browser_type: str = "chromium"


class SchedulePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    interval_hours: float | None = Field(default=None, gt=0, le=24 * 31)
    minute_offset: int = Field(default=0, ge=0, le=59)
    timezone: str = "America/Sao_Paulo"

    @model_validator(mode="after")
    def validate_interval(self) -> "SchedulePolicy":
        if self.enabled and self.interval_hours is None:
            raise ValueError("enabled schedules require interval_hours")
        return self


class SourceConfig(BaseModel):
    """Configuracao declarativa de uma fonte da plataforma V2."""

    model_config = ConfigDict(extra="forbid")

    source_id: str
    display_name: str
    enabled: bool = False
    source_type: SourceType
    market_scope: MarketScope
    collector: str
    strategy: CollectionStrategy
    access: AccessClassification
    priority: int = Field(default=50, ge=0, le=100)
    profiles: set[str] = Field(default_factory=set)
    urls: list[str] = Field(default_factory=list)
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    timeouts: TimeoutPolicy = Field(default_factory=TimeoutPolicy)
    concurrency: ConcurrencyPolicy = Field(default_factory=ConcurrencyPolicy)
    browser: BrowserPolicy = Field(default_factory=BrowserPolicy)
    scheduling: SchedulePolicy = Field(default_factory=SchedulePolicy)
    canary_enabled: bool = True
    config: dict[str, Any] = Field(default_factory=dict)

    @field_validator("source_id", "collector")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not SOURCE_ID_PATTERN.fullmatch(normalized):
            raise ValueError("must use lowercase snake_case")
        return normalized

    @field_validator("display_name")
    @classmethod
    def validate_display_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("display_name cannot be empty")
        return cleaned

    @field_validator("profiles", mode="before")
    @classmethod
    def normalize_profiles(cls, value: Any) -> set[str]:
        return {str(item).strip().lower() for item in (value or []) if str(item).strip()}

    @model_validator(mode="after")
    def validate_strategy(self) -> "SourceConfig":
        if self.strategy.uses_browser and not self.browser.enabled:
            raise ValueError("browser strategy requires browser.enabled=true")
        if not self.strategy.uses_browser and self.browser.enabled:
            raise ValueError("browser.enabled is only valid for browser strategies")
        if self.enabled and self.access in {
            AccessClassification.AUTH_REQUIRED,
            AccessClassification.COMMERCIAL_INTEGRATION_REQUIRED,
            AccessClassification.NOT_VIABLE,
            AccessClassification.COLLECTION_BLOCKED,
        }:
            raise ValueError(f"source with access={self.access.value} cannot be enabled")
        return self


class CollectionContext(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    pipeline_run_id: str
    worker_id: str
    hostname: str = Field(default_factory=socket.gethostname)
    requested_at: datetime = Field(default_factory=utc_now)
    canary: bool = False
    dry_run: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class CollectionError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error_type: CollectionErrorType
    message: str
    retryable: bool = False
    request_id: str | None = None
    url: str | None = None
    status_code: int | None = Field(default=None, ge=100, le=599)
    occurred_at: datetime = Field(default_factory=utc_now)
    details: dict[str, Any] = Field(default_factory=dict)


class RawProduct(BaseModel):
    """Evidencia de produto antes das transformacoes Silver/Gold."""

    model_config = ConfigDict(extra="forbid")

    source_id: str
    source_url: str | None = None
    external_id: str | None = None
    title: str
    price: Decimal | None = Field(default=None, ge=0)
    currency_id: str = "BRL"
    availability: str | None = None
    stock: int | None = Field(default=None, ge=0)
    sku: str | None = None
    ean: str | None = None
    brand: str | None = None
    category: str | None = None
    collected_at: datetime = Field(default_factory=utc_now)
    raw_payload: dict[str, Any]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("source_id")
    @classmethod
    def validate_source_id(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not SOURCE_ID_PATTERN.fullmatch(normalized):
            raise ValueError("source_id must use lowercase snake_case")
        return normalized

    @field_validator("title")
    @classmethod
    def validate_title(cls, value: str) -> str:
        cleaned = " ".join(value.split())
        if not cleaned:
            raise ValueError("title cannot be empty")
        return cleaned


class CollectionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    pipeline_run_id: str
    source_id: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float | None = Field(default=None, ge=0)
    status: CollectionStatus
    items_discovered: int = Field(default=0, ge=0)
    items_valid: int = Field(default=0, ge=0)
    items_invalid: int = Field(default=0, ge=0)
    items_persisted: int = Field(default=0, ge=0)
    requests_total: int = Field(default=0, ge=0)
    requests_success: int = Field(default=0, ge=0)
    requests_failed: int = Field(default=0, ge=0)
    http_requests: int = Field(default=0, ge=0)
    browser_requests: int = Field(default=0, ge=0)
    retry_count: int = Field(default=0, ge=0)
    errors: list[CollectionError] = Field(default_factory=list)
    collection_strategy: CollectionStrategy
    worker_id: str
    hostname: str
    avg_response_time_ms: float | None = Field(default=None, ge=0)
    peak_memory_mb: float | None = Field(default=None, ge=0)
    avg_cpu_percent: float | None = Field(default=None, ge=0, le=100)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_counters(self) -> "CollectionResult":
        if self.finished_at < self.started_at:
            raise ValueError("finished_at must be >= started_at")
        actual_duration = (self.finished_at - self.started_at).total_seconds()
        if self.duration_seconds is None:
            self.duration_seconds = actual_duration
        if self.items_valid + self.items_invalid > self.items_discovered:
            raise ValueError("valid + invalid items cannot exceed discovered items")
        if self.items_persisted > self.items_valid:
            raise ValueError("persisted items cannot exceed valid items")
        if self.requests_success + self.requests_failed > self.requests_total:
            raise ValueError("request outcomes cannot exceed requests_total")
        if self.http_requests + self.browser_requests > self.requests_total:
            raise ValueError("request strategies cannot exceed requests_total")
        if self.status == CollectionStatus.SUCCESS and self.errors:
            raise ValueError("successful collection cannot contain errors")
        return self

    @property
    def requests_per_minute(self) -> float:
        if not self.duration_seconds:
            return 0.0
        return round(self.requests_total / (self.duration_seconds / 60), 3)

    @property
    def products_per_minute(self) -> float:
        if not self.duration_seconds:
            return 0.0
        return round(self.items_persisted / (self.duration_seconds / 60), 3)
