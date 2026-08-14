from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

import structlog

from pipelines.collection.models import CollectionError, CollectionErrorType


SENSITIVE_KEYS = {
    "authorization",
    "cookie",
    "cookies",
    "password",
    "senha",
    "token",
    "access_token",
    "refresh_token",
    "client_secret",
    "cpf",
}


def _redact_sensitive_values(_logger, _method_name, event_dict: dict[str, Any]) -> dict[str, Any]:
    def redact(value: Any, key: str | None = None) -> Any:
        if key and key.lower() in SENSITIVE_KEYS:
            return "[REDACTED]"
        if isinstance(value, dict):
            return {item_key: redact(item_value, item_key) for item_key, item_value in value.items()}
        if isinstance(value, list):
            return [redact(item) for item in value]
        return value

    return redact(event_dict)


def configure_structured_logging(log_level: str = "INFO") -> None:
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO), format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _redact_sensitive_values,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_collection_logger(
    *,
    run_id: str,
    source_id: str,
    worker_id: str,
    hostname: str,
):
    return structlog.get_logger("sillo.collection").bind(
        run_id=run_id,
        source_id=source_id,
        worker=worker_id,
        hostname=hostname,
    )


@dataclass
class RequestMetrics:
    requests_total: int = 0
    requests_success: int = 0
    requests_failed: int = 0
    http_requests: int = 0
    browser_requests: int = 0
    retry_count: int = 0
    response_times_ms: list[float] = field(default_factory=list)

    def record_success(self, *, browser: bool, elapsed_ms: float | None = None) -> None:
        self.requests_total += 1
        self.requests_success += 1
        self.browser_requests += int(browser)
        self.http_requests += int(not browser)
        if elapsed_ms is not None and elapsed_ms >= 0:
            self.response_times_ms.append(elapsed_ms)

    def record_failure(self, *, browser: bool, retry_count: int = 0) -> None:
        self.requests_total += 1
        self.requests_failed += 1
        self.browser_requests += int(browser)
        self.http_requests += int(not browser)
        self.retry_count += max(retry_count, 0)

    @property
    def avg_response_time_ms(self) -> float | None:
        if not self.response_times_ms:
            return None
        return round(sum(self.response_times_ms) / len(self.response_times_ms), 3)


def classify_collection_error(
    error: Exception,
    *,
    request_id: str | None = None,
    url: str | None = None,
) -> CollectionError:
    message = str(error)
    lowered = message.lower()
    module_name = type(error).__module__.lower()
    status_code = _status_code(error, message)

    if module_name.startswith(("psycopg", "sqlalchemy")) or "(psycopg.errors." in lowered:
        error_type = CollectionErrorType.DATABASE_ERROR
        retryable = False
    elif status_code == 429:
        error_type = CollectionErrorType.HTTP_429
        retryable = True
    elif status_code == 401:
        error_type = CollectionErrorType.AUTH_ERROR
        retryable = False
    elif status_code == 403:
        error_type = CollectionErrorType.HTTP_403
        retryable = False
    elif status_code is not None and 500 <= status_code <= 599:
        error_type = CollectionErrorType.HTTP_5XX
        retryable = True
    elif status_code is not None:
        error_type = CollectionErrorType.HTTP_ERROR
        retryable = status_code in {408, 409, 425}
    elif "timeout" in lowered or "timed out" in lowered:
        error_type = CollectionErrorType.TIMEOUT
        retryable = True
    elif any(term in lowered for term in ("network", "connection", "dns", "transport")):
        error_type = CollectionErrorType.NETWORK_ERROR
        retryable = True
    elif any(term in lowered for term in ("captcha", "access denied", "blocked")):
        error_type = CollectionErrorType.COLLECTION_BLOCKED
        retryable = False
    elif any(term in lowered for term in ("unauthorized", "authentication", "login required")):
        error_type = CollectionErrorType.AUTH_ERROR
        retryable = False
    elif any(term in lowered for term in ("parse", "selector", "schema")):
        error_type = CollectionErrorType.PARSER_ERROR
        retryable = False
    else:
        error_type = CollectionErrorType.UNKNOWN
        retryable = False

    return CollectionError(
        error_type=error_type,
        message=message[:2000],
        retryable=retryable,
        request_id=request_id,
        url=url,
        status_code=status_code,
    )


def _status_code(error: Exception, message: str) -> int | None:
    value = getattr(error, "status_code", None)
    if isinstance(value, int) and 100 <= value <= 599:
        return value
    match = re.search(r"(?:status(?: code)?\s*[:=]?\s*|http\s+)([1-5]\d{2})", message, flags=re.I)
    return int(match.group(1)) if match else None
