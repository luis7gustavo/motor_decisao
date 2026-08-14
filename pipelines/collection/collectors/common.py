from __future__ import annotations

import re
from dataclasses import asdict
from datetime import timedelta
from typing import Any
from urllib.parse import urlparse

from crawlee.request_loaders import ThrottlingRequestManager
from crawlee.storages import RequestQueue

from pipelines.collection.models import CollectionError, CollectionErrorType, CollectionStatus


def storage_name(prefix: str, run_id: str) -> str:
    safe = re.sub(r"[^a-z0-9-]+", "-", f"{prefix}-{run_id}".lower()).strip("-")
    return safe[:220]


async def open_run_queue(
    source_id: str,
    run_id: str,
    *,
    urls: list[str],
    base_delay_seconds: float,
    max_delay_seconds: float,
) -> ThrottlingRequestManager[RequestQueue]:
    """Create an isolated queue that can honor robots crawl-delay and HTTP 429s."""

    queue = await RequestQueue.open(name=storage_name(f"sillo-{source_id}", run_id))
    domains = sorted(
        {
            parsed.netloc.lower()
            for url in urls
            if (parsed := urlparse(url)).netloc
        }
    )
    return ThrottlingRequestManager(
        inner=queue,
        domains=domains,
        request_manager_opener=RequestQueue.open,
        base_delay=timedelta(seconds=base_delay_seconds),
        max_delay=timedelta(seconds=max_delay_seconds),
    )


async def context_html(context: Any) -> str:
    if hasattr(context, "parse_with_static_parser"):
        parsed = await context.parse_with_static_parser()
        return parsed.get() or ""
    if hasattr(context, "selector"):
        return context.selector.get() or ""
    page = getattr(context, "page", None)
    if page is not None:
        return await page.content()
    raise RuntimeError("Crawlee context does not expose parsed HTML")


def register_skipped_request_handler(crawler: Any, errors: list[CollectionError]) -> None:
    """Turn robots.txt skips into a traceable terminal source outcome."""

    @crawler.on_skipped_request
    async def record_skipped_request(url: str, reason: str) -> None:
        errors.append(
            CollectionError(
                error_type=CollectionErrorType.COLLECTION_BLOCKED,
                message=f"Request skipped by Crawlee ({reason})",
                retryable=False,
                url=url,
                details={"reason": str(reason)},
            )
        )


def result_status(*, persisted: int, invalid: int, errors: list[CollectionError]) -> CollectionStatus:
    if not errors and not invalid:
        return CollectionStatus.SUCCESS
    if persisted:
        return CollectionStatus.PARTIAL
    if errors and all(error.error_type is CollectionErrorType.COLLECTION_BLOCKED for error in errors):
        return CollectionStatus.BLOCKED
    return CollectionStatus.FAILED


def dataclass_payload(value: Any) -> dict[str, Any]:
    return asdict(value)


def retry_count_from_statistics(statistics: Any) -> int:
    histogram = getattr(statistics, "retry_histogram", []) or []
    return sum(index * count for index, count in enumerate(histogram))


def adaptive_request_counts(crawler: Any, *, total: int, browser_default: bool) -> tuple[int, int]:
    try:
        state = crawler.statistics.state
        http_requests = int(getattr(state, "http_only_request_handler_runs", 0))
        browser_requests = int(getattr(state, "browser_request_handler_runs", 0))
        if http_requests or browser_requests:
            # Adaptive counters can include retry attempts while requests_total
            # is the unique-request total exposed by Crawlee statistics.
            browser_requests = min(browser_requests, total)
            http_requests = min(http_requests, max(total - browser_requests, 0))
            return http_requests, browser_requests
    except (AttributeError, RuntimeError):
        pass
    return (0, total) if browser_default else (total, 0)
