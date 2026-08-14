from __future__ import annotations

from crawlee.crawlers import AdaptivePlaywrightCrawler, ParselCrawler, PlaywrightCrawler

from pipelines.collection import (
    AccessClassification,
    BrowserPolicy,
    CollectionError,
    CollectionErrorType,
    CollectionStrategy,
    MarketScope,
    SourceConfig,
    SourceType,
)
from pipelines.collection.crawlee import (
    CrawleeCrawlerFactory,
    RequestMetrics,
    classify_collection_error,
)
from pipelines.collection.collectors.common import result_status, storage_name


def _config(strategy: CollectionStrategy) -> SourceConfig:
    uses_browser = strategy in {
        CollectionStrategy.ADAPTIVE_PLAYWRIGHT,
        CollectionStrategy.PLAYWRIGHT,
    }
    return SourceConfig(
        source_id="test_source",
        display_name="Test Source",
        enabled=True,
        source_type=SourceType.MARKET,
        market_scope=MarketScope.RETAILER,
        collector="test_source",
        strategy=strategy,
        access=(
            AccessClassification.PUBLIC_BROWSER
            if uses_browser
            else AccessClassification.PUBLIC_HTTP
        ),
        browser=BrowserPolicy(enabled=uses_browser),
        concurrency={
            "desired_concurrency": 2,
            "max_concurrency": 3,
            "max_tasks_per_minute": 20,
        },
    )


def test_factory_builds_supported_crawlers() -> None:
    assert isinstance(CrawleeCrawlerFactory.build(_config(CollectionStrategy.PARSEL)), ParselCrawler)
    assert isinstance(
        CrawleeCrawlerFactory.build(_config(CollectionStrategy.ADAPTIVE_PLAYWRIGHT)),
        AdaptivePlaywrightCrawler,
    )
    assert isinstance(
        CrawleeCrawlerFactory.build(_config(CollectionStrategy.PLAYWRIGHT)),
        PlaywrightCrawler,
    )


def test_factory_disables_automatic_block_bypass() -> None:
    crawler = CrawleeCrawlerFactory.build(_config(CollectionStrategy.PARSEL))
    assert crawler._retry_on_blocked is False
    assert crawler._use_session_pool is True
    assert crawler._max_session_rotations == 0
    assert crawler._respect_robots_txt_file is True
    assert crawler._max_request_retries == 3


def test_request_metrics_keep_http_and_browser_budgets_separate() -> None:
    metrics = RequestMetrics()
    metrics.record_success(browser=False, elapsed_ms=100)
    metrics.record_success(browser=True, elapsed_ms=300)
    metrics.record_failure(browser=True, retry_count=2)

    assert metrics.requests_total == 3
    assert metrics.http_requests == 1
    assert metrics.browser_requests == 2
    assert metrics.retry_count == 2
    assert metrics.avg_response_time_ms == 200


def test_error_classifier_distinguishes_retryable_and_terminal_errors() -> None:
    rate_limited = classify_collection_error(RuntimeError("HTTP status 429"))
    forbidden = classify_collection_error(RuntimeError("Status 403: access denied"))
    unauthorized = classify_collection_error(RuntimeError("Status 401: invalid access token"))
    parser = classify_collection_error(ValueError("product parser schema invalid"))

    assert rate_limited.error_type == CollectionErrorType.HTTP_429
    assert rate_limited.retryable
    assert forbidden.error_type == CollectionErrorType.HTTP_403
    assert not forbidden.retryable
    assert unauthorized.error_type == CollectionErrorType.AUTH_ERROR
    assert not unauthorized.retryable
    assert parser.error_type == CollectionErrorType.PARSER_ERROR
    assert not parser.retryable


def test_storage_name_matches_crawlee_contract() -> None:
    assert storage_name("SILLO-grupo_tek", "ABC_123") == "sillo-grupo-tek-abc-123"


def test_robots_skip_is_reported_as_blocked() -> None:
    status = result_status(
        persisted=0,
        invalid=0,
        errors=[
            CollectionError(
                error_type=CollectionErrorType.COLLECTION_BLOCKED,
                message="robots.txt disallowed",
                retryable=False,
            )
        ],
    )

    assert status.value == "blocked"
