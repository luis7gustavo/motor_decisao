from __future__ import annotations

import asyncio
from datetime import timedelta
import random
from typing import Any

from crawlee import ConcurrencySettings
from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    HttpCrawler,
    ParselCrawler,
    PlaywrightCrawler,
)

from pipelines.collection.models import BrowserPolicy, CollectionStrategy, SourceConfig
from pipelines.collection.crawlee.observability import classify_collection_error


MEDIA_PATTERNS = ["*.mp4", "*.webm", "*.mp3", "*.wav", "*.avi", "*.mov"]
IMAGE_PATTERNS = ["*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp", "*.svg", "*.ico"]
FONT_PATTERNS = ["*.woff", "*.woff2", "*.ttf", "*.otf", "*.eot"]
TRACKING_PATTERNS = [
    "*google-analytics.com*",
    "*googletagmanager.com*",
    "*doubleclick.net*",
    "*facebook.net/tr*",
    "*hotjar.com*",
]


def blocked_url_patterns(policy: BrowserPolicy) -> list[str]:
    patterns: list[str] = []
    if policy.block_images:
        patterns.extend(IMAGE_PATTERNS)
    if policy.block_fonts:
        patterns.extend(FONT_PATTERNS)
    if policy.block_media:
        patterns.extend(MEDIA_PATTERNS)
    if policy.block_tracking:
        patterns.extend(TRACKING_PATTERNS)
    return patterns


class CrawleeCrawlerFactory:
    """Cria crawlers com limites padronizados por fonte.

    A factory desabilita explicitamente o retry de bloqueios automatico do
    framework. O SILLO registra bloqueio e nao tenta contornar CAPTCHA ou
    controles de acesso.
    """

    @staticmethod
    def concurrency_settings(config: SourceConfig) -> ConcurrencySettings:
        policy = config.concurrency
        return ConcurrencySettings(
            min_concurrency=1,
            desired_concurrency=policy.desired_concurrency,
            max_concurrency=policy.max_concurrency,
            max_tasks_per_minute=float(policy.max_tasks_per_minute),
        )

    @classmethod
    def common_options(cls, config: SourceConfig, **overrides: Any) -> dict[str, Any]:
        options = {
            "concurrency_settings": cls.concurrency_settings(config),
            "max_request_retries": config.retry.max_retries,
            "request_handler_timeout": timedelta(seconds=config.timeouts.handler_seconds),
            "use_session_pool": True,
            # Sessions keep cookies/state consistent; rotations stay disabled
            # because this platform does not attempt to bypass access controls.
            "max_session_rotations": 0,
            "retry_on_blocked": False,
            "respect_robots_txt_file": True,
            "configure_logging": False,
        }
        options.update(overrides)
        return options

    @classmethod
    def http_options(cls, config: SourceConfig, **overrides: Any) -> dict[str, Any]:
        return {
            **cls.common_options(config, **overrides),
            "navigation_timeout": timedelta(seconds=config.timeouts.request_seconds),
        }

    @classmethod
    def playwright_options(cls, config: SourceConfig) -> dict[str, Any]:
        if not config.browser.enabled:
            raise ValueError(f"Browser is disabled for source: {config.source_id}")
        return {
            "browser_type": config.browser.browser_type,
            "headless": config.browser.headless,
            "navigation_timeout": timedelta(seconds=config.timeouts.request_seconds),
            "browser_launch_options": {
                "args": ["--disable-gpu", "--disable-dev-shm-usage"],
                # Containers run the browser as root. Disabling Chromium's
                # process sandbox is required there; this is not an anti-bot bypass.
                "chromium_sandbox": False,
            },
        }

    @classmethod
    def build(cls, config: SourceConfig, **overrides: Any):
        strategy = config.strategy
        if strategy == CollectionStrategy.HTTP:
            crawler = HttpCrawler(**cls.http_options(config, **overrides))
            cls._register_retry_policy(crawler, config)
            return crawler
        if strategy == CollectionStrategy.PARSEL:
            crawler = ParselCrawler(**cls.http_options(config, **overrides))
            cls._register_retry_policy(crawler, config)
            return crawler
        if strategy == CollectionStrategy.ADAPTIVE_PLAYWRIGHT:
            crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
                **cls.common_options(config, **overrides),
                playwright_crawler_specific_kwargs=cls.playwright_options(config),
            )
            cls._register_retry_policy(crawler, config)
            cls._register_adaptive_resource_blocking(crawler, config.browser)
            return crawler
        if strategy == CollectionStrategy.PLAYWRIGHT:
            crawler = PlaywrightCrawler(
                **cls.common_options(config, **overrides),
                **cls.playwright_options(config),
            )
            cls._register_retry_policy(crawler, config)
            cls._register_playwright_resource_blocking(crawler, config.browser)
            return crawler
        raise ValueError(f"Crawlee does not handle strategy: {strategy.value}")

    @staticmethod
    def _register_retry_policy(crawler, config: SourceConfig) -> None:
        @crawler.error_handler
        async def standardized_backoff(context, error: Exception) -> None:
            classified = classify_collection_error(error)
            if not classified.retryable:
                context.request.no_retry = True
                return
            attempt = max(int(context.request.retry_count), 0)
            delay = min(
                config.retry.backoff_base_seconds * (2**attempt),
                config.retry.backoff_max_seconds,
            )
            if config.retry.jitter_seconds:
                delay += random.uniform(0, config.retry.jitter_seconds)
            if delay:
                await asyncio.sleep(delay)

    @staticmethod
    def _register_playwright_resource_blocking(
        crawler: PlaywrightCrawler,
        policy: BrowserPolicy,
    ) -> None:
        patterns = blocked_url_patterns(policy)
        if not patterns:
            return

        @crawler.pre_navigation_hook
        async def block_resources(context) -> None:
            await context.block_requests(url_patterns=patterns)

    @staticmethod
    def _register_adaptive_resource_blocking(
        crawler: AdaptivePlaywrightCrawler,
        policy: BrowserPolicy,
    ) -> None:
        patterns = blocked_url_patterns(policy)
        if not patterns:
            return

        @crawler.pre_navigation_hook(playwright_only=True)
        async def block_resources(context) -> None:
            if context.block_requests is not None:
                await context.block_requests(url_patterns=patterns)
