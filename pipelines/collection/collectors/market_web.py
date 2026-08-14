from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from crawlee import Request

from app.core.database import engine
from pipelines.collection.contracts import Collector
from pipelines.collection.crawlee import CrawleeCrawlerFactory, classify_collection_error
from pipelines.collection.models import (
    CollectionContext,
    CollectionError,
    CollectionResult,
)
from pipelines.collection.parsers import parse_market_listings
from pipelines.market_web.base import MarketListingSnapshot
from pipelines.market_web.ingest import _insert_listing
from pipelines.market_web.sources import SOURCE_CONFIGS

from .common import (
    adaptive_request_counts,
    context_html,
    dataclass_payload,
    open_run_queue,
    register_skipped_request_handler,
    result_status,
    retry_count_from_statistics,
    storage_name,
)


class CrawleeMarketCollector(Collector):
    def __init__(
        self,
        source_config,
        *,
        queries: list[str],
        max_results: int = 20,
    ) -> None:
        super().__init__(source_config)
        if source_config.source_id not in SOURCE_CONFIGS:
            raise ValueError(f"Market parser not found: {source_config.source_id}")
        self.queries = queries
        self.max_results = max_results

    async def collect(self, run_id: str, context: CollectionContext) -> CollectionResult:
        started = datetime.now(timezone.utc)
        parser_config = SOURCE_CONFIGS[self.source_id]
        queue = await open_run_queue(
            self.source_id,
            run_id,
            urls=self.source_config.urls,
            base_delay_seconds=self.source_config.retry.backoff_base_seconds,
            max_delay_seconds=self.source_config.retry.backoff_max_seconds,
        )
        crawler = CrawleeCrawlerFactory.build(self.source_config, request_manager=queue)
        dataset_name = storage_name(f"dataset-{self.source_id}", run_id)
        errors: list[CollectionError] = []
        register_skipped_request_handler(crawler, errors)

        @crawler.router.default_handler
        async def handle_request(crawling_context) -> None:
            html = await context_html(crawling_context)
            query = str(crawling_context.request.user_data.get("query") or "")
            snapshots = parse_market_listings(
                html,
                config=parser_config,
                query=query,
                page_url=crawling_context.request.url,
                max_results=1 if context.canary else self.max_results,
            )
            if not snapshots:
                raise ValueError(f"parser returned zero products for {self.source_id}: {query}")
            await crawling_context.push_data(
                [dataclass_payload(snapshot) for snapshot in snapshots],
                dataset_name=dataset_name,
            )

        @crawler.failed_request_handler
        async def failed_request(crawling_context, error: Exception) -> None:
            errors.append(
                classify_collection_error(
                    error,
                    request_id=crawling_context.request.unique_key,
                    url=crawling_context.request.url,
                )
            )

        selected_queries = self.queries[:1] if context.canary else self.queries
        requests = [
            Request.from_url(
                parser_config.search_url(query),
                user_data={"query": query},
                unique_key=f"{self.source_id}:{query}",
            )
            for query in selected_queries
        ]
        statistics = await crawler.run(requests, purge_request_queue=False)
        data = await crawler.get_data(dataset_name=dataset_name)
        snapshots = [MarketListingSnapshot(**dict(item)) for item in data.items]
        valid = [snapshot for snapshot in snapshots if snapshot.price is not None and snapshot.price > 0]
        invalid = len(snapshots) - len(valid)
        persisted = await asyncio.to_thread(self._persist, UUID(run_id), snapshots)
        await (await crawler.get_dataset(name=dataset_name)).drop()
        await queue.drop()
        finished = datetime.now(timezone.utc)
        http_requests, browser_requests = adaptive_request_counts(
            crawler,
            total=statistics.requests_total,
            browser_default=self.source_config.strategy.uses_browser,
        )
        # Bronze uses idempotent upserts: duplicates are expected and are not a
        # partial collection. Only parsing/request failures degrade the run.
        status = result_status(persisted=persisted, invalid=invalid, errors=errors)
        return CollectionResult(
            run_id=run_id,
            pipeline_run_id=context.pipeline_run_id,
            source_id=self.source_id,
            started_at=started,
            finished_at=finished,
            status=status,
            items_discovered=len(snapshots),
            items_valid=len(valid),
            items_invalid=invalid,
            items_persisted=persisted,
            requests_total=statistics.requests_total,
            requests_success=statistics.requests_finished,
            requests_failed=statistics.requests_failed,
            http_requests=http_requests,
            browser_requests=browser_requests,
            retry_count=retry_count_from_statistics(statistics),
            errors=errors,
            collection_strategy=self.source_config.strategy,
            worker_id=context.worker_id,
            hostname=context.hostname,
            avg_response_time_ms=(
                statistics.request_avg_finished_duration.total_seconds() * 1000
                if statistics.request_avg_finished_duration
                else None
            ),
            metadata={"queries": selected_queries, "canary": context.canary},
        )

    @staticmethod
    def _persist(source_run_id: UUID, snapshots: list[MarketListingSnapshot]) -> int:
        persisted = 0
        with engine.begin() as connection:
            for snapshot in snapshots:
                # CollectionResult defines persisted records as a subset of
                # valid records. Keep price-less parser candidates observable
                # through the counters, but do not write them to Bronze.
                if snapshot.price is None or snapshot.price <= 0:
                    continue
                if _insert_listing(connection, source_run_id=source_run_id, snapshot=snapshot):
                    persisted += 1
        return persisted
