from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from crawlee import Request

from app.core.database import engine
from pipelines.collection.contracts import Collector
from pipelines.collection.crawlee import CrawleeCrawlerFactory, classify_collection_error
from pipelines.collection.models import CollectionContext, CollectionError, CollectionResult
from pipelines.collection.parsers import parse_supplier_products
from pipelines.suppliers.base import SupplierProductSnapshot
from pipelines.suppliers.ingest import _insert_snapshot

from .common import (
    context_html,
    dataclass_payload,
    open_run_queue,
    register_skipped_request_handler,
    result_status,
    retry_count_from_statistics,
    storage_name,
)


class CrawleeSupplierCollector(Collector):
    async def collect(self, run_id: str, context: CollectionContext) -> CollectionResult:
        started = datetime.now(timezone.utc)
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
        selectors = dict(self.source_config.config.get("selectors") or {})
        pagination = dict(self.source_config.config.get("pagination") or {})

        @crawler.router.default_handler
        async def handle_request(crawling_context) -> None:
            html = await context_html(crawling_context)
            page_url = crawling_context.request.url
            snapshots = parse_supplier_products(
                html,
                supplier_slug=self.source_id,
                page_url=page_url,
                selectors=selectors,
            )
            if snapshots:
                await crawling_context.push_data(
                    [dataclass_payload(snapshot) for snapshot in snapshots],
                    dataset_name=dataset_name,
                )
            page_number = int(crawling_context.request.user_data.get("page") or 1)
            if (
                snapshots
                and not context.canary
                and pagination.get("enabled")
                and page_number < int(pagination.get("max_pages", 1))
            ):
                base_url = str(crawling_context.request.user_data.get("base_url") or page_url)
                next_page = page_number + 1
                next_url = str(pagination.get("url_pattern", "{url}?p={page}")).format(
                    url=base_url,
                    page=next_page,
                )
                await crawling_context.add_requests(
                    [
                        Request.from_url(
                            next_url,
                            user_data={"base_url": base_url, "page": next_page},
                            unique_key=f"{base_url}:page:{next_page}",
                        )
                    ]
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

        urls = self.source_config.urls[:1] if context.canary else self.source_config.urls
        requests = [
            Request.from_url(
                url,
                user_data={"base_url": url, "page": 1},
                unique_key=f"{self.source_id}:{url}:page:1",
            )
            for url in urls
        ]
        statistics = await crawler.run(requests, purge_request_queue=False)
        data = await crawler.get_data(dataset_name=dataset_name)
        snapshots = [SupplierProductSnapshot(**dict(item)) for item in data.items]
        valid = [snapshot for snapshot in snapshots if snapshot.raw_title and snapshot.raw_price is not None]
        persisted = await asyncio.to_thread(self._persist, UUID(run_id), snapshots)
        await (await crawler.get_dataset(name=dataset_name)).drop()
        await queue.drop()
        finished = datetime.now(timezone.utc)
        invalid = len(snapshots) - len(valid)
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
            http_requests=statistics.requests_total,
            browser_requests=0,
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
            metadata={"urls": urls, "canary": context.canary},
        )

    @staticmethod
    def _persist(source_run_id: UUID, snapshots: list[SupplierProductSnapshot]) -> int:
        persisted = 0
        with engine.begin() as connection:
            for snapshot in snapshots:
                if _insert_snapshot(connection, source_run_id=source_run_id, snapshot=snapshot):
                    persisted += 1
        return persisted
