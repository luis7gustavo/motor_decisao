from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID

from crawlee import Request

from app.core.database import engine
from pipelines.collection.contracts import Collector
from pipelines.collection.crawlee import CrawleeCrawlerFactory, classify_collection_error
from pipelines.collection.models import CollectionContext, CollectionError, CollectionResult
from pipelines.collection.parsers import parse_supplier_products
from pipelines.market_web.base import MarketListingSnapshot
from pipelines.market_web.ingest import _insert_listing

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


class CrawleeCatalogMarketCollector(Collector):
    """Coletor declarativo para vitrines publicas sem endpoint de busca estavel."""

    def __init__(self, source_config, *, max_results: int = 50) -> None:
        super().__init__(source_config)
        self.max_results = max_results

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
        selectors = dict(self.source_config.config.get("selectors") or {})
        errors: list[CollectionError] = []
        register_skipped_request_handler(crawler, errors)

        @crawler.router.default_handler
        async def handle_request(crawling_context) -> None:
            html = await context_html(crawling_context)
            parsed = parse_supplier_products(
                html,
                supplier_slug=self.source_id,
                page_url=crawling_context.request.url,
                selectors=selectors,
            )
            snapshots = []
            for position, item in enumerate(parsed[: 1 if context.canary else self.max_results], start=1):
                snapshots.append(
                    MarketListingSnapshot(
                        source_name=self.source_id,
                        # Bronze v1 models every market comparison as benchmark;
                        # local_retailer remains the taxonomy in SourceConfig.
                        source_role="benchmark",
                        query="catalog",
                        position=position,
                        title=item.raw_title,
                        price=item.raw_price,
                        old_price=None,
                        currency_id="BRL" if item.raw_price is not None else None,
                        sold_quantity_text=None,
                        sold_quantity=None,
                        demand_signal_type="retailer_offer",
                        demand_signal_value=None,
                        bsr_text=None,
                        rating_text=None,
                        reviews_count=None,
                        seller_text=None,
                        shipping_text=None,
                        installments_text=None,
                        item_url=item.source_url,
                        image_url=None,
                        is_sponsored=False,
                        is_full=None,
                        is_catalog=True,
                        blocked=False,
                        block_reason=None,
                        payload=item.payload,
                    )
                )
            if not snapshots:
                raise ValueError(f"parser returned zero products for {self.source_id}")
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

        urls = self.source_config.urls[:1] if context.canary else self.source_config.urls
        statistics = await crawler.run(
            [Request.from_url(url, unique_key=f"{self.source_id}:{url}") for url in urls],
            purge_request_queue=False,
        )
        data = await crawler.get_data(dataset_name=dataset_name)
        snapshots = [MarketListingSnapshot(**dict(item)) for item in data.items]
        valid = [item for item in snapshots if item.price is not None and item.price > 0]
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
            metadata={"urls": urls, "canary": context.canary},
        )

    @staticmethod
    def _persist(source_run_id: UUID, snapshots: list[MarketListingSnapshot]) -> int:
        persisted = 0
        with engine.begin() as connection:
            for snapshot in snapshots:
                if snapshot.price is None or snapshot.price <= 0:
                    continue
                if _insert_listing(connection, source_run_id=source_run_id, snapshot=snapshot):
                    persisted += 1
        return persisted
