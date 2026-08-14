from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode, urljoin
from uuid import UUID

from crawlee import Request

from app.core.database import engine
from pipelines.collection.contracts import Collector
from pipelines.collection.crawlee import CrawleeCrawlerFactory, classify_collection_error
from pipelines.collection.models import CollectionContext, CollectionError, CollectionResult
from pipelines.market_web.parsing import clean_text, parse_brl_price
from pipelines.price_history.base import PriceHistorySnapshot
from pipelines.price_history.comparison_scraper import SOURCE_BASE_URLS
from pipelines.price_history.comparison_web_scraper import (
    _as_float,
    _extract_next_data,
    _extract_product_detail,
    _first_number,
    _first_offer_price,
)
from pipelines.price_history.ingest import _insert_snapshot

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


class CrawleeComparisonCollector(Collector):
    def __init__(self, source_config, *, queries: list[str], max_results: int = 8) -> None:
        super().__init__(source_config)
        if self.source_id not in SOURCE_BASE_URLS:
            raise ValueError(f"Comparison source not supported: {self.source_id}")
        self.queries = queries
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
        errors: list[CollectionError] = []
        register_skipped_request_handler(crawler, errors)
        detail_limit = 1 if context.canary else self.max_results

        @crawler.router.default_handler
        async def search_handler(crawling_context) -> None:
            html = await context_html(crawling_context)
            query = str(crawling_context.request.user_data.get("query") or "")
            hits = self._search_hits(html)
            if not hits:
                raise ValueError(f"parser returned zero comparison hits for {self.source_id}: {query}")
            requests: list[Request] = []
            fallback_snapshots: list[PriceHistorySnapshot] = []
            for position, hit in enumerate(hits[:detail_limit], start=1):
                title = clean_text(hit.get("name") or hit.get("shortName"))
                current_price = _as_float(hit.get("price"))
                path = hit.get("url")
                product_url = urljoin(SOURCE_BASE_URLS[self.source_id], str(path)) if path else None
                product_key = str(hit.get("objectId") or hit.get("sourceId") or product_url or title)
                if product_url:
                    requests.append(
                        Request.from_url(
                            product_url,
                            label="detail",
                            user_data={
                                "query": query,
                                "position": position,
                                "hit": hit,
                                "product_key": product_key,
                            },
                            unique_key=f"{self.source_id}:detail:{product_key}",
                        )
                    )
                elif title:
                    fallback_snapshots.append(
                        PriceHistorySnapshot(
                            source_name=self.source_id,
                            product_key=product_key,
                            product_url=None,
                            title=title,
                            current_price=current_price,
                            min_price=current_price,
                            median_price=None,
                            max_price=current_price,
                            history_window_days=None,
                            payload={"query": query, "position": position, "search_hit": hit},
                            query=query,
                            source_mode="search_current_crawlee",
                        )
                    )
            if fallback_snapshots:
                await crawling_context.push_data(
                    [dataclass_payload(snapshot) for snapshot in fallback_snapshots],
                    dataset_name=dataset_name,
                )
            if requests:
                await crawling_context.add_requests(requests)

        @crawler.router.handler("detail")
        async def detail_handler(crawling_context) -> None:
            html = await context_html(crawling_context)
            selector = await crawling_context.parse_with_static_parser()
            body_text = clean_text(selector.xpath("string(//body)").get()) or ""
            user_data = crawling_context.request.user_data
            hit = dict(user_data.get("hit") or {})
            query = str(user_data.get("query") or "")
            detail = _extract_product_detail(
                html=html,
                body_text=body_text,
                source_name=self.source_id,
                max_body_chars=50000,
            )
            history = detail.get("history_summary") or {}
            schema = detail.get("schema_summary") or {}
            offers = schema.get("offers") or []
            aggregate = schema.get("aggregate_offer") or {}
            search_price = _as_float(hit.get("price"))
            current = _first_number(
                [
                    search_price,
                    aggregate.get("lowPrice"),
                    parse_brl_price(detail.get("lowest_price_text")),
                    _first_offer_price(offers),
                ]
            )
            snapshot = PriceHistorySnapshot(
                source_name=self.source_id,
                product_key=str(user_data.get("product_key") or crawling_context.request.url),
                product_url=crawling_context.request.url,
                title=clean_text(schema.get("name") or detail.get("title") or hit.get("name")),
                current_price=current,
                min_price=_first_number([aggregate.get("lowPrice"), current]),
                median_price=None,
                max_price=_first_number([aggregate.get("highPrice"), current]),
                history_window_days=history.get("window_days"),
                payload={
                    "query": query,
                    "position": user_data.get("position"),
                    "search_hit": hit,
                    "product_page": detail,
                },
                avg_price=_as_float(history.get("avg_price")),
                query=query,
                source_mode="product_history_crawlee",
            )
            await crawling_context.push_data(dataclass_payload(snapshot), dataset_name=dataset_name)

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
                f"{SOURCE_BASE_URLS[self.source_id]}/search?{urlencode({'q': query})}",
                user_data={"query": query},
                unique_key=f"{self.source_id}:search:{query}",
            )
            for query in selected_queries
        ]
        statistics = await crawler.run(requests, purge_request_queue=False)
        data = await crawler.get_data(dataset_name=dataset_name)
        snapshots = [PriceHistorySnapshot(**dict(item)) for item in data.items]
        valid = [snapshot for snapshot in snapshots if snapshot.current_price is not None and snapshot.current_price > 0]
        persisted = await asyncio.to_thread(self._persist, UUID(run_id), snapshots)
        await (await crawler.get_dataset(name=dataset_name)).drop()
        await queue.drop()
        finished = datetime.now(timezone.utc)
        invalid = len(snapshots) - len(valid)
        http_requests, browser_requests = adaptive_request_counts(
            crawler,
            total=statistics.requests_total,
            browser_default=True,
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
            avg_response_time_ms=(
                statistics.request_avg_finished_duration.total_seconds() * 1000
                if statistics.request_avg_finished_duration
                else None
            ),
            metadata={"queries": selected_queries, "canary": context.canary},
        )

    @staticmethod
    def _search_hits(html: str) -> list[dict[str, Any]]:
        next_data = _extract_next_data(html)
        hits = next_data.get("props", {}).get("initialReduxState", {}).get("hits", {}).get("hits", [])
        if not isinstance(hits, list):
            raise ValueError("comparison hits are not a list")
        return [hit for hit in hits if isinstance(hit, dict)]

    @staticmethod
    def _persist(source_run_id: UUID, snapshots: list[PriceHistorySnapshot]) -> int:
        persisted = 0
        with engine.begin() as connection:
            for snapshot in snapshots:
                if _insert_snapshot(connection, source_run_id=source_run_id, snapshot=snapshot):
                    persisted += 1
        return persisted
