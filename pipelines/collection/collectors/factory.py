from __future__ import annotations

from pipelines.collection.contracts import Collector
from pipelines.collection.models import SourceConfig


def build_collector(
    source_config: SourceConfig,
    *,
    queries: list[str],
    max_results: int | None = None,
) -> Collector:
    from pipelines.collection.collectors.market_web import CrawleeMarketCollector
    from pipelines.collection.collectors.catalog_market import CrawleeCatalogMarketCollector
    from pipelines.collection.collectors.mercado_livre import MercadoLivreAPICollector
    from pipelines.collection.collectors.price_history import CrawleeComparisonCollector
    from pipelines.collection.collectors.supplier import CrawleeSupplierCollector

    source_id = source_config.source_id
    if source_id == "mercado_livre":
        return MercadoLivreAPICollector(
            source_config,
            queries=queries,
            max_items_per_query=max_results or 40,
        )
    if source_id in {"buscape", "zoom"}:
        return CrawleeComparisonCollector(
            source_config,
            queries=queries,
            max_results=max_results or 8,
        )
    if source_config.source_type.value == "supplier" and source_config.strategy.value in {"http", "parsel", "adaptive_playwright", "playwright"}:
        return CrawleeSupplierCollector(source_config)
    if source_config.source_type.value == "market":
        if source_config.config.get("catalog_mode"):
            return CrawleeCatalogMarketCollector(source_config, max_results=max_results or 50)
        return CrawleeMarketCollector(
            source_config,
            queries=queries,
            max_results=max_results or 20,
        )
    raise ValueError(f"No collector implementation registered for: {source_id}")
