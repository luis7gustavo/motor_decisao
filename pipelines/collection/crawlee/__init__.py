"""Base Crawlee compartilhada pelos coletores web SILLO V2."""

from pipelines.collection.crawlee.factory import CrawleeCrawlerFactory
from pipelines.collection.crawlee.observability import (
    RequestMetrics,
    classify_collection_error,
    configure_structured_logging,
    get_collection_logger,
)

__all__ = [
    "CrawleeCrawlerFactory",
    "RequestMetrics",
    "classify_collection_error",
    "configure_structured_logging",
    "get_collection_logger",
]
