"""Contratos centrais da plataforma de coleta SILLO V2."""

from pipelines.collection.contracts import Collector
from pipelines.collection.models import (
    AccessClassification,
    BrowserPolicy,
    CollectionContext,
    CollectionError,
    CollectionErrorType,
    CollectionResult,
    CollectionStatus,
    CollectionStrategy,
    ConcurrencyPolicy,
    MarketScope,
    RawProduct,
    RetryPolicy,
    SchedulePolicy,
    SourceConfig,
    SourceType,
    TimeoutPolicy,
)
from pipelines.collection.registry import SourceRegistry, SourceRegistryError

__all__ = [
    "AccessClassification",
    "BrowserPolicy",
    "CollectionContext",
    "CollectionError",
    "CollectionErrorType",
    "CollectionResult",
    "CollectionStatus",
    "CollectionStrategy",
    "Collector",
    "ConcurrencyPolicy",
    "MarketScope",
    "RawProduct",
    "RetryPolicy",
    "SchedulePolicy",
    "SourceConfig",
    "SourceRegistry",
    "SourceRegistryError",
    "SourceType",
    "TimeoutPolicy",
]
