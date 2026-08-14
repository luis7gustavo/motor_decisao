from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from pipelines.collection import (
    AccessClassification,
    BrowserPolicy,
    CollectionContext,
    CollectionResult,
    CollectionStatus,
    CollectionStrategy,
    Collector,
    MarketScope,
    SourceConfig,
    SourceType,
)


def source_config(**overrides: object) -> SourceConfig:
    values: dict[str, object] = {
        "source_id": "kabum",
        "display_name": "KaBuM!",
        "enabled": True,
        "source_type": SourceType.MARKET,
        "market_scope": MarketScope.RETAILER,
        "collector": "kabum",
        "strategy": CollectionStrategy.PLAYWRIGHT,
        "access": AccessClassification.PUBLIC_BROWSER,
        "profiles": ["market", "full"],
        "browser": BrowserPolicy(enabled=True),
    }
    values.update(overrides)
    return SourceConfig.model_validate(values)


class DummyCollector(Collector):
    async def collect(self, run_id: str, context: CollectionContext) -> CollectionResult:
        started = datetime.now(timezone.utc)
        return CollectionResult(
            run_id=run_id,
            pipeline_run_id=context.pipeline_run_id,
            source_id=self.source_id,
            started_at=started,
            finished_at=started + timedelta(seconds=30),
            status=CollectionStatus.SUCCESS,
            items_discovered=2,
            items_valid=2,
            items_persisted=2,
            requests_total=1,
            requests_success=1,
            browser_requests=1,
            collection_strategy=self.source_config.strategy,
            worker_id=context.worker_id,
            hostname=context.hostname,
        )


def test_collector_contract_returns_traceable_result() -> None:
    collector = DummyCollector(source_config())
    context = CollectionContext(pipeline_run_id="pipeline-1", worker_id="pc1-browser")

    result = asyncio.run(collector.collect("source-run-1", context))

    assert result.source_id == "kabum"
    assert result.duration_seconds == 30
    assert result.requests_per_minute == 2
    assert result.products_per_minute == 4


def test_source_config_rejects_browser_strategy_without_browser_budget() -> None:
    with pytest.raises(ValidationError, match="browser strategy"):
        source_config(browser=BrowserPolicy(enabled=False))


def test_collection_result_rejects_inconsistent_counters() -> None:
    started = datetime.now(timezone.utc)
    with pytest.raises(ValidationError, match=r"valid \+ invalid"):
        CollectionResult(
            run_id="source-run-1",
            pipeline_run_id="pipeline-1",
            source_id="kabum",
            started_at=started,
            finished_at=started,
            status=CollectionStatus.PARTIAL,
            items_discovered=1,
            items_valid=1,
            items_invalid=1,
            collection_strategy=CollectionStrategy.PLAYWRIGHT,
            worker_id="pc1-browser",
            hostname="pc1",
        )
