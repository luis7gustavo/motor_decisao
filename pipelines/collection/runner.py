from __future__ import annotations

import asyncio
import os
import socket
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import psutil
from sqlalchemy import text

from app.core.database import engine
from app.core.settings import get_settings
from pipelines.collection.alerts import evaluate_alerts
from pipelines.collection.collectors import build_collector
from pipelines.collection.crawlee import classify_collection_error, configure_structured_logging, get_collection_logger
from pipelines.collection.models import (
    CollectionContext,
    CollectionErrorType,
    CollectionResult,
    CollectionStatus,
    SourceConfig,
)
from pipelines.collection.platform_config import CollectionPlatformConfig
from pipelines.collection.registry import SourceRegistry
from pipelines.common.run_manager import (
    create_pipeline_run,
    create_source_run,
    finish_pipeline_run,
    finish_source_run,
    record_quality_check,
)


RAW_TABLES = {
    "mercado_livre": "bronze.mercado_livre_items_raw",
    "buscape": "bronze.price_history_raw",
    "zoom": "bronze.price_history_raw",
    "mirao": "bronze.supplier_products_raw",
}


async def _sample_process_resources(stop: asyncio.Event, samples: list[tuple[float, float]]) -> None:
    process = psutil.Process()
    process.cpu_percent(interval=None)
    while not stop.is_set():
        samples.append((process.memory_info().rss / 1024 / 1024, process.cpu_percent(interval=None)))
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.5)
        except TimeoutError:
            pass


def _source_run_type(source: SourceConfig) -> str:
    if source.source_id == "mercado_livre":
        return "marketplace_api"
    if source.source_id in {"buscape", "zoom"}:
        return "price_history"
    if source.source_type.value == "supplier":
        return "supplier_scraper"
    return "marketplace_scraper"


def _raw_table(source: SourceConfig) -> str:
    if source.source_type.value == "supplier":
        return RAW_TABLES.get(source.source_id, "bronze.supplier_products_raw")
    return RAW_TABLES.get(source.source_id, "bronze.market_web_listings_raw")


class CollectionRunner:
    def __init__(
        self,
        *,
        registry: SourceRegistry | None = None,
        platform_config: CollectionPlatformConfig | None = None,
    ) -> None:
        settings = get_settings()
        self.registry = registry or SourceRegistry.from_directory(settings.source_registry_path)
        self.platform_config = platform_config or CollectionPlatformConfig.load()
        configure_structured_logging(settings.log_level)

    async def run_source(
        self,
        source_id: str,
        *,
        triggered_by: str = "sillo_cli",
        canary: bool = False,
        max_results: int | None = None,
        pipeline_run_id: UUID | None = None,
    ) -> CollectionResult:
        source = self.registry.get(source_id)
        if not source.enabled:
            raise ValueError(f"Source is disabled: {source.source_id}")

        owns_pipeline = pipeline_run_id is None
        if pipeline_run_id is None:
            with engine.begin() as connection:
                pipeline_run_id = create_pipeline_run(
                    connection,
                    pipeline_name="sillo_collection_v2",
                    triggered_by=triggered_by,
                    config_snapshot={"source": source.model_dump(mode="json")},
                    metadata={"sources": [source.source_id], "canary": canary},
                )

        with engine.begin() as connection:
            source_run_id = create_source_run(
                connection,
                pipeline_run_id=pipeline_run_id,
                source_name=source.source_id,
                source_type=_source_run_type(source),
                raw_table_name=_raw_table(source),
                metadata={
                    "strategy": source.strategy.value,
                    "canary": canary,
                    "collector_contract": "v2",
                },
            )

        worker_id = os.getenv("SILLO_WORKER_ID") or socket.gethostname()
        collection_context = CollectionContext(
            pipeline_run_id=str(pipeline_run_id),
            worker_id=worker_id,
            canary=canary,
            metadata={"triggered_by": triggered_by},
        )
        logger = get_collection_logger(
            run_id=str(source_run_id),
            source_id=source.source_id,
            worker_id=worker_id,
            hostname=collection_context.hostname,
        )
        logger.info(event="collection_started", strategy=source.strategy.value)

        resource_stop = asyncio.Event()
        resource_samples: list[tuple[float, float]] = []
        resource_task = asyncio.create_task(_sample_process_resources(resource_stop, resource_samples))
        try:
            query_group = source.config.get("query_group")
            queries = self.platform_config.queries_for(str(query_group) if query_group else None)
            worker_key = worker_id.split("-", 1)[0].lower()
            worker_limits = self.platform_config.workers.get(worker_key)
            effective_source = source
            if worker_limits:
                if source.strategy.uses_browser:
                    desired = worker_limits.browser_desired_concurrency
                    maximum = worker_limits.browser_max_concurrency
                else:
                    desired = worker_limits.http_desired_concurrency
                    maximum = worker_limits.http_max_concurrency
                maximum = min(maximum, source.concurrency.max_concurrency)
                desired = min(desired, maximum)
                effective_source = source.model_copy(
                    update={
                        "concurrency": source.concurrency.model_copy(
                            update={"desired_concurrency": desired, "max_concurrency": maximum}
                        )
                    }
                )
            collector = build_collector(effective_source, queries=queries, max_results=max_results)
            result = await asyncio.wait_for(
                collector.collect(str(source_run_id), collection_context),
                timeout=source.timeouts.collection_seconds,
            )
        except Exception as error:  # noqa: BLE001 - persist terminal collection state.
            classified = classify_collection_error(error)
            now = datetime.now(timezone.utc)
            terminal_status = (
                CollectionStatus.BLOCKED
                if classified.error_type is CollectionErrorType.COLLECTION_BLOCKED
                else CollectionStatus.FAILED
            )
            result = CollectionResult(
                run_id=str(source_run_id),
                pipeline_run_id=str(pipeline_run_id),
                source_id=source.source_id,
                started_at=now,
                finished_at=now,
                status=terminal_status,
                errors=[classified],
                collection_strategy=source.strategy,
                worker_id=worker_id,
                hostname=collection_context.hostname,
            )
        finally:
            resource_stop.set()
            await resource_task

        if resource_samples:
            result.peak_memory_mb = round(max(sample[0] for sample in resource_samples), 3)
            result.avg_cpu_percent = round(
                min(sum(sample[1] for sample in resource_samples) / len(resource_samples), 100.0),
                3,
            )

        metadata = result.model_dump(mode="json")
        db_status = result.status.value
        error_message = "; ".join(item.message for item in result.errors)[:4000] or None
        for item in result.errors:
            logger.error(
                event="collection_error",
                request_id=item.request_id,
                url=item.url,
                error_type=item.error_type.value,
                retryable=item.retryable,
                status_code=item.status_code,
            )
        with engine.begin() as connection:
            finish_source_run(
                connection,
                source_run_id=source_run_id,
                status=db_status,
                records_extracted=result.items_discovered,
                records_loaded=result.items_persisted,
                records_skipped=result.items_invalid + max(
                    result.items_valid - result.items_persisted,
                    0,
                ),
                metadata=metadata,
                error_message=error_message,
            )
            connection.execute(
                text(
                    """
                    UPDATE control.source_runs SET
                        duration_seconds = :duration_seconds,
                        requests_total = :requests_total,
                        requests_success = :requests_success,
                        requests_failed = :requests_failed,
                        http_requests = :http_requests,
                        browser_requests = :browser_requests,
                        retry_count = :retry_count,
                        worker_id = :worker_id,
                        hostname = :hostname,
                        collection_strategy = :collection_strategy,
                        avg_response_time_ms = :avg_response_time_ms,
                        peak_memory_mb = :peak_memory_mb,
                        avg_cpu_percent = :avg_cpu_percent
                    WHERE id = :source_run_id
                    """
                ),
                {
                    "source_run_id": source_run_id,
                    "duration_seconds": result.duration_seconds,
                    "requests_total": result.requests_total,
                    "requests_success": result.requests_success,
                    "requests_failed": result.requests_failed,
                    "http_requests": result.http_requests,
                    "browser_requests": result.browser_requests,
                    "retry_count": result.retry_count,
                    "worker_id": result.worker_id,
                    "hostname": result.hostname,
                    "collection_strategy": result.collection_strategy.value,
                    "avg_response_time_ms": result.avg_response_time_ms,
                    "peak_memory_mb": result.peak_memory_mb,
                    "avg_cpu_percent": result.avg_cpu_percent,
                },
            )
            record_quality_check(
                connection,
                pipeline_run_id=pipeline_run_id,
                source_run_id=source_run_id,
                schema_name=_raw_table(source).split(".", 1)[0],
                table_name=_raw_table(source).split(".", 1)[1],
                check_name="collection_v2_items_discovered_gt_zero",
                status="passed" if result.items_discovered > 0 else "failed",
                metric_name="items_discovered",
                metric_value=result.items_discovered,
                threshold_value=1,
                details={"worker_id": worker_id, "strategy": source.strategy.value},
                message=None if result.items_discovered else "Collector returned zero items",
            )
            alerts = evaluate_alerts(connection, source_run_id=source_run_id, result=result)
            if owns_pipeline:
                finish_pipeline_run(
                    connection,
                    pipeline_run_id=pipeline_run_id,
                    status=db_status,
                    metadata={"results": [metadata], "alert_count": len(alerts)},
                    error_message=error_message,
                )

        logger.info(
            event="collection_finished",
            status=result.status.value,
            items_persisted=result.items_persisted,
            duration_seconds=result.duration_seconds,
        )
        return result

    async def run_source_guarded(
        self,
        source_id: str,
        *,
        triggered_by: str = "sillo_cli",
        canary: bool = False,
        max_results: int | None = None,
    ) -> CollectionResult:
        """Executa canary rastreavel antes da coleta integral, salvo pedido explicito de canary."""
        source = self.registry.get(source_id)
        if canary or not source.canary_enabled:
            return await self.run_source(
                source_id,
                triggered_by=triggered_by,
                canary=canary,
                max_results=max_results,
            )
        canary_result = await self.run_source(
            source_id,
            triggered_by=f"{triggered_by}_canary",
            canary=True,
            max_results=1,
        )
        if canary_result.status in {CollectionStatus.FAILED, CollectionStatus.BLOCKED} or not canary_result.items_valid:
            return canary_result
        return await self.run_source(
            source_id,
            triggered_by=triggered_by,
            canary=False,
            max_results=max_results,
        )

    async def run_profile(
        self,
        profile: str,
        *,
        triggered_by: str = "sillo_cli_profile",
        canary: bool = False,
        max_results: int | None = None,
    ) -> dict[str, Any]:
        sources = self.registry.list(profile=profile, enabled_only=True)
        if not sources:
            raise ValueError(f"Profile has no enabled sources: {profile}")
        with engine.begin() as connection:
            pipeline_run_id = create_pipeline_run(
                connection,
                pipeline_name=f"sillo_collection_v2_{profile}",
                triggered_by=triggered_by,
                config_snapshot={"sources": [source.model_dump(mode="json") for source in sources]},
                metadata={"profile": profile, "canary": canary},
            )

        results: list[CollectionResult] = []
        for source in sources:
            if not canary and source.canary_enabled:
                canary_result = await self.run_source(
                    source.source_id,
                    triggered_by=f"{triggered_by}_canary",
                    canary=True,
                    max_results=1,
                    pipeline_run_id=pipeline_run_id,
                )
                if canary_result.status in {CollectionStatus.FAILED, CollectionStatus.BLOCKED} or not canary_result.items_valid:
                    results.append(canary_result)
                    continue
            result = await self.run_source(
                source.source_id,
                triggered_by=triggered_by,
                canary=canary,
                max_results=max_results,
                pipeline_run_id=pipeline_run_id,
            )
            results.append(result)

        statuses = {result.status for result in results}
        if statuses == {CollectionStatus.SUCCESS}:
            final_status = "success"
        elif CollectionStatus.SUCCESS in statuses or CollectionStatus.PARTIAL in statuses:
            final_status = "partial"
        else:
            final_status = "failed"
        result_payload = [result.model_dump(mode="json") for result in results]
        with engine.begin() as connection:
            finish_pipeline_run(
                connection,
                pipeline_run_id=pipeline_run_id,
                status=final_status,
                metadata={"profile": profile, "results": result_payload},
                error_message=None if final_status != "failed" else "All profile sources failed",
            )
        return {
            "pipeline_run_id": str(pipeline_run_id),
            "profile": profile,
            "status": final_status,
            "results": result_payload,
        }


def collection_status(run_id: str | None = None, *, limit: int = 25) -> dict[str, Any]:
    with engine.connect() as connection:
        if run_id:
            pipeline = connection.execute(
                text("SELECT * FROM control.pipeline_runs WHERE id = :run_id"),
                {"run_id": run_id},
            ).mappings().first()
            source = connection.execute(
                text("SELECT * FROM control.source_runs WHERE id = :run_id"),
                {"run_id": run_id},
            ).mappings().first()
            if not pipeline and not source:
                raise ValueError(f"Run not found: {run_id}")
            alerts = connection.execute(
                text(
                    """
                    SELECT id, source_name, alert_type, severity, status, message, created_at
                    FROM control.collection_alerts
                    WHERE source_run_id = :run_id OR pipeline_run_id = :run_id
                    ORDER BY created_at DESC
                    """
                ),
                {"run_id": run_id},
            ).mappings().all()
            return {
                "pipeline_run": dict(pipeline) if pipeline else None,
                "source_run": dict(source) if source else None,
                "alerts": [dict(alert) for alert in alerts],
            }
        rows = connection.execute(
            text(
                """
                SELECT id, pipeline_run_id, source_name, status, records_extracted,
                       records_loaded, records_skipped, started_at, finished_at,
                       duration_seconds, requests_total, requests_success,
                       requests_failed, retry_count, http_requests,
                       browser_requests, avg_response_time_ms, peak_memory_mb,
                       avg_cpu_percent, worker_id, hostname, collection_strategy,
                       metadata, error_message
                FROM control.source_runs
                ORDER BY started_at DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
        open_alerts = connection.execute(
            text(
                """
                SELECT id, source_name, alert_type, severity, message, created_at
                FROM control.collection_alerts
                WHERE status = 'open'
                ORDER BY created_at DESC LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
        latest = {
            row.source_name: row.finished_at
            for row in connection.execute(
                text(
                    """
                    SELECT source_name, MAX(finished_at) AS finished_at
                    FROM control.source_runs GROUP BY source_name
                    """
                )
            ).mappings()
        }
    registry = SourceRegistry.from_directory(get_settings().source_registry_path)
    now = datetime.now(timezone.utc)
    late_sources = []
    for source in registry.list(enabled_only=True):
        interval = source.scheduling.interval_hours
        finished_at = latest.get(source.source_id)
        if interval and finished_at and (now - finished_at).total_seconds() > interval * 1.5 * 3600:
            late_sources.append(
                {
                    "source_id": source.source_id,
                    "last_finished_at": finished_at,
                    "expected_interval_hours": interval,
                }
            )
    return {
        "runs": [dict(row) for row in rows],
        "open_alerts": [dict(row) for row in open_alerts],
        "late_sources": late_sources,
    }
