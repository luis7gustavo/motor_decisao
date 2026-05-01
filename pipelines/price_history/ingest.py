from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import text

from app.core.database import engine
from app.core.settings import get_settings
from pipelines.common.run_manager import (
    create_pipeline_run,
    create_source_run,
    finish_pipeline_run,
    finish_source_run,
    record_quality_check,
)
from pipelines.common.serialization import payload_hash, to_json_text
from pipelines.price_history.comparison_scraper import ComparisonSearchScraper
from pipelines.price_history.comparison_web_scraper import ComparisonWebHistoryScraper


@dataclass(frozen=True)
class PriceComparisonIngestionResult:
    pipeline_run_id: UUID
    source_run_id: UUID
    status: str
    source_name: str
    query: str
    extracted: int
    loaded: int
    skipped: int
    blocked: int = 0


def _insert_snapshot(connection, *, source_run_id: UUID, snapshot) -> bool:
    row = connection.execute(
        text(
            """
            INSERT INTO bronze.price_history_raw (
                source_run_id,
                source_name,
                product_key,
                product_url,
                title,
                current_price,
                min_price,
                median_price,
                max_price,
                avg_price,
                history_window_days,
                query,
                source_mode,
                blocked,
                block_reason,
                payload,
                payload_hash
            )
            VALUES (
                :source_run_id,
                :source_name,
                :product_key,
                :product_url,
                :title,
                :current_price,
                :min_price,
                :median_price,
                :max_price,
                :avg_price,
                :history_window_days,
                :query,
                :source_mode,
                :blocked,
                :block_reason,
                CAST(:payload AS jsonb),
                :payload_hash
            )
            ON CONFLICT (source_name, payload_hash, fetched_date) DO NOTHING
            RETURNING id
            """
        ),
        {
            "source_run_id": source_run_id,
            "source_name": snapshot.source_name,
            "product_key": snapshot.product_key,
            "product_url": snapshot.product_url,
            "title": snapshot.title,
            "current_price": snapshot.current_price,
            "min_price": snapshot.min_price,
            "median_price": snapshot.median_price,
            "max_price": snapshot.max_price,
            "avg_price": snapshot.avg_price,
            "history_window_days": snapshot.history_window_days,
            "query": snapshot.query,
            "source_mode": snapshot.source_mode,
            "blocked": snapshot.blocked,
            "block_reason": snapshot.block_reason,
            "payload": to_json_text(snapshot.payload),
            "payload_hash": payload_hash(snapshot.payload),
        },
    ).first()
    return row is not None


def ingest_price_comparison_search(
    *,
    source_name: str = "buscape",
    query: str,
    max_results: int | None = None,
    triggered_by: str = "local_cli",
) -> PriceComparisonIngestionResult:
    settings = get_settings()
    project_config = settings.load_project_config()
    source_config = project_config["market_sources"]["price_history"]
    result_limit = int(max_results or source_config.get("max_results", 30))

    metadata: dict[str, Any] = {
        "source_name": source_name,
        "query": query,
        "max_results": result_limit,
        "note": "comparison search current prices; not full historical series yet",
    }

    with engine.begin() as connection:
        pipeline_run_id = create_pipeline_run(
            connection,
            pipeline_name="price_comparison_search_ingestion",
            triggered_by=triggered_by,
            config_snapshot=project_config,
            metadata=metadata,
        )
        source_run_id = create_source_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_name=source_name,
            source_type="price_history",
            raw_table_name="bronze.price_history_raw",
            metadata=metadata,
        )

    extracted = 0
    loaded = 0
    skipped = 0
    status = "success"
    error_message: str | None = None

    try:
        with ComparisonSearchScraper(
            source_name=source_name,
            timeout_seconds=int(source_config.get("timeout_seconds", 30)),
            rate_limit_ms=int(source_config.get("rate_limit_ms", 1000)),
        ) as scraper:
            snapshots = scraper.fetch(query=query, max_results=result_limit)
        extracted = len(snapshots)

        with engine.begin() as connection:
            for snapshot in snapshots:
                if _insert_snapshot(connection, source_run_id=source_run_id, snapshot=snapshot):
                    loaded += 1
                else:
                    skipped += 1
    except Exception as error:
        status = "failed"
        error_message = str(error)

    with engine.begin() as connection:
        finish_source_run(
            connection,
            source_run_id=source_run_id,
            status=status,
            records_extracted=extracted,
            records_loaded=loaded,
            records_skipped=skipped,
            metadata=metadata,
            error_message=error_message,
        )
        record_quality_check(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_run_id=source_run_id,
            schema_name="bronze",
            table_name="price_history_raw",
            check_name="records_loaded_gt_zero",
            status="passed" if loaded > 0 else "failed",
            metric_name="records_loaded",
            metric_value=loaded,
            threshold_value=1,
            details={"extracted": extracted, "skipped": skipped, "source_name": source_name},
            message=None if loaded > 0 else f"No comparison records loaded from {source_name}",
        )
        finish_pipeline_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            status=status if status == "failed" else ("success" if loaded > 0 else "partial"),
            metadata={
                **metadata,
                "records_extracted": extracted,
                "records_loaded": loaded,
                "records_skipped": skipped,
            },
            error_message=error_message,
        )

    if error_message:
        raise RuntimeError(error_message)

    return PriceComparisonIngestionResult(
        pipeline_run_id=pipeline_run_id,
        source_run_id=source_run_id,
        status=status if loaded > 0 else "partial",
        source_name=source_name,
        query=query,
        extracted=extracted,
        loaded=loaded,
        skipped=skipped,
        blocked=0,
    )


def ingest_price_comparison_web_history(
    *,
    source_name: str = "buscape",
    query: str,
    max_results: int | None = None,
    product_detail_limit: int | None = None,
    triggered_by: str = "local_cli",
) -> PriceComparisonIngestionResult:
    settings = get_settings()
    project_config = settings.load_project_config()
    source_config = project_config["market_sources"]["price_history"]
    web_config = source_config.get("web_history", {})
    result_limit = int(max_results or web_config.get("max_results", source_config.get("max_results", 30)))
    detail_limit = int(product_detail_limit or web_config.get("product_detail_limit", result_limit))

    metadata: dict[str, Any] = {
        "source_name": source_name,
        "query": query,
        "max_results": result_limit,
        "product_detail_limit": detail_limit,
        "note": "comparison product pages via Playwright; captures raw page state, offers and visible price-history summary",
    }

    with engine.begin() as connection:
        pipeline_run_id = create_pipeline_run(
            connection,
            pipeline_name="price_comparison_web_history_ingestion",
            triggered_by=triggered_by,
            config_snapshot=project_config,
            metadata=metadata,
        )
        source_run_id = create_source_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_name=source_name,
            source_type="price_history",
            raw_table_name="bronze.price_history_raw",
            metadata=metadata,
        )

    extracted = 0
    loaded = 0
    skipped = 0
    blocked = 0
    status = "success"
    error_message: str | None = None

    try:
        with ComparisonWebHistoryScraper(
            source_name=source_name,
            headless=bool(web_config.get("headless", True)),
            navigation_timeout_seconds=int(web_config.get("navigation_timeout_seconds", 45)),
            action_timeout_seconds=int(web_config.get("action_timeout_seconds", 15)),
            scroll_steps=int(web_config.get("scroll_steps", 5)),
            scroll_pause_ms=int(web_config.get("scroll_pause_ms", 900)),
            rate_limit_ms=int(web_config.get("rate_limit_ms", source_config.get("rate_limit_ms", 1000))),
            max_body_chars=int(web_config.get("max_body_chars", 50000)),
        ) as scraper:
            snapshots = scraper.fetch(
                query=query,
                max_results=result_limit,
                product_detail_limit=detail_limit,
            )
        extracted = len(snapshots)
        blocked = sum(1 for snapshot in snapshots if snapshot.blocked)

        with engine.begin() as connection:
            for snapshot in snapshots:
                if _insert_snapshot(connection, source_run_id=source_run_id, snapshot=snapshot):
                    loaded += 1
                else:
                    skipped += 1
    except Exception as error:
        status = "failed"
        error_message = str(error)

    with engine.begin() as connection:
        final_status = status if status == "failed" else ("success" if loaded > 0 else "partial")
        finish_source_run(
            connection,
            source_run_id=source_run_id,
            status=final_status,
            records_extracted=extracted,
            records_loaded=loaded,
            records_skipped=skipped,
            metadata={**metadata, "blocked": blocked},
            error_message=error_message,
        )
        record_quality_check(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_run_id=source_run_id,
            schema_name="bronze",
            table_name="price_history_raw",
            check_name="web_history_records_loaded_gt_zero",
            status="passed" if loaded > 0 else "failed",
            metric_name="records_loaded",
            metric_value=loaded,
            threshold_value=1,
            details={
                "extracted": extracted,
                "skipped": skipped,
                "blocked": blocked,
                "source_name": source_name,
            },
            message=None if loaded > 0 else f"No web history records loaded from {source_name}",
        )
        finish_pipeline_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            status=final_status,
            metadata={
                **metadata,
                "records_extracted": extracted,
                "records_loaded": loaded,
                "records_skipped": skipped,
                "blocked": blocked,
            },
            error_message=error_message,
        )

    if error_message:
        raise RuntimeError(error_message)

    return PriceComparisonIngestionResult(
        pipeline_run_id=pipeline_run_id,
        source_run_id=source_run_id,
        status="success" if loaded > 0 and blocked == 0 else ("partial" if loaded > 0 else "partial"),
        source_name=source_name,
        query=query,
        extracted=extracted,
        loaded=loaded,
        skipped=skipped,
        blocked=blocked,
    )
