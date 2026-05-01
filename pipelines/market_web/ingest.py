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
from pipelines.market_web.base import MarketListingSnapshot
from pipelines.market_web.sources import SOURCE_CONFIGS, build_market_source


@dataclass(frozen=True)
class MarketWebIngestionResult:
    pipeline_run_id: UUID
    source_run_ids: list[UUID]
    status: str
    extracted: int
    loaded: int
    skipped: int
    blocked: int
    results: list[dict[str, Any]]


def _insert_listing(connection, *, source_run_id: UUID, snapshot: MarketListingSnapshot) -> bool:
    row = connection.execute(
        text(
            """
            INSERT INTO bronze.market_web_listings_raw (
                source_run_id,
                source_name,
                source_role,
                query,
                position,
                title,
                price,
                old_price,
                currency_id,
                sold_quantity_text,
                sold_quantity,
                demand_signal_type,
                demand_signal_value,
                bsr_text,
                rating_text,
                reviews_count,
                seller_text,
                shipping_text,
                installments_text,
                item_url,
                image_url,
                is_sponsored,
                is_full,
                is_catalog,
                blocked,
                block_reason,
                payload,
                payload_hash
            )
            VALUES (
                :source_run_id,
                :source_name,
                :source_role,
                :query,
                :position,
                :title,
                :price,
                :old_price,
                :currency_id,
                :sold_quantity_text,
                :sold_quantity,
                :demand_signal_type,
                :demand_signal_value,
                :bsr_text,
                :rating_text,
                :reviews_count,
                :seller_text,
                :shipping_text,
                :installments_text,
                :item_url,
                :image_url,
                :is_sponsored,
                :is_full,
                :is_catalog,
                :blocked,
                :block_reason,
                CAST(:payload AS jsonb),
                :payload_hash
            )
            ON CONFLICT (source_name, query, payload_hash, fetched_date) DO NOTHING
            RETURNING id
            """
        ),
        {
            "source_run_id": source_run_id,
            "source_name": snapshot.source_name,
            "source_role": snapshot.source_role,
            "query": snapshot.query,
            "position": snapshot.position,
            "title": snapshot.title,
            "price": snapshot.price,
            "old_price": snapshot.old_price,
            "currency_id": snapshot.currency_id,
            "sold_quantity_text": snapshot.sold_quantity_text,
            "sold_quantity": snapshot.sold_quantity,
            "demand_signal_type": snapshot.demand_signal_type,
            "demand_signal_value": snapshot.demand_signal_value,
            "bsr_text": snapshot.bsr_text,
            "rating_text": snapshot.rating_text,
            "reviews_count": snapshot.reviews_count,
            "seller_text": snapshot.seller_text,
            "shipping_text": snapshot.shipping_text,
            "installments_text": snapshot.installments_text,
            "item_url": snapshot.item_url,
            "image_url": snapshot.image_url,
            "is_sponsored": snapshot.is_sponsored,
            "is_full": snapshot.is_full,
            "is_catalog": snapshot.is_catalog,
            "blocked": snapshot.blocked,
            "block_reason": snapshot.block_reason,
            "payload": to_json_text(snapshot.payload),
            "payload_hash": payload_hash(snapshot.payload),
        },
    ).first()
    return row is not None


def ingest_market_web(
    *,
    sources: list[str],
    queries: list[str],
    max_results: int | None = None,
    triggered_by: str = "local_cli_market_web",
) -> MarketWebIngestionResult:
    settings = get_settings()
    project_config = settings.load_project_config()
    market_web_config = project_config.get("market_sources", {}).get("market_web", {})
    max_results = int(max_results or market_web_config.get("max_results_per_query", 30))
    source_names = [source for source in sources if source in SOURCE_CONFIGS]
    if not source_names:
        raise ValueError("No supported market web source selected.")

    metadata = {
        "sources": source_names,
        "queries": queries,
        "max_results": max_results,
        "engine": "playwright",
    }

    with engine.begin() as connection:
        pipeline_run_id = create_pipeline_run(
            connection,
            pipeline_name="market_web_ingestion",
            triggered_by=triggered_by,
            config_snapshot=project_config,
            metadata=metadata,
        )

    source_run_ids: list[UUID] = []
    results: list[dict[str, Any]] = []
    total_extracted = 0
    total_loaded = 0
    total_skipped = 0
    total_blocked = 0
    overall_status = "success"

    for source_name in source_names:
        source_config = market_web_config.get("sources", {}).get(source_name, {})
        if source_config.get("enabled", True) is False:
            continue
        with engine.begin() as connection:
            source_run_id = create_source_run(
                connection,
                pipeline_run_id=pipeline_run_id,
                source_name=source_name,
                source_type="marketplace_scraper",
                raw_table_name="bronze.market_web_listings_raw",
                metadata={"queries": queries, "max_results": max_results},
            )
        source_run_ids.append(source_run_id)

        extracted = 0
        loaded = 0
        skipped = 0
        blocked = 0
        error_message = None
        source_status = "success"

        try:
            source = build_market_source(
                source_name,
                headless=bool(market_web_config.get("headless", True)),
                locale=market_web_config.get("locale", "pt-BR"),
                timezone_id=market_web_config.get("timezone_id", "America/Sao_Paulo"),
                navigation_timeout_seconds=int(
                    market_web_config.get("navigation_timeout_seconds", 45)
                ),
                action_timeout_seconds=int(market_web_config.get("action_timeout_seconds", 15)),
                scroll_steps=int(market_web_config.get("scroll_steps", 4)),
                scroll_pause_ms=int(market_web_config.get("scroll_pause_ms", 900)),
                rate_limit_ms=int(market_web_config.get("rate_limit_ms", 2500)),
            )
            for query in queries:
                snapshots = source.fetch(query=query, max_results=max_results)
                with engine.begin() as connection:
                    for snapshot in snapshots:
                        extracted += 1
                        if snapshot.blocked:
                            blocked += 1
                        if _insert_listing(connection, source_run_id=source_run_id, snapshot=snapshot):
                            loaded += 1
                        else:
                            skipped += 1
        except Exception as error:
            source_status = "failed"
            overall_status = "partial" if total_loaded > 0 else "failed"
            error_message = str(error)

        total_extracted += extracted
        total_loaded += loaded
        total_skipped += skipped
        total_blocked += blocked
        if source_status == "failed" and overall_status != "failed":
            overall_status = "partial"

        source_result = {
            "source_name": source_name,
            "status": source_status,
            "extracted": extracted,
            "loaded": loaded,
            "skipped": skipped,
            "blocked": blocked,
            "error": error_message,
        }
        results.append(source_result)
        with engine.begin() as connection:
            finish_source_run(
                connection,
                source_run_id=source_run_id,
                status=source_status if loaded > 0 or blocked > 0 else "partial",
                records_extracted=extracted,
                records_loaded=loaded,
                records_skipped=skipped,
                metadata=source_result,
                error_message=error_message,
            )
            record_quality_check(
                connection,
                pipeline_run_id=pipeline_run_id,
                source_run_id=source_run_id,
                schema_name="bronze",
                table_name="market_web_listings_raw",
                check_name="records_loaded_gt_zero",
                status="passed" if loaded > 0 else "failed",
                metric_name="records_loaded",
                metric_value=loaded,
                threshold_value=1,
                details=source_result,
                message=None if loaded > 0 else "No web listings loaded",
            )

    final_status = (
        "success"
        if total_loaded > 0 and all(result["status"] == "success" for result in results)
        else ("partial" if total_loaded > 0 else overall_status)
    )
    with engine.begin() as connection:
        finish_pipeline_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            status=final_status,
            metadata={
                **metadata,
                "records_extracted": total_extracted,
                "records_loaded": total_loaded,
                "records_skipped": total_skipped,
                "blocked": total_blocked,
                "results": results,
            },
            error_message=None if final_status != "failed" else "No market web listings loaded",
        )

    return MarketWebIngestionResult(
        pipeline_run_id=pipeline_run_id,
        source_run_ids=source_run_ids,
        status=final_status,
        extracted=total_extracted,
        loaded=total_loaded,
        skipped=total_skipped,
        blocked=total_blocked,
        results=results,
    )
