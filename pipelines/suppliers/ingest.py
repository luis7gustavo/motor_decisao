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
from pipelines.suppliers.generic_html import GenericHtmlSupplierScraper


@dataclass(frozen=True)
class SupplierIngestionResult:
    pipeline_run_id: UUID
    source_run_id: UUID
    status: str
    supplier_slug: str
    extracted: int
    loaded: int
    skipped: int


def _insert_snapshot(connection, *, source_run_id: UUID, snapshot) -> bool:
    row = connection.execute(
        text(
            """
            INSERT INTO bronze.supplier_products_raw (
                source_run_id,
                supplier_slug,
                source_url,
                raw_title,
                raw_price,
                raw_stock,
                sku,
                ean,
                payload,
                payload_hash
            )
            VALUES (
                :source_run_id,
                :supplier_slug,
                :source_url,
                :raw_title,
                :raw_price,
                :raw_stock,
                :sku,
                :ean,
                CAST(:payload AS jsonb),
                :payload_hash
            )
            ON CONFLICT (supplier_slug, payload_hash, fetched_date) DO NOTHING
            RETURNING id
            """
        ),
        {
            "source_run_id": source_run_id,
            "supplier_slug": snapshot.supplier_slug,
            "source_url": snapshot.source_url,
            "raw_title": snapshot.raw_title,
            "raw_price": snapshot.raw_price,
            "raw_stock": snapshot.raw_stock,
            "sku": snapshot.sku,
            "ean": snapshot.ean,
            "payload": to_json_text(snapshot.payload),
            "payload_hash": payload_hash(snapshot.payload),
        },
    ).first()
    return row is not None


def _get_supplier_config(project_config: dict[str, Any], supplier_slug: str) -> dict[str, Any]:
    suppliers = project_config.get("supplier_sources", {}).get("suppliers") or []
    for supplier_config in suppliers:
        if supplier_config.get("slug") == supplier_slug:
            return supplier_config
    raise ValueError(f"Supplier not configured: {supplier_slug}")


def ingest_supplier_html(
    *,
    supplier_slug: str,
    triggered_by: str = "local_cli",
) -> SupplierIngestionResult:
    settings = get_settings()
    project_config = settings.load_project_config()
    supplier_config = _get_supplier_config(project_config, supplier_slug)
    if not supplier_config.get("enabled", False):
        raise ValueError(f"Supplier is disabled in config: {supplier_slug}")

    metadata = {
        "supplier_slug": supplier_slug,
        "urls": supplier_config.get("urls", []),
        "selector_keys": sorted((supplier_config.get("selectors") or {}).keys()),
    }

    with engine.begin() as connection:
        pipeline_run_id = create_pipeline_run(
            connection,
            pipeline_name="supplier_html_ingestion",
            triggered_by=triggered_by,
            config_snapshot=project_config,
            metadata=metadata,
        )
        source_run_id = create_source_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_name=supplier_slug,
            source_type="supplier_scraper",
            raw_table_name="bronze.supplier_products_raw",
            metadata=metadata,
        )

    extracted = 0
    loaded = 0
    skipped = 0
    status = "success"
    error_message: str | None = None

    try:
        with GenericHtmlSupplierScraper(supplier_config=supplier_config) as scraper:
            snapshots = scraper.fetch()
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
            table_name="supplier_products_raw",
            check_name="records_loaded_gt_zero",
            status="passed" if loaded > 0 else "failed",
            metric_name="records_loaded",
            metric_value=loaded,
            threshold_value=1,
            details={"extracted": extracted, "skipped": skipped, "supplier_slug": supplier_slug},
            message=None if loaded > 0 else f"No supplier records loaded from {supplier_slug}",
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

    return SupplierIngestionResult(
        pipeline_run_id=pipeline_run_id,
        source_run_id=source_run_id,
        status=status if loaded > 0 else "partial",
        supplier_slug=supplier_slug,
        extracted=extracted,
        loaded=loaded,
        skipped=skipped,
    )

