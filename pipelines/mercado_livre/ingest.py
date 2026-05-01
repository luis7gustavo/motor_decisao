from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
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
from pipelines.mercado_livre.client import MercadoLivreApiError, MercadoLivreClient


@dataclass(frozen=True)
class MercadoLivreIngestionResult:
    pipeline_run_id: UUID
    source_run_id: UUID
    status: str
    category_id: str | None
    query: str | None
    extracted: int
    loaded: int
    skipped: int
    pages: int
    price_summaries: int = 0
    catalog_products_loaded: int = 0
    catalog_products_skipped: int = 0


def _extract_item_fields(item: dict[str, Any]) -> dict[str, Any]:
    seller = item.get("seller") or {}
    buy_box_winner = item.get("buy_box_winner") or {}
    if not isinstance(buy_box_winner, dict):
        buy_box_winner = {}
    seller_id = seller.get("id") if isinstance(seller, dict) else None
    seller_id = item.get("seller_id") if seller_id is None else seller_id
    return {
        "external_id": str(item.get("id") or item.get("item_id") or ""),
        "title": item.get("title") or item.get("name") or item.get("_catalog_product_name"),
        "price": item.get("price") or buy_box_winner.get("price"),
        "currency_id": item.get("currency_id") or buy_box_winner.get("currency_id"),
        "seller_id": str(seller_id) if seller_id is not None else None,
        "permalink": item.get("permalink"),
    }


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _offer_with_catalog_context(
    *,
    product: dict[str, Any],
    offer: dict[str, Any],
) -> dict[str, Any]:
    product_id = str(product.get("id") or "")
    product_name = product.get("name")
    return {
        **offer,
        "_catalog_product_id": product_id,
        "_catalog_product_name": product_name,
        "_catalog_domain_id": product.get("domain_id"),
        "_source_endpoint": f"/products/{product_id}/items",
    }


def _insert_raw_item(
    connection,
    *,
    source_run_id: UUID,
    site_id: str,
    category_id: str | None,
    query: str | None,
    item: dict[str, Any],
) -> bool:
    fields = _extract_item_fields(item)
    if not fields["external_id"]:
        return False

    row = connection.execute(
        text(
            """
            INSERT INTO bronze.mercado_livre_items_raw (
                source_run_id,
                source_name,
                site_id,
                category_id,
                query,
                external_id,
                title,
                price,
                currency_id,
                seller_id,
                permalink,
                payload,
                payload_hash
            )
            VALUES (
                :source_run_id,
                'mercado_livre',
                :site_id,
                :category_id,
                :query,
                :external_id,
                :title,
                :price,
                :currency_id,
                :seller_id,
                :permalink,
                CAST(:payload AS jsonb),
                :payload_hash
            )
            ON CONFLICT (source_name, external_id, fetched_date) DO NOTHING
            RETURNING id
            """
        ),
        {
            "source_run_id": source_run_id,
            "site_id": site_id,
            "category_id": category_id,
            "query": query,
            "external_id": fields["external_id"],
            "title": fields["title"],
            "price": fields["price"],
            "currency_id": fields["currency_id"],
            "seller_id": fields["seller_id"],
            "permalink": fields["permalink"],
            "payload": to_json_text(item),
            "payload_hash": payload_hash(item),
        },
    ).first()
    return row is not None


def _insert_raw_product(
    connection,
    *,
    source_run_id: UUID,
    site_id: str,
    query: str | None,
    product: dict[str, Any],
) -> bool:
    product_id = str(product.get("id") or "")
    if not product_id:
        return False

    row = connection.execute(
        text(
            """
            INSERT INTO bronze.mercado_livre_products_raw (
                source_run_id,
                source_name,
                site_id,
                query,
                product_id,
                product_name,
                domain_id,
                status,
                payload,
                payload_hash
            )
            VALUES (
                :source_run_id,
                'mercado_livre',
                :site_id,
                :query,
                :product_id,
                :product_name,
                :domain_id,
                :status,
                CAST(:payload AS jsonb),
                :payload_hash
            )
            ON CONFLICT (source_name, product_id, query, fetched_date) DO NOTHING
            RETURNING id
            """
        ),
        {
            "source_run_id": source_run_id,
            "site_id": site_id,
            "query": query,
            "product_id": product_id,
            "product_name": product.get("name"),
            "domain_id": product.get("domain_id"),
            "status": product.get("status"),
            "payload": to_json_text(product),
            "payload_hash": payload_hash(product),
        },
    ).first()
    return row is not None


def _upsert_product_price_summary(
    connection,
    *,
    source_run_id: UUID,
    site_id: str,
    query: str | None,
    product: dict[str, Any],
    offers: list[dict[str, Any]],
) -> bool:
    product_id = str(product.get("id") or "")
    if not product_id:
        return False

    prices_by_currency: dict[str, list[Decimal]] = {}
    for offer in offers:
        price = _decimal_or_none(offer.get("price"))
        currency_id = offer.get("currency_id") or "BRL"
        if price is None:
            continue
        prices_by_currency.setdefault(str(currency_id), []).append(price)

    inserted_or_updated = False
    for currency_id, prices in prices_by_currency.items():
        if not prices:
            continue
        min_price = min(prices)
        max_price = max(prices)
        avg_price = sum(prices) / Decimal(len(prices))
        payload = {
            "product": product,
            "offers": offers,
            "price_source": "/products/{product_id}/items",
        }

        row = connection.execute(
            text(
                """
                INSERT INTO silver.mercado_livre_product_prices (
                    source_run_id,
                    source_name,
                    site_id,
                    query,
                    product_id,
                    product_name,
                    domain_id,
                    min_price,
                    avg_price,
                    max_price,
                    offers_count,
                    currency_id,
                    payload,
                    payload_hash
                )
                VALUES (
                    :source_run_id,
                    'mercado_livre',
                    :site_id,
                    :query,
                    :product_id,
                    :product_name,
                    :domain_id,
                    :min_price,
                    :avg_price,
                    :max_price,
                    :offers_count,
                    :currency_id,
                    CAST(:payload AS jsonb),
                    :payload_hash
                )
                ON CONFLICT (source_name, product_id, query, currency_id, fetched_date)
                DO UPDATE SET
                    source_run_id = EXCLUDED.source_run_id,
                    product_name = EXCLUDED.product_name,
                    domain_id = EXCLUDED.domain_id,
                    min_price = EXCLUDED.min_price,
                    avg_price = EXCLUDED.avg_price,
                    max_price = EXCLUDED.max_price,
                    offers_count = EXCLUDED.offers_count,
                    payload = EXCLUDED.payload,
                    payload_hash = EXCLUDED.payload_hash,
                    fetched_at = NOW()
                RETURNING id
                """
            ),
            {
                "source_run_id": source_run_id,
                "site_id": site_id,
                "query": query,
                "product_id": product_id,
                "product_name": product.get("name"),
                "domain_id": product.get("domain_id"),
                "min_price": min_price,
                "avg_price": avg_price,
                "max_price": max_price,
                "offers_count": len(prices),
                "currency_id": currency_id,
                "payload": to_json_text(payload),
                "payload_hash": payload_hash(payload),
            },
        ).first()
        inserted_or_updated = inserted_or_updated or row is not None

    return inserted_or_updated


def ingest_mercado_livre_category(
    *,
    category_id: str | None = None,
    query: str | None = None,
    max_items: int | None = None,
    page_limit: int = 50,
    sort: str | None = "sold_quantity_desc",
    catalog_product_scan_limit: int | None = None,
    catalog_offer_scan_limit: int | None = None,
    triggered_by: str = "local_cli",
) -> MercadoLivreIngestionResult:
    settings = get_settings()
    project_config = settings.load_project_config()
    pipeline_config = project_config["pipeline"]
    source_config = project_config["market_sources"]["mercado_livre"]

    site_id = pipeline_config.get("site_id", "MLB")
    category = category_id or pipeline_config.get("initial_category_id")
    item_limit = int(max_items or pipeline_config.get("max_items_per_category", 1000))
    item_limit = max(1, min(item_limit, 1000))
    resolved_catalog_product_scan_limit = int(
        catalog_product_scan_limit
        or source_config.get("catalog_product_scan_limit", min(max(item_limit * 3, 10), 50))
    )
    resolved_catalog_product_scan_limit = max(1, resolved_catalog_product_scan_limit)
    resolved_catalog_offer_scan_limit = int(
        catalog_offer_scan_limit
        or source_config.get("catalog_offer_scan_limit", resolved_catalog_product_scan_limit)
    )
    resolved_catalog_offer_scan_limit = max(0, resolved_catalog_offer_scan_limit)

    metadata = {
        "site_id": site_id,
        "category_id": category,
        "query": query,
        "max_items": item_limit,
        "page_limit": min(page_limit, 50),
        "sort": sort,
        "search_mode": "items",
        "catalog_product_scan_limit": resolved_catalog_product_scan_limit,
        "catalog_offer_scan_limit": resolved_catalog_offer_scan_limit,
    }

    with engine.begin() as connection:
        pipeline_run_id = create_pipeline_run(
            connection,
            pipeline_name="mercado_livre_category_ingestion",
            triggered_by=triggered_by,
            config_snapshot=project_config,
            metadata=metadata,
        )
        source_run_id = create_source_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_name="mercado_livre",
            source_type="marketplace_api",
            raw_table_name="bronze.mercado_livre_items_raw",
            metadata=metadata,
        )

    extracted = 0
    loaded = 0
    skipped = 0
    pages = 0
    status = "success"
    error_message: str | None = None
    search_mode = "items"
    catalog_products_seen = 0
    catalog_offer_products_seen = 0
    catalog_products_loaded = 0
    catalog_products_skipped = 0
    catalog_products_without_offers = 0
    catalog_product_item_errors = 0
    price_summaries = 0

    try:
        with MercadoLivreClient(
            api_base=source_config.get("api_base", "https://api.mercadolibre.com"),
            site_id=site_id,
            timeout_seconds=int(source_config.get("timeout_seconds", 20)),
            max_retries=int(source_config.get("max_retries", 3)),
            rate_limit_ms=int(source_config.get("rate_limit_ms", 250)),
            access_token=os.getenv("ML_ACCESS_TOKEN") or None,
        ) as client:
            offset = 0
            while extracted < item_limit:
                remaining = item_limit - extracted
                try:
                    if search_mode == "catalog_offers":
                        if not query:
                            raise MercadoLivreApiError(
                                "Catalog fallback requires a query. Use --query."
                            )
                        catalog_page = client.search_catalog_products(
                            query=query,
                            offset=offset,
                            limit=min(page_limit, remaining, 50),
                        )
                        pages += 1
                        if not catalog_page.results:
                            break

                        catalog_products: list[dict[str, Any]] = []
                        product_offer_groups: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
                        for product in catalog_page.results:
                            if catalog_products_seen >= resolved_catalog_product_scan_limit:
                                break
                            catalog_products_seen += 1
                            product_id = str(product.get("id") or "")
                            if not product_id:
                                catalog_products_without_offers += 1
                                continue
                            catalog_products.append(product)
                            if catalog_offer_products_seen >= resolved_catalog_offer_scan_limit:
                                continue
                            catalog_offer_products_seen += 1
                            try:
                                offers_page = client.search_product_items(
                                    product_id=product_id,
                                    limit=50,
                                )
                            except MercadoLivreApiError:
                                catalog_product_item_errors += 1
                                continue
                            offers = [
                                _offer_with_catalog_context(product=product, offer=offer)
                                for offer in offers_page.results
                            ]
                            if not offers:
                                catalog_products_without_offers += 1
                                continue
                            product_offer_groups.append((product, offers))

                        with engine.begin() as connection:
                            for product in catalog_products:
                                inserted_product = _insert_raw_product(
                                    connection,
                                    source_run_id=source_run_id,
                                    site_id=site_id,
                                    query=query,
                                    product=product,
                                )
                                if inserted_product:
                                    catalog_products_loaded += 1
                                else:
                                    catalog_products_skipped += 1

                            for product, offers in product_offer_groups:
                                if _upsert_product_price_summary(
                                    connection,
                                    source_run_id=source_run_id,
                                    site_id=site_id,
                                    query=query,
                                    product=product,
                                    offers=offers,
                                ):
                                    price_summaries += 1
                                for offer in offers:
                                    if extracted >= item_limit:
                                        break
                                    extracted += 1
                                    inserted = _insert_raw_item(
                                        connection,
                                        source_run_id=source_run_id,
                                        site_id=site_id,
                                        category_id=offer.get("category_id") or category,
                                        query=query,
                                        item=offer,
                                    )
                                    if inserted:
                                        loaded += 1
                                    else:
                                        skipped += 1
                                if extracted >= item_limit:
                                    break

                        offset += catalog_page.limit
                        if (
                            catalog_products_seen >= resolved_catalog_product_scan_limit
                            or catalog_page.total is not None
                            and offset >= catalog_page.total
                        ):
                            break
                        continue
                    else:
                        page = client.search_items(
                            category_id=category,
                            query=query,
                            offset=offset,
                            limit=min(page_limit, remaining, 50),
                            sort=sort,
                        )
                except MercadoLivreApiError as error:
                    if "Status 403" not in str(error) or not query or search_mode == "catalog_offers":
                        raise
                    search_mode = "catalog_offers"
                    offset = 0
                    continue
                pages += 1
                if not page.results:
                    break

                with engine.begin() as connection:
                    for item in page.results:
                        extracted += 1
                        inserted = _insert_raw_item(
                            connection,
                            source_run_id=source_run_id,
                            site_id=site_id,
                            category_id=category,
                            query=query,
                            item=item,
                        )
                        if inserted:
                            loaded += 1
                        else:
                            skipped += 1

                offset += page.limit
                if page.total is not None and offset >= page.total:
                    break

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
            metadata={
                **metadata,
                "pages": pages,
                "search_mode": search_mode,
                "catalog_products_seen": catalog_products_seen,
                "catalog_offer_products_seen": catalog_offer_products_seen,
                "catalog_products_loaded": catalog_products_loaded,
                "catalog_products_skipped": catalog_products_skipped,
                "catalog_products_without_offers": catalog_products_without_offers,
                "catalog_product_item_errors": catalog_product_item_errors,
                "price_summaries": price_summaries,
            },
            error_message=error_message,
        )
        record_quality_check(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_run_id=source_run_id,
            schema_name="bronze",
            table_name="mercado_livre_items_raw",
            check_name="records_loaded_or_price_summary_gt_zero",
            status=(
                "passed"
                if loaded > 0 or price_summaries > 0 or catalog_products_loaded > 0
                else "failed"
            ),
            metric_name="records_loaded",
            metric_value=loaded,
            threshold_value=1,
            details={
                "extracted": extracted,
                "skipped": skipped,
                "pages": pages,
                "price_summaries": price_summaries,
                "catalog_products_loaded": catalog_products_loaded,
            },
            message=(
                None
                if loaded > 0 or price_summaries > 0 or catalog_products_loaded > 0
                else "No new Mercado Livre items, products or price summaries loaded"
            ),
        )
        finish_pipeline_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            status=(
                status
                if status == "failed"
                else (
                    "success"
                    if loaded > 0 or price_summaries > 0 or catalog_products_loaded > 0
                    else "partial"
                )
            ),
            metadata={
                **metadata,
                "records_extracted": extracted,
                "records_loaded": loaded,
                "records_skipped": skipped,
                "pages": pages,
                "search_mode": search_mode,
                "catalog_products_seen": catalog_products_seen,
                "catalog_offer_products_seen": catalog_offer_products_seen,
                "catalog_products_loaded": catalog_products_loaded,
                "catalog_products_skipped": catalog_products_skipped,
                "catalog_products_without_offers": catalog_products_without_offers,
                "catalog_product_item_errors": catalog_product_item_errors,
                "price_summaries": price_summaries,
            },
            error_message=error_message,
        )

    if error_message:
        raise RuntimeError(error_message)

    return MercadoLivreIngestionResult(
        pipeline_run_id=pipeline_run_id,
        source_run_id=source_run_id,
        status=(
            status
            if loaded > 0 or price_summaries > 0 or catalog_products_loaded > 0
            else "partial"
        ),
        category_id=category,
        query=query,
        extracted=extracted,
        loaded=loaded,
        skipped=skipped,
        pages=pages,
        price_summaries=price_summaries,
        catalog_products_loaded=catalog_products_loaded,
        catalog_products_skipped=catalog_products_skipped,
    )
