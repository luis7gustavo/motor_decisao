from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.database import engine  # noqa: E402
from app.core.settings import get_settings  # noqa: E402
from pipelines.common.run_manager import (  # noqa: E402
    create_pipeline_run,
    create_source_run,
    finish_pipeline_run,
    finish_source_run,
    record_quality_check,
)
from pipelines.mercado_livre.client import MercadoLivreClient  # noqa: E402
from pipelines.mercado_livre.ingest import _insert_raw_product  # noqa: E402
from scripts.collect_mercado_livre_perifericos import DEFAULT_QUERIES  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coleta rapida de catalogo do Mercado Livre sem buscar ofertas."
    )
    parser.add_argument("--query", action="append", dest="queries")
    parser.add_argument("--limit-per-page", type=int, default=50)
    parser.add_argument("--pages-per-query", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    queries = args.queries or DEFAULT_QUERIES
    settings = get_settings()
    project_config = settings.load_project_config()
    pipeline_config = project_config["pipeline"]
    source_config = project_config["market_sources"]["mercado_livre"]
    site_id = pipeline_config.get("site_id", "MLB")
    limit = min(max(args.limit_per_page, 1), 50)
    pages_per_query = max(args.pages_per_query, 1)

    metadata = {
        "site_id": site_id,
        "queries": queries,
        "limit_per_page": limit,
        "pages_per_query": pages_per_query,
        "mode": "catalog_fast",
    }

    with engine.begin() as connection:
        pipeline_run_id = create_pipeline_run(
            connection,
            pipeline_name="mercado_livre_catalog_fast_ingestion",
            triggered_by="local_cli_catalog_fast",
            config_snapshot=project_config,
            metadata=metadata,
        )
        source_run_id = create_source_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_name="mercado_livre",
            source_type="marketplace_api",
            raw_table_name="bronze.mercado_livre_products_raw",
            metadata=metadata,
        )

    extracted = 0
    loaded = 0
    skipped = 0
    results = []
    status = "success"
    error_message = None

    try:
        with MercadoLivreClient(
            api_base=source_config.get("api_base", settings.ml_api_base),
            site_id=site_id,
            timeout_seconds=int(source_config.get("timeout_seconds", 20)),
            max_retries=int(source_config.get("max_retries", 3)),
            rate_limit_ms=int(source_config.get("rate_limit_ms", 250)),
            access_token=os.getenv("ML_ACCESS_TOKEN") or None,
        ) as client:
            for query in queries:
                query_extracted = 0
                query_loaded = 0
                query_skipped = 0
                query_status = "success"
                query_error = None

                try:
                    for page_index in range(pages_per_query):
                        page = client.search_catalog_products(
                            query=query,
                            offset=page_index * limit,
                            limit=limit,
                        )
                        products = page.results
                        if not products:
                            break

                        with engine.begin() as connection:
                            for product in products:
                                extracted += 1
                                query_extracted += 1
                                inserted = _insert_raw_product(
                                    connection,
                                    source_run_id=source_run_id,
                                    site_id=site_id,
                                    query=query,
                                    product=product,
                                )
                                if inserted:
                                    loaded += 1
                                    query_loaded += 1
                                else:
                                    skipped += 1
                                    query_skipped += 1

                        if page.total is not None and page.offset + page.limit >= page.total:
                            break
                except Exception as error:
                    query_status = "failed"
                    query_error = str(error)
                    status = "partial"

                results.append(
                    {
                        "query": query,
                        "status": query_status,
                        "extracted": query_extracted,
                        "loaded": query_loaded,
                        "skipped": query_skipped,
                        "error": query_error,
                    }
                )
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
            metadata={**metadata, "results": results},
            error_message=error_message,
        )
        record_quality_check(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_run_id=source_run_id,
            schema_name="bronze",
            table_name="mercado_livre_products_raw",
            check_name="catalog_products_loaded_gt_zero",
            status="passed" if loaded > 0 else "failed",
            metric_name="records_loaded",
            metric_value=loaded,
            threshold_value=1,
            details={"extracted": extracted, "skipped": skipped},
            message=None if loaded > 0 else "No catalog products loaded",
        )
        finish_pipeline_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            status=status if status == "failed" else ("success" if loaded > 0 else "partial"),
            metadata={**metadata, "records_extracted": extracted, "records_loaded": loaded},
            error_message=error_message,
        )

    output = {
        "pipeline_run_id": str(pipeline_run_id),
        "source_run_id": str(source_run_id),
        "status": status if loaded > 0 else "partial",
        "queries": len(queries),
        "extracted": extracted,
        "loaded": loaded,
        "skipped": skipped,
        "results": results,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
