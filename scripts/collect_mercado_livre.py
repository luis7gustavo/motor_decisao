from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.mercado_livre.ingest import ingest_mercado_livre_category  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coleta itens do Mercado Livre para Bronze.")
    parser.add_argument("--category-id", default=None, help="Categoria ML, ex: MLB1051.")
    parser.add_argument("--query", default=None, help="Termo de busca opcional.")
    parser.add_argument("--max-items", type=int, default=None, help="Maximo de itens a coletar.")
    parser.add_argument("--page-limit", type=int, default=50, help="Itens por pagina, maximo 50.")
    parser.add_argument("--sort", default="sold_quantity_desc", help="Ordenacao da API.")
    parser.add_argument(
        "--catalog-product-scan-limit",
        type=int,
        default=None,
        help="Maximo de produtos de catalogo a testar no fallback de precos.",
    )
    parser.add_argument(
        "--catalog-offer-scan-limit",
        type=int,
        default=None,
        help="Maximo de produtos de catalogo com consulta de ofertas/precos.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = ingest_mercado_livre_category(
        category_id=args.category_id,
        query=args.query,
        max_items=args.max_items,
        page_limit=args.page_limit,
        sort=args.sort,
        catalog_product_scan_limit=args.catalog_product_scan_limit,
        catalog_offer_scan_limit=args.catalog_offer_scan_limit,
    )
    print(
        json.dumps(
            {
                "pipeline_run_id": str(result.pipeline_run_id),
                "source_run_id": str(result.source_run_id),
                "status": result.status,
                "category_id": result.category_id,
                "query": result.query,
                "extracted": result.extracted,
                "loaded": result.loaded,
                "skipped": result.skipped,
                "pages": result.pages,
                "price_summaries": result.price_summaries,
                "catalog_products_loaded": result.catalog_products_loaded,
                "catalog_products_skipped": result.catalog_products_skipped,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
