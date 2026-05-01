from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.mercado_livre.ingest import ingest_mercado_livre_category  # noqa: E402


DEFAULT_QUERIES = [
    "mouse gamer",
    "mouse sem fio",
    "mouse bluetooth",
    "teclado gamer",
    "teclado mecanico",
    "teclado sem fio",
    "teclado bluetooth",
    "combo teclado mouse",
    "headset gamer",
    "fone bluetooth",
    "fone gamer",
    "webcam",
    "mousepad gamer",
    "suporte notebook",
    "hub usb",
    "adaptador bluetooth",
    "adaptador wifi usb",
    "cabo hdmi",
    "cabo usb c",
    "carregador usb c",
    "suporte monitor",
    "cooler notebook",
    "base notebook",
    "microfone usb",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coleta ampla de perifericos baratos no Mercado Livre."
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Termo extra. Pode ser usado varias vezes. Se omitido, usa a lista padrao.",
    )
    parser.add_argument(
        "--max-items-per-query",
        type=int,
        default=25,
        help="Maximo de ofertas com preco por termo.",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=10,
        help="Produtos de catalogo por pagina no fallback.",
    )
    parser.add_argument(
        "--catalog-product-scan-limit",
        type=int,
        default=120,
        help="Maximo de produtos de catalogo salvos por termo.",
    )
    parser.add_argument(
        "--catalog-offer-scan-limit",
        type=int,
        default=20,
        help="Maximo de produtos por termo com consulta cara de ofertas/precos.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    queries = args.queries or DEFAULT_QUERIES
    results = []

    for query in queries:
        try:
            result = ingest_mercado_livre_category(
                query=query,
                max_items=args.max_items_per_query,
                page_limit=args.page_limit,
                catalog_product_scan_limit=args.catalog_product_scan_limit,
                catalog_offer_scan_limit=args.catalog_offer_scan_limit,
                triggered_by="local_cli_perifericos_batch",
            )
            results.append(
                {
                    "query": query,
                    "status": result.status,
                    "extracted": result.extracted,
                    "loaded": result.loaded,
                    "skipped": result.skipped,
                    "pages": result.pages,
                    "price_summaries": result.price_summaries,
                    "catalog_products_loaded": result.catalog_products_loaded,
                    "catalog_products_skipped": result.catalog_products_skipped,
                    "source_run_id": str(result.source_run_id),
                }
            )
        except Exception as error:
            results.append(
                {
                    "query": query,
                    "status": "failed",
                    "error": str(error),
                }
            )

    summary = {
        "queries": len(results),
        "success": sum(1 for result in results if result["status"] == "success"),
        "partial": sum(1 for result in results if result["status"] == "partial"),
        "failed": sum(1 for result in results if result["status"] == "failed"),
        "extracted": sum(int(result.get("extracted", 0)) for result in results),
        "loaded": sum(int(result.get("loaded", 0)) for result in results),
        "skipped": sum(int(result.get("skipped", 0)) for result in results),
        "price_summaries": sum(int(result.get("price_summaries", 0)) for result in results),
        "catalog_products_loaded": sum(
            int(result.get("catalog_products_loaded", 0)) for result in results
        ),
        "catalog_products_skipped": sum(
            int(result.get("catalog_products_skipped", 0)) for result in results
        ),
        "results": results,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
