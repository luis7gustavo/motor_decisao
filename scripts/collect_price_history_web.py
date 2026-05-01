from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.price_history.ingest import ingest_price_comparison_web_history  # noqa: E402


DEFAULT_QUERIES = [
    "mouse gamer",
    "mouse sem fio",
    "teclado mecanico",
    "teclado gamer",
    "combo teclado mouse",
    "headset gamer",
    "fone bluetooth",
    "hub usb",
    "adaptador wifi usb",
    "cabo usb c",
    "cabo hdmi",
    "mousepad gamer",
    "webcam",
    "carregador usb c",
    "suporte notebook",
    "base notebook",
    "adaptador bluetooth",
    "microfone usb",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coleta web de historico de precos em Buscape/Zoom."
    )
    parser.add_argument(
        "--source",
        action="append",
        choices=["buscape", "zoom", "all"],
        default=None,
        help="Fonte a coletar. Pode repetir. Use all para Buscape e Zoom.",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Termo de busca. Pode repetir. Se omitido, usa lista padrao de perifericos baratos.",
    )
    parser.add_argument("--max-results", type=int, default=8)
    parser.add_argument(
        "--product-detail-limit",
        type=int,
        default=None,
        help="Maximo de paginas de produto abertas por termo/fonte. Padrao: igual a --max-results.",
    )
    return parser.parse_args()


def _sources(values: list[str] | None) -> list[str]:
    if not values or "all" in values:
        return ["buscape", "zoom"]
    return values


def main() -> int:
    args = parse_args()
    queries = args.queries or DEFAULT_QUERIES
    sources = _sources(args.source)
    results = []

    for source_name in sources:
        for query in queries:
            try:
                result = ingest_price_comparison_web_history(
                    source_name=source_name,
                    query=query,
                    max_results=args.max_results,
                    product_detail_limit=args.product_detail_limit,
                    triggered_by="local_cli_price_history_web",
                )
                results.append(
                    {
                        "source_name": source_name,
                        "query": query,
                        "status": result.status,
                        "extracted": result.extracted,
                        "loaded": result.loaded,
                        "skipped": result.skipped,
                        "blocked": result.blocked,
                        "source_run_id": str(result.source_run_id),
                    }
                )
            except Exception as error:
                results.append(
                    {
                        "source_name": source_name,
                        "query": query,
                        "status": "failed",
                        "error": str(error),
                    }
                )

    summary = {
        "sources": sources,
        "queries": len(queries),
        "runs": len(results),
        "success": sum(1 for result in results if result["status"] == "success"),
        "partial": sum(1 for result in results if result["status"] == "partial"),
        "failed": sum(1 for result in results if result["status"] == "failed"),
        "extracted": sum(int(result.get("extracted", 0)) for result in results),
        "loaded": sum(int(result.get("loaded", 0)) for result in results),
        "skipped": sum(int(result.get("skipped", 0)) for result in results),
        "blocked": sum(int(result.get("blocked", 0)) for result in results),
        "results": results,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
